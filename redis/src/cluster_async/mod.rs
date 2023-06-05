//! This module provides async functionality for Redis Cluster.
//!
//! By default, [`ClusterConnection`] makes use of [`MultiplexedConnection`] and maintains a pool
//! of connections to each node in the cluster. While it  generally behaves similarly to
//! the sync cluster module, certain commands do not route identically, due most notably to
//! a current lack of support for routing commands to multiple nodes.
//!
//! Also note that pubsub functionality is not currently provided by this module.
//!
//! # Example
//! ```rust,no_run
//! use redis::cluster::ClusterClient;
//! use redis::AsyncCommands;
//!
//! async fn fetch_an_integer() -> String {
//!     let nodes = vec!["redis://127.0.0.1/"];
//!     let client = ClusterClient::new(nodes).unwrap();
//!     let mut connection = client.get_async_connection().await.unwrap();
//!     let _: () = connection.set("test", "test_data").await.unwrap();
//!     let rv: String = connection.get("test").await.unwrap();
//!     return rv;
//! }
//! ```
use std::{
    collections::HashMap,
    fmt, io,
    iter::Iterator,
    marker::Unpin,
    mem,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

use crate::{
    aio::{ConnectionLike, MultiplexedConnection},
    cluster::{get_connection_info, parse_slots, slot_cmd},
    cluster_client::{ClusterParams, RetryParams},
    cluster_routing::{Redirect, Route, RoutingInfo, Slot, SlotMap},
    Cmd, ConnectionInfo, ErrorKind, IntoConnectionInfo, RedisError, RedisFuture, RedisResult,
    Value,
};

#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use crate::aio::{async_std::AsyncStd, RedisRuntime};
use futures::{
    future::{self, BoxFuture},
    prelude::*,
    ready, stream,
};
use log::trace;
use pin_project_lite::pin_project;
use rand::{seq::IteratorRandom, thread_rng};
use tokio::sync::{mpsc, oneshot, RwLock};

const SLOT_SIZE: usize = 16384;

/// This represents an async Redis Cluster connection. It stores the
/// underlying connections maintained for each node in the cluster, as well
/// as common parameters for connecting to nodes and executing commands.
#[derive(Clone)]
pub struct ClusterConnection<C = MultiplexedConnection>(mpsc::Sender<Message<C>>);

impl<C> ClusterConnection<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    pub(crate) async fn new(
        initial_nodes: &[ConnectionInfo],
        cluster_params: ClusterParams,
    ) -> RedisResult<ClusterConnection<C>> {
        ClusterConnInner::new(initial_nodes, cluster_params)
            .await
            .map(|inner| {
                let (tx, mut rx) = mpsc::channel::<Message<_>>(100);
                let stream = async move {
                    let _ = stream::poll_fn(move |cx| rx.poll_recv(cx))
                        .map(Ok)
                        .forward(inner)
                        .await;
                };
                #[cfg(feature = "tokio-comp")]
                tokio::spawn(stream);
                #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
                AsyncStd::spawn(stream);

                ClusterConnection(tx)
            })
    }
}

type ConnectionFuture<C> = future::Shared<BoxFuture<'static, C>>;
type ConnectionMap<C> = HashMap<String, ConnectionFuture<C>>;

struct InnerCore<C> {
    conn_lock: RwLock<(ConnectionMap<C>, SlotMap)>,
    cluster_params: ClusterParams,
}

type Core<C> = Arc<InnerCore<C>>;

struct ClusterConnInner<C> {
    inner: Core<C>,
    state: ConnectionState,
    #[allow(clippy::complexity)]
    in_flight_requests: stream::FuturesUnordered<
        Pin<
            Box<Request<BoxFuture<'static, (OperationTarget, RedisResult<Response>)>, Response, C>>,
        >,
    >,
    refresh_error: Option<RedisError>,
    pending_requests: Vec<PendingRequest<Response, C>>,
}

#[derive(Clone)]
enum CmdArg<C> {
    Cmd {
        cmd: Arc<Cmd>,
        func: fn(C, Arc<Cmd>) -> RedisFuture<'static, Response>,
        routing: Option<RoutingInfo>,
    },
    Pipeline {
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        func: fn(C, Arc<crate::Pipeline>, usize, usize) -> RedisFuture<'static, Response>,
        route: Option<Route>,
    },
}

fn route_pipeline(pipeline: &crate::Pipeline) -> Option<Route> {
    fn route_for_command(cmd: &Cmd) -> Option<Route> {
        match RoutingInfo::for_routable(cmd) {
            Some(RoutingInfo::Random) => None,
            Some(RoutingInfo::SpecificNode(route)) => Some(route),
            Some(RoutingInfo::AllNodes) | Some(RoutingInfo::AllMasters) => None,
            _ => None,
        }
    }

    // Find first specific slot and send to it. There's no need to check If later commands
    // should be routed to a different slot, since the server will return an error indicating this.
    pipeline
        .cmd_iter()
        .map(route_for_command)
        .find(|route| route.is_some())
        .flatten()
}
enum Response {
    Single(Value),
    Multiple(Vec<Value>),
}

enum OperationTarget {
    Node { address: String },
    FanOut,
}

impl From<String> for OperationTarget {
    fn from(address: String) -> Self {
        OperationTarget::Node { address }
    }
}

struct Message<C> {
    cmd: CmdArg<C>,
    sender: oneshot::Sender<RedisResult<Response>>,
}

enum RecoverFuture {
    RecoverSlots(BoxFuture<'static, RedisResult<()>>),
    Reconnect(BoxFuture<'static, ()>),
}

enum ConnectionState {
    PollComplete,
    Recover(RecoverFuture),
}

impl fmt::Debug for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ConnectionState::PollComplete => "PollComplete",
                ConnectionState::Recover(_) => "Recover",
            }
        )
    }
}

#[derive(Clone)]
struct RequestInfo<C> {
    cmd: CmdArg<C>,
    redirect: Option<Redirect>,
}

pin_project! {
    #[project = RequestStateProj]
    enum RequestState<F> {
        None,
        Future {
            #[pin]
            future: F,
        },
        Sleep {
            #[pin]
            sleep: BoxFuture<'static, ()>,
        },
    }
}

struct PendingRequest<I, C> {
    retry: u32,
    sender: oneshot::Sender<RedisResult<I>>,
    info: RequestInfo<C>,
}

pin_project! {
    struct Request<F, I, C> {
        retry_params: RetryParams,
        request: Option<PendingRequest<I, C>>,
        #[pin]
        future: RequestState<F>,
    }
}

#[must_use]
enum Next<I, C> {
    Retry {
        request: PendingRequest<I, C>,
    },
    Reconnect {
        request: PendingRequest<I, C>,
        target: OperationTarget,
    },
    RefreshSlots {
        request: PendingRequest<I, C>,
    },
    Done,
}

impl<F, I, C> Future for Request<F, I, C>
where
    F: Future<Output = (OperationTarget, RedisResult<I>)>,
    C: ConnectionLike,
{
    type Output = Next<I, C>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        if this.request.is_none() {
            return Poll::Ready(Next::Done);
        }
        let future = match this.future.as_mut().project() {
            RequestStateProj::Future { future } => future,
            RequestStateProj::Sleep { sleep } => {
                ready!(sleep.poll(cx));
                return Next::Retry {
                    request: self.project().request.take().unwrap(),
                }
                .into();
            }
            _ => panic!("Request future must be Some"),
        };
        match ready!(future.poll(cx)) {
            (_, Ok(item)) => {
                trace!("Ok");
                self.respond(Ok(item));
                Next::Done.into()
            }
            (target, Err(err)) => {
                trace!("Request error {}", err);

                let address = match target {
                    OperationTarget::Node { address } => address,
                    OperationTarget::FanOut => {
                        // TODO - implement retries on fan-out operations
                        self.respond(Err(err));
                        return Next::Done.into();
                    }
                };

                let request = this.request.as_mut().unwrap();

                if request.retry >= this.retry_params.number_of_retries {
                    self.respond(Err(err));
                    return Next::Done.into();
                }
                request.retry = request.retry.saturating_add(1);

                match err.kind() {
                    ErrorKind::Ask => {
                        let mut request = this.request.take().unwrap();
                        request.info.redirect = err
                            .redirect_node()
                            .map(|(node, _slot)| Redirect::Ask(node.to_string()));
                        Next::Retry { request }.into()
                    }
                    ErrorKind::Moved => {
                        let mut request = this.request.take().unwrap();
                        request.info.redirect = err
                            .redirect_node()
                            .map(|(node, _slot)| Redirect::Moved(node.to_string()));
                        Next::RefreshSlots { request }.into()
                    }
                    ErrorKind::TryAgain | ErrorKind::ClusterDown => {
                        // Sleep and retry.
                        let sleep_duration = this.retry_params.wait_time_for_retry(request.retry);
                        this.future.set(RequestState::Sleep {
                            #[cfg(feature = "tokio-comp")]
                            sleep: Box::pin(tokio::time::sleep(sleep_duration)),

                            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
                            sleep: Box::pin(async_std::task::sleep(sleep_duration)),
                        });
                        self.poll(cx)
                    }
                    ErrorKind::IoError => Next::Reconnect {
                        request: this.request.take().unwrap(),
                        target: OperationTarget::Node { address },
                    }
                    .into(),
                    _ => {
                        if err.is_retryable() {
                            Next::Retry {
                                request: this.request.take().unwrap(),
                            }
                            .into()
                        } else {
                            self.respond(Err(err));
                            Next::Done.into()
                        }
                    }
                }
            }
        }
    }
}

impl<F, I, C> Request<F, I, C>
where
    F: Future<Output = (OperationTarget, RedisResult<I>)>,
    C: ConnectionLike,
{
    fn respond(self: Pin<&mut Self>, msg: RedisResult<I>) {
        // If `send` errors the receiver has dropped and thus does not care about the message
        let _ = self
            .project()
            .request
            .take()
            .expect("Result should only be sent once")
            .sender
            .send(msg);
    }
}

impl<C> ClusterConnInner<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    async fn new(
        initial_nodes: &[ConnectionInfo],
        cluster_params: ClusterParams,
    ) -> RedisResult<Self> {
        let connections = Self::create_initial_connections(initial_nodes, &cluster_params).await?;
        let inner = Arc::new(InnerCore {
            conn_lock: RwLock::new((connections, Default::default())),
            cluster_params,
        });
        let mut connection = ClusterConnInner {
            inner,
            in_flight_requests: Default::default(),
            refresh_error: None,
            pending_requests: Vec::new(),
            state: ConnectionState::PollComplete,
        };
        connection.refresh_slots().await?;
        Ok(connection)
    }

    async fn create_initial_connections(
        initial_nodes: &[ConnectionInfo],
        params: &ClusterParams,
    ) -> RedisResult<ConnectionMap<C>> {
        let connections = stream::iter(initial_nodes.iter().cloned())
            .map(|info| {
                let params = params.clone();
                async move {
                    let addr = info.addr.to_string();
                    let result = connect_and_check(&addr, params).await;
                    match result {
                        Ok(conn) => Some((addr, async { conn }.boxed().shared())),
                        Err(e) => {
                            trace!("Failed to connect to initial node: {:?}", e);
                            None
                        }
                    }
                }
            })
            .buffer_unordered(initial_nodes.len())
            .fold(
                HashMap::with_capacity(initial_nodes.len()),
                |mut connections: ConnectionMap<C>, conn| async move {
                    connections.extend(conn);
                    connections
                },
            )
            .await;
        if connections.is_empty() {
            return Err(RedisError::from((
                ErrorKind::IoError,
                "Failed to create initial connections",
            )));
        }
        Ok(connections)
    }

    fn refresh_connections(&mut self, addrs: Vec<String>) -> impl Future<Output = ()> {
        let inner = self.inner.clone();
        async move {
            let mut write_guard = inner.conn_lock.write().await;
            let mut connections = stream::iter(addrs)
                .fold(
                    mem::take(&mut write_guard.0),
                    |mut connections, addr| async {
                        let conn = Self::get_or_create_conn(
                            &addr,
                            connections.remove(&addr),
                            &inner.cluster_params,
                        )
                        .await;
                        if let Ok(conn) = conn {
                            connections.insert(addr, async { conn }.boxed().shared());
                        }
                        connections
                    },
                )
                .await;
            write_guard.0 = mem::take(&mut connections);
        }
    }

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(&mut self) -> impl Future<Output = RedisResult<()>> {
        let inner = self.inner.clone();

        async move {
            let mut write_guard = inner.conn_lock.write().await;
            let mut connections = mem::take(&mut write_guard.0);
            let slots = &mut write_guard.1;
            let mut result = Ok(());
            for (_, conn) in connections.iter_mut() {
                let mut conn = conn.clone().await;
                let value = match conn.req_packed_command(&slot_cmd()).await {
                    Ok(value) => value,
                    Err(err) => {
                        result = Err(err);
                        continue;
                    }
                };
                match parse_slots(value, inner.cluster_params.tls).and_then(|v| {
                    Self::build_slot_map(slots, v, inner.cluster_params.read_from_replicas)
                }) {
                    Ok(_) => {
                        result = Ok(());
                        break;
                    }
                    Err(err) => result = Err(err),
                }
            }
            result?;

            let mut nodes = write_guard.1.values().flatten().collect::<Vec<_>>();
            nodes.sort_unstable();
            nodes.dedup();
            let nodes_len = nodes.len();
            let addresses_and_connections_iter = nodes
                .into_iter()
                .map(|addr| (addr, connections.remove(addr)));

            write_guard.0 = stream::iter(addresses_and_connections_iter)
                .fold(
                    HashMap::with_capacity(nodes_len),
                    |mut connections, (addr, connection)| async {
                        let conn =
                            Self::get_or_create_conn(addr, connection, &inner.cluster_params).await;
                        if let Ok(conn) = conn {
                            connections.insert(addr.to_string(), async { conn }.boxed().shared());
                        }
                        connections
                    },
                )
                .await;

            Ok(())
        }
    }

    fn build_slot_map(
        slot_map: &mut SlotMap,
        mut slots_data: Vec<Slot>,
        read_from_replicas: bool,
    ) -> RedisResult<()> {
        slots_data.sort_by_key(|slot_data| slot_data.start());
        let last_slot = slots_data.iter().try_fold(0, |prev_end, slot_data| {
            if prev_end != slot_data.start() {
                return Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "Slot refresh error.",
                    format!(
                        "Received overlapping slots {} and {}..{}",
                        prev_end,
                        slot_data.start(),
                        slot_data.end()
                    ),
                )));
            }
            Ok(slot_data.end() + 1)
        })?;

        if usize::from(last_slot) != SLOT_SIZE {
            return Err(RedisError::from((
                ErrorKind::ResponseError,
                "Slot refresh error.",
                format!("Lacks the slots >= {last_slot}"),
            )));
        }
        slot_map.clear();
        slot_map.fill_slots(&slots_data, read_from_replicas);
        trace!("{:?}", slot_map);
        Ok(())
    }

    async fn execute_on_all_nodes(
        func: fn(C, Arc<Cmd>) -> RedisFuture<'static, Response>,
        cmd: &Arc<Cmd>,
        only_primaries: bool,
        core: Core<C>,
    ) -> (OperationTarget, RedisResult<Response>) {
        let read_guard = core.conn_lock.read().await;
        let results = future::join_all(
            read_guard
                .1
                .all_unique_addresses(only_primaries)
                .into_iter()
                .filter_map(|addr| read_guard.0.get(addr).cloned())
                .map(|conn| {
                    (async {
                        let conn = conn.await;
                        func(conn, cmd.clone()).await
                    })
                    .boxed()
                }),
        )
        .await;
        drop(read_guard);
        let mut merged_results = Vec::with_capacity(results.len());

        // TODO - we can have better error reporting here if we had an Error variant on Value.
        for result in results {
            match result {
                Ok(response) => match response {
                    Response::Single(value) => merged_results.push(value),
                    Response::Multiple(_) => unreachable!(),
                },
                Err(_) => {
                    return (OperationTarget::FanOut, result);
                }
            }
        }

        (
            OperationTarget::FanOut,
            Ok(Response::Single(Value::Bulk(merged_results))),
        )
    }

    async fn try_cmd_request(
        cmd: Arc<Cmd>,
        func: fn(C, Arc<Cmd>) -> RedisFuture<'static, Response>,
        redirect: Option<Redirect>,
        routing: Option<RoutingInfo>,
        core: Core<C>,
        asking: bool,
    ) -> (OperationTarget, RedisResult<Response>) {
        let route_option = match routing.as_ref().unwrap_or(&RoutingInfo::Random) {
            RoutingInfo::AllNodes => {
                return Self::execute_on_all_nodes(func, &cmd, false, core).await
            }
            RoutingInfo::AllMasters => {
                return Self::execute_on_all_nodes(func, &cmd, true, core).await
            }
            RoutingInfo::Random => None,
            RoutingInfo::SpecificNode(route) => Some(route),
        };
        let (addr, conn) = Self::get_connection(redirect, route_option, core, asking).await;
        let result = func(conn, cmd).await;
        (addr.into(), result)
    }

    async fn try_pipeline_request(
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        func: fn(C, Arc<crate::Pipeline>, usize, usize) -> RedisFuture<'static, Response>,
        conn: impl Future<Output = (String, C)>,
    ) -> (OperationTarget, RedisResult<Response>) {
        let (addr, conn) = conn.await;
        let result = func(conn, pipeline, offset, count).await;
        (OperationTarget::Node { address: addr }, result)
    }

    async fn try_request(
        info: RequestInfo<C>,
        core: Core<C>,
    ) -> (OperationTarget, RedisResult<Response>) {
        let asking = matches!(&info.redirect, Some(Redirect::Ask(_)));

        match info.cmd {
            CmdArg::Cmd { cmd, func, routing } => {
                Self::try_cmd_request(cmd, func, info.redirect, routing, core, asking).await
            }
            CmdArg::Pipeline {
                pipeline,
                offset,
                count,
                func,
                route,
            } => {
                Self::try_pipeline_request(
                    pipeline,
                    offset,
                    count,
                    func,
                    Self::get_connection(info.redirect, route.as_ref(), core, asking),
                )
                .await
            }
        }
    }

    async fn get_connection(
        mut redirect: Option<Redirect>,
        route: Option<&Route>,
        core: Core<C>,
        asking: bool,
    ) -> (String, C) {
        let read_guard = core.conn_lock.read().await;

        let conn = match redirect.take() {
            Some(Redirect::Moved(moved_addr)) => Some(moved_addr),
            Some(Redirect::Ask(ask_addr)) => Some(ask_addr),
            None => route
                .as_ref()
                .and_then(|route| read_guard.1.slot_addr_for_route(route))
                .map(|addr| addr.to_string()),
        }
        .map(|addr| {
            let conn = read_guard.0.get(&addr).cloned();
            (addr, conn)
        });
        drop(read_guard);

        let addr_conn_option = match conn {
            Some((addr, Some(conn))) => Some((addr, conn.await)),
            Some((addr, None)) => connect_and_check(&addr, core.cluster_params.clone())
                .await
                .ok()
                .map(|conn| (addr, conn)),
            None => None,
        };

        let (addr, mut conn) = match addr_conn_option {
            Some(tuple) => tuple,
            None => {
                let read_guard = core.conn_lock.read().await;
                let (random_addr, random_conn_future) = get_random_connection(&read_guard.0);
                drop(read_guard);
                (random_addr, random_conn_future.await)
            }
        };
        if asking {
            let _ = conn.req_packed_command(&crate::cmd::cmd("ASKING")).await;
        }
        (addr, conn)
    }

    fn poll_recover(
        &mut self,
        cx: &mut task::Context<'_>,
        future: RecoverFuture,
    ) -> Poll<Result<(), RedisError>> {
        match future {
            RecoverFuture::RecoverSlots(mut future) => match future.as_mut().poll(cx) {
                Poll::Ready(Ok(_)) => {
                    trace!("Recovered!");
                    self.state = ConnectionState::PollComplete;
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => {
                    self.state = ConnectionState::Recover(RecoverFuture::RecoverSlots(future));
                    trace!("Recover not ready");
                    Poll::Pending
                }
                Poll::Ready(Err(err)) => {
                    self.state = ConnectionState::Recover(RecoverFuture::RecoverSlots(Box::pin(
                        self.refresh_slots(),
                    )));
                    Poll::Ready(Err(err))
                }
            },
            RecoverFuture::Reconnect(mut future) => match future.as_mut().poll(cx) {
                Poll::Ready(_) => {
                    trace!("Reconnected connections");
                    self.state = ConnectionState::PollComplete;
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => {
                    self.state = ConnectionState::Recover(RecoverFuture::Reconnect(future));
                    trace!("Recover not ready");
                    Poll::Pending
                }
            },
        }
    }

    fn poll_complete(&mut self, cx: &mut task::Context<'_>) -> Poll<PollFlushAction> {
        let mut poll_flush_action = PollFlushAction::None;

        if !self.pending_requests.is_empty() {
            let mut pending_requests = mem::take(&mut self.pending_requests);
            for request in pending_requests.drain(..) {
                // Drop the request if noone is waiting for a response to free up resources for
                // requests callers care about (load shedding). It will be ambigous whether the
                // request actually goes through regardless.
                if request.sender.is_closed() {
                    continue;
                }

                let future = Self::try_request(request.info.clone(), self.inner.clone()).boxed();
                self.in_flight_requests.push(Box::pin(Request {
                    retry_params: self.inner.cluster_params.retry_params.clone(),
                    request: Some(request),
                    future: RequestState::Future { future },
                }));
            }
            self.pending_requests = pending_requests;
        }

        loop {
            let result = match Pin::new(&mut self.in_flight_requests).poll_next(cx) {
                Poll::Ready(Some(result)) => result,
                Poll::Ready(None) | Poll::Pending => break,
            };
            match result {
                Next::Done => {}
                Next::Retry { request } => {
                    let future = Self::try_request(request.info.clone(), self.inner.clone());
                    self.in_flight_requests.push(Box::pin(Request {
                        retry_params: self.inner.cluster_params.retry_params.clone(),
                        request: Some(request),
                        future: RequestState::Future {
                            future: Box::pin(future),
                        },
                    }));
                }
                Next::RefreshSlots { request } => {
                    poll_flush_action =
                        poll_flush_action.change_state(PollFlushAction::RebuildSlots);
                    let future = Self::try_request(request.info.clone(), self.inner.clone());
                    self.in_flight_requests.push(Box::pin(Request {
                        retry_params: self.inner.cluster_params.retry_params.clone(),
                        request: Some(request),
                        future: RequestState::Future {
                            future: Box::pin(future),
                        },
                    }));
                }
                Next::Reconnect {
                    request, target, ..
                } => {
                    poll_flush_action = match target {
                        OperationTarget::Node { address } => poll_flush_action
                            .change_state(PollFlushAction::Reconnect(vec![address])),
                        OperationTarget::FanOut => {
                            poll_flush_action.change_state(PollFlushAction::RebuildSlots)
                        }
                    };
                    self.pending_requests.push(request);
                }
            }
        }

        match poll_flush_action {
            PollFlushAction::None => {
                if self.in_flight_requests.is_empty() {
                    Poll::Ready(poll_flush_action)
                } else {
                    Poll::Pending
                }
            }
            rebuild @ PollFlushAction::RebuildSlots => Poll::Ready(rebuild),
            reestablish @ PollFlushAction::Reconnect(_) => Poll::Ready(reestablish),
        }
    }

    fn send_refresh_error(&mut self) {
        if self.refresh_error.is_some() {
            if let Some(mut request) = Pin::new(&mut self.in_flight_requests)
                .iter_pin_mut()
                .find(|request| request.request.is_some())
            {
                (*request)
                    .as_mut()
                    .respond(Err(self.refresh_error.take().unwrap()));
            } else if let Some(request) = self.pending_requests.pop() {
                let _ = request.sender.send(Err(self.refresh_error.take().unwrap()));
            }
        }
    }

    async fn get_or_create_conn(
        addr: &str,
        conn_option: Option<ConnectionFuture<C>>,
        params: &ClusterParams,
    ) -> RedisResult<C> {
        if let Some(conn) = conn_option {
            let mut conn = conn.await;
            match check_connection(&mut conn).await {
                Ok(_) => Ok(conn),
                Err(_) => connect_and_check(addr, params.clone()).await,
            }
        } else {
            connect_and_check(addr, params.clone()).await
        }
    }
}

enum PollFlushAction {
    None,
    RebuildSlots,
    Reconnect(Vec<String>),
}

impl PollFlushAction {
    fn change_state(self, next_state: PollFlushAction) -> PollFlushAction {
        match self {
            Self::None => next_state,
            rebuild @ Self::RebuildSlots => rebuild,
            Self::Reconnect(mut addrs) => match next_state {
                rebuild @ Self::RebuildSlots => rebuild,
                Self::Reconnect(new_addrs) => {
                    addrs.extend(new_addrs);
                    Self::Reconnect(addrs)
                }
                Self::None => Self::Reconnect(addrs),
            },
        }
    }
}

impl<C> Sink<Message<C>> for ClusterConnInner<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    type Error = ();

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        match mem::replace(&mut self.state, ConnectionState::PollComplete) {
            ConnectionState::PollComplete => Poll::Ready(Ok(())),
            ConnectionState::Recover(future) => {
                match ready!(self.as_mut().poll_recover(cx, future)) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(err) => {
                        // We failed to reconnect, while we will try again we will report the
                        // error if we can to avoid getting trapped in an infinite loop of
                        // trying to reconnect
                        if let Some(mut request) = Pin::new(&mut self.in_flight_requests)
                            .iter_pin_mut()
                            .find(|request| request.request.is_some())
                        {
                            (*request).as_mut().respond(Err(err));
                        } else {
                            self.refresh_error = Some(err);
                        }
                        Poll::Ready(Ok(()))
                    }
                }
            }
        }
    }

    fn start_send(mut self: Pin<&mut Self>, msg: Message<C>) -> Result<(), Self::Error> {
        trace!("start_send");
        let Message { cmd, sender } = msg;

        let redirect = None;
        let info = RequestInfo { cmd, redirect };

        self.pending_requests.push(PendingRequest {
            retry: 0,
            sender,
            info,
        });
        Ok(())
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        trace!("poll_complete: {:?}", self.state);
        loop {
            self.send_refresh_error();

            match mem::replace(&mut self.state, ConnectionState::PollComplete) {
                ConnectionState::Recover(future) => {
                    match ready!(self.as_mut().poll_recover(cx, future)) {
                        Ok(()) => (),
                        Err(err) => {
                            // We failed to reconnect, while we will try again we will report the
                            // error if we can to avoid getting trapped in an infinite loop of
                            // trying to reconnect
                            self.refresh_error = Some(err);

                            // Give other tasks a chance to progress before we try to recover
                            // again. Since the future may not have registered a wake up we do so
                            // now so the task is not forgotten
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                }
                ConnectionState::PollComplete => match ready!(self.poll_complete(cx)) {
                    PollFlushAction::None => return Poll::Ready(Ok(())),
                    PollFlushAction::RebuildSlots => {
                        self.state = ConnectionState::Recover(RecoverFuture::RecoverSlots(
                            Box::pin(self.refresh_slots()),
                        ));
                    }
                    PollFlushAction::Reconnect(addrs) => {
                        self.state = ConnectionState::Recover(RecoverFuture::Reconnect(Box::pin(
                            self.refresh_connections(addrs),
                        )));
                    }
                },
            }
        }
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        // Try to drive any in flight requests to completion
        match self.poll_complete(cx) {
            Poll::Ready(poll_flush_action) => match poll_flush_action {
                PollFlushAction::None => (),
                _ => Err(()).map_err(|_| ())?,
            },
            Poll::Pending => (),
        };
        // If we no longer have any requests in flight we are done (skips any reconnection
        // attempts)
        if self.in_flight_requests.is_empty() {
            return Poll::Ready(Ok(()));
        }

        self.poll_flush(cx)
    }
}

impl<C> ConnectionLike for ClusterConnection<C>
where
    C: ConnectionLike + Send + 'static,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        trace!("req_packed_command");
        let (sender, receiver) = oneshot::channel();
        Box::pin(async move {
            self.0
                .send(Message {
                    cmd: CmdArg::Cmd {
                        cmd: Arc::new(cmd.clone()), // TODO Remove this clone?
                        func: |mut conn, cmd| {
                            Box::pin(async move {
                                conn.req_packed_command(&cmd).await.map(Response::Single)
                            })
                        },
                        routing: RoutingInfo::for_routable(cmd),
                    },
                    sender,
                })
                .await
                .map_err(|_| {
                    RedisError::from(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "redis_cluster: Unable to send command",
                    ))
                })?;
            receiver
                .await
                .unwrap_or_else(|_| {
                    Err(RedisError::from(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "redis_cluster: Unable to receive command",
                    )))
                })
                .map(|response| match response {
                    Response::Single(value) => value,
                    Response::Multiple(_) => unreachable!(),
                })
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        let (sender, receiver) = oneshot::channel();
        Box::pin(async move {
            self.0
                .send(Message {
                    cmd: CmdArg::Pipeline {
                        pipeline: Arc::new(pipeline.clone()), // TODO Remove this clone?
                        offset,
                        count,
                        func: |mut conn, pipeline, offset, count| {
                            Box::pin(async move {
                                conn.req_packed_commands(&pipeline, offset, count)
                                    .await
                                    .map(Response::Multiple)
                            })
                        },
                        route: route_pipeline(pipeline),
                    },
                    sender,
                })
                .await
                .map_err(|_| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))?;

            receiver
                .await
                .unwrap_or_else(|_| {
                    Err(RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
                })
                .map(|response| match response {
                    Response::Multiple(values) => values,
                    Response::Single(_) => unreachable!(),
                })
        })
    }

    fn get_db(&self) -> i64 {
        0
    }
}
/// Implements the process of connecting to a Redis server
/// and obtaining a connection handle.
pub trait Connect: Sized {
    /// Connect to a node, returning handle for command execution.
    fn connect<'a, T>(info: T) -> RedisFuture<'a, Self>
    where
        T: IntoConnectionInfo + Send + 'a;
}

impl Connect for MultiplexedConnection {
    fn connect<'a, T>(info: T) -> RedisFuture<'a, MultiplexedConnection>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        async move {
            let connection_info = info.into_connection_info()?;
            let client = crate::Client::open(connection_info)?;

            #[cfg(feature = "tokio-comp")]
            return client.get_multiplexed_tokio_connection().await;

            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
            return client.get_multiplexed_async_std_connection().await;
        }
        .boxed()
    }
}

async fn connect_and_check<C>(node: &str, params: ClusterParams) -> RedisResult<C>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let read_from_replicas = params.read_from_replicas;
    let info = get_connection_info(node, params)?;
    let mut conn = C::connect(info).await?;
    check_connection(&mut conn).await?;
    if read_from_replicas {
        // If READONLY is sent to primary nodes, it will have no effect
        crate::cmd("READONLY").query_async(&mut conn).await?;
    }
    Ok(conn)
}

async fn check_connection<C>(conn: &mut C) -> RedisResult<()>
where
    C: ConnectionLike + Send + 'static,
{
    let mut cmd = Cmd::new();
    cmd.arg("PING");
    cmd.query_async::<_, String>(conn).await?;
    Ok(())
}

// TODO: This function can panic and should probably
// return an Option instead:
fn get_random_connection<C>(connections: &ConnectionMap<C>) -> (String, ConnectionFuture<C>)
where
    C: Clone,
{
    let addr = connections
        .keys()
        .choose(&mut thread_rng())
        .expect("Connections is empty")
        .to_string();
    let conn = connections
        .get(&addr)
        .expect("Connections is empty")
        .clone();
    (addr, conn)
}

#[cfg(test)]
mod pipeline_routing_tests {
    use super::route_pipeline;
    use crate::{
        cluster_routing::{Route, SlotAddr},
        cmd,
    };

    #[test]
    fn test_first_route_is_found() {
        let mut pipeline = crate::Pipeline::new();

        pipeline
            .add_command(cmd("FLUSHALL")) // route to all masters
            .get("foo") // route to slot 12182
            .add_command(cmd("EVAL")); // route randomly

        assert_eq!(
            route_pipeline(&pipeline),
            Some(Route::new(12182, SlotAddr::Replica))
        );
    }

    #[test]
    fn test_ignore_conflicting_slots() {
        let mut pipeline = crate::Pipeline::new();

        pipeline
            .add_command(cmd("FLUSHALL")) // route to all masters
            .set("baz", "bar") // route to slot 4813
            .get("foo"); // route to slot 12182

        assert_eq!(
            route_pipeline(&pipeline),
            Some(Route::new(4813, SlotAddr::Master))
        );
    }
}
