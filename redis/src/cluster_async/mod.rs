//! This is a rust implementation for Redis cluster library.
//!
//! This library extends redis-rs library to be able to use cluster.
//! Client impletemts traits of ConnectionLike and Commands.
//! So you can use redis-rs's access methods.
//! If you want more information, read document of redis-rs.
//!
//! Note that this library is currently not have features of Pubsub.
//!
//! # Example
//! ```rust,no_run
//! use redis::{
//!     cluster_async::Client,
//!     Commands, cmd, RedisResult
//! };
//!
//! #[tokio::main]
//! async fn main() -> RedisResult<()> {
//! #   let _ = env_logger::try_init();
//!     let nodes = vec!["redis://127.0.0.1:7000/", "redis://127.0.0.1:7001/", "redis://127.0.0.1:7002/"];
//!
//!     let client = Client::open(nodes)?;
//!     let mut connection = client.get_connection().await?;
//!     cmd("SET").arg("test").arg("test_data").query_async(&mut connection).await?;
//!     let res: String = cmd("GET").arg("test").query_async(&mut connection).await?;
//!     assert_eq!(res, "test_data");
//!     Ok(())
//! }
//! ```
//!
//! # Pipelining
//! ```rust,no_run
//! use redis::{
//!     cluster_async::Client,
//!     pipe, RedisResult
//! };
//!
//! #[tokio::main]
//! async fn main() -> RedisResult<()> {
//! #   let _ = env_logger::try_init();
//!     let nodes = vec!["redis://127.0.0.1:7000/", "redis://127.0.0.1:7001/", "redis://127.0.0.1:7002/"];
//!
//!     let client = Client::open(nodes)?;
//!     let mut connection = client.get_connection().await?;
//!     let key = "test2";
//!
//!     let mut pipe = pipe();
//!     pipe.rpush(key, "123").ignore()
//!         .ltrim(key, -10, -1).ignore()
//!         .expire(key, 60).ignore();
//!     pipe.query_async(&mut connection)
//!         .await?;
//!     Ok(())
//! }
//! ```

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt, io,
    iter::Iterator,
    marker::Unpin,
    mem,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};

use crate::{
    aio::{ConnectionLike, MultiplexedConnection},
    cluster::{get_connection_info, parse_slots, slot_cmd, TlsMode},
    cluster_client::ClusterParams,
    cluster_routing::{RoutingInfo, Slot},
    Cmd, ConnectionAddr, ConnectionInfo, ErrorKind, IntoConnectionInfo, RedisError, RedisFuture,
    RedisResult, Value,
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
use rand::seq::IteratorRandom;
use rand::thread_rng;
use tokio::sync::{mpsc, oneshot};

const SLOT_SIZE: usize = 16384;
const DEFAULT_RETRIES: u32 = 16;

/// This is a Redis cluster client.
pub struct Client {
    initial_nodes: Vec<ConnectionInfo>,
    retries: Option<u32>,
}

impl Client {
    /// Connect to a redis cluster server and return a cluster client.
    /// This does not actually open a connection yet but it performs some basic checks on the URL.
    ///
    /// # Errors
    ///
    /// If it is failed to parse initial_nodes, an error is returned.
    pub fn open<T: IntoConnectionInfo>(initial_nodes: Vec<T>) -> RedisResult<Client> {
        let mut nodes = Vec::with_capacity(initial_nodes.len());

        for info in initial_nodes {
            let info = info.into_connection_info()?;
            if let ConnectionAddr::Unix(_) = info.addr {
                return Err(RedisError::from((ErrorKind::InvalidClientConfig,
                                             "This library cannot use unix socket because Redis's cluster command returns only cluster's IP and port.")));
            }
            nodes.push(info);
        }

        Ok(Client {
            initial_nodes: nodes,
            retries: Some(DEFAULT_RETRIES),
        })
    }

    /// Set how many times we should retry a query. Set `None` to retry forever.
    /// Default: 16
    pub fn set_retries(&mut self, retries: Option<u32>) -> &mut Self {
        self.retries = retries;
        self
    }

    /// Open and get a Redis cluster connection.
    ///
    /// # Errors
    ///
    /// If it is failed to open connections and to create slots, an error is returned.
    pub async fn get_connection(&self) -> RedisResult<Connection> {
        Connection::new(&self.initial_nodes, self.retries).await
    }

    #[doc(hidden)]
    pub async fn get_generic_connection<C>(&self) -> RedisResult<Connection<C>>
    where
        C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
    {
        Connection::new(&self.initial_nodes, self.retries).await
    }
}

/// This is a connection of Redis cluster.
#[derive(Clone)]
pub struct Connection<C = MultiplexedConnection>(mpsc::Sender<Message<C>>);

impl<C> Connection<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    async fn new(
        initial_nodes: &[ConnectionInfo],
        retries: Option<u32>,
    ) -> RedisResult<Connection<C>> {
        Pipeline::new(initial_nodes, retries).await.map(|pipeline| {
            let (tx, mut rx) = mpsc::channel::<Message<_>>(100);
            let stream = async move {
                let _ = stream::poll_fn(move |cx| rx.poll_recv(cx))
                    .map(Ok)
                    .forward(pipeline)
                    .await;
            };
            #[cfg(feature = "tokio-comp")]
            tokio::spawn(stream);
            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
            AsyncStd::spawn(stream);

            Connection(tx)
        })
    }
}

type SlotMap = BTreeMap<u16, String>;
type ConnectionFuture<C> = future::Shared<BoxFuture<'static, C>>;
type ConnectionMap<C> = HashMap<String, ConnectionFuture<C>>;

struct Pipeline<C> {
    connections: ConnectionMap<C>,
    slots: SlotMap,
    state: ConnectionState<C>,
    #[allow(clippy::complexity)]
    in_flight_requests: stream::FuturesUnordered<
        Pin<Box<Request<BoxFuture<'static, (String, RedisResult<Response>)>, Response, C>>>,
    >,
    refresh_error: Option<RedisError>,
    pending_requests: Vec<PendingRequest<Response, C>>,
    retries: Option<u32>,
    cluster_params: ClusterParams,
}

#[derive(Clone)]
enum CmdArg<C> {
    Cmd {
        cmd: Arc<Cmd>,
        func: fn(C, Arc<Cmd>) -> RedisFuture<'static, Response>,
    },
    Pipeline {
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        func: fn(C, Arc<crate::Pipeline>, usize, usize) -> RedisFuture<'static, Response>,
    },
}

impl<C> CmdArg<C> {
    fn exec(&self, con: C) -> RedisFuture<'static, Response> {
        match self {
            Self::Cmd { cmd, func } => func(con, cmd.clone()),
            Self::Pipeline {
                pipeline,
                offset,
                count,
                func,
            } => func(con, pipeline.clone(), *offset, *count),
        }
    }

    // TODO -- return offset for master/replica to support replica reads:
    fn slot(&self) -> Option<u16> {
        fn slot_for_command(cmd: &Cmd) -> Option<(u16, u16)> {
            match RoutingInfo::for_routable(cmd) {
                Some(RoutingInfo::Random) => None,
                Some(RoutingInfo::MasterSlot(slot)) => Some((slot, 0)),
                Some(RoutingInfo::ReplicaSlot(slot)) => Some((slot, 1)),
                Some(RoutingInfo::AllNodes) | Some(RoutingInfo::AllMasters) => None,
                _ => None,
            }
        }

        match self {
            Self::Cmd { ref cmd, .. } => slot_for_command(cmd).map(|x| x.0),
            Self::Pipeline { ref pipeline, .. } => {
                let mut iter = pipeline.cmd_iter();
                let slot = iter.next().map(slot_for_command)?;
                for cmd in iter {
                    if slot != slot_for_command(cmd) {
                        return None;
                    }
                }
                slot.map(|x| x.0)
            }
        }
    }
}

enum Response {
    Single(Value),
    Multiple(Vec<Value>),
}

struct Message<C> {
    cmd: CmdArg<C>,
    sender: oneshot::Sender<RedisResult<Response>>,
}

type RecoverFuture<C> =
    BoxFuture<'static, Result<(SlotMap, ConnectionMap<C>), (RedisError, ConnectionMap<C>)>>;

enum ConnectionState<C> {
    PollComplete,
    Recover(RecoverFuture<C>),
}

impl<C> fmt::Debug for ConnectionState<C> {
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

struct RequestInfo<C> {
    cmd: CmdArg<C>,
    slot: Option<u16>,
    excludes: HashSet<String>,
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
        max_retries: Option<u32>,
        request: Option<PendingRequest<I, C>>,
        #[pin]
        future: RequestState<F>,
    }
}

#[must_use]
enum Next<I, C> {
    TryNewConnection {
        request: PendingRequest<I, C>,
        error: Option<RedisError>,
    },
    Err {
        request: PendingRequest<I, C>,
        error: RedisError,
    },
    Done,
}

impl<F, I, C> Future for Request<F, I, C>
where
    F: Future<Output = (String, RedisResult<I>)>,
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
                return Next::TryNewConnection {
                    request: self.project().request.take().unwrap(),
                    error: None,
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
            (addr, Err(err)) => {
                trace!("Request error {}", err);

                let request = this.request.as_mut().unwrap();

                match *this.max_retries {
                    Some(max_retries) if request.retry >= max_retries => {
                        self.respond(Err(err));
                        return Next::Done.into();
                    }
                    _ => (),
                }
                request.retry = request.retry.saturating_add(1);

                if let Some(error_code) = err.code() {
                    if error_code == "MOVED" || error_code == "ASK" {
                        // Refresh slots and request again.
                        request.info.excludes.clear();
                        return Next::Err {
                            request: this.request.take().unwrap(),
                            error: err,
                        }
                        .into();
                    } else if error_code == "TRYAGAIN" || error_code == "CLUSTERDOWN" {
                        // Sleep and retry.
                        let sleep_duration =
                            Duration::from_millis(2u64.pow(request.retry.clamp(7, 16)) * 10);
                        request.info.excludes.clear();
                        this.future.set(RequestState::Sleep {
                            #[cfg(feature = "tokio-comp")]
                            sleep: Box::pin(tokio::time::sleep(sleep_duration)),

                            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
                            sleep: Box::pin(async_std::task::sleep(sleep_duration)),
                        });
                        return self.poll(cx);
                    }
                }

                request.info.excludes.insert(addr);

                Next::TryNewConnection {
                    request: this.request.take().unwrap(),
                    error: Some(err),
                }
                .into()
            }
        }
    }
}

impl<F, I, C> Request<F, I, C>
where
    F: Future<Output = (String, RedisResult<I>)>,
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

impl<C> Pipeline<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    async fn new(initial_nodes: &[ConnectionInfo], retries: Option<u32>) -> RedisResult<Self> {
        // This is mostly copied from ClusterClientBuilder
        // and is just a placeholder until ClusterClient
        // handles async connections
        let first_node = match initial_nodes.first() {
            Some(node) => node,
            None => {
                return Err(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "Initial nodes can't be empty.",
                )))
            }
        };

        let cluster_params = ClusterParams {
            password: first_node.redis.password.clone(),
            username: first_node.redis.username.clone(),
            tls: match first_node.addr {
                ConnectionAddr::TcpTls {
                    host: _,
                    port: _,
                    insecure,
                } => Some(match insecure {
                    false => TlsMode::Secure,
                    true => TlsMode::Insecure,
                }),
                _ => None,
            },
            ..Default::default()
        };

        let connections =
            Self::create_initial_connections(initial_nodes, cluster_params.clone()).await?;
        let mut connection = Pipeline {
            connections,
            slots: Default::default(),
            in_flight_requests: Default::default(),
            refresh_error: None,
            pending_requests: Vec::new(),
            state: ConnectionState::PollComplete,
            retries,
            cluster_params,
        };
        let (slots, connections) = connection.refresh_slots().await.map_err(|(err, _)| err)?;
        connection.slots = slots;
        connection.connections = connections;
        Ok(connection)
    }

    async fn create_initial_connections(
        initial_nodes: &[ConnectionInfo],
        params: ClusterParams,
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

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(
        &mut self,
    ) -> impl Future<Output = Result<(SlotMap, ConnectionMap<C>), (RedisError, ConnectionMap<C>)>>
    {
        let mut connections = mem::take(&mut self.connections);
        let params = self.cluster_params.clone();

        async move {
            let mut result = Ok(SlotMap::new());
            for (_, conn) in connections.iter_mut() {
                let mut conn = conn.clone().await;
                let value = match conn.req_packed_command(&slot_cmd()).await {
                    Ok(value) => value,
                    Err(err) => {
                        result = Err(err);
                        continue;
                    }
                };
                match parse_slots(value, params.tls).and_then(|v| Self::build_slot_map(v)) {
                    Ok(s) => {
                        result = Ok(s);
                        break;
                    }
                    Err(err) => result = Err(err),
                }
            }
            let slots = match result {
                Ok(slots) => slots,
                Err(err) => return Err((err, connections)),
            };

            // Remove dead connections and connect to new nodes if necessary
            let new_connections = HashMap::with_capacity(connections.len());

            let (_, connections) = stream::iter(slots.values())
                .fold(
                    (connections, new_connections),
                    move |(mut connections, mut new_connections), addr| {
                        let params = params.clone();
                        async move {
                            if !new_connections.contains_key(addr) {
                                let new_connection = if let Some(conn) = connections.remove(addr) {
                                    let mut conn = conn.await;
                                    match check_connection(&mut conn).await {
                                        Ok(_) => Some((addr.to_string(), conn)),
                                        Err(_) => match connect_and_check(addr, params).await {
                                            Ok(conn) => Some((addr.to_string(), conn)),
                                            Err(_) => None,
                                        },
                                    }
                                } else {
                                    match connect_and_check(addr, params).await {
                                        Ok(conn) => Some((addr.to_string(), conn)),
                                        Err(_) => None,
                                    }
                                };
                                if let Some((addr, new_connection)) = new_connection {
                                    new_connections
                                        .insert(addr, async { new_connection }.boxed().shared());
                                }
                            }
                            (connections, new_connections)
                        }
                    },
                )
                .await;
            Ok((slots, connections))
        }
    }

    fn build_slot_map(mut slots_data: Vec<Slot>) -> RedisResult<SlotMap> {
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
        let slot_map = slots_data
            .iter()
            .map(|slot_data| (slot_data.end(), slot_data.master().to_string()))
            .collect();
        trace!("{:?}", slot_map);
        Ok(slot_map)
    }

    fn get_connection(&mut self, slot: u16) -> (String, ConnectionFuture<C>) {
        if let Some((_, addr)) = self.slots.range(&slot..).next() {
            if let Some(conn) = self.connections.get(addr) {
                return (addr.clone(), conn.clone());
            }

            // Create new connection.
            //
            let (_, random_conn) = get_random_connection(&self.connections, None); // TODO Only do this lookup if the first check fails
            let connection_future = {
                let addr = addr.clone();
                let params = self.cluster_params.clone();
                async move {
                    match connect_and_check(&addr, params).await {
                        Ok(conn) => conn,
                        Err(_) => random_conn.await,
                    }
                }
            }
            .boxed()
            .shared();
            self.connections
                .insert(addr.clone(), connection_future.clone());
            (addr.clone(), connection_future)
        } else {
            // Return a random connection
            get_random_connection(&self.connections, None)
        }
    }

    fn try_request(
        &mut self,
        info: &RequestInfo<C>,
    ) -> impl Future<Output = (String, RedisResult<Response>)> {
        // TODO remove clone by changing the ConnectionLike trait
        let cmd = info.cmd.clone();
        let (addr, conn) = if !info.excludes.is_empty() || info.slot.is_none() {
            get_random_connection(&self.connections, Some(&info.excludes))
        } else {
            self.get_connection(info.slot.unwrap())
        };
        async move {
            let conn = conn.await;
            let result = cmd.exec(conn).await;
            (addr, result)
        }
    }

    fn poll_recover(
        &mut self,
        cx: &mut task::Context<'_>,
        mut future: RecoverFuture<C>,
    ) -> Poll<Result<(), RedisError>> {
        match future.as_mut().poll(cx) {
            Poll::Ready(Ok((slots, connections))) => {
                trace!("Recovered with {} connections!", connections.len());
                self.slots = slots;
                self.connections = connections;
                self.state = ConnectionState::PollComplete;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                self.state = ConnectionState::Recover(future);
                trace!("Recover not ready");
                Poll::Pending
            }
            Poll::Ready(Err((err, connections))) => {
                self.connections = connections;
                self.state = ConnectionState::Recover(Box::pin(self.refresh_slots()));
                Poll::Ready(Err(err))
            }
        }
    }

    fn poll_complete(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), RedisError>> {
        let mut connection_error = None;

        if !self.pending_requests.is_empty() {
            let mut pending_requests = mem::take(&mut self.pending_requests);
            for request in pending_requests.drain(..) {
                // Drop the request if noone is waiting for a response to free up resources for
                // requests callers care about (load shedding). It will be ambigous whether the
                // request actually goes through regardless.
                if request.sender.is_closed() {
                    continue;
                }

                let future = self.try_request(&request.info);
                self.in_flight_requests.push(Box::pin(Request {
                    max_retries: self.retries,
                    request: Some(request),
                    future: RequestState::Future {
                        future: future.boxed(),
                    },
                }));
            }
            self.pending_requests = pending_requests;
        }

        loop {
            let result = match Pin::new(&mut self.in_flight_requests).poll_next(cx) {
                Poll::Ready(Some(result)) => result,
                Poll::Ready(None) | Poll::Pending => break,
            };
            let self_ = &mut *self;
            match result {
                Next::Done => {}
                Next::TryNewConnection { request, error } => {
                    if let Some(error) = error {
                        if request.info.excludes.len() >= self_.connections.len() {
                            let _ = request.sender.send(Err(error));
                            continue;
                        }
                    }
                    let future = self.try_request(&request.info);
                    self.in_flight_requests.push(Box::pin(Request {
                        max_retries: self.retries,
                        request: Some(request),
                        future: RequestState::Future {
                            future: Box::pin(future),
                        },
                    }));
                }
                Next::Err { request, error } => {
                    connection_error = Some(error);
                    self.pending_requests.push(request);
                }
            }
        }

        if let Some(err) = connection_error {
            Poll::Ready(Err(err))
        } else if self.in_flight_requests.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
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
}

impl<C> Sink<Message<C>> for Pipeline<C>
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

        let excludes = HashSet::new();
        let slot = cmd.slot();

        let info = RequestInfo {
            cmd,
            slot,
            excludes,
        };

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
                    Ok(()) => return Poll::Ready(Ok(())),
                    Err(err) => {
                        trace!("Recovering {}", err);
                        self.state = ConnectionState::Recover(Box::pin(self.refresh_slots()));
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
            Poll::Ready(result) => {
                result.map_err(|_| ())?;
            }
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

impl<C> ConnectionLike for Connection<C>
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

impl Clone for Client {
    fn clone(&self) -> Client {
        Client::open(self.initial_nodes.clone()).unwrap()
    }
}

/// Implements the process of connecting to a redis server
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
    let info = get_connection_info(node, params)?;
    let mut conn = C::connect(info).await?;
    check_connection(&mut conn).await?;
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

fn get_random_connection<'a, C>(
    connections: &'a ConnectionMap<C>,
    excludes: Option<&'a HashSet<String>>,
) -> (String, ConnectionFuture<C>)
where
    C: Clone,
{
    debug_assert!(!connections.is_empty());

    let mut rng = thread_rng();
    let sample = match excludes {
        Some(excludes) if excludes.len() < connections.len() => {
            let target_keys = connections.keys().filter(|key| !excludes.contains(*key));
            target_keys.choose(&mut rng)
        }
        _ => connections.keys().choose(&mut rng),
    };

    let addr = sample.expect("No targets to choose from");
    (addr.to_string(), connections.get(addr).unwrap().clone())
}
