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

mod connections_container;
mod connections_logic;
/// Exposed only for testing.
pub mod testing {
    pub use super::connections_logic::*;
}
use std::{
    collections::HashMap,
    fmt, io, mem,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        atomic::{self, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{self, Poll},
};

use crate::{
    aio::{get_socket_addrs, ConnectionLike, MultiplexedConnection},
    cluster::slot_cmd,
    cluster_async::connections_logic::{
        get_host_and_port_from_addr, get_or_create_conn, ConnectionFuture, RefreshConnectionType,
    },
    cluster_client::{ClusterParams, RetryParams},
    cluster_routing::{
        self, MultipleNodeRoutingInfo, Redirect, ResponsePolicy, Route, SingleNodeRoutingInfo,
        SlotAddr,
    },
    cluster_topology::{
        calculate_topology, DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES,
        DEFAULT_REFRESH_SLOTS_RETRY_INITIAL_INTERVAL, DEFAULT_REFRESH_SLOTS_RETRY_TIMEOUT,
    },
    Cmd, ConnectionInfo, ErrorKind, IntoConnectionInfo, RedisError, RedisFuture, RedisResult,
    Value,
};
use std::time::Duration;

#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use crate::aio::{async_std::AsyncStd, RedisRuntime};
use arcstr::ArcStr;
#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use backoff_std_async::future::retry;
#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use backoff_std_async::{Error as BackoffError, ExponentialBackoff};

#[cfg(feature = "tokio-comp")]
use backoff_tokio::future::retry;
#[cfg(feature = "tokio-comp")]
use backoff_tokio::{Error as BackoffError, ExponentialBackoff};

use dispose::{Disposable, Dispose};
use futures::{future::BoxFuture, prelude::*, ready};
use futures_time::{future::FutureExt, task::sleep};
use pin_project_lite::pin_project;
use std::sync::atomic::AtomicBool;
use tokio::sync::{
    mpsc,
    oneshot::{self, Receiver},
    RwLock,
};
use tracing::{info, trace, warn};

use self::{
    connections_container::{
        ConnectionAndIdentifier, ConnectionType, ConnectionsMap, Identifier as ConnectionIdentifier,
    },
    connections_logic::connect_and_check,
};

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

    /// Send a command to the given `routing`. If `routing` is [None], it will be computed from `cmd`.
    pub async fn route_command(
        &mut self,
        cmd: &Cmd,
        routing: cluster_routing::RoutingInfo,
    ) -> RedisResult<Value> {
        trace!("route_command");
        let (sender, receiver) = oneshot::channel();
        self.0
            .send(Message {
                cmd: CmdArg::Cmd {
                    cmd: Arc::new(cmd.clone()),
                    routing: routing.into(),
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
    }

    /// Send commands in `pipeline` to the given `route`. If `route` is [None], it will be computed from `pipeline`.
    pub async fn route_pipeline<'a>(
        &'a mut self,
        pipeline: &'a crate::Pipeline,
        offset: usize,
        count: usize,
        route: SingleNodeRoutingInfo,
    ) -> RedisResult<Vec<Value>> {
        let (sender, receiver) = oneshot::channel();
        self.0
            .send(Message {
                cmd: CmdArg::Pipeline {
                    pipeline: Arc::new(pipeline.clone()),
                    offset,
                    count,
                    route: route.into(),
                },
                sender,
            })
            .await
            .map_err(|_| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))?;

        receiver
            .await
            .unwrap_or_else(|_| Err(RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe))))
            .map(|response| match response {
                Response::Multiple(values) => values,
                Response::Single(_) => unreachable!(),
            })
    }
}

type ConnectionMap<C> = connections_container::ConnectionsMap<ConnectionFuture<C>>;
type ConnectionsContainer<C> =
    self::connections_container::ConnectionsContainer<ConnectionFuture<C>>;

struct InnerCore<C> {
    conn_lock: RwLock<ConnectionsContainer<C>>,
    cluster_params: ClusterParams,
    pending_requests: Mutex<Vec<PendingRequest<C>>>,
    slot_refresh_in_progress: AtomicBool,
    initial_nodes: Vec<ConnectionInfo>,
}

type Core<C> = Arc<InnerCore<C>>;

struct ClusterConnInner<C> {
    inner: Core<C>,
    state: ConnectionState,
    #[allow(clippy::complexity)]
    in_flight_requests: stream::FuturesUnordered<Pin<Box<Request<C>>>>,
    refresh_error: Option<RedisError>,
    // A flag indicating the connection's closure and the requirement to shut down all related tasks.
    shutdown_flag: Arc<AtomicBool>,
}

impl<C> Dispose for ClusterConnInner<C> {
    fn dispose(self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }
}

#[derive(Clone)]
enum InternalRoutingInfo<C> {
    SingleNode(InternalSingleNodeRouting<C>),
    MultiNode((MultipleNodeRoutingInfo, Option<ResponsePolicy>)),
}

impl<C> From<cluster_routing::RoutingInfo> for InternalRoutingInfo<C> {
    fn from(value: cluster_routing::RoutingInfo) -> Self {
        match value {
            cluster_routing::RoutingInfo::SingleNode(route) => {
                InternalRoutingInfo::SingleNode(route.into())
            }
            cluster_routing::RoutingInfo::MultiNode(routes) => {
                InternalRoutingInfo::MultiNode(routes)
            }
        }
    }
}

impl<C> From<InternalSingleNodeRouting<C>> for InternalRoutingInfo<C> {
    fn from(value: InternalSingleNodeRouting<C>) -> Self {
        InternalRoutingInfo::SingleNode(value)
    }
}

#[derive(Clone)]
enum InternalSingleNodeRouting<C> {
    Random,
    SpecificNode(Route),
    ByAddress(String),
    Connection {
        identifier: ConnectionIdentifier,
        conn: ConnectionFuture<C>,
    },
    Redirect {
        redirect: Redirect,
        previous_routing: Box<InternalSingleNodeRouting<C>>,
    },
}

impl<C> Default for InternalSingleNodeRouting<C> {
    fn default() -> Self {
        Self::Random
    }
}

impl<C> From<SingleNodeRoutingInfo> for InternalSingleNodeRouting<C> {
    fn from(value: SingleNodeRoutingInfo) -> Self {
        match value {
            SingleNodeRoutingInfo::Random => InternalSingleNodeRouting::Random,
            SingleNodeRoutingInfo::SpecificNode(route) => {
                InternalSingleNodeRouting::SpecificNode(route)
            }
            SingleNodeRoutingInfo::ByAddress { host, port } => {
                InternalSingleNodeRouting::ByAddress(format!("{host}:{port}"))
            }
        }
    }
}

#[derive(Clone)]
enum CmdArg<C> {
    Cmd {
        cmd: Arc<Cmd>,
        routing: InternalRoutingInfo<C>,
    },
    Pipeline {
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        route: InternalSingleNodeRouting<C>,
    },
}

fn route_for_pipeline(pipeline: &crate::Pipeline) -> RedisResult<Option<Route>> {
    fn route_for_command(cmd: &Cmd) -> Option<Route> {
        match cluster_routing::RoutingInfo::for_routable(cmd) {
            Some(cluster_routing::RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random)) => None,
            Some(cluster_routing::RoutingInfo::SingleNode(
                SingleNodeRoutingInfo::SpecificNode(route),
            )) => Some(route),
            Some(cluster_routing::RoutingInfo::MultiNode(_)) => None,
            Some(cluster_routing::RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                ..
            })) => None,
            None => None,
        }
    }

    // Find first specific slot and send to it. There's no need to check If later commands
    // should be routed to a different slot, since the server will return an error indicating this.
    pipeline.cmd_iter().map(route_for_command).try_fold(
        None,
        |chosen_route, next_cmd_route| match (chosen_route, next_cmd_route) {
            (None, _) => Ok(next_cmd_route),
            (_, None) => Ok(chosen_route),
            (Some(chosen_route), Some(next_cmd_route)) => {
                if chosen_route.slot() != next_cmd_route.slot() {
                    Err((ErrorKind::CrossSlot, "Received crossed slots in pipeline").into())
                } else if chosen_route.slot_addr() == SlotAddr::ReplicaOptional {
                    Ok(Some(next_cmd_route))
                } else {
                    Ok(Some(chosen_route))
                }
            }
        },
    )
}

enum Response {
    Single(Value),
    Multiple(Vec<Value>),
}

enum OperationTarget {
    Node { identifier: ConnectionIdentifier },
    FanOut,
    NoTargetFound,
}
type OperationResult = Result<Response, (OperationTarget, RedisError)>;

impl From<ConnectionIdentifier> for OperationTarget {
    fn from(identifier: ConnectionIdentifier) -> Self {
        OperationTarget::Node { identifier }
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
}

impl<C> RequestInfo<C> {
    fn set_redirect(&mut self, redirect: Option<Redirect>) {
        if let Some(redirect) = redirect {
            match &mut self.cmd {
                CmdArg::Cmd { routing, .. } => match routing {
                    InternalRoutingInfo::SingleNode(route) => {
                        let redirect = InternalSingleNodeRouting::Redirect {
                            redirect,
                            previous_routing: Box::new(std::mem::take(route)),
                        }
                        .into();
                        *routing = redirect;
                    }
                    InternalRoutingInfo::MultiNode(_) => {
                        panic!("Cannot redirect multinode requests")
                    }
                },
                CmdArg::Pipeline { route, .. } => {
                    let redirect = InternalSingleNodeRouting::Redirect {
                        redirect,
                        previous_routing: Box::new(std::mem::take(route)),
                    };
                    *route = redirect;
                }
            }
        }
    }

    fn reset_redirect(&mut self) {
        match &mut self.cmd {
            CmdArg::Cmd { routing, .. } => {
                if let InternalRoutingInfo::SingleNode(InternalSingleNodeRouting::Redirect {
                    previous_routing,
                    ..
                }) = routing
                {
                    let previous_routing = std::mem::take(previous_routing.as_mut());
                    *routing = previous_routing.into();
                }
            }
            CmdArg::Pipeline { route, .. } => {
                if let InternalSingleNodeRouting::Redirect {
                    previous_routing, ..
                } = route
                {
                    let previous_routing = std::mem::take(previous_routing.as_mut());
                    *route = previous_routing;
                }
            }
        }
    }
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

struct PendingRequest<C> {
    retry: u32,
    sender: oneshot::Sender<RedisResult<Response>>,
    info: RequestInfo<C>,
}

pin_project! {
    struct Request<C> {
        retry_params: RetryParams,
        request: Option<PendingRequest<C>>,
        #[pin]
        future: RequestState<BoxFuture<'static, OperationResult>>,
    }
}

#[must_use]
enum Next<C> {
    Retry {
        request: PendingRequest<C>,
    },
    RetryBusyLoadingError {
        request: PendingRequest<C>,
        identifier: ConnectionIdentifier,
    },
    Reconnect {
        request: PendingRequest<C>,
        target: ConnectionIdentifier,
    },
    RefreshSlots {
        request: PendingRequest<C>,
    },
    ReconnectToInitialNodes {
        request: PendingRequest<C>,
    },
    Done,
}

impl<C> Future for Request<C> {
    type Output = Next<C>;

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
            Ok(item) => {
                self.respond(Ok(item));
                Next::Done.into()
            }
            Err((target, err)) => {
                let request = this.request.as_mut().unwrap();

                if request.retry >= this.retry_params.number_of_retries {
                    self.respond(Err(err));
                    return Next::Done.into();
                }
                request.retry = request.retry.saturating_add(1);

                if err.kind() == ErrorKind::ConnectionNotFound {
                    return Next::ReconnectToInitialNodes {
                        request: this.request.take().unwrap(),
                    }
                    .into();
                }

                let identifier = match target {
                    OperationTarget::Node { identifier } => identifier,
                    OperationTarget::FanOut => {
                        trace!("Request error `{}` multi-node request", err);

                        // Fanout operation are retried per internal request, and don't need additional retries.
                        self.respond(Err(err));
                        return Next::Done.into();
                    }
                    OperationTarget::NoTargetFound => {
                        warn!("No connection found: `{err}`");
                        let mut request = this.request.take().unwrap();
                        request.info.reset_redirect();
                        return Next::RefreshSlots { request }.into();
                    }
                };
                trace!("Request error `{}` on node `{:?}", err, identifier);

                match err.retry_method() {
                    crate::types::RetryMethod::AskRedirect => {
                        let mut request = this.request.take().unwrap();
                        request.info.set_redirect(
                            err.redirect_node()
                                .map(|(node, _slot)| Redirect::Ask(node.to_string())),
                        );
                        Next::Retry { request }.into()
                    }
                    crate::types::RetryMethod::MovedRedirect => {
                        let mut request = this.request.take().unwrap();
                        request.info.set_redirect(
                            err.redirect_node()
                                .map(|(node, _slot)| Redirect::Moved(node.to_string())),
                        );
                        Next::RefreshSlots { request }.into()
                    }
                    crate::types::RetryMethod::WaitAndRetry => {
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
                    crate::types::RetryMethod::Reconnect => {
                        let mut request = this.request.take().unwrap();
                        // TODO should we reset the redirect here?
                        request.info.reset_redirect();
                        warn!("disconnected from {:?}", identifier);
                        Next::Reconnect {
                            request,
                            target: identifier,
                        }
                        .into()
                    }
                    crate::types::RetryMethod::WaitAndRetryOnPrimaryRedirectOnReplica => {
                        Next::RetryBusyLoadingError {
                            request: this.request.take().unwrap(),
                            identifier,
                        }
                        .into()
                    }
                    crate::types::RetryMethod::RetryImmediately => Next::Retry {
                        request: this.request.take().unwrap(),
                    }
                    .into(),
                    crate::types::RetryMethod::NoRetry => {
                        self.respond(Err(err));
                        Next::Done.into()
                    }
                }
            }
        }
    }
}

impl<C> Request<C> {
    fn respond(self: Pin<&mut Self>, msg: RedisResult<Response>) {
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

enum ConnectionCheck<C> {
    Found((ConnectionIdentifier, ConnectionFuture<C>)),
    OnlyAddress(String),
    RandomConnection,
}

impl<C> ClusterConnInner<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    async fn new(
        initial_nodes: &[ConnectionInfo],
        cluster_params: ClusterParams,
    ) -> RedisResult<Disposable<Self>> {
        let connections = Self::create_initial_connections(initial_nodes, &cluster_params).await?;
        let topology_checks_interval = cluster_params.topology_checks_interval;
        let inner = Arc::new(InnerCore {
            conn_lock: RwLock::new(ConnectionsContainer::new(
                Default::default(),
                connections,
                cluster_params.read_from_replicas,
                0,
            )),
            cluster_params,
            pending_requests: Mutex::new(Vec::new()),
            slot_refresh_in_progress: AtomicBool::new(false),
            initial_nodes: initial_nodes.to_vec(),
        });
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let connection = ClusterConnInner {
            inner,
            in_flight_requests: Default::default(),
            refresh_error: None,
            state: ConnectionState::PollComplete,
            shutdown_flag: shutdown_flag.clone(),
        };
        Self::refresh_slots_with_retries(connection.inner.clone()).await?;
        if let Some(duration) = topology_checks_interval {
            let periodic_task = ClusterConnInner::periodic_topology_check(
                connection.inner.clone(),
                duration,
                shutdown_flag,
            );
            #[cfg(feature = "tokio-comp")]
            tokio::spawn(periodic_task);
            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
            AsyncStd::spawn(periodic_task);
        }

        Ok(Disposable::new(connection))
    }

    /// Go through each of the initial nodes and attempt to retrieve all IP entries from them.
    /// If there's a DNS endpoint that directs to several IP addresses, add all addresses to the initial nodes list.
    /// Returns a vector of tuples, each containing a node's address (including the hostname) and its corresponding SocketAddr if retrieved.
    pub(crate) async fn try_to_expand_initial_nodes(
        initial_nodes: &[ConnectionInfo],
    ) -> Vec<(String, Option<SocketAddr>)> {
        stream::iter(initial_nodes)
            .fold(
                Vec::with_capacity(initial_nodes.len()),
                |mut acc, info| async {
                    let (host, port) = match &info.addr {
                        crate::ConnectionAddr::Tcp(host, port) => (host, port),
                        crate::ConnectionAddr::TcpTls {
                            host,
                            port,
                            insecure: _,
                            tls_params: _,
                        } => (host, port),
                        crate::ConnectionAddr::Unix(_) => {
                            // We don't support multiple addresses for a Unix address. Store the initial node address and continue
                            acc.push((info.addr.to_string(), None));
                            return acc;
                        }
                    };
                    match get_socket_addrs(host, *port).await {
                        Ok(socket_addrs) => {
                            for addr in socket_addrs {
                                acc.push((info.addr.to_string(), Some(addr)));
                            }
                        }
                        Err(_) => {
                            // Couldn't find socket addresses, store the initial node address and continue
                            acc.push((info.addr.to_string(), None));
                        }
                    };
                    acc
                },
            )
            .await
    }

    async fn create_initial_connections(
        initial_nodes: &[ConnectionInfo],
        params: &ClusterParams,
    ) -> RedisResult<ConnectionMap<C>> {
        let initial_nodes: Vec<(String, Option<SocketAddr>)> =
            Self::try_to_expand_initial_nodes(initial_nodes).await;
        let connections = stream::iter(initial_nodes.iter().cloned())
            .map(|(node_addr, socket_addr)| {
                let params: ClusterParams = params.clone();
                async move {
                    let result = connect_and_check(
                        &node_addr,
                        params,
                        socket_addr,
                        RefreshConnectionType::AllConnections,
                        None,
                    )
                    .await
                    .get_node();
                    let node_identifier = if let Some(socket_addr) = socket_addr {
                        socket_addr.to_string()
                    } else {
                        node_addr
                    };
                    result.map(|node| (node_identifier, node))
                }
            })
            .buffer_unordered(initial_nodes.len())
            .fold(
                (
                    ConnectionsMap(HashMap::with_capacity(initial_nodes.len())),
                    None,
                ),
                |mut connections: (ConnectionMap<C>, Option<String>), addr_conn_res| async move {
                    match addr_conn_res {
                        Ok((addr, node)) => {
                            connections.0 .0.insert(addr.into(), node);
                            (connections.0, None)
                        }
                        Err(e) => (connections.0, Some(e.to_string())),
                    }
                },
            )
            .await;
        if connections.0 .0.is_empty() {
            return Err(RedisError::from((
                ErrorKind::IoError,
                "Failed to create initial connections",
                connections.1.unwrap_or("".to_string()),
            )));
        }
        info!("Connected to initial nodes:\n{}", connections.0);
        Ok(connections.0)
    }

    fn reconnect_to_initial_nodes(&mut self) -> impl Future<Output = ()> {
        let inner = self.inner.clone();
        async move {
            let connection_map =
                match Self::create_initial_connections(&inner.initial_nodes, &inner.cluster_params)
                    .await
                {
                    Ok(map) => map,
                    Err(err) => {
                        warn!("Can't reconnect to initial nodes: `{err}`");
                        return;
                    }
                };
            let mut write_lock = inner.conn_lock.write().await;
            *write_lock = ConnectionsContainer::new(
                Default::default(),
                connection_map,
                inner.cluster_params.read_from_replicas,
                0,
            );
            drop(write_lock);
            if let Err(err) = Self::refresh_slots_with_retries(inner.clone()).await {
                warn!("Can't refresh slots with initial nodes: `{err}`");
            };
        }
    }

    async fn refresh_connections(
        inner: Arc<InnerCore<C>>,
        identifiers: Vec<ConnectionIdentifier>,
        conn_type: RefreshConnectionType,
    ) {
        info!("Started refreshing connections to {:?}", identifiers);
        let mut connections_container = inner.conn_lock.write().await;
        let cluster_params = &inner.cluster_params;
        stream::iter(identifiers.into_iter())
            .fold(
                &mut *connections_container,
                |connections_container, identifier| async move {
                    let addr_option = connections_container.address_for_identifier(&identifier);
                    let node_option = connections_container.remove_node(&identifier);
                    if let Some(addr) = addr_option {
                        let node =
                            get_or_create_conn(&addr, node_option, cluster_params, conn_type).await;
                        if let Ok(node) = node {
                            connections_container.replace_or_add_connection_for_address(addr, node);
                        }
                    }
                    connections_container
                },
            )
            .await;
        info!("refresh connections completed");
    }

    async fn aggregate_results(
        receivers: Vec<(Option<ArcStr>, oneshot::Receiver<RedisResult<Response>>)>,
        routing: &MultipleNodeRoutingInfo,
        response_policy: Option<ResponsePolicy>,
    ) -> RedisResult<Value> {
        let extract_result = |response| match response {
            Response::Single(value) => value,
            Response::Multiple(_) => unreachable!(),
        };

        let convert_result = |res: Result<RedisResult<Response>, _>| {
            res.map_err(|_| RedisError::from((ErrorKind::ResponseError, "request wasn't handled due to internal failure"))) // this happens only if the result sender is dropped before usage.
            .and_then(|res| res.map(extract_result))
        };

        let get_receiver = |(_, receiver): (_, oneshot::Receiver<RedisResult<Response>>)| async {
            convert_result(receiver.await)
        };

        // TODO - once Value::Error will be merged, these will need to be updated to handle this new value.
        match response_policy {
            Some(ResponsePolicy::AllSucceeded) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .map(|mut results| results.pop().unwrap()) // unwrap is safe, since at least one function succeeded
            }
            Some(ResponsePolicy::OneSucceeded) => future::select_ok(
                receivers
                    .into_iter()
                    .map(|tuple| Box::pin(get_receiver(tuple))),
            )
            .await
            .map(|(result, _)| result),
            Some(ResponsePolicy::OneSucceededNonEmpty) => {
                future::select_ok(receivers.into_iter().map(|(_, receiver)| {
                    Box::pin(async move {
                        let result = convert_result(receiver.await)?;
                        match result {
                            Value::Nil => Err((ErrorKind::ResponseError, "no value found").into()),
                            _ => Ok(result),
                        }
                    })
                }))
                .await
                .map(|(result, _)| result)
            }
            Some(ResponsePolicy::Aggregate(op)) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .and_then(|results| crate::cluster_routing::aggregate(results, op))
            }
            Some(ResponsePolicy::AggregateLogical(op)) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .and_then(|results| crate::cluster_routing::logical_aggregate(results, op))
            }
            Some(ResponsePolicy::CombineArrays) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .and_then(|results| match routing {
                        MultipleNodeRoutingInfo::MultiSlot(vec) => {
                            crate::cluster_routing::combine_and_sort_array_results(
                                results,
                                vec.iter().map(|(_, indices)| indices),
                            )
                        }
                        _ => crate::cluster_routing::combine_array_results(results),
                    })
            }
            Some(ResponsePolicy::Special) | None => {
                // This is our assumption - if there's no coherent way to aggregate the responses, we just map each response to the sender, and pass it to the user.
                // TODO - once Value::Error is merged, we can use join_all and report separate errors and also pass successes.
                future::try_join_all(receivers.into_iter().map(|(addr, receiver)| async move {
                    let result = convert_result(receiver.await)?;
                    // The unwrap here is possible, because if `addr` is None, an error should have been sent on the receiver.
                    Ok((Value::BulkString(addr.unwrap().as_bytes().to_vec()), result))
                }))
                .await
                .map(Value::Map)
            }
        }
    }

    // Query a node to discover slot-> master mappings with retries
    async fn refresh_slots_with_retries(inner: Arc<InnerCore<C>>) -> RedisResult<()> {
        if inner
            .slot_refresh_in_progress
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }
        let retry_strategy = ExponentialBackoff {
            initial_interval: DEFAULT_REFRESH_SLOTS_RETRY_INITIAL_INTERVAL,
            max_interval: DEFAULT_REFRESH_SLOTS_RETRY_TIMEOUT,
            max_elapsed_time: None,
            ..Default::default()
        };
        let retries_counter = AtomicUsize::new(0);
        let res = retry(retry_strategy, || {
            let curr_retry = retries_counter.fetch_add(1, atomic::Ordering::Relaxed);
            Self::refresh_slots(inner.clone(), curr_retry)
        })
        .await;
        inner
            .slot_refresh_in_progress
            .store(false, Ordering::Relaxed);
        res
    }

    async fn periodic_topology_check(
        inner: Arc<InnerCore<C>>,
        interval_duration: Duration,
        shutdown_flag: Arc<AtomicBool>,
    ) {
        loop {
            if shutdown_flag.load(Ordering::Relaxed) {
                return;
            }
            let _ = sleep(interval_duration.into()).await;

            let retry_strategy = ExponentialBackoff {
                initial_interval: DEFAULT_REFRESH_SLOTS_RETRY_INITIAL_INTERVAL,
                max_interval: DEFAULT_REFRESH_SLOTS_RETRY_TIMEOUT,
                ..Default::default()
            };
            let topology_check_res = retry(retry_strategy, || {
                Self::check_for_topology_diff(inner.clone()).map_err(BackoffError::from)
            })
            .await;
            if let Ok(true) = topology_check_res {
                let _ = Self::refresh_slots_with_retries(inner.clone()).await;
            };
        }
    }

    /// Queries log2n nodes (where n represents the number of cluster nodes) to determine whether their
    /// topology view differs from the one currently stored in the connection manager.
    /// Returns true if change was detected, otherwise false.
    async fn check_for_topology_diff(inner: Arc<InnerCore<C>>) -> RedisResult<bool> {
        let read_guard = inner.conn_lock.read().await;
        let num_of_nodes: usize = read_guard.len();
        // TODO: Starting from Rust V1.67, integers has logarithms support.
        // When we no longer need to support Rust versions < 1.67, remove fast_math and transition to the ilog2 function.
        let num_of_nodes_to_query =
            std::cmp::max(fast_math::log2_raw(num_of_nodes as f32) as usize, 1);
        let (_, found_topology_hash) = calculate_topology_from_random_nodes(
            &inner,
            num_of_nodes_to_query,
            &read_guard,
            DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES,
        )
        .await?;
        let change_found = read_guard.get_current_topology_hash() != found_topology_hash;
        Ok(change_found)
    }

    async fn refresh_slots(
        inner: Arc<InnerCore<C>>,
        curr_retry: usize,
    ) -> Result<(), BackoffError<RedisError>> {
        Self::refresh_slots_inner(inner, curr_retry)
            .await
            .map_err(|err| {
                if curr_retry > DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES {
                    BackoffError::Permanent(err)
                } else {
                    BackoffError::from(err)
                }
            })
    }

    // Query a node to discover slot-> master mappings
    async fn refresh_slots_inner(inner: Arc<InnerCore<C>>, curr_retry: usize) -> RedisResult<()> {
        let read_guard = inner.conn_lock.read().await;
        let num_of_nodes = read_guard.len();
        const MAX_REQUESTED_NODES: usize = 50;
        let num_of_nodes_to_query = std::cmp::min(num_of_nodes, MAX_REQUESTED_NODES);
        let (new_slots, topology_hash) = calculate_topology_from_random_nodes(
            &inner,
            num_of_nodes_to_query,
            &read_guard,
            curr_retry,
        )
        .await?;
        info!("Found slot map: {new_slots}");
        let connections = &*read_guard;
        // Create a new connection vector of the found nodes
        let mut nodes = new_slots.values().flatten().collect::<Vec<_>>();
        nodes.sort_unstable();
        nodes.dedup();
        let nodes_len = nodes.len();
        let addresses_and_connections_iter = stream::iter(nodes)
            .fold(
                Vec::with_capacity(nodes_len),
                |mut addrs_and_conns, addr| async move {
                    if let Some(node) = connections.node_for_address(addr.as_str()) {
                        addrs_and_conns.push((addr, Some(node)));
                        return addrs_and_conns;
                    }
                    // If it's a DNS endpoint, it could have been stored in the existing connections vector using the resolved IP address instead of the DNS endpoint's name.
                    // We shall check if a connection is already exists under the resolved IP name.
                    let (host, port) = match get_host_and_port_from_addr(addr) {
                        Some((host, port)) => (host, port),
                        None => {
                            addrs_and_conns.push((addr, None));
                            return addrs_and_conns;
                        }
                    };
                    let conn = get_socket_addrs(host, port)
                        .await
                        .ok()
                        .map(|mut socket_addresses| {
                            socket_addresses
                                .find_map(|addr| connections.node_for_address(&addr.to_string()))
                        })
                        .unwrap_or(None);
                    addrs_and_conns.push((addr, conn));
                    addrs_and_conns
                },
            )
            .await;
        let new_connections: ConnectionMap<C> = stream::iter(addresses_and_connections_iter)
            .fold(
                ConnectionsMap(HashMap::with_capacity(nodes_len)),
                |mut connections, (addr, node)| async {
                    let node = get_or_create_conn(
                        addr,
                        node,
                        &inner.cluster_params,
                        RefreshConnectionType::AllConnections,
                    )
                    .await;
                    if let Ok(node) = node {
                        connections.0.insert(addr.into(), node);
                    }
                    connections
                },
            )
            .await;

        drop(read_guard);
        info!("refresh_slots found nodes:\n{new_connections}");
        // Replace the current slot map and connection vector with the new ones
        let mut write_guard = inner.conn_lock.write().await;
        *write_guard = ConnectionsContainer::new(
            new_slots,
            new_connections,
            inner.cluster_params.read_from_replicas,
            topology_hash,
        );
        Ok(())
    }

    async fn execute_on_multiple_nodes<'a>(
        cmd: &'a Arc<Cmd>,
        routing: &'a MultipleNodeRoutingInfo,
        core: Core<C>,
        response_policy: Option<ResponsePolicy>,
    ) -> OperationResult {
        trace!("execute_on_multiple_nodes");
        let connections_container = core.conn_lock.read().await;
        if connections_container.is_empty() {
            return OperationResult::Err((
                OperationTarget::FanOut,
                (
                    ErrorKind::ConnectionNotFound,
                    "No connections found for multi-node operation",
                )
                    .into(),
            ));
        }

        // This function maps the connections to senders & receivers of one-shot channels, and the receivers are mapped to `PendingRequest`s.
        // This allows us to pass the new `PendingRequest`s to `try_request`, while letting `execute_on_multiple_nodes` wait on the receivers
        // for all of the individual requests to complete.
        #[allow(clippy::type_complexity)] // The return value is complex, but indentation and linebreaks make it human readable.
        fn into_channels<C>(
            iterator: impl Iterator<
                Item = Option<(Arc<Cmd>, ConnectionAndIdentifier<ConnectionFuture<C>>)>,
            >,
            connections_container: &ConnectionsContainer<C>,
        ) -> (
            Vec<(Option<ArcStr>, Receiver<Result<Response, RedisError>>)>,
            Vec<Option<PendingRequest<C>>>,
        ) {
            iterator
                .map(|tuple_opt| {
                    let (sender, receiver) = oneshot::channel();
                    if let Some((cmd, identifier, conn, address)) =
                        tuple_opt.and_then(|(cmd, (identifier, conn))| {
                            // TODO - this lookup can be avoided, since address is only needed on Special aggregates.
                            connections_container
                                .address_for_identifier(&identifier)
                                .map(|address| (cmd, identifier, conn, address))
                        })
                    {
                        (
                            (Some(address), receiver),
                            Some(PendingRequest {
                                retry: 0,
                                sender,
                                info: RequestInfo {
                                    cmd: CmdArg::Cmd {
                                        cmd,
                                        routing: InternalSingleNodeRouting::Connection {
                                            identifier,
                                            conn,
                                        }
                                        .into(),
                                    },
                                },
                            }),
                        )
                    } else {
                        let _ = sender.send(Err((
                            ErrorKind::ConnectionNotFound,
                            "Connection not found",
                        )
                            .into()));
                        ((None, receiver), None)
                    }
                })
                .unzip()
        }

        let (receivers, requests): (Vec<_>, Vec<_>) = match routing {
            MultipleNodeRoutingInfo::AllNodes => into_channels(
                connections_container
                    .all_node_connections()
                    .map(|tuple| Some((cmd.clone(), tuple))),
                &connections_container,
            ),
            MultipleNodeRoutingInfo::AllMasters => into_channels(
                connections_container
                    .all_primary_connections()
                    .map(|tuple| Some((cmd.clone(), tuple))),
                &connections_container,
            ),
            MultipleNodeRoutingInfo::MultiSlot(slots) => into_channels(
                slots.iter().map(|(route, indices)| {
                    connections_container
                        .connection_for_route(route)
                        .map(|tuple| {
                            let new_cmd = crate::cluster_routing::command_for_multi_slot_indices(
                                cmd.as_ref(),
                                indices.iter(),
                            );
                            (Arc::new(new_cmd), tuple)
                        })
                }),
                &connections_container,
            ),
        };

        drop(connections_container);
        core.pending_requests
            .lock()
            .unwrap()
            .extend(requests.into_iter().flatten());

        Self::aggregate_results(receivers, routing, response_policy)
            .await
            .map(Response::Single)
            .map_err(|err| (OperationTarget::FanOut, err))
    }

    async fn try_cmd_request(
        cmd: Arc<Cmd>,
        routing: InternalRoutingInfo<C>,
        core: Core<C>,
    ) -> OperationResult {
        let routing = match routing {
            // commands that are sent to multiple nodes are handled here.
            InternalRoutingInfo::MultiNode((multi_node_routing, response_policy)) => {
                return Self::execute_on_multiple_nodes(
                    &cmd,
                    &multi_node_routing,
                    core,
                    response_policy,
                )
                .await;
            }

            InternalRoutingInfo::SingleNode(routing) => routing,
        };
        trace!("route request to single node");

        // if we reached this point, we're sending the command only to single node, and we need to find the
        // right connection to the node.
        let (identifier, mut conn) = Self::get_connection(routing, core)
            .await
            .map_err(|err| (OperationTarget::NoTargetFound, err))?;
        conn.req_packed_command(&cmd)
            .await
            .map(Response::Single)
            .map_err(|err| (identifier.into(), err))
    }

    async fn try_pipeline_request(
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        conn: impl Future<Output = RedisResult<(ConnectionIdentifier, C)>>,
    ) -> OperationResult {
        trace!("try_pipeline_request");
        let (identifier, mut conn) = conn
            .await
            .map_err(|err| (OperationTarget::NoTargetFound, err))?;
        conn.req_packed_commands(&pipeline, offset, count)
            .await
            .map(Response::Multiple)
            .map_err(|err| (OperationTarget::Node { identifier }, err))
    }

    async fn try_request(info: RequestInfo<C>, core: Core<C>) -> OperationResult {
        match info.cmd {
            CmdArg::Cmd { cmd, routing } => Self::try_cmd_request(cmd, routing, core).await,
            CmdArg::Pipeline {
                pipeline,
                offset,
                count,
                route,
            } => {
                Self::try_pipeline_request(
                    pipeline,
                    offset,
                    count,
                    Self::get_connection(route, core),
                )
                .await
            }
        }
    }

    async fn get_connection(
        routing: InternalSingleNodeRouting<C>,
        core: Core<C>,
    ) -> RedisResult<(ConnectionIdentifier, C)> {
        let read_guard = core.conn_lock.read().await;
        let mut asking = false;

        let conn_check = match routing {
            InternalSingleNodeRouting::Redirect {
                redirect: Redirect::Moved(moved_addr),
                ..
            } => read_guard
                .connection_for_address(moved_addr.as_str())
                .map_or(
                    ConnectionCheck::OnlyAddress(moved_addr),
                    ConnectionCheck::Found,
                ),
            InternalSingleNodeRouting::Redirect {
                redirect: Redirect::Ask(ask_addr),
                ..
            } => {
                asking = true;
                read_guard.connection_for_address(ask_addr.as_str()).map_or(
                    ConnectionCheck::OnlyAddress(ask_addr),
                    ConnectionCheck::Found,
                )
            }
            // This means that a request routed to a route without a matching connection will be sent to a random node, hopefully to be redirected afterwards.
            InternalSingleNodeRouting::SpecificNode(route) => {
                read_guard.connection_for_route(&route).map_or_else(
                    || {
                        warn!("No connection found for route `{route:?}");
                        ConnectionCheck::RandomConnection
                    },
                    ConnectionCheck::Found,
                )
            }
            InternalSingleNodeRouting::Random => ConnectionCheck::RandomConnection,
            InternalSingleNodeRouting::Connection { identifier, conn } => {
                return Ok((identifier, conn.await));
            }
            InternalSingleNodeRouting::ByAddress(address) => {
                if let Some((identifier, conn)) = read_guard.connection_for_address(&address) {
                    return Ok((identifier, conn.await));
                } else {
                    return Err((
                        ErrorKind::ConnectionNotFound,
                        "Requested connection not found",
                        address,
                    )
                        .into());
                }
            }
        };
        drop(read_guard);

        let (identifier, mut conn) = match conn_check {
            ConnectionCheck::Found((identifier, connection)) => (identifier, connection.await),
            ConnectionCheck::OnlyAddress(addr) => {
                match connect_and_check::<C>(
                    &addr,
                    core.cluster_params.clone(),
                    None,
                    RefreshConnectionType::AllConnections,
                    None,
                )
                .await
                .get_node()
                {
                    Ok(node) => {
                        let connection_clone = node.user_connection.clone().await;
                        let mut connections = core.conn_lock.write().await;
                        let identifier =
                            connections.replace_or_add_connection_for_address(addr, node);
                        drop(connections);
                        (identifier, connection_clone)
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
            ConnectionCheck::RandomConnection => {
                let read_guard = core.conn_lock.read().await;
                let (random_identifier, random_conn_future) = read_guard
                    .random_connections(1, ConnectionType::User)
                    .next()
                    .ok_or(RedisError::from((
                        ErrorKind::ConnectionNotFound,
                        "No random connection found",
                    )))?;
                return Ok((random_identifier, random_conn_future.await));
            }
        };

        if asking {
            let _ = conn.req_packed_command(&crate::cmd::cmd("ASKING")).await;
        }
        Ok((identifier, conn))
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
                        Self::refresh_slots_with_retries(self.inner.clone()),
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

    async fn handle_loading_error(
        core: Core<C>,
        info: RequestInfo<C>,
        identifier: ConnectionIdentifier,
        retry: u32,
    ) -> OperationResult {
        let is_primary = core.conn_lock.read().await.is_primary(&identifier);

        if !is_primary {
            // If the connection is a replica, remove the connection and retry.
            // The connection will be established again on the next call to refresh slots once the replica is no longer in loading state.
            core.conn_lock.write().await.remove_node(&identifier);
        } else {
            // If the connection is primary, just sleep and retry
            let sleep_duration = core.cluster_params.retry_params.wait_time_for_retry(retry);
            sleep(sleep_duration.into()).await;
        }

        Self::try_request(info, core).await
    }

    fn poll_complete(&mut self, cx: &mut task::Context<'_>) -> Poll<PollFlushAction> {
        let mut poll_flush_action = PollFlushAction::None;

        let mut pending_requests_guard = self.inner.pending_requests.lock().unwrap();
        if !pending_requests_guard.is_empty() {
            let mut pending_requests = mem::take(&mut *pending_requests_guard);
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
            *pending_requests_guard = pending_requests;
        }
        drop(pending_requests_guard);

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
                Next::RetryBusyLoadingError {
                    request,
                    identifier,
                } => {
                    // TODO - do we also want to try and reconnect to replica if it is loading?
                    let future = Self::handle_loading_error(
                        self.inner.clone(),
                        request.info.clone(),
                        identifier,
                        request.retry,
                    );
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
                    poll_flush_action =
                        poll_flush_action.change_state(PollFlushAction::Reconnect(vec![target]));
                    self.inner.pending_requests.lock().unwrap().push(request);
                }
                Next::ReconnectToInitialNodes { request } => {
                    poll_flush_action = poll_flush_action
                        .change_state(PollFlushAction::ReconnectFromInitialConnections);
                    self.inner.pending_requests.lock().unwrap().push(request);
                }
            }
        }

        if matches!(poll_flush_action, PollFlushAction::None) {
            if self.in_flight_requests.is_empty() {
                Poll::Ready(poll_flush_action)
            } else {
                Poll::Pending
            }
        } else {
            Poll::Ready(poll_flush_action)
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
            } else if let Some(request) = self.inner.pending_requests.lock().unwrap().pop() {
                let _ = request.sender.send(Err(self.refresh_error.take().unwrap()));
            }
        }
    }
}

enum PollFlushAction {
    None,
    RebuildSlots,
    Reconnect(Vec<ConnectionIdentifier>),
    ReconnectFromInitialConnections,
}

impl PollFlushAction {
    fn change_state(self, next_state: PollFlushAction) -> PollFlushAction {
        match (self, next_state) {
            (PollFlushAction::None, next_state) => next_state,
            (next_state, PollFlushAction::None) => next_state,
            (PollFlushAction::ReconnectFromInitialConnections, _)
            | (_, PollFlushAction::ReconnectFromInitialConnections) => {
                PollFlushAction::ReconnectFromInitialConnections
            }

            (PollFlushAction::RebuildSlots, _) | (_, PollFlushAction::RebuildSlots) => {
                PollFlushAction::RebuildSlots
            }

            (PollFlushAction::Reconnect(mut addrs), PollFlushAction::Reconnect(new_addrs)) => {
                addrs.extend(new_addrs);
                Self::Reconnect(addrs)
            }
        }
    }
}

impl<C> Sink<Message<C>> for Disposable<ClusterConnInner<C>>
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

    fn start_send(self: Pin<&mut Self>, msg: Message<C>) -> Result<(), Self::Error> {
        let Message { cmd, sender } = msg;

        let info = RequestInfo { cmd };

        self.inner
            .pending_requests
            .lock()
            .unwrap()
            .push(PendingRequest {
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
                        self.state =
                            ConnectionState::Recover(RecoverFuture::RecoverSlots(Box::pin(
                                ClusterConnInner::refresh_slots_with_retries(self.inner.clone()),
                            )));
                    }
                    PollFlushAction::Reconnect(identifiers) => {
                        self.state = ConnectionState::Recover(RecoverFuture::Reconnect(Box::pin(
                            ClusterConnInner::refresh_connections(
                                self.inner.clone(),
                                identifiers,
                                RefreshConnectionType::OnlyUserConnection,
                            ),
                        )));
                    }
                    PollFlushAction::ReconnectFromInitialConnections => {
                        self.state = ConnectionState::Recover(RecoverFuture::Reconnect(Box::pin(
                            self.reconnect_to_initial_nodes(),
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

async fn calculate_topology_from_random_nodes<'a, C>(
    inner: &Core<C>,
    num_of_nodes_to_query: usize,
    read_guard: &tokio::sync::RwLockReadGuard<'a, ConnectionsContainer<C>>,
    curr_retry: usize,
) -> RedisResult<(
    crate::cluster_slotmap::SlotMap,
    crate::cluster_topology::TopologyHash,
)>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    let requested_nodes = read_guard
        .random_connections(num_of_nodes_to_query, ConnectionType::PreferManagement)
        .filter_map(|(identifier, conn)| {
            read_guard
                .address_for_identifier(&identifier)
                .map(|addr| (addr, conn))
        });
    let topology_join_results =
        futures::future::join_all(requested_nodes.map(|(host, conn)| async move {
            let mut conn: C = conn.await;
            conn.req_packed_command(&slot_cmd())
                .await
                .map(|res| (host, res))
        }))
        .await;
    let topology_values = topology_join_results.iter().filter_map(|r| {
        r.as_ref().ok().and_then(|(addr, value)| {
            get_host_and_port_from_addr(addr).map(|(host, _)| (host, value))
        })
    });
    calculate_topology(
        topology_values,
        curr_retry,
        inner.cluster_params.tls,
        num_of_nodes_to_query,
        inner.cluster_params.read_from_replicas,
    )
}

impl<C> ConnectionLike for ClusterConnection<C>
where
    C: ConnectionLike + Send + Clone + Unpin + Sync + Connect + 'static,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        let routing = cluster_routing::RoutingInfo::for_routable(cmd).unwrap_or(
            cluster_routing::RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random),
        );
        self.route_command(cmd, routing).boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        async move {
            let route = route_for_pipeline(pipeline)?;
            self.route_pipeline(pipeline, offset, count, route.into())
                .await
        }
        .boxed()
    }

    fn get_db(&self) -> i64 {
        0
    }
}
/// Implements the process of connecting to a Redis server
/// and obtaining a connection handle.
pub trait Connect: Sized {
    /// Connect to a node.
    /// For TCP connections, returning a tuple of handle for command execution and the node's IP address.
    /// For UNIX connections, returning a tuple of handle for command execution and None.
    fn connect<'a, T>(
        info: T,
        response_timeout: Duration,
        connection_timeout: Duration,
        socket_addr: Option<SocketAddr>,
    ) -> RedisFuture<'a, (Self, Option<IpAddr>)>
    where
        T: IntoConnectionInfo + Send + 'a;
}

impl Connect for MultiplexedConnection {
    fn connect<'a, T>(
        info: T,
        response_timeout: Duration,
        connection_timeout: Duration,
        socket_addr: Option<SocketAddr>,
    ) -> RedisFuture<'a, (MultiplexedConnection, Option<IpAddr>)>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        async move {
            let connection_info = info.into_connection_info()?;
            let client = crate::Client::open(connection_info)?;
            let connection_timeout: futures_time::time::Duration = connection_timeout.into();

            #[cfg(feature = "tokio-comp")]
            return client
                .get_multiplexed_async_connection_inner::<crate::aio::tokio::Tokio>(
                    response_timeout,
                    socket_addr,
                )
                .timeout(connection_timeout)
                .await?;

            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
            return client
                .get_multiplexed_async_connection_inner::<crate::aio::async_std::AsyncStd>(
                    response_timeout,
                    socket_addr,
                )
                .timeout(connection_timeout)
                .await?;
        }
        .boxed()
    }
}

#[cfg(test)]
mod pipeline_routing_tests {
    use super::route_for_pipeline;
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
            route_for_pipeline(&pipeline),
            Ok(Some(Route::new(12182, SlotAddr::ReplicaOptional)))
        );
    }

    #[test]
    fn test_return_none_if_no_route_is_found() {
        let mut pipeline = crate::Pipeline::new();

        pipeline
            .add_command(cmd("FLUSHALL")) // route to all masters
            .add_command(cmd("EVAL")); // route randomly

        assert_eq!(route_for_pipeline(&pipeline), Ok(None));
    }

    #[test]
    fn test_prefer_primary_route_over_replica() {
        let mut pipeline = crate::Pipeline::new();

        pipeline
            .get("foo") // route to replica of slot 12182
            .add_command(cmd("FLUSHALL")) // route to all masters
            .add_command(cmd("EVAL"))// route randomly
            .cmd("CONFIG").arg("GET").arg("timeout") // unkeyed command
            .set("foo", "bar"); // route to primary of slot 12182

        assert_eq!(
            route_for_pipeline(&pipeline),
            Ok(Some(Route::new(12182, SlotAddr::Master)))
        );
    }

    #[test]
    fn test_raise_cross_slot_error_on_conflicting_slots() {
        let mut pipeline = crate::Pipeline::new();

        pipeline
            .add_command(cmd("FLUSHALL")) // route to all masters
            .set("baz", "bar") // route to slot 4813
            .get("foo"); // route to slot 12182

        assert_eq!(
            route_for_pipeline(&pipeline).unwrap_err().kind(),
            crate::ErrorKind::CrossSlot
        );
    }

    #[test]
    fn unkeyed_commands_dont_affect_route() {
        let mut pipeline = crate::Pipeline::new();

        pipeline
            .set("{foo}bar", "baz") // route to primary of slot 12182
            .cmd("CONFIG").arg("GET").arg("timeout") // unkeyed command
            .set("foo", "bar") // route to primary of slot 12182
            .cmd("DEBUG").arg("PAUSE").arg("100") // unkeyed command
            .cmd("ECHO").arg("hello world"); // unkeyed command

        assert_eq!(
            route_for_pipeline(&pipeline),
            Ok(Some(Route::new(12182, SlotAddr::Master)))
        );
    }
}
