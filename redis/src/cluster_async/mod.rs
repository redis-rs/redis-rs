//! This module provides async functionality for connecting to Redis / Valkey Clusters.
//!
//! The cluster connection is meant to abstract the fact that a cluster is composed of multiple nodes,
//! and to provide an API which is as close as possible to that of a single node connection. In order to do that,
//! the cluster connection maintains connections to each node in the Redis/ Valkey cluster, and can route
//! requests automatically to the relevant nodes. In cases that the cluster connection receives indications
//! that the cluster topology has changed, it will query nodes in order to find the current cluster topology.
//! If it disconnects from some nodes, it will automatically reconnect to those nodes.
//!
//! Note that pubsub & push sending functionality is not currently provided by this module.
//!
//! By default, [`ClusterConnection`] makes use of [`MultiplexedConnection`] and maintains a pool
//! of connections to each node in the cluster.
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
//!
//! # Pipelining
//! ```rust,no_run
//! use redis::cluster::ClusterClient;
//! use redis::{Value, AsyncCommands};
//!
//! async fn fetch_an_integer() -> redis::RedisResult<()> {
//!     let nodes = vec!["redis://127.0.0.1/"];
//!     let client = ClusterClient::new(nodes).unwrap();
//!     let mut connection = client.get_async_connection().await.unwrap();
//!     let key = "test";
//!     
//!     redis::pipe()
//!         .rpush(key, "123").ignore()
//!         .ltrim(key, -10, -1).ignore()
//!         .expire(key, 60).ignore()
//!         .exec_async(&mut connection).await
//! }
//! ```
//!
//! # Sending request to specific node
//! In some cases you'd want to send a request to a specific node in the cluster, instead of
//! letting the cluster connection decide by itself to which node it should send the request.
//! This can happen, for example, if you want to send SCAN commands to each node in the cluster.
//!
//! ```rust,no_run
//! use redis::cluster::ClusterClient;
//! use redis::{Value, AsyncCommands};
//! use redis::cluster_routing::{ RoutingInfo, SingleNodeRoutingInfo };
//!
//! async fn fetch_an_integer() -> redis::RedisResult<Value> {
//!     let nodes = vec!["redis://127.0.0.1/"];
//!     let client = ClusterClient::new(nodes).unwrap();
//!     let mut connection = client.get_async_connection().await.unwrap();
//!     let routing_info = RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress{
//!         host: "redis://127.0.0.1".to_string(),
//!         port: 6378
//!     });
//!     connection.route_command(&redis::cmd("PING"), routing_info).await
//! }
//! ```
use std::{
    collections::HashMap,
    fmt, io, mem,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{self, Poll},
    time::Duration,
};

mod request;
mod routing;
use crate::{
    aio::{ConnectionLike, MultiplexedConnection, Runtime, SharedHandleContainer},
    cluster::{get_connection_info, slot_cmd},
    cluster_client::ClusterParams,
    cluster_routing::{
        MultipleNodeRoutingInfo, Redirect, ResponsePolicy, RoutingInfo, SingleNodeRoutingInfo,
        Slot, SlotMap,
    },
    cluster_topology::parse_slots,
    types::closed_connection_error,
    Cmd, ConnectionInfo, ErrorKind, IntoConnectionInfo, RedisError, RedisFuture, RedisResult,
    Value,
};

use futures::{future::BoxFuture, prelude::*, ready};
use log::{trace, warn};
use rand::{seq::IteratorRandom, thread_rng};
use request::{CmdArg, PendingRequest, Request, RequestState, Retry};
use routing::{route_for_pipeline, InternalRoutingInfo, InternalSingleNodeRouting};
use tokio::sync::{mpsc, oneshot, RwLock};

/// This represents an async Redis Cluster connection.
///
/// It stores the underlying connections maintained for each node in the cluster,
/// as well as common parameters for connecting to nodes and executing commands.
#[derive(Clone)]
pub struct ClusterConnection<C = MultiplexedConnection> {
    sender: mpsc::Sender<Message<C>>,
    _task_handle: SharedHandleContainer,
}

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
                let (sender, mut receiver) = mpsc::channel::<Message<_>>(100);
                let stream = async move {
                    let _ = stream::poll_fn(move |cx| receiver.poll_recv(cx))
                        .map(Ok)
                        .forward(inner)
                        .await;
                };
                let _task_handle = SharedHandleContainer::new(Runtime::locate().spawn(stream));

                ClusterConnection {
                    sender,
                    _task_handle,
                }
            })
    }

    /// Send a command to the given `routing`, and aggregate the response according to `response_policy`.
    pub async fn route_command(&mut self, cmd: &Cmd, routing: RoutingInfo) -> RedisResult<Value> {
        trace!("send_packed_command");
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Message {
                cmd: CmdArg::Cmd {
                    cmd: Arc::new(cmd.clone()), // TODO Remove this clone?
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

    /// Send commands in `pipeline` to the given `route`. If `route` is [None], it will be sent to a random node.
    pub async fn route_pipeline<'a>(
        &'a mut self,
        pipeline: &'a crate::Pipeline,
        offset: usize,
        count: usize,
        route: SingleNodeRoutingInfo,
    ) -> RedisResult<Vec<Value>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Message {
                cmd: CmdArg::Pipeline {
                    pipeline: Arc::new(pipeline.clone()), // TODO Remove this clone?
                    offset,
                    count,
                    route: route.into(),
                },
                sender,
            })
            .await
            .map_err(|_| closed_connection_error())?;

        receiver
            .await
            .unwrap_or_else(|_| Err(closed_connection_error()))
            .map(|response| match response {
                Response::Multiple(values) => values,
                Response::Single(_) => unreachable!(),
            })
    }
}

type ConnectionFuture<C> = future::Shared<BoxFuture<'static, C>>;
type ConnectionMap<C> = HashMap<String, ConnectionFuture<C>>;

/// This is the internal representation of an async Redis Cluster connection. It stores the
/// underlying connections maintained for each node in the cluster, as well
/// as common parameters for connecting to nodes and executing commands.
struct InnerCore<C> {
    conn_lock: RwLock<(ConnectionMap<C>, SlotMap)>,
    cluster_params: ClusterParams,
    pending_requests: Mutex<Vec<PendingRequest<C>>>,
    initial_nodes: Vec<ConnectionInfo>,
}

type Core<C> = Arc<InnerCore<C>>;

/// This is the sink for requests sent by the user.
/// It holds the stream of requests which are "in flight", E.G. on their way to the server,
/// and the inner representation of the connection.
struct ClusterConnInner<C> {
    inner: Core<C>,
    state: ConnectionState,
    #[allow(clippy::complexity)]
    in_flight_requests: stream::FuturesUnordered<Pin<Box<Request<C>>>>,
    refresh_error: Option<RedisError>,
}

fn boxed_sleep(duration: Duration) -> BoxFuture<'static, ()> {
    Box::pin(Runtime::locate_and_sleep(duration))
}

#[derive(Debug, PartialEq)]
pub(crate) enum Response {
    Single(Value),
    Multiple(Vec<Value>),
}

enum OperationTarget {
    Node { address: String },
    NotFound,
    FanOut,
}
type OperationResult = Result<Response, (OperationTarget, RedisError)>;

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
            conn_lock: RwLock::new((connections, SlotMap::new(cluster_params.read_from_replicas))),
            cluster_params,
            pending_requests: Mutex::new(Vec::new()),
            initial_nodes: initial_nodes.to_vec(),
        });
        let connection = ClusterConnInner {
            inner,
            in_flight_requests: Default::default(),
            refresh_error: None,
            state: ConnectionState::PollComplete,
        };
        Self::refresh_slots(connection.inner.clone()).await?;
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
            *write_lock = (
                connection_map,
                SlotMap::new(inner.cluster_params.read_from_replicas),
            );
            drop(write_lock);
            if let Err(err) = Self::refresh_slots(inner.clone()).await {
                warn!("Can't refresh slots with initial nodes: `{err}`");
            };
        }
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
    async fn refresh_slots(inner: Arc<InnerCore<C>>) -> RedisResult<()> {
        let mut write_guard = inner.conn_lock.write().await;
        let mut connections = mem::take(&mut write_guard.0);
        let slots = &mut write_guard.1;
        let mut result = Ok(());
        for (addr, conn) in connections.iter_mut() {
            let mut conn = conn.clone().await;
            let value = match conn
                .req_packed_command(&slot_cmd())
                .await
                .and_then(|value| value.extract_error())
            {
                Ok(value) => value,
                Err(err) => {
                    result = Err(err);
                    continue;
                }
            };
            match parse_slots(
                value,
                inner.cluster_params.tls,
                addr.rsplit_once(':').unwrap().0,
            )
            .and_then(|v: Vec<Slot>| Self::build_slot_map(slots, v))
            {
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

    fn build_slot_map(slot_map: &mut SlotMap, slots_data: Vec<Slot>) -> RedisResult<()> {
        slot_map.clear();
        slot_map.fill_slots(slots_data);
        trace!("{:?}", slot_map);
        Ok(())
    }

    async fn aggregate_results(
        receivers: Vec<(String, oneshot::Receiver<RedisResult<Response>>)>,
        routing: &MultipleNodeRoutingInfo,
        response_policy: Option<ResponsePolicy>,
    ) -> RedisResult<Value> {
        if receivers.is_empty() {
            return Err((
                ErrorKind::ClusterConnectionNotFound,
                "No nodes found for multi-node operation",
            )
                .into());
        }

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
                    .and_then(|mut results| {
                        results.pop().ok_or(
                            (
                                ErrorKind::ClusterConnectionNotFound,
                                "No results received for multi-node operation",
                            )
                                .into(),
                        )
                    })
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
                    Ok((Value::BulkString(addr.into_bytes()), result))
                }))
                .await
                .map(Value::Map)
            }
        }
    }

    async fn execute_on_multiple_nodes<'a>(
        cmd: &'a Arc<Cmd>,
        routing: &'a MultipleNodeRoutingInfo,
        core: Core<C>,
        response_policy: Option<ResponsePolicy>,
    ) -> OperationResult {
        let read_guard = core.conn_lock.read().await;
        if read_guard.0.is_empty() {
            return OperationResult::Err((
                OperationTarget::FanOut,
                (
                    ErrorKind::ClusterConnectionNotFound,
                    "No connections found for multi-node operation",
                )
                    .into(),
            ));
        }
        let (receivers, requests): (Vec<_>, Vec<_>) = {
            let to_request = |(addr, cmd): (&str, Arc<Cmd>)| {
                read_guard.0.get(addr).cloned().map(|conn| {
                    let (sender, receiver) = oneshot::channel();
                    let addr = addr.to_string();
                    (
                        (addr.clone(), receiver),
                        PendingRequest {
                            retry: 0,
                            sender,
                            cmd: CmdArg::Cmd {
                                cmd,
                                routing: InternalSingleNodeRouting::Connection {
                                    identifier: addr,
                                    conn,
                                }
                                .into(),
                            },
                        },
                    )
                })
            };
            let slot_map = &read_guard.1;

            // TODO - these filter_map calls mean that we ignore nodes that are missing. Should we report an error in such cases?
            // since some of the operators drop other requests, mapping to errors here might mean that no request is sent.
            match routing {
                MultipleNodeRoutingInfo::AllNodes => slot_map
                    .addresses_for_all_nodes()
                    .into_iter()
                    .filter_map(|addr| to_request((addr, cmd.clone())))
                    .unzip(),
                MultipleNodeRoutingInfo::AllMasters => slot_map
                    .addresses_for_all_primaries()
                    .into_iter()
                    .filter_map(|addr| to_request((addr, cmd.clone())))
                    .unzip(),
                MultipleNodeRoutingInfo::MultiSlot(routes) => slot_map
                    .addresses_for_multi_slot(routes)
                    .enumerate()
                    .filter_map(|(index, addr_opt)| {
                        addr_opt.and_then(|addr| {
                            let (_, indices) = routes.get(index).unwrap();
                            let cmd =
                                Arc::new(crate::cluster_routing::command_for_multi_slot_indices(
                                    cmd.as_ref(),
                                    indices.iter(),
                                ));
                            to_request((addr, cmd))
                        })
                    })
                    .unzip(),
            }
        };
        drop(read_guard);
        core.pending_requests.lock().unwrap().extend(requests);

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
        let route = match routing {
            InternalRoutingInfo::SingleNode(single_node_routing) => single_node_routing,
            InternalRoutingInfo::MultiNode((multi_node_routing, response_policy)) => {
                return Self::execute_on_multiple_nodes(
                    &cmd,
                    &multi_node_routing,
                    core,
                    response_policy,
                )
                .await;
            }
        };

        match Self::get_connection(route, core).await {
            Ok((addr, mut conn)) => conn
                .req_packed_command(&cmd)
                .await
                .and_then(|value| value.extract_error())
                .map(Response::Single)
                .map_err(|err| (addr.into(), err)),
            Err(err) => Err((OperationTarget::NotFound, err)),
        }
    }

    async fn try_pipeline_request(
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        conn: impl Future<Output = RedisResult<(String, C)>>,
    ) -> OperationResult {
        match conn.await {
            Ok((addr, mut conn)) => conn
                .req_packed_commands(&pipeline, offset, count)
                .await
                .and_then(Value::extract_error_vec)
                .map(Response::Multiple)
                .map_err(|err| (OperationTarget::Node { address: addr }, err)),
            Err(err) => Err((OperationTarget::NotFound, err)),
        }
    }

    async fn try_request(cmd: CmdArg<C>, core: Core<C>) -> OperationResult {
        match cmd {
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
        route: InternalSingleNodeRouting<C>,
        core: Core<C>,
    ) -> RedisResult<(String, C)> {
        let read_guard = core.conn_lock.read().await;

        let conn = match route {
            InternalSingleNodeRouting::Random => None,
            InternalSingleNodeRouting::SpecificNode(route) => read_guard
                .1
                .slot_addr_for_route(&route)
                .map(|addr| addr.to_string()),
            InternalSingleNodeRouting::Connection { identifier, conn } => {
                return Ok((identifier, conn.await));
            }
            InternalSingleNodeRouting::Redirect { redirect, .. } => {
                drop(read_guard);
                // redirected requests shouldn't use a random connection, so they have a separate codepath.
                return Self::get_redirected_connection(redirect, core).await;
            }
            InternalSingleNodeRouting::ByAddress(address) => {
                if let Some(conn) = read_guard.0.get(&address).cloned() {
                    return Ok((address, conn.await));
                } else {
                    return Err((
                        ErrorKind::ClientError,
                        "Requested connection not found",
                        address,
                    )
                        .into());
                }
            }
        }
        .map(|addr| {
            let conn = read_guard.0.get(&addr).cloned();
            (addr, conn)
        });
        drop(read_guard);

        let addr_conn_option = match conn {
            Some((addr, Some(conn))) => Some((addr, conn.await)),
            Some((addr, None)) => connect_check_and_add(core.clone(), addr.clone())
                .await
                .ok()
                .map(|conn| (addr, conn)),
            None => None,
        };

        let (addr, conn) = match addr_conn_option {
            Some(tuple) => tuple,
            None => {
                let read_guard = core.conn_lock.read().await;
                if let Some((random_addr, random_conn_future)) =
                    get_random_connection(&read_guard.0)
                {
                    drop(read_guard);
                    (random_addr, random_conn_future.await)
                } else {
                    return Err(
                        (ErrorKind::ClusterConnectionNotFound, "No connections found").into(),
                    );
                }
            }
        };

        Ok((addr, conn))
    }

    async fn get_redirected_connection(
        redirect: Redirect,
        core: Core<C>,
    ) -> RedisResult<(String, C)> {
        let asking = matches!(redirect, Redirect::Ask(_));
        let addr = match redirect {
            Redirect::Moved(addr) => addr,
            Redirect::Ask(addr) => addr,
        };
        let read_guard = core.conn_lock.read().await;
        let conn = read_guard.0.get(&addr).cloned();
        drop(read_guard);
        let mut conn = match conn {
            Some(conn) => conn.await,
            None => connect_check_and_add(core.clone(), addr.clone()).await?,
        };
        if asking {
            let _ = conn
                .req_packed_command(&crate::cmd::cmd("ASKING"))
                .await
                .and_then(|value| value.extract_error());
        }

        Ok((addr, conn))
    }

    fn poll_recover(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), RedisError>> {
        let recover_future = match &mut self.state {
            ConnectionState::PollComplete => return Poll::Ready(Ok(())),
            ConnectionState::Recover(future) => future,
        };
        match recover_future {
            RecoverFuture::RecoverSlots(ref mut future) => match ready!(future.as_mut().poll(cx)) {
                Ok(_) => {
                    trace!("Recovered!");
                    self.state = ConnectionState::PollComplete;
                    Poll::Ready(Ok(()))
                }
                Err(err) => {
                    trace!("Recover slots failed!");
                    *future = Box::pin(Self::refresh_slots(self.inner.clone()));
                    Poll::Ready(Err(err))
                }
            },
            RecoverFuture::Reconnect(ref mut future) => {
                ready!(future.as_mut().poll(cx));
                trace!("Reconnected connections");
                self.state = ConnectionState::PollComplete;
                Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_complete(&mut self, cx: &mut task::Context<'_>) -> Poll<PollFlushAction> {
        let mut poll_flush_action = PollFlushAction::None;

        let mut pending_requests_guard = self.inner.pending_requests.lock().unwrap();
        if !pending_requests_guard.is_empty() {
            let mut pending_requests = mem::take(&mut *pending_requests_guard);
            for request in pending_requests.drain(..) {
                // Drop the request if noone is waiting for a response to free up resources for
                // requests callers care about (load shedding). It will be ambiguous whether the
                // request actually goes through regardless.
                if request.sender.is_closed() {
                    continue;
                }

                let future = Self::try_request(request.cmd.clone(), self.inner.clone()).boxed();
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
            let (request_handling, next) =
                match Pin::new(&mut self.in_flight_requests).poll_next(cx) {
                    Poll::Ready(Some(result)) => result,
                    Poll::Ready(None) | Poll::Pending => break,
                };
            match request_handling {
                Some(Retry::MoveToPending { request }) => {
                    self.inner.pending_requests.lock().unwrap().push(request)
                }
                Some(Retry::Immediately { request }) => {
                    let future = Self::try_request(request.cmd.clone(), self.inner.clone());
                    self.in_flight_requests.push(Box::pin(Request {
                        retry_params: self.inner.cluster_params.retry_params.clone(),
                        request: Some(request),
                        future: RequestState::Future {
                            future: Box::pin(future),
                        },
                    }));
                }
                Some(Retry::AfterSleep {
                    request,
                    sleep_duration,
                }) => {
                    let future = RequestState::Sleep {
                        sleep: boxed_sleep(sleep_duration),
                    };
                    self.in_flight_requests.push(Box::pin(Request {
                        retry_params: self.inner.cluster_params.retry_params.clone(),
                        request: Some(request),
                        future,
                    }));
                }
                None => {}
            };
            poll_flush_action = poll_flush_action.change_state(next);
        }

        if !matches!(poll_flush_action, PollFlushAction::None) || self.in_flight_requests.is_empty()
        {
            Poll::Ready(poll_flush_action)
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
            } else if let Some(request) = self.inner.pending_requests.lock().unwrap().pop() {
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

#[derive(Debug, PartialEq)]
enum PollFlushAction {
    None,
    RebuildSlots,
    Reconnect(Vec<String>),
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

impl<C> Sink<Message<C>> for ClusterConnInner<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut task::Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, msg: Message<C>) -> Result<(), Self::Error> {
        trace!("start_send");
        let Message { cmd, sender } = msg;

        self.inner
            .pending_requests
            .lock()
            .unwrap()
            .push(PendingRequest {
                retry: 0,
                sender,
                cmd,
            });
        Ok(())
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        trace!("poll_flush: {:?}", self.state);
        loop {
            self.send_refresh_error();

            if let Err(err) = ready!(self.as_mut().poll_recover(cx)) {
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

            match ready!(self.poll_complete(cx)) {
                PollFlushAction::None => return Poll::Ready(Ok(())),
                PollFlushAction::RebuildSlots => {
                    self.state = ConnectionState::Recover(RecoverFuture::RecoverSlots(Box::pin(
                        Self::refresh_slots(self.inner.clone()),
                    )));
                }
                PollFlushAction::Reconnect(addrs) => {
                    self.state = ConnectionState::Recover(RecoverFuture::Reconnect(Box::pin(
                        self.refresh_connections(addrs),
                    )));
                }
                PollFlushAction::ReconnectFromInitialConnections => {
                    self.state = ConnectionState::Recover(RecoverFuture::Reconnect(Box::pin(
                        self.reconnect_to_initial_nodes(),
                    )));
                }
            }
        }
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        // Try to drive any in flight requests to completion
        match self.poll_complete(cx) {
            Poll::Ready(PollFlushAction::None) => (),
            Poll::Ready(_) => Err(())?,
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
    C: ConnectionLike + Send + Clone + Unpin + Sync + Connect + 'static,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        let routing = RoutingInfo::for_routable(cmd)
            .unwrap_or(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random));
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
    /// Connect to a node, returning handle for command execution.
    fn connect<'a, T>(
        info: T,
        response_timeout: Duration,
        connection_timeout: Duration,
    ) -> RedisFuture<'a, Self>
    where
        T: IntoConnectionInfo + Send + 'a;
}

impl Connect for MultiplexedConnection {
    fn connect<'a, T>(
        info: T,
        response_timeout: Duration,
        connection_timeout: Duration,
    ) -> RedisFuture<'a, MultiplexedConnection>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        async move {
            let connection_info = info.into_connection_info()?;
            let client = crate::Client::open(connection_info)?;
            let config = crate::AsyncConnectionConfig::new()
                .set_connection_timeout(connection_timeout)
                .set_response_timeout(response_timeout);
            client
                .get_multiplexed_async_connection_with_config(&config)
                .await
        }
        .boxed()
    }
}

async fn connect_check_and_add<C>(core: Core<C>, addr: String) -> RedisResult<C>
where
    C: ConnectionLike + Connect + Send + Clone + 'static,
{
    match connect_and_check::<C>(&addr, core.cluster_params.clone()).await {
        Ok(conn) => {
            let conn_clone = conn.clone();
            core.conn_lock
                .write()
                .await
                .0
                .insert(addr, async { conn_clone }.boxed().shared());
            Ok(conn)
        }
        Err(err) => Err(err),
    }
}

async fn connect_and_check<C>(node: &str, params: ClusterParams) -> RedisResult<C>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let read_from_replicas = params.read_from_replicas;
    let connection_timeout = params.connection_timeout;
    let response_timeout = params.response_timeout;
    let info = get_connection_info(node, params)?;
    let mut conn: C = C::connect(info, response_timeout, connection_timeout).await?;
    check_connection(&mut conn).await?;
    if read_from_replicas {
        // If READONLY is sent to primary nodes, it will have no effect
        crate::cmd("READONLY").exec_async(&mut conn).await?;
    }
    Ok(conn)
}

async fn check_connection<C>(conn: &mut C) -> RedisResult<()>
where
    C: ConnectionLike + Send + 'static,
{
    let mut cmd = Cmd::new();
    cmd.arg("PING");
    cmd.query_async::<String>(conn).await?;
    Ok(())
}

fn get_random_connection<C>(connections: &ConnectionMap<C>) -> Option<(String, ConnectionFuture<C>)>
where
    C: Clone,
{
    connections
        .keys()
        .choose(&mut thread_rng())
        .and_then(|addr| {
            connections
                .get(addr)
                .map(|conn| (addr.clone(), conn.clone()))
        })
}
