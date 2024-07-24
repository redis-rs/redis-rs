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
//!     let mut connection = client.get_async_connection(None).await.unwrap();
//!     let _: () = connection.set("test", "test_data").await.unwrap();
//!     let rv: String = connection.get("test").await.unwrap();
//!     return rv;
//! }
//! ```

mod connections_container;
mod connections_logic;
/// Exposed only for testing.
pub mod testing {
    pub use super::connections_container::ConnectionWithIp;
    pub use super::connections_logic::*;
}
use crate::{
    cluster_routing::{Routable, RoutingInfo},
    cluster_slotmap::SlotMap,
    cluster_topology::SLOT_SIZE,
    cmd,
    commands::cluster_scan::{cluster_scan, ClusterScanArgs, ObjectType, ScanStateRC},
    FromRedisValue, InfoDict, ToRedisArgs,
};
#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use async_std::task::{spawn, JoinHandle};
#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use futures::executor::block_on;
use std::{
    collections::{HashMap, HashSet},
    fmt, io, mem,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        atomic::{self, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{self, Poll},
    time::SystemTime,
};
#[cfg(feature = "tokio-comp")]
use tokio::task::JoinHandle;

use crate::{
    aio::{get_socket_addrs, ConnectionLike, MultiplexedConnection, Runtime},
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
        calculate_topology, get_slot, SlotRefreshState, DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES,
        DEFAULT_REFRESH_SLOTS_RETRY_INITIAL_INTERVAL, DEFAULT_REFRESH_SLOTS_RETRY_MAX_INTERVAL,
    },
    connection::{PubSubSubscriptionInfo, PubSubSubscriptionKind},
    push_manager::PushInfo,
    Cmd, ConnectionInfo, ErrorKind, IntoConnectionInfo, RedisError, RedisFuture, RedisResult,
    Value,
};
use futures::stream::{FuturesUnordered, StreamExt};
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
use pin_project_lite::pin_project;
use tokio::sync::{
    mpsc,
    oneshot::{self, Receiver},
    RwLock,
};
use tracing::{debug, info, trace, warn};

use self::{
    connections_container::{ConnectionAndAddress, ConnectionType, ConnectionsMap},
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
        push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
    ) -> RedisResult<ClusterConnection<C>> {
        ClusterConnInner::new(initial_nodes, cluster_params, push_sender)
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

    /// Special handling for `SCAN` command, using `cluster_scan`.
    /// If you wish to use a match pattern, use [`cluster_scan_with_pattern`].
    /// Perform a `SCAN` command on a Redis cluster, using scan state object in order to handle changes in topology
    /// and make sure that all keys that were in the cluster from start to end of the scan are scanned.
    /// In order to make sure all keys in the cluster scanned, topology refresh occurs more frequently and may affect performance.
    ///
    /// # Arguments
    ///
    /// * `scan_state_rc` - A reference to the scan state, For initiating new scan send [`ScanStateRC::new()`],
    /// for each subsequent iteration use the returned [`ScanStateRC`].    
    /// * `count` - An optional count of keys requested,
    /// the amount returned can vary and not obligated to return exactly count.
    /// * `object_type` - An optional [`ObjectType`] enum of requested key redis type.
    ///
    /// # Returns
    ///
    /// A [`ScanStateRC`] for the updated state of the scan and the vector of keys that were found in the scan.
    /// structure of returned value:
    /// `Ok((ScanStateRC, Vec<Value>))`
    ///
    /// When the scan is finished [`ScanStateRC`] will be None, and can be checked by calling `scan_state_wrapper.is_finished()`.
    ///
    /// # Example
    /// ```rust,no_run
    /// use redis::cluster::ClusterClient;
    /// use redis::{ScanStateRC, FromRedisValue, from_redis_value, Value, ObjectType};
    ///
    /// async fn scan_all_cluster() -> Vec<String> {
    ///     let nodes = vec!["redis://127.0.0.1/"];
    ///     let client = ClusterClient::new(nodes).unwrap();
    ///     let mut connection = client.get_async_connection(None).await.unwrap();
    ///     let mut scan_state_rc = ScanStateRC::new();
    ///     let mut keys: Vec<String> = vec![];
    ///     loop {
    ///         let (next_cursor, scan_keys): (ScanStateRC, Vec<Value>) =
    ///             connection.cluster_scan(scan_state_rc, None, None).await.unwrap();
    ///         scan_state_rc = next_cursor;
    ///         let mut scan_keys = scan_keys
    ///             .into_iter()
    ///             .map(|v| from_redis_value(&v).unwrap())
    ///             .collect::<Vec<String>>(); // Change the type of `keys` to `Vec<String>`
    ///         keys.append(&mut scan_keys);
    ///         if scan_state_rc.is_finished() {
    ///             break;
    ///             }
    ///         }
    ///     keys     
    ///     }
    /// ```
    pub async fn cluster_scan(
        &mut self,
        scan_state_rc: ScanStateRC,
        count: Option<usize>,
        object_type: Option<ObjectType>,
    ) -> RedisResult<(ScanStateRC, Vec<Value>)> {
        let cluster_scan_args = ClusterScanArgs::new(scan_state_rc, None, count, object_type);
        self.route_cluster_scan(cluster_scan_args).await
    }

    /// Special handling for `SCAN` command, using `cluster_scan_with_pattern`.
    /// It is a special case of [`cluster_scan`], with an additional match pattern.
    /// Perform a `SCAN` command on a Redis cluster, using scan state object in order to handle changes in topology
    /// and make sure that all keys that were in the cluster from start to end of the scan are scanned.
    /// In order to make sure all keys in the cluster scanned, topology refresh occurs more frequently and may affect performance.
    ///
    /// # Arguments
    ///
    /// * `scan_state_rc` - A reference to the scan state, For initiating new scan send [`ScanStateRC::new()`],
    /// for each subsequent iteration use the returned [`ScanStateRC`].
    /// * `match_pattern` - A match pattern of requested keys.
    /// * `count` - An optional count of keys requested,
    /// the amount returned can vary and not obligated to return exactly count.
    /// * `object_type` - An optional [`ObjectType`] enum of requested key redis type.
    ///
    /// # Returns
    ///
    /// A [`ScanStateRC`] for the updated state of the scan and the vector of keys that were found in the scan.
    /// structure of returned value:
    /// `Ok((ScanStateRC, Vec<Value>))`
    ///
    /// When the scan is finished [`ScanStateRC`] will be None, and can be checked by calling `scan_state_wrapper.is_finished()`.
    ///
    /// # Example
    /// ```rust,no_run
    /// use redis::cluster::ClusterClient;
    /// use redis::{ScanStateRC, FromRedisValue, from_redis_value, Value, ObjectType};
    ///
    /// async fn scan_all_cluster() -> Vec<String> {
    ///     let nodes = vec!["redis://127.0.0.1/"];
    ///     let client = ClusterClient::new(nodes).unwrap();
    ///     let mut connection = client.get_async_connection(None).await.unwrap();
    ///     let mut scan_state_rc = ScanStateRC::new();
    ///     let mut keys: Vec<String> = vec![];
    ///     loop {
    ///         let (next_cursor, scan_keys): (ScanStateRC, Vec<Value>) =
    ///             connection.cluster_scan_with_pattern(scan_state_rc, b"my_key", None, None).await.unwrap();
    ///         scan_state_rc = next_cursor;
    ///         let mut scan_keys = scan_keys
    ///             .into_iter()
    ///             .map(|v| from_redis_value(&v).unwrap())
    ///             .collect::<Vec<String>>(); // Change the type of `keys` to `Vec<String>`
    ///         keys.append(&mut scan_keys);
    ///         if scan_state_rc.is_finished() {
    ///             break;
    ///             }
    ///         }
    ///     keys     
    ///     }
    /// ```
    pub async fn cluster_scan_with_pattern<K: ToRedisArgs>(
        &mut self,
        scan_state_rc: ScanStateRC,
        match_pattern: K,
        count: Option<usize>,
        object_type: Option<ObjectType>,
    ) -> RedisResult<(ScanStateRC, Vec<Value>)> {
        let cluster_scan_args = ClusterScanArgs::new(
            scan_state_rc,
            Some(match_pattern.to_redis_args().concat()),
            count,
            object_type,
        );
        self.route_cluster_scan(cluster_scan_args).await
    }

    /// Route cluster scan to be handled by internal cluster_scan command
    async fn route_cluster_scan(
        &mut self,
        cluster_scan_args: ClusterScanArgs,
    ) -> RedisResult<(ScanStateRC, Vec<Value>)> {
        let (sender, receiver) = oneshot::channel();
        self.0
            .send(Message {
                cmd: CmdArg::ClusterScan { cluster_scan_args },
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
                Response::ClusterScanResult(new_scan_state_ref, key) => (new_scan_state_ref, key),
                Response::Single(_) => unreachable!(),
                Response::Multiple(_) => unreachable!(),
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
                Response::ClusterScanResult(_, _) => unreachable!(),
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
                Response::ClusterScanResult(_, _) => unreachable!(),
            })
    }
}

type ConnectionMap<C> = connections_container::ConnectionsMap<ConnectionFuture<C>>;
type ConnectionsContainer<C> =
    self::connections_container::ConnectionsContainer<ConnectionFuture<C>>;

pub(crate) struct InnerCore<C> {
    pub(crate) conn_lock: RwLock<ConnectionsContainer<C>>,
    cluster_params: ClusterParams,
    pending_requests: Mutex<Vec<PendingRequest<C>>>,
    slot_refresh_state: SlotRefreshState,
    initial_nodes: Vec<ConnectionInfo>,
    push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
    subscriptions_by_address: RwLock<HashMap<ArcStr, PubSubSubscriptionInfo>>,
    unassigned_subscriptions: RwLock<PubSubSubscriptionInfo>,
}

pub(crate) type Core<C> = Arc<InnerCore<C>>;

impl<C> InnerCore<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    // return address of node for slot
    pub(crate) async fn get_address_from_slot(
        &self,
        slot: u16,
        slot_addr: SlotAddr,
    ) -> Option<String> {
        self.conn_lock
            .read()
            .await
            .slot_map
            .get_node_address_for_slot(slot, slot_addr)
    }

    // return epoch of node
    pub(crate) async fn get_address_epoch(&self, node_address: &str) -> Result<u64, RedisError> {
        let command = cmd("CLUSTER").arg("INFO").to_owned();
        let node_conn = self
            .conn_lock
            .read()
            .await
            .connection_for_address(node_address)
            .ok_or(RedisError::from((
                ErrorKind::ResponseError,
                "Failed to parse cluster info",
            )))?;

        let cluster_info = node_conn.1.await.req_packed_command(&command).await;
        match cluster_info {
            Ok(value) => {
                let info_dict: Result<InfoDict, RedisError> =
                    FromRedisValue::from_redis_value(&value);
                if let Ok(info_dict) = info_dict {
                    let epoch = info_dict.get("cluster_my_epoch");
                    if let Some(epoch) = epoch {
                        Ok(epoch)
                    } else {
                        Err(RedisError::from((
                            ErrorKind::ResponseError,
                            "Failed to get epoch from cluster info",
                        )))
                    }
                } else {
                    Err(RedisError::from((
                        ErrorKind::ResponseError,
                        "Failed to parse cluster info",
                    )))
                }
            }
            Err(redis_error) => Err(redis_error),
        }
    }

    // return slots of node
    pub(crate) async fn get_slots_of_address(&self, node_address: &str) -> Vec<u16> {
        self.conn_lock
            .read()
            .await
            .slot_map
            .get_slots_of_node(node_address)
    }
}

pub(crate) struct ClusterConnInner<C> {
    pub(crate) inner: Core<C>,
    state: ConnectionState,
    #[allow(clippy::complexity)]
    in_flight_requests: stream::FuturesUnordered<Pin<Box<Request<C>>>>,
    refresh_error: Option<RedisError>,
    // Handler of the periodic check task.
    periodic_checks_handler: Option<JoinHandle<()>>,
}

impl<C> Dispose for ClusterConnInner<C> {
    fn dispose(self) {
        if let Some(handle) = self.periodic_checks_handler {
            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
            block_on(handle.cancel());
            #[cfg(feature = "tokio-comp")]
            handle.abort()
        }
    }
}

#[derive(Clone)]
pub(crate) enum InternalRoutingInfo<C> {
    SingleNode(InternalSingleNodeRouting<C>),
    MultiNode((MultipleNodeRoutingInfo, Option<ResponsePolicy>)),
}

#[derive(PartialEq, Clone, Debug)]
/// Represents different policies for refreshing the cluster slots.
pub(crate) enum RefreshPolicy {
    /// `Throttable` indicates that the refresh operation can be throttled,
    /// meaning it can be delayed or rate-limited if necessary.
    Throttable,
    /// `NotThrottable` indicates that the refresh operation should not be throttled,
    /// meaning it should be executed immediately without any delay or rate-limiting.
    NotThrottable,
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
pub(crate) enum InternalSingleNodeRouting<C> {
    Random,
    SpecificNode(Route),
    ByAddress(String),
    Connection {
        address: ArcStr,
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
    ClusterScan {
        // struct containing the arguments for the cluster scan command - scan state cursor, match pattern, count and object type.
        cluster_scan_args: ClusterScanArgs,
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

fn boxed_sleep(duration: Duration) -> BoxFuture<'static, ()> {
    #[cfg(feature = "tokio-comp")]
    return Box::pin(tokio::time::sleep(duration));

    #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
    return Box::pin(async_std::task::sleep(duration));
}

pub(crate) enum Response {
    Single(Value),
    ClusterScanResult(ScanStateRC, Vec<Value>),
    Multiple(Vec<Value>),
}

pub(crate) enum OperationTarget {
    Node { address: ArcStr },
    FanOut,
    NotFound,
}
type OperationResult = Result<Response, (OperationTarget, RedisError)>;

impl From<ArcStr> for OperationTarget {
    fn from(address: ArcStr) -> Self {
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
                // cluster_scan is sent as a normal command internally so we will not reach that point.
                CmdArg::ClusterScan { .. } => {
                    unreachable!()
                }
            }
        }
    }

    fn reset_routing(&mut self) {
        let fix_route = |route: &mut InternalSingleNodeRouting<C>| {
            match route {
                InternalSingleNodeRouting::Redirect {
                    previous_routing, ..
                } => {
                    let previous_routing = std::mem::take(previous_routing.as_mut());
                    *route = previous_routing;
                }
                // If a specific connection is specified, then reconnecting without resetting the routing
                // will mean that the request is still routed to the old connection.
                InternalSingleNodeRouting::Connection { address, .. } => {
                    *route = InternalSingleNodeRouting::ByAddress(address.to_string());
                }
                _ => {}
            }
        };
        match &mut self.cmd {
            CmdArg::Cmd { routing, .. } => {
                if let InternalRoutingInfo::SingleNode(route) = routing {
                    fix_route(route);
                }
            }
            CmdArg::Pipeline { route, .. } => {
                fix_route(route);
            }
            // cluster_scan is sent as a normal command internally so we will not reach that point.
            CmdArg::ClusterScan { .. } => {
                unreachable!()
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
        address: ArcStr,
    },
    Reconnect {
        // if not set, then a reconnect should happen without sending a request afterwards
        request: Option<PendingRequest<C>>,
        target: ArcStr,
    },
    RefreshSlots {
        // if not set, then a slot refresh should happen without sending a request afterwards
        request: Option<PendingRequest<C>>,
        sleep_duration: Option<Duration>,
    },
    ReconnectToInitialNodes {
        // if not set, then a reconnect should happen without sending a request afterwards
        request: Option<PendingRequest<C>>,
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
                // TODO - would be nice if we didn't need to repeat this code twice, with & without retries.
                if request.retry >= this.retry_params.number_of_retries {
                    let next = if err.kind() == ErrorKind::ClusterConnectionNotFound {
                        Next::ReconnectToInitialNodes { request: None }.into()
                    } else if matches!(err.retry_method(), crate::types::RetryMethod::MovedRedirect)
                        || matches!(target, OperationTarget::NotFound)
                    {
                        Next::RefreshSlots {
                            request: None,
                            sleep_duration: None,
                        }
                        .into()
                    } else if matches!(err.retry_method(), crate::types::RetryMethod::Reconnect) {
                        if let OperationTarget::Node { address } = target {
                            Next::Reconnect {
                                request: None,
                                target: address,
                            }
                            .into()
                        } else {
                            Next::Done.into()
                        }
                    } else {
                        Next::Done.into()
                    };
                    self.respond(Err(err));
                    return next;
                }
                request.retry = request.retry.saturating_add(1);

                if err.kind() == ErrorKind::ClusterConnectionNotFound {
                    return Next::ReconnectToInitialNodes {
                        request: Some(this.request.take().unwrap()),
                    }
                    .into();
                }

                let sleep_duration = this.retry_params.wait_time_for_retry(request.retry);

                let address = match target {
                    OperationTarget::Node { address } => address,
                    OperationTarget::FanOut => {
                        trace!("Request error `{}` multi-node request", err);

                        // Fanout operation are retried per internal request, and don't need additional retries.
                        self.respond(Err(err));
                        return Next::Done.into();
                    }
                    OperationTarget::NotFound => {
                        // TODO - this is essentially a repeat of the retirable error. probably can remove duplication.
                        let mut request = this.request.take().unwrap();
                        request.info.reset_routing();
                        return Next::RefreshSlots {
                            request: Some(request),
                            sleep_duration: Some(sleep_duration),
                        }
                        .into();
                    }
                };

                warn!("Received request error {} on node {:?}.", err, address);

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
                        Next::RefreshSlots {
                            request: Some(request),
                            sleep_duration: None,
                        }
                        .into()
                    }
                    crate::types::RetryMethod::WaitAndRetry => {
                        let sleep_duration = this.retry_params.wait_time_for_retry(request.retry);
                        // Sleep and retry.
                        this.future.set(RequestState::Sleep {
                            sleep: boxed_sleep(sleep_duration),
                        });
                        self.poll(cx)
                    }
                    crate::types::RetryMethod::Reconnect => {
                        let mut request = this.request.take().unwrap();
                        // TODO should we reset the redirect here?
                        request.info.reset_routing();
                        warn!("disconnected from {:?}", address);
                        Next::Reconnect {
                            request: Some(request),
                            target: address,
                        }
                        .into()
                    }
                    crate::types::RetryMethod::WaitAndRetryOnPrimaryRedirectOnReplica => {
                        Next::RetryBusyLoadingError {
                            request: this.request.take().unwrap(),
                            address,
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
    Found((ArcStr, ConnectionFuture<C>)),
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
        push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
    ) -> RedisResult<Disposable<Self>> {
        let connections =
            Self::create_initial_connections(initial_nodes, &cluster_params, push_sender.clone())
                .await?;

        let topology_checks_interval = cluster_params.topology_checks_interval;
        let slots_refresh_rate_limiter = cluster_params.slots_refresh_rate_limit;
        let inner = Arc::new(InnerCore {
            conn_lock: RwLock::new(ConnectionsContainer::new(
                Default::default(),
                connections,
                cluster_params.read_from_replicas,
                0,
            )),
            cluster_params: cluster_params.clone(),
            pending_requests: Mutex::new(Vec::new()),
            slot_refresh_state: SlotRefreshState::new(slots_refresh_rate_limiter),
            initial_nodes: initial_nodes.to_vec(),
            push_sender: push_sender.clone(),
            unassigned_subscriptions: RwLock::new(
                if let Some(subs) = cluster_params.pubsub_subscriptions {
                    subs.clone()
                } else {
                    PubSubSubscriptionInfo::new()
                },
            ),
            subscriptions_by_address: RwLock::new(Default::default()),
        });
        let mut connection = ClusterConnInner {
            inner,
            in_flight_requests: Default::default(),
            refresh_error: None,
            state: ConnectionState::PollComplete,
            periodic_checks_handler: None,
        };
        Self::refresh_slots_and_subscriptions_with_retries(
            connection.inner.clone(),
            &RefreshPolicy::NotThrottable,
        )
        .await?;

        if let Some(duration) = topology_checks_interval {
            let periodic_task =
                ClusterConnInner::periodic_topology_check(connection.inner.clone(), duration);
            #[cfg(feature = "tokio-comp")]
            {
                connection.periodic_checks_handler = Some(tokio::spawn(periodic_task));
            }
            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
            {
                connection.periodic_checks_handler = Some(spawn(periodic_task));
            }
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
        push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
    ) -> RedisResult<ConnectionMap<C>> {
        let initial_nodes: Vec<(String, Option<SocketAddr>)> =
            Self::try_to_expand_initial_nodes(initial_nodes).await;
        let connections = stream::iter(initial_nodes.iter().cloned())
            .map(|(node_addr, socket_addr)| {
                let mut params: ClusterParams = params.clone();
                let push_sender = push_sender.clone();
                // set subscriptions to none, they will be applied upon the topology discovery
                params.pubsub_subscriptions = None;

                async move {
                    let result = connect_and_check(
                        &node_addr,
                        params,
                        socket_addr,
                        RefreshConnectionType::AllConnections,
                        None,
                        push_sender,
                    )
                    .await
                    .get_node();
                    let node_address = if let Some(socket_addr) = socket_addr {
                        socket_addr.to_string()
                    } else {
                        node_addr
                    };
                    result.map(|node| (node_address, node))
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
            let connection_map = match Self::create_initial_connections(
                &inner.initial_nodes,
                &inner.cluster_params,
                None,
            )
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
            if let Err(err) = Self::refresh_slots_and_subscriptions_with_retries(
                inner.clone(),
                &RefreshPolicy::Throttable,
            )
            .await
            {
                warn!("Can't refresh slots with initial nodes: `{err}`");
            };
        }
    }

    async fn refresh_connections(
        inner: Arc<InnerCore<C>>,
        addresses: Vec<ArcStr>,
        conn_type: RefreshConnectionType,
    ) {
        info!("Started refreshing connections to {:?}", addresses);
        let mut connections_container = inner.conn_lock.write().await;
        let cluster_params = &inner.cluster_params;
        let subscriptions_by_address = &inner.subscriptions_by_address;
        let push_sender = &inner.push_sender;

        stream::iter(addresses.into_iter())
            .fold(
                &mut *connections_container,
                |connections_container, address| async move {
                    let node_option = connections_container.remove_node(&address);

                    // override subscriptions for this connection
                    let mut cluster_params = cluster_params.clone();
                    let subs_guard = subscriptions_by_address.read().await;
                    cluster_params.pubsub_subscriptions = subs_guard.get(&address).cloned();
                    drop(subs_guard);
                    let node = get_or_create_conn(
                        &address,
                        node_option,
                        &cluster_params,
                        conn_type,
                        push_sender.clone(),
                    )
                    .await;
                    match node {
                        Ok(node) => {
                            connections_container
                                .replace_or_add_connection_for_address(address, node);
                        }
                        Err(err) => {
                            warn!(
                                "Failed to refresh connection for node {}. Error: `{:?}`",
                                address, err
                            );
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
            Response::ClusterScanResult(_, _) => unreachable!(),
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
            Some(ResponsePolicy::FirstSucceededNonEmptyOrAllEmpty) => {
                // Attempt to return the first result that isn't `Nil` or an error.
                // If no such response is found and all servers returned `Nil`, it indicates that all shards are empty, so return `Nil`.
                // If we received only errors, return the last received error.
                // If we received a mix of errors and `Nil`s, we can't determine if all shards are empty,
                // thus we return the last received error instead of `Nil`.
                let num_of_results: usize = receivers.len();
                let mut futures = receivers
                    .into_iter()
                    .map(get_receiver)
                    .collect::<FuturesUnordered<_>>();
                let mut nil_counter = 0;
                let mut last_err = None;
                while let Some(result) = futures.next().await {
                    match result {
                        Ok(Value::Nil) => nil_counter += 1,
                        Ok(val) => return Ok(val),
                        Err(e) => last_err = Some(e),
                    }
                }

                if nil_counter == num_of_results {
                    // All received results are `Nil`
                    Ok(Value::Nil)
                } else {
                    Err(last_err.unwrap_or_else(|| {
                        (
                            ErrorKind::ClusterConnectionNotFound,
                            "Couldn't find any connection",
                        )
                            .into()
                    }))
                }
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
    async fn refresh_slots_and_subscriptions_with_retries(
        inner: Arc<InnerCore<C>>,
        policy: &RefreshPolicy,
    ) -> RedisResult<()> {
        let SlotRefreshState {
            in_progress,
            last_run,
            rate_limiter,
        } = &inner.slot_refresh_state;
        // Ensure only a single slot refresh operation occurs at a time
        if in_progress
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }
        let mut skip_slots_refresh = false;
        if *policy == RefreshPolicy::Throttable {
            // Check if the current slot refresh is triggered before the wait duration has passed
            let last_run_rlock = last_run.read().await;
            if let Some(last_run_time) = *last_run_rlock {
                let passed_time = SystemTime::now()
                    .duration_since(last_run_time)
                    .unwrap_or_else(|err| {
                        warn!(
                            "Failed to get the duration since the last slot refresh, received error: {:?}",
                            err
                        );
                        // Setting the passed time to 0 will force the current refresh to continue and reset the stored last_run timestamp with the current one
                        Duration::from_secs(0)
                    });
                let wait_duration = rate_limiter.wait_duration();
                if passed_time <= wait_duration {
                    debug!("Skipping slot refresh as the wait duration hasn't yet passed. Passed time = {:?}, 
                            Wait duration = {:?}", passed_time, wait_duration);
                    skip_slots_refresh = true;
                }
            }
        }

        let mut res = Ok(());
        if !skip_slots_refresh {
            let retry_strategy = ExponentialBackoff {
                initial_interval: DEFAULT_REFRESH_SLOTS_RETRY_INITIAL_INTERVAL,
                max_interval: DEFAULT_REFRESH_SLOTS_RETRY_MAX_INTERVAL,
                max_elapsed_time: None,
                ..Default::default()
            };
            let retries_counter = AtomicUsize::new(0);
            res = retry(retry_strategy, || {
                let curr_retry = retries_counter.fetch_add(1, atomic::Ordering::Relaxed);
                Self::refresh_slots(inner.clone(), curr_retry)
            })
            .await;
        }
        in_progress.store(false, Ordering::Relaxed);

        Self::refresh_pubsub_subscriptions(inner).await;

        res
    }

    pub(crate) async fn check_topology_and_refresh_if_diff(
        inner: Arc<InnerCore<C>>,
        policy: &RefreshPolicy,
    ) -> bool {
        let topology_changed = Self::check_for_topology_diff(inner.clone()).await;
        if topology_changed {
            let _ = Self::refresh_slots_and_subscriptions_with_retries(inner.clone(), policy).await;
        }
        topology_changed
    }

    async fn periodic_topology_check(inner: Arc<InnerCore<C>>, interval_duration: Duration) {
        loop {
            let _ = boxed_sleep(interval_duration).await;
            let topology_changed =
                Self::check_topology_and_refresh_if_diff(inner.clone(), &RefreshPolicy::Throttable)
                    .await;
            if !topology_changed {
                // This serves as a safety measure for validating pubsub subsctiptions state in case it has drifted
                // while topology stayed the same.
                // For example, a failed attempt to refresh a connection which is triggered from refresh_pubsub_subscriptions(),
                // might leave a node unconnected indefinitely in case topology is stable and no request are attempted to this node.
                Self::refresh_pubsub_subscriptions(inner.clone()).await;
            }
        }
    }

    async fn refresh_pubsub_subscriptions(inner: Arc<InnerCore<C>>) {
        if inner.cluster_params.protocol != crate::types::ProtocolVersion::RESP3 {
            return;
        }

        let mut addrs_to_refresh: HashSet<ArcStr> = HashSet::new();
        let mut subs_by_address_guard = inner.subscriptions_by_address.write().await;
        let mut unassigned_subs_guard = inner.unassigned_subscriptions.write().await;
        let conns_read_guard = inner.conn_lock.read().await;

        // validate active subscriptions location
        subs_by_address_guard.retain(|current_address, address_subs| {
            address_subs.retain(|kind, channels_patterns| {
                channels_patterns.retain(|channel_pattern| {
                    let new_slot = get_slot(channel_pattern);
                    let mut valid = false;
                    if let Some((new_address, _)) = conns_read_guard
                        .connection_for_route(&Route::new(new_slot, SlotAddr::Master))
                    {
                        if *new_address == *current_address {
                            valid = true;
                        }
                    }
                    // no new address or new address differ - move to unassigned and store this address for connection reset
                    if !valid {
                        // need to drop the original connection for clearing the subscription in the server, avoiding possible double-receivers
                        if conns_read_guard
                            .connection_for_address(current_address)
                            .is_some()
                        {
                            addrs_to_refresh.insert(current_address.clone());
                        }

                        unassigned_subs_guard
                            .entry(*kind)
                            .and_modify(|channels_patterns| {
                                channels_patterns.insert(channel_pattern.clone());
                            })
                            .or_insert(HashSet::from([channel_pattern.clone()]));
                    }
                    valid
                });
                !channels_patterns.is_empty()
            });
            !address_subs.is_empty()
        });

        // try to assign new addresses
        unassigned_subs_guard.retain(|kind: &PubSubSubscriptionKind, channels_patterns| {
            channels_patterns.retain(|channel_pattern| {
                let new_slot = get_slot(channel_pattern);
                if let Some((new_address, _)) =
                    conns_read_guard.connection_for_route(&Route::new(new_slot, SlotAddr::Master))
                {
                    // need to drop the new connection so the subscription will be picked up in setup_connection()
                    addrs_to_refresh.insert(new_address.clone());

                    let e = subs_by_address_guard
                        .entry(new_address.clone())
                        .or_insert(PubSubSubscriptionInfo::new());

                    e.entry(*kind)
                        .or_insert(HashSet::new())
                        .insert(channel_pattern.clone());

                    return false;
                }
                true
            });
            !channels_patterns.is_empty()
        });

        drop(conns_read_guard);
        drop(unassigned_subs_guard);
        drop(subs_by_address_guard);

        if !addrs_to_refresh.is_empty() {
            let mut conns_write_guard = inner.conn_lock.write().await;
            // have to remove or otherwise the refresh_connection wont trigger node recreation
            for addr_to_refresh in addrs_to_refresh.iter() {
                conns_write_guard.remove_node(addr_to_refresh);
            }
            drop(conns_write_guard);
            // immediately trigger connection reestablishment
            Self::refresh_connections(
                inner.clone(),
                addrs_to_refresh.into_iter().collect(),
                RefreshConnectionType::AllConnections,
            )
            .await;
        }
    }

    /// Queries log2n nodes (where n represents the number of cluster nodes) to determine whether their
    /// topology view differs from the one currently stored in the connection manager.
    /// Returns true if change was detected, otherwise false.
    async fn check_for_topology_diff(inner: Arc<InnerCore<C>>) -> bool {
        let read_guard = inner.conn_lock.read().await;
        let num_of_nodes: usize = read_guard.len();
        // TODO: Starting from Rust V1.67, integers has logarithms support.
        // When we no longer need to support Rust versions < 1.67, remove fast_math and transition to the ilog2 function.
        let num_of_nodes_to_query =
            std::cmp::max(fast_math::log2_raw(num_of_nodes as f32) as usize, 1);
        let (res, failed_connections) = calculate_topology_from_random_nodes(
            &inner,
            num_of_nodes_to_query,
            &read_guard,
            DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES,
        )
        .await;

        if let Ok((_, found_topology_hash)) = res {
            if read_guard.get_current_topology_hash() != found_topology_hash {
                return true;
            }
        }
        drop(read_guard);

        if !failed_connections.is_empty() {
            Self::refresh_connections(
                inner,
                failed_connections,
                RefreshConnectionType::OnlyManagementConnection,
            )
            .await;
        }

        false
    }

    async fn refresh_slots(
        inner: Arc<InnerCore<C>>,
        curr_retry: usize,
    ) -> Result<(), BackoffError<RedisError>> {
        // Update the slot refresh last run timestamp
        let now = SystemTime::now();
        let mut last_run_wlock = inner.slot_refresh_state.last_run.write().await;
        *last_run_wlock = Some(now);
        drop(last_run_wlock);
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

    pub(crate) fn check_if_all_slots_covered(slot_map: &SlotMap) -> bool {
        let mut slots_covered = 0;
        for (end, slots) in slot_map.slots.iter() {
            slots_covered += end.saturating_sub(slots.start).saturating_add(1);
        }
        slots_covered == SLOT_SIZE
    }

    // Query a node to discover slot-> master mappings
    async fn refresh_slots_inner(inner: Arc<InnerCore<C>>, curr_retry: usize) -> RedisResult<()> {
        let read_guard = inner.conn_lock.read().await;
        let num_of_nodes = read_guard.len();
        const MAX_REQUESTED_NODES: usize = 10;
        let num_of_nodes_to_query = std::cmp::min(num_of_nodes, MAX_REQUESTED_NODES);
        let (new_slots, topology_hash) = calculate_topology_from_random_nodes(
            &inner,
            num_of_nodes_to_query,
            &read_guard,
            curr_retry,
        )
        .await
        .0?;
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
                    let mut cluster_params = inner.cluster_params.clone();
                    let subs_guard = inner.subscriptions_by_address.read().await;
                    cluster_params.pubsub_subscriptions =
                        subs_guard.get(&ArcStr::from(addr.as_str())).cloned();
                    drop(subs_guard);
                    let node = get_or_create_conn(
                        addr,
                        node,
                        &cluster_params,
                        RefreshConnectionType::AllConnections,
                        inner.push_sender.clone(),
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
                    ErrorKind::ClusterConnectionNotFound,
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
                Item = Option<(Arc<Cmd>, ConnectionAndAddress<ConnectionFuture<C>>)>,
            >,
        ) -> (
            Vec<(Option<ArcStr>, Receiver<Result<Response, RedisError>>)>,
            Vec<Option<PendingRequest<C>>>,
        ) {
            iterator
                .map(|tuple_opt| {
                    let (sender, receiver) = oneshot::channel();
                    if let Some((cmd, conn, address)) =
                        tuple_opt.map(|(cmd, (address, conn))| (cmd, conn, address))
                    {
                        (
                            (Some(address.clone()), receiver),
                            Some(PendingRequest {
                                retry: 0,
                                sender,
                                info: RequestInfo {
                                    cmd: CmdArg::Cmd {
                                        cmd,
                                        routing: InternalSingleNodeRouting::Connection {
                                            address,
                                            conn,
                                        }
                                        .into(),
                                    },
                                },
                            }),
                        )
                    } else {
                        let _ = sender.send(Err((
                            ErrorKind::ClusterConnectionNotFound,
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
            ),
            MultipleNodeRoutingInfo::AllMasters => into_channels(
                connections_container
                    .all_primary_connections()
                    .map(|tuple| Some((cmd.clone(), tuple))),
            ),
            MultipleNodeRoutingInfo::MultiSlot(slots) => {
                into_channels(slots.iter().map(|(route, indices)| {
                    connections_container
                        .connection_for_route(route)
                        .map(|tuple| {
                            let new_cmd = crate::cluster_routing::command_for_multi_slot_indices(
                                cmd.as_ref(),
                                indices.iter(),
                            );
                            (Arc::new(new_cmd), tuple)
                        })
                }))
            }
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

    pub(crate) async fn try_cmd_request(
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
        let (address, mut conn) = Self::get_connection(routing, core, Some(cmd.clone()))
            .await
            .map_err(|err| (OperationTarget::NotFound, err))?;
        conn.req_packed_command(&cmd)
            .await
            .map(Response::Single)
            .map_err(|err| (address.into(), err))
    }

    async fn try_pipeline_request(
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        conn: impl Future<Output = RedisResult<(ArcStr, C)>>,
    ) -> OperationResult {
        trace!("try_pipeline_request");
        let (address, mut conn) = conn.await.map_err(|err| (OperationTarget::NotFound, err))?;
        conn.req_packed_commands(&pipeline, offset, count)
            .await
            .map(Response::Multiple)
            .map_err(|err| (OperationTarget::Node { address }, err))
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
                    Self::get_connection(route, core, None),
                )
                .await
            }
            CmdArg::ClusterScan {
                cluster_scan_args, ..
            } => {
                let core = core;
                let scan_result = cluster_scan(core, cluster_scan_args).await;
                match scan_result {
                    Ok((scan_state_ref, values)) => {
                        Ok(Response::ClusterScanResult(scan_state_ref, values))
                    }
                    // TODO: After routing issues with sending to random node on not-key based commands are resolved,
                    // this error should be handled in the same way as other errors and not fan-out.
                    Err(err) => Err((OperationTarget::FanOut, err)),
                }
            }
        }
    }

    async fn get_connection(
        routing: InternalSingleNodeRouting<C>,
        core: Core<C>,
        cmd: Option<Arc<Cmd>>,
    ) -> RedisResult<(ArcStr, C)> {
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
            InternalSingleNodeRouting::SpecificNode(route) => {
                match read_guard.connection_for_route(&route) {
                    Some((conn, address)) => ConnectionCheck::Found((conn, address)),
                    None => {
                        // No connection is found for the given route:
                        // - For key-based commands, attempt redirection to a random node,
                        //   hopefully to be redirected afterwards by a MOVED error.
                        // - For non-key-based commands, avoid attempting redirection to a random node
                        //   as it wouldn't result in MOVED hints and can lead to unwanted results
                        //   (e.g., sending management command to a different node than the user asked for); instead, raise the error.
                        let routable_cmd = cmd.and_then(|cmd| Routable::command(&*cmd));
                        if routable_cmd.is_some()
                            && !RoutingInfo::is_key_based_cmd(&routable_cmd.unwrap())
                        {
                            return Err((
                                ErrorKind::ClusterConnectionNotFound,
                                "Requested connection not found for route",
                                format!("{route:?}"),
                            )
                                .into());
                        } else {
                            warn!("No connection found for route `{route:?}`. Attempting redirection to a random node.");
                            ConnectionCheck::RandomConnection
                        }
                    }
                }
            }
            InternalSingleNodeRouting::Random => ConnectionCheck::RandomConnection,
            InternalSingleNodeRouting::Connection { address, conn } => {
                return Ok((address, conn.await));
            }
            InternalSingleNodeRouting::ByAddress(address) => {
                if let Some((address, conn)) = read_guard.connection_for_address(&address) {
                    return Ok((address, conn.await));
                } else {
                    return Err((
                        ErrorKind::ClusterConnectionNotFound,
                        "Requested connection not found",
                        address,
                    )
                        .into());
                }
            }
        };
        drop(read_guard);

        let (address, mut conn) = match conn_check {
            ConnectionCheck::Found((address, connection)) => (address, connection.await),
            ConnectionCheck::OnlyAddress(addr) => {
                let mut this_conn_params = core.cluster_params.clone();
                let subs_guard = core.subscriptions_by_address.read().await;
                this_conn_params.pubsub_subscriptions = subs_guard.get(addr.as_str()).cloned();
                drop(subs_guard);
                match connect_and_check::<C>(
                    &addr,
                    this_conn_params,
                    None,
                    RefreshConnectionType::AllConnections,
                    None,
                    core.push_sender.clone(),
                )
                .await
                .get_node()
                {
                    Ok(node) => {
                        let connection_clone = node.user_connection.conn.clone().await;
                        let mut connections = core.conn_lock.write().await;
                        let address = connections.replace_or_add_connection_for_address(addr, node);
                        drop(connections);
                        (address, connection_clone)
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
            ConnectionCheck::RandomConnection => {
                let read_guard = core.conn_lock.read().await;
                let (random_address, random_conn_future) = read_guard
                    .random_connections(1, ConnectionType::User)
                    .next()
                    .ok_or(RedisError::from((
                        ErrorKind::ClusterConnectionNotFound,
                        "No random connection found",
                    )))?;
                return Ok((random_address, random_conn_future.await));
            }
        };

        if asking {
            let _ = conn.req_packed_command(&crate::cmd::cmd("ASKING")).await;
        }
        Ok((address, conn))
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
                    *future = Box::pin(Self::refresh_slots_and_subscriptions_with_retries(
                        self.inner.clone(),
                        &RefreshPolicy::Throttable,
                    ));
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

    async fn handle_loading_error(
        core: Core<C>,
        info: RequestInfo<C>,
        address: ArcStr,
        retry: u32,
    ) -> OperationResult {
        let is_primary = core.conn_lock.read().await.is_primary(&address);

        if !is_primary {
            // If the connection is a replica, remove the connection and retry.
            // The connection will be established again on the next call to refresh slots once the replica is no longer in loading state.
            core.conn_lock.write().await.remove_node(&address);
        } else {
            // If the connection is primary, just sleep and retry
            let sleep_duration = core.cluster_params.retry_params.wait_time_for_retry(retry);
            boxed_sleep(sleep_duration).await;
        }

        Self::try_request(info, core).await
    }

    fn poll_complete(&mut self, cx: &mut task::Context<'_>) -> Poll<PollFlushAction> {
        let mut poll_flush_action = PollFlushAction::None;

        let mut pending_requests_guard = self.inner.pending_requests.lock().unwrap();
        if !pending_requests_guard.is_empty() {
            let mut pending_requests = mem::take(&mut *pending_requests_guard);
            for request in pending_requests.drain(..) {
                // Drop the request if none is waiting for a response to free up resources for
                // requests callers care about (load shedding). It will be ambiguous whether the
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
                Next::RetryBusyLoadingError { request, address } => {
                    // TODO - do we also want to try and reconnect to replica if it is loading?
                    let future = Self::handle_loading_error(
                        self.inner.clone(),
                        request.info.clone(),
                        address,
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
                Next::RefreshSlots {
                    request,
                    sleep_duration,
                } => {
                    poll_flush_action =
                        poll_flush_action.change_state(PollFlushAction::RebuildSlots);
                    if let Some(request) = request {
                        let future: RequestState<
                            Pin<Box<dyn Future<Output = OperationResult> + Send>>,
                        > = match sleep_duration {
                            Some(sleep_duration) => RequestState::Sleep {
                                sleep: boxed_sleep(sleep_duration),
                            },
                            None => RequestState::Future {
                                future: Box::pin(Self::try_request(
                                    request.info.clone(),
                                    self.inner.clone(),
                                )),
                            },
                        };
                        self.in_flight_requests.push(Box::pin(Request {
                            retry_params: self.inner.cluster_params.retry_params.clone(),
                            request: Some(request),
                            future,
                        }));
                    }
                }
                Next::Reconnect {
                    request, target, ..
                } => {
                    poll_flush_action =
                        poll_flush_action.change_state(PollFlushAction::Reconnect(vec![target]));
                    if let Some(request) = request {
                        self.inner.pending_requests.lock().unwrap().push(request);
                    }
                }
                Next::ReconnectToInitialNodes { request } => {
                    poll_flush_action = poll_flush_action
                        .change_state(PollFlushAction::ReconnectFromInitialConnections);
                    if let Some(request) = request {
                        self.inner.pending_requests.lock().unwrap().push(request);
                    }
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
    Reconnect(Vec<ArcStr>),
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

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut task::Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
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
                        ClusterConnInner::refresh_slots_and_subscriptions_with_retries(
                            self.inner.clone(),
                            &RefreshPolicy::Throttable,
                        ),
                    )));
                }
                PollFlushAction::Reconnect(addresses) => {
                    self.state = ConnectionState::Recover(RecoverFuture::Reconnect(Box::pin(
                        ClusterConnInner::refresh_connections(
                            self.inner.clone(),
                            addresses,
                            RefreshConnectionType::OnlyUserConnection,
                        ),
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

async fn calculate_topology_from_random_nodes<'a, C>(
    inner: &Core<C>,
    num_of_nodes_to_query: usize,
    read_guard: &tokio::sync::RwLockReadGuard<'a, ConnectionsContainer<C>>,
    curr_retry: usize,
) -> (
    RedisResult<(
        crate::cluster_slotmap::SlotMap,
        crate::cluster_topology::TopologyHash,
    )>,
    Vec<ArcStr>,
)
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    let requested_nodes =
        read_guard.random_connections(num_of_nodes_to_query, ConnectionType::PreferManagement);
    let topology_join_results =
        futures::future::join_all(requested_nodes.map(|(addr, conn)| async move {
            let mut conn: C = conn.await;
            let res = conn.req_packed_command(&slot_cmd()).await;
            (addr, res)
        }))
        .await;
    let failed_addresses = topology_join_results
        .iter()
        .filter_map(|(address, res)| match res {
            Err(err) if err.is_unrecoverable_error() => Some(address.clone()),
            _ => None,
        })
        .collect();
    let topology_values = topology_join_results.iter().filter_map(|(addr, res)| {
        res.as_ref()
            .ok()
            .and_then(|value| get_host_and_port_from_addr(addr).map(|(host, _)| (host, value)))
    });
    (
        calculate_topology(
            topology_values,
            curr_retry,
            inner.cluster_params.tls,
            num_of_nodes_to_query,
            inner.cluster_params.read_from_replicas,
        ),
        failed_addresses,
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
        push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
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
        push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
    ) -> RedisFuture<'a, (MultiplexedConnection, Option<IpAddr>)>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        async move {
            let connection_info = info.into_connection_info()?;
            let client = crate::Client::open(connection_info)?;

            match Runtime::locate() {
                #[cfg(feature = "tokio-comp")]
                rt @ Runtime::Tokio => {
                    rt.timeout(
                        connection_timeout,
                        client.get_multiplexed_async_connection_inner::<crate::aio::tokio::Tokio>(
                            response_timeout,
                            socket_addr,
                            push_sender,
                        ),
                    )
                    .await?
                }
                #[cfg(feature = "async-std-comp")]
                rt @ Runtime::AsyncStd => {
                    rt.timeout(connection_timeout,client
                        .get_multiplexed_async_connection_inner::<crate::aio::async_std::AsyncStd>(
                            response_timeout,
                            socket_addr,
                            push_sender,
                        ))
                        .await?
                }
            }
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
