//! This module extends the library to support Redis Cluster.
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
//! # Example
//! ```rust,no_run
//! use redis::TypedCommands;
//! use redis::cluster::ClusterClient;
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let client = ClusterClient::new(nodes).unwrap();
//! let mut connection = client.get_connection().unwrap();
//!
//! connection.set("test", "test_data").unwrap();
//! let rv = connection.get("test").unwrap().unwrap();
//!
//! assert_eq!(rv.as_str(), "test_data");
//! ```
//!
//! # Pipelining
//! ```rust,no_run
//! use redis::TypedCommands;
//! use redis::cluster::{cluster_pipe, ClusterClient};
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let client = ClusterClient::new(nodes).unwrap();
//! let mut connection = client.get_connection().unwrap();
//!
//! let key = "test";
//!
//! cluster_pipe()
//!     .rpush(key, "123").ignore()
//!     .ltrim(key, -10, -1).ignore()
//!     .expire(key, 60).ignore()
//!     .exec(&mut connection).unwrap();
//! ```
//!
//! # Sending request to specific node
//! In some cases you'd want to send a request to a specific node in the cluster, instead of
//! letting the cluster connection decide by itself to which node it should send the request.
//! This can happen, for example, if you want to send SCAN commands to each node in the cluster.
//!
//! ```rust,no_run
//! use redis::Commands;
//! use redis::cluster::ClusterClient;
//! use redis::cluster_routing::{ RoutingInfo, SingleNodeRoutingInfo };
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let client = ClusterClient::new(nodes).unwrap();
//! let mut connection = client.get_connection().unwrap();
//!
//! let routing_info = RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress{
//!     host: "redis://127.0.0.1".to_string(),
//!     port: 6378
//! });
//! let _: redis::Value = connection.route_command(&redis::cmd("PING"), routing_info).unwrap();
//! ```
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use arcstr::ArcStr;

mod pipeline;

pub use super::NodeAddress;
pub use super::client::{ClusterClient, ClusterClientBuilder};
use super::topology::{
    parse_cluster_shards_availability_zones, parse_hostname_availability_zone,
    parse_info_server_availability_zone, parse_slots,
};
use super::{
    client::ClusterParams,
    read_routing::{
        NodeAvailabilityZoneCoverage, NodeAvailabilityZoneDiscoveryCache,
        NodeAvailabilityZoneDiscoveryMethod, NodeAvailabilityZoneDiscoveryState,
        ReadRoutingStrategy,
    },
    routing::{Redirect, Route, RoutingInfo},
    slot_map::SlotMap,
};
use crate::IntoConnectionInfo;
pub use crate::TlsMode; // Pub for backwards compatibility
use crate::cluster_handling::{get_connection_info, info_server_cmd, shard_cmd, slot_cmd};
use crate::cluster_routing::{
    MultipleNodeRoutingInfo, ResponsePolicy, Routable, SingleNodeRoutingInfo, Slot, SlotAddr,
};
use crate::cmd::{Cmd, cmd};
use crate::connection::{Connection, ConnectionInfo, ConnectionLike, connect};
use crate::errors::{ErrorKind, RedisError, RetryMethod};
use crate::parser::parse_redis_value;
use crate::types::{HashMap, RedisResult, Value};
use pipeline::UNROUTABLE_ERROR;
use rand::{Rng, rng, seq::IteratorRandom};

pub use pipeline::{ClusterPipeline, cluster_pipe};

#[derive(Clone)]
enum Input<'a> {
    Slice {
        cmd: &'a [u8],
        routable: Value,
    },
    Cmd(&'a Cmd),
    Commands {
        cmd: &'a [u8],
        offset: usize,
        count: usize,
    },
}

impl<'a> Input<'a> {
    fn send(&'a self, connection: &mut impl ConnectionLike) -> RedisResult<Output> {
        match self {
            Input::Slice { cmd, routable: _ } => connection
                .req_packed_command(cmd)
                .and_then(|value| value.extract_error())
                .map(Output::Single),
            Input::Cmd(cmd) => connection
                .req_command(cmd)
                .and_then(|value| value.extract_error())
                .map(Output::Single),
            Input::Commands { cmd, offset, count } => connection
                .req_packed_commands(cmd, *offset, *count)
                .and_then(Value::extract_error_vec)
                .map(Output::Multi),
        }
    }
}

impl Routable for Input<'_> {
    fn arg_idx(&self, idx: usize) -> Option<&[u8]> {
        match self {
            Input::Slice { cmd: _, routable } => routable.arg_idx(idx),
            Input::Cmd(cmd) => cmd.arg_idx(idx),
            Input::Commands { .. } => None,
        }
    }

    fn position(&self, candidate: &[u8]) -> Option<usize> {
        match self {
            Input::Slice { cmd: _, routable } => routable.position(candidate),
            Input::Cmd(cmd) => cmd.position(candidate),
            Input::Commands { .. } => None,
        }
    }
}

enum Output {
    Single(Value),
    Multi(Vec<Value>),
}

impl From<Output> for Value {
    fn from(value: Output) -> Self {
        match value {
            Output::Single(value) => value,
            Output::Multi(values) => Value::Array(values),
        }
    }
}

impl From<Output> for Vec<Value> {
    fn from(value: Output) -> Self {
        match value {
            Output::Single(value) => vec![value],
            Output::Multi(values) => values,
        }
    }
}

/// Implements the process of connecting to a Redis server
/// and obtaining and configuring a connection handle.
pub trait Connect: Sized {
    /// Connect to a node, returning handle for command execution.
    fn connect<T>(info: T, timeout: Option<Duration>) -> RedisResult<Self>
    where
        T: IntoConnectionInfo;

    /// Sends an already encoded (packed) command into the TCP socket and
    /// does not read a response.  This is useful for commands like
    /// `MONITOR` which yield multiple items.  This needs to be used with
    /// care because it changes the state of the connection.
    fn send_packed_command(&mut self, cmd: &[u8]) -> RedisResult<()>;

    /// Sets the write timeout for the connection.
    ///
    /// If the provided value is `None`, then `send_packed_command` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()>;

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `recv_response` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()>;

    /// Fetches a single response from the connection.  This is useful
    /// if used in combination with `send_packed_command`.
    fn recv_response(&mut self) -> RedisResult<Value>;
}

impl Connect for Connection {
    fn connect<T>(info: T, timeout: Option<Duration>) -> RedisResult<Self>
    where
        T: IntoConnectionInfo,
    {
        connect(&info.into_connection_info()?, timeout)
    }

    fn send_packed_command(&mut self, cmd: &[u8]) -> RedisResult<()> {
        Self::send_packed_command(self, cmd)
    }

    fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        Self::set_write_timeout(self, dur)
    }

    fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        Self::set_read_timeout(self, dur)
    }

    fn recv_response(&mut self) -> RedisResult<Value> {
        Self::recv_response(self)
    }
}

/// Options for creation of connection
#[derive(Clone, Default)]
pub struct ClusterConfig {
    pub(crate) connection_timeout: Option<Duration>,
    pub(crate) response_timeout: Option<Duration>,
    pub(crate) client_name_factory: Option<Arc<super::client::ClientNameFactory>>,
    #[cfg(feature = "cluster-async")]
    pub(crate) async_push_sender: Option<std::sync::Arc<dyn crate::aio::AsyncPushSender>>,
    #[cfg(feature = "cluster-async")]
    pub(crate) async_dns_resolver: Option<std::sync::Arc<dyn crate::io::AsyncDNSResolver>>,
}

impl ClusterConfig {
    /// Creates a new instance of the options with nothing set
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the connection timeout
    pub fn set_connection_timeout(mut self, connection_timeout: std::time::Duration) -> Self {
        self.connection_timeout = Some(connection_timeout);
        self
    }

    /// Sets the response timeout
    pub fn set_response_timeout(mut self, response_timeout: std::time::Duration) -> Self {
        self.response_timeout = Some(response_timeout);
        self
    }

    /// Sets a factory that builds a Redis client name for each cluster node
    /// connection opened by this `ClusterConnection`.
    ///
    /// The name is sent with `CLIENT SETNAME` after the connection enters
    /// cluster-read mode. Errors are ignored so naming never prevents a usable
    /// connection from being established.
    pub fn set_client_name_factory(
        mut self,
        factory: impl Fn(&NodeAddress) -> String + Send + Sync + 'static,
    ) -> Self {
        self.client_name_factory = Some(Arc::new(factory));
        self
    }

    #[cfg(feature = "cluster-async")]
    /// Sets a sender to receive pushed values.
    ///
    /// The sender can be a channel, or an arbitrary function that handles [crate::PushInfo] values.
    /// This will fail client creation if the connection isn't configured for RESP3 communications via the [crate::RedisConnectionInfo::set_protocol] function.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use redis::cluster::ClusterConfig;
    /// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    /// let config = ClusterConfig::new().set_push_sender(tx);
    /// ```
    ///
    /// ```rust
    /// # use std::sync::{Mutex, Arc};
    /// # use redis::cluster::ClusterConfig;
    /// let messages = Arc::new(Mutex::new(Vec::new()));
    /// let config = ClusterConfig::new().set_push_sender(move |msg|{
    ///     let Ok(mut messages) = messages.lock() else {
    ///         return Err(redis::aio::SendError);
    ///     };
    ///     messages.push(msg);
    ///     Ok(())
    /// });
    pub fn set_push_sender(mut self, sender: impl crate::aio::AsyncPushSender) -> Self {
        self.async_push_sender = Some(std::sync::Arc::new(sender));
        self
    }

    /// Set asynchronous DNS resolver for the underlying TCP connection.
    ///
    /// The parameter resolver must implement the [`crate::io::AsyncDNSResolver`] trait.
    #[cfg(feature = "cluster-async")]
    pub fn set_dns_resolver(mut self, resolver: impl crate::io::AsyncDNSResolver) -> Self {
        self.async_dns_resolver = Some(std::sync::Arc::new(resolver));
        self
    }
}

/// This represents a Redis Cluster connection.
///
/// It stores the underlying connections maintained for each node in the cluster,
/// as well as common parameters for connecting to nodes and executing commands.
pub struct ClusterConnection<C = Connection> {
    initial_nodes: Vec<ConnectionInfo>,
    connections: RefCell<HashMap<NodeAddress, C>>,
    slots: RefCell<SlotMap>,
    auto_reconnect: RefCell<bool>,
    read_timeout: RefCell<Option<Duration>>,
    write_timeout: RefCell<Option<Duration>>,
    routing_strategy: Option<Box<dyn ReadRoutingStrategy>>,
    read_fallback_enabled: bool,
    az_discovery: Arc<NodeAvailabilityZoneDiscoveryState>,
    /// Earliest instant at which a rate-limited caller may refetch the whole slot
    /// map. `None` means one is allowed now. See
    /// [`Self::refresh_slots_rate_limited`].
    next_slot_refresh: RefCell<Option<Instant>>,
    cluster_params: ClusterParams,
}

impl<C> ClusterConnection<C> {
    /// Returns the node addresses that currently have open connections.
    ///
    /// This method is intended for diagnostics and tests. The result is a
    /// moment-in-time snapshot and should not be used for routing decisions.
    #[doc(hidden)]
    pub fn connected_node_addresses(&self) -> Vec<NodeAddress> {
        let mut addrs = self
            .connections
            .borrow()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        addrs.sort_unstable();
        addrs
    }

    /// Returns the number of node connections currently held by this cluster connection.
    #[doc(hidden)]
    pub fn connected_node_count(&self) -> usize {
        self.connections.borrow().len()
    }
}

/// Upper bound on the number of worker threads used to open node connections
/// concurrently during a slot/connection refresh. Connecting is network-bound,
/// so this is sized to cover typical clusters in a single wave while still
/// capping the thread and file-descriptor burst for unusually large topologies.
const MAX_CONCURRENT_CONNECTS: usize = 256;

impl<C> ClusterConnection<C>
where
    // `Send` is required so connections can be opened on worker threads during
    // parallel connection refresh (see `refresh_slots`).
    C: ConnectionLike + Connect + Send,
{
    pub(crate) fn new(
        cluster_params: ClusterParams,
        initial_nodes: Vec<ConnectionInfo>,
    ) -> RedisResult<Self> {
        let routing_strategy = cluster_params
            .read_routing_factory
            .as_ref()
            .map(|f| f.create_strategy());
        let read_fallback_enabled = routing_strategy
            .as_deref()
            .is_some_and(ReadRoutingStrategy::supports_read_fallback);

        let connection = Self {
            connections: RefCell::new(HashMap::new()),
            slots: RefCell::new(SlotMap::new()),
            auto_reconnect: RefCell::new(true),
            read_timeout: RefCell::new(cluster_params.response_timeout),
            write_timeout: RefCell::new(None),
            routing_strategy,
            read_fallback_enabled,
            az_discovery: Arc::new(NodeAvailabilityZoneDiscoveryState::default()),
            next_slot_refresh: RefCell::new(None),
            initial_nodes: initial_nodes.to_vec(),
            cluster_params,
        };
        connection.create_initial_connections()?;

        Ok(connection)
    }

    /// Set an auto reconnect attribute.
    /// Default value is true;
    pub fn set_auto_reconnect(&self, value: bool) {
        let mut auto_reconnect = self.auto_reconnect.borrow_mut();
        *auto_reconnect = value;
    }

    /// Sets the write timeout for the connection.
    ///
    /// If the provided value is `None`, then `send_packed_command` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        // Check if duration is valid before updating local value.
        if dur.is_some() && dur.unwrap().is_zero() {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "Duration should be None or non-zero.",
            )));
        }

        let mut t = self.write_timeout.borrow_mut();
        *t = dur;
        let connections = self.connections.borrow();
        for conn in connections.values() {
            conn.set_write_timeout(dur)?;
        }
        Ok(())
    }

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `recv_response` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        // Check if duration is valid before updating local value.
        if dur.is_some() && dur.unwrap().is_zero() {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "Duration should be None or non-zero.",
            )));
        }

        let mut t = self.read_timeout.borrow_mut();
        *t = dur;
        let connections = self.connections.borrow();
        for conn in connections.values() {
            conn.set_read_timeout(dur)?;
        }
        Ok(())
    }

    /// Check that all connections it has are available (`PING` internally).
    #[doc(hidden)]
    pub fn check_connection(&mut self) -> bool {
        <Self as ConnectionLike>::check_connection(self)
    }

    pub(crate) fn execute_pipeline(&mut self, pipe: &ClusterPipeline) -> RedisResult<Vec<Value>> {
        self.send_recv_and_retry_cmds(pipe.commands())
    }

    /// Returns the connection status.
    ///
    /// The connection is open until any `read_response` call received an
    /// invalid response from the server (most likely a closed or dropped
    /// connection, otherwise a Redis protocol error). When using unix
    /// sockets the connection is open until writing a command failed with a
    /// `BrokenPipe` error.
    fn create_initial_connections(&self) -> RedisResult<()> {
        let mut connections = HashMap::with_capacity(self.initial_nodes.len());
        let mut failed_connections = Vec::new();

        for info in self.initial_nodes.iter() {
            let addr = NodeAddress::try_from(&info.addr)?;

            match self.connect(&addr) {
                Ok(mut conn) => {
                    if conn.check_connection() {
                        connections.insert(addr, conn);
                        break;
                    } else {
                        failed_connections.push((
                            addr,
                            RedisError::from((
                                ErrorKind::Io,
                                "Node failed to respond to connection check,",
                            )),
                        ));
                    }
                }
                Err(conn_err) => {
                    failed_connections.push((addr, conn_err));
                }
            }
        }

        if connections.is_empty() {
            // Create a composite description of why connecting to each node failed.
            let detail = if failed_connections.is_empty() {
                "List of initial nodes is empty".to_string()
            } else {
                let mut formatted_detail = "Failed to connect to each cluster node (".to_string();

                for (index, (addr, conn_err)) in failed_connections.into_iter().enumerate() {
                    if index != 0 {
                        formatted_detail += "; ";
                    }
                    use std::fmt::Write;
                    let _ = write!(&mut formatted_detail, "{addr}: {conn_err}");
                }
                formatted_detail += ")";
                formatted_detail
            };

            return Err(RedisError::from((
                ErrorKind::Io,
                "It failed to check startup nodes.",
                detail,
            )));
        }

        *self.connections.borrow_mut() = connections;
        self.refresh_slots()?;
        Ok(())
    }

    /// Refreshes the whole slot map, unless one was refreshed recently.
    ///
    /// Only for callers that have another way to reach the right node — a MOVED
    /// whose hint has already been recorded by [`SlotMap::update_slot`], or a
    /// retry that will follow its own redirect. For those, refetching all 16384
    /// mappings is not what makes the request correct; it only refreshes replica
    /// sets, picks up nodes joining or leaving, and compacts a map that
    /// incremental updates have split. Skipping it costs at worst one extra
    /// redirect on some *other* slot that also moved, which is far cheaper than
    /// the `CLUSTER SLOTS` it saves on a fragmented cluster.
    ///
    /// Callers with no other recovery path must use [`Self::refresh_slots`].
    fn refresh_slots_rate_limited(&self) -> RedisResult<()> {
        if !self.cluster_params.min_slot_refresh_interval.is_zero() {
            if let Some(next) = *self.next_slot_refresh.borrow() {
                if Instant::now() < next {
                    return Ok(());
                }
            }
        }
        self.refresh_slots()
    }

    /// Arms the deadline checked by [`Self::refresh_slots_rate_limited`].
    ///
    /// Every full refresh arms it, whatever triggered it, because the thing being
    /// rate-limited is refetching the whole map — a MOVED arriving just after a
    /// reconnect-driven refresh has nothing new to learn. Arming it *before* the
    /// refresh runs also means a slow or failing refresh cannot be re-entered in a
    /// tight loop by the redirects that arrive while it is in flight.
    fn arm_slot_refresh_deadline(&self) {
        let interval = self.cluster_params.min_slot_refresh_interval;
        if interval.is_zero() {
            return;
        }
        // Jittered because pooled connections are typically constructed together:
        // an exact interval keeps their deadlines in phase and turns a steady
        // trickle of refreshes into a synchronised burst.
        let jitter = rng().random_range(0.5..1.5);
        *self.next_slot_refresh.borrow_mut() = Some(Instant::now() + interval.mul_f64(jitter));
    }

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(&self) -> RedisResult<()> {
        self.arm_slot_refresh_deadline();
        let mut new_slots = self.create_new_slots()?;

        if let Some(ref strategy) = self.routing_strategy {
            if strategy.requires_node_availability_zones() {
                let zones = self.discover_node_availability_zones(strategy.as_ref(), &new_slots);
                new_slots.set_node_availability_zones(zones);
            }

            let topology = new_slots.topology();
            strategy.on_topology_changed(topology.clone());

            let mut nodes = strategy
                .eager_connection_nodes(&topology)
                .unwrap_or_else(|| new_slots.values().flatten().cloned().collect());
            let mut nodes = nodes.drain().collect::<Vec<_>>();
            nodes.sort_unstable();
            nodes.dedup();

            *self.slots.borrow_mut() = new_slots;
            self.refresh_connections(nodes);
            return Ok(());
        }

        let mut nodes = new_slots.values().flatten().cloned().collect::<Vec<_>>();
        nodes.sort_unstable();
        nodes.dedup();

        *self.slots.borrow_mut() = new_slots;
        self.refresh_connections(nodes);

        Ok(())
    }

    fn refresh_connections(&self, nodes: Vec<NodeAddress>) {
        let mut connections = self.connections.borrow_mut();

        // Build a work item per node, salvaging any existing connection so the
        // worker can try to reuse it before opening a fresh one.
        let mut work: Vec<(NodeAddress, Option<C>)> = nodes
            .into_iter()
            .map(|addr| {
                let connection = connections.remove(&addr);
                (addr, connection)
            })
            .collect();
        let node_count = work.len();

        // Opening connections is dominated by network round-trips (TCP connect +
        // READONLY + PING), and each node is independent. Doing them one-at-a-time
        // serializes a large cluster's startup into seconds; instead we fan the
        // work out across a bounded pool of scoped threads so the connects overlap.
        // Reading these out of their `RefCell`s up front lets the workers borrow
        // them without touching `&self`'s non-`Sync` interior.
        let read_timeout = *self.read_timeout.borrow();
        let write_timeout = *self.write_timeout.borrow();
        let cluster_params = &self.cluster_params;

        let mut refreshed: HashMap<NodeAddress, C> = HashMap::with_capacity(node_count);

        if node_count > 0 {
            // Cap concurrency so we don't spawn an unbounded thread/FD burst for
            // very large clusters; round-robin nodes into that many buckets.
            let worker_count = node_count.min(MAX_CONCURRENT_CONNECTS);
            let mut buckets: Vec<Vec<(NodeAddress, Option<C>)>> =
                (0..worker_count).map(|_| Vec::new()).collect();
            for (i, item) in work.drain(..).enumerate() {
                buckets[i % worker_count].push(item);
            }

            std::thread::scope(|scope| {
                let handles: Vec<_> = buckets
                    .into_iter()
                    .map(|bucket| {
                        scope.spawn(move || {
                            let mut opened = Vec::with_capacity(bucket.len());
                            for (addr, existing) in bucket {
                                // Reuse a still-healthy existing connection if we have one.
                                if let Some(mut conn) = existing {
                                    if conn.check_connection() {
                                        opened.push((addr, conn));
                                        continue;
                                    }
                                }

                                if let Ok(mut conn) = connect_node::<C>(
                                    &addr,
                                    cluster_params,
                                    read_timeout,
                                    write_timeout,
                                ) {
                                    if conn.check_connection() {
                                        opened.push((addr, conn));
                                    }
                                }
                            }
                            opened
                        })
                    })
                    .collect();

                for handle in handles {
                    // A panicking worker just drops its nodes; this matches the
                    // previous best-effort behavior of skipping unreachable nodes.
                    if let Ok(opened) = handle.join() {
                        refreshed.extend(opened);
                    }
                }
            });
        }

        *connections = refreshed;
        self.notify_connections_changed(&connections);
    }

    fn create_new_slots(&self) -> RedisResult<SlotMap> {
        let mut connections = self.connections.borrow_mut();
        let mut new_slots = None;

        for (addr, conn) in connections.iter_mut() {
            let value = conn.req_command(&slot_cmd())?;
            if let Ok(slots_data) = parse_slots(
                value,
                addr.host(),
                self.cluster_params.replica_filter.as_deref(),
            ) {
                new_slots = Some(SlotMap::from_slots(slots_data));
                break;
            }
        }

        match new_slots {
            Some(new_slots) => Ok(new_slots),
            None => Err(RedisError::from((
                ErrorKind::Client,
                "Slot refresh error. didn't get any slots from server",
            ))),
        }
    }

    fn discover_node_availability_zones(
        &self,
        strategy: &dyn ReadRoutingStrategy,
        slots: &SlotMap,
    ) -> std::collections::HashMap<NodeAddress, ArcStr> {
        let nodes = Self::nodes_for_availability_zone_discovery(slots);
        let cache = strategy
            .node_availability_zone_discovery_cache()
            .unwrap_or_else(|| self.az_discovery.clone());
        let mut best_zones = cache.cached_zones(&nodes);

        if Self::zones_cover_all_nodes(&best_zones, &nodes) {
            self.log_node_availability_zone_coverage(cache.as_ref(), best_zones.len(), nodes.len());
            return best_zones;
        }

        let preferred_method = cache.preferred_method();
        if let Some(method) = preferred_method {
            let mut zones = self.discover_node_availability_zones_with_method(method, slots);
            Self::retain_zones_for_nodes(&mut zones, &nodes);
            if Self::zones_cover_all_nodes(&zones, &nodes) {
                cache.record_success(method, &zones);
                self.log_node_availability_zone_coverage(cache.as_ref(), zones.len(), nodes.len());
                return zones;
            }
            best_zones.extend(zones);
            if Self::zones_cover_all_nodes(&best_zones, &nodes) {
                cache.update_zones(&best_zones);
                self.log_node_availability_zone_coverage(
                    cache.as_ref(),
                    best_zones.len(),
                    nodes.len(),
                );
                return best_zones;
            }
            cache.record_failure(method);
        }

        for method in NodeAvailabilityZoneDiscoveryMethod::ALL {
            if Some(method) == preferred_method {
                continue;
            }
            let mut zones = self.discover_node_availability_zones_with_method(method, slots);
            Self::retain_zones_for_nodes(&mut zones, &nodes);
            if Self::zones_cover_all_nodes(&zones, &nodes) {
                cache.record_success(method, &zones);
                self.log_node_availability_zone_coverage(cache.as_ref(), zones.len(), nodes.len());
                return zones;
            }
            best_zones.extend(zones);
            if Self::zones_cover_all_nodes(&best_zones, &nodes) {
                cache.update_zones(&best_zones);
                self.log_node_availability_zone_coverage(
                    cache.as_ref(),
                    best_zones.len(),
                    nodes.len(),
                );
                return best_zones;
            }
        }

        cache.update_zones(&best_zones);
        self.log_node_availability_zone_coverage(cache.as_ref(), best_zones.len(), nodes.len());
        best_zones
    }

    fn log_node_availability_zone_coverage(
        &self,
        cache: &dyn NodeAvailabilityZoneDiscoveryCache,
        known_nodes: usize,
        total_nodes: usize,
    ) {
        let coverage = NodeAvailabilityZoneCoverage::from_counts(known_nodes, total_nodes);
        if !cache.should_log_coverage(coverage) {
            return;
        }

        match coverage {
            NodeAvailabilityZoneCoverage::None => {
                log::info!(
                    "Zonal read routing requested, but no availability-zone metadata could be discovered for any of {total_nodes} cluster nodes using CLUSTER SHARDS, hostname parsing, or INFO SERVER; falling back to non-zonal replica routing until metadata is available"
                );
            }
            NodeAvailabilityZoneCoverage::Partial => {
                log::info!(
                    "Zonal read routing availability-zone metadata is incomplete ({known_nodes}/{total_nodes} cluster nodes); using known zone metadata where available and falling back to non-zonal replica routing for shards without a local replica"
                );
            }
            NodeAvailabilityZoneCoverage::Complete => {
                log::info!(
                    "Zonal read routing availability-zone metadata is complete ({known_nodes}/{total_nodes} cluster nodes); local replica preference is active for the current topology"
                );
            }
        }
    }

    fn discover_node_availability_zones_with_method(
        &self,
        method: NodeAvailabilityZoneDiscoveryMethod,
        slots: &SlotMap,
    ) -> std::collections::HashMap<NodeAddress, ArcStr> {
        match method {
            NodeAvailabilityZoneDiscoveryMethod::ClusterShards => {
                self.discover_node_availability_zones_with_cluster_shards()
            }
            NodeAvailabilityZoneDiscoveryMethod::InfoServer => {
                self.discover_node_availability_zones_with_info_server(slots)
            }
            NodeAvailabilityZoneDiscoveryMethod::Hostname => {
                self.discover_node_availability_zones_with_hostnames(slots)
            }
        }
    }

    fn nodes_for_availability_zone_discovery(slots: &SlotMap) -> Vec<NodeAddress> {
        let mut nodes = slots.values().flatten().cloned().collect::<Vec<_>>();
        nodes.sort_unstable();
        nodes.dedup();
        nodes
    }

    fn zones_cover_all_nodes(
        zones: &std::collections::HashMap<NodeAddress, ArcStr>,
        nodes: &[NodeAddress],
    ) -> bool {
        !nodes.is_empty() && nodes.iter().all(|node| zones.contains_key(node))
    }

    fn retain_zones_for_nodes(
        zones: &mut std::collections::HashMap<NodeAddress, ArcStr>,
        nodes: &[NodeAddress],
    ) {
        zones.retain(|node, _| nodes.binary_search(node).is_ok());
    }

    fn discover_node_availability_zones_with_cluster_shards(
        &self,
    ) -> std::collections::HashMap<NodeAddress, ArcStr> {
        let mut connections = self.connections.borrow_mut();

        for (addr, conn) in connections.iter_mut() {
            let value = conn
                .req_command(&shard_cmd())
                .and_then(|value| value.extract_error());
            if let Ok(value) = value {
                let zones = parse_cluster_shards_availability_zones(&value, addr.host());
                if !zones.is_empty() {
                    return zones;
                }
            }
        }

        std::collections::HashMap::new()
    }

    fn discover_node_availability_zones_with_info_server(
        &self,
        slots: &SlotMap,
    ) -> std::collections::HashMap<NodeAddress, ArcStr> {
        let mut zones = std::collections::HashMap::new();
        let mut nodes = slots.values().flatten().cloned().collect::<Vec<_>>();
        nodes.sort_unstable();
        nodes.dedup();

        for node in nodes {
            let zone = {
                let mut connections = self.connections.borrow_mut();
                connections
                    .get_mut(&node)
                    .and_then(Self::query_info_server_availability_zone)
            };

            let zone = match zone {
                Some(zone) => Some(zone),
                None => self
                    .connect(&node)
                    .ok()
                    .and_then(|mut conn| Self::query_info_server_availability_zone(&mut conn)),
            };

            if let Some(zone) = zone {
                zones.insert(node, zone);
            }
        }

        zones
    }

    fn query_info_server_availability_zone(conn: &mut C) -> Option<ArcStr> {
        let value = conn
            .req_command(&info_server_cmd())
            .and_then(|value| value.extract_error())
            .ok()?;
        let info = crate::from_redis_value::<String>(value).ok()?;
        parse_info_server_availability_zone(&info)
    }

    fn discover_node_availability_zones_with_hostnames(
        &self,
        slots: &SlotMap,
    ) -> std::collections::HashMap<NodeAddress, ArcStr> {
        slots
            .values()
            .flat_map(|slot| slot.into_iter())
            .filter_map(|addr| {
                parse_hostname_availability_zone(addr.host()).map(|zone| (addr.clone(), zone))
            })
            .collect()
    }

    fn connect(&self, node: &NodeAddress) -> RedisResult<C> {
        connect_node(
            node,
            &self.cluster_params,
            *self.read_timeout.borrow(),
            *self.write_timeout.borrow(),
        )
    }

    fn get_connection<'a>(
        &self,
        connections: &'a mut HashMap<NodeAddress, C>,
        route: &Route,
    ) -> (NodeAddress, RedisResult<&'a mut C>) {
        let slots = self.slots.borrow();
        if let Some(addr) = slots.slot_addr_for_route(route, self.routing_strategy.as_deref()) {
            (addr.clone(), self.get_connection_by_addr(connections, addr))
        } else {
            // try a random node next. This is safe if slots are involved
            // as a wrong node would reject the request.
            get_random_connection_or_error(connections)
        }
    }

    fn get_connection_excluding<'a>(
        &self,
        connections: &'a mut HashMap<NodeAddress, C>,
        route: &Route,
        excluded_nodes: &mut Vec<NodeAddress>,
        mut preferred_addr: Option<NodeAddress>,
    ) -> (NodeAddress, RedisResult<&'a mut C>) {
        if route.slot_addr() == SlotAddr::ReplicaRequired {
            return self.get_connection_excluding_slow(
                connections,
                route,
                excluded_nodes,
                preferred_addr,
            );
        }

        if excluded_nodes.is_empty() {
            let addr = preferred_addr.take().or_else(|| {
                self.slots
                    .borrow()
                    .slot_addr_for_route(route, self.routing_strategy.as_deref())
                    .cloned()
            });
            if let Some(addr) = addr {
                let connection = self.get_connection_by_addr(connections, &addr);
                return (addr, connection);
            }
        }

        self.get_connection_excluding_slow(connections, route, excluded_nodes, preferred_addr)
    }

    fn get_connection_excluding_slow<'a>(
        &self,
        connections: &'a mut HashMap<NodeAddress, C>,
        route: &Route,
        excluded_nodes: &mut Vec<NodeAddress>,
        mut preferred_addr: Option<NodeAddress>,
    ) -> (NodeAddress, RedisResult<&'a mut C>) {
        let mut last_failure = None;

        loop {
            let addr = {
                let slots = self.slots.borrow();
                preferred_addr
                    .take()
                    .filter(|addr| !excluded_nodes.contains(addr))
                    .or_else(|| {
                        slots
                            .slot_addr_for_route_excluding(
                                route,
                                self.routing_strategy.as_deref(),
                                excluded_nodes,
                            )
                            .cloned()
                    })
            };

            let Some(addr) = addr else {
                return match last_failure {
                    Some((addr, err)) => (addr, Err(err)),
                    None if excluded_nodes.is_empty() => {
                        get_random_connection_or_error(connections)
                    }
                    None => (
                        NodeAddress::default(),
                        Err(RedisError::from((
                            ErrorKind::ClusterConnectionNotFound,
                            "No eligible read connection found",
                        ))),
                    ),
                };
            };

            match self.ensure_connection_by_addr(connections, &addr) {
                Ok(()) => {
                    let return_addr = addr.clone();
                    return (
                        return_addr,
                        connections.get_mut(&addr).ok_or_else(|| {
                            RedisError::from((
                                ErrorKind::ClusterConnectionNotFound,
                                "Couldn't find connection",
                            ))
                        }),
                    );
                }
                Err(err) => {
                    if excluded_nodes.contains(&addr) {
                        return (addr, Err(err));
                    }
                    excluded_nodes.push(addr.clone());
                    last_failure = Some((addr, err));
                }
            }
        }
    }

    fn ensure_connection_by_addr(
        &self,
        connections: &mut HashMap<NodeAddress, C>,
        addr: &NodeAddress,
    ) -> RedisResult<()> {
        if connections.contains_key(addr) {
            return Ok(());
        }

        let conn = self.connect(addr)?;
        connections.insert(addr.clone(), conn);
        if let Some(strategy) = &self.routing_strategy {
            strategy.on_connection_added(addr);
        }
        Ok(())
    }

    fn notify_connections_changed(&self, connections: &HashMap<NodeAddress, C>) {
        if let Some(strategy) = &self.routing_strategy {
            let connected_nodes = connections.keys().cloned().collect();
            strategy.on_connections_changed(&connected_nodes);
        }
    }

    fn uses_read_fallback(&self, route: &Route) -> bool {
        self.read_fallback_enabled
            && matches!(
                route.slot_addr(),
                SlotAddr::ReplicaOptional | SlotAddr::ReplicaRequired
            )
    }

    fn note_read_node_failure(
        &self,
        routing: &SingleNodeRoutingInfo,
        addr: &NodeAddress,
        excluded_nodes: &mut Vec<NodeAddress>,
    ) {
        if let SingleNodeRoutingInfo::SpecificNode(route) = routing {
            if self.uses_read_fallback(route) && !excluded_nodes.contains(addr) {
                excluded_nodes.push(addr.clone());
            }
        }
    }

    fn get_connection_by_addr<'a>(
        &self,
        connections: &'a mut HashMap<NodeAddress, C>,
        addr: &NodeAddress,
    ) -> RedisResult<&'a mut C> {
        match connections.entry(addr.clone()) {
            std::collections::hash_map::Entry::Occupied(occupied_entry) => {
                Ok(occupied_entry.into_mut())
            }
            std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                // Create new connection.
                // TODO: error handling
                let conn = self.connect(addr)?;
                let connection = vacant_entry.insert(conn);
                if let Some(strategy) = &self.routing_strategy {
                    strategy.on_connection_added(addr);
                }
                Ok(connection)
            }
        }
    }

    fn get_addr_for_cmd(&self, cmd: &Cmd) -> RedisResult<NodeAddress> {
        let slots = self.slots.borrow();

        let addr_for_slot = |route: Route| -> RedisResult<NodeAddress> {
            let slot_addr = slots
                .slot_addr_for_route(&route, self.routing_strategy.as_deref())
                .ok_or((ErrorKind::Client, "Missing slot coverage"))?;
            Ok(slot_addr.clone())
        };

        match RoutingInfo::for_routable(cmd) {
            Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random)) => Ok(addr_for_slot(
                Route::with_slot(Slot::new_random(), SlotAddr::Master),
            )?),
            Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(route))) => {
                Ok(addr_for_slot(route)?)
            }
            _ => fail!(UNROUTABLE_ERROR),
        }
    }

    fn map_cmds_to_nodes(&self, cmds: &[Cmd]) -> RedisResult<Vec<NodeCmd>> {
        let mut cmd_map: HashMap<NodeAddress, NodeCmd> = HashMap::new();

        for (idx, cmd) in cmds.iter().enumerate() {
            let addr = self.get_addr_for_cmd(cmd)?;
            let nc = cmd_map
                .entry(addr.clone())
                .or_insert_with(|| NodeCmd::new(addr));
            nc.indexes.push(idx);
            cmd.write_packed_command(&mut nc.pipe);
        }

        let mut result = Vec::new();
        for (_, v) in cmd_map.drain() {
            result.push(v);
        }
        Ok(result)
    }

    fn execute_on_all(
        &self,
        input: Input,
        addresses: HashSet<NodeAddress>,
    ) -> Vec<RedisResult<(NodeAddress, Value)>> {
        addresses
            .into_iter()
            .map(|addr| {
                self.request(
                    input.clone(),
                    Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                        host: addr.host().to_string(),
                        port: addr.port(),
                    })),
                )
                .map(|res| match res {
                    Output::Single(value) => (addr, value),
                    // technically this shouldn't be possible, but I prefer not to crash here.
                    Output::Multi(values) => (addr, Value::Array(values)),
                })
            })
            .collect()
    }

    fn execute_on_all_nodes(&self, input: Input) -> Vec<RedisResult<(NodeAddress, Value)>> {
        let preserved_connections = self.routing_strategy.as_ref().map(|_| {
            self.connections
                .borrow()
                .keys()
                .cloned()
                .collect::<HashSet<_>>()
        });
        let addresses = self
            .slots
            .borrow()
            .addresses_for_all_nodes()
            .into_iter()
            .cloned()
            .collect();
        let results = self.execute_on_all(input, addresses);
        if let Some(preserved_connections) = preserved_connections {
            self.prune_to_eager_connections(&preserved_connections);
        }
        results
    }

    fn prune_to_eager_connections(&self, preserved_connections: &HashSet<NodeAddress>) {
        let Some(strategy) = &self.routing_strategy else {
            return;
        };
        let topology = self.slots.borrow().topology();
        let Some(eager_nodes) = strategy.eager_connection_nodes(&topology) else {
            return;
        };
        let mut connections = self.connections.borrow_mut();
        connections.retain(|address, _| {
            eager_nodes.contains(address) || preserved_connections.contains(address)
        });
        self.notify_connections_changed(&connections);
    }

    fn execute_on_all_primaries(&self, input: Input) -> Vec<RedisResult<(NodeAddress, Value)>> {
        let addresses = self
            .slots
            .borrow()
            .addresses_for_all_primaries()
            .into_iter()
            .cloned()
            .collect();
        self.execute_on_all(input, addresses)
    }

    fn execute_multi_slot(
        &self,
        input: Input,
        routes: &[(Route, Vec<usize>)],
    ) -> Vec<RedisResult<(NodeAddress, Value)>> {
        routes
            .iter()
            .map(|(route, indices)| {
                let addr = self
                    .slots
                    .borrow()
                    .slot_addr_for_route(route, self.routing_strategy.as_deref())
                    .cloned()
                    .ok_or(RedisError::from((
                        ErrorKind::Io,
                        "Couldn't find connection",
                    )))?;
                let cmd =
                    crate::cluster_routing::command_for_multi_slot_indices(&input, indices.iter());
                let response = if self.uses_read_fallback(route) {
                    let routing = Some(RoutingInfo::SingleNode(
                        SingleNodeRoutingInfo::SpecificNode(*route),
                    ));
                    self.request_with_preferred_addr(Input::Cmd(&cmd), routing, addr.clone())
                } else {
                    // Stateful strategies must dispatch the address selected above instead of
                    // resolving the route a second time.
                    let mut connections = self.connections.borrow_mut();
                    self.get_connection_by_addr(&mut connections, &addr)
                        .and_then(|connection| Input::Cmd(&cmd).send(connection))
                };
                response.map(|res| match res {
                    Output::Single(value) => (addr, value),
                    Output::Multi(values) => (addr, Value::Array(values)),
                })
            })
            .collect()
    }

    fn execute_on_multiple_nodes(
        &self,
        input: Input,
        routing: MultipleNodeRoutingInfo,
        response_policy: Option<ResponsePolicy>,
    ) -> RedisResult<Value> {
        let results = match &routing {
            MultipleNodeRoutingInfo::MultiSlot((routes, _)) => {
                self.execute_multi_slot(input, routes)
            }
            MultipleNodeRoutingInfo::AllMasters => self.execute_on_all_primaries(input),
            MultipleNodeRoutingInfo::AllNodes => self.execute_on_all_nodes(input),
        };

        match response_policy {
            Some(ResponsePolicy::AllSucceeded) => {
                let mut last_result = None;
                for result in results {
                    last_result = Some(result?);
                }

                last_result
                    .ok_or(
                        (
                            ErrorKind::ClusterConnectionNotFound,
                            "No results received for multi-node operation",
                        )
                            .into(),
                    )
                    .map(|(_, res)| res)
            }
            Some(ResponsePolicy::OneSucceeded) => {
                let mut last_failure = None;

                for result in results {
                    match result {
                        Ok((_, val)) => return Ok(val),
                        Err(err) => last_failure = Some(err),
                    }
                }

                Err(last_failure
                    .unwrap_or_else(|| (ErrorKind::Io, "Couldn't find a connection").into()))
            }
            Some(ResponsePolicy::CombineMaps) => crate::cluster_routing::combine_map_results(
                results
                    .into_iter()
                    .map(|result| result.map(|(_, value)| value))
                    .collect::<RedisResult<Vec<_>>>()?,
            ),
            Some(ResponsePolicy::FirstSucceededNonEmptyOrAllEmpty) => {
                // Attempt to return the first result that isn't `Nil` or an error.
                // If no such response is found and all servers returned `Nil`, it indicates that all shards are empty, so return `Nil`.
                // If we received only errors, return the last received error.
                // If we received a mix of errors and `Nil`s, we can't determine if all shards are empty,
                // thus we return the last received error instead of `Nil`.
                let mut last_failure = None;
                let num_of_results = results.len();
                let mut nil_counter = 0;
                for result in results {
                    match result.map(|(_, res)| res) {
                        Ok(Value::Nil) => nil_counter += 1,
                        Ok(val) => return Ok(val),
                        Err(err) => last_failure = Some(err),
                    }
                }
                if nil_counter == num_of_results {
                    Ok(Value::Nil)
                } else {
                    Err(last_failure
                        .unwrap_or_else(|| (ErrorKind::Io, "Couldn't find a connection").into()))
                }
            }
            Some(ResponsePolicy::Aggregate(op)) => {
                let results = results
                    .into_iter()
                    .map(|res| res.map(|(_, val)| val))
                    .collect::<RedisResult<Vec<_>>>()?;
                crate::cluster_routing::aggregate(results, op)
            }
            Some(ResponsePolicy::AggregateLogical(op)) => {
                let results = results
                    .into_iter()
                    .map(|res| res.map(|(_, val)| val))
                    .collect::<RedisResult<Vec<_>>>()?;
                crate::cluster_routing::logical_aggregate(results, op)
            }
            Some(ResponsePolicy::CombineArrays) => {
                let results = results
                    .into_iter()
                    .map(|res| res.map(|(_, val)| val))
                    .collect::<RedisResult<Vec<_>>>()?;
                match routing {
                    MultipleNodeRoutingInfo::MultiSlot((vec, pattern)) => {
                        crate::cluster_routing::combine_and_sort_array_results(
                            results, &vec, &pattern,
                        )
                    }
                    _ => crate::cluster_routing::combine_array_results(results),
                }
            }
            Some(ResponsePolicy::Special) | None => {
                // This is our assumption - if there's no coherent way to aggregate the responses, we just map each response to the sender, and pass it to the user.
                // TODO - once Value::Error is merged, we can use join_all and report separate errors and also pass successes.
                let results = results
                    .into_iter()
                    .map(|result| {
                        result.map(|(addr, val)| {
                            (Value::BulkString(addr.to_string().into_bytes()), val)
                        })
                    })
                    .collect::<RedisResult<Vec<_>>>()?;
                Ok(Value::Map(results))
            }
        }
    }

    #[inline(always)]
    #[allow(clippy::unnecessary_unwrap)]
    fn request(&self, input: Input, route_option: Option<RoutingInfo>) -> RedisResult<Output> {
        if self.read_fallback_enabled {
            self.request_inner::<true, false>(input, route_option, None)
        } else {
            self.request_inner::<false, false>(input, route_option, None)
        }
    }

    #[inline(always)]
    fn request_with_preferred_addr(
        &self,
        input: Input,
        route_option: Option<RoutingInfo>,
        preferred_addr: NodeAddress,
    ) -> RedisResult<Output> {
        self.request_inner::<true, true>(input, route_option, Some(preferred_addr))
    }

    #[allow(clippy::unnecessary_unwrap)]
    fn request_inner<const READ_FALLBACK: bool, const HAS_PREFERRED_ADDR: bool>(
        &self,
        input: Input,
        route_option: Option<RoutingInfo>,
        mut preferred_addr: Option<NodeAddress>,
    ) -> RedisResult<Output> {
        let single_node_routing = match route_option {
            Some(RoutingInfo::SingleNode(single_node_routing)) => single_node_routing,
            Some(RoutingInfo::MultiNode((multi_node_routing, response_policy))) => {
                return self
                    .execute_on_multiple_nodes(input, multi_node_routing, response_policy)
                    .map(Output::Single);
            }
            None => fail!(UNROUTABLE_ERROR),
        };

        let mut retries = 0;
        let mut redirected = None::<Redirect>;
        let mut excluded_read_nodes = Vec::new();

        loop {
            // Get target address and response.
            let (addr, rv) = {
                let mut connections = self.connections.borrow_mut();
                let (addr, conn) = if let Some(redirected) = redirected.take() {
                    let (addr, is_asking) = match redirected {
                        Redirect::Moved(addr) => (addr, false),
                        Redirect::Ask(addr) => (addr, true),
                    };
                    let mut conn = self.get_connection_by_addr(&mut connections, &addr);
                    if is_asking {
                        // if we are in asking mode we want to feed a single
                        // ASKING command into the connection before what we
                        // actually want to execute.
                        conn = conn.and_then(|conn| {
                            conn.req_packed_command(&b"*1\r\n$6\r\nASKING\r\n"[..])
                                .and_then(|value| value.extract_error())?;
                            Ok(conn)
                        });
                    }
                    (addr, conn)
                } else {
                    match &single_node_routing {
                        SingleNodeRoutingInfo::Random => {
                            get_random_connection_or_error(&mut connections)
                        }
                        SingleNodeRoutingInfo::SpecificNode(route) => {
                            if READ_FALLBACK && self.uses_read_fallback(route) {
                                self.get_connection_excluding(
                                    &mut connections,
                                    route,
                                    &mut excluded_read_nodes,
                                    if HAS_PREFERRED_ADDR {
                                        preferred_addr.take()
                                    } else {
                                        None
                                    },
                                )
                            } else {
                                self.get_connection(&mut connections, route)
                            }
                        }
                        SingleNodeRoutingInfo::ByAddress { host, port } => {
                            let address = NodeAddress::new(host.as_str(), *port);
                            let conn = self.get_connection_by_addr(&mut connections, &address);
                            (address, conn)
                        }
                        SingleNodeRoutingInfo::RandomPrimary => {
                            self.get_connection(&mut connections, &Route::new_random_primary())
                        }
                    }
                };
                (addr, conn.and_then(|conn| input.send(conn)))
            };

            match rv {
                Ok(rv) => return Ok(rv),
                Err(err) => {
                    if err.kind() == ErrorKind::ClusterConnectionNotFound
                        && *self.auto_reconnect.borrow()
                    {
                        for node in &self.initial_nodes {
                            let addr = NodeAddress::try_from(&node.addr)?;
                            if let Ok(mut conn) = self.connect(&addr) {
                                if conn.check_connection() {
                                    self.connections.borrow_mut().insert(addr, conn);
                                }
                            }
                        }
                        self.refresh_slots()?;
                    }

                    if retries == self.cluster_params.retry_params.number_of_retries {
                        return Err(err);
                    }
                    retries += 1;

                    match err.retry_method() {
                        RetryMethod::AskRedirect => {
                            redirected = err.redirect_node().and_then(|(node, _slot)| {
                                NodeAddress::try_from(node).ok().map(Redirect::Ask)
                            });
                        }
                        RetryMethod::MovedRedirect => {
                            let moved_to = err.redirect_node().and_then(|(node, slot)| {
                                NodeAddress::try_from(node).ok().map(|addr| (addr, slot))
                            });
                            match moved_to {
                                Some((addr, slot)) => {
                                    // A MOVED is authoritative about one slot's new
                                    // owner, so record it instead of discarding it and
                                    // asking the cluster to describe all 16384 slots
                                    // again. This arm is MOVED-only: the ASK arm below
                                    // must never do this, because an ASK slot is still
                                    // owned by the node that issued the redirect.
                                    //
                                    // The borrow is scoped to this statement so it ends
                                    // before the refresh borrows `slots` again.
                                    self.slots.borrow_mut().update_slot(slot, addr.clone());
                                    // Having learned the mapping first-hand, the full
                                    // refresh is now only about replica and node-set
                                    // freshness, so it is safe to rate limit.
                                    self.refresh_slots_rate_limited()?;
                                    redirected = Some(Redirect::Moved(addr));
                                }
                                None => {
                                    // A redirect we cannot parse teaches us nothing and
                                    // leaves us no node to retry against, so the full
                                    // refresh is the only way to make progress. Never
                                    // rate limit this one.
                                    self.refresh_slots()?;
                                    redirected = None;
                                }
                            }
                        }
                        RetryMethod::WaitAndRetry => {
                            // Sleep and retry.
                            let sleep_time = self
                                .cluster_params
                                .retry_params
                                .wait_time_for_retry(retries);
                            thread::sleep(sleep_time);
                        }
                        RetryMethod::Reconnect => {
                            if READ_FALLBACK {
                                self.note_read_node_failure(
                                    &single_node_routing,
                                    &addr,
                                    &mut excluded_read_nodes,
                                );
                            }
                            if *self.auto_reconnect.borrow() {
                                // if the connection is no longer valid, we should remove it.
                                let mut connections = self.connections.borrow_mut();
                                connections.remove(&addr);
                                if let Ok(mut conn) = self.connect(&addr) {
                                    if conn.check_connection() {
                                        connections.insert(addr, conn);
                                    }
                                }
                                self.notify_connections_changed(&connections);
                            }
                        }
                        RetryMethod::NoRetry => {
                            return Err(err);
                        }
                        RetryMethod::RetryImmediately => {
                            if READ_FALLBACK && err.kind() == ErrorKind::Io {
                                self.note_read_node_failure(
                                    &single_node_routing,
                                    &addr,
                                    &mut excluded_read_nodes,
                                );
                            }
                        }
                        RetryMethod::ReconnectFromInitialConnections => {
                            // TODO - implement reconnect from initial connections
                            if *self.auto_reconnect.borrow() {
                                if let Ok(mut conn) = self.connect(&addr) {
                                    if conn.check_connection() {
                                        self.connections.borrow_mut().insert(addr, conn);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn send_recv_and_retry_cmds(&self, cmds: &[Cmd]) -> RedisResult<Vec<Value>> {
        // Vector to hold the results, pre-populated with `Nil` values. This allows the original
        // cmd ordering to be re-established by inserting the response directly into the result
        // vector (e.g., results[10] = response).
        let mut results = vec![Value::Nil; cmds.len()];

        let to_retry = self
            .send_all_commands(cmds)
            .and_then(|node_cmds| self.recv_all_commands(&mut results, &node_cmds))?;

        if to_retry.is_empty() {
            return Ok(results);
        }

        // Refresh the slots to give the retry attempts a clean slate. Rate limited
        // because it is an optimisation rather than the recovery mechanism: each
        // command below is retried through `request`, which follows its own
        // redirects and falls back to an unthrottled refresh when it cannot.
        self.refresh_slots_rate_limited()?;

        // Given that there are commands that need to be retried, it means something in the cluster
        // topology changed. Execute each command separately to take advantage of the existing
        // retry logic that handles these cases.
        for retry_idx in to_retry {
            let cmd = &cmds[retry_idx];
            let routing = RoutingInfo::for_routable(cmd);
            results[retry_idx] = self.request(Input::Cmd(cmd), routing)?.into();
        }
        Ok(results)
    }

    // Build up a pipeline per node, then send it
    fn send_all_commands(&self, cmds: &[Cmd]) -> RedisResult<Vec<NodeCmd>> {
        let mut connections = self.connections.borrow_mut();

        let node_cmds = self.map_cmds_to_nodes(cmds)?;
        for nc in &node_cmds {
            self.get_connection_by_addr(&mut connections, &nc.addr)?
                .send_packed_command(&nc.pipe)?;
        }
        Ok(node_cmds)
    }

    // Receive from each node, keeping track of which commands need to be retried.
    fn recv_all_commands(
        &self,
        results: &mut [Value],
        node_cmds: &[NodeCmd],
    ) -> RedisResult<Vec<usize>> {
        let mut to_retry = Vec::new();
        let mut connections = self.connections.borrow_mut();
        let mut first_err = None;

        for nc in node_cmds {
            for cmd_idx in &nc.indexes {
                match self
                    .get_connection_by_addr(&mut connections, &nc.addr)?
                    .recv_response()
                {
                    // The RESP parser reports server errors as
                    // `Ok(Value::ServerError(_))` rather than `Err`.
                    // A pipelined sub-command that gets a redirect/retryable error
                    // needs to be processed in the Ok arm and retried
                    Ok(item) if item.is_error_that_requires_action() => to_retry.push(*cmd_idx),
                    Ok(item) => results[*cmd_idx] = item,
                    Err(err) if err.is_cluster_error() => to_retry.push(*cmd_idx),
                    Err(err) => first_err = first_err.or(Some(err)),
                }
            }
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(to_retry),
        }
    }

    /// Send a command to the given `routing`.
    pub fn route_command(&mut self, cmd: &Cmd, routing: RoutingInfo) -> RedisResult<Value> {
        self.request(Input::Cmd(cmd), Some(routing))
            .map(|res| res.into())
    }
}

const MULTI: &[u8] = "*1\r\n$5\r\nMULTI\r\n".as_bytes();
impl<C: Connect + ConnectionLike + Send> ConnectionLike for ClusterConnection<C> {
    fn supports_pipelining(&self) -> bool {
        false
    }

    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        if cmd.is_empty() {
            return Err(RedisError::make_empty_command());
        }
        let routing = RoutingInfo::for_routable(cmd);
        self.request(Input::Cmd(cmd), routing).map(|res| res.into())
    }

    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        if cmd.is_empty() {
            return Err(RedisError::make_empty_command());
        }
        let actual_cmd = if cmd.starts_with(MULTI) {
            &cmd[MULTI.len()..]
        } else {
            cmd
        };
        let value = parse_redis_value(actual_cmd)?;
        let routing = RoutingInfo::for_routable(&value);
        self.request(
            Input::Slice {
                cmd,
                routable: value,
            },
            routing,
        )
        .map(|res| res.into())
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        if cmd.is_empty() {
            return Err(RedisError::make_empty_command());
        }
        let actual_cmd = if cmd.starts_with(MULTI) {
            &cmd[MULTI.len()..]
        } else {
            cmd
        };
        let value = parse_redis_value(actual_cmd)?;
        let route = match RoutingInfo::for_routable(&value) {
            // we don't allow routing multiple commands to multiple nodes.
            Some(RoutingInfo::MultiNode(_)) => None,
            Some(RoutingInfo::SingleNode(route)) => Some(route),
            None => None,
        }
        .unwrap_or(SingleNodeRoutingInfo::Random);
        self.request(
            Input::Commands { cmd, offset, count },
            Some(RoutingInfo::SingleNode(route)),
        )
        .map(|res| res.into())
    }

    fn get_db(&self) -> i64 {
        0
    }

    fn is_open(&self) -> bool {
        let connections = self.connections.borrow();
        for conn in connections.values() {
            if !conn.is_open() {
                return false;
            }
        }
        true
    }

    fn check_connection(&mut self) -> bool {
        let mut connections = self.connections.borrow_mut();
        for conn in connections.values_mut() {
            if !conn.check_connection() {
                return false;
            }
        }
        true
    }
}

#[derive(Debug)]
struct NodeCmd {
    // The original command indexes
    indexes: Vec<usize>,
    pipe: Vec<u8>,
    addr: NodeAddress,
}

impl NodeCmd {
    fn new(a: NodeAddress) -> NodeCmd {
        NodeCmd {
            indexes: vec![],
            pipe: vec![],
            addr: a,
        }
    }
}

/// Open a single connection to `node` and prepare it for cluster use.
///
/// This is a free function (rather than a `ClusterConnection` method) so it can
/// run on a worker thread during parallel connection refresh without borrowing
/// `&self`, which holds non-`Sync` `RefCell` state. The read/write timeouts are
/// passed in by value so callers can read them out of their `RefCell`s up front.
fn connect_node<C: ConnectionLike + Connect>(
    node: &NodeAddress,
    cluster_params: &ClusterParams,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
) -> RedisResult<C> {
    let info = get_connection_info(node, cluster_params);

    let mut conn = C::connect(info, Some(cluster_params.connection_timeout))?;
    if cluster_params.read_routing_factory.is_some() {
        cmd("READONLY").exec(&mut conn)?;
    }
    set_client_name(&mut conn, node, cluster_params);
    conn.set_read_timeout(read_timeout)?;
    conn.set_write_timeout(write_timeout)?;
    Ok(conn)
}

fn set_client_name<C: ConnectionLike>(
    conn: &mut C,
    node: &NodeAddress,
    cluster_params: &ClusterParams,
) {
    let Some(factory) = &cluster_params.client_name_factory else {
        return;
    };
    let name = factory(node);
    if name.is_empty() {
        return;
    }
    let _ = cmd("CLIENT").arg("SETNAME").arg(name).exec(conn);
}

fn get_random_connection<C: ConnectionLike + Connect + Sized>(
    connections: &mut HashMap<NodeAddress, C>,
) -> Option<(NodeAddress, &mut C)> {
    connections
        .iter_mut()
        .choose(&mut rng())
        .map(|(addr, conn)| (addr.clone(), conn))
}

fn get_random_connection_or_error<C: ConnectionLike + Connect + Sized>(
    connections: &mut HashMap<NodeAddress, C>,
) -> (NodeAddress, RedisResult<&mut C>) {
    match get_random_connection(connections) {
        Some((addr, conn)) => (addr, Ok(conn)),
        None => (
            // we need to add a fake address in order for the error to be handled - the code that uses it assumes there's an address attached.
            NodeAddress::default(),
            Err(RedisError::from((
                ErrorKind::ClusterConnectionNotFound,
                "No connections found",
            ))),
        ),
    }
}
