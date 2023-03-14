//! This module extends the library to support Redis Cluster.
//!
//! Note that this module does not currently provide pubsub
//! functionality.
//!
//! # Example
//! ```rust,no_run
//! use redis::Commands;
//! use redis::cluster::ClusterClient;
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let client = ClusterClient::new(nodes).unwrap();
//! let mut connection = client.get_connection().unwrap();
//!
//! let _: () = connection.set("test", "test_data").unwrap();
//! let rv: String = connection.get("test").unwrap();
//!
//! assert_eq!(rv, "test_data");
//! ```
//!
//! # Pipelining
//! ```rust,no_run
//! use redis::Commands;
//! use redis::cluster::{cluster_pipe, ClusterClient};
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let client = ClusterClient::new(nodes).unwrap();
//! let mut connection = client.get_connection().unwrap();
//!
//! let key = "test";
//!
//! let _: () = cluster_pipe()
//!     .rpush(key, "123").ignore()
//!     .ltrim(key, -10, -1).ignore()
//!     .expire(key, 60).ignore()
//!     .query(&mut connection).unwrap();
//! ```
use std::cell::RefCell;
use std::iter::Iterator;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use rand::{seq::IteratorRandom, thread_rng, Rng};

use crate::cluster_pipeline::UNROUTABLE_ERROR;
use crate::cluster_routing::{Routable, RoutingInfo, Slot, SLOT_SIZE};
use crate::cmd::{cmd, Cmd};
use crate::connection::{
    connect, Connection, ConnectionAddr, ConnectionInfo, ConnectionLike, RedisConnectionInfo,
};
use crate::parser::parse_redis_value;
use crate::types::{ErrorKind, HashMap, HashSet, RedisError, RedisResult, Value};
use crate::IntoConnectionInfo;
use crate::{
    cluster_client::ClusterParams,
    cluster_routing::{Route, SlotAddr, SlotAddrs, SlotMap},
};

pub use crate::cluster_client::{ClusterClient, ClusterClientBuilder};
pub use crate::cluster_pipeline::{cluster_pipe, ClusterPipeline};

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

/// This represents a Redis Cluster connection. It stores the
/// underlying connections maintained for each node in the cluster, as well
/// as common parameters for connecting to nodes and executing commands.
pub struct ClusterConnection<C = Connection> {
    initial_nodes: Vec<ConnectionInfo>,
    connections: RefCell<HashMap<String, C>>,
    slots: RefCell<SlotMap>,
    auto_reconnect: RefCell<bool>,
    read_from_replicas: bool,
    username: Option<String>,
    password: Option<String>,
    read_timeout: RefCell<Option<Duration>>,
    write_timeout: RefCell<Option<Duration>>,
    tls: Option<TlsMode>,
    retries: u32,
}

impl<C> ClusterConnection<C>
where
    C: ConnectionLike + Connect,
{
    pub(crate) fn new(
        cluster_params: ClusterParams,
        initial_nodes: Vec<ConnectionInfo>,
    ) -> RedisResult<Self> {
        let connection = Self {
            connections: RefCell::new(HashMap::new()),
            slots: RefCell::new(SlotMap::new()),
            auto_reconnect: RefCell::new(true),
            read_from_replicas: cluster_params.read_from_replicas,
            username: cluster_params.username,
            password: cluster_params.password,
            read_timeout: RefCell::new(None),
            write_timeout: RefCell::new(None),
            tls: cluster_params.tls,
            initial_nodes: initial_nodes.to_vec(),
            retries: cluster_params.retries,
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
    /// The connection is open until any `read_response` call recieved an
    /// invalid response from the server (most likely a closed or dropped
    /// connection, otherwise a Redis protocol error). When using unix
    /// sockets the connection is open until writing a command failed with a
    /// `BrokenPipe` error.
    fn create_initial_connections(&self) -> RedisResult<()> {
        let mut connections = HashMap::with_capacity(self.initial_nodes.len());

        for info in self.initial_nodes.iter() {
            let addr = info.addr.to_string();

            if let Ok(mut conn) = self.connect(&addr) {
                if conn.check_connection() {
                    connections.insert(addr, conn);
                    break;
                }
            }
        }

        if connections.is_empty() {
            return Err(RedisError::from((
                ErrorKind::IoError,
                "It failed to check startup nodes.",
            )));
        }

        *self.connections.borrow_mut() = connections;
        self.refresh_slots()?;
        Ok(())
    }

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(&self) -> RedisResult<()> {
        let mut slots = self.slots.borrow_mut();
        *slots = self.create_new_slots()?;

        let mut nodes = slots.values().flatten().collect::<Vec<_>>();
        nodes.sort_unstable();
        nodes.dedup();

        let mut connections = self.connections.borrow_mut();
        *connections = nodes
            .into_iter()
            .filter_map(|addr| {
                if connections.contains_key(addr) {
                    let mut conn = connections.remove(addr).unwrap();
                    if conn.check_connection() {
                        return Some((addr.to_string(), conn));
                    }
                }

                if let Ok(mut conn) = self.connect(addr) {
                    if conn.check_connection() {
                        conn.set_read_timeout(*self.read_timeout.borrow()).unwrap();
                        conn.set_write_timeout(*self.write_timeout.borrow())
                            .unwrap();
                        return Some((addr.to_string(), conn));
                    }
                }

                None
            })
            .collect();

        Ok(())
    }

    fn create_new_slots(&self) -> RedisResult<SlotMap> {
        let mut connections = self.connections.borrow_mut();
        let mut new_slots = None;
        let mut rng = thread_rng();
        let len = connections.len();
        let mut samples = connections.values_mut().choose_multiple(&mut rng, len);

        for conn in samples.iter_mut() {
            let value = conn.req_command(&slot_cmd())?;
            if let Ok(mut slots_data) = parse_slots(value, self.tls) {
                slots_data.sort_by_key(|s| s.start());
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

                if last_slot != SLOT_SIZE {
                    return Err(RedisError::from((
                        ErrorKind::ResponseError,
                        "Slot refresh error.",
                        format!("Lacks the slots >= {last_slot}"),
                    )));
                }

                new_slots = Some(
                    slots_data
                        .iter()
                        .map(|slot| {
                            (
                                slot.end(),
                                SlotAddrs::from_slot(slot, self.read_from_replicas),
                            )
                        })
                        .collect(),
                );
                break;
            }
        }

        match new_slots {
            Some(new_slots) => Ok(new_slots),
            None => Err(RedisError::from((
                ErrorKind::ResponseError,
                "Slot refresh error.",
                "didn't get any slots from server".to_string(),
            ))),
        }
    }

    fn connect(&self, node: &str) -> RedisResult<C> {
        let params = ClusterParams {
            password: self.password.clone(),
            username: self.username.clone(),
            tls: self.tls,
            ..Default::default()
        };
        let info = get_connection_info(node, params)?;

        let mut conn = C::connect(info, None)?;
        if self.read_from_replicas {
            // If READONLY is sent to primary nodes, it will have no effect
            cmd("READONLY").query(&mut conn)?;
        }
        Ok(conn)
    }

    fn get_connection<'a>(
        &self,
        connections: &'a mut HashMap<String, C>,
        route: &Route,
    ) -> RedisResult<(String, &'a mut C)> {
        let slots = self.slots.borrow();
        if let Some((_, slot_addrs)) = slots.range(route.slot()..).next() {
            let addr = &slot_addrs.slot_addr(route.slot_addr());
            Ok((
                addr.to_string(),
                self.get_connection_by_addr(connections, addr)?,
            ))
        } else {
            // try a random node next.  This is safe if slots are involved
            // as a wrong node would reject the request.
            Ok(get_random_connection(connections, None))
        }
    }

    fn get_connection_by_addr<'a>(
        &self,
        connections: &'a mut HashMap<String, C>,
        addr: &str,
    ) -> RedisResult<&'a mut C> {
        if connections.contains_key(addr) {
            Ok(connections.get_mut(addr).unwrap())
        } else {
            // Create new connection.
            // TODO: error handling
            let conn = self.connect(addr)?;
            Ok(connections.entry(addr.to_string()).or_insert(conn))
        }
    }

    fn get_addr_for_cmd(&self, cmd: &Cmd) -> RedisResult<String> {
        let slots = self.slots.borrow();

        let addr_for_slot = |slot: u16, slot_addr: SlotAddr| -> RedisResult<String> {
            let (_, slot_addrs) = slots
                .range(&slot..)
                .next()
                .ok_or((ErrorKind::ClusterDown, "Missing slot coverage"))?;
            Ok(slot_addrs.slot_addr(&slot_addr).to_string())
        };

        match RoutingInfo::for_routable(cmd) {
            Some(RoutingInfo::Random) => {
                let mut rng = thread_rng();
                Ok(addr_for_slot(
                    rng.gen_range(0..SLOT_SIZE),
                    SlotAddr::Master,
                )?)
            }
            Some(RoutingInfo::MasterSlot(slot)) => Ok(addr_for_slot(slot, SlotAddr::Master)?),
            Some(RoutingInfo::ReplicaSlot(slot)) => Ok(addr_for_slot(slot, SlotAddr::Replica)?),
            _ => fail!(UNROUTABLE_ERROR),
        }
    }

    fn map_cmds_to_nodes(&self, cmds: &[Cmd]) -> RedisResult<Vec<NodeCmd>> {
        let mut cmd_map: HashMap<String, NodeCmd> = HashMap::new();

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

    fn execute_on_all_nodes<T, F>(&self, mut func: F) -> RedisResult<T>
    where
        T: MergeResults,
        F: FnMut(&mut C) -> RedisResult<T>,
    {
        let mut connections = self.connections.borrow_mut();
        let mut results = HashMap::new();

        // TODO: reconnect and shit
        for (addr, connection) in connections.iter_mut() {
            results.insert(addr.as_str(), func(connection)?);
        }

        Ok(T::merge_results(results))
    }

    #[allow(clippy::unnecessary_unwrap)]
    fn request<R, T, F>(&self, cmd: &R, mut func: F) -> RedisResult<T>
    where
        R: ?Sized + Routable,
        T: MergeResults + std::fmt::Debug,
        F: FnMut(&mut C) -> RedisResult<T>,
    {
        let route = match RoutingInfo::for_routable(cmd) {
            Some(RoutingInfo::Random) => None,
            Some(RoutingInfo::MasterSlot(slot)) => Some(Route::new(slot, SlotAddr::Master)),
            Some(RoutingInfo::ReplicaSlot(slot)) => Some(Route::new(slot, SlotAddr::Replica)),
            Some(RoutingInfo::AllNodes) | Some(RoutingInfo::AllMasters) => {
                return self.execute_on_all_nodes(func);
            }
            None => fail!(UNROUTABLE_ERROR),
        };

        let mut retries = self.retries;
        let mut excludes = HashSet::new();
        let mut redirected = None::<String>;
        let mut is_asking = false;
        loop {
            // Get target address and response.
            let (addr, rv) = {
                let mut connections = self.connections.borrow_mut();
                let (addr, conn) = if let Some(addr) = redirected.take() {
                    let conn = self.get_connection_by_addr(&mut connections, &addr)?;
                    if is_asking {
                        // if we are in asking mode we want to feed a single
                        // ASKING command into the connection before what we
                        // actually want to execute.
                        conn.req_packed_command(&b"*1\r\n$6\r\nASKING\r\n"[..])?;
                        is_asking = false;
                    }
                    (addr.to_string(), conn)
                } else if !excludes.is_empty() || route.is_none() {
                    get_random_connection(&mut connections, Some(&excludes))
                } else {
                    self.get_connection(&mut connections, route.as_ref().unwrap())?
                };
                (addr, func(conn))
            };

            match rv {
                Ok(rv) => return Ok(rv),
                Err(err) => {
                    if retries == 0 {
                        return Err(err);
                    }
                    retries -= 1;

                    if err.is_cluster_error() {
                        let kind = err.kind();

                        if kind == ErrorKind::Ask {
                            redirected = err.redirect_node().map(|(node, _slot)| node.to_string());
                            is_asking = true;
                        } else if kind == ErrorKind::Moved {
                            // Refresh slots.
                            self.refresh_slots()?;
                            excludes.clear();

                            // Request again.
                            redirected = err.redirect_node().map(|(node, _slot)| node.to_string());
                            is_asking = false;
                            continue;
                        } else if kind == ErrorKind::TryAgain || kind == ErrorKind::ClusterDown {
                            // Sleep and retry.
                            let sleep_time = 2u64.pow(16 - retries.max(9)) * 10;
                            thread::sleep(Duration::from_millis(sleep_time));
                            excludes.clear();
                            continue;
                        }
                    } else if *self.auto_reconnect.borrow() && err.is_io_error() {
                        self.create_initial_connections()?;
                        excludes.clear();
                        continue;
                    } else {
                        return Err(err);
                    }

                    excludes.insert(addr);

                    let connections = self.connections.borrow();
                    if excludes.len() >= connections.len() {
                        return Err(err);
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

        // Refresh the slots to ensure that we have a clean slate for the retry attempts.
        self.refresh_slots()?;

        // Given that there are commands that need to be retried, it means something in the cluster
        // topology changed. Execute each command seperately to take advantage of the existing
        // retry logic that handles these cases.
        for retry_idx in to_retry {
            let cmd = &cmds[retry_idx];
            results[retry_idx] = self.request(cmd, move |conn| conn.req_command(cmd))?;
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
}

impl<C: Connect + ConnectionLike> ConnectionLike for ClusterConnection<C> {
    fn supports_pipelining(&self) -> bool {
        false
    }

    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        self.request(cmd, move |conn| conn.req_command(cmd))
    }

    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        let value = parse_redis_value(cmd)?;
        self.request(&value, move |conn| conn.req_packed_command(cmd))
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        let value = parse_redis_value(cmd)?;
        self.request(&value, move |conn| {
            conn.req_packed_commands(cmd, offset, count)
        })
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

trait MergeResults {
    fn merge_results(_values: HashMap<&str, Self>) -> Self
    where
        Self: Sized;
}

impl MergeResults for Value {
    fn merge_results(values: HashMap<&str, Value>) -> Value {
        let mut items = vec![];
        for (addr, value) in values.into_iter() {
            items.push(Value::Bulk(vec![
                Value::Data(addr.as_bytes().to_vec()),
                value,
            ]));
        }
        Value::Bulk(items)
    }
}

impl MergeResults for Vec<Value> {
    fn merge_results(_values: HashMap<&str, Vec<Value>>) -> Vec<Value> {
        unreachable!("attempted to merge a pipeline. This should not happen");
    }
}

#[derive(Debug)]
struct NodeCmd {
    // The original command indexes
    indexes: Vec<usize>,
    pipe: Vec<u8>,
    addr: String,
}

impl NodeCmd {
    fn new(a: String) -> NodeCmd {
        NodeCmd {
            indexes: vec![],
            pipe: vec![],
            addr: a,
        }
    }
}

/// TlsMode indicates use or do not use verification of certification.
/// Check [ConnectionAddr](ConnectionAddr::TcpTls::insecure) for more.
#[derive(Clone, Copy)]
pub enum TlsMode {
    /// Secure verify certification.
    Secure,
    /// Insecure do not verify certification.
    Insecure,
}

fn get_random_connection<'a, C: ConnectionLike + Connect + Sized>(
    connections: &'a mut HashMap<String, C>,
    excludes: Option<&'a HashSet<String>>,
) -> (String, &'a mut C) {
    let mut rng = thread_rng();
    let addr = match excludes {
        Some(excludes) if excludes.len() < connections.len() => connections
            .keys()
            .filter(|key| !excludes.contains(*key))
            .choose(&mut rng)
            .unwrap()
            .to_string(),
        _ => connections.keys().choose(&mut rng).unwrap().to_string(),
    };

    let con = connections.get_mut(&addr).unwrap();
    (addr, con)
}

// Parse slot data from raw redis value.
pub(crate) fn parse_slots(raw_slot_resp: Value, tls: Option<TlsMode>) -> RedisResult<Vec<Slot>> {
    // Parse response.
    let mut result = Vec::with_capacity(2);

    if let Value::Bulk(items) = raw_slot_resp {
        let mut iter = items.into_iter();
        while let Some(Value::Bulk(item)) = iter.next() {
            if item.len() < 3 {
                continue;
            }

            let start = if let Value::Int(start) = item[0] {
                start as u16
            } else {
                continue;
            };

            let end = if let Value::Int(end) = item[1] {
                end as u16
            } else {
                continue;
            };

            let mut nodes: Vec<String> = item
                .into_iter()
                .skip(2)
                .filter_map(|node| {
                    if let Value::Bulk(node) = node {
                        if node.len() < 2 {
                            return None;
                        }

                        let ip = if let Value::Data(ref ip) = node[0] {
                            String::from_utf8_lossy(ip)
                        } else {
                            return None;
                        };
                        if ip.is_empty() {
                            return None;
                        }

                        let port = if let Value::Int(port) = node[1] {
                            port as u16
                        } else {
                            return None;
                        };
                        Some(get_connection_addr(ip.into_owned(), port, tls).to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if nodes.is_empty() {
                continue;
            }

            let replicas = nodes.split_off(1);
            result.push(Slot::new(start, end, nodes.pop().unwrap(), replicas));
        }
    }

    Ok(result)
}

// The node string passed to this function will always be in the format host:port as it is either:
// - Created by calling ConnectionAddr::to_string (unix connections are not supported in cluster mode)
// - Returned from redis via the ASK/MOVED response
pub(crate) fn get_connection_info(
    node: &str,
    cluster_params: ClusterParams,
) -> RedisResult<ConnectionInfo> {
    let invalid_error = || (ErrorKind::InvalidClientConfig, "Invalid node string");

    let (host, port) = node
        .rsplit_once(':')
        .and_then(|(host, port)| {
            Some(host.trim_start_matches('[').trim_end_matches(']'))
                .filter(|h| !h.is_empty())
                .zip(u16::from_str(port).ok())
        })
        .ok_or_else(invalid_error)?;

    Ok(ConnectionInfo {
        addr: get_connection_addr(host.to_string(), port, cluster_params.tls),
        redis: RedisConnectionInfo {
            password: cluster_params.password,
            username: cluster_params.username,
            ..Default::default()
        },
    })
}

fn get_connection_addr(host: String, port: u16, tls: Option<TlsMode>) -> ConnectionAddr {
    match tls {
        Some(TlsMode::Secure) => ConnectionAddr::TcpTls {
            host,
            port,
            insecure: false,
        },
        Some(TlsMode::Insecure) => ConnectionAddr::TcpTls {
            host,
            port,
            insecure: true,
        },
        _ => ConnectionAddr::Tcp(host, port),
    }
}

pub(crate) fn slot_cmd() -> Cmd {
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");
    cmd
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cluster_node_host_port() {
        let cases = vec![
            (
                "127.0.0.1:6379",
                ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379u16),
            ),
            (
                "localhost.localdomain:6379",
                ConnectionAddr::Tcp("localhost.localdomain".to_string(), 6379u16),
            ),
            (
                "dead::cafe:beef:30001",
                ConnectionAddr::Tcp("dead::cafe:beef".to_string(), 30001u16),
            ),
            (
                "[fe80::cafe:beef%en1]:30001",
                ConnectionAddr::Tcp("fe80::cafe:beef%en1".to_string(), 30001u16),
            ),
        ];

        for (input, expected) in cases {
            let res = get_connection_info(input, ClusterParams::default());
            assert_eq!(res.unwrap().addr, expected);
        }

        let cases = vec![":0", "[]:6379"];
        for input in cases {
            let res = get_connection_info(input, ClusterParams::default());
            assert_eq!(
                res.err(),
                Some(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "Invalid node string",
                ))),
            );
        }
    }
}
