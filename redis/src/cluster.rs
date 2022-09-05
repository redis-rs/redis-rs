//! Redis cluster support.
//!
//! This module extends the library to be able to use cluster.
//! ClusterClient implements traits of ConnectionLike and Commands.
//!
//! Note that the cluster support currently does not provide pubsub
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
use std::collections::BTreeMap;
use std::iter::Iterator;
use std::thread;
use std::time::Duration;

use rand::{
    seq::{IteratorRandom, SliceRandom},
    thread_rng, Rng,
};

use crate::cluster_client::ClusterParams;
use crate::cluster_pipeline::UNROUTABLE_ERROR;
use crate::cluster_routing::{Routable, RoutingInfo, Slot, SLOT_SIZE};
use crate::cmd::{cmd, Cmd};
use crate::connection::{connect, Connection, ConnectionInfo, ConnectionLike};
use crate::parser::parse_redis_value;
use crate::types::{ErrorKind, HashMap, HashSet, RedisError, RedisResult, Value};

pub use crate::cluster_client::{ClusterClient, ClusterClientBuilder};
pub use crate::cluster_pipeline::{cluster_pipe, ClusterPipeline};

type SlotMap = BTreeMap<u16, [String; 2]>;

#[derive(Clone, Copy)]
/// See [ConnectionAddr](ConnectionAddr::TcpTls::insecure).
pub enum TlsMode {
    /// Tls Secure mode
    Secure,
    /// Tls Insecure mode
    Insecure,
}

/// This is a connection of Redis cluster.
#[derive(Default)]
pub struct ClusterConnection {
    cluster_params: ClusterParams,
    initial_nodes: Vec<ConnectionInfo>,

    connections: RefCell<HashMap<String, Connection>>,
    slots: RefCell<SlotMap>,
}

impl ClusterConnection {
    pub(crate) fn new(
        cluster_params: ClusterParams,
        initial_nodes: Vec<ConnectionInfo>,
    ) -> RedisResult<ClusterConnection> {
        let connection = ClusterConnection {
            cluster_params,
            initial_nodes,
            ..Default::default()
        };
        connection.create_initial_connections()?;

        Ok(connection)
    }

    /// Set the timeout for connecting to new nodes.
    ///
    /// See [ClusterClientBuilder](ClusterClientBuilder::connect_timeout).
    pub fn set_connect_timeout(&mut self, dur: Option<Duration>) -> RedisResult<()> {
        // Check if duration is valid before updating local value.
        ClusterParams::validate_duration(&dur)?;

        self.cluster_params.connect_timeout = dur;
        Ok(())
    }

    /// Set the write timeout for this connection.
    ///
    /// See [ClusterClientBuilder](ClusterClientBuilder::write_timeout).
    pub fn set_write_timeout(&mut self, dur: Option<Duration>) -> RedisResult<()> {
        // Check if duration is valid before updating local value.
        ClusterParams::validate_duration(&dur)?;

        self.cluster_params.write_timeout = dur;
        for conn in self.connections.borrow().values() {
            conn.set_write_timeout(dur)?;
        }
        Ok(())
    }

    /// Set the read timeout for this connection.
    ///
    /// See [ClusterClientBuilder](ClusterClientBuilder::read_timeout).
    pub fn set_read_timeout(&mut self, dur: Option<Duration>) -> RedisResult<()> {
        // Check if duration is valid before updating local value.
        ClusterParams::validate_duration(&dur)?;

        self.cluster_params.read_timeout = dur;
        for conn in self.connections.borrow().values() {
            conn.set_read_timeout(dur)?;
        }
        Ok(())
    }

    /// Set the read from replicas parameter for this connection.
    pub fn set_read_from_replicas(&mut self, value: bool) {
        self.cluster_params.read_from_replicas = value;
    }

    /// Set the retries parameter for this connection.
    pub fn set_retries(&mut self, value: u8) {
        self.cluster_params.retries = value;
    }

    /// Set the auto reconnect parameter for this connection.
    pub fn set_auto_reconnect(&mut self, value: bool) {
        self.cluster_params.auto_reconnect = value;
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
        *slots = self.create_new_slots(|slot_data| {
            let replica =
                if !self.cluster_params.read_from_replicas || slot_data.replicas().is_empty() {
                    slot_data.master().to_string()
                } else {
                    slot_data
                        .replicas()
                        .choose(&mut thread_rng())
                        .unwrap()
                        .to_string()
                };

            [slot_data.master().to_string(), replica]
        })?;

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
                        conn.set_read_timeout(self.cluster_params.read_timeout)
                            .unwrap();
                        conn.set_write_timeout(self.cluster_params.write_timeout)
                            .unwrap();
                        return Some((addr.to_string(), conn));
                    }
                }

                None
            })
            .collect();

        Ok(())
    }

    fn create_new_slots<F>(&self, mut get_addr: F) -> RedisResult<SlotMap>
    where
        F: FnMut(&Slot) -> [String; 2],
    {
        let mut connections = self.connections.borrow_mut();
        let mut new_slots = None;
        let mut rng = thread_rng();
        let len = connections.len();
        let mut samples = connections.values_mut().choose_multiple(&mut rng, len);

        for conn in samples.iter_mut() {
            if let Ok(mut slots_data) = get_slots(conn, &self.cluster_params) {
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
                        format!("Lacks the slots >= {}", last_slot),
                    )));
                }

                new_slots = Some(
                    slots_data
                        .iter()
                        .map(|slot_data| (slot_data.end(), get_addr(slot_data)))
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

    fn connect(&self, node: &str) -> RedisResult<Connection> {
        let info = self.cluster_params.get_connection_info(node);

        let mut conn = connect(&info, self.cluster_params.connect_timeout)?;
        if self.cluster_params.read_from_replicas {
            // If READONLY is sent to primary nodes, it will have no effect
            cmd("READONLY").query(&mut conn)?;
        }
        Ok(conn)
    }

    fn get_connection<'a>(
        &self,
        connections: &'a mut HashMap<String, Connection>,
        route: (u16, usize),
    ) -> RedisResult<(String, &'a mut Connection)> {
        let (slot, idx) = route;
        let slots = self.slots.borrow();
        if let Some((_, addr)) = slots.range(&slot..).next() {
            Ok((
                addr[idx].clone(),
                self.get_connection_by_addr(connections, &addr[idx])?,
            ))
        } else {
            // try a random node next.  This is safe if slots are involved
            // as a wrong node would reject the request.
            Ok(get_random_connection(connections, None))
        }
    }

    fn get_connection_by_addr<'a>(
        &self,
        connections: &'a mut HashMap<String, Connection>,
        addr: &str,
    ) -> RedisResult<&'a mut Connection> {
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

        let addr_for_slot = |slot: u16, idx: usize| -> RedisResult<String> {
            let (_, addr) = slots
                .range(&slot..)
                .next()
                .ok_or((ErrorKind::ClusterDown, "Missing slot coverage"))?;
            Ok(addr[idx].clone())
        };

        match RoutingInfo::for_routable(cmd) {
            Some(RoutingInfo::Random) => {
                let mut rng = thread_rng();
                Ok(addr_for_slot(rng.gen_range(0..SLOT_SIZE) as u16, 0)?)
            }
            Some(RoutingInfo::MasterSlot(slot)) => Ok(addr_for_slot(slot, 0)?),
            Some(RoutingInfo::ReplicaSlot(slot)) => Ok(addr_for_slot(slot, 1)?),
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
        F: FnMut(&mut Connection) -> RedisResult<T>,
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
        F: FnMut(&mut Connection) -> RedisResult<T>,
    {
        let route = match RoutingInfo::for_routable(cmd) {
            Some(RoutingInfo::Random) => None,
            Some(RoutingInfo::MasterSlot(slot)) => Some((slot, 0)),
            Some(RoutingInfo::ReplicaSlot(slot)) => Some((slot, 1)),
            Some(RoutingInfo::AllNodes) | Some(RoutingInfo::AllMasters) => {
                return self.execute_on_all_nodes(func);
            }
            None => fail!(UNROUTABLE_ERROR),
        };

        let mut retries = self.cluster_params.retries;
        let mut excludes = HashSet::new();
        let mut redirected = None::<String>;
        let mut is_asking = false;
        loop {
            // Get target address and response.
            let (addr, rv) = {
                let mut connections = self.connections.borrow_mut();
                let (addr, conn) = if let Some(addr) = redirected.take() {
                    let conn = self.get_connection_by_addr(&mut *connections, &addr)?;
                    if is_asking {
                        // if we are in asking mode we want to feed a single
                        // ASKING command into the connection before what we
                        // actually want to execute.
                        conn.req_packed_command(&b"*1\r\n$6\r\nASKING\r\n"[..])?;
                        is_asking = false;
                    }
                    (addr.to_string(), conn)
                } else if !excludes.is_empty() || route.is_none() {
                    get_random_connection(&mut *connections, Some(&excludes))
                } else {
                    self.get_connection(&mut *connections, route.unwrap())?
                };
                (addr, func(conn))
            };

            match rv {
                Ok(rv) => return Ok(rv),
                Err(err) => {
                    retries -= 1;
                    if retries == 0 {
                        return Err(err);
                    }

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
                            let sleep_time = 2u64.pow(16 - u32::from(retries.max(9))) * 10;
                            thread::sleep(Duration::from_millis(sleep_time));
                            excludes.clear();
                            continue;
                        }
                    } else if self.cluster_params.auto_reconnect && err.is_io_error() {
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

impl ConnectionLike for ClusterConnection {
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

fn get_random_connection<'a>(
    connections: &'a mut HashMap<String, Connection>,
    excludes: Option<&'a HashSet<String>>,
) -> (String, &'a mut Connection) {
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

// Get slot data from connection.
fn get_slots(
    connection: &mut Connection,
    cluster_params: &ClusterParams,
) -> RedisResult<Vec<Slot>> {
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");
    let value = connection.req_command(&cmd)?;

    // Parse response.
    let mut result = Vec::with_capacity(2);

    if let Value::Bulk(items) = value {
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
                        Some(
                            cluster_params
                                .get_connection_addr(ip.into_owned(), port)
                                .to_string(),
                        )
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
