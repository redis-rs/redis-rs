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
//! let client = ClusterClient::open(nodes).unwrap();
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
//! let client = ClusterClient::open(nodes).unwrap();
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

use super::{
    cmd, parse_redis_value,
    types::{HashMap, HashSet},
    Cmd, Connection, ConnectionAddr, ConnectionInfo, ConnectionLike, ErrorKind, IntoConnectionInfo,
    RedisError, RedisResult, Value,
};

pub use crate::cluster_client::{ClusterClient, ClusterClientBuilder};
use crate::cluster_pipeline::UNROUTABLE_ERROR;
pub use crate::cluster_pipeline::{cluster_pipe, ClusterPipeline};
use crate::cluster_routing::{Routable, RoutingInfo, Slot, SLOT_SIZE};

type SlotMap = BTreeMap<u16, [String; 2]>;

/// This is a connection of Redis cluster.
pub struct ClusterConnection {
    initial_nodes: Vec<ConnectionInfo>,
    connections: RefCell<HashMap<String, Connection>>,
    slots: RefCell<SlotMap>,
    auto_reconnect: RefCell<bool>,
    read_from_replicas: bool,
    username: Option<String>,
    password: Option<String>,
    read_timeout: RefCell<Option<Duration>>,
    write_timeout: RefCell<Option<Duration>>,
    tls: Option<TlsMode>,
}

#[derive(Clone, Copy)]
enum TlsMode {
    Secure,
    Insecure,
}

impl TlsMode {
    fn from_insecure_flag(insecure: bool) -> TlsMode {
        if insecure {
            TlsMode::Insecure
        } else {
            TlsMode::Secure
        }
    }
}

impl ClusterConnection {
    pub(crate) fn new(
        initial_nodes: Vec<ConnectionInfo>,
        read_from_replicas: bool,
        username: Option<String>,
        password: Option<String>,
    ) -> RedisResult<ClusterConnection> {
        let connections = Self::create_initial_connections(
            &initial_nodes,
            read_from_replicas,
            username.clone(),
            password.clone(),
        )?;

        let connection = ClusterConnection {
            connections: RefCell::new(connections),
            slots: RefCell::new(SlotMap::new()),
            auto_reconnect: RefCell::new(true),
            read_from_replicas,
            username,
            password,
            read_timeout: RefCell::new(None),
            write_timeout: RefCell::new(None),
            #[cfg(feature = "tls")]
            tls: {
                if initial_nodes.is_empty() {
                    None
                } else {
                    // TODO: Maybe should run through whole list and make sure they're all matching?
                    match &initial_nodes.get(0).unwrap().addr {
                        ConnectionAddr::Tcp(_, _) => None,
                        ConnectionAddr::TcpTls {
                            host: _,
                            port: _,
                            insecure,
                        } => Some(TlsMode::from_insecure_flag(*insecure)),
                        _ => None,
                    }
                }
            },
            #[cfg(not(feature = "tls"))]
            tls: None,
            initial_nodes,
        };
        connection.refresh_slots()?;

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
    pub fn check_connection(&mut self) -> bool {
        let mut connections = self.connections.borrow_mut();
        for conn in connections.values_mut() {
            if !conn.check_connection() {
                return false;
            }
        }
        true
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
    fn create_initial_connections(
        initial_nodes: &[ConnectionInfo],
        read_from_replicas: bool,
        username: Option<String>,
        password: Option<String>,
    ) -> RedisResult<HashMap<String, Connection>> {
        let mut connections = HashMap::with_capacity(initial_nodes.len());

        for info in initial_nodes.iter() {
            let addr = match info.addr {
                ConnectionAddr::Tcp(ref host, port) => format!("redis://{}:{}", host, port),
                ConnectionAddr::TcpTls {
                    ref host,
                    port,
                    insecure,
                } => {
                    let tls_mode = TlsMode::from_insecure_flag(insecure);
                    build_connection_string(host, Some(port), Some(tls_mode))
                }
                _ => panic!("No reach."),
            };

            if let Ok(mut conn) = connect(
                info.clone(),
                read_from_replicas,
                username.clone(),
                password.clone(),
            ) {
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
        Ok(connections)
    }

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(&self) -> RedisResult<()> {
        let mut slots = self.slots.borrow_mut();
        *slots = self.create_new_slots(|slot_data| {
            let replica = if !self.read_from_replicas || slot_data.replicas().is_empty() {
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

                if let Ok(mut conn) = connect(
                    addr.as_ref(),
                    self.read_from_replicas,
                    self.username.clone(),
                    self.password.clone(),
                ) {
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
            if let Ok(mut slots_data) = get_slots(conn, self.tls) {
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
            let conn = connect(
                addr,
                self.read_from_replicas,
                self.username.clone(),
                self.password.clone(),
            )?;
            Ok(connections.entry(addr.to_string()).or_insert(conn))
        }
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

        let mut retries = 16;
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
                            redirected = err
                                .redirect_node()
                                .map(|(node, _slot)| build_connection_string(node, None, self.tls));
                            is_asking = true;
                        } else if kind == ErrorKind::Moved {
                            // Refresh slots.
                            self.refresh_slots()?;
                            excludes.clear();

                            // Request again.
                            redirected = err
                                .redirect_node()
                                .map(|(node, _slot)| build_connection_string(node, None, self.tls));
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
                        let new_connections = Self::create_initial_connections(
                            &self.initial_nodes,
                            self.read_from_replicas,
                            self.username.clone(),
                            self.password.clone(),
                        )?;
                        {
                            let mut connections = self.connections.borrow_mut();
                            *connections = new_connections;
                        }
                        self.refresh_slots()?;
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

fn connect<T: IntoConnectionInfo>(
    info: T,
    read_from_replicas: bool,
    username: Option<String>,
    password: Option<String>,
) -> RedisResult<Connection>
where
    T: std::fmt::Debug,
{
    let mut connection_info = info.into_connection_info()?;
    connection_info.redis.username = username;
    connection_info.redis.password = password;
    let client = super::Client::open(connection_info)?;

    let mut con = client.get_connection()?;
    if read_from_replicas {
        // If READONLY is sent to primary nodes, it will have no effect
        cmd("READONLY").query(&mut con)?;
    }
    Ok(con)
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
fn get_slots(connection: &mut Connection, tls_mode: Option<TlsMode>) -> RedisResult<Vec<Slot>> {
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
                        Some(build_connection_string(&ip, Some(port), tls_mode))
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

fn build_connection_string(host: &str, port: Option<u16>, tls_mode: Option<TlsMode>) -> String {
    let host_port = match port {
        Some(port) => format!("{}:{}", host, port),
        None => host.to_string(),
    };
    match tls_mode {
        None => format!("redis://{}", host_port),
        Some(TlsMode::Insecure) => {
            format!("rediss://{}/#insecure", host_port)
        }
        Some(TlsMode::Secure) => format!("rediss://{}", host_port),
    }
}
