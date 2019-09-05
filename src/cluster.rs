//! Redis cluster support.
//!
//! This module extends the library to be able to use cluster.
//! ClusterClient impletemts traits of ConnectionLike and Commands.
//!
//! Note that the cluster support currently does not provide pubsub
//! functionality.
//!
//! # Example
//! ```rust,no_run
//! use redis::Commands;
//! use redis::cluster::ClusterClient;
//!
//! fn main() {
//!     let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//!     let client = ClusterClient::open(nodes).unwrap();
//!     let mut connection = client.get_connection().unwrap();
//!
//!     let _: () = connection.set("test", "test_data").unwrap();
//!     let res: String = connection.get("test").unwrap();
//!
//!     assert_eq!(res, "test_data");
//! }
//! ```
//!
//! # Pipelining
//! ```rust,no_run
//! use redis::{PipelineCommands, pipe};
//! use redis::cluster::ClusterClient;
//!
//! fn main() {
//!     let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//!     let client = ClusterClient::open(nodes).unwrap();
//!     let mut connection = client.get_connection().unwrap();
//!
//!     let key = "test";
//!
//!     let _: () = pipe()
//!         .rpush(key, "123").ignore()
//!         .ltrim(key, -10, -1).ignore()
//!         .expire(key, 60).ignore()
//!         .query(&mut connection).unwrap();
//! }
//! ```
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Cursor};
use std::iter::Iterator;
use std::thread;
use std::time::Duration;

use rand::{
    seq::{IteratorRandom, SliceRandom},
    thread_rng,
};

use super::{
    cmd, Cmd, Connection, ConnectionAddr, ConnectionInfo, ConnectionLike, ErrorKind,
    IntoConnectionInfo, RedisError, RedisResult, Value,
};

const SLOT_SIZE: usize = 16384;

/// This is a ClusterClientBuilder of Redis cluster client.
pub struct ClusterClientBuilder {
    initial_nodes: RedisResult<Vec<ConnectionInfo>>,
    readonly: bool,
    password: Option<String>,
}

impl ClusterClientBuilder {
    /// Generate the base configuration for new Client.
    pub fn new<T: IntoConnectionInfo>(initial_nodes: Vec<T>) -> ClusterClientBuilder {
        ClusterClientBuilder {
            initial_nodes: initial_nodes
                .into_iter()
                .map(|x| x.into_connection_info())
                .collect(),
            readonly: false,
            password: None,
        }
    }

    /// Connect to a redis cluster server and return a cluster client.
    /// This does not actually open a connection yet but it performs some basic checks on the URL.
    /// The password of initial nodes must be the same all.
    ///
    /// # Errors
    ///
    /// If it is failed to parse initial_nodes or the initial nodes has different password, an error is returned.
    pub fn open(self) -> RedisResult<ClusterClient> {
        ClusterClient::open_internal(self)
    }

    /// Set password for new ClusterClient.
    pub fn password(mut self, password: String) -> ClusterClientBuilder {
        self.password = Some(password);
        self
    }

    /// Set read only mode for new ClusterClient.
    /// Default is not read only mode.
    /// When it is set to readonly mode, all query use replica nodes except there are no replica nodes.
    /// If there are no replica nodes, it use master node.
    pub fn readonly(mut self, readonly: bool) -> ClusterClientBuilder {
        self.readonly = readonly;
        self
    }
}

/// This is a Redis cluster client.
pub struct ClusterClient {
    initial_nodes: Vec<ConnectionInfo>,
    readonly: bool,
    password: Option<String>,
}

impl ClusterClient {
    /// Connect to a redis cluster server and return a cluster client.
    /// This does not actually open a connection yet but it performs some basic checks on the URL.
    /// The password of initial nodes must be the same all.
    ///
    /// # Errors
    ///
    /// If it is failed to parse initial_nodes or the initial nodes has different password, an error is returned.
    pub fn open<T: IntoConnectionInfo>(initial_nodes: Vec<T>) -> RedisResult<ClusterClient> {
        ClusterClientBuilder::new(initial_nodes).open()
    }

    /// Open and get a Redis cluster connection.
    ///
    /// # Errors
    ///
    /// If it is failed to open connections and to create slots, an error is returned.
    pub fn get_connection(&self) -> RedisResult<ClusterConnection> {
        ClusterConnection::new(
            self.initial_nodes.clone(),
            self.readonly,
            self.password.clone(),
        )
    }

    fn open_internal(builder: ClusterClientBuilder) -> RedisResult<ClusterClient> {
        let initial_nodes = builder.initial_nodes?;
        let mut nodes = Vec::with_capacity(initial_nodes.len());
        let mut connection_info_password = None::<String>;

        for (index, info) in initial_nodes.into_iter().enumerate() {
            if let ConnectionAddr::Unix(_) = *info.addr {
                return Err(RedisError::from((ErrorKind::InvalidClientConfig,
                                             "This library cannot use unix socket because Redis's cluster command returns only cluster's IP and port.")));
            }

            if builder.password.is_none() {
                if index == 0 {
                    connection_info_password = info.passwd.clone();
                } else if connection_info_password != info.passwd {
                    return Err(RedisError::from((
                        ErrorKind::InvalidClientConfig,
                        "Cannot use different password among initial nodes.",
                    )));
                }
            }

            nodes.push(info);
        }

        Ok(ClusterClient {
            initial_nodes: nodes,
            readonly: builder.readonly,
            password: builder.password.or(connection_info_password),
        })
    }
}

/// This is a connection of Redis cluster.
pub struct ClusterConnection {
    initial_nodes: Vec<ConnectionInfo>,
    connections: RefCell<HashMap<String, Connection>>,
    slots: RefCell<HashMap<u16, String>>,
    auto_reconnect: RefCell<bool>,
    readonly: bool,
    password: Option<String>,
}

impl ClusterConnection {
    fn new(
        initial_nodes: Vec<ConnectionInfo>,
        readonly: bool,
        password: Option<String>,
    ) -> RedisResult<ClusterConnection> {
        let connections =
            Self::create_initial_connections(&initial_nodes, readonly, password.clone())?;
        let connection = ClusterConnection {
            initial_nodes,
            connections: RefCell::new(connections),
            slots: RefCell::new(HashMap::with_capacity(SLOT_SIZE)),
            auto_reconnect: RefCell::new(true),
            readonly,
            password,
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
        let connections = self.connections.borrow();
        for conn in connections.values() {
            conn.set_read_timeout(dur)?;
        }
        Ok(())
    }

    /// Check that all connections it has are available.
    pub fn check_connection(&mut self) -> bool {
        let mut connections = self.connections.borrow_mut();
        for conn in connections.values_mut() {
            if !check_connection(conn) {
                return false;
            }
        }
        true
    }

    fn create_initial_connections(
        initial_nodes: &[ConnectionInfo],
        readonly: bool,
        password: Option<String>,
    ) -> RedisResult<HashMap<String, Connection>> {
        let mut connections = HashMap::with_capacity(initial_nodes.len());

        for info in initial_nodes.iter() {
            let addr = match *info.addr {
                ConnectionAddr::Tcp(ref host, port) => format!("redis://{}:{}", host, port),
                _ => panic!("No reach."),
            };

            if let Ok(mut conn) = connect(info.clone(), readonly, password.clone()) {
                if check_connection(&mut conn) {
                    connections.insert(addr, conn);
                    break;
                }
            }
        }

        if connections.is_empty() {
            return Err(RedisError::from((
                ErrorKind::IoError,
                "It is failed to check startup nodes.",
            )));
        }
        Ok(connections)
    }

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(&self) -> RedisResult<()> {
        let mut slots = self.slots.borrow_mut();
        *slots = {
            let new_slots = if self.readonly {
                let mut rng = thread_rng();
                self.create_new_slots(|slot_data| {
                    let replicas = slot_data.replicas();
                    if replicas.is_empty() {
                        slot_data.master().to_string()
                    } else {
                        replicas.choose(&mut rng).unwrap().to_string()
                    }
                })
            } else {
                self.create_new_slots(|slot_data| slot_data.master().to_string())
            };

            if new_slots.len() != SLOT_SIZE {
                return Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "Slot refresh error.",
                )));
            }
            new_slots
        };

        let mut connections = self.connections.borrow_mut();
        *connections = {
            // Remove dead connections and connect to new nodes if necessary
            let mut new_connections = HashMap::with_capacity(connections.len());

            for addr in slots.values() {
                if !new_connections.contains_key(addr) {
                    if connections.contains_key(addr) {
                        let mut conn = connections.remove(addr).unwrap();
                        if check_connection(&mut conn) {
                            new_connections.insert(addr.to_string(), conn);
                            continue;
                        }
                    }

                    if let Ok(mut conn) =
                        connect(addr.as_ref(), self.readonly, self.password.clone())
                    {
                        if check_connection(&mut conn) {
                            new_connections.insert(addr.to_string(), conn);
                        }
                    }
                }
            }
            new_connections
        };

        Ok(())
    }

    fn create_new_slots<F>(&self, mut get_addr: F) -> HashMap<u16, String>
    where
        F: FnMut(&Slot) -> String,
    {
        let mut connections = self.connections.borrow_mut();
        let mut new_slots = HashMap::with_capacity(SLOT_SIZE);
        let mut rng = thread_rng();
        let len = connections.len();
        let mut samples = connections.values_mut().choose_multiple(&mut rng, len);

        for mut conn in samples.iter_mut() {
            if let Ok(slots_data) = get_slots(&mut conn) {
                for slot_data in slots_data {
                    for slot in slot_data.start()..=slot_data.end() {
                        new_slots.insert(slot, get_addr(&slot_data));
                    }
                }
                break;
            }
        }
        new_slots
    }

    fn get_connection<'a>(
        &self,
        connections: &'a mut HashMap<String, Connection>,
        slot: u16,
    ) -> (String, &'a mut Connection) {
        let slots = self.slots.borrow();

        if let Some(addr) = slots.get(&slot) {
            if connections.contains_key(addr) {
                return (addr.clone(), connections.get_mut(addr).unwrap());
            }

            // Create new connection.
            if let Ok(mut conn) = connect(addr.as_ref(), self.readonly, self.password.clone()) {
                if check_connection(&mut conn) {
                    return (
                        addr.to_string(),
                        connections.entry(addr.to_string()).or_insert(conn),
                    );
                }
            }
        }

        // Return a random connection
        get_random_connection(connections, None)
    }

    fn request<T, F>(&self, cmd: &[u8], mut func: F) -> RedisResult<T>
    where
        F: FnMut(&mut Connection) -> RedisResult<T>,
    {
        let mut retries = 16;
        let mut excludes = HashSet::new();
        let slot = slot_for_packed_command(cmd);

        loop {
            // Get target address and response.
            let (addr, res) = {
                let mut connections = self.connections.borrow_mut();
                let (addr, mut conn) = if !excludes.is_empty() || slot.is_none() {
                    get_random_connection(&mut *connections, Some(&excludes))
                } else {
                    self.get_connection(&mut *connections, slot.unwrap())
                };
                (addr, func(&mut conn))
            };

            match res {
                Ok(res) => return Ok(res),
                Err(err) => {
                    retries -= 1;
                    if retries == 0 {
                        return Err(err);
                    }

                    if err.kind() == ErrorKind::ExtensionError {
                        let error_code = err.extension_error_code().unwrap();

                        if error_code == "MOVED" || error_code == "ASK" {
                            // Refresh slots and request again.
                            self.refresh_slots()?;
                            excludes.clear();
                            continue;
                        } else if error_code == "TRYAGAIN" || error_code == "CLUSTERDOWN" {
                            // Sleep and retry.
                            let sleep_time = 2u64.pow(16 - retries.max(9)) * 10;
                            thread::sleep(Duration::from_millis(sleep_time));
                            excludes.clear();
                            continue;
                        }
                    } else if *self.auto_reconnect.borrow()
                        && err.kind() == ErrorKind::ResponseError
                    {
                        // Reconnect when ResponseError is occurred.
                        let new_connections = Self::create_initial_connections(
                            &self.initial_nodes,
                            self.readonly,
                            self.password.clone(),
                        )?;
                        {
                            let mut connections = self.connections.borrow_mut();
                            *connections = new_connections;
                        }
                        self.refresh_slots()?;
                        excludes.clear();
                        continue;
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
}

impl ConnectionLike for ClusterConnection {
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        self.request(cmd, move |conn| conn.req_packed_command(cmd))
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        self.request(cmd, move |conn| {
            conn.req_packed_commands(cmd, offset, count)
        })
    }

    fn get_db(&self) -> i64 {
        0
    }
}

impl Clone for ClusterClient {
    fn clone(&self) -> ClusterClient {
        ClusterClient::open(self.initial_nodes.clone()).unwrap()
    }
}

fn connect<T: IntoConnectionInfo>(
    info: T,
    readonly: bool,
    password: Option<String>,
) -> RedisResult<Connection>
where
    T: std::fmt::Debug,
{
    let mut connection_info = info.into_connection_info()?;
    connection_info.passwd = password;
    let client = super::Client::open(connection_info)?;

    let mut con = client.get_connection()?;
    if readonly {
        cmd("READONLY").query(&mut con)?;
    }
    Ok(con)
}

fn check_connection(conn: &mut Connection) -> bool {
    let mut cmd = Cmd::new();
    cmd.arg("PING");
    cmd.query::<String>(conn).is_ok()
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

fn slot_for_packed_command(cmd: &[u8]) -> Option<u16> {
    let args = unpack_command(cmd);
    if args.len() > 1 {
        let key = match get_hashtag(&args[1]) {
            Some(tag) => tag,
            None => &args[1],
        };
        Some(crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE as u16)
    } else {
        None
    }
}

fn get_hashtag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|v| *v == b'{');
    let open = match open {
        Some(open) => open,
        None => return None,
    };

    let close = key[open..].iter().position(|v| *v == b'}');
    let close = match close {
        Some(close) => close,
        None => return None,
    };

    if close - open > 1 {
        Some(&key[open + 1..close])
    } else {
        None
    }
}

fn unpack_command(cmd: &[u8]) -> Vec<Vec<u8>> {
    let mut args: Vec<Vec<u8>> = Vec::new();

    let cursor = Cursor::new(cmd);
    for line in cursor.lines() {
        if let Ok(line) = line {
            if !line.starts_with('*') && !line.starts_with('$') {
                args.push(line.into_bytes());
            }
        }
    }
    args
}

#[derive(Debug)]
struct Slot {
    start: u16,
    end: u16,
    master: String,
    replicas: Vec<String>,
}

impl Slot {
    pub fn start(&self) -> u16 {
        self.start
    }
    pub fn end(&self) -> u16 {
        self.end
    }
    pub fn master(&self) -> &str {
        &self.master
    }
    #[allow(dead_code)]
    pub fn replicas(&self) -> &Vec<String> {
        &self.replicas
    }
}

// Get slot data from connection.
fn get_slots(connection: &mut Connection) -> RedisResult<Vec<Slot>> {
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");
    let packed_command = cmd.get_packed_command();
    let value = connection.req_packed_command(&packed_command)?;

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
                            port
                        } else {
                            return None;
                        };
                        Some(format!("redis://{}:{}", ip, port))
                    } else {
                        None
                    }
                })
                .collect();

            if nodes.is_empty() {
                continue;
            }

            let replicas = nodes.split_off(1);
            result.push(Slot {
                start,
                end,
                master: nodes.pop().unwrap(),
                replicas,
            });
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::{ClusterClient, ClusterClientBuilder};
    use super::{ConnectionInfo, IntoConnectionInfo};

    fn get_connection_data() -> Vec<ConnectionInfo> {
        vec![
            "redis://127.0.0.1:6379".into_connection_info().unwrap(),
            "redis://127.0.0.1:6378".into_connection_info().unwrap(),
            "redis://127.0.0.1:6377".into_connection_info().unwrap(),
        ]
    }

    fn get_connection_data_with_password() -> Vec<ConnectionInfo> {
        vec![
            "redis://:password@127.0.0.1:6379"
                .into_connection_info()
                .unwrap(),
            "redis://:password@127.0.0.1:6378"
                .into_connection_info()
                .unwrap(),
            "redis://:password@127.0.0.1:6377"
                .into_connection_info()
                .unwrap(),
        ]
    }

    #[test]
    fn give_no_password() {
        let client = ClusterClient::open(get_connection_data()).unwrap();
        assert_eq!(client.password, None);
    }

    #[test]
    fn give_password_by_initial_nodes() {
        let client = ClusterClient::open(get_connection_data_with_password()).unwrap();
        assert_eq!(client.password, Some("password".to_string()));
    }

    #[test]
    fn give_different_password_by_initial_nodes() {
        let result = ClusterClient::open(vec![
            "redis://:password1@127.0.0.1:6379",
            "redis://:password2@127.0.0.1:6378",
            "redis://:password3@127.0.0.1:6377",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn give_password_by_method() {
        let client = ClusterClientBuilder::new(get_connection_data_with_password())
            .password("pass".to_string())
            .open()
            .unwrap();
        assert_eq!(client.password, Some("pass".to_string()));
    }
}
