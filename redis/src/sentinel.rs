//! Defines a Sentinel type that connects to Redis sentinels and creates clients to
//! master or replica nodes.
//!
//! # Example
//! ```rust,no_run
//! use redis::Commands;
//! use redis::sentinel::Sentinel;
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let sentinel = Sentinel::build(nodes).unwrap();
//! let mut master = sentinel.master_for("master_name", None).unwrap().get_connection().unwrap();
//! let mut replica = sentinel.replica_for("master_name", None).unwrap().get_connection().unwrap();
//!
//! let _: () = master.set("test", "test_data").unwrap();
//! let rv: String = replica.get("test").unwrap();
//!
//! assert_eq!(rv, "test_data");
//! ```
//!
//! There is also a SentinelClient which acts like a regular Client, providing the
//! `get_connection` and `get_async_connection` methods, internally using the Sentinel
//! type to create clients on demand for the desired node type (Master or Replica).
//!
//! # Example
//! ```rust,no_run
//! use redis::Commands;
//! use redis::sentinel::{ SentinelServerType, SentinelClient };
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let master_client = SentinelClient::build(nodes, String::from("master_name"), None, SentinelServerType::Master).unwrap();
//! let replica_client = SentinelClient::build(nodes, String::from("master_name"), None, SentinelServerType::Replica).unwrap();
//! let mut master_conn = master_client.get_connection().unwrap();
//! let mut replica_conn = replica_client.get_connection().unwrap();
//!
//! let _: () = master_conn.set("test", "test_data").unwrap();
//! let rv: String = replica_conn.get("test").unwrap();
//!
//! assert_eq!(rv, "test_data");
//! ```
//!
//! If the sentinel's nodes are using TLS or require authentication, a full
//! SentinelNodeConnectionInfo struct may be used instead of just the master's name:
//!
//! # Example
//! ```rust,no_run
//! use redis::{ Commands, RedisConnectionInfo };
//! use redis::sentinel::{ Sentinel, SentinelNodeConnectionInfo };
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let sentinel = Sentinel::build(nodes).unwrap();
//!
//! let mut master_with_auth = sentinel
//!     .master_for(
//!         "master_name",
//!         Some(&SentinelNodeConnectionInfo {
//!             tls_mode: None,
//!             redis_connection_info: Some(RedisConnectionInfo {
//!                 db: 1,
//!                 username: Some(String::from("foo")),
//!                 password: Some(String::from("bar")),
//!             }),
//!         }),
//!     )
//!     .unwrap()
//!     .get_connection()
//!     .unwrap();
//!
//! let mut replica_with_tls = sentinel
//!     .master_for(
//!         "master_name",
//!         Some(&SentinelNodeConnectionInfo {
//!             tls_mode: Some(redis::cluster::TlsMode::Secure),
//!             redis_connection_info: None,
//!         }),
//!     )
//!     .unwrap()
//!     .get_connection()
//!     .unwrap();
//! ```
//!
//! # Example
//! ```rust,no_run
//! use redis::{ Commands, RedisConnectionInfo };
//! use redis::sentinel::{ SentinelServerType, SentinelClient, SentinelNodeConnectionInfo };
//!
//! let master_client = SentinelClient::build(
//!     nodes,
//!     String::from("master1"),
//!     Some(SentinelNodeConnectionInfo {
//!         tls_mode: Some(redis::cluster::TlsMode::Insecure),
//!         redis_connection_info: Some(RedisConnectionInfo {
//!             db: 0,
//!             username: Some(String::from("user")),
//!             password: Some(String::from("pass")),
//!         }),
//!     }),
//!     redis::sentinel::SentinelServerType::Master,
//! )
//! .unwrap();
//! ```
//!

use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "aio")]
use std::future::Future;

#[cfg(feature = "aio")]
use futures_util::StreamExt;

use crate::{
    cluster::TlsMode, connection::ConnectionInfo, types::RedisResult, Client, Connection,
    ErrorKind, FromRedisValue, IntoConnectionInfo, RedisConnectionInfo, Value,
};

/// The Sentinel type, serves as a special purpose client which builds other clients on
/// demand.
#[derive(Debug, Clone)]
pub struct Sentinel {
    sentinels_connection_info: Vec<ConnectionInfo>,
    replica_start_index: usize,
}

/// Holds the connection information that a sentinel should use when connecting to the
/// servers (masters and replicas) belonging to it.
#[derive(Clone, Default)]
pub struct SentinelNodeConnectionInfo {
    /// The TLS mode of the connection, or None if we do not want to connect using TLS
    /// (just a plain TCP connection).
    pub tls_mode: Option<TlsMode>,

    /// The Redis specific/connection independent information to be used.
    pub redis_connection_info: Option<RedisConnectionInfo>,
}

impl SentinelNodeConnectionInfo {
    fn create_connection_info(&self, ip: String, port: u16) -> ConnectionInfo {
        let addr = match self.tls_mode {
            None => crate::ConnectionAddr::Tcp(ip, port),
            Some(TlsMode::Secure) => crate::ConnectionAddr::TcpTls {
                host: ip,
                port,
                insecure: false,
            },
            Some(TlsMode::Insecure) => crate::ConnectionAddr::TcpTls {
                host: ip,
                port,
                insecure: true,
            },
        };

        ConnectionInfo {
            addr,
            redis: self.redis_connection_info.clone().unwrap_or_default(),
        }
    }
}

impl Default for &SentinelNodeConnectionInfo {
    fn default() -> Self {
        static DEFAULT_VALUE: SentinelNodeConnectionInfo = SentinelNodeConnectionInfo {
            tls_mode: None,
            redis_connection_info: None,
        };
        &DEFAULT_VALUE
    }
}

fn sentinel_masters_cmd() -> crate::Cmd {
    let mut cmd = crate::cmd("SENTINEL");
    cmd.arg("MASTERS");
    cmd
}

fn sentinel_replicas_cmd(master_name: &str) -> crate::Cmd {
    let mut cmd = crate::cmd("SENTINEL");
    cmd.arg("SLAVES"); // For compatibility with older redis versions
    cmd.arg(master_name);
    cmd
}

/// Async version of [try_sentinel_masters].
#[cfg(feature = "aio")]
async fn async_try_sentinel_masters(
    connection_info: &ConnectionInfo,
) -> RedisResult<Vec<HashMap<String, String>>> {
    let sentinel_client = Client::open(connection_info.clone())?;
    let mut conn = sentinel_client.get_async_connection().await?;
    sentinel_masters_cmd().query_async(&mut conn).await
}

#[cfg(feature = "aio")]
async fn async_try_sentinel_replicas(
    connection_info: &ConnectionInfo,
    master_name: &str,
) -> RedisResult<Vec<HashMap<String, String>>> {
    let sentinel_client = Client::open(connection_info.clone())?;
    let mut conn = sentinel_client.get_async_connection().await?;
    sentinel_replicas_cmd(master_name)
        .query_async(&mut conn)
        .await
}

/// Executes the command SENTINEL MASTERS in the given server (which we expect to be
/// a sentinel server).
fn try_sentinel_masters(
    connection_info: &ConnectionInfo,
) -> RedisResult<Vec<HashMap<String, String>>> {
    let sentinel_client = Client::open(connection_info.clone())?;
    let mut conn = sentinel_client.get_connection()?;
    sentinel_masters_cmd().query(&mut conn)
}

fn try_sentinel_replicas(
    connection_info: &ConnectionInfo,
    master_name: &str,
) -> RedisResult<Vec<HashMap<String, String>>> {
    let sentinel_client = Client::open(connection_info.clone())?;
    let mut conn = sentinel_client.get_connection()?;
    sentinel_replicas_cmd(master_name).query(&mut conn)
}

fn is_master_valid(master_info: &HashMap<String, String>, service_name: &str) -> bool {
    master_info.get("name").map(|s| s.as_str()) == Some(service_name)
        && master_info.contains_key("ip")
        && master_info.contains_key("port")
        && master_info.get("flags").map_or(false, |flags| {
            flags.contains("master") && !flags.contains("s_down") && !flags.contains("o_down")
        })
        && master_info["port"].parse::<u16>().is_ok()
}

fn is_replica_valid(replica_info: &HashMap<String, String>) -> bool {
    replica_info.contains_key("ip")
        && replica_info.contains_key("port")
        && replica_info.get("flags").map_or(false, |flags| {
            !flags.contains("s_down") && !flags.contains("o_down")
        })
        && replica_info["port"].parse::<u16>().is_ok()
}

fn random_replica_index(max: usize) -> usize {
    if max == 0 {
        0
    } else {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        (nanos as usize) % max
    }
}

fn try_connect_to_first_replica(
    addresses: &Vec<ConnectionInfo>,
    start_index: usize,
) -> Result<Client, crate::RedisError> {
    if addresses.is_empty() {
        fail!((
            ErrorKind::NoValidReplicasFound,
            "No valid replica found in sentinel for given name",
        ));
    }

    let mut last_err = None;
    for i in 0..addresses.len() {
        let index = (i + start_index) % addresses.len();
        match Client::open(addresses[index].clone()) {
            Ok(client) => return Ok(client),
            Err(err) => last_err = Some(err),
        }
    }

    // We can unwrap here because we know there is at least one error, since there is at
    // least one address, so we'll either return a client for it or store an error in
    // last_err.
    Err(last_err.expect("There should be an error because there is should be at least one address"))
}

fn valid_addrs<'a>(
    servers_info: Vec<HashMap<String, String>>,
    validate: impl Fn(&HashMap<String, String>) -> bool + 'a,
) -> impl Iterator<Item = (String, u16)> {
    servers_info
        .into_iter()
        .filter(move |info| validate(info))
        .map(|mut info| {
            // We can unwrap here because we already checked everything
            let ip = info.remove("ip").unwrap();
            let port = info["port"].parse::<u16>().unwrap();
            (ip, port)
        })
}

fn check_role_result(result: &RedisResult<Vec<Value>>, target_role: &str) -> bool {
    if let Ok(values) = result {
        if !values.is_empty() {
            if let Ok(role) = String::from_redis_value(&values[0]) {
                return role.to_ascii_lowercase() == target_role;
            }
        }
    }
    false
}

fn check_role(connection_info: &ConnectionInfo, target_role: &str) -> bool {
    if let Ok(client) = Client::open(connection_info.clone()) {
        if let Ok(mut conn) = client.get_connection() {
            let result: RedisResult<Vec<Value>> = crate::cmd("ROLE").query(&mut conn);
            return check_role_result(&result, target_role);
        }
    }
    false
}

/// Searches for a valid master with the given name in the list of masters returned by
/// a sentinel. A valid master is one which has a role of "master" (checked by running
/// the `ROLE` command and by seeing if its flags contains the "master" flag) and which
/// does not have the flags s_down or o_down set to it (these flags are returned by the
/// `SENTINEL MASTERS` command, and we expect the `masters` parameter to be the result of
/// that command).
fn find_valid_master(
    masters: Vec<HashMap<String, String>>,
    service_name: &str,
    node_connection_info: &SentinelNodeConnectionInfo,
) -> RedisResult<ConnectionInfo> {
    for (ip, port) in valid_addrs(masters, |m| is_master_valid(m, service_name)) {
        let connection_info = node_connection_info.create_connection_info(ip, port);
        if check_role(&connection_info, "master") {
            return Ok(connection_info);
        }
    }

    fail!((
        ErrorKind::MasterNameNotFound,
        "Master with given name not found in sentinel",
    ))
}

#[cfg(feature = "aio")]
async fn async_check_role(connection_info: &ConnectionInfo, target_role: &str) -> bool {
    if let Ok(client) = Client::open(connection_info.clone()) {
        if let Ok(mut conn) = client.get_async_connection().await {
            let result: RedisResult<Vec<Value>> = crate::cmd("ROLE").query_async(&mut conn).await;
            return check_role_result(&result, target_role);
        }
    }
    false
}

/// Async version of [find_valid_master].
#[cfg(feature = "aio")]
async fn async_find_valid_master(
    masters: Vec<HashMap<String, String>>,
    service_name: &str,
    node_connection_info: &SentinelNodeConnectionInfo,
) -> RedisResult<ConnectionInfo> {
    for (ip, port) in valid_addrs(masters, |m| is_master_valid(m, service_name)) {
        let connection_info = node_connection_info.create_connection_info(ip, port);
        if async_check_role(&connection_info, "master").await {
            return Ok(connection_info);
        }
    }

    fail!((
        ErrorKind::MasterNameNotFound,
        "Master with given name not found in sentinel",
    ))
}

fn get_valid_replicas_addresses(
    replicas: Vec<HashMap<String, String>>,
    node_connection_info: &SentinelNodeConnectionInfo,
) -> Vec<ConnectionInfo> {
    valid_addrs(replicas, is_replica_valid)
        .map(|(ip, port)| node_connection_info.create_connection_info(ip, port))
        .filter(|connection_info| check_role(connection_info, "slave"))
        .collect()
}

#[cfg(feature = "aio")]
async fn async_get_valid_replicas_addresses<'a>(
    replicas: Vec<HashMap<String, String>>,
    node_connection_info: &SentinelNodeConnectionInfo,
) -> Vec<ConnectionInfo> {
    async fn is_replica_role_valid(connection_info: ConnectionInfo) -> Option<ConnectionInfo> {
        if async_check_role(&connection_info, "slave").await {
            Some(connection_info)
        } else {
            None
        }
    }

    futures_util::stream::iter(valid_addrs(replicas, is_replica_valid))
        .map(|(ip, port)| node_connection_info.create_connection_info(ip, port))
        .filter_map(is_replica_role_valid)
        .collect()
        .await
}

// async methods
#[cfg(feature = "aio")]
impl Sentinel {
    /// Async version of [Sentinel::try_all_sentinels].
    async fn async_try_all_sentinels<'a, F, T, Fut>(&'a self, f: F) -> RedisResult<T>
    where
        F: Fn(&'a ConnectionInfo) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        let mut last_err = None;
        for connection_info in self.sentinels_connection_info.iter() {
            match f(connection_info).await {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    last_err = Some(err);
                }
            }
        }

        // We can unwrap here because we know there is at least one connection info.
        // Maybe we should have a more specific error for this situation?
        Err(last_err.expect("There should be at least one connection info"))
    }

    /// Async version of [Sentinel::get_sentinel_masters].
    async fn async_get_sentinel_masters(&self) -> RedisResult<Vec<HashMap<String, String>>> {
        self.async_try_all_sentinels(async_try_sentinel_masters)
            .await
    }

    async fn async_get_sentinel_replicas(
        &self,
        service_name: &str,
    ) -> RedisResult<Vec<HashMap<String, String>>> {
        self.async_try_all_sentinels(|conn_info| {
            async_try_sentinel_replicas(conn_info, service_name)
        })
        .await
    }

    async fn async_find_master_address<'a>(
        &self,
        service_name: &str,
        node_connection_info: &SentinelNodeConnectionInfo,
    ) -> RedisResult<ConnectionInfo> {
        let masters = self.async_get_sentinel_masters().await?;
        async_find_valid_master(masters, service_name, node_connection_info).await
    }

    async fn async_find_valid_replica_addresses<'a>(
        &self,
        service_name: &str,
        node_connection_info: &SentinelNodeConnectionInfo,
    ) -> RedisResult<Vec<ConnectionInfo>> {
        let replicas = self.async_get_sentinel_replicas(service_name).await?;
        Ok(async_get_valid_replicas_addresses(replicas, node_connection_info).await)
    }

    /// Determines the masters address for the given name, and returns a client for that
    /// master.
    ///
    /// To determine the master we ask the sentinels one by one until one of
    /// then correctly responds to the SENTINEL MASTERS command, and then we check to
    /// see if there is a master with the given name; if there is, we create a
    /// client with that address (ip and port) and the extra information from the
    /// `node_connection_info` parameter.
    pub async fn async_master_for(
        &self,
        service_name: &str,
        node_connection_info: Option<&SentinelNodeConnectionInfo>,
    ) -> RedisResult<Client> {
        let address = self
            .async_find_master_address(service_name, node_connection_info.unwrap_or_default())
            .await?;
        Client::open(address)
    }

    /// Connects to the first available replica of the given master name.
    ///
    /// After listing the valid replicas (those without a sdown or odown flag) for the
    /// given master name, we start trying to connect to then one by one, starting at a
    /// random index (simple random derived from the nanoseconds part of the current
    /// unix timestamp).
    pub async fn async_replica_for(
        &self,
        service_name: &str,
        node_connection_info: Option<&SentinelNodeConnectionInfo>,
    ) -> RedisResult<Client> {
        let addresses = self
            .async_find_valid_replica_addresses(
                service_name,
                node_connection_info.unwrap_or_default(),
            )
            .await?;
        let start_index = random_replica_index(addresses.len());
        try_connect_to_first_replica(&addresses, start_index)
    }

    /// Connects to the first available replica of the given master name, starting in a
    /// different (incremented) index each time the function is called.
    ///
    /// After listing the valid replicas (those without a sdown or odown flag) for the
    /// given master name, we start trying to connect to then one by one, starting at a
    /// rotating index which gets incremented on each call to this function.
    pub async fn async_replica_rotate_for<'a>(
        &mut self,
        service_name: &str,
        node_connection_info: Option<&SentinelNodeConnectionInfo>,
    ) -> RedisResult<Client> {
        let addresses = self
            .async_find_valid_replica_addresses(
                service_name,
                node_connection_info.unwrap_or_default(),
            )
            .await?;
        if !addresses.is_empty() {
            self.replica_start_index = (self.replica_start_index + 1) % addresses.len();
        }
        try_connect_to_first_replica(&addresses, self.replica_start_index)
    }
}

// non-async methods
impl Sentinel {
    /// Creates a Sentinel client performing some basic
    /// checks on the URLs that might make the operation fail.
    pub fn build<T: IntoConnectionInfo>(params: Vec<T>) -> RedisResult<Sentinel> {
        if params.is_empty() {
            fail!((
                ErrorKind::EmptySentinelList,
                "At least one sentinel is required",
            ))
        }

        Ok(Sentinel {
            sentinels_connection_info: params
                .into_iter()
                .map(|p| p.into_connection_info())
                .collect::<RedisResult<Vec<ConnectionInfo>>>()?,
            replica_start_index: random_replica_index(1000000),
        })
    }

    /// Try to execute the given function in each sentinel, returning the result of the
    /// first one that executes without errors. If all return errors, we return the
    /// error of the last attempt.
    fn try_all_sentinels<F, T>(&self, f: F) -> RedisResult<T>
    where
        F: Fn(&ConnectionInfo) -> RedisResult<T>,
    {
        let mut last_err = None;
        for connection_info in self.sentinels_connection_info.iter() {
            match f(connection_info) {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    last_err = Some(err);
                }
            }
        }

        // We can unwrap here because we know there is at least one connection info.
        // Maybe we should have a more specific error for this situation?
        Err(last_err.expect("There should be at least one connection info"))
    }

    /// Get a list of all masters (using the command SENTINEL MASTERS) from the
    /// sentinels (we'll return the result of the first sentinel who responds to that
    /// command without errors).
    fn get_sentinel_masters(&self) -> RedisResult<Vec<HashMap<String, String>>> {
        self.try_all_sentinels(try_sentinel_masters)
    }

    fn get_sentinel_replicas(
        &self,
        service_name: &str,
    ) -> RedisResult<Vec<HashMap<String, String>>> {
        self.try_all_sentinels(|conn_info| try_sentinel_replicas(conn_info, service_name))
    }

    fn find_master_address(
        &self,
        service_name: &str,
        node_connection_info: &SentinelNodeConnectionInfo,
    ) -> RedisResult<ConnectionInfo> {
        let masters = self.get_sentinel_masters()?;
        find_valid_master(masters, service_name, node_connection_info)
    }

    fn find_valid_replica_addresses(
        &self,
        service_name: &str,
        node_connection_info: &SentinelNodeConnectionInfo,
    ) -> RedisResult<Vec<ConnectionInfo>> {
        let replicas = self.get_sentinel_replicas(service_name)?;
        Ok(get_valid_replicas_addresses(replicas, node_connection_info))
    }

    /// Determines the masters address for the given name, and returns a client for that
    /// master.
    ///
    /// To determine the master we ask the sentinels one by one until one of
    /// then correctly responds to the SENTINEL MASTERS command, and then we check to
    /// see if there is a master with the given name; if there is, we create a
    /// client with that address (ip and port) and the extra information from the
    /// `node_connection_info` parameter.
    pub fn master_for(
        &self,
        service_name: &str,
        node_connection_info: Option<&SentinelNodeConnectionInfo>,
    ) -> RedisResult<Client> {
        let connection_info =
            self.find_master_address(service_name, node_connection_info.unwrap_or_default())?;
        Client::open(connection_info)
    }

    /// Connects to the first available replica of the given master name.
    ///
    /// After listing the valid replicas (those without a sdown or odown flag) for the
    /// given master name, we start trying to connect to then one by one, starting at a
    /// random index (simple random derived from the nanoseconds part of the current
    /// unix timestamp).
    pub fn replica_for(
        &self,
        service_name: &str,
        node_connection_info: Option<&SentinelNodeConnectionInfo>,
    ) -> RedisResult<Client> {
        let addresses = self
            .find_valid_replica_addresses(service_name, node_connection_info.unwrap_or_default())?;
        let start_index = random_replica_index(addresses.len());
        try_connect_to_first_replica(&addresses, start_index)
    }

    /// Connects to the first available replica of the given master name, starting in a
    /// different (incremented) index each time the function is called.
    ///
    /// After listing the valid replicas (those without a sdown or odown flag) for the
    /// given master name, we start trying to connect to then one by one, starting at a
    /// rotating index which gets incremented on each call to this function.
    pub fn replica_rotate_for(
        &mut self,
        service_name: &str,
        node_connection_info: Option<&SentinelNodeConnectionInfo>,
    ) -> RedisResult<Client> {
        let addresses = self
            .find_valid_replica_addresses(service_name, node_connection_info.unwrap_or_default())?;
        if !addresses.is_empty() {
            self.replica_start_index = (self.replica_start_index + 1) % addresses.len();
        }
        try_connect_to_first_replica(&addresses, self.replica_start_index)
    }
}

/// Enum defining the server types from a sentinel's point of view.
#[derive(Debug, Clone)]
pub enum SentinelServerType {
    /// Master connections only
    Master,
    /// Replica connections only
    Replica,
}

/// An alternative to the Client type which creates connections from clients created
/// on-demand based on information fetched from the sentinels. Uses the Sentinel type
/// internally. This is basic an utility to help make it easier to use sentinels but
/// with an interface similar to the client (`get_connection` and
/// `get_async_connection`).
#[derive(Clone)]
pub struct SentinelClient {
    sentinel: Sentinel,
    service_name: String,
    node_connection_info: SentinelNodeConnectionInfo,
    server_type: SentinelServerType,
}

impl SentinelClient {
    /// Creates a SentinelClient performing some basic checks on the URLs that might
    /// result in an error.
    pub fn build<T: IntoConnectionInfo>(
        params: Vec<T>,
        service_name: String,
        node_connection_info: Option<SentinelNodeConnectionInfo>,
        server_type: SentinelServerType,
    ) -> RedisResult<Self> {
        Ok(SentinelClient {
            sentinel: Sentinel::build(params)?,
            service_name,
            node_connection_info: node_connection_info.unwrap_or_default(),
            server_type,
        })
    }

    fn get_client(&self) -> RedisResult<Client> {
        match self.server_type {
            SentinelServerType::Master => self
                .sentinel
                .master_for(self.service_name.as_str(), Some(&self.node_connection_info)),
            SentinelServerType::Replica => self
                .sentinel
                .replica_for(self.service_name.as_str(), Some(&self.node_connection_info)),
        }
    }

    /// Creates a new connection to the desired type of server (based on the
    /// service/master name, and the server type). We use a Sentinel to create a client
    /// for the target type of server, and then create a connection using that client.
    pub fn get_connection(&self) -> RedisResult<Connection> {
        let client = self.get_client()?;
        client.get_connection()
    }
}

/// To enable async support you need to chose one of the supported runtimes and active its
/// corresponding feature: `tokio-comp` or `async-std-comp`
#[cfg(feature = "aio")]
#[cfg_attr(docsrs, doc(cfg(feature = "aio")))]
impl SentinelClient {
    async fn async_get_client(&self) -> RedisResult<Client> {
        match self.server_type {
            SentinelServerType::Master => {
                self.sentinel
                    .async_master_for(self.service_name.as_str(), Some(&self.node_connection_info))
                    .await
            }
            SentinelServerType::Replica => {
                self.sentinel
                    .async_replica_for(self.service_name.as_str(), Some(&self.node_connection_info))
                    .await
            }
        }
    }

    /// Returns an async connection from the client, using the same logic from
    /// `SentinelClient::get_connection`.
    #[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
    pub async fn get_async_connection(&self) -> RedisResult<crate::aio::Connection> {
        let client = self.async_get_client().await?;
        client.get_async_connection().await
    }
}
