//! Defines a Sentinel type that connects to Redis sentinels and creates clients to
//! master or replica nodes.
//!
//! # Example
//! ```rust,no_run
//! use redis::Commands;
//! use redis::sentinel::Sentinel;
//!
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let mut sentinel = Sentinel::build(nodes).unwrap();
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
//! let mut master_client = SentinelClient::build(nodes.clone(), String::from("master_name"), None, SentinelServerType::Master).unwrap();
//! let mut replica_client = SentinelClient::build(nodes, String::from("master_name"), None, SentinelServerType::Replica).unwrap();
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
//! let mut sentinel = Sentinel::build(nodes).unwrap();
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
//!             tls_mode: Some(redis::TlsMode::Secure),
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
//! let nodes = vec!["redis://127.0.0.1:6379/", "redis://127.0.0.1:6378/", "redis://127.0.0.1:6377/"];
//! let mut master_client = SentinelClient::build(
//!     nodes,
//!     String::from("master1"),
//!     Some(SentinelNodeConnectionInfo {
//!         tls_mode: Some(redis::TlsMode::Insecure),
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

use std::{collections::HashMap, num::NonZeroUsize};

#[cfg(feature = "aio")]
use futures_util::StreamExt;
use rand::Rng;

#[cfg(feature = "aio")]
use crate::aio::MultiplexedConnection as AsyncConnection;

use crate::{
    connection::ConnectionInfo, types::RedisResult, Client, Cmd, Connection, ErrorKind,
    FromRedisValue, IntoConnectionInfo, RedisConnectionInfo, TlsMode, Value,
};

/// The Sentinel type, serves as a special purpose client which builds other clients on
/// demand.
pub struct Sentinel {
    sentinels_connection_info: Vec<ConnectionInfo>,
    connections_cache: Vec<Option<Connection>>,
    #[cfg(feature = "aio")]
    async_connections_cache: Vec<Option<AsyncConnection>>,
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
                tls_params: None,
            },
            Some(TlsMode::Insecure) => crate::ConnectionAddr::TcpTls {
                host: ip,
                port,
                insecure: true,
                tls_params: None,
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

/// Generates a random value in the 0..max range.
fn random_replica_index(max: NonZeroUsize) -> usize {
    rand::thread_rng().gen_range(0..max.into())
}

fn try_connect_to_first_replica(
    addresses: &[ConnectionInfo],
    start_index: Option<usize>,
) -> Result<Client, crate::RedisError> {
    if addresses.is_empty() {
        fail!((
            ErrorKind::NoValidReplicasFoundBySentinel,
            "No valid replica found in sentinel for given name",
        ));
    }

    let start_index = start_index.unwrap_or(0);

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
        ErrorKind::MasterNameNotFoundBySentinel,
        "Master with given name not found in sentinel",
    ))
}

#[cfg(feature = "aio")]
async fn async_check_role(connection_info: &ConnectionInfo, target_role: &str) -> bool {
    if let Ok(client) = Client::open(connection_info.clone()) {
        if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
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
        ErrorKind::MasterNameNotFoundBySentinel,
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

#[cfg(feature = "aio")]
async fn async_reconnect(
    connection: &mut Option<AsyncConnection>,
    connection_info: &ConnectionInfo,
) -> RedisResult<()> {
    let sentinel_client = Client::open(connection_info.clone())?;
    let new_connection = sentinel_client.get_multiplexed_async_connection().await?;
    connection.replace(new_connection);
    Ok(())
}

#[cfg(feature = "aio")]
async fn async_try_single_sentinel<T: FromRedisValue>(
    cmd: Cmd,
    connection_info: &ConnectionInfo,
    cached_connection: &mut Option<AsyncConnection>,
) -> RedisResult<T> {
    if cached_connection.is_none() {
        async_reconnect(cached_connection, connection_info).await?;
    }

    let result = cmd.query_async(cached_connection.as_mut().unwrap()).await;

    if let Err(err) = result {
        if err.is_unrecoverable_error() || err.is_io_error() {
            async_reconnect(cached_connection, connection_info).await?;
            cmd.query_async(cached_connection.as_mut().unwrap()).await
        } else {
            Err(err)
        }
    } else {
        result
    }
}

fn reconnect(
    connection: &mut Option<Connection>,
    connection_info: &ConnectionInfo,
) -> RedisResult<()> {
    let sentinel_client = Client::open(connection_info.clone())?;
    let new_connection = sentinel_client.get_connection()?;
    connection.replace(new_connection);
    Ok(())
}

fn try_single_sentinel<T: FromRedisValue>(
    cmd: Cmd,
    connection_info: &ConnectionInfo,
    cached_connection: &mut Option<Connection>,
) -> RedisResult<T> {
    if cached_connection.is_none() {
        reconnect(cached_connection, connection_info)?;
    }

    let result = cmd.query(cached_connection.as_mut().unwrap());

    if let Err(err) = result {
        if err.is_unrecoverable_error() || err.is_io_error() {
            reconnect(cached_connection, connection_info)?;
            cmd.query(cached_connection.as_mut().unwrap())
        } else {
            Err(err)
        }
    } else {
        result
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

        let sentinels_connection_info = params
            .into_iter()
            .map(|p| p.into_connection_info())
            .collect::<RedisResult<Vec<ConnectionInfo>>>()?;

        let mut connections_cache = vec![];
        connections_cache.resize_with(sentinels_connection_info.len(), Default::default);

        #[cfg(feature = "aio")]
        {
            let mut async_connections_cache = vec![];
            async_connections_cache.resize_with(sentinels_connection_info.len(), Default::default);

            Ok(Sentinel {
                sentinels_connection_info,
                connections_cache,
                async_connections_cache,
                replica_start_index: random_replica_index(NonZeroUsize::new(1000000).unwrap()),
            })
        }

        #[cfg(not(feature = "aio"))]
        {
            Ok(Sentinel {
                sentinels_connection_info,
                connections_cache,
                replica_start_index: random_replica_index(NonZeroUsize::new(1000000).unwrap()),
            })
        }
    }

    /// Try to execute the given command in each sentinel, returning the result of the
    /// first one that executes without errors. If all return errors, we return the
    /// error of the last attempt.
    ///
    /// For each sentinel, we first check if there is a cached connection, and if not
    /// we attempt to connect to it (skipping that sentinel if there is an error during
    /// the connection). Then, we attempt to execute the given command with the cached
    /// connection. If there is an error indicating that the connection is invalid, we
    /// reconnect and try one more time in the new connection.
    ///
    fn try_all_sentinels<T: FromRedisValue>(&mut self, cmd: Cmd) -> RedisResult<T> {
        let mut last_err = None;
        for (connection_info, cached_connection) in self
            .sentinels_connection_info
            .iter()
            .zip(self.connections_cache.iter_mut())
        {
            match try_single_sentinel(cmd.clone(), connection_info, cached_connection) {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    last_err = Some(err);
                }
            }
        }

        // We can unwrap here because we know there is at least one connection info.
        Err(last_err.expect("There should be at least one connection info"))
    }

    /// Get a list of all masters (using the command SENTINEL MASTERS) from the
    /// sentinels.
    fn get_sentinel_masters(&mut self) -> RedisResult<Vec<HashMap<String, String>>> {
        self.try_all_sentinels(sentinel_masters_cmd())
    }

    fn get_sentinel_replicas(
        &mut self,
        service_name: &str,
    ) -> RedisResult<Vec<HashMap<String, String>>> {
        self.try_all_sentinels(sentinel_replicas_cmd(service_name))
    }

    fn find_master_address(
        &mut self,
        service_name: &str,
        node_connection_info: &SentinelNodeConnectionInfo,
    ) -> RedisResult<ConnectionInfo> {
        let masters = self.get_sentinel_masters()?;
        find_valid_master(masters, service_name, node_connection_info)
    }

    fn find_valid_replica_addresses(
        &mut self,
        service_name: &str,
        node_connection_info: &SentinelNodeConnectionInfo,
    ) -> RedisResult<Vec<ConnectionInfo>> {
        let replicas = self.get_sentinel_replicas(service_name)?;
        Ok(get_valid_replicas_addresses(replicas, node_connection_info))
    }

    /// Determines the masters address for the given name, and returns a client for that
    /// master.
    pub fn master_for(
        &mut self,
        service_name: &str,
        node_connection_info: Option<&SentinelNodeConnectionInfo>,
    ) -> RedisResult<Client> {
        let connection_info =
            self.find_master_address(service_name, node_connection_info.unwrap_or_default())?;
        Client::open(connection_info)
    }

    /// Connects to a randomly chosen replica of the given master name.
    pub fn replica_for(
        &mut self,
        service_name: &str,
        node_connection_info: Option<&SentinelNodeConnectionInfo>,
    ) -> RedisResult<Client> {
        let addresses = self
            .find_valid_replica_addresses(service_name, node_connection_info.unwrap_or_default())?;
        let start_index = NonZeroUsize::new(addresses.len()).map(random_replica_index);
        try_connect_to_first_replica(&addresses, start_index)
    }

    /// Attempts to connect to a different replica of the given master name each time.
    /// There is no guarantee that we'll actually be connecting to a different replica
    /// in the next call, but in a static set of replicas (no replicas added or
    /// removed), on average we'll choose each replica the same number of times.
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
        try_connect_to_first_replica(&addresses, Some(self.replica_start_index))
    }
}

// Async versions of the public methods above, along with async versions of private
// methods required for the public methods.
#[cfg(feature = "aio")]
impl Sentinel {
    async fn async_try_all_sentinels<T: FromRedisValue>(&mut self, cmd: Cmd) -> RedisResult<T> {
        let mut last_err = None;
        for (connection_info, cached_connection) in self
            .sentinels_connection_info
            .iter()
            .zip(self.async_connections_cache.iter_mut())
        {
            match async_try_single_sentinel(cmd.clone(), connection_info, cached_connection).await {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    last_err = Some(err);
                }
            }
        }

        // We can unwrap here because we know there is at least one connection info.
        Err(last_err.expect("There should be at least one connection info"))
    }

    async fn async_get_sentinel_masters(&mut self) -> RedisResult<Vec<HashMap<String, String>>> {
        self.async_try_all_sentinels(sentinel_masters_cmd()).await
    }

    async fn async_get_sentinel_replicas<'a>(
        &mut self,
        service_name: &'a str,
    ) -> RedisResult<Vec<HashMap<String, String>>> {
        self.async_try_all_sentinels(sentinel_replicas_cmd(service_name))
            .await
    }

    async fn async_find_master_address<'a>(
        &mut self,
        service_name: &str,
        node_connection_info: &SentinelNodeConnectionInfo,
    ) -> RedisResult<ConnectionInfo> {
        let masters = self.async_get_sentinel_masters().await?;
        async_find_valid_master(masters, service_name, node_connection_info).await
    }

    async fn async_find_valid_replica_addresses<'a>(
        &mut self,
        service_name: &str,
        node_connection_info: &SentinelNodeConnectionInfo,
    ) -> RedisResult<Vec<ConnectionInfo>> {
        let replicas = self.async_get_sentinel_replicas(service_name).await?;
        Ok(async_get_valid_replicas_addresses(replicas, node_connection_info).await)
    }

    /// Determines the masters address for the given name, and returns a client for that
    /// master.
    pub async fn async_master_for(
        &mut self,
        service_name: &str,
        node_connection_info: Option<&SentinelNodeConnectionInfo>,
    ) -> RedisResult<Client> {
        let address = self
            .async_find_master_address(service_name, node_connection_info.unwrap_or_default())
            .await?;
        Client::open(address)
    }

    /// Connects to a randomly chosen replica of the given master name.
    pub async fn async_replica_for(
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
        let start_index = NonZeroUsize::new(addresses.len()).map(random_replica_index);
        try_connect_to_first_replica(&addresses, start_index)
    }

    /// Attempts to connect to a different replica of the given master name each time.
    /// There is no guarantee that we'll actually be connecting to a different replica
    /// in the next call, but in a static set of replicas (no replicas added or
    /// removed), on average we'll choose each replica the same number of times.
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
        try_connect_to_first_replica(&addresses, Some(self.replica_start_index))
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
/// `get_async_connection`). The type of server (master or replica) and name of the
/// desired master are specified when constructing an instance, so it will always
/// return connections to the same target (for example, always to the master with name
/// "mymaster123", or always to replicas of the master "another-master-abc").
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

    fn get_client(&mut self) -> RedisResult<Client> {
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
    pub fn get_connection(&mut self) -> RedisResult<Connection> {
        let client = self.get_client()?;
        client.get_connection()
    }
}

/// To enable async support you need to chose one of the supported runtimes and active its
/// corresponding feature: `tokio-comp` or `async-std-comp`
#[cfg(feature = "aio")]
#[cfg_attr(docsrs, doc(cfg(feature = "aio")))]
impl SentinelClient {
    async fn async_get_client(&mut self) -> RedisResult<Client> {
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
    pub async fn get_async_connection(&mut self) -> RedisResult<AsyncConnection> {
        let client = self.async_get_client().await?;
        client.get_multiplexed_async_connection().await
    }
}
