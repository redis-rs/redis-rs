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
//! let mut master = sentinel.master_for("master_name").unwrap().get_connection().unwrap();
//! let mut replica = sentinel.replica_for("master_name").unwrap().get_connection().unwrap();
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
//! let master_client = SentinelClient::build(nodes, "master_name", SentinelServerType::Master).unwrap();
//! let replica_client = SentinelClient::build(nodes, "master_name", SentinelServerType::Replica).unwrap();
//! let mut master_conn = master_client.get_connection().unwrap();
//! let mut replica_conn = replica_client.get_connection().unwrap();
//!
//! let _: () = master_conn.set("test", "test_data").unwrap();
//! let rv: String = replica_conn.get("test").unwrap();
//!
//! assert_eq!(rv, "test_data");
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
    connection::ConnectionInfo, types::RedisResult, Client, Connection, ErrorKind, FromRedisValue,
    IntoConnectionInfo, Value,
};

/// The Sentinel type, serves as a special purpose client which builds other clients on
/// demand.
#[derive(Debug, Clone)]
pub struct Sentinel {
    sentinels_connection_info: Vec<ConnectionInfo>,
    replica_start_index: usize,
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

#[cfg(feature = "aio")]
async fn async_try_sentinel_masters(
    connection_info: &ConnectionInfo,
) -> RedisResult<Vec<HashMap<String, String>>> {
    let sentinel_client = Client::open(connection_info.clone())?;
    let mut conn = sentinel_client.get_async_connection().await?;
    let result: Vec<HashMap<String, String>> =
        sentinel_masters_cmd().query_async(&mut conn).await?;
    Ok(result)
}

#[cfg(feature = "aio")]
async fn async_try_sentinel_replicas(
    connection_info: &ConnectionInfo,
    master_name: &str,
) -> RedisResult<Vec<HashMap<String, String>>> {
    let sentinel_client = Client::open(connection_info.clone())?;
    let mut conn = sentinel_client.get_async_connection().await?;
    let result: Vec<HashMap<String, String>> = sentinel_replicas_cmd(master_name)
        .query_async(&mut conn)
        .await?;
    Ok(result)
}

fn try_sentinel_masters(
    connection_info: &ConnectionInfo,
) -> RedisResult<Vec<HashMap<String, String>>> {
    let sentinel_client = Client::open(connection_info.clone())?;
    let mut conn = sentinel_client.get_connection()?;
    let result: Vec<HashMap<String, String>> = sentinel_masters_cmd().query(&mut conn)?;
    Ok(result)
}

fn try_sentinel_replicas(
    connection_info: &ConnectionInfo,
    master_name: &str,
) -> RedisResult<Vec<HashMap<String, String>>> {
    let sentinel_client = Client::open(connection_info.clone())?;
    let mut conn = sentinel_client.get_connection()?;
    let result: Vec<HashMap<String, String>> =
        sentinel_replicas_cmd(master_name).query(&mut conn)?;
    Ok(result)
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
    addresses: &Vec<(String, u16)>,
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

    // We can unwrap here because we know there is at least one address.
    Err(last_err.expect("There should be at least one address"))
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
                if role.to_ascii_lowercase() == target_role {
                    return true;
                }
            }
        }
    }
    false
}

fn check_role(ip: &str, port: u16, target_role: &str) -> bool {
    if let Ok(client) = Client::open((ip, port)) {
        if let Ok(mut conn) = client.get_connection() {
            let result: RedisResult<Vec<Value>> = crate::cmd("ROLE").query(&mut conn);
            if check_role_result(&result, target_role) {
                return true;
            }
        }
    }
    false
}

fn get_first_valid_master(
    masters: Vec<HashMap<String, String>>,
    service_name: &str,
) -> RedisResult<(String, u16)> {
    for (ip, port) in valid_addrs(masters, |m| is_master_valid(m, service_name)) {
        if check_role(ip.as_str(), port, "master") {
            return Ok((ip, port));
        }
    }

    fail!((
        ErrorKind::MasterNameNotFound,
        "Master with given name not found in sentinel",
    ))
}

#[cfg(feature = "aio")]
async fn async_check_role(ip: &str, port: u16, target_role: &str) -> bool {
    if let Ok(client) = Client::open((ip, port)) {
        if let Ok(mut conn) = client.get_async_connection().await {
            let result: RedisResult<Vec<Value>> = crate::cmd("ROLE").query_async(&mut conn).await;
            if check_role_result(&result, target_role) {
                return true;
            }
        }
    }
    false
}

#[cfg(feature = "aio")]
async fn async_get_first_valid_master(
    masters: Vec<HashMap<String, String>>,
    service_name: &str,
) -> RedisResult<(String, u16)> {
    for (ip, port) in valid_addrs(masters, |m| is_master_valid(m, service_name)) {
        if async_check_role(ip.as_str(), port, "master").await {
            return Ok((ip, port));
        }
    }

    fail!((
        ErrorKind::MasterNameNotFound,
        "Master with given name not found in sentinel",
    ))
}

fn get_valid_replicas_addresses(replicas: Vec<HashMap<String, String>>) -> Vec<(String, u16)> {
    valid_addrs(replicas, is_replica_valid)
        .filter(|(ip, port)| check_role(ip.as_str(), *port, "slave"))
        .collect()
}

#[cfg(feature = "aio")]
async fn async_get_valid_replicas_addresses(
    replicas: Vec<HashMap<String, String>>,
) -> Vec<(String, u16)> {
    async fn is_replica_role_valid((ip, port): (String, u16)) -> Option<(String, u16)> {
        if async_check_role(ip.clone().as_str(), port, "slave").await {
            Some((ip, port))
        } else {
            None
        }
    }

    futures_util::stream::iter(valid_addrs(replicas, is_replica_valid))
        .filter_map(is_replica_role_valid)
        .collect()
        .await
}

// async methods
#[cfg(feature = "aio")]
impl Sentinel {
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

    async fn async_find_master_address(&self, service_name: &str) -> RedisResult<(String, u16)> {
        let masters = self.async_get_sentinel_masters().await?;
        async_get_first_valid_master(masters, service_name).await
    }

    async fn async_find_valid_replica_addresses(
        &self,
        service_name: &str,
    ) -> RedisResult<Vec<(String, u16)>> {
        let replicas = self.async_get_sentinel_replicas(service_name).await?;
        Ok(async_get_valid_replicas_addresses(replicas).await)
    }

    /// Determines the masters address for the given name, and returns a client for that
    /// master.
    ///
    /// To determine the master we ask the sentinels one by one until one of
    /// then correctly responds to the SENTINEL MASTERS command, and then we check to
    /// see if there is a master with the given name; if there is, we create a
    /// client with that address (ip and port).
    pub async fn async_master_for(&self, service_name: &str) -> RedisResult<Client> {
        let address = self.async_find_master_address(service_name).await?;
        Client::open(address)
    }

    /// Connects to the first available replica of the given master name.
    ///
    /// After listing the valid replicas (those without a sdown or odown flag) for the
    /// given master name, we start trying to connect to then one by one, starting at a
    /// random index (simple random derived from the nanoseconds part of the current
    /// unix timestamp).
    pub async fn async_replica_for(&self, service_name: &str) -> RedisResult<Client> {
        let addresses = self
            .async_find_valid_replica_addresses(service_name)
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
    pub async fn async_replica_rotate_for(&mut self, service_name: &str) -> RedisResult<Client> {
        let addresses = self
            .async_find_valid_replica_addresses(service_name)
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
        Ok(Sentinel {
            sentinels_connection_info: params
                .into_iter()
                .map(|p| p.into_connection_info())
                .collect::<RedisResult<Vec<ConnectionInfo>>>()?,
            replica_start_index: random_replica_index(1000000),
        })
    }

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

    fn get_sentinel_masters(&self) -> RedisResult<Vec<HashMap<String, String>>> {
        self.try_all_sentinels(try_sentinel_masters)
    }

    fn get_sentinel_replicas(
        &self,
        service_name: &str,
    ) -> RedisResult<Vec<HashMap<String, String>>> {
        self.try_all_sentinels(|conn_info| try_sentinel_replicas(conn_info, service_name))
    }

    fn find_master_address(&self, service_name: &str) -> RedisResult<(String, u16)> {
        let masters = self.get_sentinel_masters()?;
        get_first_valid_master(masters, service_name)
    }

    fn find_valid_replica_addresses(&self, service_name: &str) -> RedisResult<Vec<(String, u16)>> {
        let replicas = self.get_sentinel_replicas(service_name)?;
        Ok(get_valid_replicas_addresses(replicas))
    }

    /// Determines the masters address for the given name, and returns a client for that
    /// master.
    ///
    /// To determine the master we ask the sentinels one by one until one of
    /// then correctly responds to the SENTINEL MASTERS command, and then we check to
    /// see if there is a master with the given name; if there is, we create a
    /// client with that address (ip and port).
    pub fn master_for(&self, service_name: &str) -> RedisResult<Client> {
        let address = self.find_master_address(service_name)?;
        Client::open(address)
    }

    /// Connects to the first available replica of the given master name.
    ///
    /// After listing the valid replicas (those without a sdown or odown flag) for the
    /// given master name, we start trying to connect to then one by one, starting at a
    /// random index (simple random derived from the nanoseconds part of the current
    /// unix timestamp).
    pub fn replica_for(&self, service_name: &str) -> RedisResult<Client> {
        let addresses = self.find_valid_replica_addresses(service_name)?;
        let start_index = random_replica_index(addresses.len());
        try_connect_to_first_replica(&addresses, start_index)
    }

    /// Connects to the first available replica of the given master name, starting in a
    /// different (incremented) index each time the function is called.
    ///
    /// After listing the valid replicas (those without a sdown or odown flag) for the
    /// given master name, we start trying to connect to then one by one, starting at a
    /// rotating index which gets incremented on each call to this function.
    pub fn replica_rotate_for(&mut self, service_name: &str) -> RedisResult<Client> {
        let addresses = self.find_valid_replica_addresses(service_name)?;
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
#[derive(Debug, Clone)]
pub struct SentinelClient {
    sentinel: Sentinel,
    service_name: String,
    server_type: SentinelServerType,
}

impl SentinelClient {
    /// Creates a SentinelClient performing some basic checks on the URLs that might
    /// result in an error.
    pub fn build<T: IntoConnectionInfo>(
        params: Vec<T>,
        service_name: String,
        server_type: SentinelServerType,
    ) -> RedisResult<Self> {
        Ok(SentinelClient {
            sentinel: Sentinel::build(params)?,
            service_name,
            server_type,
        })
    }

    fn get_client(&self) -> RedisResult<Client> {
        match self.server_type {
            SentinelServerType::Master => self.sentinel.master_for(self.service_name.as_str()),
            SentinelServerType::Replica => self.sentinel.replica_for(self.service_name.as_str()),
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
                    .async_master_for(self.service_name.as_str())
                    .await
            }
            SentinelServerType::Replica => {
                self.sentinel
                    .async_replica_for(self.service_name.as_str())
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
