use std::net::{IpAddr, SocketAddr};

use super::{connections_container::ClusterNode, Connect};
use crate::{
    aio::{get_socket_addrs, ConnectionLike},
    cluster::get_connection_info,
    cluster_client::ClusterParams,
    ErrorKind, RedisError, RedisResult,
};

use futures::prelude::*;
use futures_time::future::FutureExt;
use futures_util::{future::BoxFuture, join};
use tracing::warn;

pub(crate) type ConnectionFuture<C> = futures::future::Shared<BoxFuture<'static, C>>;
/// Cluster node for async connections
#[doc(hidden)]
pub type AsyncClusterNode<C> = ClusterNode<ConnectionFuture<C>>;

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RefreshConnectionType {
    // Refresh only user connections
    OnlyUserConnection,
    // Refresh only management connections
    OnlyManagementConnection,
    // Refresh all connections: both management and user connections.
    AllConnections,
}

fn to_future<C>(conn: C) -> ConnectionFuture<C>
where
    C: Clone + Send + 'static,
{
    async { conn }.boxed().shared()
}

/// Return true if a DNS change is detected, otherwise return false.
/// This function takes a node's address, examines if its host has encountered a DNS change, where the node's endpoint now leads to a different IP address.
/// If no socket addresses are discovered for the node's host address, or if it's a non-DNS address, it returns false.
/// In case the node's host address resolves to socket addresses and none of them match the current connection's IP,
/// a DNS change is detected, so the current connection isn't valid anymore and a new connection should be made.
async fn has_dns_changed(addr: &str, curr_ip: &IpAddr) -> bool {
    let (host, port) = match get_host_and_port_from_addr(addr) {
        Some((host, port)) => (host, port),
        None => return false,
    };
    let mut updated_addresses = match get_socket_addrs(host, port).await {
        Ok(socket_addrs) => socket_addrs,
        Err(_) => return false,
    };

    !updated_addresses.any(|socket_addr| socket_addr.ip() == *curr_ip)
}

fn failed_management_connection<C>(
    addr: &str,
    user_conn: ConnectionFuture<C>,
    ip: Option<IpAddr>,
    err: RedisError,
) -> ConnectAndCheckResult<C>
where
    C: ConnectionLike + Send + Clone + Sync + Connect + 'static,
{
    warn!(
        "Failed to create management connection for node `{:?}`. Error: `{:?}`",
        addr, err
    );
    ConnectAndCheckResult::ManagementConnectionFailed {
        node: AsyncClusterNode::new(user_conn, None, ip),
        err,
    }
}

pub(crate) async fn get_or_create_conn<C>(
    addr: &str,
    node: Option<AsyncClusterNode<C>>,
    params: &ClusterParams,
    conn_type: RefreshConnectionType,
) -> RedisResult<AsyncClusterNode<C>>
where
    C: ConnectionLike + Send + Clone + Sync + Connect + 'static,
{
    if let Some(node) = node {
        // We won't check whether the DNS address of this node has changed and now points to a new IP.
        // Instead, we depend on managed Redis services to close the connection for refresh if the node has changed.
        match check_node_connections(&node, params, conn_type, addr).await {
            None => Ok(node),
            Some(conn_type) => connect_and_check(addr, params.clone(), None, conn_type, Some(node))
                .await
                .get_node(),
        }
    } else {
        connect_and_check(addr, params.clone(), None, conn_type, None)
            .await
            .get_node()
    }
}

fn warn_mismatch_ip(addr: &str, new_ip: Option<IpAddr>, prev_ip: Option<IpAddr>) {
    warn!(
        "New IP was found for node {:?}: 
                new connection IP = {:?}, previous connection IP = {:?}",
        addr, new_ip, prev_ip
    );
}

fn create_async_node<C>(
    user_conn: C,
    management_conn: Option<C>,
    ip: Option<IpAddr>,
) -> AsyncClusterNode<C>
where
    C: ConnectionLike + Connect + Send + Sync + 'static + Clone,
{
    let user_conn = to_future(user_conn);
    let management_conn = management_conn.map(to_future);

    AsyncClusterNode::new(user_conn, management_conn, ip)
}

pub(crate) async fn connect_and_check_all_connections<C>(
    addr: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
) -> ConnectAndCheckResult<C>
where
    C: ConnectionLike + Connect + Send + Sync + 'static + Clone,
{
    match future::join(
        create_connection(addr, params.clone(), socket_addr),
        create_connection(addr, params.clone(), socket_addr),
    )
    .await
    {
        (Ok(conn_1), Ok(conn_2)) => {
            // Both connections were successfully established
            let (mut user_conn, mut user_ip): (C, Option<IpAddr>) = conn_1;
            let (mut management_conn, management_ip): (C, Option<IpAddr>) = conn_2;
            if user_ip == management_ip {
                // Set up both connections
                if let Err(err) = setup_user_connection(&mut user_conn, params).await {
                    return err.into();
                }
                match setup_management_connection(&mut management_conn).await {
                    Ok(_) => ConnectAndCheckResult::Success(create_async_node(
                        user_conn,
                        Some(management_conn),
                        user_ip,
                    )),
                    Err(err) => {
                        failed_management_connection(addr, to_future(user_conn), user_ip, err)
                    }
                }
            } else {
                // Use only the connection with the latest IP address
                warn_mismatch_ip(addr, user_ip, management_ip);
                if has_dns_changed(addr, &user_ip.unwrap()).await {
                    // The user_ip is incorrect. Use the created `management_conn` for the user connection
                    user_conn = management_conn;
                    user_ip = management_ip;
                }
                match setup_user_connection(&mut user_conn, params).await {
                    Ok(_) => failed_management_connection(
                        addr,
                        to_future(user_conn),
                        user_ip,
                        (ErrorKind::IoError, "mismatched IP").into(),
                    ),
                    Err(err) => err.into(),
                }
            }
        }
        (Ok(conn), Err(err)) | (Err(err), Ok(conn)) => {
            // Only a single connection was successfully established. Use it for the user connection
            let (mut user_conn, user_ip): (C, Option<IpAddr>) = conn;
            match setup_user_connection(&mut user_conn, params).await {
                Ok(_) => failed_management_connection(addr, to_future(user_conn), user_ip, err),
                Err(err) => err.into(),
            }
        }
        (Err(err_1), Err(err_2)) => {
            // Neither of the connections succeeded.
            RedisError::from((
                ErrorKind::IoError,
                "Failed to refresh both connections",
                format!(
                    "Node: {:?} received errors: `{:?}`, `{:?}`",
                    addr, err_1, err_2
                ),
            ))
            .into()
        }
    }
}

async fn connect_and_check_only_management_conn<C>(
    addr: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
    prev_node: AsyncClusterNode<C>,
) -> ConnectAndCheckResult<C>
where
    C: ConnectionLike + Connect + Send + Sync + 'static + Clone,
{
    let (mut new_conn, new_ip) = match create_connection(addr, params.clone(), socket_addr).await {
        Ok(tuple) => tuple,
        Err(err) => {
            return failed_management_connection(
                addr,
                prev_node.user_connection,
                prev_node.ip,
                err,
            );
        }
    };

    let (user_connection, mut management_connection) = if new_ip != prev_node.ip {
        // An IP mismatch was detected. Attempt to establish a new connection to replace both the management and user connections.
        // Use the successfully established connection for the user, then proceed to create a new one for management.
        warn_mismatch_ip(addr, new_ip, prev_node.ip);
        if let Err(err) = setup_user_connection(&mut new_conn, params.clone()).await {
            return ConnectAndCheckResult::Failed(err);
        }
        let user_connection = to_future(new_conn);
        let management_connection = create_connection(addr, params.clone(), socket_addr)
            .await
            .map(|(conn, _ip)| conn)
            .ok();
        (user_connection, management_connection)
    } else {
        // The new IP matches the existing one. Use the created connection as the management connection.
        (prev_node.user_connection, Some(new_conn))
    };

    if let Some(new_conn) = management_connection.as_mut() {
        if let Err(err) = setup_management_connection(new_conn).await {
            return failed_management_connection(addr, user_connection, new_ip, err);
        };
    };
    ConnectAndCheckResult::Success(ClusterNode {
        user_connection,
        ip: new_ip,
        management_connection: management_connection.map(|conn| to_future(conn)),
    })
}

#[doc(hidden)]
#[must_use]
pub enum ConnectAndCheckResult<C> {
    // Returns a node that was fully connected according to the request.
    Success(AsyncClusterNode<C>),
    // Returns a node that failed to create a management connection, but has a working user connection.
    ManagementConnectionFailed {
        node: AsyncClusterNode<C>,
        err: RedisError,
    },
    // Request failed completely, could not return a node with any working connection.
    Failed(RedisError),
}

impl<C> ConnectAndCheckResult<C> {
    pub fn get_node(self) -> RedisResult<AsyncClusterNode<C>> {
        match self {
            ConnectAndCheckResult::Success(node) => Ok(node),
            ConnectAndCheckResult::ManagementConnectionFailed { node, .. } => Ok(node),
            ConnectAndCheckResult::Failed(err) => Err(err),
        }
    }

    pub fn get_error(self) -> Option<RedisError> {
        match self {
            ConnectAndCheckResult::Success(_) => None,
            ConnectAndCheckResult::ManagementConnectionFailed { err, .. } => Some(err),
            ConnectAndCheckResult::Failed(err) => Some(err),
        }
    }
}

impl<C> From<RedisError> for ConnectAndCheckResult<C> {
    fn from(value: RedisError) -> Self {
        ConnectAndCheckResult::Failed(value)
    }
}

impl<C> From<AsyncClusterNode<C>> for ConnectAndCheckResult<C> {
    fn from(value: AsyncClusterNode<C>) -> Self {
        ConnectAndCheckResult::Success(value)
    }
}

impl<C> From<RedisResult<AsyncClusterNode<C>>> for ConnectAndCheckResult<C> {
    fn from(value: RedisResult<AsyncClusterNode<C>>) -> Self {
        match value {
            Ok(value) => value.into(),
            Err(err) => err.into(),
        }
    }
}

#[doc(hidden)]
pub async fn connect_and_check<C>(
    addr: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
    conn_type: RefreshConnectionType,
    node: Option<AsyncClusterNode<C>>,
) -> ConnectAndCheckResult<C>
where
    C: ConnectionLike + Connect + Send + Sync + 'static + Clone,
{
    match conn_type {
        RefreshConnectionType::OnlyUserConnection => {
            let (user_conn, ip) =
                match create_and_setup_user_connection(addr, params.clone(), socket_addr).await {
                    Ok(tuple) => tuple,
                    Err(err) => return err.into(),
                };
            if let Some(node) = node {
                let mut management_conn = match node.management_connection {
                    Some(ref conn) => Some(conn.clone().await),
                    None => None,
                };
                if ip != node.ip {
                    // New IP was found, refresh the management connection too
                    management_conn =
                        create_and_setup_management_connection(addr, params, socket_addr)
                            .await
                            .ok()
                            .map(|(conn, _ip): (C, Option<IpAddr>)| conn);
                }
                create_async_node(user_conn, management_conn, ip).into()
            } else {
                create_async_node(user_conn, None, ip).into()
            }
        }
        RefreshConnectionType::OnlyManagementConnection => {
            // Refreshing only the management connection requires the node to exist alongside a user connection. Otherwise, refresh all connections.
            match node {
                Some(node) => {
                    connect_and_check_only_management_conn(addr, params, socket_addr, node).await
                }
                None => connect_and_check_all_connections(addr, params, socket_addr).await,
            }
        }
        RefreshConnectionType::AllConnections => {
            connect_and_check_all_connections(addr, params, socket_addr).await
        }
    }
}

async fn create_and_setup_user_connection<C>(
    node: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
) -> RedisResult<(C, Option<IpAddr>)>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let (mut conn, ip): (C, Option<IpAddr>) =
        create_connection(node, params.clone(), socket_addr).await?;
    setup_user_connection(&mut conn, params).await?;
    Ok((conn, ip))
}

async fn create_and_setup_management_connection<C>(
    node: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
) -> RedisResult<(C, Option<IpAddr>)>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let (mut conn, ip): (C, Option<IpAddr>) =
        create_connection(node, params.clone(), socket_addr).await?;
    setup_management_connection(&mut conn).await?;
    Ok((conn, ip))
}

async fn setup_user_connection<C>(conn: &mut C, params: ClusterParams) -> RedisResult<()>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let read_from_replicas = params.read_from_replicas
        != crate::cluster_slotmap::ReadFromReplicaStrategy::AlwaysFromPrimary;
    let connection_timeout = params.connection_timeout.into();
    check_connection(conn, connection_timeout).await?;
    if read_from_replicas {
        // If READONLY is sent to primary nodes, it will have no effect
        crate::cmd("READONLY").query_async(conn).await?;
    }
    Ok(())
}

#[doc(hidden)]
pub const MANAGEMENT_CONN_NAME: &str = "glide_management_connection";

async fn setup_management_connection<C>(conn: &mut C) -> RedisResult<()>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    crate::cmd("CLIENT")
        .arg(&["SETNAME", MANAGEMENT_CONN_NAME])
        .query_async(conn)
        .await?;
    Ok(())
}

async fn create_connection<C>(
    node: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
) -> RedisResult<(C, Option<IpAddr>)>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let connection_timeout = params.connection_timeout;
    let response_timeout = params.response_timeout;
    let info = get_connection_info(node, params)?;
    C::connect(info, response_timeout, connection_timeout, socket_addr).await
}

/// The function returns None if the checked connection/s are healthy. Otherwise, it returns the type of the unhealthy connection/s.
#[allow(dead_code)]
#[doc(hidden)]
pub async fn check_node_connections<C>(
    node: &AsyncClusterNode<C>,
    params: &ClusterParams,
    conn_type: RefreshConnectionType,
    address: &str,
) -> Option<RefreshConnectionType>
where
    C: ConnectionLike + Send + 'static + Clone,
{
    let timeout = params.connection_timeout.into();
    let (check_mgmt_connection, check_user_connection) = match conn_type {
        RefreshConnectionType::OnlyUserConnection => (false, true),
        RefreshConnectionType::OnlyManagementConnection => (true, false),
        RefreshConnectionType::AllConnections => (true, true),
    };
    let check = |conn, timeout, conn_type| async move {
        match check_connection(&mut conn.await, timeout).await {
            Ok(_) => false,
            Err(err) => {
                warn!(
                    "The {} connection for node {} is unhealthy. Error: {:?}",
                    conn_type, address, err
                );
                true
            }
        }
    };
    let (mgmt_failed, user_failed) = join!(
        async {
            if !check_mgmt_connection {
                return false;
            }
            match node.management_connection.clone() {
                Some(conn) => check(conn, timeout, "management").await,
                None => {
                    warn!("The management connection for node {} isn't set", address);
                    true
                }
            }
        },
        async {
            if !check_user_connection {
                return false;
            }
            let conn = node.user_connection.clone();
            check(conn, timeout, "user").await
        },
    );

    match (mgmt_failed, user_failed) {
        (true, true) => Some(RefreshConnectionType::AllConnections),
        (true, false) => Some(RefreshConnectionType::OnlyManagementConnection),
        (false, true) => Some(RefreshConnectionType::OnlyUserConnection),
        (false, false) => None,
    }
}

async fn check_connection<C>(conn: &mut C, timeout: futures_time::time::Duration) -> RedisResult<()>
where
    C: ConnectionLike + Send + 'static,
{
    crate::cmd("PING")
        .query_async::<_, String>(conn)
        .timeout(timeout)
        .await??;
    Ok(())
}

/// Splits a string address into host and port. If the passed address cannot be parsed, None is returned.
/// [addr] should be in the following format: "<host>:<port>".
pub(crate) fn get_host_and_port_from_addr(addr: &str) -> Option<(&str, u16)> {
    let parts: Vec<&str> = addr.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let host = parts.first().unwrap();
    let port = parts.get(1).unwrap();
    port.parse::<u16>().ok().map(|port| (*host, port))
}
