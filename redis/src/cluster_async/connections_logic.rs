use std::net::SocketAddr;

use super::{
    connections_container::{ClusterNode, ConnectionWithIp},
    Connect,
};
use crate::{
    aio::{ConnectionLike, Runtime},
    cluster::get_connection_info,
    cluster_client::ClusterParams,
    push_manager::PushInfo,
    ErrorKind, RedisError, RedisResult,
};

use futures::prelude::*;
use futures_util::{future::BoxFuture, join};
use tokio::sync::mpsc;
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

fn failed_management_connection<C>(
    addr: &str,
    user_conn: ConnectionWithIp<ConnectionFuture<C>>,
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
        node: AsyncClusterNode::new(user_conn, None),
        err,
    }
}

pub(crate) async fn get_or_create_conn<C>(
    addr: &str,
    node: Option<AsyncClusterNode<C>>,
    params: &ClusterParams,
    conn_type: RefreshConnectionType,
    push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
) -> RedisResult<AsyncClusterNode<C>>
where
    C: ConnectionLike + Send + Clone + Sync + Connect + 'static,
{
    if let Some(node) = node {
        // We won't check whether the DNS address of this node has changed and now points to a new IP.
        // Instead, we depend on managed Redis services to close the connection for refresh if the node has changed.
        match check_node_connections(&node, params, conn_type, addr).await {
            None => Ok(node),
            Some(conn_type) => connect_and_check(
                addr,
                params.clone(),
                None,
                conn_type,
                Some(node),
                push_sender,
            )
            .await
            .get_node(),
        }
    } else {
        connect_and_check(addr, params.clone(), None, conn_type, None, push_sender)
            .await
            .get_node()
    }
}

fn create_async_node<C>(
    user_conn: ConnectionWithIp<C>,
    management_conn: Option<ConnectionWithIp<C>>,
) -> AsyncClusterNode<C>
where
    C: ConnectionLike + Connect + Send + Sync + 'static + Clone,
{
    AsyncClusterNode::new(
        user_conn.into_future(),
        management_conn.map(|conn| conn.into_future()),
    )
}

pub(crate) async fn connect_and_check_all_connections<C>(
    addr: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
    push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
) -> ConnectAndCheckResult<C>
where
    C: ConnectionLike + Connect + Send + Sync + 'static + Clone,
{
    match future::join(
        create_connection(
            addr,
            params.clone(),
            socket_addr,
            push_sender.clone(),
            false,
        ),
        create_connection(addr, params.clone(), socket_addr, push_sender, true),
    )
    .await
    {
        (Ok(conn_1), Ok(conn_2)) => {
            // Both connections were successfully established
            let mut user_conn: ConnectionWithIp<C> = conn_1;
            let mut management_conn: ConnectionWithIp<C> = conn_2;
            if let Err(err) = setup_user_connection(&mut user_conn.conn, params).await {
                return err.into();
            }
            match setup_management_connection(&mut management_conn.conn).await {
                Ok(_) => ConnectAndCheckResult::Success(create_async_node(
                    user_conn,
                    Some(management_conn),
                )),
                Err(err) => failed_management_connection(addr, user_conn.into_future(), err),
            }
        }
        (Ok(mut connection), Err(err)) | (Err(err), Ok(mut connection)) => {
            // Only a single connection was successfully established. Use it for the user connection
            match setup_user_connection(&mut connection.conn, params).await {
                Ok(_) => failed_management_connection(addr, connection.into_future(), err),
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
    match create_connection::<C>(addr, params.clone(), socket_addr, None, true).await {
        Err(conn_err) => failed_management_connection(addr, prev_node.user_connection, conn_err),

        Ok(mut connection) => {
            if let Err(err) = setup_management_connection(&mut connection.conn).await {
                return failed_management_connection(addr, prev_node.user_connection, err);
            }

            ConnectAndCheckResult::Success(ClusterNode {
                user_connection: prev_node.user_connection,
                management_connection: Some(connection.into_future()),
            })
        }
    }
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
    push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
) -> ConnectAndCheckResult<C>
where
    C: ConnectionLike + Connect + Send + Sync + 'static + Clone,
{
    match conn_type {
        RefreshConnectionType::OnlyUserConnection => {
            let user_conn = match create_and_setup_user_connection(
                addr,
                params.clone(),
                socket_addr,
                push_sender,
            )
            .await
            {
                Ok(tuple) => tuple,
                Err(err) => return err.into(),
            };
            let management_conn = node.and_then(|node| node.management_connection);
            AsyncClusterNode::new(user_conn.into_future(), management_conn).into()
        }
        RefreshConnectionType::OnlyManagementConnection => {
            // Refreshing only the management connection requires the node to exist alongside a user connection. Otherwise, refresh all connections.
            match node {
                Some(node) => {
                    connect_and_check_only_management_conn(addr, params, socket_addr, node).await
                }
                None => {
                    connect_and_check_all_connections(addr, params, socket_addr, push_sender).await
                }
            }
        }
        RefreshConnectionType::AllConnections => {
            connect_and_check_all_connections(addr, params, socket_addr, push_sender).await
        }
    }
}

async fn create_and_setup_user_connection<C>(
    node: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
    push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
) -> RedisResult<ConnectionWithIp<C>>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let mut connection: ConnectionWithIp<C> =
        create_connection(node, params.clone(), socket_addr, push_sender, false).await?;
    setup_user_connection(&mut connection.conn, params).await?;
    Ok(connection)
}

async fn setup_user_connection<C>(conn: &mut C, params: ClusterParams) -> RedisResult<()>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let read_from_replicas = params.read_from_replicas
        != crate::cluster_slotmap::ReadFromReplicaStrategy::AlwaysFromPrimary;
    let connection_timeout = params.connection_timeout;
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
    mut params: ClusterParams,
    socket_addr: Option<SocketAddr>,
    push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
    is_management: bool,
) -> RedisResult<ConnectionWithIp<C>>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let connection_timeout = params.connection_timeout;
    let response_timeout = params.response_timeout;
    // ignore pubsub subscriptions and push notifications for management connections
    if is_management {
        params.pubsub_subscriptions = None;
    }
    let info = get_connection_info(node, params)?;
    C::connect(
        info,
        response_timeout,
        connection_timeout,
        socket_addr,
        if !is_management { push_sender } else { None },
    )
    .await
    .map(|conn| conn.into())
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
    let timeout = params.connection_timeout;
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
                Some(connection) => check(connection.conn, timeout, "management").await,
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
            let conn = node.user_connection.conn.clone();
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

async fn check_connection<C>(conn: &mut C, timeout: std::time::Duration) -> RedisResult<()>
where
    C: ConnectionLike + Send + 'static,
{
    Runtime::locate()
        .timeout(timeout, crate::cmd("PING").query_async::<_, String>(conn))
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
