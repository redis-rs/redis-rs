use super::{AsyncClusterNode, Connect};
use crate::{
    aio::{get_socket_addrs, ConnectionLike},
    cluster::get_connection_info,
    cluster_client::ClusterParams,
    RedisResult,
};
use futures_time::future::FutureExt;
use std::{
    iter::Iterator,
    net::{IpAddr, SocketAddr},
};

/// Return true if a DNS change is detected, otherwise return false.
/// This function takes a node's address, examines if its host has encountered a DNS change, where the node's endpoint now leads to a different IP address.
/// If no socket addresses are discovered for the node's host address, or if it's a non-DNS address, it returns false.
/// In case the node's host address resolves to socket addresses and none of them match the current connection's IP,
/// a DNS change is detected, so the current connection isn't valid anymore and a new connection should be made.
async fn is_dns_changed(addr: &str, curr_ip: &IpAddr) -> bool {
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

pub(crate) async fn get_or_create_conn<C>(
    addr: &str,
    node: Option<AsyncClusterNode<C>>,
    params: &ClusterParams,
) -> RedisResult<(C, Option<IpAddr>)>
where
    C: ConnectionLike + Connect + Send + Clone + 'static,
{
    if let Some(node) = node {
        let mut conn = node.user_connection.await;
        if let Some(ref ip) = node.ip {
            if is_dns_changed(addr, ip).await {
                return connect_and_check(addr, params.clone(), None).await;
            }
        };
        match check_connection(&mut conn, params.connection_timeout.into()).await {
            Ok(_) => Ok((conn, node.ip)),
            Err(_) => connect_and_check(addr, params.clone(), None).await,
        }
    } else {
        connect_and_check(addr, params.clone(), None).await
    }
}

#[doc(hidden)]
pub async fn connect_and_check<C>(
    node: &str,
    params: ClusterParams,
    socket_addr: Option<SocketAddr>,
) -> RedisResult<(C, Option<IpAddr>)>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let read_from_replicas = params.read_from_replicas
        != crate::cluster_slotmap::ReadFromReplicaStrategy::AlwaysFromPrimary;
    let connection_timeout = params.connection_timeout;
    let response_timeout = params.response_timeout;
    let info = get_connection_info(node, params)?;
    let (mut conn, ip) =
        C::connect(info, response_timeout, connection_timeout, socket_addr).await?;
    check_connection(&mut conn, connection_timeout.into()).await?;
    if read_from_replicas {
        // If READONLY is sent to primary nodes, it will have no effect
        crate::cmd("READONLY").query_async(&mut conn).await?;
    }
    Ok((conn, ip))
}

async fn check_connection<C>(conn: &mut C, timeout: futures_time::time::Duration) -> RedisResult<()>
where
    C: ConnectionLike + Send + 'static,
{
    // TODO: Add a check to re-resolve DNS addresses to verify we that we have a connection to the right node
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
