use crate::{
    Cmd, ConnectionAddr, ConnectionInfo, ErrorKind, RedisConnectionInfo, RedisError, RedisResult,
    TlsMode, cluster_handling::client::ClusterParams, connection::TlsConnParams,
};

use std::str::FromStr;

#[cfg(feature = "cluster-async")]
pub mod async_connection;
pub mod client;
/// Routing information for cluster commands.
pub mod routing;
pub(crate) mod slot_map;
pub mod sync_connection;
pub(crate) mod topology;

pub(crate) fn slot_cmd() -> Cmd {
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");
    cmd
}

pub(crate) fn split_node_address(node: &str) -> RedisResult<(&str, u16)> {
    let invalid_error =
        || RedisError::from((ErrorKind::InvalidClientConfig, "Invalid node string"));
    node.rsplit_once(':')
        .and_then(|(host, port)| {
            Some(host.trim_start_matches('[').trim_end_matches(']'))
                .filter(|h| !h.is_empty())
                .zip(u16::from_str(port).ok())
        })
        .ok_or_else(invalid_error)
}

pub(crate) fn get_connection_addr(
    host: String,
    port: u16,
    tls: Option<TlsMode>,
    tls_params: Option<TlsConnParams>,
) -> ConnectionAddr {
    match tls {
        Some(TlsMode::Secure) => ConnectionAddr::TcpTls {
            host,
            port,
            insecure: false,
            tls_params,
        },
        Some(TlsMode::Insecure) => ConnectionAddr::TcpTls {
            host,
            port,
            insecure: true,
            tls_params,
        },
        _ => ConnectionAddr::Tcp(host, port),
    }
}

// The node string passed to this function will always be in the format host:port as it is either:
// - Created by calling ConnectionAddr::to_string (unix connections are not supported in cluster mode)
// - Returned from redis via the ASK/MOVED response
pub(crate) fn get_connection_info(
    node: &str,
    cluster_params: &ClusterParams,
) -> RedisResult<ConnectionInfo> {
    let (host, port) = split_node_address(node)?;

    Ok(ConnectionInfo {
        addr: get_connection_addr(
            host.to_string(),
            port,
            cluster_params.tls,
            cluster_params.tls_params.clone(),
        ),
        redis: RedisConnectionInfo {
            password: cluster_params.password.clone(),
            username: cluster_params.username.clone(),
            protocol: cluster_params.protocol.unwrap_or_default(),
            ..Default::default()
        },
        tcp_settings: cluster_params.tcp_settings.clone(),
    })
}
