use crate::{
    cluster_handling::client::ClusterParams, connection::TlsConnParams, Cmd, ConnectionAddr,
    ConnectionInfo, RedisConnectionInfo, TlsMode,
};

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

pub(crate) fn get_connection_info(
    node: &crate::cluster_handling::slot_map::Node,
    cluster_params: &ClusterParams,
) -> ConnectionInfo {
    ConnectionInfo {
        addr: get_connection_addr(
            node.host.to_string(),
            node.port,
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
    }
}
