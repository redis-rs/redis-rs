use arcstr::ArcStr;

use crate::{
    Cmd, ConnectionAddr, ConnectionInfo, ErrorKind, RedisConnectionInfo, RedisError, RedisResult,
    TlsMode, cluster_handling::client::ClusterParams, connection::TlsConnParams,
};

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "cluster-async")]
pub mod async_connection;
pub mod client;
/// Routing information for cluster commands.
pub mod routing;
pub(crate) mod slot_map;
pub mod sync_connection;
pub(crate) mod topology;

/// The address of a node in a Redis Cluster.
///
/// Stores the host and port components separately, providing structured access
/// without repeated string parsing. The host may be an IPv4 address
/// (e.g. `127.0.0.1`), an IPv6 address (e.g. `dead::cafe:beef`), or a hostname
/// (e.g. `redis-node-1.example.com`).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct NodeAddress {
    host: ArcStr,
    port: u16,
}

impl NodeAddress {
    /// Creates a new `NodeAddress` from a host and port.
    pub fn new(host: impl Into<ArcStr>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Returns the hostname portion of the address.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the port number.
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl fmt::Debug for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl TryFrom<&str> for NodeAddress {
    type Error = RedisError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let (host, port) = split_node_address(s)?;
        Ok(Self::new(host, port))
    }
}

impl TryFrom<&ConnectionAddr> for NodeAddress {
    type Error = RedisError;

    fn try_from(addr: &ConnectionAddr) -> Result<Self, Self::Error> {
        match addr {
            ConnectionAddr::Tcp(host, port) | ConnectionAddr::TcpTls { host, port, .. } => {
                Ok(NodeAddress::new(host.as_str(), *port))
            }
            ConnectionAddr::Unix(_) => Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "Unix sockets are not supported in cluster mode",
            ))),
        }
    }
}

impl PartialEq<str> for NodeAddress {
    fn eq(&self, other: &str) -> bool {
        if let Ok(parsed) = NodeAddress::try_from(other) {
            self == &parsed
        } else {
            false
        }
    }
}

impl PartialEq<&str> for NodeAddress {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

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

pub(crate) fn get_connection_info(
    node: &NodeAddress,
    cluster_params: &ClusterParams,
) -> ConnectionInfo {
    ConnectionInfo {
        addr: get_connection_addr(
            node.host().to_string(),
            node.port(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_node_address() {
        let cases = vec![
            ("127.0.0.1:6379", "127.0.0.1", 6379),
            ("localhost.localdomain:6379", "localhost.localdomain", 6379),
            ("dead::cafe:beef:30001", "dead::cafe:beef", 30001),
            ("[fe80::cafe:beef%en1]:30001", "fe80::cafe:beef%en1", 30001),
        ];

        for (input, expected_host, expected_port) in cases {
            let addr = NodeAddress::try_from(input).unwrap();
            assert_eq!(addr.host(), expected_host);
            assert_eq!(addr.port(), expected_port);
        }
    }

    #[test]
    fn reject_invalid_node_address() {
        let cases = vec![":0", "[]:6379"];
        for input in cases {
            let res = NodeAddress::try_from(input);
            assert_eq!(
                res.err(),
                Some(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "Invalid node string",
                ))),
            );
        }
    }
}
