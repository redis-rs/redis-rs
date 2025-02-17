use std::fmt;
use std::path::PathBuf;

use crate::connection::io::TlsConnParams;

#[cfg(feature = "cluster")]
use crate::TlsMode;

/// Defines the connection address.
///
/// Not all connection addresses are supported on all platforms.  For instance
/// to connect to a unix socket you need to run this on an operating system
/// that supports them.
#[derive(Clone, Debug)]
pub enum ConnectionAddr {
    /// Format for this is `(host, port)`.
    Tcp(String, u16),
    /// Format for this is `(host, port)`.
    TcpTls {
        /// Hostname
        host: String,
        /// Port
        port: u16,
        /// Disable hostname verification when connecting.
        ///
        /// # Warning
        ///
        /// You should think very carefully before you use this method. If hostname
        /// verification is not used, any valid certificate for any site will be
        /// trusted for use from any other. This introduces a significant
        /// vulnerability to man-in-the-middle attacks.
        insecure: bool,

        /// TLS certificates and client key.
        /// TODO: don't couple this to rustls?!
        tls_params: Option<TlsConnParams>,
    },
    /// Format for this is the path to the unix socket.
    Unix(PathBuf),
}

impl PartialEq for ConnectionAddr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ConnectionAddr::Tcp(host1, port1), ConnectionAddr::Tcp(host2, port2)) => {
                host1 == host2 && port1 == port2
            }
            (
                ConnectionAddr::TcpTls {
                    host: host1,
                    port: port1,
                    insecure: insecure1,
                    tls_params: _,
                },
                ConnectionAddr::TcpTls {
                    host: host2,
                    port: port2,
                    insecure: insecure2,
                    tls_params: _,
                },
            ) => port1 == port2 && host1 == host2 && insecure1 == insecure2,
            (ConnectionAddr::Unix(path1), ConnectionAddr::Unix(path2)) => path1 == path2,
            _ => false,
        }
    }
}

impl Eq for ConnectionAddr {}

impl ConnectionAddr {
    /// Checks if this address is supported.
    ///
    /// Because not all platforms support all connection addresses this is a
    /// quick way to figure out if a connection method is supported.  Currently
    /// this only affects unix connections which are only supported on unix
    /// platforms and on older versions of rust also require an explicit feature
    /// to be enabled.
    pub fn is_supported(&self) -> bool {
        match *self {
            ConnectionAddr::Tcp(_, _) => true,
            ConnectionAddr::TcpTls { .. } => {
                cfg!(any(feature = "tls-native-tls", feature = "tls-rustls"))
            }
            ConnectionAddr::Unix(_) => cfg!(unix),
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn tls_mode(&self) -> Option<TlsMode> {
        match self {
            ConnectionAddr::TcpTls { insecure, .. } => {
                if *insecure {
                    Some(TlsMode::Insecure)
                } else {
                    Some(TlsMode::Secure)
                }
            }
            _ => None,
        }
    }
}

impl fmt::Display for ConnectionAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Cluster::get_connection_info depends on the return value from this function
        match *self {
            ConnectionAddr::Tcp(ref host, port) => write!(f, "{host}:{port}"),
            ConnectionAddr::TcpTls { ref host, port, .. } => write!(f, "{host}:{port}"),
            ConnectionAddr::Unix(ref path) => write!(f, "{}", path.display()),
        }
    }
}
