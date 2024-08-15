use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Write};
use std::net::{self, SocketAddr, TcpStream, ToSocketAddrs};
use std::ops::DerefMut;
use std::path::PathBuf;
use std::str::{from_utf8, FromStr};
use std::time::Duration;

use crate::cmd::{cmd, pipe, Cmd};
use crate::parser::Parser;
use crate::pipeline::Pipeline;
use crate::types::{
    from_redis_value, ErrorKind, FromRedisValue, HashMap, PushKind, RedisError, RedisResult,
    SyncPushSender, ToRedisArgs, Value,
};
use crate::{from_owned_redis_value, ProtocolVersion};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use crate::commands::resp3_hello;
#[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
use native_tls::{TlsConnector, TlsStream};

#[cfg(feature = "tls-rustls")]
use rustls::{RootCertStore, StreamOwned};
#[cfg(feature = "tls-rustls")]
use std::sync::Arc;

use crate::PushInfo;

#[cfg(all(
    feature = "tls-rustls",
    not(feature = "tls-native-tls"),
    not(feature = "tls-rustls-webpki-roots")
))]
use rustls_native_certs::load_native_certs;

#[cfg(feature = "tls-rustls")]
use crate::tls::TlsConnParams;

// Non-exhaustive to prevent construction outside this crate
#[cfg(not(feature = "tls-rustls"))]
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TlsConnParams;

static DEFAULT_PORT: u16 = 6379;

#[inline(always)]
fn connect_tcp(addr: (&str, u16)) -> io::Result<TcpStream> {
    let socket = TcpStream::connect(addr)?;
    #[cfg(feature = "tcp_nodelay")]
    socket.set_nodelay(true)?;
    #[cfg(feature = "keep-alive")]
    {
        //For now rely on system defaults
        const KEEP_ALIVE: socket2::TcpKeepalive = socket2::TcpKeepalive::new();
        //these are useless error that not going to happen
        let socket2: socket2::Socket = socket.into();
        socket2.set_tcp_keepalive(&KEEP_ALIVE)?;
        Ok(socket2.into())
    }
    #[cfg(not(feature = "keep-alive"))]
    {
        Ok(socket)
    }
}

#[inline(always)]
fn connect_tcp_timeout(addr: &SocketAddr, timeout: Duration) -> io::Result<TcpStream> {
    let socket = TcpStream::connect_timeout(addr, timeout)?;
    #[cfg(feature = "tcp_nodelay")]
    socket.set_nodelay(true)?;
    #[cfg(feature = "keep-alive")]
    {
        //For now rely on system defaults
        const KEEP_ALIVE: socket2::TcpKeepalive = socket2::TcpKeepalive::new();
        //these are useless error that not going to happen
        let socket2: socket2::Socket = socket.into();
        socket2.set_tcp_keepalive(&KEEP_ALIVE)?;
        Ok(socket2.into())
    }
    #[cfg(not(feature = "keep-alive"))]
    {
        Ok(socket)
    }
}

/// This function takes a redis URL string and parses it into a URL
/// as used by rust-url.  This is necessary as the default parser does
/// not understand how redis URLs function.
pub fn parse_redis_url(input: &str) -> Option<url::Url> {
    match url::Url::parse(input) {
        Ok(result) => match result.scheme() {
            "redis" | "rediss" | "redis+unix" | "unix" => Some(result),
            _ => None,
        },
        Err(_) => None,
    }
}

/// TlsMode indicates use or do not use verification of certification.
/// Check [ConnectionAddr](ConnectionAddr::TcpTls::insecure) for more.
#[derive(Clone, Copy)]
pub enum TlsMode {
    /// Secure verify certification.
    Secure,
    /// Insecure do not verify certification.
    Insecure,
}

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

/// Holds the connection information that redis should use for connecting.
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    /// A connection address for where to connect to.
    pub addr: ConnectionAddr,

    /// A redis connection info for how to handshake with redis.
    pub redis: RedisConnectionInfo,
}

/// Redis specific/connection independent information used to establish a connection to redis.
#[derive(Clone, Debug, Default)]
pub struct RedisConnectionInfo {
    /// The database number to use.  This is usually `0`.
    pub db: i64,
    /// Optionally a username that should be used for connection.
    pub username: Option<String>,
    /// Optionally a password that should be used for connection.
    pub password: Option<String>,
    /// Version of the protocol to use.
    pub protocol: ProtocolVersion,
}

impl FromStr for ConnectionInfo {
    type Err = RedisError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.into_connection_info()
    }
}

/// Converts an object into a connection info struct.  This allows the
/// constructor of the client to accept connection information in a
/// range of different formats.
pub trait IntoConnectionInfo {
    /// Converts the object into a connection info object.
    fn into_connection_info(self) -> RedisResult<ConnectionInfo>;
}

impl IntoConnectionInfo for ConnectionInfo {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        Ok(self)
    }
}

/// URL format: `{redis|rediss}://[<username>][:<password>@]<hostname>[:port][/<db>]`
///
/// - Basic: `redis://127.0.0.1:6379`
/// - Username & Password: `redis://user:password@127.0.0.1:6379`
/// - Password only: `redis://:password@127.0.0.1:6379`
/// - Specifying DB: `redis://127.0.0.1:6379/0`
/// - Enabling TLS: `rediss://127.0.0.1:6379`
/// - Enabling Insecure TLS: `rediss://127.0.0.1:6379/#insecure`
/// - Enabling RESP3: `redis://127.0.0.1:6379/?protocol=resp3`
impl<'a> IntoConnectionInfo for &'a str {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        match parse_redis_url(self) {
            Some(u) => u.into_connection_info(),
            None => fail!((ErrorKind::InvalidClientConfig, "Redis URL did not parse")),
        }
    }
}

impl<T> IntoConnectionInfo for (T, u16)
where
    T: Into<String>,
{
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        Ok(ConnectionInfo {
            addr: ConnectionAddr::Tcp(self.0.into(), self.1),
            redis: RedisConnectionInfo::default(),
        })
    }
}

/// URL format: `{redis|rediss}://[<username>][:<password>@]<hostname>[:port][/<db>]`
///
/// - Basic: `redis://127.0.0.1:6379`
/// - Username & Password: `redis://user:password@127.0.0.1:6379`
/// - Password only: `redis://:password@127.0.0.1:6379`
/// - Specifying DB: `redis://127.0.0.1:6379/0`
/// - Enabling TLS: `rediss://127.0.0.1:6379`
/// - Enabling Insecure TLS: `rediss://127.0.0.1:6379/#insecure`
/// - Enabling RESP3: `redis://127.0.0.1:6379/?protocol=resp3`
impl IntoConnectionInfo for String {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        match parse_redis_url(&self) {
            Some(u) => u.into_connection_info(),
            None => fail!((ErrorKind::InvalidClientConfig, "Redis URL did not parse")),
        }
    }
}

fn parse_protocol(query: &HashMap<Cow<str>, Cow<str>>) -> RedisResult<ProtocolVersion> {
    Ok(match query.get("protocol") {
        Some(protocol) => {
            if protocol == "2" || protocol == "resp2" {
                ProtocolVersion::RESP2
            } else if protocol == "3" || protocol == "resp3" {
                ProtocolVersion::RESP3
            } else {
                fail!((
                    ErrorKind::InvalidClientConfig,
                    "Invalid protocol version",
                    protocol.to_string()
                ))
            }
        }
        None => ProtocolVersion::RESP2,
    })
}

fn url_to_tcp_connection_info(url: url::Url) -> RedisResult<ConnectionInfo> {
    let host = match url.host() {
        Some(host) => {
            // Here we manually match host's enum arms and call their to_string().
            // Because url.host().to_string() will add `[` and `]` for ipv6:
            // https://docs.rs/url/latest/src/url/host.rs.html#170
            // And these brackets will break host.parse::<Ipv6Addr>() when
            // `client.open()` - `ActualConnection::new()` - `addr.to_socket_addrs()`:
            // https://doc.rust-lang.org/src/std/net/addr.rs.html#963
            // https://doc.rust-lang.org/src/std/net/parser.rs.html#158
            // IpAddr string with brackets can ONLY parse to SocketAddrV6:
            // https://doc.rust-lang.org/src/std/net/parser.rs.html#255
            // But if we call Ipv6Addr.to_string directly, it follows rfc5952 without brackets:
            // https://doc.rust-lang.org/src/std/net/ip.rs.html#1755
            match host {
                url::Host::Domain(path) => path.to_string(),
                url::Host::Ipv4(v4) => v4.to_string(),
                url::Host::Ipv6(v6) => v6.to_string(),
            }
        }
        None => fail!((ErrorKind::InvalidClientConfig, "Missing hostname")),
    };
    let port = url.port().unwrap_or(DEFAULT_PORT);
    let addr = if url.scheme() == "rediss" {
        #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
        {
            match url.fragment() {
                Some("insecure") => ConnectionAddr::TcpTls {
                    host,
                    port,
                    insecure: true,
                    tls_params: None,
                },
                Some(_) => fail!((
                    ErrorKind::InvalidClientConfig,
                    "only #insecure is supported as URL fragment"
                )),
                _ => ConnectionAddr::TcpTls {
                    host,
                    port,
                    insecure: false,
                    tls_params: None,
                },
            }
        }

        #[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]
        fail!((
            ErrorKind::InvalidClientConfig,
            "can't connect with TLS, the feature is not enabled"
        ));
    } else {
        ConnectionAddr::Tcp(host, port)
    };
    let query: HashMap<_, _> = url.query_pairs().collect();
    Ok(ConnectionInfo {
        addr,
        redis: RedisConnectionInfo {
            db: match url.path().trim_matches('/') {
                "" => 0,
                path => path.parse::<i64>().map_err(|_| -> RedisError {
                    (ErrorKind::InvalidClientConfig, "Invalid database number").into()
                })?,
            },
            username: if url.username().is_empty() {
                None
            } else {
                match percent_encoding::percent_decode(url.username().as_bytes()).decode_utf8() {
                    Ok(decoded) => Some(decoded.into_owned()),
                    Err(_) => fail!((
                        ErrorKind::InvalidClientConfig,
                        "Username is not valid UTF-8 string"
                    )),
                }
            },
            password: match url.password() {
                Some(pw) => match percent_encoding::percent_decode(pw.as_bytes()).decode_utf8() {
                    Ok(decoded) => Some(decoded.into_owned()),
                    Err(_) => fail!((
                        ErrorKind::InvalidClientConfig,
                        "Password is not valid UTF-8 string"
                    )),
                },
                None => None,
            },
            protocol: parse_protocol(&query)?,
        },
    })
}

#[cfg(unix)]
fn url_to_unix_connection_info(url: url::Url) -> RedisResult<ConnectionInfo> {
    let query: HashMap<_, _> = url.query_pairs().collect();
    Ok(ConnectionInfo {
        addr: ConnectionAddr::Unix(url.to_file_path().map_err(|_| -> RedisError {
            (ErrorKind::InvalidClientConfig, "Missing path").into()
        })?),
        redis: RedisConnectionInfo {
            db: match query.get("db") {
                Some(db) => db.parse::<i64>().map_err(|_| -> RedisError {
                    (ErrorKind::InvalidClientConfig, "Invalid database number").into()
                })?,

                None => 0,
            },
            username: query.get("user").map(|username| username.to_string()),
            password: query.get("pass").map(|password| password.to_string()),
            protocol: parse_protocol(&query)?,
        },
    })
}

#[cfg(not(unix))]
fn url_to_unix_connection_info(_: url::Url) -> RedisResult<ConnectionInfo> {
    fail!((
        ErrorKind::InvalidClientConfig,
        "Unix sockets are not available on this platform."
    ));
}

impl IntoConnectionInfo for url::Url {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        match self.scheme() {
            "redis" | "rediss" => url_to_tcp_connection_info(self),
            "unix" | "redis+unix" => url_to_unix_connection_info(self),
            _ => fail!((
                ErrorKind::InvalidClientConfig,
                "URL provided is not a redis URL"
            )),
        }
    }
}

struct TcpConnection {
    reader: TcpStream,
    open: bool,
}

#[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
struct TcpNativeTlsConnection {
    reader: TlsStream<TcpStream>,
    open: bool,
}

#[cfg(feature = "tls-rustls")]
struct TcpRustlsConnection {
    reader: StreamOwned<rustls::ClientConnection, TcpStream>,
    open: bool,
}

#[cfg(unix)]
struct UnixConnection {
    sock: UnixStream,
    open: bool,
}

enum ActualConnection {
    Tcp(TcpConnection),
    #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
    TcpNativeTls(Box<TcpNativeTlsConnection>),
    #[cfg(feature = "tls-rustls")]
    TcpRustls(Box<TcpRustlsConnection>),
    #[cfg(unix)]
    Unix(UnixConnection),
}

#[cfg(feature = "tls-rustls-insecure")]
struct NoCertificateVerification {
    supported: rustls::crypto::WebPkiSupportedAlgorithms,
}

#[cfg(feature = "tls-rustls-insecure")]
impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.supported.supported_schemes()
    }
}

#[cfg(feature = "tls-rustls-insecure")]
impl fmt::Debug for NoCertificateVerification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NoCertificateVerification").finish()
    }
}

/// Represents a stateful redis TCP connection.
pub struct Connection {
    con: ActualConnection,
    parser: Parser,
    db: i64,

    /// Flag indicating whether the connection was left in the PubSub state after dropping `PubSub`.
    ///
    /// This flag is checked when attempting to send a command, and if it's raised, we attempt to
    /// exit the pubsub state before executing the new request.
    pubsub: bool,

    // Field indicating which protocol to use for server communications.
    protocol: ProtocolVersion,

    /// This is used to manage Push messages in RESP3 mode.
    push_sender: Option<SyncPushSender>,
}

/// Represents a pubsub connection.
pub struct PubSub<'a> {
    con: &'a mut Connection,
    waiting_messages: VecDeque<Msg>,
}

/// Represents a pubsub message.
#[derive(Debug, Clone)]
pub struct Msg {
    payload: Value,
    channel: Value,
    pattern: Option<Value>,
}

impl ActualConnection {
    pub fn new(addr: &ConnectionAddr, timeout: Option<Duration>) -> RedisResult<ActualConnection> {
        Ok(match *addr {
            ConnectionAddr::Tcp(ref host, ref port) => {
                let addr = (host.as_str(), *port);
                let tcp = match timeout {
                    None => connect_tcp(addr)?,
                    Some(timeout) => {
                        let mut tcp = None;
                        let mut last_error = None;
                        for addr in addr.to_socket_addrs()? {
                            match connect_tcp_timeout(&addr, timeout) {
                                Ok(l) => {
                                    tcp = Some(l);
                                    break;
                                }
                                Err(e) => {
                                    last_error = Some(e);
                                }
                            };
                        }
                        match (tcp, last_error) {
                            (Some(tcp), _) => tcp,
                            (None, Some(e)) => {
                                fail!(e);
                            }
                            (None, None) => {
                                fail!((
                                    ErrorKind::InvalidClientConfig,
                                    "could not resolve to any addresses"
                                ));
                            }
                        }
                    }
                };
                ActualConnection::Tcp(TcpConnection {
                    reader: tcp,
                    open: true,
                })
            }
            #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
            ConnectionAddr::TcpTls {
                ref host,
                port,
                insecure,
                ..
            } => {
                let tls_connector = if insecure {
                    TlsConnector::builder()
                        .danger_accept_invalid_certs(true)
                        .danger_accept_invalid_hostnames(true)
                        .use_sni(false)
                        .build()?
                } else {
                    TlsConnector::new()?
                };
                let addr = (host.as_str(), port);
                let tls = match timeout {
                    None => {
                        let tcp = connect_tcp(addr)?;
                        match tls_connector.connect(host, tcp) {
                            Ok(res) => res,
                            Err(e) => {
                                fail!((ErrorKind::IoError, "SSL Handshake error", e.to_string()));
                            }
                        }
                    }
                    Some(timeout) => {
                        let mut tcp = None;
                        let mut last_error = None;
                        for addr in (host.as_str(), port).to_socket_addrs()? {
                            match connect_tcp_timeout(&addr, timeout) {
                                Ok(l) => {
                                    tcp = Some(l);
                                    break;
                                }
                                Err(e) => {
                                    last_error = Some(e);
                                }
                            };
                        }
                        match (tcp, last_error) {
                            (Some(tcp), _) => tls_connector.connect(host, tcp).unwrap(),
                            (None, Some(e)) => {
                                fail!(e);
                            }
                            (None, None) => {
                                fail!((
                                    ErrorKind::InvalidClientConfig,
                                    "could not resolve to any addresses"
                                ));
                            }
                        }
                    }
                };
                ActualConnection::TcpNativeTls(Box::new(TcpNativeTlsConnection {
                    reader: tls,
                    open: true,
                }))
            }
            #[cfg(feature = "tls-rustls")]
            ConnectionAddr::TcpTls {
                ref host,
                port,
                insecure,
                ref tls_params,
            } => {
                let host: &str = host;
                let config = create_rustls_config(insecure, tls_params.clone())?;
                let conn = rustls::ClientConnection::new(
                    Arc::new(config),
                    rustls_pki_types::ServerName::try_from(host)?.to_owned(),
                )?;
                let reader = match timeout {
                    None => {
                        let tcp = connect_tcp((host, port))?;
                        StreamOwned::new(conn, tcp)
                    }
                    Some(timeout) => {
                        let mut tcp = None;
                        let mut last_error = None;
                        for addr in (host, port).to_socket_addrs()? {
                            match connect_tcp_timeout(&addr, timeout) {
                                Ok(l) => {
                                    tcp = Some(l);
                                    break;
                                }
                                Err(e) => {
                                    last_error = Some(e);
                                }
                            };
                        }
                        match (tcp, last_error) {
                            (Some(tcp), _) => StreamOwned::new(conn, tcp),
                            (None, Some(e)) => {
                                fail!(e);
                            }
                            (None, None) => {
                                fail!((
                                    ErrorKind::InvalidClientConfig,
                                    "could not resolve to any addresses"
                                ));
                            }
                        }
                    }
                };

                ActualConnection::TcpRustls(Box::new(TcpRustlsConnection { reader, open: true }))
            }
            #[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]
            ConnectionAddr::TcpTls { .. } => {
                fail!((
                    ErrorKind::InvalidClientConfig,
                    "Cannot connect to TCP with TLS without the tls feature"
                ));
            }
            #[cfg(unix)]
            ConnectionAddr::Unix(ref path) => ActualConnection::Unix(UnixConnection {
                sock: UnixStream::connect(path)?,
                open: true,
            }),
            #[cfg(not(unix))]
            ConnectionAddr::Unix(ref _path) => {
                fail!((
                    ErrorKind::InvalidClientConfig,
                    "Cannot connect to unix sockets \
                     on this platform"
                ));
            }
        })
    }

    pub fn send_bytes(&mut self, bytes: &[u8]) -> RedisResult<Value> {
        match *self {
            ActualConnection::Tcp(ref mut connection) => {
                let res = connection.reader.write_all(bytes).map_err(RedisError::from);
                match res {
                    Err(e) => {
                        if e.is_unrecoverable_error() {
                            connection.open = false;
                        }
                        Err(e)
                    }
                    Ok(_) => Ok(Value::Okay),
                }
            }
            #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
            ActualConnection::TcpNativeTls(ref mut connection) => {
                let res = connection.reader.write_all(bytes).map_err(RedisError::from);
                match res {
                    Err(e) => {
                        if e.is_unrecoverable_error() {
                            connection.open = false;
                        }
                        Err(e)
                    }
                    Ok(_) => Ok(Value::Okay),
                }
            }
            #[cfg(feature = "tls-rustls")]
            ActualConnection::TcpRustls(ref mut connection) => {
                let res = connection.reader.write_all(bytes).map_err(RedisError::from);
                match res {
                    Err(e) => {
                        if e.is_unrecoverable_error() {
                            connection.open = false;
                        }
                        Err(e)
                    }
                    Ok(_) => Ok(Value::Okay),
                }
            }
            #[cfg(unix)]
            ActualConnection::Unix(ref mut connection) => {
                let result = connection.sock.write_all(bytes).map_err(RedisError::from);
                match result {
                    Err(e) => {
                        if e.is_unrecoverable_error() {
                            connection.open = false;
                        }
                        Err(e)
                    }
                    Ok(_) => Ok(Value::Okay),
                }
            }
        }
    }

    pub fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        match *self {
            ActualConnection::Tcp(TcpConnection { ref reader, .. }) => {
                reader.set_write_timeout(dur)?;
            }
            #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
            ActualConnection::TcpNativeTls(ref boxed_tls_connection) => {
                let reader = &(boxed_tls_connection.reader);
                reader.get_ref().set_write_timeout(dur)?;
            }
            #[cfg(feature = "tls-rustls")]
            ActualConnection::TcpRustls(ref boxed_tls_connection) => {
                let reader = &(boxed_tls_connection.reader);
                reader.get_ref().set_write_timeout(dur)?;
            }
            #[cfg(unix)]
            ActualConnection::Unix(UnixConnection { ref sock, .. }) => {
                sock.set_write_timeout(dur)?;
            }
        }
        Ok(())
    }

    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        match *self {
            ActualConnection::Tcp(TcpConnection { ref reader, .. }) => {
                reader.set_read_timeout(dur)?;
            }
            #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
            ActualConnection::TcpNativeTls(ref boxed_tls_connection) => {
                let reader = &(boxed_tls_connection.reader);
                reader.get_ref().set_read_timeout(dur)?;
            }
            #[cfg(feature = "tls-rustls")]
            ActualConnection::TcpRustls(ref boxed_tls_connection) => {
                let reader = &(boxed_tls_connection.reader);
                reader.get_ref().set_read_timeout(dur)?;
            }
            #[cfg(unix)]
            ActualConnection::Unix(UnixConnection { ref sock, .. }) => {
                sock.set_read_timeout(dur)?;
            }
        }
        Ok(())
    }

    pub fn is_open(&self) -> bool {
        match *self {
            ActualConnection::Tcp(TcpConnection { open, .. }) => open,
            #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
            ActualConnection::TcpNativeTls(ref boxed_tls_connection) => boxed_tls_connection.open,
            #[cfg(feature = "tls-rustls")]
            ActualConnection::TcpRustls(ref boxed_tls_connection) => boxed_tls_connection.open,
            #[cfg(unix)]
            ActualConnection::Unix(UnixConnection { open, .. }) => open,
        }
    }
}

#[cfg(feature = "tls-rustls")]
pub(crate) fn create_rustls_config(
    insecure: bool,
    tls_params: Option<TlsConnParams>,
) -> RedisResult<rustls::ClientConfig> {
    use crate::tls::ClientTlsParams;

    #[allow(unused_mut)]
    let mut root_store = RootCertStore::empty();
    #[cfg(feature = "tls-rustls-webpki-roots")]
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    #[cfg(all(
        feature = "tls-rustls",
        not(feature = "tls-native-tls"),
        not(feature = "tls-rustls-webpki-roots")
    ))]
    for cert in load_native_certs()? {
        root_store.add(cert)?;
    }

    let config = rustls::ClientConfig::builder();
    let config = if let Some(tls_params) = tls_params {
        let config_builder =
            config.with_root_certificates(tls_params.root_cert_store.unwrap_or(root_store));

        if let Some(ClientTlsParams {
            client_cert_chain: client_cert,
            client_key,
        }) = tls_params.client_tls_params
        {
            config_builder
                .with_client_auth_cert(client_cert, client_key)
                .map_err(|err| {
                    RedisError::from((
                        ErrorKind::InvalidClientConfig,
                        "Unable to build client with TLS parameters provided.",
                        err.to_string(),
                    ))
                })?
        } else {
            config_builder.with_no_client_auth()
        }
    } else {
        config
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

    match (insecure, cfg!(feature = "tls-rustls-insecure")) {
        #[cfg(feature = "tls-rustls-insecure")]
        (true, true) => {
            let mut config = config;
            config.enable_sni = false;
            config
                .dangerous()
                .set_certificate_verifier(Arc::new(NoCertificateVerification {
                    supported: rustls::crypto::ring::default_provider()
                        .signature_verification_algorithms,
                }));

            Ok(config)
        }
        (true, false) => {
            fail!((
                ErrorKind::InvalidClientConfig,
                "Cannot create insecure client without tls-rustls-insecure feature"
            ));
        }
        _ => Ok(config),
    }
}

fn authenticate_cmd(
    connection_info: &RedisConnectionInfo,
    check_username: bool,
    password: &str,
) -> Cmd {
    let mut command = cmd("AUTH");
    if check_username {
        if let Some(username) = &connection_info.username {
            command.arg(username);
        }
    }
    command.arg(password);
    command
}

pub fn connect(
    connection_info: &ConnectionInfo,
    timeout: Option<Duration>,
) -> RedisResult<Connection> {
    let con: ActualConnection = ActualConnection::new(&connection_info.addr, timeout)?;
    setup_connection(con, &connection_info.redis)
}

pub(crate) struct ConnectionSetupComponents {
    authenticate_with_resp3_cmd_index: Option<usize>,
    authenticate_with_resp2_cmd_index: Option<usize>,
    select_db_cmd_index: Option<usize>,
}

pub(crate) fn connection_setup_pipeline(
    connection_info: &RedisConnectionInfo,
    check_username: bool,
) -> (crate::Pipeline, ConnectionSetupComponents) {
    let mut last_cmd_index = 0;

    let mut get_next_command_index = |condition| {
        if condition {
            last_cmd_index += 1;
            Some(last_cmd_index - 1)
        } else {
            None
        }
    };

    let authenticate_with_resp3_cmd_index =
        get_next_command_index(connection_info.protocol != ProtocolVersion::RESP2);
    let authenticate_with_resp2_cmd_index = get_next_command_index(
        authenticate_with_resp3_cmd_index.is_none() && connection_info.password.is_some(),
    );
    let select_db_cmd_index = get_next_command_index(connection_info.db != 0);

    let mut pipeline = pipe();

    if authenticate_with_resp3_cmd_index.is_some() {
        pipeline.add_command(resp3_hello(connection_info));
    } else if authenticate_with_resp2_cmd_index.is_some() {
        pipeline.add_command(authenticate_cmd(
            connection_info,
            check_username,
            connection_info.password.as_ref().unwrap(),
        ));
    }

    if select_db_cmd_index.is_some() {
        pipeline.cmd("SELECT").arg(connection_info.db);
    }

    // result is ignored, as per the command's instructions.
    // https://redis.io/commands/client-setinfo/
    #[cfg(not(feature = "disable-client-setinfo"))]
    pipeline
        .cmd("CLIENT")
        .arg("SETINFO")
        .arg("LIB-NAME")
        .arg("redis-rs")
        .ignore();
    #[cfg(not(feature = "disable-client-setinfo"))]
    pipeline
        .cmd("CLIENT")
        .arg("SETINFO")
        .arg("LIB-VER")
        .arg(env!("CARGO_PKG_VERSION"))
        .ignore();

    (
        pipeline,
        ConnectionSetupComponents {
            authenticate_with_resp3_cmd_index,
            authenticate_with_resp2_cmd_index,
            select_db_cmd_index,
        },
    )
}

fn check_resp3_auth(
    results: &[Value],
    instructions: &ConnectionSetupComponents,
) -> RedisResult<()> {
    if let Some(index) = instructions.authenticate_with_resp3_cmd_index {
        if let Some(Value::ServerError(err)) = results.get(index) {
            return Err(get_resp3_hello_command_error(err.clone().into()));
        }
    }
    Ok(())
}

#[derive(PartialEq)]
pub(crate) enum AuthResult {
    Succeeded,
    ShouldRetryWithoutUsername,
}

fn check_resp2_auth(
    results: &[Value],
    instructions: &ConnectionSetupComponents,
) -> RedisResult<AuthResult> {
    if let Some(index) = instructions.authenticate_with_resp2_cmd_index {
        let err = match results.get(index).unwrap() {
            Value::Okay => {
                return Ok(AuthResult::Succeeded);
            }
            Value::ServerError(err) => err,
            _ => {
                return Err((
                    ErrorKind::ResponseError,
                    "Redis server refused to authenticate, returns Ok() != Value::Okay",
                )
                    .into());
            }
        };
        let err_msg = err.details().ok_or((
            ErrorKind::AuthenticationFailed,
            "Password authentication failed",
        ))?;
        if !err_msg.contains("wrong number of arguments for 'auth' command") {
            return Err((
                ErrorKind::AuthenticationFailed,
                "Password authentication failed",
            )
                .into());
        }
        return Ok(AuthResult::ShouldRetryWithoutUsername);
    }
    Ok(AuthResult::Succeeded)
}

fn check_db_select(results: &[Value], instructions: &ConnectionSetupComponents) -> RedisResult<()> {
    if let Some(index) = instructions.select_db_cmd_index {
        if let Some(Value::ServerError(err)) = results.get(index) {
            return match err.details() {
                Some(err_msg) => Err((
                    ErrorKind::ResponseError,
                    "Redis server refused to switch database",
                    err_msg.to_string(),
                )
                    .into()),
                None => Err((
                    ErrorKind::ResponseError,
                    "Redis server refused to switch database",
                )
                    .into()),
            };
        }
    }
    Ok(())
}

pub(crate) fn check_connection_setup(
    results: Vec<Value>,
    instructions: ConnectionSetupComponents,
) -> RedisResult<AuthResult> {
    check_resp3_auth(&results, &instructions)?;
    if check_resp2_auth(&results, &instructions)? == AuthResult::ShouldRetryWithoutUsername {
        return Ok(AuthResult::ShouldRetryWithoutUsername);
    }
    check_db_select(&results, &instructions)?;
    Ok(AuthResult::Succeeded)
}

fn execute_connection_pipeline(
    rv: &mut Connection,
    (pipeline, instructions): (crate::Pipeline, ConnectionSetupComponents),
) -> RedisResult<AuthResult> {
    if pipeline.len() == 0 {
        return Ok(AuthResult::Succeeded);
    }
    let results = rv.req_packed_commands(&pipeline.get_packed_pipeline(), 0, pipeline.len())?;

    check_connection_setup(results, instructions)
}

fn setup_connection(
    con: ActualConnection,
    connection_info: &RedisConnectionInfo,
) -> RedisResult<Connection> {
    let mut rv = Connection {
        con,
        parser: Parser::new(),
        db: connection_info.db,
        pubsub: false,
        protocol: connection_info.protocol,
        push_sender: None,
    };

    if execute_connection_pipeline(&mut rv, connection_setup_pipeline(connection_info, true))?
        == AuthResult::ShouldRetryWithoutUsername
    {
        execute_connection_pipeline(&mut rv, connection_setup_pipeline(connection_info, false))?;
    }

    Ok(rv)
}

/// Implements the "stateless" part of the connection interface that is used by the
/// different objects in redis-rs.  Primarily it obviously applies to `Connection`
/// object but also some other objects implement the interface (for instance
/// whole clients or certain redis results).
///
/// Generally clients and connections (as well as redis results of those) implement
/// this trait.  Actual connections provide more functionality which can be used
/// to implement things like `PubSub` but they also can modify the intrinsic
/// state of the TCP connection.  This is not possible with `ConnectionLike`
/// implementors because that functionality is not exposed.
pub trait ConnectionLike {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value>;

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    /// Important - this function is meant for internal usage, since it's
    /// easy to pass incorrect `offset` & `count` parameters, which might
    /// cause the connection to enter an erroneous state. Users shouldn't
    /// call it, instead using the Pipeline::query function.
    #[doc(hidden)]
    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>>;

    /// Sends a [Cmd] into the TCP socket and reads a single response from it.
    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        let pcmd = cmd.get_packed_command();
        self.req_packed_command(&pcmd)
    }

    /// Returns the database this connection is bound to.  Note that this
    /// information might be unreliable because it's initially cached and
    /// also might be incorrect if the connection like object is not
    /// actually connected.
    fn get_db(&self) -> i64;

    /// Does this connection support pipelining?
    #[doc(hidden)]
    fn supports_pipelining(&self) -> bool {
        true
    }

    /// Check that all connections it has are available (`PING` internally).
    fn check_connection(&mut self) -> bool;

    /// Returns the connection status.
    ///
    /// The connection is open until any `read_response` call received an
    /// invalid response from the server (most likely a closed or dropped
    /// connection, otherwise a Redis protocol error). When using unix
    /// sockets the connection is open until writing a command failed with a
    /// `BrokenPipe` error.
    fn is_open(&self) -> bool;
}

/// A connection is an object that represents a single redis connection.  It
/// provides basic support for sending encoded commands into a redis connection
/// and to read a response from it.  It's bound to a single database and can
/// only be created from the client.
///
/// You generally do not much with this object other than passing it to
/// `Cmd` objects.
impl Connection {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// does not read a response.  This is useful for commands like
    /// `MONITOR` which yield multiple items.  This needs to be used with
    /// care because it changes the state of the connection.
    pub fn send_packed_command(&mut self, cmd: &[u8]) -> RedisResult<()> {
        self.send_bytes(cmd)?;
        Ok(())
    }

    /// Fetches a single response from the connection.  This is useful
    /// if used in combination with `send_packed_command`.
    pub fn recv_response(&mut self) -> RedisResult<Value> {
        self.read_response()
    }

    /// Sets the write timeout for the connection.
    ///
    /// If the provided value is `None`, then `send_packed_command` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.con.set_write_timeout(dur)
    }

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `recv_response` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.con.set_read_timeout(dur)
    }

    /// Creates a [`PubSub`] instance for this connection.
    pub fn as_pubsub(&mut self) -> PubSub<'_> {
        // NOTE: The pubsub flag is intentionally not raised at this time since
        // running commands within the pubsub state should not try and exit from
        // the pubsub state.
        PubSub::new(self)
    }

    fn exit_pubsub(&mut self) -> RedisResult<()> {
        let res = self.clear_active_subscriptions();
        if res.is_ok() {
            self.pubsub = false;
        } else {
            // Raise the pubsub flag to indicate the connection is "stuck" in that state.
            self.pubsub = true;
        }

        res
    }

    /// Get the inner connection out of a PubSub
    ///
    /// Any active subscriptions are unsubscribed. In the event of an error, the connection is
    /// dropped.
    fn clear_active_subscriptions(&mut self) -> RedisResult<()> {
        // Responses to unsubscribe commands return in a 3-tuple with values
        // ("unsubscribe" or "punsubscribe", name of subscription removed, count of remaining subs).
        // The "count of remaining subs" includes both pattern subscriptions and non pattern
        // subscriptions. Thus, to accurately drain all unsubscribe messages received from the
        // server, both commands need to be executed at once.
        {
            // Prepare both unsubscribe commands
            let unsubscribe = cmd("UNSUBSCRIBE").get_packed_command();
            let punsubscribe = cmd("PUNSUBSCRIBE").get_packed_command();

            // Execute commands
            self.send_bytes(&unsubscribe)?;
            self.send_bytes(&punsubscribe)?;
        }

        // Receive responses
        //
        // There will be at minimum two responses - 1 for each of punsubscribe and unsubscribe
        // commands. There may be more responses if there are active subscriptions. In this case,
        // messages are received until the _subscription count_ in the responses reach zero.
        let mut received_unsub = false;
        let mut received_punsub = false;
        if self.protocol != ProtocolVersion::RESP2 {
            while let Value::Push { kind, data } = from_owned_redis_value(self.recv_response()?)? {
                if data.len() >= 2 {
                    if let Value::Int(num) = data[1] {
                        if resp3_is_pub_sub_state_cleared(
                            &mut received_unsub,
                            &mut received_punsub,
                            &kind,
                            num as isize,
                        ) {
                            break;
                        }
                    }
                }
            }
        } else {
            loop {
                let res: (Vec<u8>, (), isize) = from_owned_redis_value(self.recv_response()?)?;
                if resp2_is_pub_sub_state_cleared(
                    &mut received_unsub,
                    &mut received_punsub,
                    &res.0,
                    res.2,
                ) {
                    break;
                }
            }
        }

        // Finally, the connection is back in its normal state since all subscriptions were
        // cancelled *and* all unsubscribe messages were received.
        Ok(())
    }

    fn send_push(&self, push: PushInfo) {
        if let Some(sender) = &self.push_sender {
            let _ = sender.send(push);
        }
    }

    fn try_send(&self, value: &RedisResult<Value>) {
        if let Ok(Value::Push { kind, data }) = value {
            self.send_push(PushInfo {
                kind: kind.clone(),
                data: data.clone(),
            });
        }
    }

    fn send_disconnect(&self) {
        self.send_push(PushInfo {
            kind: PushKind::Disconnection,
            data: vec![],
        })
    }

    /// Fetches a single response from the connection.
    fn read_response(&mut self) -> RedisResult<Value> {
        let result = match self.con {
            ActualConnection::Tcp(TcpConnection { ref mut reader, .. }) => {
                let result = self.parser.parse_value(reader);
                self.try_send(&result);
                result
            }
            #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
            ActualConnection::TcpNativeTls(ref mut boxed_tls_connection) => {
                let reader = &mut boxed_tls_connection.reader;
                let result = self.parser.parse_value(reader);
                self.try_send(&result);
                result
            }
            #[cfg(feature = "tls-rustls")]
            ActualConnection::TcpRustls(ref mut boxed_tls_connection) => {
                let reader = &mut boxed_tls_connection.reader;
                let result = self.parser.parse_value(reader);
                self.try_send(&result);
                result
            }
            #[cfg(unix)]
            ActualConnection::Unix(UnixConnection { ref mut sock, .. }) => {
                let result = self.parser.parse_value(sock);
                self.try_send(&result);
                result
            }
        };
        // shutdown connection on protocol error
        if let Err(e) = &result {
            let shutdown = match e.as_io_error() {
                Some(e) => e.kind() == io::ErrorKind::UnexpectedEof,
                None => false,
            };
            if shutdown {
                // Notify the PushManager that the connection was lost
                self.send_disconnect();
                match self.con {
                    ActualConnection::Tcp(ref mut connection) => {
                        let _ = connection.reader.shutdown(net::Shutdown::Both);
                        connection.open = false;
                    }
                    #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
                    ActualConnection::TcpNativeTls(ref mut connection) => {
                        let _ = connection.reader.shutdown();
                        connection.open = false;
                    }
                    #[cfg(feature = "tls-rustls")]
                    ActualConnection::TcpRustls(ref mut connection) => {
                        let _ = connection.reader.get_mut().shutdown(net::Shutdown::Both);
                        connection.open = false;
                    }
                    #[cfg(unix)]
                    ActualConnection::Unix(ref mut connection) => {
                        let _ = connection.sock.shutdown(net::Shutdown::Both);
                        connection.open = false;
                    }
                }
            }
        }
        result
    }

    /// Sets sender channel for push values.
    pub fn set_push_sender(&mut self, sender: SyncPushSender) {
        self.push_sender = Some(sender);
    }

    fn send_bytes(&mut self, bytes: &[u8]) -> RedisResult<Value> {
        let result = self.con.send_bytes(bytes);
        if self.protocol != ProtocolVersion::RESP2 {
            if let Err(e) = &result {
                if e.is_connection_dropped() {
                    self.send_disconnect();
                }
            }
        }
        result
    }
}

impl ConnectionLike for Connection {
    /// Sends a [Cmd] into the TCP socket and reads a single response from it.
    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        let pcmd = cmd.get_packed_command();
        if self.pubsub {
            self.exit_pubsub()?;
        }

        self.send_bytes(&pcmd)?;
        if cmd.is_no_response() {
            return Ok(Value::Nil);
        }
        loop {
            match self.read_response()? {
                Value::Push {
                    kind: _kind,
                    data: _data,
                } => continue,
                val => return Ok(val),
            }
        }
    }
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        if self.pubsub {
            self.exit_pubsub()?;
        }

        self.send_bytes(cmd)?;
        loop {
            match self.read_response()? {
                Value::Push {
                    kind: _kind,
                    data: _data,
                } => continue,
                val => return Ok(val),
            }
        }
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        if self.pubsub {
            self.exit_pubsub()?;
        }
        self.send_bytes(cmd)?;
        let mut rv = vec![];
        let mut first_err = None;
        let mut count = count;
        let mut idx = 0;
        while idx < (offset + count) {
            // When processing a transaction, some responses may be errors.
            // We need to keep processing the rest of the responses in that case,
            // so bailing early with `?` would not be correct.
            // See: https://github.com/redis-rs/redis-rs/issues/436
            let response = self.read_response();
            match response {
                Ok(Value::ServerError(err)) => {
                    if idx < offset {
                        if first_err.is_none() {
                            first_err = Some(err.into());
                        }
                    } else {
                        rv.push(Value::ServerError(err));
                    }
                }
                Ok(item) => {
                    // RESP3 can insert push data between command replies
                    if let Value::Push {
                        kind: _kind,
                        data: _data,
                    } = item
                    {
                        // if that is the case we have to extend the loop and handle push data
                        count += 1;
                    } else if idx >= offset {
                        rv.push(item);
                    }
                }
                Err(err) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
            idx += 1;
        }

        first_err.map_or(Ok(rv), Err)
    }

    fn get_db(&self) -> i64 {
        self.db
    }

    fn check_connection(&mut self) -> bool {
        cmd("PING").query::<String>(self).is_ok()
    }

    fn is_open(&self) -> bool {
        self.con.is_open()
    }
}

impl<C, T> ConnectionLike for T
where
    C: ConnectionLike,
    T: DerefMut<Target = C>,
{
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        self.deref_mut().req_packed_command(cmd)
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        self.deref_mut().req_packed_commands(cmd, offset, count)
    }

    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        self.deref_mut().req_command(cmd)
    }

    fn get_db(&self) -> i64 {
        self.deref().get_db()
    }

    fn supports_pipelining(&self) -> bool {
        self.deref().supports_pipelining()
    }

    fn check_connection(&mut self) -> bool {
        self.deref_mut().check_connection()
    }

    fn is_open(&self) -> bool {
        self.deref().is_open()
    }
}

/// The pubsub object provides convenient access to the redis pubsub
/// system.  Once created you can subscribe and unsubscribe from channels
/// and listen in on messages.
///
/// Example:
///
/// ```rust,no_run
/// # fn do_something() -> redis::RedisResult<()> {
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let mut con = client.get_connection()?;
/// let mut pubsub = con.as_pubsub();
/// pubsub.subscribe("channel_1")?;
/// pubsub.subscribe("channel_2")?;
///
/// loop {
///     let msg = pubsub.get_message()?;
///     let payload : String = msg.get_payload()?;
///     println!("channel '{}': {}", msg.get_channel_name(), payload);
/// }
/// # }
/// ```
impl<'a> PubSub<'a> {
    fn new(con: &'a mut Connection) -> Self {
        Self {
            con,
            waiting_messages: VecDeque::new(),
        }
    }

    fn cache_messages_until_received_response(&mut self, cmd: &mut Cmd) -> RedisResult<()> {
        if self.con.protocol != ProtocolVersion::RESP2 {
            cmd.set_no_response(true);
        }
        let mut response = cmd.query(self.con)?;
        loop {
            if let Some(msg) = Msg::from_owned_value(response) {
                self.waiting_messages.push_back(msg);
            } else {
                return Ok(());
            }
            response = self.con.recv_response()?;
        }
    }

    /// Subscribes to a new channel.
    pub fn subscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        self.cache_messages_until_received_response(cmd("SUBSCRIBE").arg(channel))
    }

    /// Subscribes to a new channel with a pattern.
    pub fn psubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        self.cache_messages_until_received_response(cmd("PSUBSCRIBE").arg(pchannel))
    }

    /// Unsubscribes from a channel.
    pub fn unsubscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        self.cache_messages_until_received_response(cmd("UNSUBSCRIBE").arg(channel))
    }

    /// Unsubscribes from a channel with a pattern.
    pub fn punsubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        self.cache_messages_until_received_response(cmd("PUNSUBSCRIBE").arg(pchannel))
    }

    /// Fetches the next message from the pubsub connection.  Blocks until
    /// a message becomes available.  This currently does not provide a
    /// wait not to block :(
    ///
    /// The message itself is still generic and can be converted into an
    /// appropriate type through the helper methods on it.
    pub fn get_message(&mut self) -> RedisResult<Msg> {
        if let Some(msg) = self.waiting_messages.pop_front() {
            return Ok(msg);
        }
        loop {
            if let Some(msg) = Msg::from_owned_value(self.con.recv_response()?) {
                return Ok(msg);
            } else {
                continue;
            }
        }
    }

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `get_message` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.con.set_read_timeout(dur)
    }
}

impl<'a> Drop for PubSub<'a> {
    fn drop(&mut self) {
        let _ = self.con.exit_pubsub();
    }
}

/// This holds the data that comes from listening to a pubsub
/// connection.  It only contains actual message data.
impl Msg {
    /// Tries to convert provided [`Value`] into [`Msg`].
    pub fn from_value(value: &Value) -> Option<Self> {
        Self::from_owned_value(value.clone())
    }

    /// Tries to convert provided [`Value`] into [`Msg`].
    pub fn from_owned_value(value: Value) -> Option<Self> {
        let mut pattern = None;
        let payload;
        let channel;

        if let Value::Push { kind, data } = value {
            return Self::from_push_info(PushInfo { kind, data });
        } else {
            let raw_msg: Vec<Value> = from_owned_redis_value(value).ok()?;
            let mut iter = raw_msg.into_iter();
            let msg_type: String = from_owned_redis_value(iter.next()?).ok()?;
            if msg_type == "message" {
                channel = iter.next()?;
                payload = iter.next()?;
            } else if msg_type == "pmessage" {
                pattern = Some(iter.next()?);
                channel = iter.next()?;
                payload = iter.next()?;
            } else {
                return None;
            }
        };
        Some(Msg {
            payload,
            channel,
            pattern,
        })
    }

    /// Tries to convert provided [`PushInfo`] into [`Msg`].
    pub fn from_push_info(push_info: PushInfo) -> Option<Self> {
        let mut pattern = None;
        let payload;
        let channel;

        let mut iter = push_info.data.into_iter();
        if push_info.kind == PushKind::Message || push_info.kind == PushKind::SMessage {
            channel = iter.next()?;
            payload = iter.next()?;
        } else if push_info.kind == PushKind::PMessage {
            pattern = Some(iter.next()?);
            channel = iter.next()?;
            payload = iter.next()?;
        } else {
            return None;
        }

        Some(Msg {
            payload,
            channel,
            pattern,
        })
    }

    /// Returns the channel this message came on.
    pub fn get_channel<T: FromRedisValue>(&self) -> RedisResult<T> {
        from_redis_value(&self.channel)
    }

    /// Convenience method to get a string version of the channel.  Unless
    /// your channel contains non utf-8 bytes you can always use this
    /// method.  If the channel is not a valid string (which really should
    /// not happen) then the return value is `"?"`.
    pub fn get_channel_name(&self) -> &str {
        match self.channel {
            Value::BulkString(ref bytes) => from_utf8(bytes).unwrap_or("?"),
            _ => "?",
        }
    }

    /// Returns the message's payload in a specific format.
    pub fn get_payload<T: FromRedisValue>(&self) -> RedisResult<T> {
        from_redis_value(&self.payload)
    }

    /// Returns the bytes that are the message's payload.  This can be used
    /// as an alternative to the `get_payload` function if you are interested
    /// in the raw bytes in it.
    pub fn get_payload_bytes(&self) -> &[u8] {
        match self.payload {
            Value::BulkString(ref bytes) => bytes,
            _ => b"",
        }
    }

    /// Returns true if the message was constructed from a pattern
    /// subscription.
    #[allow(clippy::wrong_self_convention)]
    pub fn from_pattern(&self) -> bool {
        self.pattern.is_some()
    }

    /// If the message was constructed from a message pattern this can be
    /// used to find out which one.  It's recommended to match against
    /// an `Option<String>` so that you do not need to use `from_pattern`
    /// to figure out if a pattern was set.
    pub fn get_pattern<T: FromRedisValue>(&self) -> RedisResult<T> {
        match self.pattern {
            None => from_redis_value(&Value::Nil),
            Some(ref x) => from_redis_value(x),
        }
    }
}

/// This function simplifies transaction management slightly.  What it
/// does is automatically watching keys and then going into a transaction
/// loop util it succeeds.  Once it goes through the results are
/// returned.
///
/// To use the transaction two pieces of information are needed: a list
/// of all the keys that need to be watched for modifications and a
/// closure with the code that should be execute in the context of the
/// transaction.  The closure is invoked with a fresh pipeline in atomic
/// mode.  To use the transaction the function needs to return the result
/// from querying the pipeline with the connection.
///
/// The end result of the transaction is then available as the return
/// value from the function call.
///
/// Example:
///
/// ```rust,no_run
/// use redis::Commands;
/// # fn do_something() -> redis::RedisResult<()> {
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let mut con = client.get_connection().unwrap();
/// let key = "the_key";
/// let (new_val,) : (isize,) = redis::transaction(&mut con, &[key], |con, pipe| {
///     let old_val : isize = con.get(key)?;
///     pipe
///         .set(key, old_val + 1).ignore()
///         .get(key).query(con)
/// })?;
/// println!("The incremented number is: {}", new_val);
/// # Ok(()) }
/// ```
pub fn transaction<
    C: ConnectionLike,
    K: ToRedisArgs,
    T,
    F: FnMut(&mut C, &mut Pipeline) -> RedisResult<Option<T>>,
>(
    con: &mut C,
    keys: &[K],
    func: F,
) -> RedisResult<T> {
    let mut func = func;
    loop {
        cmd("WATCH").arg(keys).exec(con)?;
        let mut p = pipe();
        let response: Option<T> = func(con, p.atomic())?;
        match response {
            None => {
                continue;
            }
            Some(response) => {
                // make sure no watch is left in the connection, even if
                // someone forgot to use the pipeline.
                cmd("UNWATCH").exec(con)?;
                return Ok(response);
            }
        }
    }
}
//TODO: for both clearing logic support sharded channels.

/// Common logic for clearing subscriptions in RESP2 async/sync
pub fn resp2_is_pub_sub_state_cleared(
    received_unsub: &mut bool,
    received_punsub: &mut bool,
    kind: &[u8],
    num: isize,
) -> bool {
    match kind.first() {
        Some(&b'u') => *received_unsub = true,
        Some(&b'p') => *received_punsub = true,
        _ => (),
    };
    *received_unsub && *received_punsub && num == 0
}

/// Common logic for clearing subscriptions in RESP3 async/sync
pub fn resp3_is_pub_sub_state_cleared(
    received_unsub: &mut bool,
    received_punsub: &mut bool,
    kind: &PushKind,
    num: isize,
) -> bool {
    match kind {
        PushKind::Unsubscribe => *received_unsub = true,
        PushKind::PUnsubscribe => *received_punsub = true,
        _ => (),
    };
    *received_unsub && *received_punsub && num == 0
}

/// Common logic for checking real cause of hello3 command error
pub fn get_resp3_hello_command_error(err: RedisError) -> RedisError {
    if let Some(detail) = err.detail() {
        if detail.starts_with("unknown command `HELLO`") {
            return (
                ErrorKind::RESP3NotSupported,
                "Redis Server doesn't support HELLO command therefore resp3 cannot be used",
            )
                .into();
        }
    }
    err
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_redis_url() {
        let cases = vec![
            ("redis://127.0.0.1", true),
            ("redis://[::1]", true),
            ("redis+unix:///run/redis.sock", true),
            ("unix:///run/redis.sock", true),
            ("http://127.0.0.1", false),
            ("tcp://127.0.0.1", false),
        ];
        for (url, expected) in cases.into_iter() {
            let res = parse_redis_url(url);
            assert_eq!(
                res.is_some(),
                expected,
                "Parsed result of `{url}` is not expected",
            );
        }
    }

    #[test]
    fn test_url_to_tcp_connection_info() {
        let cases = vec![
            (
                url::Url::parse("redis://127.0.0.1").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379),
                    redis: Default::default(),
                },
            ),
            (
                url::Url::parse("redis://[::1]").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Tcp("::1".to_string(), 6379),
                    redis: Default::default(),
                },
            ),
            (
                url::Url::parse("redis://%25johndoe%25:%23%40%3C%3E%24@example.com/2").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Tcp("example.com".to_string(), 6379),
                    redis: RedisConnectionInfo {
                        db: 2,
                        username: Some("%johndoe%".to_string()),
                        password: Some("#@<>$".to_string()),
                        ..Default::default()
                    },
                },
            ),
            (
                url::Url::parse("redis://127.0.0.1/?protocol=2").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379),
                    redis: Default::default(),
                },
            ),
            (
                url::Url::parse("redis://127.0.0.1/?protocol=resp3").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379),
                    redis: RedisConnectionInfo {
                        protocol: ProtocolVersion::RESP3,
                        ..Default::default()
                    },
                },
            ),
        ];
        for (url, expected) in cases.into_iter() {
            let res = url_to_tcp_connection_info(url.clone()).unwrap();
            assert_eq!(res.addr, expected.addr, "addr of {url} is not expected");
            assert_eq!(
                res.redis.db, expected.redis.db,
                "db of {url} is not expected",
            );
            assert_eq!(
                res.redis.username, expected.redis.username,
                "username of {url} is not expected",
            );
            assert_eq!(
                res.redis.password, expected.redis.password,
                "password of {url} is not expected",
            );
        }
    }

    #[test]
    fn test_url_to_tcp_connection_info_failed() {
        let cases = vec![
            (
                url::Url::parse("redis://").unwrap(),
                "Missing hostname",
                None,
            ),
            (
                url::Url::parse("redis://127.0.0.1/db").unwrap(),
                "Invalid database number",
                None,
            ),
            (
                url::Url::parse("redis://C3%B0@127.0.0.1").unwrap(),
                "Username is not valid UTF-8 string",
                None,
            ),
            (
                url::Url::parse("redis://:C3%B0@127.0.0.1").unwrap(),
                "Password is not valid UTF-8 string",
                None,
            ),
            (
                url::Url::parse("redis://127.0.0.1/?protocol=4").unwrap(),
                "Invalid protocol version",
                Some("4"),
            ),
        ];
        for (url, expected, detail) in cases.into_iter() {
            let res = url_to_tcp_connection_info(url).unwrap_err();
            assert_eq!(
                res.kind(),
                crate::ErrorKind::InvalidClientConfig,
                "{}",
                &res,
            );
            #[allow(deprecated)]
            let desc = std::error::Error::description(&res);
            assert_eq!(desc, expected, "{}", &res);
            assert_eq!(res.detail(), detail, "{}", &res);
        }
    }

    #[test]
    #[cfg(unix)]
    fn test_url_to_unix_connection_info() {
        let cases = vec![
            (
                url::Url::parse("unix:///var/run/redis.sock").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Unix("/var/run/redis.sock".into()),
                    redis: RedisConnectionInfo {
                        db: 0,
                        username: None,
                        password: None,
                        protocol: ProtocolVersion::RESP2,
                    },
                },
            ),
            (
                url::Url::parse("redis+unix:///var/run/redis.sock?db=1").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Unix("/var/run/redis.sock".into()),
                    redis: RedisConnectionInfo {
                        db: 1,
                        ..Default::default()
                    },
                },
            ),
            (
                url::Url::parse(
                    "unix:///example.sock?user=%25johndoe%25&pass=%23%40%3C%3E%24&db=2",
                )
                .unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Unix("/example.sock".into()),
                    redis: RedisConnectionInfo {
                        db: 2,
                        username: Some("%johndoe%".to_string()),
                        password: Some("#@<>$".to_string()),
                        ..Default::default()
                    },
                },
            ),
            (
                url::Url::parse(
                    "redis+unix:///example.sock?pass=%26%3F%3D+%2A%2B&db=2&user=%25johndoe%25",
                )
                .unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Unix("/example.sock".into()),
                    redis: RedisConnectionInfo {
                        db: 2,
                        username: Some("%johndoe%".to_string()),
                        password: Some("&?= *+".to_string()),
                        ..Default::default()
                    },
                },
            ),
            (
                url::Url::parse("redis+unix:///var/run/redis.sock?protocol=3").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Unix("/var/run/redis.sock".into()),
                    redis: RedisConnectionInfo {
                        protocol: ProtocolVersion::RESP3,
                        ..Default::default()
                    },
                },
            ),
        ];
        for (url, expected) in cases.into_iter() {
            assert_eq!(
                ConnectionAddr::Unix(url.to_file_path().unwrap()),
                expected.addr,
                "addr of {url} is not expected",
            );
            let res = url_to_unix_connection_info(url.clone()).unwrap();
            assert_eq!(res.addr, expected.addr, "addr of {url} is not expected");
            assert_eq!(
                res.redis.db, expected.redis.db,
                "db of {url} is not expected",
            );
            assert_eq!(
                res.redis.username, expected.redis.username,
                "username of {url} is not expected",
            );
            assert_eq!(
                res.redis.password, expected.redis.password,
                "password of {url} is not expected",
            );
        }
    }
}
