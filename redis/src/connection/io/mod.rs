use std::{io::Write as _, net, time::Duration};

use crate::{connection::addr::ConnectionAddr, Parser, RedisError, RedisResult, Value};

pub(crate) mod tcp;
pub use tcp::TcpConnection;

#[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
pub(crate) mod tls_native;

#[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
pub use tls_native::TcpNativeTlsConnection;

#[cfg(feature = "tls-rustls")]
pub(crate) mod tls_rustls;

#[cfg(feature = "tls-rustls")]
pub use tls_rustls::{ClientTlsConfig, TcpRustlsConnection, TlsCertificates, TlsConnParams};

#[cfg(not(feature = "tls-rustls"))]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TlsConnParams;

#[cfg(unix)]
pub(crate) mod unix;

#[cfg(unix)]
pub use unix::UnixConnection;

/// Driver trait that can be implemented
/// to bring your own IO when using Redis synchronously.
///
/// Most users however will probably just use [`ActualConnection`] instead.
pub trait ConnectionDriver: Sized {
    /// Establish a connection, returning handle for command execution.
    fn establish_conn(addr: &ConnectionAddr, timeout: Option<Duration>) -> RedisResult<Self>;

    /// Sets the write timeout for the connection.
    fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()>;
    /// Sets the read timeout for the connection.
    fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()>;

    /// Send bytes over the connection driver.
    fn send_bytes(&mut self, bytes: &[u8]) -> RedisResult<Value>;

    /// Read a Redis value from the connection driver.
    fn read_value(&mut self, parser: &mut Parser) -> RedisResult<Value>;

    /// Reports if the connection driver is open.
    fn is_open(&self) -> bool;
}

pub enum ActualConnection {
    Tcp(tcp::TcpConnection),
    #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
    TcpNativeTls(Box<tls_native::TcpNativeTlsConnection>),
    #[cfg(feature = "tls-rustls")]
    TcpRustls(Box<tls_rustls::TcpRustlsConnection>),
    #[cfg(unix)]
    Unix(unix::UnixConnection),
}

impl ConnectionDriver for ActualConnection {
    fn establish_conn(
        addr: &ConnectionAddr,
        timeout: Option<Duration>,
    ) -> RedisResult<ActualConnection> {
        Ok(match *addr {
            ConnectionAddr::Tcp(ref host, port) => {
                ActualConnection::Tcp(TcpConnection::try_new(host, port, timeout)?)
            }
            #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
            ConnectionAddr::TcpTls {
                ref host,
                port,
                insecure,
                ..
            } => ActualConnection::TcpNativeTls(Box::new(TcpNativeTlsConnection::try_new(
                host, port, insecure, timeout,
            )?)),
            #[cfg(feature = "tls-rustls")]
            ConnectionAddr::TcpTls {
                ref host,
                port,
                insecure,
                ref tls_params,
            } => ActualConnection::TcpRustls(Box::new(TcpRustlsConnection::try_new(
                host,
                port,
                insecure,
                tls_params.as_ref(),
                timeout,
            )?)),
            #[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]
            ConnectionAddr::TcpTls { .. } => {
                fail!((
                    crate::ErrorKind::InvalidClientConfig,
                    "Cannot connect to TCP with TLS without the tls feature"
                ));
            }
            #[cfg(unix)]
            ConnectionAddr::Unix(ref path) => {
                ActualConnection::Unix(UnixConnection::try_new(path)?)
            }
            #[cfg(not(unix))]
            ConnectionAddr::Unix(ref _path) => {
                fail!((
                    crate::ErrorKind::InvalidClientConfig,
                    "Cannot connect to unix sockets \
                     on this platform"
                ));
            }
        })
    }

    fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
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

    fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
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

    fn send_bytes(&mut self, bytes: &[u8]) -> RedisResult<Value> {
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

    fn read_value(&mut self, parser: &mut Parser) -> RedisResult<Value> {
        match self {
            ActualConnection::Tcp(TcpConnection { ref mut reader, .. }) => {
                parser.parse_value(reader)
            }
            #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
            ActualConnection::TcpNativeTls(ref mut boxed_tls_connection) => {
                let reader = &mut boxed_tls_connection.reader;
                parser.parse_value(reader)
            }
            #[cfg(feature = "tls-rustls")]
            ActualConnection::TcpRustls(ref mut boxed_tls_connection) => {
                let reader = &mut boxed_tls_connection.reader;
                parser.parse_value(reader)
            }
            #[cfg(unix)]
            ActualConnection::Unix(UnixConnection { ref mut sock, .. }) => parser.parse_value(sock),
        }
    }

    fn is_open(&self) -> bool {
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

impl Drop for ActualConnection {
    fn drop(&mut self) {
        match self {
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
