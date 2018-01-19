use std::fmt::Arguments;
use std::io::{self, BufReader, Read, Write};
use std::mem;
use std::net::{SocketAddr, ToSocketAddrs};

use net2::TcpBuilder;

#[cfg(feature = "with-async-unix-sockets")]
use tokio_uds::UnixStream;

use tokio_io::{self, AsyncWrite};
use tokio_reactor;
use tokio_tcp::TcpStream;

use futures::future::Either;
use futures::{future, Async, Future, Poll};

use cmd::cmd;
use types::{ErrorKind, RedisError, RedisFuture, Value};

use connection::{ConnectionAddr, ConnectionInfo};

enum ActualConnection {
    Tcp(BufReader<TcpStream>),
    #[cfg(feature = "with-async-unix-sockets")]
    Unix(BufReader<UnixStream>),
}

struct WriteWrapper<T>(BufReader<T>);

impl<T> Write for WriteWrapper<T>
where
    T: Read + Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.get_mut().write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.get_mut().flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.get_mut().write_all(buf)
    }
    fn write_fmt(&mut self, fmt: Arguments) -> io::Result<()> {
        self.0.get_mut().write_fmt(fmt)
    }
}

impl<T> AsyncWrite for WriteWrapper<T>
where
    T: Read + AsyncWrite,
{
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.0.get_mut().shutdown()
    }
}

/// Represents a stateful redis TCP connection.
pub struct Connection {
    con: ActualConnection,
    db: i64,
}

macro_rules! with_connection {
    ($con:expr, $f:expr) => {
        match $con {
            #[cfg(not(feature = "with-async-unix-sockets"))]
            ActualConnection::Tcp(con) => {
                $f(con).map(|(con, value)| (ActualConnection::Tcp(con), value))
            }

            #[cfg(feature = "with-async-unix-sockets")]
            ActualConnection::Tcp(con) => {
                Either::A($f(con).map(|(con, value)| (ActualConnection::Tcp(con), value)))
            }
            #[cfg(feature = "with-async-unix-sockets")]
            ActualConnection::Unix(con) => {
                Either::B($f(con).map(|(con, value)| (ActualConnection::Unix(con), value)))
            }
        }
    };
}

macro_rules! with_write_connection {
    ($con:expr, $f:expr) => {
        match $con {
            #[cfg(not(feature = "with-async-unix-sockets"))]
            ActualConnection::Tcp(con) => {
                $f(WriteWrapper(con)).map(|(con, value)| (ActualConnection::Tcp(con.0), value))
            }

            #[cfg(feature = "with-async-unix-sockets")]
            ActualConnection::Tcp(con) => Either::A(
                $f(WriteWrapper(con)).map(|(con, value)| (ActualConnection::Tcp(con.0), value)),
            ),
            #[cfg(feature = "with-async-unix-sockets")]
            ActualConnection::Unix(con) => Either::B(
                $f(WriteWrapper(con))
                    .map(|(con, value)| (ActualConnection::Unix(con.0), value)),
            ),
        }
    };
}

impl Connection {
    pub fn read_response(self) -> RedisFuture<(Self, Value)> {
        let db = self.db;
        Box::new(
            with_connection!(self.con, ::parser::parse_async).then(move |result| {
                match result {
                    Ok((con, value)) => Ok((Connection { con: con, db }, value)),
                    Err(err) => {
                        // TODO Do we need to shutdown here as we do in the sync version?
                        Err(err)
                    }
                }
            }),
        )
    }
}

pub fn connect(
    connection_info: ConnectionInfo,
    handle: &tokio_reactor::Handle,
) -> RedisFuture<Connection> {
    let connection = match *connection_info.addr {
        ConnectionAddr::Tcp(ref host, port) => {
            let socket_addr = match (&host[..], port).to_socket_addrs() {
                Ok(mut socket_addrs) => match socket_addrs.next() {
                    Some(socket_addr) => socket_addr,
                    None => {
                        return Box::new(future::err(RedisError::from((
                            ErrorKind::InvalidClientConfig,
                            "No address found for host",
                        ))))
                    }
                },
                Err(err) => return Box::new(future::err(err.into())),
            };

            let stream_result = (|| -> io::Result<_> {
                let builder = match socket_addr {
                    SocketAddr::V4(_) => TcpBuilder::new_v4()?,
                    SocketAddr::V6(_) => TcpBuilder::new_v6()?,
                };
                Ok(builder.to_tcp_stream()?)
            })();
            let stream = match stream_result {
                Ok(stream) => stream,
                Err(err) => return Box::new(future::err(err.into())),
            };
            Either::A(
                TcpStream::connect_std(stream, &socket_addr, handle)
                    .from_err()
                    .map(|con| ActualConnection::Tcp(BufReader::new(con))),
            )
        }
        #[cfg(feature = "with-async-unix-sockets")]
        ConnectionAddr::Unix(ref path) => {
            let result = UnixStream::connect(path)
                .map(|stream| ActualConnection::Unix(BufReader::new(stream)));
            Either::B(future::result(result))
        }
        #[cfg(not(feature = "with-async-unix-sockets"))]
        ConnectionAddr::Unix(_) => Either::B(future::err(RedisError::from((
            ErrorKind::InvalidClientConfig,
            "Cannot connect to unix sockets \
             on this platform",
        )))),
    };

    Box::new(connection.from_err().and_then(move |con| {
        let rv = Connection {
            con,
            db: connection_info.db,
        };

        let login = match connection_info.passwd {
            Some(ref passwd) => {
                Either::A(cmd("AUTH").arg(&**passwd).query_async::<_, Value>(rv).then(
                    |x| match x {
                        Ok((rv, Value::Okay)) => Ok(rv),
                        _ => {
                            fail!((
                                ErrorKind::AuthenticationFailed,
                                "Password authentication failed"
                            ));
                        }
                    },
                ))
            }
            None => Either::B(future::ok(rv)),
        };

        login.and_then(move |rv| {
            if connection_info.db != 0 {
                Either::A(
                    cmd("SELECT")
                        .arg(connection_info.db)
                        .query_async::<_, Value>(rv)
                        .then(|result| match result {
                            Ok((rv, Value::Okay)) => Ok(rv),
                            _ => fail!((
                                ErrorKind::ResponseError,
                                "Redis server refused to switch database"
                            )),
                        }),
                )
            } else {
                Either::B(future::ok(rv))
            }
        })
    }))
}

pub trait ConnectionLike: Sized {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    fn req_packed_command(self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)>;

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    fn req_packed_commands(
        self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> RedisFuture<(Self, Vec<Value>)>;

    /// Returns the database this connection is bound to.  Note that this
    /// information might be unreliable because it's initially cached and
    /// also might be incorrect if the connection like object is not
    /// actually connected.
    fn get_db(&self) -> i64;
}

impl ConnectionLike for Connection {
    fn req_packed_command(self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        let db = self.db;
        Box::new(
            with_write_connection!(self.con, |con| tokio_io::io::write_all(con, cmd))
                .from_err()
                .and_then(move |(con, _)| Connection { con, db }.read_response()),
        )
    }

    fn req_packed_commands(
        self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> RedisFuture<(Self, Vec<Value>)> {
        let db = self.db;
        Box::new(
            with_write_connection!(self.con, |con| tokio_io::io::write_all(con, cmd))
                .from_err()
                .and_then(move |(con, _)| {
                    let mut con = Some(Connection { con, db });
                    let mut rv = vec![];
                    let mut future = None;
                    let mut idx = 0;
                    future::poll_fn(move || {
                        while idx < offset + count {
                            if future.is_none() {
                                future = Some(con.take().unwrap().read_response());
                            }
                            let (con2, item) = try_ready!(future.as_mut().unwrap().poll());
                            con = Some(con2);
                            future = None;

                            if idx >= offset {
                                rv.push(item);
                            }
                            idx += 1;
                        }
                        Ok(Async::Ready((
                            con.take().unwrap(),
                            mem::replace(&mut rv, Vec::new()),
                        )))
                    })
                }),
        )
    }

    fn get_db(&self) -> i64 {
        self.db
    }
}
