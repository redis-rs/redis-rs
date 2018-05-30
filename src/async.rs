use std::collections::VecDeque;
use std::fmt::Arguments;
use std::io::{self, BufReader, Read, Write};
use std::mem;
use std::net::ToSocketAddrs;

#[cfg(feature = "with-unix-sockets")]
use tokio_uds::UnixStream;

use tokio_executor;
use tokio_io::codec::Framed;
use tokio_io::{self, AsyncRead, AsyncWrite};
use tokio_tcp::TcpStream;

use futures::future::Either;
use futures::sync::{mpsc, oneshot};
use futures::{future, stream, Async, AsyncSink, Future, Poll, Sink, Stream};

use cmd::cmd;
use types::{ErrorKind, RedisError, RedisFuture, Value};

use connection::{ConnectionAddr, ConnectionInfo};

use parser::ValueCodec;

enum ActualConnection {
    Tcp(BufReader<TcpStream>),
    #[cfg(feature = "with-unix-sockets")]
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
            #[cfg(not(feature = "with-unix-sockets"))]
            ActualConnection::Tcp(con) => {
                $f(con).map(|(con, value)| (ActualConnection::Tcp(con), value))
            }

            #[cfg(feature = "with-unix-sockets")]
            ActualConnection::Tcp(con) => {
                Either::A($f(con).map(|(con, value)| (ActualConnection::Tcp(con), value)))
            }
            #[cfg(feature = "with-unix-sockets")]
            ActualConnection::Unix(con) => {
                Either::B($f(con).map(|(con, value)| (ActualConnection::Unix(con), value)))
            }
        }
    };
}

macro_rules! with_write_connection {
    ($con:expr, $f:expr) => {
        match $con {
            #[cfg(not(feature = "with-unix-sockets"))]
            ActualConnection::Tcp(con) => {
                $f(WriteWrapper(con)).map(|(con, value)| (ActualConnection::Tcp(con.0), value))
            }

            #[cfg(feature = "with-unix-sockets")]
            ActualConnection::Tcp(con) => Either::A(
                $f(WriteWrapper(con)).map(|(con, value)| (ActualConnection::Tcp(con.0), value)),
            ),
            #[cfg(feature = "with-unix-sockets")]
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

pub fn connect(connection_info: ConnectionInfo) -> RedisFuture<Connection> {
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

            Either::A(
                TcpStream::connect(&socket_addr)
                    .from_err()
                    .map(|con| ActualConnection::Tcp(BufReader::new(con))),
            )
        }
        #[cfg(feature = "with-unix-sockets")]
        ConnectionAddr::Unix(ref path) => Either::B(
            UnixStream::connect(path).map(|stream| ActualConnection::Unix(BufReader::new(stream))),
        ),
        #[cfg(not(feature = "with-unix-sockets"))]
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

struct Pipeline<T>(
    mpsc::UnboundedSender<(T::SinkItem, Vec<oneshot::Sender<Result<T::Item, T::Error>>>)>,
)
where
    T: Stream + Sink;

impl<T> Clone for Pipeline<T>
where
    T: Stream + Sink,
{
    fn clone(&self) -> Self {
        Pipeline(self.0.clone())
    }
}

enum SendState<T> {
    Unsent(T),
    Sent,
    Free,
}

struct PipelineFuture<T>
where
    T: Sink<SinkError = <T as Stream>::Error> + Stream + 'static,
{
    sink_stream: T,
    in_flight: VecDeque<Vec<oneshot::Sender<Result<T::Item, T::Error>>>>,
    senders: Vec<oneshot::Sender<Result<T::Item, T::Error>>>,
    incoming:
        mpsc::UnboundedReceiver<(T::SinkItem, Vec<oneshot::Sender<Result<T::Item, T::Error>>>)>,
    send_state: SendState<T::SinkItem>,
}

impl<T> Future for PipelineFuture<T>
where
    T: Sink<SinkError = <T as Stream>::Error> + Stream + 'static,
    T::Error: ::std::fmt::Debug,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let read_ready = self.poll_read()?;
            match self.poll_write() {
                Ok(Async::Ready(Some(()))) => (),
                Ok(Async::Ready(None)) => {
                    if self.in_flight.is_empty() {
                        return Ok(Async::Ready(()));
                    }
                }
                Ok(Async::NotReady) => {
                    return Ok(Async::NotReady);
                }
                Err(err) => {
                    self.send_result(Err(err));
                    continue;
                }
            }
            return Ok(read_ready);
        }
    }
}

impl<T> PipelineFuture<T>
where
    T: Sink<SinkError = <T as Stream>::Error> + Stream + 'static,
{
    fn poll_read(&mut self) -> Poll<(), ()> {
        loop {
            let item = match self.sink_stream.poll() {
                Ok(Async::Ready(Some(item))) => Ok(item),
                Ok(Async::Ready(None)) => return Ok(Async::Ready(())),
                Err(err) => Err(err),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
            };
            self.send_result(item);
        }
    }
    fn send_result(&mut self, item: Result<T::Item, T::Error>) {
        let sender = match self.senders.pop() {
            Some(sender) => sender,
            None => {
                self.senders = self.in_flight.pop_front().expect("Matching sender");
                self.senders.reverse();
                match self.senders.pop() {
                    Some(sender) => sender,
                    None => return,
                }
            }
        };
        match sender.send(item) {
            Ok(_) => (),
            Err(_) => {
                // `Err` means that the receiver were dropped in which case it does not
                // care about the output and we can continue by just dropping the value
                // and sender
            }
        }
    }

    fn poll_write(&mut self) -> Poll<Option<()>, T::Error> {
        loop {
            match mem::replace(&mut self.send_state, SendState::Free) {
                SendState::Unsent(item) => match self.sink_stream.start_send(item)? {
                    AsyncSink::Ready => {
                        // Try to take another incoming and start sending it as well
                        // before completing the send to give the sink a chance to coalesce
                        // multiple messages into a large one.
                        match self.incoming.poll() {
                            Ok(Async::NotReady) | Ok(Async::Ready(None)) | Err(()) => {
                                self.send_state = SendState::Sent
                            }
                            Ok(Async::Ready(Some((item, senders)))) => {
                                self.in_flight.push_back(senders);
                                self.send_state = SendState::Unsent(item);
                            }
                        }
                    }
                    AsyncSink::NotReady(item) => {
                        self.send_state = SendState::Unsent(item);
                        return Ok(Async::NotReady);
                    }
                },
                SendState::Sent => {
                    match self.sink_stream.poll_complete()? {
                        Async::Ready(()) => (),
                        Async::NotReady => {
                            self.send_state = SendState::Sent;
                            return Ok(Async::NotReady);
                        }
                    }
                    self.send_state = SendState::Free;
                    return Ok(Async::Ready(Some(())));
                }
                SendState::Free => {
                    let (item, senders) = match self.incoming.poll() {
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Ok(Async::Ready(Some(x))) => x,
                        Ok(Async::Ready(None)) | Err(()) => return Ok(Async::Ready(None)),
                    };
                    self.in_flight.push_back(senders);
                    self.send_state = SendState::Unsent(item);
                }
            }
        }
    }
}

impl<T> Pipeline<T>
where
    T: Sink<SinkError = <T as Stream>::Error> + Stream + Send + 'static,
    T::SinkItem: Send,
    T::Item: Send,
    T::Error: Send,
    T::Error: ::std::fmt::Debug,
{
    fn new(sink_stream: T) -> Self {
        let (sender, receiver) = mpsc::unbounded();
        tokio_executor::spawn(PipelineFuture {
            sink_stream,
            in_flight: VecDeque::new(),
            senders: Vec::new(),
            send_state: SendState::Free,
            incoming: receiver,
        });
        Pipeline(sender)
    }

    // `None` means that the stream was out of items causing that poll loop to shut down.
    fn send(
        &self,
        item: T::SinkItem,
    ) -> Box<Future<Item = T::Item, Error = Option<T::Error>> + Send> {
        Box::new(
            self.send_recv_multiple(item, 1)
                .into_future()
                .map(|(item, _)| item.unwrap())
                .map_err(|(err, _)| err),
        )
    }

    fn send_recv_multiple(
        &self,
        item: T::SinkItem,
        count: usize,
    ) -> Box<Stream<Item = T::Item, Error = Option<T::Error>> + Send> {
        let self_ = self.0.clone();

        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..count {
            let (sender, receiver) = oneshot::channel();
            senders.push(sender);
            receivers.push(receiver);
        }

        Box::new(
            self_
                .send((item, senders))
                .map_err(|_| panic!("Unexpected close of redis mpsc sender"))
                .map(|_| {
                    stream::futures_ordered(receivers.into_iter().map(|receiver| {
                        receiver.then(|result| match result {
                            Ok(result) => result.map_err(Some),
                            Err(_) => {
                                // The `sender` was dropped which likely means that the stream part
                                // failed for one reason or another
                                Err(None)
                            }
                        })
                    }))
                })
                .flatten_stream(),
        )
    }
}

#[derive(Clone)]
enum ActualPipeline {
    Tcp(Pipeline<Framed<TcpStream, ValueCodec>>),
    #[cfg(feature = "with-async-unix-sockets")]
    Unix(Pipeline<Framed<UnixStream, ValueCodec>>),
}

#[derive(Clone)]
pub struct SharedConnection {
    pipeline: ActualPipeline,
    db: i64,
}

impl SharedConnection {
    pub fn new(con: Connection) -> RedisFuture<Self> {
        Box::new(future::lazy(|| {
            let pipeline = match con.con {
                ActualConnection::Tcp(tcp) => ActualPipeline::Tcp(Pipeline::new(
                    tcp.into_inner().framed(ValueCodec::default()),
                )),
                #[cfg(feature = "with-async-unix-sockets")]
                ActualConnection::Unix(unix) => ActualPipeline::Unix(Pipeline::new(
                    unix.into_inner().framed(ValueCodec::default()),
                )),
            };
            Ok(SharedConnection {
                pipeline,
                db: con.db,
            })
        }))
    }
}

impl ConnectionLike for SharedConnection {
    fn req_packed_command(self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        let future = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => pipeline.send(cmd),
            #[cfg(feature = "with-async-unix-sockets")]
            ActualPipeline::Unix(ref pipeline) => pipeline.send(cmd),
        };
        Box::new(future.map(|value| (self, value)).map_err(|err| {
            err.unwrap_or_else(|| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
        }))
    }

    fn req_packed_commands(
        self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> RedisFuture<(Self, Vec<Value>)> {
        let stream = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => pipeline.send_recv_multiple(cmd, offset + count),
            #[cfg(feature = "with-async-unix-sockets")]
            ActualPipeline::Unix(ref pipeline) => pipeline.send_recv_multiple(cmd, offset + count),
        };
        Box::new(
            stream
                .skip(offset as u64)
                .collect()
                .map(|value| (self, value))
                .map_err(|err| {
                    err.unwrap_or_else(|| {
                        RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe))
                    })
                }),
        )
    }

    fn get_db(&self) -> i64 {
        self.db
    }
}
