use std::collections::VecDeque;
use std::fmt::Arguments;
use std::io::{self, BufReader, Read, Write};
use std::mem;
use std::net::ToSocketAddrs;

#[cfg(feature = "with-unix-sockets")]
use tokio_uds::UnixStream;

use tokio_codec::{Decoder, Framed};
use tokio_executor;
use tokio_io::{self, AsyncWrite};
use tokio_tcp::TcpStream;

use futures::future::Either;
use futures::{future, Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use tokio_sync::{mpsc, oneshot};

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
                $f(WriteWrapper(con)).map(|(con, value)| (ActualConnection::Unix(con.0), value)),
            ),
        }
    };
}

impl Connection {
    pub fn read_response(self) -> impl Future<Item = (Self, Value), Error = RedisError> {
        let db = self.db;
        with_connection!(self.con, ::parser::parse_async).then(move |result| {
            match result {
                Ok((con, value)) => Ok((Connection { con: con, db }, value)),
                Err(err) => {
                    // TODO Do we need to shutdown here as we do in the sync version?
                    Err(err)
                }
            }
        })
    }
}

pub fn connect(
    connection_info: ConnectionInfo,
) -> impl Future<Item = Connection, Error = RedisError> {
    let connection = match *connection_info.addr {
        ConnectionAddr::Tcp(ref host, port) => {
            let socket_addr = match (&host[..], port).to_socket_addrs() {
                Ok(mut socket_addrs) => match socket_addrs.next() {
                    Some(socket_addr) => socket_addr,
                    None => {
                        return Either::A(future::err(RedisError::from((
                            ErrorKind::InvalidClientConfig,
                            "No address found for host",
                        ))));
                    }
                },
                Err(err) => return Either::A(future::err(err.into())),
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

    Either::B(connection.from_err().and_then(move |con| {
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

// Senders which the result of a single request are sent through
type PipelineOutput<O, E> = oneshot::Sender<Result<Vec<O>, E>>;

struct InFlight<O, E> {
    output: PipelineOutput<O, E>,
    response_count: usize,
    buffer: Vec<O>,
}

// A single message sent through the pipeline
struct PipelineMessage<S, I, E> {
    input: S,
    output: PipelineOutput<I, E>,
    response_count: usize,
}

/// Wrapper around a `Stream + Sink` where each item sent through the `Sink` results in one or more
/// items being output by the `Stream` (the number is specified at time of sending). With the
/// interface provided by `Pipeline` an easy interface of request to response, hiding the `Stream`
/// and `Sink`.
struct Pipeline<T>(mpsc::Sender<PipelineMessage<T::SinkItem, T::Item, T::Error>>)
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

struct PipelineSink<T>
where
    T: Sink<SinkError = <T as Stream>::Error> + Stream + 'static,
{
    sink_stream: T,
    in_flight: VecDeque<InFlight<T::Item, T::Error>>,
}

impl<T> PipelineSink<T>
where
    T: Sink<SinkError = <T as Stream>::Error> + Stream + 'static,
{
    // Read messages from the stream and send them back to the caller
    fn poll_read(&mut self) -> Poll<(), ()> {
        loop {
            let item = match self.sink_stream.poll() {
                Ok(Async::Ready(Some(item))) => Ok(item),
                // The redis response stream is not going to produce any more items so we `Err`
                // to break out of the `forward` combinator and stop handling requests
                Ok(Async::Ready(None)) => return Err(()),
                Err(err) => Err(err),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
            };
            self.send_result(item);
        }
    }

    fn send_result(&mut self, result: Result<T::Item, T::Error>) {
        let response = {
            let entry = match self.in_flight.front_mut() {
                Some(entry) => entry,
                None => return,
            };
            match result {
                Ok(item) => {
                    entry.buffer.push(item);
                    if entry.response_count > entry.buffer.len() {
                        // Need to gather more response values
                        return;
                    }
                    Ok(mem::replace(&mut entry.buffer, Vec::new()))
                }
                // If we fail we must respond immediately
                Err(err) => Err(err),
            }
        };

        let entry = self.in_flight.pop_front().unwrap();
        match entry.output.send(response) {
            Ok(_) => (),
            Err(_) => {
                // `Err` means that the receiver was dropped in which case it does not
                // care about the output and we can continue by just dropping the value
                // and sender
            }
        }
    }
}

impl<T> Sink for PipelineSink<T>
where
    T: Sink<SinkError = <T as Stream>::Error> + Stream + 'static,
{
    type SinkItem = PipelineMessage<T::SinkItem, T::Item, T::Error>;
    type SinkError = ();

    // Retrieve incoming messages and write them to the sink
    fn start_send(
        &mut self,
        PipelineMessage {
            input,
            output,
            response_count,
        }: Self::SinkItem,
    ) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self.sink_stream.start_send(input) {
            Ok(AsyncSink::NotReady(input)) => Ok(AsyncSink::NotReady(PipelineMessage {
                input,
                output,
                response_count,
            })),
            Ok(AsyncSink::Ready) => {
                self.in_flight.push_back(InFlight {
                    output,
                    response_count,
                    buffer: Vec::new(),
                });
                Ok(AsyncSink::Ready)
            }
            Err(err) => {
                let _ = output.send(Err(err));
                Err(())
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        try_ready!(self.sink_stream.poll_complete().map_err(|err| {
            self.send_result(Err(err));
        }));
        self.poll_read()
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        try_ready!(self.sink_stream.close().map_err(|err| {
            self.send_result(Err(err));
        }));
        self.poll_read()
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
        const BUFFER_SIZE: usize = 50;
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        tokio_executor::spawn(
            receiver
                .map_err(|_| ())
                .forward(PipelineSink {
                    sink_stream,
                    in_flight: VecDeque::new(),
                })
                .map(|_| ()),
        );
        Pipeline(sender)
    }

    // `None` means that the stream was out of items causing that poll loop to shut down.
    fn send(
        &self,
        item: T::SinkItem,
    ) -> impl Future<Item = T::Item, Error = Option<T::Error>> + Send {
        self.send_recv_multiple(item, 1)
            .map(|mut item| item.pop().unwrap())
    }

    fn send_recv_multiple(
        &self,
        input: T::SinkItem,
        count: usize,
    ) -> impl Future<Item = Vec<T::Item>, Error = Option<T::Error>> + Send {
        let self_ = self.0.clone();

        let (sender, receiver) = oneshot::channel();

        self_
            .send(PipelineMessage {
                input,
                response_count: count,
                output: sender,
            })
            .map_err(|_| None)
            .and_then(|_| {
                receiver.then(|result| match result {
                    Ok(result) => result.map_err(Some),
                    Err(_) => {
                        // The `sender` was dropped which likely means that the stream part
                        // failed for one reason or another
                        Err(None)
                    }
                })
            })
    }
}

#[derive(Clone)]
enum ActualPipeline {
    Tcp(Pipeline<Framed<TcpStream, ValueCodec>>),
    #[cfg(feature = "with-unix-sockets")]
    Unix(Pipeline<Framed<UnixStream, ValueCodec>>),
}

#[derive(Clone)]
pub struct SharedConnection {
    pipeline: ActualPipeline,
    db: i64,
}

impl SharedConnection {
    pub fn new(con: Connection) -> impl Future<Item = Self, Error = RedisError> {
        future::lazy(|| {
            let pipeline = match con.con {
                ActualConnection::Tcp(tcp) => {
                    let codec = ValueCodec::default().framed(tcp.into_inner());
                    ActualPipeline::Tcp(Pipeline::new(codec))
                }
                #[cfg(feature = "with-unix-sockets")]
                ActualConnection::Unix(unix) => {
                    let codec = ValueCodec::default().framed(unix.into_inner());
                    ActualPipeline::Unix(Pipeline::new(codec))
                }
            };
            Ok(SharedConnection {
                pipeline,
                db: con.db,
            })
        })
    }
}

impl ConnectionLike for SharedConnection {
    fn req_packed_command(self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        #[cfg(not(feature = "with-unix-sockets"))]
        let future = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => pipeline.send(cmd),
        };

        #[cfg(feature = "with-unix-sockets")]
        let future = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => Either::A(pipeline.send(cmd)),
            ActualPipeline::Unix(ref pipeline) => Either::B(pipeline.send(cmd)),
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
        #[cfg(not(feature = "with-unix-sockets"))]
        let future = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => pipeline.send_recv_multiple(cmd, offset + count),
        };

        #[cfg(feature = "with-unix-sockets")]
        let future = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => {
                Either::A(pipeline.send_recv_multiple(cmd, offset + count))
            }
            ActualPipeline::Unix(ref pipeline) => {
                Either::B(pipeline.send_recv_multiple(cmd, offset + count))
            }
        };

        Box::new(
            future
                .map(move |mut value| {
                    value.drain(..offset);
                    (self, value)
                })
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
