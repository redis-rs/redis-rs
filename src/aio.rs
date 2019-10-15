//! Adds experimental async IO support to redis.
use std::collections::VecDeque;
use std::fmt::Arguments;
use std::io::{self, BufReader, Read, Write};
use std::mem;
use std::net::ToSocketAddrs;

#[cfg(unix)]
use tokio_uds::UnixStream;

#[cfg(feature = "tls")]
use tokio_tls::TlsStream;

use tokio_codec::{Decoder, Framed};
use tokio_io::{self, AsyncRead, AsyncWrite};
use tokio_tcp::TcpStream;

use futures::future::{Either, Executor};
use futures::{future, try_ready, Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use tokio_sync::{mpsc, oneshot};

use crate::cmd::cmd;
use crate::types::{ErrorKind, RedisError, RedisFuture, Value};

use crate::connection::{ConnectionAddr, ConnectionInfo};

use crate::parser::ValueCodec;

use std::io::BufRead;

struct WriteWrapper(Box<dyn AsyncBufRead>);

impl Write for WriteWrapper {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.get_mut().write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.get_mut().flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.get_mut().write_all(buf)
    }
    fn write_fmt(&mut self, fmt: Arguments<'_>) -> io::Result<()> {
        self.0.get_mut().write_fmt(fmt)
    }
}

impl AsyncWrite for WriteWrapper {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.0.get_mut().shutdown()
    }
}

trait AsyncStream: Read + AsyncRead + AsyncWrite + Send {}

impl AsyncStream for TcpStream {}

#[cfg(feature = "tls")]
impl AsyncStream for TlsStream<TcpStream> {}

impl AsyncStream for UnixStream {}

/// Type used to implement a BufRead which can be consumed returning the underlying stream.
trait AsyncBufRead: AsyncRead + BufRead + Send {
    fn into_stream(self: Box<Self>) -> Box<dyn AsyncStream>;
    fn get_mut(&mut self) -> &mut dyn AsyncStream;
}

impl AsyncBufRead for BufReader<TcpStream> {
    fn into_stream(self: Box<Self>) -> Box<dyn AsyncStream> {
        Box::new((*self).into_inner())
    }

    fn get_mut(&mut self) -> &mut dyn AsyncStream {
        self.get_mut()
    }
}

#[cfg(feature = "tls")]
impl AsyncBufRead for BufReader<TlsStream<TcpStream>> {
    fn into_stream(self: Box<Self>) -> Box<dyn AsyncStream> {
        Box::new((*self).into_inner())
    }

    fn get_mut(&mut self) -> &mut dyn AsyncStream {
        self.get_mut()
    }
}

impl AsyncBufRead for BufReader<UnixStream> {
    fn into_stream(self: Box<Self>) -> Box<dyn AsyncStream> {
        Box::new((*self).into_inner())
    }

    fn get_mut(&mut self) -> &mut dyn AsyncStream {
        self.get_mut()
    }
}

/// Represents a stateful redis TCP connection.
pub struct Connection {
    con: Box<dyn AsyncBufRead>,
    db: i64,
}

impl Connection {
    /// Retrieves a single response from the connection.
    pub fn read_response(self) -> impl Future<Item = (Self, Value), Error = RedisError> {
        let db = self.db;
        crate::parser::parse_redis_value_async(self.con).then(move |result| {
            match result {
                Ok((con, value)) => Ok((Connection { con, db }, value)),
                Err(err) => {
                    // TODO Do we need to shutdown here as we do in the sync version?
                    Err(err)
                }
            }
        })
    }
}

/// Opens a connection.
pub fn connect(
    connection_info: ConnectionInfo,
) -> impl Future<Item = Connection, Error = RedisError> {
    let ConnectionInfo {
        addr,
        db,
        passwd,
        tls,
    } = connection_info;
    let connection = match *addr {
        #[cfg_attr(not(feature = "tls"), allow(unused_variables))]
        ConnectionAddr::Tcp(host, port) => {
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

            if let Some(tls) = tls.as_ref() {
                #[cfg(feature = "tls")]
                {
                    use std::convert::TryInto;
                    let cx: native_tls::TlsConnector = match tls.try_into() {
                        Ok(cx) => cx,
                        Err(e) => return Either::A(future::err(e)),
                    };
                    let cx = tokio_tls::TlsConnector::from(cx);
                    CaseFuture::A(
                        TcpStream::connect(&socket_addr)
                            .and_then(move |socket| {
                                cx.connect(&host, socket)
                                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                            })
                            .map(move |con| Connection {
                                con: Box::new(BufReader::new(con)),
                                db,
                            })
                            .from_err(),
                    )
                }

                #[cfg(not(feature = "tls"))]
                CaseFuture::A(future::err(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "can't connect with TLS, the feature is not enabled",
                ))))
            } else {
                CaseFuture::B(
                    TcpStream::connect(&socket_addr)
                        .map(move |con| Connection {
                            con: Box::new(BufReader::new(con)),
                            db,
                        })
                        .from_err(),
                )
            }
        }
        #[cfg(unix)]
        ConnectionAddr::Unix(ref path) => CaseFuture::C(
            UnixStream::connect(path)
                .map(move |stream| Connection {
                    con: Box::new(BufReader::new(stream)),
                    db,
                })
                .from_err(),
        ),
        #[cfg(not(unix))]
        ConnectionAddr::Unix(_) => CaseFuture::C(future::err(RedisError::from((
            ErrorKind::InvalidClientConfig,
            "Cannot connect to unix sockets on this platform",
        )))),
    };

    let login = connection.and_then(move |con| match passwd {
        Some(ref passwd) => Either::A(
            cmd("AUTH")
                .arg(&**passwd)
                .query_async::<_, Value>(con)
                .then(|x| match x {
                    Ok((rv, Value::Okay)) => Ok(rv),
                    _ => Err(RedisError::from((
                        ErrorKind::AuthenticationFailed,
                        "Password authentication failed",
                    ))),
                }),
        ),
        None => Either::B(future::ok(con)),
    });

    let connection = login.and_then(move |con| {
        if db != 0 {
            Either::A(cmd("SELECT").arg(db).query_async::<_, Value>(con).then(
                |result| match result {
                    Ok((con, Value::Okay)) => Ok(con),
                    _ => Err(RedisError::from((
                        ErrorKind::ResponseError,
                        "Redis server refused to switch database",
                    ))),
                },
            ))
        } else {
            Either::B(future::ok(con))
        }
    });
    Either::B(connection)
}

/// An async abstraction over connections.
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
            tokio_io::io::write_all(WriteWrapper(self.con), cmd)
                .map(|(con, value)| (con.0, value))
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
            tokio_io::io::write_all(WriteWrapper(self.con), cmd)
                .map(|(con, value)| (con.0, value))
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
        // `Err` means that the receiver was dropped in which case it does not
        // care about the output and we can continue by just dropping the value
        // and sender
        entry.output.send(response).ok();
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
        // No new requests will come in after the first call to `close` but we need to complete any
        // in progress requests before closing
        if !self.in_flight.is_empty() {
            try_ready!(self.poll_complete());
        }
        self.sink_stream.close().map_err(|err| {
            self.send_result(Err(err));
        })
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
    fn new<E>(sink_stream: T, executor: E) -> Self
    where
        E: Executor<Box<dyn Future<Item = (), Error = ()> + Send>>,
    {
        const BUFFER_SIZE: usize = 50;
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        let f = receiver
            .map_err(|_| ())
            .forward(PipelineSink {
                sink_stream,
                in_flight: VecDeque::new(),
            })
            .map(|_| ());

        executor.execute(Box::new(f)).unwrap();
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

/// A connection object bound to an executor.
#[derive(Clone)]
pub struct SharedConnection {
    pipeline: Pipeline<Framed<Box<dyn AsyncStream>, ValueCodec>>,
    db: i64,
}

impl SharedConnection {
    /// Creates a shared connection from a connection and executor.
    pub fn new<E>(con: Connection, executor: E) -> impl Future<Item = Self, Error = RedisError>
    where
        E: Executor<Box<dyn Future<Item = (), Error = ()> + Send>>,
    {
        future::lazy(|| {
            let codec = ValueCodec::default().framed(con.con.into_stream());
            let pipeline = Pipeline::new(codec, executor);
            Ok(SharedConnection {
                pipeline,
                db: con.db,
            })
        })
    }
}

impl ConnectionLike for SharedConnection {
    fn req_packed_command(self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        #[cfg(not(unix))]
        let future = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => pipeline.send(cmd),
        };

        #[cfg(unix)]
        let future = self.pipeline.send(cmd);
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
        #[cfg(not(unix))]
        let future = match self.pipeline {
            ActualPipeline::Tcp(ref pipeline) => pipeline.send_recv_multiple(cmd, offset + count),
        };

        #[cfg(unix)]
        let future = self.pipeline.send_recv_multiple(cmd, offset + count);
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

/// Generalization of `futures::future::Either` future.
///
/// Use it when you need to return a future from a function based on several cases.
#[derive(Debug)]
pub enum CaseFuture<A, B, C> {
    /// A value of type `A`.
    A(A),
    /// A value of type `B`.
    B(B),
    /// A value of type `C`.
    C(C),
}

impl<A, B, C> Future for CaseFuture<A, B, C>
where
    A: Future,
    B: Future<Item = A::Item, Error = A::Error>,
    C: Future<Item = A::Item, Error = A::Error>,
{
    type Item = A::Item;
    type Error = A::Error;

    fn poll(&mut self) -> Poll<A::Item, A::Error> {
        match *self {
            CaseFuture::A(ref mut a) => a.poll(),
            CaseFuture::B(ref mut b) => b.poll(),
            CaseFuture::C(ref mut c) => c.poll(),
        }
    }
}
