//! Adds experimental async IO support to redis.
use std::collections::VecDeque;
use std::io::{self};
use std::mem;
use std::net::ToSocketAddrs;
use std::pin::Pin;

#[cfg(unix)]
use tokio::net::unix::UnixStream;

use tokio::codec::Decoder;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::TcpStream;
use tokio::sync::{mpsc, oneshot};

use futures::{
    prelude::*,
    ready, task,
    task::{Spawn, SpawnExt},
    Poll, Sink, Stream,
};

use pin_project::pin_project;

use crate::cmd::{cmd, Cmd};
use crate::types::{ErrorKind, RedisError, RedisFuture, RedisResult, Value};

use crate::connection::{ConnectionAddr, ConnectionInfo};

use crate::parser::ValueCodec;

enum ActualConnection {
    Tcp(Buffered<TcpStream>),
    #[cfg(unix)]
    Unix(Buffered<UnixStream>),
}

type Buffered<T> = BufReader<BufWriter<T>>;

impl AsyncWrite for ActualConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            ActualConnection::Tcp(r) => Pin::new(r).poll_write(cx, buf),
            #[cfg(unix)]
            ActualConnection::Unix(r) => Pin::new(r).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match &mut *self {
            ActualConnection::Tcp(r) => Pin::new(r).poll_flush(cx),
            #[cfg(unix)]
            ActualConnection::Unix(r) => Pin::new(r).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match &mut *self {
            ActualConnection::Tcp(r) => Pin::new(r).poll_shutdown(cx),
            #[cfg(unix)]
            ActualConnection::Unix(r) => Pin::new(r).poll_shutdown(cx),
        }
    }
}

impl AsyncRead for ActualConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            ActualConnection::Tcp(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(unix)]
            ActualConnection::Unix(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

impl AsyncBufRead for ActualConnection {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<&[u8]>> {
        match self.get_mut() {
            ActualConnection::Tcp(r) => Pin::new(r).poll_fill_buf(cx),
            #[cfg(unix)]
            ActualConnection::Unix(r) => Pin::new(r).poll_fill_buf(cx),
        }
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        match &mut *self {
            ActualConnection::Tcp(r) => Pin::new(r).consume(amt),
            #[cfg(unix)]
            ActualConnection::Unix(r) => Pin::new(r).consume(amt),
        }
    }
}

/// Represents a stateful redis TCP connection.
pub struct Connection {
    con: ActualConnection,
    db: i64,
}

impl ActualConnection {
    pub async fn read_response(&mut self) -> RedisResult<Value> {
        let value = crate::parser::parse_redis_value_async(self).await?;
        Ok(value)
    }
}

/// Opens a connection.
pub async fn connect(connection_info: &ConnectionInfo) -> RedisResult<Connection> {
    let con = match *connection_info.addr {
        ConnectionAddr::Tcp(ref host, port) => {
            let socket_addr = match (&host[..], port).to_socket_addrs() {
                Ok(mut socket_addrs) => match socket_addrs.next() {
                    Some(socket_addr) => socket_addr,
                    None => {
                        return Err(RedisError::from((
                            ErrorKind::InvalidClientConfig,
                            "No address found for host",
                        )));
                    }
                },
                Err(err) => return Err(err.into()),
            };

            TcpStream::connect(&socket_addr)
                .map_ok(|con| ActualConnection::Tcp(BufReader::new(BufWriter::new(con))))
                .await?
        }
        #[cfg(unix)]
        ConnectionAddr::Unix(ref path) => {
            UnixStream::connect(path)
                .map_ok(|stream| ActualConnection::Unix(BufReader::new(BufWriter::new(stream))))
                .await?
        }

        #[cfg(not(unix))]
        ConnectionAddr::Unix(_) => {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "Cannot connect to unix sockets \
                 on this platform",
            )))
        }
    };

    let mut rv = Connection {
        con,
        db: connection_info.db,
    };

    match connection_info.passwd {
        Some(ref passwd) => {
            let mut cmd = cmd("AUTH");
            cmd.arg(&**passwd);
            let x = cmd.query_async::<_, Value>(&mut rv).await;
            match x {
                Ok(Value::Okay) => (),
                _ => {
                    fail!((
                        ErrorKind::AuthenticationFailed,
                        "Password authentication failed"
                    ));
                }
            }
        }
        None => (),
    }

    if connection_info.db != 0 {
        let mut cmd = cmd("SELECT");
        cmd.arg(connection_info.db);
        let result = cmd.query_async::<_, Value>(&mut rv).await;
        match result {
            Ok(Value::Okay) => Ok(rv),
            _ => fail!((
                ErrorKind::ResponseError,
                "Redis server refused to switch database"
            )),
        }
    } else {
        Ok(rv)
    }
}

/// An async abstraction over connections.
pub trait ConnectionLike: Sized {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value>;

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>>;

    /// Returns the database this connection is bound to.  Note that this
    /// information might be unreliable because it's initially cached and
    /// also might be incorrect if the connection like object is not
    /// actually connected.
    fn get_db(&self) -> i64;
}

impl ConnectionLike for Connection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        (async move {
            cmd.write_command_async(Pin::new(&mut self.con)).await?;
            self.con.flush().await?;
            self.con.read_response().await
        })
            .boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        (async move {
            cmd.write_pipeline_async(Pin::new(&mut self.con)).await?;
            self.con.flush().await?;

            for _ in 0..offset {
                self.con.read_response().await?;
            }

            let mut rv = Vec::with_capacity(count);
            for _ in 0..count {
                let item = self.con.read_response().await?;
                rv.push(item);
            }

            Ok(rv)
        })
            .boxed()
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
struct Pipeline<SinkItem, I, E>(mpsc::Sender<PipelineMessage<SinkItem, I, E>>);

impl<SinkItem, I, E> Clone for Pipeline<SinkItem, I, E> {
    fn clone(&self) -> Self {
        Pipeline(self.0.clone())
    }
}

#[pin_project]
struct PipelineSink<T, I, E>
where
    T: Stream<Item = Result<I, E>> + 'static,
{
    #[pin]
    sink_stream: T,
    in_flight: VecDeque<InFlight<I, E>>,
    error: Option<E>,
}

impl<T, I, E> PipelineSink<T, I, E>
where
    T: Stream<Item = Result<I, E>> + 'static,
{
    fn new<SinkItem>(sink_stream: T) -> Self
    where
        T: Sink<SinkItem, Error = E> + Stream<Item = Result<I, E>> + 'static,
    {
        PipelineSink {
            sink_stream,
            in_flight: VecDeque::new(),
            error: None,
        }
    }

    // Read messages from the stream and send them back to the caller
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<()> {
        loop {
            let item = match ready!(self.as_mut().project().sink_stream.poll_next(cx)) {
                Some(Ok(item)) => Ok(item),
                Some(Err(err)) => Err(err),
                // The redis response stream is not going to produce any more items so we `Err`
                // to break out of the `forward` combinator and stop handling requests
                None => return Poll::Ready(()),
            };
            self.as_mut().send_result(item);
        }
    }

    fn send_result(self: Pin<&mut Self>, result: Result<I, E>) {
        let self_ = self.project();
        let response = {
            let entry = match self_.in_flight.front_mut() {
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

        let entry = self_.in_flight.pop_front().unwrap();
        // `Err` means that the receiver was dropped in which case it does not
        // care about the output and we can continue by just dropping the value
        // and sender
        entry.output.send(response).ok();
    }
}

impl<SinkItem, T, I, E> Sink<PipelineMessage<SinkItem, I, E>> for PipelineSink<T, I, E>
where
    T: Sink<SinkItem, Error = E> + Stream<Item = Result<I, E>> + 'static,
{
    type Error = ();

    // Retrieve incoming messages and write them to the sink
    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        match ready!(self.as_mut().project().sink_stream.poll_ready(cx)) {
            Ok(()) => Ok(()).into(),
            Err(err) => {
                *self.project().error = Some(err);
                Ok(()).into()
            }
        }
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        PipelineMessage {
            input,
            output,
            response_count,
        }: PipelineMessage<SinkItem, I, E>,
    ) -> Result<(), Self::Error> {
        let self_ = self.as_mut().project();
        if let Some(err) = self_.error.take() {
            let _ = output.send(Err(err));
            return Err(());
        }
        match self_.sink_stream.start_send(input) {
            Ok(()) => {
                self_.in_flight.push_back(InFlight {
                    output,
                    response_count,
                    buffer: Vec::new(),
                });
                Ok(())
            }
            Err(err) => {
                let _ = output.send(Err(err));
                Err(())
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        ready!(self
            .as_mut()
            .project()
            .sink_stream
            .poll_flush(cx)
            .map_err(|err| {
                self.as_mut().send_result(Err(err));
            }))?;
        self.poll_read(cx).map(Ok)
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        // No new requests will come in after the first call to `close` but we need to complete any
        // in progress requests before closing
        if !self.in_flight.is_empty() {
            ready!(self.as_mut().poll_flush(cx))?;
        }
        let this = self.as_mut().project();
        this.sink_stream.poll_close(cx).map_err(|err| {
            self.send_result(Err(err));
        })
    }
}

impl<SinkItem, I, E> Pipeline<SinkItem, I, E>
where
    SinkItem: Send + 'static,
    I: Send + 'static,
    E: Send + 'static,
{
    fn new<T, F>(sink_stream: T, mut executor: F) -> Self
    where
        F: Spawn,
        T: Sink<SinkItem, Error = E> + Stream<Item = Result<I, E>> + 'static,
        T: Send + 'static,
        T::Item: Send,
        T::Error: Send,
        T::Error: ::std::fmt::Debug,
    {
        const BUFFER_SIZE: usize = 50;
        let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
        let f = receiver
            .map(Ok)
            .forward(PipelineSink::new::<SinkItem>(sink_stream))
            .map(|_| ());
        executor.spawn(f).expect("Unable to spawn redis task");
        Pipeline(sender)
    }

    // `None` means that the stream was out of items causing that poll loop to shut down.
    async fn send(&mut self, item: SinkItem) -> Result<I, Option<E>> {
        self.send_recv_multiple(item, 1)
            .map_ok(|mut item| item.pop().unwrap())
            .await
    }

    async fn send_recv_multiple(
        &mut self,
        input: SinkItem,
        count: usize,
    ) -> Result<Vec<I>, Option<E>> {
        let (sender, receiver) = oneshot::channel();

        self.0
            .send(PipelineMessage {
                input,
                response_count: count,
                output: sender,
            })
            .map_err(|_| None)
            .and_then(|_| {
                receiver.then(|result| {
                    future::ready(match result {
                        Ok(result) => result.map_err(Some),
                        Err(_) => {
                            // The `sender` was dropped which likely means that the stream part
                            // failed for one reason or another
                            Err(None)
                        }
                    })
                })
            })
            .await
    }
}

/// A connection object bound to an executor.
#[derive(Clone)]
pub struct SharedConnection {
    pipeline: Pipeline<Vec<u8>, Value, RedisError>,
    db: i64,
}

impl SharedConnection {
    /// Creates a shared connection from a connection and executor.
    pub fn new<E>(con: Connection, executor: E) -> RedisResult<Self>
    where
        E: Spawn,
    {
        let pipeline = match con.con {
            ActualConnection::Tcp(tcp) => {
                let codec = ValueCodec::default().framed(tcp.into_inner().into_inner());
                Pipeline::new(codec, executor)
            }
            #[cfg(unix)]
            ActualConnection::Unix(unix) => {
                let codec = ValueCodec::default().framed(unix.into_inner().into_inner());
                Pipeline::new(codec, executor)
            }
        };
        Ok(SharedConnection {
            pipeline,
            db: con.db,
        })
    }
}

impl ConnectionLike for SharedConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        (async move {
            let value = self
                .pipeline
                .send(cmd.get_packed_command())
                .await
                .map_err(|err| {
                    err.unwrap_or_else(|| {
                        RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe))
                    })
                })?;
            Ok(value)
        })
            .boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        (async move {
            let mut value = self
                .pipeline
                .send_recv_multiple(cmd.get_packed_pipeline(), offset + count)
                .await
                .map_err(|err| {
                    err.unwrap_or_else(|| {
                        RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe))
                    })
                })?;

            value.drain(..offset);
            Ok(value)
        })
            .boxed()
    }

    fn get_db(&self) -> i64 {
        self.db
    }
}
