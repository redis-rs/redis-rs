//! Adds experimental async IO support to redis.
use async_trait::async_trait;
use std::collections::VecDeque;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
#[cfg(unix)]
use std::path::Path;
use std::pin::Pin;
use std::task::{self, Poll};

use combine::{parser::combinator::AnySendSyncPartialState, stream::PointerOffset};

#[cfg(all(unix, feature = "tokio-comp"))]
use tokio::net::UnixStream as UnixStreamTokio;

use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot},
};

#[cfg(feature = "tokio-comp")]
use tokio::net::TcpStream as TcpStreamTokio;

#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
use tokio_util::codec::Decoder;

use futures_util::{
    future::{Future, FutureExt, TryFutureExt},
    ready,
    sink::Sink,
    stream::{Stream, StreamExt, TryStreamExt as _},
};

use pin_project_lite::pin_project;

use crate::cmd::{cmd, Cmd};
use crate::connection::Msg;
use crate::connection::{ConnectionAddr, ConnectionInfo};

#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
use crate::parser::ValueCodec;
use crate::types::{ErrorKind, RedisError, RedisFuture, RedisResult, Value};
use crate::{from_redis_value, ToRedisArgs};

#[cfg(feature = "async-std-comp")]
use crate::aio_async_std;

/// Represents the ability of connecting via TCP or via Unix socket
#[async_trait]
pub(crate) trait Connect {
    /// Performs a TCP connection
    async fn connect_tcp(socket_addr: SocketAddr) -> RedisResult<ActualConnection>;
    /// Performans an UNIX connection
    #[cfg(unix)]
    async fn connect_unix(path: &Path) -> RedisResult<ActualConnection>;
}

#[cfg(feature = "tokio-comp")]
mod tokio_aio {
    use super::{async_trait, ActualConnection, Connect, RedisResult, SocketAddr, TcpStreamTokio};

    #[cfg(unix)]
    use super::{Path, UnixStreamTokio};

    pub struct Tokio;

    #[async_trait]
    impl Connect for Tokio {
        async fn connect_tcp(socket_addr: SocketAddr) -> RedisResult<ActualConnection> {
            Ok(TcpStreamTokio::connect(&socket_addr)
                .await
                .map(ActualConnection::TcpTokio)?)
        }
        #[cfg(unix)]
        async fn connect_unix(path: &Path) -> RedisResult<ActualConnection> {
            Ok(UnixStreamTokio::connect(path)
                .await
                .map(ActualConnection::UnixTokio)?)
        }
    }
}

/// Represents an async Connection (TCP or Unix. Tokio or Async Std)
pub(crate) enum ActualConnection {
    /// Represents a Tokio TCP connection.
    #[cfg(feature = "tokio-comp")]
    TcpTokio(TcpStreamTokio),
    /// Represents a Tokio Unix connection.
    #[cfg(unix)]
    #[cfg(feature = "tokio-comp")]
    UnixTokio(UnixStreamTokio),
    /// Represents an Async_std TCP connection.
    #[cfg(feature = "async-std-comp")]
    TcpAsyncStd(aio_async_std::TcpStreamAsyncStdWrapped),
    /// Represents an Async_std Unix connection.
    #[cfg(feature = "async-std-comp")]
    #[cfg(unix)]
    UnixAsyncStd(aio_async_std::UnixStreamAsyncStdWrapped),
}

impl AsyncWrite for ActualConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            #[cfg(feature = "tokio-comp")]
            ActualConnection::TcpTokio(r) => Pin::new(r).poll_write(cx, buf),
            #[cfg(unix)]
            #[cfg(feature = "tokio-comp")]
            ActualConnection::UnixTokio(r) => Pin::new(r).poll_write(cx, buf),
            #[cfg(feature = "async-std-comp")]
            ActualConnection::TcpAsyncStd(r) => Pin::new(r).poll_write(cx, buf),
            #[cfg(feature = "async-std-comp")]
            #[cfg(unix)]
            ActualConnection::UnixAsyncStd(r) => Pin::new(r).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match &mut *self {
            #[cfg(feature = "tokio-comp")]
            ActualConnection::TcpTokio(r) => Pin::new(r).poll_flush(cx),
            #[cfg(unix)]
            #[cfg(feature = "tokio-comp")]
            ActualConnection::UnixTokio(r) => Pin::new(r).poll_flush(cx),
            #[cfg(feature = "async-std-comp")]
            ActualConnection::TcpAsyncStd(r) => Pin::new(r).poll_flush(cx),
            #[cfg(feature = "async-std-comp")]
            #[cfg(unix)]
            ActualConnection::UnixAsyncStd(r) => Pin::new(r).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match &mut *self {
            #[cfg(feature = "tokio-comp")]
            ActualConnection::TcpTokio(r) => Pin::new(r).poll_shutdown(cx),
            #[cfg(unix)]
            #[cfg(feature = "tokio-comp")]
            ActualConnection::UnixTokio(r) => Pin::new(r).poll_shutdown(cx),
            #[cfg(feature = "async-std-comp")]
            ActualConnection::TcpAsyncStd(r) => Pin::new(r).poll_shutdown(cx),
            #[cfg(feature = "async-std-comp")]
            #[cfg(unix)]
            ActualConnection::UnixAsyncStd(r) => Pin::new(r).poll_shutdown(cx),
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
            #[cfg(feature = "tokio-comp")]
            ActualConnection::TcpTokio(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(unix)]
            #[cfg(feature = "tokio-comp")]
            ActualConnection::UnixTokio(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(feature = "async-std-comp")]
            ActualConnection::TcpAsyncStd(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(feature = "async-std-comp")]
            #[cfg(unix)]
            ActualConnection::UnixAsyncStd(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

/// Represents a `PubSub` connection.
pub struct PubSub(Connection);

impl PubSub {
    fn new(con: Connection) -> Self {
        Self(con)
    }

    /// Subscribes to a new channel.
    pub async fn subscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        Ok(cmd("SUBSCRIBE")
            .arg(channel)
            .query_async(&mut self.0)
            .await?)
    }

    /// Subscribes to a new channel with a pattern.
    pub async fn psubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        Ok(cmd("PSUBSCRIBE")
            .arg(pchannel)
            .query_async(&mut self.0)
            .await?)
    }

    /// Unsubscribes from a channel.
    pub async fn unsubscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        Ok(cmd("UNSUBSCRIBE")
            .arg(channel)
            .query_async(&mut self.0)
            .await?)
    }

    /// Unsubscribes from a channel with a pattern.
    pub async fn punsubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        Ok(cmd("PUNSUBSCRIBE")
            .arg(pchannel)
            .query_async(&mut self.0)
            .await?)
    }

    /// Returns [`Stream`] of [`Msg`]s from this [`PubSub`]s subscriptions.
    ///
    /// The message itself is still generic and can be converted into an appropriate type through
    /// the helper methods on it.
    pub fn on_message<'a>(&'a mut self) -> impl Stream<Item = Msg> + 'a {
        ValueCodec::default()
            .framed(&mut self.0.con)
            .into_stream()
            .filter_map(|msg| Box::pin(async move { Msg::from_value(&msg.ok()?) }))
    }

    /// Returns [`Stream`] of [`Msg`]s from this [`PubSub`]s subscriptions consuming it.
    ///
    /// The message itself is still generic and can be converted into an appropriate type through
    /// the helper methods on it.
    /// This can be useful in cases where the stream needs to be returned or held by something other
    //  than the [`PubSub`].
    pub fn into_on_message(self) -> impl Stream<Item = Msg> {
        ValueCodec::default()
            .framed(self.0.con)
            .into_stream()
            .filter_map(|msg| Box::pin(async move { Msg::from_value(&msg.ok()?) }))
    }

    /// Exits from `PubSub` mode and converts [`PubSub`] into [`Connection`].
    pub async fn into_connection(mut self) -> Connection {
        self.0.exit_pubsub().await.ok();

        self.0
    }
}

/// Represents a stateful redis TCP connection.
pub struct Connection {
    con: ActualConnection,
    buf: Vec<u8>,
    decoder: combine::stream::Decoder<AnySendSyncPartialState, PointerOffset<[u8]>>,
    db: i64,

    /// Flag indicating whether the connection was left in the PubSub state after dropping `PubSub`.
    ///
    /// This flag is checked when attempting to send a command, and if it's raised, we attempt to
    /// exit the pubsub state before executing the new request.
    pubsub: bool,
}

fn assert_sync<T: Sync>() {}

#[allow(unused)]
fn test() {
    assert_sync::<Connection>();
}

impl Connection {
    /// Converts this [`Connection`] into [`PubSub`].
    pub fn into_pubsub(self) -> PubSub {
        PubSub::new(self)
    }

    /// Fetches a single response from the connection.
    async fn read_response(&mut self) -> RedisResult<Value> {
        crate::parser::parse_redis_value_async(&mut self.decoder, &mut self.con).await
    }

    /// Brings [`Connection`] out of `PubSub` mode.
    ///
    /// This will unsubscribe this [`Connection`] from all subscriptions.
    ///
    /// If this function returns error then on all command send tries will be performed attempt
    /// to exit from `PubSub` mode until it will be successful.
    async fn exit_pubsub(&mut self) -> RedisResult<()> {
        let res = self.clear_active_subscriptions().await;
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
    async fn clear_active_subscriptions(&mut self) -> RedisResult<()> {
        // Responses to unsubscribe commands return in a 3-tuple with values
        // ("unsubscribe" or "punsubscribe", name of subscription removed, count of remaining subs).
        // The "count of remaining subs" includes both pattern subscriptions and non pattern
        // subscriptions. Thus, to accurately drain all unsubscribe messages received from the
        // server, both commands need to be executed at once.
        {
            // Prepare both unsubscribe commands
            let unsubscribe = crate::Pipeline::new()
                .add_command(cmd("UNSUBSCRIBE"))
                .add_command(cmd("PUNSUBSCRIBE"))
                .get_packed_pipeline();

            // Execute commands
            self.con.write_all(&unsubscribe).await?;
        }

        // Receive responses
        //
        // There will be at minimum two responses - 1 for each of punsubscribe and unsubscribe
        // commands. There may be more responses if there are active subscriptions. In this case,
        // messages are received until the _subscription count_ in the responses reach zero.
        let mut received_unsub = false;
        let mut received_punsub = false;
        loop {
            let res: (Vec<u8>, (), isize) = from_redis_value(&self.read_response().await?)?;

            match res.0.first() {
                Some(&b'u') => received_unsub = true,
                Some(&b'p') => received_punsub = true,
                _ => (),
            }

            if received_unsub && received_punsub && res.2 == 0 {
                break;
            }
        }

        // Finally, the connection is back in its normal state since all subscriptions were
        // cancelled *and* all unsubscribe messages were received.
        Ok(())
    }
}

/// Opens a connection.
#[cfg(feature = "tokio-comp")]
pub async fn connect_tokio(connection_info: &ConnectionInfo) -> RedisResult<Connection> {
    let con = connect_simple::<tokio_aio::Tokio>(connection_info).await?;

    prepare_connection(con, connection_info).await
}

/// Opens a connection.
#[cfg(feature = "async-std-comp")]
pub async fn connect_async_std(connection_info: &ConnectionInfo) -> RedisResult<Connection> {
    let con = connect_simple::<aio_async_std::AsyncStd>(connection_info).await?;

    prepare_connection(con, connection_info).await
}

async fn prepare_connection(
    con: ActualConnection,
    connection_info: &ConnectionInfo,
) -> RedisResult<Connection> {
    let mut rv = Connection {
        con,
        buf: Vec::new(),
        decoder: combine::stream::Decoder::new(),
        db: connection_info.db,
        pubsub: false,
    };

    authenticate(connection_info, &mut rv).await?;

    Ok(rv)
}

async fn authenticate<C>(connection_info: &ConnectionInfo, con: &mut C) -> RedisResult<()>
where
    C: ConnectionLike,
{
    if let Some(passwd) = &connection_info.passwd {
        let mut command = cmd("AUTH");
        if let Some(username) = &connection_info.username {
            command.arg(username);
        }
        match command.arg(passwd).query_async(con).await {
            Ok(Value::Okay) => (),
            _ => {
                fail!((
                    ErrorKind::AuthenticationFailed,
                    "Password authentication failed"
                ));
            }
        }
    }

    if connection_info.db != 0 {
        match cmd("SELECT").arg(connection_info.db).query_async(con).await {
            Ok(Value::Okay) => (),
            _ => fail!((
                ErrorKind::ResponseError,
                "Redis server refused to switch database"
            )),
        }
    }

    Ok(())
}

async fn connect_simple<T: Connect>(
    connection_info: &ConnectionInfo,
) -> RedisResult<ActualConnection> {
    Ok(match *connection_info.addr {
        ConnectionAddr::Tcp(ref host, port) => {
            let socket_addr = get_socket_addrs(host, port)?;

            <T>::connect_tcp(socket_addr).await?
        }

        #[cfg(unix)]
        ConnectionAddr::Unix(ref path) => <T>::connect_unix(path).await?,

        #[cfg(not(unix))]
        ConnectionAddr::Unix(_) => {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "Cannot connect to unix sockets \
                 on this platform",
            )))
        }
    })
}

fn get_socket_addrs(host: &str, port: u16) -> RedisResult<SocketAddr> {
    let mut socket_addrs = (&host[..], port).to_socket_addrs()?;
    match socket_addrs.next() {
        Some(socket_addr) => Ok(socket_addr),
        None => Err(RedisError::from((
            ErrorKind::InvalidClientConfig,
            "No address found for host",
        ))),
    }
}

/// An async abstraction over connections.
pub trait ConnectionLike {
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
            if self.pubsub {
                self.exit_pubsub().await?;
            }
            self.buf.clear();
            cmd.write_packed_command(&mut self.buf);
            self.con.write_all(&self.buf).await?;
            self.read_response().await
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
            if self.pubsub {
                self.exit_pubsub().await?;
            }

            self.buf.clear();
            cmd.write_packed_pipeline(&mut self.buf);
            self.con.write_all(&self.buf).await?;

            for _ in 0..offset {
                self.read_response().await?;
            }

            let mut rv = Vec::with_capacity(count);
            for _ in 0..count {
                rv.push(self.read_response().await?);
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

pin_project! {
    struct PipelineSink<T, I, E> {
        #[pin]
        sink_stream: T,
        in_flight: VecDeque<InFlight<I, E>>,
        error: Option<E>,
    }
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
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Result<(), ()>> {
        loop {
            let item = match ready!(self.as_mut().project().sink_stream.poll_next(cx)) {
                Some(Ok(item)) => Ok(item),
                Some(Err(err)) => Err(err),
                // The redis response stream is not going to produce any more items so we `Err`
                // to break out of the `forward` combinator and stop handling requests
                None => return Poll::Ready(Err(())),
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
        self.poll_read(cx)
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
    fn new<T>(sink_stream: T) -> (Self, impl Future<Output = ()>)
    where
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
        (Pipeline(sender), f)
    }

    // `None` means that the stream was out of items causing that poll loop to shut down.
    async fn send(&mut self, item: SinkItem) -> Result<I, Option<E>> {
        self.send_recv_multiple(item, 1)
            // We can unwrap since we do a request for `1` item
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
                receiver.map(|result| {
                    match result {
                        Ok(result) => result.map_err(Some),
                        Err(_) => {
                            // The `sender` was dropped which likely means that the stream part
                            // failed for one reason or another
                            Err(None)
                        }
                    }
                })
            })
            .await
    }
}

/// A connection object which can be cloned, allowing requests to be be sent concurrently
/// on the same underlying connection (tcp/unix socket).
#[derive(Clone)]
pub struct MultiplexedConnection {
    pipeline: Pipeline<Vec<u8>, Value, RedisError>,
    db: i64,
}

impl MultiplexedConnection {
    /// Creates a multiplexed connection from a connection and executor.
    #[cfg(feature = "tokio-comp")]
    pub(crate) async fn new_tokio(
        connection_info: &ConnectionInfo,
    ) -> RedisResult<(Self, impl Future<Output = ()>)> {
        let con = connect_simple::<tokio_aio::Tokio>(connection_info).await?;
        Ok(MultiplexedConnection::create_connection(connection_info, con).await?)
    }
    /// Creates a multiplexed connection from a connection and executor.
    #[cfg(feature = "async-std-comp")]
    pub(crate) async fn new_async_std(
        connection_info: &ConnectionInfo,
    ) -> RedisResult<(Self, impl Future<Output = ()>)> {
        let con = connect_simple::<aio_async_std::AsyncStd>(connection_info).await?;
        MultiplexedConnection::create_connection(connection_info, con).await
    }

    async fn create_connection(
        connection_info: &ConnectionInfo,
        con: ActualConnection,
    ) -> RedisResult<(Self, impl Future<Output = ()>)> {
        fn boxed(
            f: impl Future<Output = ()> + Send + 'static,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(f)
        }

        #[cfg(all(not(feature = "tokio-comp"), not(feature = "async-std-comp")))]
        compile_error!("tokio-comp or async-std-comp features required for aio feature");

        let (pipeline, driver) = match con {
            #[cfg(feature = "tokio-comp")]
            ActualConnection::TcpTokio(tcp) => {
                let codec = ValueCodec::default().framed(tcp);
                let (pipeline, driver) = Pipeline::new(codec);
                (pipeline, boxed(driver))
            }
            #[cfg(feature = "async-std-comp")]
            ActualConnection::TcpAsyncStd(tcp) => {
                let codec = ValueCodec::default().framed(tcp);
                let (pipeline, driver) = Pipeline::new(codec);
                (pipeline, boxed(driver))
            }
            #[cfg(unix)]
            #[cfg(feature = "tokio-comp")]
            ActualConnection::UnixTokio(unix) => {
                let codec = ValueCodec::default().framed(unix);
                let (pipeline, driver) = Pipeline::new(codec);

                (pipeline, boxed(driver))
            }
            #[cfg(unix)]
            #[cfg(feature = "async-std-comp")]
            ActualConnection::UnixAsyncStd(unix) => {
                let codec = ValueCodec::default().framed(unix);
                let (pipeline, driver) = Pipeline::new(codec);
                (pipeline, boxed(driver))
            }
        };
        let mut con = MultiplexedConnection {
            pipeline,
            db: connection_info.db,
        };
        authenticate(connection_info, &mut con).await?;
        Ok((con, driver))
    }
}

impl ConnectionLike for MultiplexedConnection {
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

#[cfg(feature = "connection-manager")]
mod connection_manager {
    use super::*;

    use std::sync::Arc;

    use arc_swap::{self, ArcSwap};
    use futures::future::{self, Shared};
    use futures_util::future::BoxFuture;

    /// A `ConnectionManager` is a proxy that wraps a [multiplexed
    /// connection][multiplexed-connection] and automatically reconnects to the
    /// server when necessary.
    ///
    /// Like the [`MultiplexedConnection`][multiplexed-connection], this
    /// manager can be cloned, allowing requests to be be sent concurrently on
    /// the same underlying connection (tcp/unix socket).
    ///
    /// ## Behavior
    ///
    /// - When creating an instance of the `ConnectionManager`, an initial
    ///   connection will be established and awaited. Connection errors will be
    ///   returned directly.
    /// - When a command sent to the server fails with an error that represents
    ///   a "connection dropped" condition, that error will be passed on to the
    ///   user, but it will trigger a reconnection in the background.
    /// - The reconnect code will atomically swap the current (dead) connection
    ///   with a future that will eventually resolve to a `MultiplexedConnection`
    ///   or to a `RedisError`
    /// - All commands that are issued after the reconnect process has been
    ///   initiated, will have to await the connection future.
    /// - If reconnecting fails, all pending commands will be failed as well. A
    ///   new reconnection attempt will be triggered if the error is an I/O error.
    ///
    /// [multiplexed-connection]: struct.MultiplexedConnection.html
    #[derive(Clone)]
    pub struct ConnectionManager {
        /// Information used for the connection. This is needed to be able to reconnect.
        connection_info: ConnectionInfo,
        /// The connection future.
        ///
        /// The `ArcSwap` is required to be able to replace the connection
        /// without making the `ConnectionManager` mutable.
        connection: Arc<ArcSwap<SharedRedisFuture<MultiplexedConnection>>>,
    }

    /// A `RedisResult` that can be cloned because `RedisError` is behind an `Arc`.
    type CloneableRedisResult<T> = Result<T, Arc<RedisError>>;

    /// Type alias for a shared boxed future that will resolve to a `CloneableRedisResult`.
    type SharedRedisFuture<T> = Shared<BoxFuture<'static, CloneableRedisResult<T>>>;

    impl ConnectionManager {
        /// Connect to the server and store the connection inside the returned `ConnectionManager`.
        ///
        /// This requires the `connection-manager` feature, which will also pull in
        /// the Tokio executor.
        pub async fn new(connection_info: ConnectionInfo) -> RedisResult<Self> {
            // Create a MultiplexedConnection and wait for it to be established

            #[cfg(all(feature = "tokio-comp", not(feature = "async-std-comp")))]
            let con = connect_simple::<tokio_aio::Tokio>(&connection_info).await?;

            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
            let con = connect_simple::<aio_async_std::AsyncStd>(&connection_info).await?;

            #[cfg(all(feature = "tokio-comp", feature = "async-std-comp"))]
            let con = if tokio::runtime::Handle::try_current().is_ok() {
                connect_simple::<tokio_aio::Tokio>(&connection_info).await?
            } else {
                connect_simple::<aio_async_std::AsyncStd>(&connection_info).await?
            };

            #[cfg(all(not(feature = "tokio-comp"), not(feature = "async-std-comp")))]
            compile_error!("tokio-comp or async-std-comp features required for aio feature");

            let (connection, driver) =
                MultiplexedConnection::create_connection(&connection_info, con).await?;

            // Spawn the driver that drives the connection future
            tokio::spawn(driver);

            // Wrap the connection in an `ArcSwap` instance for fast atomic access
            Ok(Self {
                connection_info,
                connection: Arc::new(ArcSwap::from_pointee(
                    future::ok(connection).boxed().shared(),
                )),
            })
        }

        /// Reconnect and overwrite the old connection.
        ///
        /// The `current` guard points to the shared future that was active
        /// when the connection loss was detected.
        fn reconnect(
            &self,
            current: arc_swap::Guard<'_, Arc<SharedRedisFuture<MultiplexedConnection>>>,
        ) {
            let connection_info = self.connection_info.clone();
            let new_connection: SharedRedisFuture<MultiplexedConnection> = async move {
                #[cfg(all(feature = "tokio-comp", not(feature = "async-std-comp")))]
                let con = connect_simple::<tokio_aio::Tokio>(&connection_info).await?;

                #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
                let con = connect_simple::<aio_async_std::AsyncStd>(&connection_info).await?;

                #[cfg(all(feature = "tokio-comp", feature = "async-std-comp"))]
                let con = if tokio::runtime::Handle::try_current().is_ok() {
                    connect_simple::<tokio_aio::Tokio>(&connection_info).await?
                } else {
                    connect_simple::<aio_async_std::AsyncStd>(&connection_info).await?
                };

                #[cfg(all(not(feature = "tokio-comp"), not(feature = "async-std-comp")))]
                compile_error!("tokio-comp or async-std-comp features required for aio feature");

                let (new_connection, driver) =
                    MultiplexedConnection::create_connection(&connection_info, con).await?;

                tokio::spawn(driver);
                Ok(new_connection)
            }
            .boxed()
            .shared();

            // Update the connection in the connection manager
            let new_connection_arc = Arc::new(new_connection.clone());
            let prev = self
                .connection
                .compare_and_swap(&current, new_connection_arc);

            // If the swap happened...
            if Arc::ptr_eq(&prev, &current) {
                // ...start the connection attempt immediately but do not wait on it.
                tokio::spawn(new_connection);
            }
        }
    }

    /// Handle a command result. If the connection was dropped, reconnect.
    macro_rules! reconnect_if_dropped {
        ($self:expr, $result:expr, $current:expr) => {
            if let Err(ref e) = $result {
                if e.is_connection_dropped() {
                    $self.reconnect($current);
                }
            }
        };
    }

    /// Handle a connection result. If there's an I/O error, reconnect.
    /// Propagate any error.
    macro_rules! reconnect_if_io_error {
        ($self:expr, $result:expr, $current:expr) => {
            if let Err(e) = $result {
                if e.is_io_error() {
                    $self.reconnect($current);
                }
                return Err(e);
            }
        };
    }

    impl ConnectionLike for ConnectionManager {
        fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
            (async move {
                // Clone connection to avoid having to lock the ArcSwap in write mode
                let guard = self.connection.load();
                let connection_result = (**guard)
                    .clone()
                    .await
                    .map_err(|e| e.clone_mostly("Reconnecting failed"));
                reconnect_if_io_error!(self, connection_result, guard);
                let result = connection_result?.req_packed_command(cmd).await;
                reconnect_if_dropped!(self, &result, guard);
                result
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
                // Clone shared connection future to avoid having to lock the ArcSwap in write mode
                let guard = self.connection.load();
                let connection_result = (**guard)
                    .clone()
                    .await
                    .map_err(|e| e.clone_mostly("Reconnecting failed"));
                reconnect_if_io_error!(self, connection_result, guard);
                let result = connection_result?
                    .req_packed_commands(cmd, offset, count)
                    .await;
                reconnect_if_dropped!(self, &result, guard);
                result
            })
            .boxed()
        }

        fn get_db(&self) -> i64 {
            self.connection_info.db
        }
    }
}

#[cfg(feature = "connection-manager")]
pub use connection_manager::ConnectionManager;
