//! Adds experimental async IO support to redis.
use async_trait::async_trait;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::Path;
use std::pin::Pin;
use std::task::{self, Poll};

use combine::{parser::combinator::AnySendSyncPartialState, stream::PointerOffset};

#[cfg(feature = "tokio-comp")]
use ::tokio::net::lookup_host;
use ::tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot},
};

#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
use tokio_util::codec::Decoder;

use futures_util::{
    future::{Future, FutureExt},
    ready,
    sink::Sink,
    stream::{self, Stream, StreamExt, TryStreamExt as _},
};

use pin_project_lite::pin_project;

use crate::cmd::{cmd, Cmd};
use crate::connection::{ConnectionAddr, ConnectionInfo, Msg, RedisConnectionInfo};

#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
use crate::parser::ValueCodec;
use crate::types::{ErrorKind, FromRedisValue, RedisError, RedisFuture, RedisResult, Value};
use crate::{from_redis_value, ToRedisArgs};

/// Enables the async_std compatibility
#[cfg(feature = "async-std-comp")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std-comp")))]
pub mod async_std;

#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use ::async_std::net::ToSocketAddrs;

/// Enables the tokio compatibility
#[cfg(feature = "tokio-comp")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio-comp")))]
pub mod tokio;

/// Represents the ability of connecting via TCP or via Unix socket
#[async_trait]
pub(crate) trait RedisRuntime: AsyncStream + Send + Sync + Sized + 'static {
    /// Performs a TCP connection
    async fn connect_tcp(socket_addr: SocketAddr) -> RedisResult<Self>;

    // Performs a TCP TLS connection
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    async fn connect_tcp_tls(
        hostname: &str,
        socket_addr: SocketAddr,
        insecure: bool,
    ) -> RedisResult<Self>;

    /// Performs a UNIX connection
    #[cfg(unix)]
    async fn connect_unix(path: &Path) -> RedisResult<Self>;

    fn spawn(f: impl Future<Output = ()> + Send + 'static);

    fn boxed(self) -> Pin<Box<dyn AsyncStream + Send + Sync>> {
        Box::pin(self)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Runtime {
    #[cfg(feature = "tokio-comp")]
    Tokio,
    #[cfg(feature = "async-std-comp")]
    AsyncStd,
}

impl Runtime {
    pub(crate) fn locate() -> Self {
        #[cfg(all(feature = "tokio-comp", not(feature = "async-std-comp")))]
        {
            Runtime::Tokio
        }

        #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
        {
            Runtime::AsyncStd
        }

        #[cfg(all(feature = "tokio-comp", feature = "async-std-comp"))]
        {
            if ::tokio::runtime::Handle::try_current().is_ok() {
                Runtime::Tokio
            } else {
                Runtime::AsyncStd
            }
        }

        #[cfg(all(not(feature = "tokio-comp"), not(feature = "async-std-comp")))]
        {
            compile_error!("tokio-comp or async-std-comp features required for aio feature")
        }
    }

    #[allow(dead_code)]
    fn spawn(&self, f: impl Future<Output = ()> + Send + 'static) {
        match self {
            #[cfg(feature = "tokio-comp")]
            Runtime::Tokio => tokio::Tokio::spawn(f),
            #[cfg(feature = "async-std-comp")]
            Runtime::AsyncStd => async_std::AsyncStd::spawn(f),
        }
    }
}

/// Trait for objects that implements `AsyncRead` and `AsyncWrite`
pub trait AsyncStream: AsyncRead + AsyncWrite {}
impl<S> AsyncStream for S where S: AsyncRead + AsyncWrite {}

/// Represents a `PubSub` connection.
pub struct PubSub<C = Pin<Box<dyn AsyncStream + Send + Sync>>>(Connection<C>);

/// Represents a `Monitor` connection.
pub struct Monitor<C = Pin<Box<dyn AsyncStream + Send + Sync>>>(Connection<C>);

impl<C> PubSub<C>
where
    C: Unpin + AsyncRead + AsyncWrite + Send,
{
    fn new(con: Connection<C>) -> Self {
        Self(con)
    }

    /// Subscribes to a new channel.
    pub async fn subscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        cmd("SUBSCRIBE").arg(channel).query_async(&mut self.0).await
    }

    /// Subscribes to a new channel with a pattern.
    pub async fn psubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        cmd("PSUBSCRIBE")
            .arg(pchannel)
            .query_async(&mut self.0)
            .await
    }

    /// Unsubscribes from a channel.
    pub async fn unsubscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        cmd("UNSUBSCRIBE")
            .arg(channel)
            .query_async(&mut self.0)
            .await
    }

    /// Unsubscribes from a channel with a pattern.
    pub async fn punsubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        cmd("PUNSUBSCRIBE")
            .arg(pchannel)
            .query_async(&mut self.0)
            .await
    }

    /// Returns [`Stream`] of [`Msg`]s from this [`PubSub`]s subscriptions.
    ///
    /// The message itself is still generic and can be converted into an appropriate type through
    /// the helper methods on it.
    pub fn on_message(&mut self) -> impl Stream<Item = Msg> + '_ {
        ValueCodec::default()
            .framed(&mut self.0.con)
            .filter_map(|msg| Box::pin(async move { Msg::from_value(&msg.ok()?.ok()?) }))
    }

    /// Returns [`Stream`] of [`Msg`]s from this [`PubSub`]s subscriptions consuming it.
    ///
    /// The message itself is still generic and can be converted into an appropriate type through
    /// the helper methods on it.
    /// This can be useful in cases where the stream needs to be returned or held by something other
    /// than the [`PubSub`].
    pub fn into_on_message(self) -> impl Stream<Item = Msg> {
        ValueCodec::default()
            .framed(self.0.con)
            .filter_map(|msg| Box::pin(async move { Msg::from_value(&msg.ok()?.ok()?) }))
    }

    /// Exits from `PubSub` mode and converts [`PubSub`] into [`Connection`].
    pub async fn into_connection(mut self) -> Connection<C> {
        self.0.exit_pubsub().await.ok();

        self.0
    }
}

impl<C> Monitor<C>
where
    C: Unpin + AsyncRead + AsyncWrite + Send,
{
    /// Create a [`Monitor`] from a [`Connection`]
    pub fn new(con: Connection<C>) -> Self {
        Self(con)
    }

    /// Deliver the MONITOR command to this [`Monitor`]ing wrapper.
    pub async fn monitor(&mut self) -> RedisResult<()> {
        cmd("MONITOR").query_async(&mut self.0).await
    }

    /// Returns [`Stream`] of [`FromRedisValue`] values from this [`Monitor`]ing connection
    pub fn on_message<T: FromRedisValue>(&mut self) -> impl Stream<Item = T> + '_ {
        ValueCodec::default()
            .framed(&mut self.0.con)
            .filter_map(|value| {
                Box::pin(async move { T::from_redis_value(&value.ok()?.ok()?).ok() })
            })
    }

    /// Returns [`Stream`] of [`FromRedisValue`] values from this [`Monitor`]ing connection
    pub fn into_on_message<T: FromRedisValue>(self) -> impl Stream<Item = T> {
        ValueCodec::default()
            .framed(self.0.con)
            .filter_map(|value| {
                Box::pin(async move { T::from_redis_value(&value.ok()?.ok()?).ok() })
            })
    }
}

/// Represents a stateful redis TCP connection.
pub struct Connection<C = Pin<Box<dyn AsyncStream + Send + Sync>>> {
    con: C,
    buf: Vec<u8>,
    decoder: combine::stream::Decoder<AnySendSyncPartialState, PointerOffset<[u8]>>,
    db: i64,

    // Flag indicating whether the connection was left in the PubSub state after dropping `PubSub`.
    //
    // This flag is checked when attempting to send a command, and if it's raised, we attempt to
    // exit the pubsub state before executing the new request.
    pubsub: bool,
}

fn assert_sync<T: Sync>() {}

#[allow(unused)]
fn test() {
    assert_sync::<Connection>();
}

impl<C> Connection<C> {
    pub(crate) fn map<D>(self, f: impl FnOnce(C) -> D) -> Connection<D> {
        let Self {
            con,
            buf,
            decoder,
            db,
            pubsub,
        } = self;
        Connection {
            con: f(con),
            buf,
            decoder,
            db,
            pubsub,
        }
    }
}

impl<C> Connection<C>
where
    C: Unpin + AsyncRead + AsyncWrite + Send,
{
    /// Constructs a new `Connection` out of a `AsyncRead + AsyncWrite` object
    /// and a `RedisConnectionInfo`
    pub async fn new(connection_info: &RedisConnectionInfo, con: C) -> RedisResult<Self> {
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

    /// Converts this [`Connection`] into [`PubSub`].
    pub fn into_pubsub(self) -> PubSub<C> {
        PubSub::new(self)
    }

    /// Converts this [`Connection`] into [`Monitor`]
    pub fn into_monitor(self) -> Monitor<C> {
        Monitor::new(self)
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

#[cfg(feature = "async-std-comp")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std-comp")))]
impl<C> Connection<async_std::AsyncStdWrapped<C>>
where
    C: Unpin + ::async_std::io::Read + ::async_std::io::Write + Send,
{
    /// Constructs a new `Connection` out of a `async_std::io::AsyncRead + async_std::io::AsyncWrite` object
    /// and a `RedisConnectionInfo`
    pub async fn new_async_std(connection_info: &RedisConnectionInfo, con: C) -> RedisResult<Self> {
        Connection::new(connection_info, async_std::AsyncStdWrapped::new(con)).await
    }
}

pub(crate) async fn connect<C>(connection_info: &ConnectionInfo) -> RedisResult<Connection<C>>
where
    C: Unpin + RedisRuntime + AsyncRead + AsyncWrite + Send,
{
    let con = connect_simple::<C>(connection_info).await?;
    Connection::new(&connection_info.redis, con).await
}

async fn authenticate<C>(connection_info: &RedisConnectionInfo, con: &mut C) -> RedisResult<()>
where
    C: ConnectionLike,
{
    if let Some(password) = &connection_info.password {
        let mut command = cmd("AUTH");
        if let Some(username) = &connection_info.username {
            command.arg(username);
        }
        match command.arg(password).query_async(con).await {
            Ok(Value::Okay) => (),
            Err(e) => {
                let err_msg = e.detail().ok_or((
                    ErrorKind::AuthenticationFailed,
                    "Password authentication failed",
                ))?;

                if !err_msg.contains("wrong number of arguments for 'auth' command") {
                    fail!((
                        ErrorKind::AuthenticationFailed,
                        "Password authentication failed",
                    ));
                }

                let mut command = cmd("AUTH");
                match command.arg(password).query_async(con).await {
                    Ok(Value::Okay) => (),
                    _ => {
                        fail!((
                            ErrorKind::AuthenticationFailed,
                            "Password authentication failed"
                        ));
                    }
                }
            }
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

pub(crate) async fn connect_simple<T: RedisRuntime>(
    connection_info: &ConnectionInfo,
) -> RedisResult<T> {
    Ok(match connection_info.addr {
        ConnectionAddr::Tcp(ref host, port) => {
            let socket_addr = get_socket_addrs(host, port).await?;
            <T>::connect_tcp(socket_addr).await?
        }

        #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
        ConnectionAddr::TcpTls {
            ref host,
            port,
            insecure,
        } => {
            let socket_addr = get_socket_addrs(host, port).await?;
            <T>::connect_tcp_tls(host, socket_addr, insecure).await?
        }

        #[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]
        ConnectionAddr::TcpTls { .. } => {
            fail!((
                ErrorKind::InvalidClientConfig,
                "Cannot connect to TCP with TLS without the tls feature"
            ));
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

async fn get_socket_addrs(host: &str, port: u16) -> RedisResult<SocketAddr> {
    #[cfg(feature = "tokio-comp")]
    let mut socket_addrs = lookup_host((host, port)).await?;
    #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
    let mut socket_addrs = (host, port).to_socket_addrs().await?;
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

impl<C> ConnectionLike for Connection<C>
where
    C: Unpin + AsyncRead + AsyncWrite + Send,
{
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

            let mut first_err = None;

            for _ in 0..offset {
                let response = self.read_response().await;
                if let Err(err) = response {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }

            let mut rv = Vec::with_capacity(count);
            for _ in 0..count {
                let response = self.read_response().await;
                match response {
                    Ok(item) => {
                        rv.push(item);
                    }
                    Err(err) => {
                        if first_err.is_none() {
                            first_err = Some(err);
                        }
                    }
                }
            }

            if let Some(err) = first_err {
                Err(err)
            } else {
                Ok(rv)
            }
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
    expected_response_count: usize,
    current_response_count: usize,
    buffer: Vec<O>,
    first_err: Option<E>,
}

impl<O, E> InFlight<O, E> {
    fn new(output: PipelineOutput<O, E>, expected_response_count: usize) -> Self {
        Self {
            output,
            expected_response_count,
            current_response_count: 0,
            buffer: Vec::new(),
            first_err: None,
        }
    }
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

impl<SinkItem, I, E> Debug for Pipeline<SinkItem, I, E>
where
    SinkItem: Debug,
    I: Debug,
    E: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Pipeline").field(&self.0).finish()
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
            // No need to try reading a message if there is no message in flight
            if self.in_flight.is_empty() {
                return Poll::Ready(Ok(()));
            }
            let item = match ready!(self.as_mut().project().sink_stream.poll_next(cx)) {
                Some(result) => result,
                // The redis response stream is not going to produce any more items so we `Err`
                // to break out of the `forward` combinator and stop handling requests
                None => return Poll::Ready(Err(())),
            };
            self.as_mut().send_result(item);
        }
    }

    fn send_result(self: Pin<&mut Self>, result: Result<I, E>) {
        let self_ = self.project();

        {
            let entry = match self_.in_flight.front_mut() {
                Some(entry) => entry,
                None => return,
            };

            match result {
                Ok(item) => {
                    entry.buffer.push(item);
                }
                Err(err) => {
                    if entry.first_err.is_none() {
                        entry.first_err = Some(err);
                    }
                }
            }

            entry.current_response_count += 1;
            if entry.current_response_count < entry.expected_response_count {
                // Need to gather more response values
                return;
            }
        }

        let entry = self_.in_flight.pop_front().unwrap();
        let response = match entry.first_err {
            Some(err) => Err(err),
            None => Ok(entry.buffer),
        };

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
        // If there is nothing to receive our output we do not need to send the message as it is
        // ambiguous whether the message will be sent anyway. Helps shed some load on the
        // connection.
        if output.is_closed() {
            return Ok(());
        }

        let self_ = self.as_mut().project();

        if let Some(err) = self_.error.take() {
            let _ = output.send(Err(err));
            return Err(());
        }

        match self_.sink_stream.start_send(input) {
            Ok(()) => {
                self_
                    .in_flight
                    .push_back(InFlight::new(output, response_count));
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
        let (sender, mut receiver) = mpsc::channel(BUFFER_SIZE);
        let f = stream::poll_fn(move |cx| receiver.poll_recv(cx))
            .map(Ok)
            .forward(PipelineSink::new::<SinkItem>(sink_stream))
            .map(|_| ());
        (Pipeline(sender), f)
    }

    // `None` means that the stream was out of items causing that poll loop to shut down.
    async fn send(&mut self, item: SinkItem) -> Result<I, Option<E>> {
        self.send_recv_multiple(item, 1)
            .await
            // We can unwrap since we do a request for `1` item
            .map(|mut item| item.pop().unwrap())
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
            .await
            .map_err(|_| None)?;
        match receiver.await {
            Ok(result) => result.map_err(Some),
            Err(_) => {
                // The `sender` was dropped which likely means that the stream part
                // failed for one reason or another
                Err(None)
            }
        }
    }
}

/// A connection object which can be cloned, allowing requests to be be sent concurrently
/// on the same underlying connection (tcp/unix socket).
#[derive(Clone)]
pub struct MultiplexedConnection {
    pipeline: Pipeline<Vec<u8>, Value, RedisError>,
    db: i64,
}

impl Debug for MultiplexedConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiplexedConnection")
            .field("pipeline", &self.pipeline)
            .field("db", &self.db)
            .finish()
    }
}

impl MultiplexedConnection {
    /// Constructs a new `MultiplexedConnection` out of a `AsyncRead + AsyncWrite` object
    /// and a `ConnectionInfo`
    pub async fn new<C>(
        connection_info: &RedisConnectionInfo,
        stream: C,
    ) -> RedisResult<(Self, impl Future<Output = ()>)>
    where
        C: Unpin + AsyncRead + AsyncWrite + Send + 'static,
    {
        fn boxed(
            f: impl Future<Output = ()> + Send + 'static,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(f)
        }

        #[cfg(all(not(feature = "tokio-comp"), not(feature = "async-std-comp")))]
        compile_error!("tokio-comp or async-std-comp features required for aio feature");

        let codec = ValueCodec::default()
            .framed(stream)
            .and_then(|msg| async move { msg });
        let (pipeline, driver) = Pipeline::new(codec);
        let driver = boxed(driver);
        let mut con = MultiplexedConnection {
            pipeline,
            db: connection_info.db,
        };
        let driver = {
            let auth = authenticate(connection_info, &mut con);
            futures_util::pin_mut!(auth);

            match futures_util::future::select(auth, driver).await {
                futures_util::future::Either::Left((result, driver)) => {
                    result?;
                    driver
                }
                futures_util::future::Either::Right(((), _)) => {
                    unreachable!("Multiplexed connection driver unexpectedly terminated")
                }
            }
        };
        Ok((con, driver))
    }

    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    pub async fn send_packed_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        let value = self
            .pipeline
            .send(cmd.get_packed_command())
            .await
            .map_err(|err| {
                err.unwrap_or_else(|| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
            })?;
        Ok(value)
    }

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    pub async fn send_packed_commands(
        &mut self,
        cmd: &crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        let mut value = self
            .pipeline
            .send_recv_multiple(cmd.get_packed_pipeline(), offset + count)
            .await
            .map_err(|err| {
                err.unwrap_or_else(|| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
            })?;

        value.drain(..offset);
        Ok(value)
    }
}

impl ConnectionLike for MultiplexedConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        (async move { self.send_packed_command(cmd).await }).boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        (async move { self.send_packed_commands(cmd, offset, count).await }).boxed()
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

    use crate::Client;

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
        client: Client,
        /// The connection future.
        ///
        /// The `ArcSwap` is required to be able to replace the connection
        /// without making the `ConnectionManager` mutable.
        connection: Arc<ArcSwap<SharedRedisFuture<MultiplexedConnection>>>,

        runtime: Runtime,
    }

    /// A `RedisResult` that can be cloned because `RedisError` is behind an `Arc`.
    type CloneableRedisResult<T> = Result<T, Arc<RedisError>>;

    /// Type alias for a shared boxed future that will resolve to a `CloneableRedisResult`.
    type SharedRedisFuture<T> = Shared<BoxFuture<'static, CloneableRedisResult<T>>>;

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

    impl ConnectionManager {
        /// Connect to the server and store the connection inside the returned `ConnectionManager`.
        ///
        /// This requires the `connection-manager` feature, which will also pull in
        /// the Tokio executor.
        pub async fn new(client: Client) -> RedisResult<Self> {
            // Create a MultiplexedConnection and wait for it to be established

            let runtime = Runtime::locate();
            let connection = client.get_multiplexed_async_connection().await?;

            // Wrap the connection in an `ArcSwap` instance for fast atomic access
            Ok(Self {
                client,
                connection: Arc::new(ArcSwap::from_pointee(
                    future::ok(connection).boxed().shared(),
                )),
                runtime,
            })
        }

        /// Reconnect and overwrite the old connection.
        ///
        /// The `current` guard points to the shared future that was active
        /// when the connection loss was detected.
        fn reconnect(
            &self,
            current: arc_swap::Guard<Arc<SharedRedisFuture<MultiplexedConnection>>>,
        ) {
            let client = self.client.clone();
            let new_connection: SharedRedisFuture<MultiplexedConnection> =
                async move { Ok(client.get_multiplexed_async_connection().await?) }
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
                self.runtime.spawn(new_connection.map(|_| ()));
            }
        }

        /// Sends an already encoded (packed) command into the TCP socket and
        /// reads the single response from it.
        pub async fn send_packed_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
            // Clone connection to avoid having to lock the ArcSwap in write mode
            let guard = self.connection.load();
            let connection_result = (**guard)
                .clone()
                .await
                .map_err(|e| e.clone_mostly("Reconnecting failed"));
            reconnect_if_io_error!(self, connection_result, guard);
            let result = connection_result?.send_packed_command(cmd).await;
            reconnect_if_dropped!(self, &result, guard);
            result
        }

        /// Sends multiple already encoded (packed) command into the TCP socket
        /// and reads `count` responses from it.  This is used to implement
        /// pipelining.
        pub async fn send_packed_commands(
            &mut self,
            cmd: &crate::Pipeline,
            offset: usize,
            count: usize,
        ) -> RedisResult<Vec<Value>> {
            // Clone shared connection future to avoid having to lock the ArcSwap in write mode
            let guard = self.connection.load();
            let connection_result = (**guard)
                .clone()
                .await
                .map_err(|e| e.clone_mostly("Reconnecting failed"));
            reconnect_if_io_error!(self, connection_result, guard);
            let result = connection_result?
                .send_packed_commands(cmd, offset, count)
                .await;
            reconnect_if_dropped!(self, &result, guard);
            result
        }
    }

    impl ConnectionLike for ConnectionManager {
        fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
            (async move { self.send_packed_command(cmd).await }).boxed()
        }

        fn req_packed_commands<'a>(
            &'a mut self,
            cmd: &'a crate::Pipeline,
            offset: usize,
            count: usize,
        ) -> RedisFuture<'a, Vec<Value>> {
            (async move { self.send_packed_commands(cmd, offset, count).await }).boxed()
        }

        fn get_db(&self) -> i64 {
            self.client.connection_info().redis.db
        }
    }
}

#[cfg(feature = "connection-manager")]
#[cfg_attr(docsrs, doc(cfg(feature = "connection-manager")))]
pub use connection_manager::ConnectionManager;
