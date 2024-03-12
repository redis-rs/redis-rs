use super::{ConnectionLike, Runtime};
use crate::aio::setup_connection;
use crate::cmd::Cmd;
#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
use crate::parser::ValueCodec;
use crate::push_manager::PushManager;
use crate::types::{RedisError, RedisFuture, RedisResult, Value};
use crate::{cmd, ConnectionInfo, ProtocolVersion, PushKind};
use ::tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
};
use arc_swap::ArcSwap;
use futures_util::{
    future::{Future, FutureExt},
    ready,
    sink::Sink,
    stream::{self, Stream, StreamExt, TryStreamExt as _},
};
use pin_project_lite::pin_project;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{self, Poll};
use std::time::Duration;
#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
use tokio_util::codec::Decoder;

// Senders which the result of a single request are sent through
type PipelineOutput = oneshot::Sender<RedisResult<Value>>;

struct InFlight {
    output: PipelineOutput,
    expected_response_count: usize,
    current_response_count: usize,
    buffer: Option<Value>,
    first_err: Option<RedisError>,
}

impl InFlight {
    fn new(output: PipelineOutput, expected_response_count: usize) -> Self {
        Self {
            output,
            expected_response_count,
            current_response_count: 0,
            buffer: None,
            first_err: None,
        }
    }
}

// A single message sent through the pipeline
struct PipelineMessage<S> {
    input: S,
    output: PipelineOutput,
    response_count: usize,
}

/// Wrapper around a `Stream + Sink` where each item sent through the `Sink` results in one or more
/// items being output by the `Stream` (the number is specified at time of sending). With the
/// interface provided by `Pipeline` an easy interface of request to response, hiding the `Stream`
/// and `Sink`.
struct Pipeline<SinkItem> {
    sender: mpsc::Sender<PipelineMessage<SinkItem>>,

    push_manager: Arc<ArcSwap<PushManager>>,
}

impl<SinkItem> Clone for Pipeline<SinkItem> {
    fn clone(&self) -> Self {
        Pipeline {
            sender: self.sender.clone(),
            push_manager: self.push_manager.clone(),
        }
    }
}

impl<SinkItem> Debug for Pipeline<SinkItem>
where
    SinkItem: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Pipeline").field(&self.sender).finish()
    }
}

pin_project! {
    struct PipelineSink<T> {
        #[pin]
        sink_stream: T,
        in_flight: VecDeque<InFlight>,
        error: Option<RedisError>,
        push_manager: Arc<ArcSwap<PushManager>>,
    }
}

impl<T> PipelineSink<T>
where
    T: Stream<Item = RedisResult<Value>> + 'static,
{
    fn new<SinkItem>(sink_stream: T, push_manager: Arc<ArcSwap<PushManager>>) -> Self
    where
        T: Sink<SinkItem, Error = RedisError> + Stream<Item = RedisResult<Value>> + 'static,
    {
        PipelineSink {
            sink_stream,
            in_flight: VecDeque::new(),
            error: None,
            push_manager,
        }
    }

    // Read messages from the stream and send them back to the caller
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Result<(), ()>> {
        loop {
            let item = match ready!(self.as_mut().project().sink_stream.poll_next(cx)) {
                Some(result) => result,
                // The redis response stream is not going to produce any more items so we `Err`
                // to break out of the `forward` combinator and stop handling requests
                None => return Poll::Ready(Err(())),
            };
            self.as_mut().send_result(item);
        }
    }

    fn send_result(self: Pin<&mut Self>, result: RedisResult<Value>) {
        let self_ = self.project();
        let mut skip_value = false;
        if let Ok(res) = &result {
            if let Value::Push { kind, data: _data } = res {
                self_.push_manager.load().try_send_raw(res);
                if !kind.has_reply() {
                    // If it's not true then push kind is converted to reply of a command
                    skip_value = true;
                }
            }
        }
        {
            let entry = match self_.in_flight.front_mut() {
                Some(entry) => entry,
                None => return,
            };
            match result {
                Ok(item) => {
                    if !skip_value {
                        entry.buffer = Some(match entry.buffer.take() {
                            Some(Value::Array(mut values)) if entry.current_response_count > 1 => {
                                values.push(item);
                                Value::Array(values)
                            }
                            Some(value) => {
                                let mut vec = Vec::with_capacity(entry.expected_response_count);
                                vec.push(value);
                                vec.push(item);
                                Value::Array(vec)
                            }
                            None => item,
                        });
                    }
                }
                Err(err) => {
                    if entry.first_err.is_none() {
                        entry.first_err = Some(err);
                    }
                }
            }

            if !skip_value {
                entry.current_response_count += 1;
            }
            if entry.current_response_count < entry.expected_response_count {
                // Need to gather more response values
                return;
            }
        }

        let entry = self_.in_flight.pop_front().unwrap();
        let response = match entry.first_err {
            Some(err) => Err(err),
            None => Ok(entry.buffer.unwrap_or(Value::Array(vec![]))),
        };

        // `Err` means that the receiver was dropped in which case it does not
        // care about the output and we can continue by just dropping the value
        // and sender
        entry.output.send(response).ok();
    }
}

impl<SinkItem, T> Sink<PipelineMessage<SinkItem>> for PipelineSink<T>
where
    T: Sink<SinkItem, Error = RedisError> + Stream<Item = RedisResult<Value>> + 'static,
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
        }: PipelineMessage<SinkItem>,
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

impl<SinkItem> Pipeline<SinkItem>
where
    SinkItem: Send + 'static,
{
    fn new<T>(sink_stream: T) -> (Self, impl Future<Output = ()>)
    where
        T: Sink<SinkItem, Error = RedisError> + Stream<Item = RedisResult<Value>> + 'static,
        T: Send + 'static,
        T::Item: Send,
        T::Error: Send,
        T::Error: ::std::fmt::Debug,
    {
        const BUFFER_SIZE: usize = 50;
        let (sender, mut receiver) = mpsc::channel(BUFFER_SIZE);
        let push_manager: Arc<ArcSwap<PushManager>> =
            Arc::new(ArcSwap::new(Arc::new(PushManager::default())));
        let sink = PipelineSink::new::<SinkItem>(sink_stream, push_manager.clone());
        let f = stream::poll_fn(move |cx| receiver.poll_recv(cx))
            .map(Ok)
            .forward(sink)
            .map(|_| ());
        (
            Pipeline {
                sender,
                push_manager,
            },
            f,
        )
    }

    // `None` means that the stream was out of items causing that poll loop to shut down.
    async fn send_single(
        &mut self,
        item: SinkItem,
        timeout: Duration,
    ) -> Result<Value, Option<RedisError>> {
        self.send_recv(item, 1, timeout).await
    }

    async fn send_recv(
        &mut self,
        input: SinkItem,
        count: usize,
        timeout: Duration,
    ) -> Result<Value, Option<RedisError>> {
        let (sender, receiver) = oneshot::channel();

        self.sender
            .send(PipelineMessage {
                input,
                response_count: count,
                output: sender,
            })
            .await
            .map_err(|_| None)?;
        match Runtime::locate().timeout(timeout, receiver).await {
            Ok(Ok(result)) => result.map_err(Some),
            Ok(Err(_)) => {
                // The `sender` was dropped which likely means that the stream part
                // failed for one reason or another
                Err(None)
            }
            Err(elapsed) => Err(Some(elapsed.into())),
        }
    }

    /// Sets `PushManager` of Pipeline
    async fn set_push_manager(&mut self, push_manager: PushManager) {
        self.push_manager.store(Arc::new(push_manager));
    }
}

/// A connection object which can be cloned, allowing requests to be be sent concurrently
/// on the same underlying connection (tcp/unix socket).
#[derive(Clone)]
pub struct MultiplexedConnection {
    pipeline: Pipeline<Vec<u8>>,
    db: i64,
    response_timeout: Duration,
    protocol: ProtocolVersion,
    push_manager: PushManager,
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
        connection_info: &ConnectionInfo,
        stream: C,
    ) -> RedisResult<(Self, impl Future<Output = ()>)>
    where
        C: Unpin + AsyncRead + AsyncWrite + Send + 'static,
    {
        Self::new_with_response_timeout(connection_info, stream, std::time::Duration::MAX).await
    }

    /// Constructs a new `MultiplexedConnection` out of a `AsyncRead + AsyncWrite` object
    /// and a `ConnectionInfo`. The new object will wait on operations for the given `response_timeout`.
    pub async fn new_with_response_timeout<C>(
        connection_info: &ConnectionInfo,
        stream: C,
        response_timeout: std::time::Duration,
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

        let redis_connection_info = &connection_info.redis;
        let codec = ValueCodec::default()
            .framed(stream)
            .and_then(|msg| async move { msg });
        let (mut pipeline, driver) = Pipeline::new(codec);
        let driver = boxed(driver);
        let pm = PushManager::default();
        pipeline.set_push_manager(pm.clone()).await;
        let mut con = MultiplexedConnection {
            pipeline,
            db: connection_info.redis.db,
            response_timeout,
            push_manager: pm,
            protocol: redis_connection_info.protocol,
        };
        let driver = {
            let auth = setup_connection(&connection_info.redis, &mut con);

            futures_util::pin_mut!(auth);

            match futures_util::future::select(auth, driver).await {
                futures_util::future::Either::Left((result, driver)) => {
                    result?;
                    driver
                }
                futures_util::future::Either::Right(((), _)) => {
                    return Err(RedisError::from((
                        crate::ErrorKind::IoError,
                        "Multiplexed connection driver unexpectedly terminated",
                    )));
                }
            }
        };
        Ok((con, driver))
    }

    /// Sets the time that the multiplexer will wait for responses on operations before failing.
    pub fn set_response_timeout(&mut self, timeout: std::time::Duration) {
        self.response_timeout = timeout;
    }

    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    pub async fn send_packed_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        let result = self
            .pipeline
            .send_single(cmd.get_packed_command(), self.response_timeout)
            .await
            .map_err(|err| {
                err.unwrap_or_else(|| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
            });
        if self.protocol != ProtocolVersion::RESP2 {
            if let Err(e) = &result {
                if e.is_connection_dropped() {
                    // Notify the PushManager that the connection was lost
                    self.push_manager.try_send_raw(&Value::Push {
                        kind: PushKind::Disconnection,
                        data: vec![],
                    });
                }
            }
        }
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
        let result = self
            .pipeline
            .send_recv(
                cmd.get_packed_pipeline(),
                offset + count,
                self.response_timeout,
            )
            .await
            .map_err(|err| {
                err.unwrap_or_else(|| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
            });

        if self.protocol != ProtocolVersion::RESP2 {
            if let Err(e) = &result {
                if e.is_connection_dropped() {
                    // Notify the PushManager that the connection was lost
                    self.push_manager.try_send_raw(&Value::Push {
                        kind: PushKind::Disconnection,
                        data: vec![],
                    });
                }
            }
        }
        let value = result?;
        match value {
            Value::Array(mut values) => {
                values.drain(..offset);
                Ok(values)
            }
            _ => Ok(vec![value]),
        }
    }

    /// Sets `PushManager` of connection
    pub async fn set_push_manager(&mut self, push_manager: PushManager) {
        self.push_manager = push_manager.clone();
        self.pipeline.set_push_manager(push_manager).await;
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
impl MultiplexedConnection {
    /// Subscribes to a new channel.
    pub async fn subscribe(&mut self, channel_name: String) -> RedisResult<()> {
        if self.protocol == ProtocolVersion::RESP2 {
            return Err(RedisError::from((
                crate::ErrorKind::InvalidClientConfig,
                "RESP3 is required for this command",
            )));
        }
        let mut cmd = cmd("SUBSCRIBE");
        cmd.arg(channel_name.clone());
        cmd.query_async(self).await?;
        Ok(())
    }

    /// Unsubscribes from channel.
    pub async fn unsubscribe(&mut self, channel_name: String) -> RedisResult<()> {
        if self.protocol == ProtocolVersion::RESP2 {
            return Err(RedisError::from((
                crate::ErrorKind::InvalidClientConfig,
                "RESP3 is required for this command",
            )));
        }
        let mut cmd = cmd("UNSUBSCRIBE");
        cmd.arg(channel_name);
        cmd.query_async(self).await?;
        Ok(())
    }

    /// Subscribes to a new channel with pattern.
    pub async fn psubscribe(&mut self, channel_pattern: String) -> RedisResult<()> {
        if self.protocol == ProtocolVersion::RESP2 {
            return Err(RedisError::from((
                crate::ErrorKind::InvalidClientConfig,
                "RESP3 is required for this command",
            )));
        }
        let mut cmd = cmd("PSUBSCRIBE");
        cmd.arg(channel_pattern.clone());
        cmd.query_async(self).await?;
        Ok(())
    }

    /// Unsubscribes from channel pattern.
    pub async fn punsubscribe(&mut self, channel_pattern: String) -> RedisResult<()> {
        if self.protocol == ProtocolVersion::RESP2 {
            return Err(RedisError::from((
                crate::ErrorKind::InvalidClientConfig,
                "RESP3 is required for this command",
            )));
        }
        let mut cmd = cmd("PUNSUBSCRIBE");
        cmd.arg(channel_pattern);
        cmd.query_async(self).await?;
        Ok(())
    }

    /// Returns `PushManager` of Connection, this method is used to subscribe/unsubscribe from Push types
    pub fn get_push_manager(&self) -> PushManager {
        self.push_manager.clone()
    }
}
