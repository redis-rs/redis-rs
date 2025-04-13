use super::{AsyncPushSender, ConnectionLike, Runtime, SharedHandleContainer, TaskHandle};
use crate::aio::{check_resp3, setup_connection};
#[cfg(feature = "cache-aio")]
use crate::caching::{CacheManager, CacheStatistics, PrepareCacheResult};
use crate::cmd::Cmd;
use crate::parser::ValueCodec;
use crate::types::{closed_connection_error, RedisError, RedisFuture, RedisResult, Value};
use crate::{
    cmd, AsyncConnectionConfig, ProtocolVersion, PushInfo, RedisConnectionInfo, ToRedisArgs,
};
use ::tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
};
use futures_util::{
    future::{Future, FutureExt},
    ready,
    sink::Sink,
    stream::{self, Stream, StreamExt},
};
use pin_project_lite::pin_project;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{self, Poll};
use std::time::Duration;
use tokio_util::codec::Decoder;

// Senders which the result of a single request are sent through
type PipelineOutput = oneshot::Sender<RedisResult<Value>>;

enum ResponseAggregate {
    SingleCommand,
    Pipeline {
        buffer: Vec<Value>,
        first_err: Option<RedisError>,
        expectation: PipelineResponseExpectation,
    },
}

// TODO - this is a really bad name.
struct PipelineResponseExpectation {
    // The number of responses to skip before starting to save responses in the buffer.
    skipped_response_count: usize,
    // The number of responses to keep in the buffer
    expected_response_count: usize,
    // whether the pipelined request is a transaction
    is_transaction: bool,
}

impl ResponseAggregate {
    fn new(expectation: Option<PipelineResponseExpectation>) -> Self {
        match expectation {
            Some(expectation) => ResponseAggregate::Pipeline {
                buffer: Vec::new(),
                first_err: None,
                expectation,
            },
            None => ResponseAggregate::SingleCommand,
        }
    }
}

struct InFlight {
    output: PipelineOutput,
    response_aggregate: ResponseAggregate,
}

// A single message sent through the pipeline
struct PipelineMessage {
    input: Vec<u8>,
    output: PipelineOutput,
    // If `None`, this is a single request, not a pipeline of multiple requests.
    // If `Some`, the first value is the number of responses to skip,
    // the second is the number of responses to keep, and the third is whether the pipeline is a transaction.
    expectation: Option<PipelineResponseExpectation>,
}

/// Wrapper around a `Stream + Sink` where each item sent through the `Sink` results in one or more
/// items being output by the `Stream` (the number is specified at time of sending). With the
/// interface provided by `Pipeline` an easy interface of request to response, hiding the `Stream`
/// and `Sink`.
#[derive(Clone)]
struct Pipeline {
    sender: mpsc::Sender<PipelineMessage>,
}

impl Debug for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Pipeline").field(&self.sender).finish()
    }
}

#[cfg(feature = "cache-aio")]
pin_project! {
    struct PipelineSink<T> {
        #[pin]
        sink_stream: T,
        in_flight: VecDeque<InFlight>,
        error: Option<RedisError>,
        push_sender: Option<Arc<dyn AsyncPushSender>>,
        cache_manager: Option<CacheManager>,
    }
}

#[cfg(not(feature = "cache-aio"))]
pin_project! {
    struct PipelineSink<T> {
        #[pin]
        sink_stream: T,
        in_flight: VecDeque<InFlight>,
        error: Option<RedisError>,
        push_sender: Option<Arc<dyn AsyncPushSender>>,
    }
}

fn send_push(push_sender: &Option<Arc<dyn AsyncPushSender>>, info: PushInfo) {
    if let Some(sender) = push_sender {
        let _ = sender.send(info);
    };
}

pub(crate) fn send_disconnect(push_sender: &Option<Arc<dyn AsyncPushSender>>) {
    send_push(push_sender, PushInfo::disconnect());
}

impl<T> PipelineSink<T>
where
    T: Stream<Item = RedisResult<Value>> + 'static,
{
    fn new(
        sink_stream: T,
        push_sender: Option<Arc<dyn AsyncPushSender>>,
        #[cfg(feature = "cache-aio")] cache_manager: Option<CacheManager>,
    ) -> Self
    where
        T: Sink<Vec<u8>, Error = RedisError> + Stream<Item = RedisResult<Value>> + 'static,
    {
        PipelineSink {
            sink_stream,
            in_flight: VecDeque::new(),
            error: None,
            push_sender,
            #[cfg(feature = "cache-aio")]
            cache_manager,
        }
    }

    // Read messages from the stream and send them back to the caller
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Result<(), ()>> {
        loop {
            let item = ready!(self.as_mut().project().sink_stream.poll_next(cx));
            let item = match item {
                Some(result) => result,
                // The redis response stream is not going to produce any more items so we simulate a disconnection error to break out of the loop.
                None => Err(closed_connection_error()),
            };

            let is_unrecoverable = item.as_ref().is_err_and(|err| err.is_unrecoverable_error());
            self.as_mut().send_result(item);
            if is_unrecoverable {
                let self_ = self.project();
                send_disconnect(self_.push_sender);
                return Poll::Ready(Err(()));
            }
        }
    }

    fn send_result(self: Pin<&mut Self>, result: RedisResult<Value>) {
        let self_ = self.project();
        let result = match result {
            // If this push message isn't a reply, we'll pass it as-is to the push manager and stop iterating
            Ok(Value::Push { kind, data }) if !kind.has_reply() => {
                #[cfg(feature = "cache-aio")]
                if let Some(cache_manager) = &self_.cache_manager {
                    cache_manager.handle_push_value(&kind, &data);
                }
                send_push(self_.push_sender, PushInfo { kind, data });

                return;
            }
            // If this push message is a reply to a query, we'll clone it to the push manager and continue with sending the reply
            Ok(Value::Push { kind, data }) if kind.has_reply() => {
                send_push(
                    self_.push_sender,
                    PushInfo {
                        kind: kind.clone(),
                        data: data.clone(),
                    },
                );
                Ok(Value::Push { kind, data })
            }
            _ => result,
        };

        let mut entry = match self_.in_flight.pop_front() {
            Some(entry) => entry,
            None => return,
        };

        match &mut entry.response_aggregate {
            ResponseAggregate::SingleCommand => {
                entry.output.send(result).ok();
            }
            ResponseAggregate::Pipeline {
                buffer,
                first_err,
                expectation:
                    PipelineResponseExpectation {
                        expected_response_count,
                        skipped_response_count,
                        is_transaction,
                    },
            } => {
                if *skipped_response_count > 0 {
                    // errors in skipped values are still counted for errors in transactions, since they're errors that will cause the transaction to fail,
                    // and we only skip values in transaction.
                    if first_err.is_none() && *is_transaction {
                        *first_err = result.and_then(Value::extract_error).err();
                    }

                    *skipped_response_count -= 1;
                    self_.in_flight.push_front(entry);
                    return;
                }

                match result {
                    Ok(item) => {
                        buffer.push(item);
                    }
                    Err(err) => {
                        if first_err.is_none() {
                            *first_err = Some(err);
                        }
                    }
                }

                if buffer.len() < *expected_response_count {
                    // Need to gather more response values
                    self_.in_flight.push_front(entry);
                    return;
                }

                let response = match first_err.take() {
                    Some(err) => Err(err),
                    None => Ok(Value::Array(std::mem::take(buffer))),
                };

                // `Err` means that the receiver was dropped in which case it does not
                // care about the output and we can continue by just dropping the value
                // and sender
                entry.output.send(response).ok();
            }
        }
    }
}

impl<T> Sink<PipelineMessage> for PipelineSink<T>
where
    T: Sink<Vec<u8>, Error = RedisError> + Stream<Item = RedisResult<Value>> + 'static,
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
            expectation,
        }: PipelineMessage,
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
                let response_aggregate = ResponseAggregate::new(expectation);
                let entry = InFlight {
                    output,
                    response_aggregate,
                };

                self_.in_flight.push_back(entry);
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

impl Pipeline {
    fn new<T>(
        sink_stream: T,
        push_sender: Option<Arc<dyn AsyncPushSender>>,
        #[cfg(feature = "cache-aio")] cache_manager: Option<CacheManager>,
    ) -> (Self, impl Future<Output = ()>)
    where
        T: Sink<Vec<u8>, Error = RedisError>,
        T: Stream<Item = RedisResult<Value>>,
        T: Unpin + Send + 'static,
    {
        const BUFFER_SIZE: usize = 50;
        let (sender, mut receiver) = mpsc::channel(BUFFER_SIZE);

        let sink = PipelineSink::new(
            sink_stream,
            push_sender,
            #[cfg(feature = "cache-aio")]
            cache_manager,
        );
        let f = stream::poll_fn(move |cx| receiver.poll_recv(cx))
            .map(Ok)
            .forward(sink)
            .map(|_| ());
        (Pipeline { sender }, f)
    }

    async fn send_recv(
        &mut self,
        input: Vec<u8>,
        // If `None`, this is a single request, not a pipeline of multiple requests.
        // If `Some`, the value inside defines how the response should look like
        expectation: Option<PipelineResponseExpectation>,
        timeout: Option<Duration>,
    ) -> Result<Value, RedisError> {
        let (sender, receiver) = oneshot::channel();

        let request = async {
            self.sender
                .send(PipelineMessage {
                    input,
                    expectation,
                    output: sender,
                })
                .await
                .map_err(|_| None)?;

            receiver.await
            // The `sender` was dropped which likely means that the stream part
            // failed for one reason or another
            .map_err(|_| None)
            .and_then(|res| res.map_err(Some))
        };

        match timeout {
            Some(timeout) => match Runtime::locate().timeout(timeout, request).await {
                Ok(res) => res,
                Err(elapsed) => Err(Some(elapsed.into())),
            },
            None => request.await,
        }
        .map_err(|err| err.unwrap_or_else(closed_connection_error))
    }
}

/// A connection object which can be cloned, allowing requests to be be sent concurrently
/// on the same underlying connection (tcp/unix socket).
///
/// This connection object is cancellation-safe, and the user can drop request future without polling them to completion,
/// but this doesn't mean that the actual request sent to the server is cancelled.
/// A side-effect of this is that the underlying connection won't be closed until all sent requests have been answered,
/// which means that in case of blocking commands, the underlying connection resource might not be released,
/// even when all clones of the multiplexed connection have been dropped (see <https://github.com/redis-rs/redis-rs/issues/1236>).
/// This isn't an issue in a connection that was created in a canonical way, which ensures that `_task_handle` is set, so that
/// once all of the connection's clones are dropped, the task will also be dropped. If the user creates the connection in
/// another way and `_task_handle` isn't set, they should manually spawn the returned driver function, keep the spawned task's
/// handle and abort the task whenever they want, at the risk of effectively closing the clones of the multiplexed connection.
#[derive(Clone)]
pub struct MultiplexedConnection {
    pipeline: Pipeline,
    db: i64,
    response_timeout: Option<Duration>,
    protocol: ProtocolVersion,
    // This handle ensures that once all the clones of the connection will be dropped, the underlying task will stop.
    // This handle is only set for connection whose task was spawned by the crate, not for users who spawned their own
    // task.
    _task_handle: Option<SharedHandleContainer>,
    #[cfg(feature = "cache-aio")]
    pub(crate) cache_manager: Option<CacheManager>,
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
    /// and a `RedisConnectionInfo`
    pub async fn new<C>(
        connection_info: &RedisConnectionInfo,
        stream: C,
    ) -> RedisResult<(Self, impl Future<Output = ()>)>
    where
        C: Unpin + AsyncRead + AsyncWrite + Send + 'static,
    {
        Self::new_with_response_timeout(connection_info, stream, None).await
    }

    /// Constructs a new `MultiplexedConnection` out of a `AsyncRead + AsyncWrite` object
    /// and a `RedisConnectionInfo`. The new object will wait on operations for the given `response_timeout`.
    pub async fn new_with_response_timeout<C>(
        connection_info: &RedisConnectionInfo,
        stream: C,
        response_timeout: Option<std::time::Duration>,
    ) -> RedisResult<(Self, impl Future<Output = ()>)>
    where
        C: Unpin + AsyncRead + AsyncWrite + Send + 'static,
    {
        Self::new_with_config(
            connection_info,
            stream,
            AsyncConnectionConfig {
                response_timeout,
                ..Default::default()
            },
        )
        .await
    }

    /// Constructs a new `MultiplexedConnection` out of a `AsyncRead + AsyncWrite` object
    /// , a `RedisConnectionInfo` and a `AsyncConnectionConfig`.
    pub async fn new_with_config<C>(
        connection_info: &RedisConnectionInfo,
        stream: C,
        config: AsyncConnectionConfig,
    ) -> RedisResult<(Self, impl Future<Output = ()>)>
    where
        C: Unpin + AsyncRead + AsyncWrite + Send + 'static,
    {
        let mut codec = ValueCodec::default().framed(stream);
        if config.push_sender.is_some() {
            check_resp3!(
                connection_info.protocol,
                "Can only pass push sender to a connection using RESP3"
            );
        }

        #[cfg(feature = "cache-aio")]
        let cache_config = config.cache.as_ref().map(|cache| match cache {
            crate::client::Cache::Config(cache_config) => *cache_config,
            #[cfg(feature = "connection-manager")]
            crate::client::Cache::Manager(cache_manager) => cache_manager.cache_config,
        });
        #[cfg(feature = "cache-aio")]
        let cache_manager_opt = config
            .cache
            .map(|cache| {
                check_resp3!(
                    connection_info.protocol,
                    "Can only enable client side caching in a connection using RESP3"
                );
                match cache {
                    crate::client::Cache::Config(cache_config) => {
                        Ok(CacheManager::new(cache_config))
                    }
                    #[cfg(feature = "connection-manager")]
                    crate::client::Cache::Manager(cache_manager) => Ok(cache_manager),
                }
            })
            .transpose()?;

        setup_connection(
            &mut codec,
            connection_info,
            #[cfg(feature = "cache-aio")]
            cache_config,
        )
        .await?;
        if config.push_sender.is_some() {
            check_resp3!(
                connection_info.protocol,
                "Can only pass push sender to a connection using RESP3"
            );
        }

        let (pipeline, driver) = Pipeline::new(
            codec,
            config.push_sender,
            #[cfg(feature = "cache-aio")]
            cache_manager_opt.clone(),
        );
        let con = MultiplexedConnection {
            pipeline,
            db: connection_info.db,
            response_timeout: config.response_timeout,
            protocol: connection_info.protocol,
            _task_handle: None,
            #[cfg(feature = "cache-aio")]
            cache_manager: cache_manager_opt,
        };

        Ok((con, driver))
    }

    /// This should be called strictly before the multiplexed connection is cloned - that is, before it is returned to the user.
    /// Otherwise some clones will be able to kill the backing task, while other clones are still alive.
    pub(crate) fn set_task_handle(&mut self, handle: TaskHandle) {
        self._task_handle = Some(SharedHandleContainer::new(handle));
    }

    /// Sets the time that the multiplexer will wait for responses on operations before failing.
    pub fn set_response_timeout(&mut self, timeout: std::time::Duration) {
        self.response_timeout = Some(timeout);
    }

    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    pub async fn send_packed_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        #[cfg(feature = "cache-aio")]
        if let Some(cache_manager) = &self.cache_manager {
            match cache_manager.get_cached_cmd(cmd) {
                PrepareCacheResult::Cached(value) => return Ok(value),
                PrepareCacheResult::NotCached(cacheable_command) => {
                    let mut pipeline = crate::Pipeline::new();
                    cacheable_command.pack_command(cache_manager, &mut pipeline);

                    let result = self
                        .pipeline
                        .send_recv(
                            pipeline.get_packed_pipeline(),
                            Some(PipelineResponseExpectation {
                                skipped_response_count: 0,
                                expected_response_count: pipeline.commands.len(),
                                is_transaction: false,
                            }),
                            self.response_timeout,
                        )
                        .await?;
                    let replies: Vec<Value> = crate::types::from_owned_redis_value(result)?;
                    return cacheable_command.resolve(cache_manager, replies.into_iter());
                }
                _ => (),
            }
        }
        self.pipeline
            .send_recv(cmd.get_packed_command(), None, self.response_timeout)
            .await
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
        #[cfg(feature = "cache-aio")]
        if let Some(cache_manager) = &self.cache_manager {
            let (cacheable_pipeline, pipeline, (skipped_response_count, expected_response_count)) =
                cache_manager.get_cached_pipeline(cmd);
            let result = self
                .pipeline
                .send_recv(
                    pipeline.get_packed_pipeline(),
                    Some(PipelineResponseExpectation {
                        skipped_response_count,
                        expected_response_count,
                        is_transaction: cacheable_pipeline.transaction_mode,
                    }),
                    self.response_timeout,
                )
                .await?;

            return cacheable_pipeline.resolve(cache_manager, result);
        }
        let value = self
            .pipeline
            .send_recv(
                cmd.get_packed_pipeline(),
                Some(PipelineResponseExpectation {
                    skipped_response_count: offset,
                    expected_response_count: count,
                    is_transaction: cmd.is_transaction(),
                }),
                self.response_timeout,
            )
            .await?;
        match value {
            Value::Array(values) => Ok(values),
            _ => Ok(vec![value]),
        }
    }

    /// Gets [`CacheStatistics`] for current connection if caching is enabled.
    #[cfg(feature = "cache-aio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cache-aio")))]
    pub fn get_cache_statistics(&self) -> Option<CacheStatistics> {
        self.cache_manager.as_ref().map(|cm| cm.statistics())
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
    /// Subscribes to a new channel(s).    
    ///
    /// Updates from the sender will be sent on the push sender that was passed to the connection.
    /// If the connection was configured without a push sender, the connection won't be able to pass messages back to the user.
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    ///
    /// ```rust,no_run
    /// # async fn func() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/?protocol=resp3").unwrap();
    /// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    /// let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
    /// let mut con = client.get_multiplexed_async_connection_with_config(&config).await?;
    /// con.subscribe(&["channel_1", "channel_2"]).await?;
    /// # Ok(()) }
    /// ```
    pub async fn subscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.protocol);
        let mut cmd = cmd("SUBSCRIBE");
        cmd.arg(channel_name);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Unsubscribes from channel(s).
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    ///
    /// ```rust,no_run
    /// # async fn func() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/?protocol=resp3").unwrap();
    /// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    /// let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
    /// let mut con = client.get_multiplexed_async_connection_with_config(&config).await?;
    /// con.subscribe(&["channel_1", "channel_2"]).await?;
    /// con.unsubscribe(&["channel_1", "channel_2"]).await?;
    /// # Ok(()) }
    /// ```
    pub async fn unsubscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.protocol);
        let mut cmd = cmd("UNSUBSCRIBE");
        cmd.arg(channel_name);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Subscribes to new channel(s) with pattern(s).
    ///
    /// Updates from the sender will be sent on the push sender that was passed to the connection.
    /// If the connection was configured without a push sender, the connection won't be able to pass messages back to the user.
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    ///
    /// ```rust,no_run
    /// # async fn func() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/?protocol=resp3").unwrap();
    /// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    /// let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
    /// let mut con = client.get_multiplexed_async_connection_with_config(&config).await?;
    /// con.subscribe(&["channel_1", "channel_2"]).await?;
    /// con.unsubscribe(&["channel_1", "channel_2"]).await?;
    /// # Ok(()) }
    /// ```
    pub async fn psubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.protocol);
        let mut cmd = cmd("PSUBSCRIBE");
        cmd.arg(channel_pattern);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Unsubscribes from channel pattern(s).
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    pub async fn punsubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.protocol);
        let mut cmd = cmd("PUNSUBSCRIBE");
        cmd.arg(channel_pattern);
        cmd.exec_async(self).await?;
        Ok(())
    }
}
