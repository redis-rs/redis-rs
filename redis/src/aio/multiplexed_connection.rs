use super::{setup_connection, ConnectionLike, Runtime};
#[cfg(feature = "cache")]
use crate::caching::{CacheConfig, CacheManager, CacheMode, CacheStatistics};
use crate::cmd::Cmd;
#[cfg(feature = "cache")]
use crate::cmd::{CommandCacheInformation, CommandCacheInformationByRef};
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
use pin_project::pin_project;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::io;
#[cfg(feature = "cache")]
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{self, Poll};
use std::time::Duration;
#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
use tokio_util::codec::Decoder;

// Senders which the result of a single request are sent through
type PipelineOutput = oneshot::Sender<RedisResult<Value>>;

enum ResponseAggregate {
    SingleCommand,
    Pipeline {
        expected_response_count: usize,
        current_response_count: usize,
        buffer: Vec<Value>,
        first_err: Option<RedisError>,
    },
}

impl ResponseAggregate {
    fn new(pipeline_response_count: Option<usize>) -> Self {
        match pipeline_response_count {
            Some(response_count) => ResponseAggregate::Pipeline {
                expected_response_count: response_count,
                current_response_count: 0,
                buffer: Vec::new(),
                first_err: None,
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
struct PipelineMessage<S> {
    input: S,
    output: PipelineOutput,
    // If `None`, this is a single request, not a pipeline of multiple requests.
    pipeline_response_count: Option<usize>,
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

#[pin_project]
struct PipelineSink<T> {
    #[pin]
    sink_stream: T,
    in_flight: VecDeque<InFlight>,
    error: Option<RedisError>,
    push_manager: Arc<ArcSwap<PushManager>>,
    #[cfg(feature = "cache")]
    cache_manager: CacheManager,
}

impl<T> PipelineSink<T>
where
    T: Stream<Item = RedisResult<Value>> + 'static,
{
    fn new<SinkItem>(
        sink_stream: T,
        push_manager: Arc<ArcSwap<PushManager>>,
        #[cfg(feature = "cache")] cache_manager: CacheManager,
    ) -> Self
    where
        T: Sink<SinkItem, Error = RedisError> + Stream<Item = RedisResult<Value>> + 'static,
    {
        PipelineSink {
            sink_stream,
            in_flight: VecDeque::new(),
            error: None,
            push_manager,
            #[cfg(feature = "cache")]
            cache_manager,
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
                #[cfg(feature = "cache")]
                if kind == &PushKind::Invalidate {
                    if let Some(Value::Array(redis_key)) = _data.first() {
                        if let Some(redis_key) = redis_key.first() {
                            if let Ok(redis_key) =
                                crate::FromRedisValue::from_redis_value(redis_key)
                            {
                                self_.cache_manager.invalidate(&redis_key)
                            }
                        }
                    }
                }
                self_.push_manager.load().try_send_raw(res);
                if !kind.has_reply() {
                    // If it's not true then push kind is converted to reply of a command
                    skip_value = true;
                }
            }
        }

        let mut entry = match self_.in_flight.pop_front() {
            Some(entry) => entry,
            None => return,
        };

        if skip_value {
            self_.in_flight.push_front(entry);
            return;
        }

        match &mut entry.response_aggregate {
            ResponseAggregate::SingleCommand => {
                entry.output.send(result).ok();
            }
            ResponseAggregate::Pipeline {
                expected_response_count,
                current_response_count,
                buffer,
                first_err,
            } => {
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

                *current_response_count += 1;
                if current_response_count < expected_response_count {
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
            pipeline_response_count,
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
                let response_aggregate = ResponseAggregate::new(pipeline_response_count);
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

impl<SinkItem> Pipeline<SinkItem>
where
    SinkItem: Send + 'static,
{
    fn new<T>(
        sink_stream: T,
        #[cfg(feature = "cache")] cache_manager: CacheManager,
    ) -> (Self, impl Future<Output = ()>)
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
        let sink = PipelineSink::new::<SinkItem>(
            sink_stream,
            push_manager.clone(),
            #[cfg(feature = "cache")]
            cache_manager,
        );
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

    async fn send_recv(
        &mut self,
        input: SinkItem,
        // If `None`, this is a single request, not a pipeline of multiple requests.
        pipeline_response_count: Option<usize>,
        timeout: Duration,
    ) -> Result<Value, Option<RedisError>> {
        let (sender, receiver) = oneshot::channel();

        self.sender
            .send(PipelineMessage {
                input,
                pipeline_response_count,
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
    #[cfg(feature = "cache")]
    pub(crate) cache_manager: CacheManager,
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
        #[cfg(feature = "cache")] cache_config: CacheConfig,
    ) -> RedisResult<(Self, impl Future<Output = ()>)>
    where
        C: Unpin + AsyncRead + AsyncWrite + Send + 'static,
    {
        Self::new_with_response_timeout(
            connection_info,
            stream,
            std::time::Duration::MAX,
            #[cfg(feature = "cache")]
            cache_config,
        )
        .await
    }

    /// Constructs a new `MultiplexedConnection` out of a `AsyncRead + AsyncWrite` object
    /// and a `ConnectionInfo`. The new object will wait on operations for the given `response_timeout`.
    pub async fn new_with_response_timeout<C>(
        connection_info: &ConnectionInfo,
        stream: C,
        response_timeout: std::time::Duration,
        #[cfg(feature = "cache")] cache_config: CacheConfig,
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
        #[cfg(feature = "cache")]
        let cache_config = if connection_info.redis.protocol == ProtocolVersion::RESP2 {
            CacheConfig::disabled()
        } else {
            cache_config
        };
        let redis_connection_info = &connection_info.redis;
        let codec = ValueCodec::default()
            .framed(stream)
            .and_then(|msg| async move { msg });
        #[cfg(feature = "cache")]
        let cache_manager = CacheManager::new(cache_config);
        let (mut pipeline, driver) = Pipeline::new(
            codec,
            #[cfg(feature = "cache")]
            cache_manager.clone(),
        );
        let driver = boxed(driver);
        let pm = PushManager::default();
        pipeline.set_push_manager(pm.clone()).await;
        let mut con = MultiplexedConnection {
            pipeline,
            db: connection_info.redis.db,
            response_timeout,
            push_manager: pm,
            protocol: redis_connection_info.protocol,
            #[cfg(feature = "cache")]
            cache_manager,
        };
        let driver = {
            let setup_conn = setup_connection(
                redis_connection_info,
                &mut con,
                #[cfg(feature = "cache")]
                cache_config,
            );
            futures_util::pin_mut!(setup_conn);

            match futures_util::future::select(setup_conn, driver).await {
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

    #[cfg(feature = "cache")]
    async fn handle_cached_mget<'a>(
        &mut self,
        cmd: &Cmd,
        ci: &CommandCacheInformationByRef<'a>,
    ) -> RedisResult<Value> {
        let mut response: Vec<Value> = vec![];
        let mut request = vec![];
        let mut senders = vec![];
        let mut cache_information_vec = vec![];
        let mut cmd_count = 0;

        let mut missing_key_indexes = vec![];

        if self.cache_manager.cache_config.mode == CacheMode::OptIn {
            let mut command = crate::cmd("CLIENT");
            command.arg("CACHING").arg("YES");
            command.write_packed_command(&mut request);
            cmd_count += 1;
        }
        let multi = crate::cmd("MULTI");
        multi.write_packed_command(&mut request);
        cmd_count += 1;

        let mut key_test_buffer = Vec::new();
        let mut tail_buf = Vec::new();
        let mut mget_cmd = cmd::cmd("MGET");
        for (i, x) in cmd.args_iter().skip(1).enumerate() {
            if let crate::cmd::Arg::Simple(redis_key) = x {
                key_test_buffer.clear();
                key_test_buffer.extend_from_slice(b"GET");
                key_test_buffer.extend_from_slice(redis_key);
                let new_ci = CommandCacheInformationByRef {
                    client_side_ttl: ci.client_side_ttl,
                    cmd: &key_test_buffer,
                    redis_key,
                    is_mget: false,
                };
                let sender = match self.cache_manager.get_with_guard_new(&new_ci).await {
                    Ok(Some(value)) => {
                        response.push(value);
                        continue;
                    }
                    Ok(None) => None,
                    Err(g) => Some(g),
                };
                mget_cmd.arg(redis_key);
                missing_key_indexes.push(i);
                cache_information_vec.push(CommandCacheInformation {
                    redis_key: redis_key.to_vec(),
                    cmd: key_test_buffer.clone(),
                    client_side_ttl: ci.client_side_ttl,
                });
                Cmd::pttl(redis_key).write_packed_command(&mut tail_buf);
                response.push(Value::Nil);
                senders.push(sender);
            }
        }
        if !missing_key_indexes.is_empty() {
            request.write_all(&tail_buf).expect("TODO: panic message");
            mget_cmd.write_packed_command(&mut request);
            crate::cmd("EXEC").write_packed_command(&mut request);
            cmd_count += 2;
            cmd_count += missing_key_indexes.len();
            let result = self
                .pipeline
                .send_recv(request, Some(cmd_count), self.response_timeout)
                .await
                .map_err(|err| {
                    err.unwrap_or_else(|| {
                        RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe))
                    })
                });
            let responses = result?;
            if let Value::Array(mut responses) = responses {
                if let Value::Array(mut value_list_f) = responses.pop().unwrap() {
                    if let Value::Array(mut value_list) = value_list_f.pop().unwrap() {
                        for missing_key_index in missing_key_indexes.iter().rev() {
                            let value = value_list.pop().unwrap();
                            let pttl = value_list_f.pop().unwrap();
                            let index = *missing_key_index;
                            if let Some(Some(sender)) = senders.get(index) {
                                let pttl: i64 = crate::FromRedisValue::from_redis_value(&pttl)?;
                                let ci = cache_information_vec.get_mut(index).unwrap();
                                if pttl >= 0 {
                                    ci.client_side_ttl = Some(Duration::from_millis(pttl as u64));
                                }
                                self.cache_manager
                                    .insert_with_guard(ci, sender, value.clone());
                            } else {
                                response[*missing_key_index] = value;
                            }
                        }
                    }
                }
            }
        }
        Ok(Value::Array(response))
    }
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    pub async fn send_packed_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        #[cfg(feature = "cache")]
        let cache_information = match self.cache_manager.cache_config.mode {
            CacheMode::All => cmd.compute_cache_information(),
            CacheMode::OptIn => {
                if cmd.has_opt_in_cache() {
                    cmd.compute_cache_information()
                } else {
                    None
                }
            }
            CacheMode::None => None,
        };
        #[cfg(feature = "cache")]
        let (cmd_bytes, cmd_count, notifier) = if let Some(ci) = &cache_information {
            if ci.is_mget {
                return self.handle_cached_mget(cmd, ci).await;
            }
            let notifier = match self.cache_manager.get_with_guard_new(ci).await {
                Ok(Some(value)) => return Ok(value),
                Ok(None) => None,
                Err(g) => Some(g),
            };

            let mut request = vec![];
            let mut cmd_count = 1;
            if self.cache_manager.cache_config.mode == CacheMode::OptIn {
                Cmd::new()
                    .arg("CLIENT")
                    .arg("CACHING")
                    .arg("YES")
                    .write_packed_command(&mut request);
                cmd_count += 1;
            }
            Cmd::new()
                .arg("PTTL")
                .arg(ci.redis_key)
                .write_packed_command(&mut request);
            cmd.write_packed_command(&mut request);
            cmd_count += 1;
            self.cache_manager.increase_sent_command_count(cmd_count);
            (request, Some(cmd_count), notifier)
        } else {
            (cmd.get_packed_command(), None, None)
        };
        #[cfg(not(feature = "cache"))]
        let (cmd_bytes, cmd_count) = (cmd.get_packed_command(), None);
        let result = self
            .pipeline
            .send_recv(cmd_bytes, cmd_count, self.response_timeout)
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
        #[cfg(feature = "cache")]
        if let Some(mut ci) = cache_information {
            if let Ok(Value::Array(mut v)) = result {
                let reply = v.pop().unwrap();
                let pttl: i64 = crate::FromRedisValue::from_redis_value(&v.pop().unwrap()).unwrap();
                if pttl >= 0 {
                    ci.client_side_ttl = Some(Duration::from_millis(pttl as u64));
                }
                if let Some(notifier) = notifier {
                    self.cache_manager
                        .insert_with_guard_by_ref(&ci, &notifier, reply.clone());
                }
                Ok(reply)
            } else {
                result
            }
        } else {
            result
        }
        #[cfg(not(feature = "cache"))]
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
                Some(offset + count),
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
    #[cfg(feature = "cache")]
    /// Gets `CacheStatistics` for this `MultiplexedConnection`.
    pub fn get_cache_statistics(&self) -> CacheStatistics {
        self.cache_manager.statistics()
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
