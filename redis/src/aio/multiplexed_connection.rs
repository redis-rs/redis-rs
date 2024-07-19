use super::{ConnectionLike, Runtime};
use crate::aio::{check_resp3, setup_connection};
use crate::cmd::Cmd;
#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
use crate::parser::ValueCodec;
use crate::types::{AsyncPushSender, RedisError, RedisFuture, RedisResult, Value};
use crate::{cmd, AsyncConnectionConfig, ConnectionInfo, ProtocolVersion, PushInfo, ToRedisArgs};
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
use std::io;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::Duration;
#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
use tokio_util::codec::Decoder;

// Senders which the result of a single request are sent through
type PipelineOutput = oneshot::Sender<RedisResult<Value>>;

enum ResponseAggregate {
    SingleCommand,
    Pipeline {
        // The number of responses to skip before starting to save responses in the buffer.
        skipped_response_count: usize,
        // The number of responses to keep in the buffer
        expected_response_count: usize,
        buffer: Vec<Value>,
        first_err: Option<RedisError>,
    },
}

impl ResponseAggregate {
    fn new(pipeline_response_counts: Option<(usize, usize)>) -> Self {
        match pipeline_response_counts {
            Some((skipped_response_count, expected_response_count)) => {
                ResponseAggregate::Pipeline {
                    expected_response_count,
                    skipped_response_count,
                    buffer: Vec::new(),
                    first_err: None,
                }
            }
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
    // If `Some`, the first value is the number of responses to skip, and the second is the number of responses to keep.
    pipeline_response_counts: Option<(usize, usize)>,
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

pin_project! {
    struct PipelineSink<T> {
        #[pin]
        sink_stream: T,
        in_flight: VecDeque<InFlight>,
        error: Option<RedisError>,
        push_sender: Option<AsyncPushSender>,
    }
}

fn send_push(push_sender: &Option<AsyncPushSender>, info: PushInfo) {
    match push_sender {
        Some(sender) => {
            let _ = sender.send(info);
        }
        None => {}
    };
}

pub(crate) fn send_disconnect(push_sender: &Option<AsyncPushSender>) {
    send_push(
        push_sender,
        PushInfo {
            kind: crate::PushKind::Disconnection,
            data: vec![],
        },
    );
}

impl<T> PipelineSink<T>
where
    T: Stream<Item = RedisResult<Value>> + 'static,
{
    fn new(sink_stream: T, push_sender: Option<AsyncPushSender>) -> Self
    where
        T: Sink<Vec<u8>, Error = RedisError> + Stream<Item = RedisResult<Value>> + 'static,
    {
        PipelineSink {
            sink_stream,
            in_flight: VecDeque::new(),
            error: None,
            push_sender,
        }
    }

    // Read messages from the stream and send them back to the caller
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Result<(), ()>> {
        loop {
            let item = ready!(self.as_mut().project().sink_stream.poll_next(cx));
            let item = match item {
                Some(result) => {
                    if let Err(err) = &result {
                        if err.is_unrecoverable_error() {
                            let self_ = self.as_mut().project();
                            send_disconnect(self_.push_sender);
                        }
                    }
                    result
                }
                // The redis response stream is not going to produce any more items so we `Err`
                // to break out of the `forward` combinator and stop handling requests
                None => {
                    let self_ = self.project();
                    send_disconnect(self_.push_sender);
                    return Poll::Ready(Err(()));
                }
            };
            self.as_mut().send_result(item);
        }
    }

    fn send_result(self: Pin<&mut Self>, result: RedisResult<Value>) {
        let self_ = self.project();
        let result = match result {
            // If this push message isn't a reply, we'll pass it as-is to the push manager and stop iterating
            Ok(Value::Push { kind, data }) if !kind.has_reply() => {
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
                expected_response_count,
                skipped_response_count,
                buffer,
                first_err,
            } => {
                if *skipped_response_count > 0 {
                    // errors in skipped values are still counted for errors, since they're errors that will cause the transaction to fail,
                    // and we only skip values in transaction.
                    // TODO - the unified pipeline/transaction flows make this confusing. consider splitting them.
                    if first_err.is_none() {
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
            pipeline_response_counts,
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
                let response_aggregate = ResponseAggregate::new(pipeline_response_counts);
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
        push_sender: Option<AsyncPushSender>,
    ) -> (Self, impl Future<Output = ()>)
    where
        T: Sink<Vec<u8>, Error = RedisError> + Stream<Item = RedisResult<Value>> + 'static,
        T: Send + 'static,
        T::Item: Send,
        T::Error: Send,
        T::Error: ::std::fmt::Debug,
    {
        const BUFFER_SIZE: usize = 50;
        let (sender, mut receiver) = mpsc::channel(BUFFER_SIZE);

        let sink = PipelineSink::new(sink_stream, push_sender);
        let f = stream::poll_fn(move |cx| receiver.poll_recv(cx))
            .map(Ok)
            .forward(sink)
            .map(|_| ());
        (Pipeline { sender }, f)
    }

    // `None` means that the stream was out of items causing that poll loop to shut down.
    async fn send_single(
        &mut self,
        item: Vec<u8>,
        timeout: Option<Duration>,
    ) -> Result<Value, Option<RedisError>> {
        self.send_recv(item, None, timeout).await
    }

    async fn send_recv(
        &mut self,
        input: Vec<u8>,
        // If `None`, this is a single request, not a pipeline of multiple requests.
        // If `Some`, the first value is the number of responses to skip, and the second is the number of responses to keep.
        pipeline_response_counts: Option<(usize, usize)>,
        timeout: Option<Duration>,
    ) -> Result<Value, Option<RedisError>> {
        let (sender, receiver) = oneshot::channel();

        self.sender
            .send(PipelineMessage {
                input,
                pipeline_response_counts,
                output: sender,
            })
            .await
            .map_err(|_| None)?;

        match timeout {
            Some(timeout) => match Runtime::locate().timeout(timeout, receiver).await {
                Ok(res) => res,
                Err(elapsed) => Ok(Err(elapsed.into())),
            },
            None => receiver.await,
        }
        // The `sender` was dropped which likely means that the stream part
        // failed for one reason or another
        .map_err(|_| None)
        .and_then(|res| res.map_err(Some))
    }
}

/// A connection object which can be cloned, allowing requests to be be sent concurrently
/// on the same underlying connection (tcp/unix socket).
/// This connection object is cancellation-safe, and the user can drop request future without polling them to completion,
/// but this doesn't mean that the actual request sent to the server is cancelled.
/// A side-effect of this is that the underlying connection won't be closed until all sent requests have been answered,
/// which means that in case of blocking commands, the underlying connection resource might not be released,
/// even when all clones of the multiplexed connection have been dropped (see <https://github.com/redis-rs/redis-rs/issues/1236>).
/// If that is an issue, the user can, instead of using [crate::Client::get_multiplexed_async_connection], use either [MultiplexedConnection::new] or
/// [crate::Client::create_multiplexed_tokio_connection]/[crate::Client::create_multiplexed_async_std_connection],
/// manually spawn the returned driver function, keep the spawned task's handle and abort the task whenever they want,
/// at the cost of effectively closing the clones of the multiplexed connection.
#[derive(Clone)]
pub struct MultiplexedConnection {
    pipeline: Pipeline,
    db: i64,
    response_timeout: Option<Duration>,
    protocol: ProtocolVersion,
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
        Self::new_with_response_timeout(connection_info, stream, None).await
    }

    /// Constructs a new `MultiplexedConnection` out of a `AsyncRead + AsyncWrite` object
    /// and a `ConnectionInfo`. The new object will wait on operations for the given `response_timeout`.
    pub async fn new_with_response_timeout<C>(
        connection_info: &ConnectionInfo,
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
                connection_timeout: None,
                push_sender: None,
            },
        )
        .await
    }

    pub(crate) async fn new_with_config<C>(
        connection_info: &ConnectionInfo,
        stream: C,
        config: AsyncConnectionConfig,
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
        let codec = ValueCodec::default().framed(stream);
        if config.push_sender.is_some() {
            check_resp3!(
                redis_connection_info.protocol,
                "Can only pass push sender to a connection using RESP3"
            );
        }
        let (pipeline, driver) = Pipeline::new(codec, config.push_sender);
        let driver = boxed(driver);
        let mut con = MultiplexedConnection {
            pipeline,
            db: connection_info.redis.db,
            response_timeout: config.response_timeout,
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
        self.response_timeout = Some(timeout);
    }

    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    pub async fn send_packed_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        self.pipeline
            .send_single(cmd.get_packed_command(), self.response_timeout)
            .await
            .map_err(|err| {
                err.unwrap_or_else(|| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
            })
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
                Some((offset, count)),
                self.response_timeout,
            )
            .await
            .map_err(|err| {
                err.unwrap_or_else(|| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
            });

        let value = result?;
        match value {
            Value::Array(values) => Ok(values),
            _ => Ok(vec![value]),
        }
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
    pub async fn subscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.protocol);
        let mut cmd = cmd("SUBSCRIBE");
        cmd.arg(channel_name);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Unsubscribes from channel.
    pub async fn unsubscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.protocol);
        let mut cmd = cmd("UNSUBSCRIBE");
        cmd.arg(channel_name);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Subscribes to a new channel with pattern.
    pub async fn psubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.protocol);
        let mut cmd = cmd("PSUBSCRIBE");
        cmd.arg(channel_pattern);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Unsubscribes from channel pattern.
    pub async fn punsubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.protocol);
        let mut cmd = cmd("PUNSUBSCRIBE");
        cmd.arg(channel_pattern);
        cmd.exec_async(self).await?;
        Ok(())
    }
}
