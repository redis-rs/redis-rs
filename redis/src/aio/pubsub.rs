use crate::aio::Runtime;
use crate::connection::{
    check_connection_setup, connection_setup_pipeline, AuthResult, ConnectionSetupComponents,
};
#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
use crate::parser::ValueCodec;
use crate::types::{closed_connection_error, RedisError, RedisResult, Value};
use crate::{cmd, Msg, RedisConnectionInfo, ToRedisArgs};
use ::tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::oneshot,
};
use futures_util::{
    future::{Future, FutureExt},
    ready,
    sink::{Sink, SinkExt},
    stream::{self, Stream, StreamExt},
};
use pin_project_lite::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{self, Poll};
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::UnboundedSender;
#[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
use tokio_util::codec::Decoder;

use super::SharedHandleContainer;

// A signal that a un/subscribe request has completed.
type RequestCompletedSignal = oneshot::Sender<RedisResult<()>>;

// A single message sent through the pipeline
struct PipelineMessage {
    input: Vec<u8>,
    output: RequestCompletedSignal,
}

/// A sender to an async task passing the messages to a `Stream + Sink`.
pub struct PubSubSink {
    sender: UnboundedSender<PipelineMessage>,
}

pin_project! {
    pub struct PubSubStream {
        #[pin]
        receiver: tokio::sync::mpsc::UnboundedReceiver<Msg>,
        // This handle ensures that once the stream will be dropped, the underlying task will stop.
        _task_handle: Option<SharedHandleContainer>,
    }
}

impl Clone for PubSubSink {
    fn clone(&self) -> Self {
        PubSubSink {
            sender: self.sender.clone(),
        }
    }
}

pin_project! {
    struct PipelineSink<T> {
        // The `Sink + Stream` that sends requests and receives values from the server.
        #[pin]
        sink_stream: T,
        // The requests that were sent and are awaiting a response.
        in_flight: VecDeque<RequestCompletedSignal>,
        // A sender for the push messages received from the server.
        sender: UnboundedSender<Msg>,
    }
}

impl<T> PipelineSink<T>
where
    T: Stream<Item = RedisResult<Value>> + 'static,
{
    fn new(sink_stream: T, sender: UnboundedSender<Msg>) -> Self
    where
        T: Sink<Vec<u8>, Error = RedisError> + Stream<Item = RedisResult<Value>> + 'static,
    {
        PipelineSink {
            sink_stream,
            in_flight: VecDeque::new(),
            sender,
        }
    }

    // Read messages from the stream and handle them.
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Result<(), ()>> {
        loop {
            let self_ = self.as_mut().project();
            if self_.sender.is_closed() {
                return Poll::Ready(Err(()));
            }

            let item = match ready!(self.as_mut().project().sink_stream.poll_next(cx)) {
                Some(result) => result,
                // The redis response stream is not going to produce any more items so we `Err`
                // to break out of the `forward` combinator and stop handling requests
                None => return Poll::Ready(Err(())),
            };
            self.as_mut().handle_message(item)?;
        }
    }

    fn handle_message(self: Pin<&mut Self>, result: RedisResult<Value>) -> Result<(), ()> {
        let self_ = self.project();

        match result {
            Ok(Value::Array(value)) => {
                if let Some(Value::BulkString(kind)) = value.first() {
                    if matches!(
                        kind.as_slice(),
                        b"subscribe" | b"psubscribe" | b"unsubscribe" | b"punsubscribe"
                    ) {
                        if let Some(entry) = self_.in_flight.pop_front() {
                            let _ = entry.send(Ok(()));
                        };
                        return Ok(());
                    }
                }

                if let Some(msg) = Msg::from_owned_value(Value::Array(value)) {
                    let _ = self_.sender.send(msg);
                    Ok(())
                } else {
                    Err(())
                }
            }

            Ok(Value::Push { kind, data }) => {
                if kind.has_reply() {
                    if let Some(entry) = self_.in_flight.pop_front() {
                        let _ = entry.send(Ok(()));
                    };
                    return Ok(());
                }

                if let Some(msg) = Msg::from_push_info(crate::PushInfo { kind, data }) {
                    let _ = self_.sender.send(msg);
                    Ok(())
                } else {
                    Err(())
                }
            }

            Err(err) if err.is_unrecoverable_error() => Err(()),

            _ => {
                if let Some(entry) = self_.in_flight.pop_front() {
                    let _ = entry.send(result.map(|_| ()));
                    Ok(())
                } else {
                    Err(())
                }
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
        self.as_mut()
            .project()
            .sink_stream
            .poll_ready(cx)
            .map_err(|_| ())
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        PipelineMessage { input, output }: PipelineMessage,
    ) -> Result<(), Self::Error> {
        let self_ = self.as_mut().project();

        match self_.sink_stream.start_send(input) {
            Ok(()) => {
                self_.in_flight.push_back(output);
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
                let _ = self.as_mut().handle_message(Err(err));
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

        if this.sender.is_closed() {
            return Poll::Ready(Ok(()));
        }

        match ready!(this.sink_stream.poll_next(cx)) {
            Some(result) => {
                let _ = self.handle_message(result);
                Poll::Pending
            }
            None => Poll::Ready(Ok(())),
        }
    }
}

impl PubSubSink {
    fn new<T>(
        sink_stream: T,
        messages_sender: UnboundedSender<Msg>,
    ) -> (Self, impl Future<Output = ()>)
    where
        T: Sink<Vec<u8>, Error = RedisError> + Stream<Item = RedisResult<Value>> + Send + 'static,
        T::Item: Send,
        T::Error: Send,
        T::Error: ::std::fmt::Debug,
    {
        let (sender, mut receiver) = unbounded_channel();
        let sink = PipelineSink::new(sink_stream, messages_sender);
        let f = stream::poll_fn(move |cx| {
            let res = receiver.poll_recv(cx);
            match res {
                // We don't want to stop the backing task for the stream, even if the sink was closed.
                Poll::Ready(None) => Poll::Pending,
                _ => res,
            }
        })
        .map(Ok)
        .forward(sink)
        .map(|_| ());
        (PubSubSink { sender }, f)
    }

    async fn send_recv(&mut self, input: Vec<u8>) -> Result<(), RedisError> {
        let (sender, receiver) = oneshot::channel();

        self.sender
            .send(PipelineMessage {
                input,
                output: sender,
            })
            .map_err(|_| closed_connection_error())?;
        match receiver.await {
            Ok(result) => result,
            Err(_) => Err(closed_connection_error()),
        }
    }

    /// Subscribes to a new channel.
    pub async fn subscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        let mut cmd = cmd("SUBSCRIBE");
        cmd.arg(channel_name);
        self.send_recv(cmd.get_packed_command()).await
    }

    /// Unsubscribes from channel.
    pub async fn unsubscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        let mut cmd = cmd("UNSUBSCRIBE");
        cmd.arg(channel_name);
        self.send_recv(cmd.get_packed_command()).await
    }

    /// Subscribes to a new channel with pattern.
    pub async fn psubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        let mut cmd = cmd("PSUBSCRIBE");
        cmd.arg(channel_pattern);
        self.send_recv(cmd.get_packed_command()).await
    }

    /// Unsubscribes from channel pattern.
    pub async fn punsubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        let mut cmd = cmd("PUNSUBSCRIBE");
        cmd.arg(channel_pattern);
        self.send_recv(cmd.get_packed_command()).await
    }
}

/// A connection dedicated to pubsub messages.
pub struct PubSub {
    sink: PubSubSink,
    stream: PubSubStream,
}

async fn execute_connection_pipeline<T>(
    codec: &mut T,
    (pipeline, instructions): (crate::Pipeline, ConnectionSetupComponents),
) -> RedisResult<AuthResult>
where
    T: Sink<Vec<u8>, Error = RedisError> + Stream<Item = RedisResult<Value>> + 'static,
    T: Send + 'static,
    T::Item: Send,
    T::Error: Send,
    T::Error: ::std::fmt::Debug,
    T: Unpin,
{
    let count = pipeline.len();
    if count == 0 {
        return Ok(AuthResult::Succeeded);
    }
    codec.send(pipeline.get_packed_pipeline()).await?;

    let mut results = Vec::with_capacity(count);
    for _ in 0..count {
        let value = codec.next().await;
        match value {
            Some(Ok(val)) => results.push(val),
            _ => return Err(closed_connection_error()),
        }
    }

    check_connection_setup(results, instructions)
}

async fn setup_connection<T>(
    codec: &mut T,
    connection_info: &RedisConnectionInfo,
) -> RedisResult<()>
where
    T: Sink<Vec<u8>, Error = RedisError> + Stream<Item = RedisResult<Value>> + 'static,
    T: Send + 'static,
    T::Item: Send,
    T::Error: Send,
    T::Error: ::std::fmt::Debug,
    T: Unpin,
{
    if execute_connection_pipeline(codec, connection_setup_pipeline(connection_info, true)).await?
        == AuthResult::ShouldRetryWithoutUsername
    {
        execute_connection_pipeline(codec, connection_setup_pipeline(connection_info, false))
            .await?;
    }

    Ok(())
}

impl PubSub {
    /// Constructs a new `MultiplexedConnection` out of a `AsyncRead + AsyncWrite` object
    /// and a `ConnectionInfo`
    pub async fn new<C>(connection_info: &RedisConnectionInfo, stream: C) -> RedisResult<Self>
    where
        C: Unpin + AsyncRead + AsyncWrite + Send + 'static,
    {
        #[cfg(all(not(feature = "tokio-comp"), not(feature = "async-std-comp")))]
        compile_error!("tokio-comp or async-std-comp features required for aio feature");

        let mut codec = ValueCodec::default().framed(stream);
        setup_connection(&mut codec, connection_info).await?;
        let (sender, receiver) = unbounded_channel();
        let (sink, driver) = PubSubSink::new(codec, sender);
        let handle = Runtime::locate().spawn(driver);
        let _task_handle = Some(SharedHandleContainer::new(handle));
        let stream = PubSubStream {
            receiver,
            _task_handle,
        };
        let con = PubSub { sink, stream };
        Ok(con)
    }

    /// Subscribes to a new channel.
    pub async fn subscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        self.sink.subscribe(channel_name).await
    }

    /// Unsubscribes from channel.
    pub async fn unsubscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        self.sink.unsubscribe(channel_name).await
    }

    /// Subscribes to a new channel with pattern.
    pub async fn psubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        self.sink.psubscribe(channel_pattern).await
    }

    /// Unsubscribes from channel pattern.
    pub async fn punsubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        self.sink.punsubscribe(channel_pattern).await
    }

    /// Returns [`Stream`] of [`Msg`]s from this [`PubSub`]s subscriptions.
    ///
    /// The message itself is still generic and can be converted into an appropriate type through
    /// the helper methods on it.
    pub fn on_message(&mut self) -> impl Stream<Item = Msg> + '_ {
        &mut self.stream
    }

    /// Returns [`Stream`] of [`Msg`]s from this [`PubSub`]s subscriptions consuming it.
    ///
    /// The message itself is still generic and can be converted into an appropriate type through
    /// the helper methods on it.
    /// This can be useful in cases where the stream needs to be returned or held by something other
    /// than the [`PubSub`].
    pub fn into_on_message(self) -> PubSubStream {
        self.stream
    }

    /// Splits the PubSub into separate sink and stream components, so that subscriptions could be
    /// updated through the `Sink` while concurrently waiting for new messages on the `Stream`.
    pub fn split(self) -> (PubSubSink, PubSubStream) {
        (self.sink, self.stream)
    }
}

impl Stream for PubSubStream {
    type Item = Msg;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().receiver.poll_recv(cx)
    }
}
