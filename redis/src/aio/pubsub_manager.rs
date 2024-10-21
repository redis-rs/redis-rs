use super::HandleContainer;
use crate::aio::Runtime;
use crate::subscription_tracker::{SubscriptionAction, SubscriptionTracker};
use crate::types::{closed_connection_error, RedisResult};
use crate::{
    from_owned_redis_value, Client, ConnectionInfo, FromRedisValue, Msg, RedisError, ToRedisArgs,
    Value,
};
use backon::{ExponentialBuilder, Retryable};
use futures::future::{join_all, select};
use futures_util::stream::{Stream, StreamExt};
use pin_project_lite::pin_project;
use std::pin::{pin, Pin};
use std::task::{self, Poll};
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

/// The sink part of a split async managed Pubsub.
///
/// The sink is used to subscribe and unsubscribe from
/// channels.
/// The stream part is independent from the sink,
/// and dropping the sink doesn't cause the stream part to
/// stop working, nor will the managed stream stop reconnecting
/// and resubscribing.
/// The sink isn't independent from the stream - dropping
/// the stream will cause the sink to return errors on requests.
pub struct PubSubManagerSink(UnboundedSender<SinkRequest>);

impl Clone for PubSubManagerSink {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pin_project! {
    /// The stream part of a split managed async Pubsub.
    ///
    /// The stream is used to receive messages from the server.
    /// The stream part is independent from the sink,
    /// and dropping the sink doesn't cause the stream part to
    /// stop working, nor will the managed stream stop reconnecting
    /// and resubscribing.
    /// The sink isn't independent from the stream - dropping
    /// the stream will cause the sink to return errors on requests.
    pub struct PubSubManagerStream{
        #[pin]
        stream: UnboundedReceiver<Msg>,
        // This handle ensures that once the stream will be dropped, the underlying task will stop.
        _task_handle: Option<HandleContainer>,
    }
}

/// The configuration for reconnect mechanism and request timing for the [PubSubManager]
#[derive(Clone, Debug)]
pub struct PubsubManagerConfig {
    /// A multiplicative factor that will be applied to the retry delay between reconnection attempts.
    ///
    /// For example, using a factor of `1000` will make each delay in units of seconds.
    factor: f32,
    /// Apply a maximum delay between reconnection attempts. The delay between attempts won't be longer than max_delay.
    max_delay: Option<Duration>,
    /// Each connection attempt to the server will time out after `connection_timeout`.
    connection_timeout: Option<std::time::Duration>,
}

impl PubsubManagerConfig {
    /// Creates a new instance of the options with nothing set
    pub fn new() -> Self {
        Self::default()
    }

    /// A multiplicative factor that will be applied to the retry delay between reconnection attempts.
    ///
    /// For example, using a factor of `1000` will make each delay in units of seconds.
    pub fn set_factor(mut self, factor: f32) -> Self {
        self.factor = factor;
        self
    }

    /// Apply a maximum delay between connection attempts. The delay between attempts won't be longer than the set duration.
    pub fn set_max_delay(mut self, time: Duration) -> Self {
        self.max_delay = Some(time);
        self
    }

    /// Each connection attempt to the server will time out after `connection_timeout`.
    pub fn set_connection_timeout(mut self, duration: std::time::Duration) -> Self {
        self.connection_timeout = Some(duration);
        self
    }
}

impl Default for PubsubManagerConfig {
    fn default() -> Self {
        Self {
            factor: crate::aio::connection_manager::ConnectionManagerConfig::DEFAULT_CONNECTION_RETRY_FACTOR as f32,
            max_delay: None,
            connection_timeout: crate::aio::connection_manager::ConnectionManagerConfig::DEFAULT_CONNECTION_TIMEOUT,
        }
    }
}

/// A managed connection dedicated to pubsub messages.
///
/// If the pubsub disconnects from the server, it will
/// automatically attempt to reconnect and resubscribe to
/// all channels.
///
/// If the user wants to be able to subscribe or ping the connection
/// while concurrently receiving messages, the `split` function allows
/// the user to split the pubsub to a sink and a stream.
///
/// IMPORTANT: Due to Redis' weak pubsub contract, messages
/// that are published before the manager is connected, or
/// during reconnect attempts, won't be received. The manager
/// doesn't notify on disconnects or reconnects, so the user
/// cannot be aware of missed messages. The only alternative
/// is to occasionally ping the manager / split sink, in order
/// to know whether it is still connected. For stronger guarantees,
/// Users of Redis 6 or above can use the RESP3 pubsub interface,
/// or the streams API.
pub struct PubSubManager {
    sink: PubSubManagerSink,
    stream: PubSubManagerStream,
}

impl PubSubManager {
    /// Constructs a new `PubSubManager`.
    pub async fn new(
        connection_info: &ConnectionInfo,
        config: PubsubManagerConfig,
    ) -> RedisResult<Self> {
        let (sink_sender, sink_receiver) = unbounded_channel();
        let (stream_sender, stream_receiver) = unbounded_channel();
        let (setup_complete_sender, setup_complete_receiver) = oneshot::channel();
        let client = Client::open(connection_info.clone())?;
        let task_handle = Runtime::locate().spawn(start_listening(
            sink_receiver,
            stream_sender,
            client,
            setup_complete_sender,
            config,
        ));
        setup_complete_receiver.await.map_err(|_| {
            RedisError::from((crate::ErrorKind::ClientError, "Failed to create pubsub"))
        })?;
        Ok(Self {
            stream: PubSubManagerStream {
                stream: stream_receiver,
                _task_handle: Some(HandleContainer::new(task_handle)),
            },
            sink: PubSubManagerSink(sink_sender),
        })
    }

    /// Subscribes to a new channel or channels.
    ///
    /// ```rust,no_run
    /// # async fn do_something() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/")?;
    /// let mut pubsub = client.get_async_pubsub_manager().await?;
    /// pubsub.subscribe(&["channel_1", "channel_2", "channel_3"]).await?;
    /// pubsub.subscribe("channel_4").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        self.sink.subscribe(channel_name).await
    }

    /// Unsubscribes from channel or channels.
    ///
    /// ```rust,no_run
    /// # async fn do_something() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/")?;
    /// let mut pubsub = client.get_async_pubsub_manager().await?;
    /// pubsub.subscribe(&["channel_1", "channel_2", "channel_3"]).await?;
    /// pubsub.unsubscribe(&["channel_1", "channel_2"]).await?;
    /// pubsub.unsubscribe("channel_3").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unsubscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        self.sink.unsubscribe(channel_name).await
    }

    /// Subscribes to a new channel with pattern or patterns.
    ///
    /// ```rust,no_run
    /// # async fn do_something() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/")?;
    /// let mut pubsub = client.get_async_pubsub_manager().await?;
    /// pubsub.psubscribe(&["channel_*", "cha**el"]).await?;
    /// pubsub.psubscribe("pat***").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn psubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        self.sink.psubscribe(channel_pattern).await
    }

    /// Unsubscribes from channel pattern or patterns.
    ///
    /// ```rust,no_run
    /// # async fn do_something() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/")?;
    /// let mut pubsub = client.get_async_pubsub_manager().await?;
    /// pubsub.psubscribe(&["channel_*", "cha**el", "cha**el_3"]).await?;
    /// pubsub.punsubscribe(&["channel_*", "cha**el"]).await?;
    /// pubsub.punsubscribe("cha**el_3").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn punsubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        self.sink.punsubscribe(channel_pattern).await
    }

    /// Sends a ping with a message to the server
    pub async fn ping_message<T: FromRedisValue>(
        &mut self,
        message: impl ToRedisArgs,
    ) -> RedisResult<T> {
        self.sink.ping_message(message).await
    }

    /// Sends a ping to the server
    pub async fn ping<T: FromRedisValue>(&mut self) -> RedisResult<T> {
        self.sink.ping().await
    }

    /// Returns [`Stream`] of [`Msg`]s from this [`crate::aio::PubSub`]s subscriptions.
    ///
    /// The message itself is still generic and can be converted into an appropriate type through
    /// the helper methods on it.
    pub fn on_message(&mut self) -> impl Stream<Item = Msg> + '_ {
        &mut self.stream
    }

    /// Returns [`Stream`] of [`Msg`]s from this [`crate::aio::PubSub`]'s subscriptions consuming it.
    ///
    /// The message itself is still generic and can be converted into an appropriate type through
    /// the helper methods on it.
    /// This can be useful in cases where the stream needs to be returned or held by something other
    /// than the [`crate::aio::PubSub`].
    pub fn into_on_message(self) -> PubSubManagerStream {
        self.stream
    }

    /// Splits the PubSub into separate sink and stream components, so that subscriptions could be
    /// updated through the `Sink` while concurrently waiting for new messages on the `Stream`.
    pub fn split(self) -> (PubSubManagerSink, PubSubManagerStream) {
        (self.sink, self.stream)
    }
}

async fn start_listening(
    mut sink_receiver: UnboundedReceiver<SinkRequest>,
    mut stream_sender: UnboundedSender<Msg>,
    client: Client,
    ready_sender: oneshot::Sender<()>,
    config: PubsubManagerConfig,
) {
    let mut ready_sender = Some(ready_sender);
    let mut subscription_tracker = SubscriptionTracker::default();

    let retry_strategy = ExponentialBuilder::default()
        .with_factor(config.factor)
        .with_max_delay(config.max_delay.unwrap_or(Duration::from_secs(60)))
        .with_jitter();
    loop {
        let connect_fn = || async {
            if let Some(timeout) = config.connection_timeout {
                Runtime::locate()
                    .timeout(timeout, client.get_async_pubsub())
                    .await
                    .map_err(|elapsed| elapsed.into())
                    .and_then(|res| res) // this just flattens the result
            } else {
                client.get_async_pubsub().await
            }
        };
        let Ok((sink, stream)) = connect_fn
            .retry(retry_strategy)
            .sleep(|duration| async move { Runtime::locate().sleep(duration).await })
            .await
            .map(|pubsub| pubsub.split())
        else {
            continue;
        };

        // report the completion of the first connection attempt
        if let Some(sender) = ready_sender.take() {
            let _ = sender.send(());
        } else {
            // resubscribe to restore state
            let pipeline = subscription_tracker.get_subscription_pipeline();
            let requests = pipeline.cmd_iter().map(|cmd| {
                let mut sink = sink.clone();
                let packed_cmd = cmd.get_packed_command();
                async move { sink.send_recv(packed_cmd).await }
            });
            // TODO - should we handle errors better than just failing? how?
            let res: RedisResult<Vec<_>> = join_all(requests).await.into_iter().collect();
            if res.is_err() {
                // (TODO) - if we integrate logging/tracing to non-cluster connections, we should log the failure here.
                continue;
            }
        }

        // this future runs until the sink is dropped, or has been disconnected.
        let sink_future = pin!(sink_handler(
            &mut sink_receiver,
            &mut subscription_tracker,
            sink
        ));

        // this future runs until the stream is dropped, or has been disconnected.
        let stream_future = pin!(stream_handler(&mut stream_sender, stream));
        match select(sink_future, stream_future).await {
            // dropping the sink will cause the sink future to stop, but the
            // stream future should be independent and keep running.
            futures::future::Either::Left((_, stream_future)) => stream_future.await,
            futures::future::Either::Right(_) => {}
        }
    }
}

async fn sink_handler(
    sink_receiver: &mut UnboundedReceiver<SinkRequest>,
    subscription_tracker: &mut SubscriptionTracker,
    mut sink: super::PubSubSink,
) {
    while let Some(request) = sink_receiver.recv().await {
        let response = match request.request_type {
            SinkRequestType::Subscribe => sink
                .subscribe(&request.arguments)
                .await
                .map(|_| Value::Okay),
            SinkRequestType::Unsubscribe => sink
                .unsubscribe(&request.arguments)
                .await
                .map(|_| Value::Okay),
            SinkRequestType::PSubscribe => sink
                .psubscribe(&request.arguments)
                .await
                .map(|_| Value::Okay),
            SinkRequestType::PUnsubscribe => sink
                .punsubscribe(&request.arguments)
                .await
                .map(|_| Value::Okay),
            SinkRequestType::Ping => sink.ping().await,
            SinkRequestType::PingMessage => sink.ping_message(&request.arguments).await,
        };
        let mut should_close = false;
        if let Err(err) = &response {
            should_close = err.is_unrecoverable_error();
        } else {
            let action = match request.request_type {
                SinkRequestType::Subscribe => Some(SubscriptionAction::Subscribe),
                SinkRequestType::Unsubscribe => Some(SubscriptionAction::Unsubscribe),
                SinkRequestType::PSubscribe => Some(SubscriptionAction::PSubscribe),
                SinkRequestType::PUnsubscribe => Some(SubscriptionAction::PUnsubscribe),
                SinkRequestType::Ping => None,
                SinkRequestType::PingMessage => None,
            };
            if let Some(action) = action {
                subscription_tracker.update_with_request(action, request.arguments.into_iter());
            }
        }
        let _ = request.response.send(response);
        if should_close {
            return;
        }
    }
}

async fn stream_handler(
    stream_sender: &mut tokio::sync::mpsc::UnboundedSender<Msg>,
    mut stream: super::PubSubStream,
) {
    while let Some(msg) = stream.next().await {
        if stream_sender.send(msg).is_err() {
            return;
        }
    }
}

enum SinkRequestType {
    Subscribe,
    Unsubscribe,
    PSubscribe,
    PUnsubscribe,
    Ping,
    PingMessage,
}

struct SinkRequest {
    request_type: SinkRequestType,
    arguments: Vec<Vec<u8>>,
    response: oneshot::Sender<RedisResult<Value>>,
}

impl PubSubManagerSink {
    async fn send_request(
        &mut self,
        request_type: SinkRequestType,
        arguments: Vec<Vec<u8>>,
    ) -> RedisResult<Value> {
        let (sender, receiver) = oneshot::channel();
        self.0
            .send(SinkRequest {
                request_type,
                arguments,
                response: sender,
            })
            .map_err(|_| closed_connection_error())?;
        receiver
            .await
            .unwrap_or_else(|_| Err(closed_connection_error()))
    }

    /// Subscribes to a new channel or channels.
    ///
    /// ```rust,no_run
    /// # async fn do_something() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/")?;
    /// let (mut sink, mut stream) = client.get_async_pubsub_manager().await?.split();
    /// sink.subscribe(&["channel_1", "channel_2", "channel_3"]).await?;
    /// sink.subscribe("channel_4").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        self.send_request(SinkRequestType::Subscribe, channel_name.to_redis_args())
            .await
            .map(|_| ())
    }

    /// Unsubscribes from channel or channels.
    ///
    /// ```rust,no_run
    /// # async fn do_something() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/")?;
    /// let (mut sink, mut stream) = client.get_async_pubsub_manager().await?.split();
    /// sink.subscribe(&["channel_1", "channel_2", "channel_3"]).await?;
    /// sink.unsubscribe(&["channel_1", "channel_2"]).await?;
    /// sink.unsubscribe("channel_3").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unsubscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        self.send_request(SinkRequestType::Unsubscribe, channel_name.to_redis_args())
            .await
            .map(|_| ())
    }

    /// Subscribes to a new channel with pattern or patterns.
    ///
    /// ```rust,no_run
    /// # async fn do_something() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/")?;
    /// let (mut sink, mut stream) = client.get_async_pubsub_manager().await?.split();
    /// sink.psubscribe(&["channel_*", "cha**el"]).await?;
    /// sink.psubscribe("pat***").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn psubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        self.send_request(SinkRequestType::PSubscribe, channel_pattern.to_redis_args())
            .await
            .map(|_| ())
    }

    /// Unsubscribes from channel pattern or patterns.
    ///
    /// ```rust,no_run
    /// # async fn do_something() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/")?;
    /// let (mut sink, mut stream) = client.get_async_pubsub_manager().await?.split();
    /// sink.psubscribe(&["channel_*", "cha**el", "cha**el_3"]).await?;
    /// sink.punsubscribe(&["channel_*", "cha**el"]).await?;
    /// sink.punsubscribe("cha**el_3").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn punsubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        self.send_request(
            SinkRequestType::PUnsubscribe,
            channel_pattern.to_redis_args(),
        )
        .await
        .map(|_| ())
    }

    /// Sends a ping with a message to the server
    pub async fn ping_message<T: FromRedisValue>(
        &mut self,
        message: impl ToRedisArgs,
    ) -> RedisResult<T> {
        let val = self
            .send_request(SinkRequestType::PingMessage, message.to_redis_args())
            .await?
            .extract_error()?;
        from_owned_redis_value(val)
    }

    /// Sends a ping to the server
    pub async fn ping<T: FromRedisValue>(&mut self) -> RedisResult<T> {
        let val = self
            .send_request(SinkRequestType::Ping, Vec::new())
            .await?
            .extract_error()?;
        from_owned_redis_value(val)
    }
}

impl Stream for PubSubManagerStream {
    type Item = Msg;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_recv(cx)
    }
}
