use super::{AsyncPushSender, HandleContainer, RedisFuture};
#[cfg(feature = "cache-aio")]
use crate::caching::CacheManager;
use crate::{
    aio::{check_resp3, ConnectionLike, MultiplexedConnection, Runtime},
    cmd,
    subscription_tracker::{SubscriptionAction, SubscriptionTracker},
    types::{RedisError, RedisResult, Value},
    AsyncConnectionConfig, Client, Cmd, Pipeline, ProtocolVersion, PushInfo, PushKind, ToRedisArgs,
};
use arc_swap::ArcSwap;
use backon::{ExponentialBuilder, Retryable};
use futures_channel::oneshot;
use futures_util::future::{self, BoxFuture, FutureExt, Shared};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::Mutex;

type OptionalPushSender = Option<Arc<dyn AsyncPushSender>>;

/// The configuration for reconnect mechanism and request timing for the [ConnectionManager]
#[derive(Clone)]
pub struct ConnectionManagerConfig {
    /// The resulting duration is calculated by taking the base to the `n`-th power,
    /// where `n` denotes the number of past attempts.
    exponent_base: u64,
    /// A multiplicative factor that will be applied to the retry delay.
    ///
    /// For example, using a factor of `1000` will make each delay in units of seconds.
    factor: u64,
    /// number_of_retries times, with an exponentially increasing delay
    number_of_retries: usize,
    /// Apply a maximum delay between connection attempts. The delay between attempts won't be longer than max_delay milliseconds.
    max_delay: Option<u64>,
    /// The new connection will time out operations after `response_timeout` has passed.
    response_timeout: Option<std::time::Duration>,
    /// Each connection attempt to the server will time out after `connection_timeout`.
    connection_timeout: Option<std::time::Duration>,
    /// sender channel for push values
    push_sender: Option<Arc<dyn AsyncPushSender>>,
    /// if true, the manager should resubscribe automatically to all pubsub channels after reconnect.
    resubscribe_automatically: bool,
    tcp_settings: crate::io::tcp::TcpSettings,
    #[cfg(feature = "cache-aio")]
    pub(crate) cache_config: Option<crate::caching::CacheConfig>,
}

impl std::fmt::Debug for ConnectionManagerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let &Self {
            exponent_base,
            factor,
            number_of_retries,
            max_delay,
            response_timeout,
            connection_timeout,
            push_sender,
            resubscribe_automatically,
            tcp_settings,
            #[cfg(feature = "cache-aio")]
            cache_config,
        } = &self;
        let mut str = f.debug_struct("ConnectionManagerConfig");
        str.field("exponent_base", &exponent_base)
            .field("factor", &factor)
            .field("number_of_retries", &number_of_retries)
            .field("max_delay", &max_delay)
            .field("response_timeout", &response_timeout)
            .field("connection_timeout", &connection_timeout)
            .field("resubscribe_automatically", &resubscribe_automatically)
            .field(
                "push_sender",
                if push_sender.is_some() {
                    &"set"
                } else {
                    &"not set"
                },
            )
            .field("tcp_settings", &tcp_settings);

        #[cfg(feature = "cache-aio")]
        str.field("cache_config", &cache_config);

        str.finish()
    }
}

impl ConnectionManagerConfig {
    const DEFAULT_CONNECTION_RETRY_EXPONENT_BASE: u64 = 2;
    const DEFAULT_CONNECTION_RETRY_FACTOR: u64 = 100;
    const DEFAULT_NUMBER_OF_CONNECTION_RETRIES: usize = 6;
    const DEFAULT_RESPONSE_TIMEOUT: Option<std::time::Duration> = None;
    const DEFAULT_CONNECTION_TIMEOUT: Option<std::time::Duration> = None;

    /// Creates a new instance of the options with nothing set
    pub fn new() -> Self {
        Self::default()
    }

    /// A multiplicative factor that will be applied to the retry delay.
    ///
    /// For example, using a factor of `1000` will make each delay in units of seconds.
    pub fn set_factor(mut self, factor: u64) -> ConnectionManagerConfig {
        self.factor = factor;
        self
    }

    /// Apply a maximum delay between connection attempts. The delay between attempts won't be longer than max_delay milliseconds.
    pub fn set_max_delay(mut self, time: u64) -> ConnectionManagerConfig {
        self.max_delay = Some(time);
        self
    }

    /// The resulting duration is calculated by taking the base to the `n`-th power,
    /// where `n` denotes the number of past attempts.
    pub fn set_exponent_base(mut self, base: u64) -> ConnectionManagerConfig {
        self.exponent_base = base;
        self
    }

    /// number_of_retries times, with an exponentially increasing delay
    pub fn set_number_of_retries(mut self, amount: usize) -> ConnectionManagerConfig {
        self.number_of_retries = amount;
        self
    }

    /// The new connection will time out operations after `response_timeout` has passed.
    pub fn set_response_timeout(
        mut self,
        duration: std::time::Duration,
    ) -> ConnectionManagerConfig {
        self.response_timeout = Some(duration);
        self
    }

    /// Each connection attempt to the server will time out after `connection_timeout`.
    pub fn set_connection_timeout(
        mut self,
        duration: std::time::Duration,
    ) -> ConnectionManagerConfig {
        self.connection_timeout = Some(duration);
        self
    }

    /// Sets sender sender for push values.
    ///
    /// The sender can be a channel, or an arbitrary function that handles [crate::PushInfo] values.
    /// This will fail client creation if the connection isn't configured for RESP3 communications via the [crate::RedisConnectionInfo::protocol] field.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use redis::aio::ConnectionManagerConfig;
    /// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    /// let config = ConnectionManagerConfig::new().set_push_sender(tx);
    /// ```
    ///
    /// ```rust
    /// # use std::sync::{Mutex, Arc};
    /// # use redis::aio::ConnectionManagerConfig;
    /// let messages = Arc::new(Mutex::new(Vec::new()));
    /// let config = ConnectionManagerConfig::new().set_push_sender(move |msg|{
    ///     let Ok(mut messages) = messages.lock() else {
    ///         return Err(redis::aio::SendError);
    ///     };
    ///     messages.push(msg);
    ///     Ok(())
    /// });
    /// ```
    pub fn set_push_sender(mut self, sender: impl AsyncPushSender) -> Self {
        self.push_sender = Some(Arc::new(sender));
        self
    }

    /// Configures the connection manager to automatically resubscribe to all pubsub channels after reconnecting.
    pub fn set_automatic_resubscription(mut self) -> Self {
        self.resubscribe_automatically = true;
        self
    }

    /// Set the behavior of the underlying TCP connection.
    pub fn set_tcp_settings(self, tcp_settings: crate::io::tcp::TcpSettings) -> Self {
        Self {
            tcp_settings,
            ..self
        }
    }

    /// Set the cache behavior.
    #[cfg(feature = "cache-aio")]
    pub fn set_cache_config(self, cache_config: crate::caching::CacheConfig) -> Self {
        Self {
            cache_config: Some(cache_config),
            ..self
        }
    }
}

impl Default for ConnectionManagerConfig {
    fn default() -> Self {
        Self {
            exponent_base: Self::DEFAULT_CONNECTION_RETRY_EXPONENT_BASE,
            factor: Self::DEFAULT_CONNECTION_RETRY_FACTOR,
            number_of_retries: Self::DEFAULT_NUMBER_OF_CONNECTION_RETRIES,
            response_timeout: Self::DEFAULT_RESPONSE_TIMEOUT,
            connection_timeout: Self::DEFAULT_CONNECTION_TIMEOUT,
            max_delay: None,
            push_sender: None,
            resubscribe_automatically: false,
            tcp_settings: Default::default(),
            #[cfg(feature = "cache-aio")]
            cache_config: None,
        }
    }
}

struct Internals {
    /// Information used for the connection. This is needed to be able to reconnect.
    client: Client,
    /// The connection future.
    ///
    /// The `ArcSwap` is required to be able to replace the connection
    /// without making the `ConnectionManager` mutable.
    connection: ArcSwap<SharedRedisFuture<MultiplexedConnection>>,

    runtime: Runtime,
    retry_strategy: ExponentialBuilder,
    connection_config: AsyncConnectionConfig,
    subscription_tracker: Option<Mutex<SubscriptionTracker>>,
    #[cfg(feature = "cache-aio")]
    cache_manager: Option<CacheManager>,
    _task_handle: HandleContainer,
}

/// A `ConnectionManager` is a proxy that wraps a [multiplexed
/// connection][multiplexed-connection] and automatically reconnects to the
/// server when necessary.
///
/// Like the [`MultiplexedConnection`][multiplexed-connection], this
/// manager can be cloned, allowing requests to be sent concurrently on
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
/// - If the connection manager uses RESP3 connection,it actively listens to updates from the
///   server, and so it will cause the manager to reconnect after a disconnection, even if the manager was unused at
///   the time of the disconnect.
///
/// [multiplexed-connection]: struct.MultiplexedConnection.html
#[derive(Clone)]
pub struct ConnectionManager(Arc<Internals>);

/// A `RedisResult` that can be cloned because `RedisError` is behind an `Arc`.
type CloneableRedisResult<T> = Result<T, Arc<RedisError>>;

/// Type alias for a shared boxed future that will resolve to a `CloneableRedisResult`.
type SharedRedisFuture<T> = Shared<BoxFuture<'static, CloneableRedisResult<T>>>;

/// Handle a command result. If the connection was dropped, reconnect.
macro_rules! reconnect_if_dropped {
    ($self:expr, $result:expr, $current:expr) => {
        if let Err(ref e) = $result {
            if e.is_unrecoverable_error() {
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
        let config = ConnectionManagerConfig::new();

        Self::new_with_config(client, config).await
    }

    /// Connect to the server and store the connection inside the returned `ConnectionManager`.
    ///
    /// This requires the `connection-manager` feature, which will also pull in
    /// the Tokio executor.
    ///
    /// In case of reconnection issues, the manager will retry reconnection
    /// number_of_retries times, with an exponentially increasing delay, calculated as
    /// rand(0 .. factor * (exponent_base ^ current-try)).
    #[deprecated(note = "Use `new_with_config`")]
    pub async fn new_with_backoff(
        client: Client,
        exponent_base: u64,
        factor: u64,
        number_of_retries: usize,
    ) -> RedisResult<Self> {
        let config = ConnectionManagerConfig::new()
            .set_exponent_base(exponent_base)
            .set_factor(factor)
            .set_number_of_retries(number_of_retries);
        Self::new_with_config(client, config).await
    }

    /// Connect to the server and store the connection inside the returned `ConnectionManager`.
    ///
    /// This requires the `connection-manager` feature, which will also pull in
    /// the Tokio executor.
    ///
    /// In case of reconnection issues, the manager will retry reconnection
    /// number_of_retries times, with an exponentially increasing delay, calculated as
    /// rand(0 .. factor * (exponent_base ^ current-try)).
    ///
    /// The new connection will time out operations after `response_timeout` has passed.
    /// Each connection attempt to the server will time out after `connection_timeout`.
    #[deprecated(note = "Use `new_with_config`")]
    pub async fn new_with_backoff_and_timeouts(
        client: Client,
        exponent_base: u64,
        factor: u64,
        number_of_retries: usize,
        response_timeout: std::time::Duration,
        connection_timeout: std::time::Duration,
    ) -> RedisResult<Self> {
        let config = ConnectionManagerConfig::new()
            .set_exponent_base(exponent_base)
            .set_factor(factor)
            .set_number_of_retries(number_of_retries)
            .set_response_timeout(response_timeout)
            .set_connection_timeout(connection_timeout);

        Self::new_with_config(client, config).await
    }

    /// Connect to the server and store the connection inside the returned `ConnectionManager`.
    ///
    /// This requires the `connection-manager` feature, which will also pull in
    /// the Tokio executor.
    ///
    /// In case of reconnection issues, the manager will retry reconnection
    /// number_of_retries times, with an exponentially increasing delay, calculated as
    /// rand(0 .. factor * (exponent_base ^ current-try)).
    ///
    /// Apply a maximum delay. No retry delay will be longer than this  ConnectionManagerConfig.max_delay` .
    ///
    /// The new connection will time out operations after `response_timeout` has passed.
    /// Each connection attempt to the server will time out after `connection_timeout`.
    pub async fn new_with_config(
        client: Client,
        config: ConnectionManagerConfig,
    ) -> RedisResult<Self> {
        // Create a MultiplexedConnection and wait for it to be established
        let runtime = Runtime::locate();

        if config.resubscribe_automatically && config.push_sender.is_none() {
            return Err((crate::ErrorKind::ClientError, "Cannot set resubscribe_automatically without setting a push sender to receive messages.").into());
        }

        let mut retry_strategy = ExponentialBuilder::default()
            .with_factor(config.factor as f32)
            .with_max_times(config.number_of_retries)
            .with_jitter();
        if let Some(max_delay) = config.max_delay {
            retry_strategy =
                retry_strategy.with_max_delay(std::time::Duration::from_millis(max_delay));
        }

        let mut connection_config = AsyncConnectionConfig::new();
        if let Some(connection_timeout) = config.connection_timeout {
            connection_config = connection_config.set_connection_timeout(connection_timeout);
        }
        if let Some(response_timeout) = config.response_timeout {
            connection_config = connection_config.set_response_timeout(response_timeout);
        }
        connection_config = connection_config.set_tcp_settings(config.tcp_settings);
        #[cfg(feature = "cache-aio")]
        let cache_manager = config
            .cache_config
            .as_ref()
            .map(|cache_config| CacheManager::new(*cache_config));
        #[cfg(feature = "cache-aio")]
        if let Some(cache_manager) = cache_manager.as_ref() {
            connection_config = connection_config.set_cache_manager(cache_manager.clone());
        }

        let (oneshot_sender, oneshot_receiver) = oneshot::channel();
        let _task_handle = HandleContainer::new(
            runtime.spawn(Self::check_for_disconnect_pushes(oneshot_receiver)),
        );

        let mut components_for_reconnection_on_push = None;
        if let Some(push_sender) = config.push_sender.clone() {
            check_resp3!(
                client.connection_info.redis.protocol,
                "Can only pass push sender to a connection using RESP3"
            );

            let (internal_sender, internal_receiver) = unbounded_channel();
            components_for_reconnection_on_push = Some((internal_receiver, Some(push_sender)));

            connection_config =
                connection_config.set_push_sender_internal(Arc::new(internal_sender));
        } else if client.connection_info.redis.protocol != ProtocolVersion::RESP2 {
            let (internal_sender, internal_receiver) = unbounded_channel();
            components_for_reconnection_on_push = Some((internal_receiver, None));

            connection_config =
                connection_config.set_push_sender_internal(Arc::new(internal_sender));
        }

        let connection =
            Self::new_connection(&client, retry_strategy, &connection_config, None).await?;
        let subscription_tracker = if config.resubscribe_automatically {
            Some(Mutex::new(SubscriptionTracker::default()))
        } else {
            None
        };

        let new_self = Self(Arc::new(Internals {
            client,
            connection: ArcSwap::from_pointee(future::ok(connection).boxed().shared()),
            runtime,
            retry_strategy,
            connection_config,
            subscription_tracker,
            #[cfg(feature = "cache-aio")]
            cache_manager,
            _task_handle,
        }));

        if let Some((internal_receiver, external_sender)) = components_for_reconnection_on_push {
            oneshot_sender
                .send((new_self.clone(), internal_receiver, external_sender))
                .map_err(|_| {
                    crate::RedisError::from((
                        crate::ErrorKind::ClientError,
                        "Failed to set automatic resubscription",
                    ))
                })?;
        };

        Ok(new_self)
    }

    async fn new_connection(
        client: &Client,
        exponential_backoff: ExponentialBuilder,
        connection_config: &AsyncConnectionConfig,
        additional_commands: Option<Pipeline>,
    ) -> RedisResult<MultiplexedConnection> {
        let connection_config = connection_config.clone();
        let get_conn = || async {
            client
                .get_multiplexed_async_connection_with_config(&connection_config)
                .await
        };
        let mut conn = get_conn
            .retry(exponential_backoff)
            .sleep(|duration| async move { Runtime::locate().sleep(duration).await })
            .await?;
        if let Some(pipeline) = additional_commands {
            // TODO - should we ignore these failures?
            let _ = pipeline.exec_async(&mut conn).await;
        }
        Ok(conn)
    }

    /// Reconnect and overwrite the old connection.
    ///
    /// The `current` guard points to the shared future that was active
    /// when the connection loss was detected.
    fn reconnect(&self, current: arc_swap::Guard<Arc<SharedRedisFuture<MultiplexedConnection>>>) {
        let self_clone = self.clone();
        #[cfg(not(feature = "cache-aio"))]
        let connection_config = self_clone.0.connection_config.clone();
        #[cfg(feature = "cache-aio")]
        let mut connection_config = self_clone.0.connection_config.clone();
        #[cfg(feature = "cache-aio")]
        if let Some(manager) = self.0.cache_manager.as_ref() {
            let new_cache_manager = manager.clone_and_increase_epoch();
            connection_config = connection_config.set_cache_manager(new_cache_manager);
        }
        let new_connection: SharedRedisFuture<MultiplexedConnection> = async move {
            let additional_commands = match &self_clone.0.subscription_tracker {
                Some(subscription_tracker) => Some(
                    subscription_tracker
                        .lock()
                        .await
                        .get_subscription_pipeline(),
                ),
                None => None,
            };

            let con = Self::new_connection(
                &self_clone.0.client,
                self_clone.0.retry_strategy,
                &connection_config,
                additional_commands,
            )
            .await?;
            Ok(con)
        }
        .boxed()
        .shared();

        // Update the connection in the connection manager
        let new_connection_arc = Arc::new(new_connection.clone());
        let prev = self
            .0
            .connection
            .compare_and_swap(&current, new_connection_arc);

        // If the swap happened...
        if Arc::ptr_eq(&prev, &current) {
            // ...start the connection attempt immediately but do not wait on it.
            self.0.runtime.spawn(new_connection.map(|_| ()));
        }
    }

    async fn check_for_disconnect_pushes(
        receiver: oneshot::Receiver<(
            ConnectionManager,
            UnboundedReceiver<PushInfo>,
            OptionalPushSender,
        )>,
    ) {
        let Ok((this, mut internal_receiver, external_sender)) = receiver.await else {
            return;
        };
        while let Some(push_info) = internal_receiver.recv().await {
            if push_info.kind == PushKind::Disconnection {
                this.reconnect(this.0.connection.load());
            }
            if let Some(sender) = external_sender.as_ref() {
                let _ = sender.send(push_info);
            }
        }
    }

    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    pub async fn send_packed_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        // Clone connection to avoid having to lock the ArcSwap in write mode
        let guard = self.0.connection.load();
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
        let guard = self.0.connection.load();
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

    async fn update_subscription_tracker(
        &self,
        action: SubscriptionAction,
        args: impl ToRedisArgs,
    ) {
        let Some(subscription_tracker) = &self.0.subscription_tracker else {
            return;
        };
        let mut guard = subscription_tracker.lock().await;
        guard.update_with_request(action, args.to_redis_args().into_iter());
    }

    /// Subscribes to a new channel(s).    
    ///
    /// Updates from the sender will be sent on the push sender that was passed to the manager.
    /// If the manager was configured without a push sender, the connection won't be able to pass messages back to the user.
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    /// It should be noted that unless [ConnectionManagerConfig::set_automatic_resubscription] was called,
    /// the subscription will be removed on a disconnect and must be re-subscribed.
    pub async fn subscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.0.client.connection_info.redis.protocol);
        let mut cmd = cmd("SUBSCRIBE");
        cmd.arg(&channel_name);
        cmd.exec_async(self).await?;
        self.update_subscription_tracker(SubscriptionAction::Subscribe, channel_name)
            .await;

        Ok(())
    }

    /// Unsubscribes from channel(s).
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    pub async fn unsubscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.0.client.connection_info.redis.protocol);
        let mut cmd = cmd("UNSUBSCRIBE");
        cmd.arg(&channel_name);
        cmd.exec_async(self).await?;
        self.update_subscription_tracker(SubscriptionAction::Unsubscribe, channel_name)
            .await;
        Ok(())
    }

    /// Subscribes to new channel(s) with pattern(s).
    ///
    /// Updates from the sender will be sent on the push sender that was passed to the manager.
    /// If the manager was configured without a push sender, the manager won't be able to pass messages back to the user.
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    /// It should be noted that unless [ConnectionManagerConfig::set_automatic_resubscription] was called,
    /// the subscription will be removed on a disconnect and must be re-subscribed.
    pub async fn psubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.0.client.connection_info.redis.protocol);
        let mut cmd = cmd("PSUBSCRIBE");
        cmd.arg(&channel_pattern);
        cmd.exec_async(self).await?;
        self.update_subscription_tracker(SubscriptionAction::PSubscribe, channel_pattern)
            .await;
        Ok(())
    }

    /// Unsubscribes from channel pattern(s).
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    pub async fn punsubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.0.client.connection_info.redis.protocol);
        let mut cmd = cmd("PUNSUBSCRIBE");
        cmd.arg(&channel_pattern);
        cmd.exec_async(self).await?;
        self.update_subscription_tracker(SubscriptionAction::PUnsubscribe, channel_pattern)
            .await;
        Ok(())
    }

    /// Gets [`crate::caching::CacheStatistics`] for current connection if caching is enabled.
    #[cfg(feature = "cache-aio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cache-aio")))]
    pub fn get_cache_statistics(&self) -> Option<crate::caching::CacheStatistics> {
        self.0.cache_manager.as_ref().map(|cm| cm.statistics())
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
        self.0.client.connection_info().redis.db
    }
}
