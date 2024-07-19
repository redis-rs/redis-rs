use super::RedisFuture;
use crate::{
    aio::{check_resp3, ConnectionLike, MultiplexedConnection, Runtime},
    cmd,
    types::{AsyncPushSender, RedisError, RedisResult, Value},
    AsyncConnectionConfig, Client, Cmd, ToRedisArgs,
};
#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use ::async_std::net::ToSocketAddrs;
use arc_swap::ArcSwap;
use futures::{
    future::{self, Shared},
    FutureExt,
};
use futures_util::future::BoxFuture;
use std::sync::Arc;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

/// ConnectionManager is the configuration for reconnect mechanism and request timing
#[derive(Clone, Debug, Default)]
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
    response_timeout: std::time::Duration,
    /// Each connection attempt to the server will time out after `connection_timeout`.
    connection_timeout: std::time::Duration,
    /// sender channel for push values
    push_sender: Option<AsyncPushSender>,
}

impl ConnectionManagerConfig {
    const DEFAULT_CONNECTION_RETRY_EXPONENT_BASE: u64 = 2;
    const DEFAULT_CONNECTION_RETRY_FACTOR: u64 = 100;
    const DEFAULT_NUMBER_OF_CONNECTION_RETRIES: usize = 6;
    const DEFAULT_RESPONSE_TIMEOUT: std::time::Duration = std::time::Duration::MAX;
    const DEFAULT_CONNECTION_TIMEOUT: std::time::Duration = std::time::Duration::MAX;

    /// Creates a new instance of the options with nothing set
    pub fn new() -> Self {
        Self {
            exponent_base: Self::DEFAULT_CONNECTION_RETRY_EXPONENT_BASE,
            factor: Self::DEFAULT_CONNECTION_RETRY_FACTOR,
            number_of_retries: Self::DEFAULT_NUMBER_OF_CONNECTION_RETRIES,
            max_delay: None,
            response_timeout: Self::DEFAULT_RESPONSE_TIMEOUT,
            connection_timeout: Self::DEFAULT_CONNECTION_TIMEOUT,
            push_sender: None,
        }
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
        self.response_timeout = duration;
        self
    }

    /// Each connection attempt to the server will time out after `connection_timeout`.
    pub fn set_connection_timeout(
        mut self,
        duration: std::time::Duration,
    ) -> ConnectionManagerConfig {
        self.connection_timeout = duration;
        self
    }

    /// Sets sender channel for push values. Will fail client creation if the connection isn't configured for RESP3 communications.
    pub fn set_push_sender(mut self, sender: AsyncPushSender) -> Self {
        self.push_sender = Some(sender);
        self
    }
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
    retry_strategy: ExponentialBackoff,
    number_of_retries: usize,
    connection_config: AsyncConnectionConfig,
}

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

        let mut retry_strategy =
            ExponentialBackoff::from_millis(config.exponent_base).factor(config.factor);
        if let Some(max_delay) = config.max_delay {
            retry_strategy = retry_strategy.max_delay(std::time::Duration::from_millis(max_delay));
        }

        let mut connection_config = AsyncConnectionConfig::new()
            .set_connection_timeout(config.connection_timeout)
            .set_response_timeout(config.response_timeout);

        if let Some(push_sender) = config.push_sender.clone() {
            check_resp3!(
                client.connection_info.redis.protocol,
                "Can only pass push sender to a connection using RESP3"
            );
            connection_config = connection_config.set_push_sender(push_sender);
        }

        let connection = Self::new_connection(
            client.clone(),
            retry_strategy.clone(),
            config.number_of_retries,
            &connection_config,
        )
        .await?;

        // Wrap the connection in an `ArcSwap` instance for fast atomic access
        Ok(Self {
            client,
            connection: Arc::new(ArcSwap::from_pointee(
                future::ok(connection).boxed().shared(),
            )),
            runtime,
            number_of_retries: config.number_of_retries,
            retry_strategy,
            connection_config,
        })
    }

    async fn new_connection(
        client: Client,
        exponential_backoff: ExponentialBackoff,
        number_of_retries: usize,
        connection_config: &AsyncConnectionConfig,
    ) -> RedisResult<MultiplexedConnection> {
        let retry_strategy = exponential_backoff.map(jitter).take(number_of_retries);
        let connection_config = connection_config.clone();
        Retry::spawn(retry_strategy, || {
            client.get_multiplexed_async_connection_with_config(&connection_config)
        })
        .await
    }

    /// Reconnect and overwrite the old connection.
    ///
    /// The `current` guard points to the shared future that was active
    /// when the connection loss was detected.
    fn reconnect(&self, current: arc_swap::Guard<Arc<SharedRedisFuture<MultiplexedConnection>>>) {
        let client = self.client.clone();
        let retry_strategy = self.retry_strategy.clone();
        let number_of_retries = self.number_of_retries;
        let connection_config = self.connection_config.clone();
        let new_connection: SharedRedisFuture<MultiplexedConnection> = async move {
            let con = Self::new_connection(
                client,
                retry_strategy,
                number_of_retries,
                &connection_config,
            )
            .await?;
            Ok(con)
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

    /// Subscribes to a new channel.
    /// It should be noted that the subscription will be removed on a disconnect and must be re-subscribed.
    pub async fn subscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.client.connection_info.redis.protocol);
        let mut cmd = cmd("SUBSCRIBE");
        cmd.arg(channel_name);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Unsubscribes from channel.
    pub async fn unsubscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.client.connection_info.redis.protocol);
        let mut cmd = cmd("UNSUBSCRIBE");
        cmd.arg(channel_name);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Subscribes to a new channel with pattern.
    /// It should be noted that the subscription will be removed on a disconnect and must be re-subscribed.
    pub async fn psubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.client.connection_info.redis.protocol);
        let mut cmd = cmd("PSUBSCRIBE");
        cmd.arg(channel_pattern);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Unsubscribes from channel pattern.
    pub async fn punsubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.client.connection_info.redis.protocol);
        let mut cmd = cmd("PUNSUBSCRIBE");
        cmd.arg(channel_pattern);
        cmd.exec_async(self).await?;
        Ok(())
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
