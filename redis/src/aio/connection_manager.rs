use super::RedisFuture;
use crate::cmd::Cmd;
use crate::types::{RedisError, RedisResult, Value};
use crate::{
    aio::{ConnectionLike, MultiplexedConnection, Runtime},
    Client,
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
    retry_strategy: ExponentialBackoff,
    number_of_retries: usize,
    response_timeout: std::time::Duration,
    connection_timeout: std::time::Duration,
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
    const DEFAULT_CONNECTION_RETRY_EXPONENT_BASE: u64 = 2;
    const DEFAULT_CONNECTION_RETRY_FACTOR: u64 = 100;
    const DEFAULT_NUMBER_OF_CONNECTION_RETRIESE: usize = 6;

    /// Connect to the server and store the connection inside the returned `ConnectionManager`.
    ///
    /// This requires the `connection-manager` feature, which will also pull in
    /// the Tokio executor.
    pub async fn new(client: Client) -> RedisResult<Self> {
        Self::new_with_backoff(
            client,
            Self::DEFAULT_CONNECTION_RETRY_EXPONENT_BASE,
            Self::DEFAULT_CONNECTION_RETRY_FACTOR,
            Self::DEFAULT_NUMBER_OF_CONNECTION_RETRIESE,
        )
        .await
    }

    /// Connect to the server and store the connection inside the returned `ConnectionManager`.
    ///
    /// This requires the `connection-manager` feature, which will also pull in
    /// the Tokio executor.
    ///
    /// In case of reconnection issues, the manager will retry reconnection
    /// number_of_retries times, with an exponentially increasing delay, calculated as
    /// rand(0 .. factor * (exponent_base ^ current-try)).
    pub async fn new_with_backoff(
        client: Client,
        exponent_base: u64,
        factor: u64,
        number_of_retries: usize,
    ) -> RedisResult<Self> {
        Self::new_with_backoff_and_timeouts(
            client,
            exponent_base,
            factor,
            number_of_retries,
            std::time::Duration::MAX,
            std::time::Duration::MAX,
        )
        .await
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
    /// The new connection will timeout operations after `response_timeout` has passed.
    /// Each connection attempt to the server will timeout after `connection_timeout`.
    pub async fn new_with_backoff_and_timeouts(
        client: Client,
        exponent_base: u64,
        factor: u64,
        number_of_retries: usize,
        response_timeout: std::time::Duration,
        connection_timeout: std::time::Duration,
    ) -> RedisResult<Self> {
        // Create a MultiplexedConnection and wait for it to be established

        let runtime = Runtime::locate();
        let retry_strategy = ExponentialBackoff::from_millis(exponent_base).factor(factor);
        let connection = Self::new_connection(
            client.clone(),
            retry_strategy.clone(),
            number_of_retries,
            response_timeout,
            connection_timeout,
        )
        .await?;

        // Wrap the connection in an `ArcSwap` instance for fast atomic access
        Ok(Self {
            client,
            connection: Arc::new(ArcSwap::from_pointee(
                future::ok(connection).boxed().shared(),
            )),
            runtime,
            number_of_retries,
            retry_strategy,
            response_timeout,
            connection_timeout,
        })
    }

    async fn new_connection(
        client: Client,
        exponential_backoff: ExponentialBackoff,
        number_of_retries: usize,
        response_timeout: std::time::Duration,
        connection_timeout: std::time::Duration,
    ) -> RedisResult<MultiplexedConnection> {
        let retry_strategy = exponential_backoff.map(jitter).take(number_of_retries);
        Retry::spawn(retry_strategy, || {
            client.get_multiplexed_async_connection_with_timeouts(
                response_timeout,
                connection_timeout,
            )
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
        let response_timeout = self.response_timeout;
        let connection_timeout = self.connection_timeout;
        let new_connection: SharedRedisFuture<MultiplexedConnection> = async move {
            Ok(Self::new_connection(
                client,
                retry_strategy,
                number_of_retries,
                response_timeout,
                connection_timeout,
            )
            .await?)
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
