use super::{AsyncPushSender, HandleContainer, RedisFuture};
#[cfg(feature = "cache-aio")]
use crate::caching::CacheManager;
use crate::{
    aio::{ConnectionLike, MultiplexedConnection, Runtime},
    check_resp3, cmd,
    subscription_tracker::{SubscriptionAction, SubscriptionTracker},
    types::{RedisError, RedisResult, Value},
    AsyncConnectionConfig, Client, Cmd, Pipeline, ProtocolVersion, PushInfo, PushKind, ToRedisArgs,
};
use arc_swap::ArcSwap;
use backon::{ExponentialBuilder, Retryable};
use futures_channel::oneshot;
use futures_util::future::{BoxFuture, FutureExt, Shared};
use std::sync::{Arc, Weak};
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
    /// Security configuration for the connection manager
    security_config: SecurityConfig,
    /// Optional client name to set on (re)connect
    client_name: Option<String>,
    /// Optional CLIENT SETINFO key/value pairs
    client_setinfo_pairs: Option<Vec<(String, String)>>,
    /// Optional user-provided initialization pipeline
    user_init_pipeline: Option<Pipeline>,
    /// Whether to allow arbitrary init commands (dangerous). Default: false
    allow_arbitrary_init_commands: bool,
}

/// Security configuration for ConnectionManager initialization
#[derive(Clone)]
pub struct SecurityConfig {
    /// Maximum size for client names in bytes (default: 64KB)
    max_client_name_size: usize,
    /// Whether to restrict client names to ASCII-only (default: false)
    validate_ascii: bool,
    /// Timeout for initialization pipeline commands (default: 30 seconds)
    init_timeout: std::time::Duration,
    /// Whether to enable resource monitoring hooks (default: false)
    enable_resource_monitoring: bool,
    /// Resource monitoring callback
    resource_monitor: Option<Arc<dyn ResourceMonitor>>,
}

/// Trait for monitoring resource usage during connection initialization
pub trait ResourceMonitor: Send + Sync {
    /// Called when connection initialization starts
    fn on_init_start(&self, client_info: &str);

    /// Called when a command is executed during initialization
    fn on_command_executed(
        &self,
        command: &str,
        duration: std::time::Duration,
        memory_delta: Option<i64>,
    );

    /// Called when initialization completes successfully
    fn on_init_complete(&self, total_duration: std::time::Duration, final_memory: Option<u64>);

    /// Called when initialization fails
    fn on_init_failed(&self, error: &InitializationError, total_duration: std::time::Duration);
}

/// Resource usage statistics
#[derive(Debug, Clone)]
pub struct ResourceStats {
    /// Approximate memory usage observed during initialization (bytes), if available
    pub memory_usage: Option<u64>,
    /// Number of connections observed during initialization, if available
    pub connection_count: Option<u32>,
    /// CPU time spent during initialization, if available
    pub cpu_time: Option<std::time::Duration>,
    /// Total network bytes sent during initialization, if available
    pub network_bytes_sent: Option<u64>,
    /// Total network bytes received during initialization, if available
    pub network_bytes_received: Option<u64>,
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
            security_config: _,
            ..
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

impl SecurityConfig {
    const DEFAULT_MAX_CLIENT_NAME_SIZE: usize = 65536; // 64KB
    const DEFAULT_VALIDATE_ASCII: bool = false;
    const DEFAULT_INIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    const DEFAULT_ENABLE_RESOURCE_MONITORING: bool = false;

    /// Creates a new SecurityConfig with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum client name size in bytes
    pub fn set_max_client_name_size(mut self, size: usize) -> Self {
        self.max_client_name_size = size;
        self
    }

    /// Restrict client names to ASCII-only
    pub fn set_validate_ascii(mut self, validate: bool) -> Self {
        self.validate_ascii = validate;
        self
    }

    /// Set timeout for initialization pipeline commands
    pub fn set_init_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.init_timeout = timeout;
        self
    }

    /// Enable resource monitoring hooks
    pub fn set_enable_resource_monitoring(mut self, enable: bool) -> Self {
        self.enable_resource_monitoring = enable;
        self
    }

    /// Set resource monitoring callback
    pub fn set_resource_monitor(mut self, monitor: impl ResourceMonitor + 'static) -> Self {
        self.resource_monitor = Some(Arc::new(monitor));
        self.enable_resource_monitoring = true;
        self
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            max_client_name_size: Self::DEFAULT_MAX_CLIENT_NAME_SIZE,
            validate_ascii: Self::DEFAULT_VALIDATE_ASCII,
            init_timeout: Self::DEFAULT_INIT_TIMEOUT,
            enable_resource_monitoring: Self::DEFAULT_ENABLE_RESOURCE_MONITORING,
            resource_monitor: None,
        }
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

    /// A multiplicative scale applied to the exponential backoff delay (milliseconds).
    ///
    /// For example, with `exponent_base = 2` and `factor = 1000`, backoff delays are approximately
    /// 1000ms, 2000ms, 4000ms (with jitter).
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

    /// Set security configuration
    pub fn set_security_config(mut self, security_config: SecurityConfig) -> Self {
        self.security_config = security_config;
        self
    }

    /// Sets a client name to be applied via CLIENT SETNAME during (re)initialization.
    pub fn set_client_name(mut self, name: String) -> Self {
        // Validate length and UTF-8 if configured
        let _ = validate_client_name(&name, &self.security_config);
        self.client_name = Some(name);
        self
    }

    /// Sets key/value pairs to be applied via CLIENT SETINFO during (re)initialization.
    pub fn set_client_setinfo(mut self, setinfo_pairs: Vec<(String, String)>) -> Self {
        self.client_setinfo_pairs = Some(setinfo_pairs);
        self
    }

    /// Sets a custom initialization pipeline.
    pub fn set_init_pipeline(mut self, pipeline: Pipeline) -> Self {
        self.user_init_pipeline = Some(pipeline);
        self
    }

    /// Enables arbitrary initialization commands (dangerous). Use with caution.
    pub fn enable_arbitrary_init_commands(mut self) -> Self {
        self.allow_arbitrary_init_commands = true;
        self
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
            security_config: SecurityConfig::default(),
            client_name: None,
            client_setinfo_pairs: None,
            user_init_pipeline: None,
            allow_arbitrary_init_commands: false,
        }
    }
}

/// Validates a client name according to security configuration
fn validate_client_name(name: &str, config: &SecurityConfig) -> RedisResult<()> {
    // Check size limit
    if name.len() > config.max_client_name_size {
        return Err(crate::RedisError::from((
            crate::ErrorKind::ClientError,
            "Client name too large",
            format!(
                "{} bytes exceeds limit of {} bytes",
                name.len(),
                config.max_client_name_size
            ),
        )));
    }

    // Check ASCII-only constraint if enabled
    if config.validate_ascii && !name.is_ascii() {
        return Err(crate::RedisError::from((
            crate::ErrorKind::ClientError,
            "Client name contains invalid UTF-8 sequences",
        )));
    }

    Ok(())
}

/// Enhanced error handling for initialization pipeline commands
#[derive(Debug, Clone)]
pub struct InitializationError {
    /// The command (or logical step) that failed during initialization
    pub command: String,
    /// The underlying error, wrapped so the result can be cloned
    pub error: Arc<RedisError>,
    /// How many retries were attempted before surfacing the error
    pub retry_count: usize,
    /// Whether the error is considered recoverable by reconnect/retry logic
    pub is_recoverable: bool,
}

impl InitializationError {
    fn new(command: String, error: RedisError, retry_count: usize) -> Self {
        let is_recoverable = Self::is_error_recoverable(&error);
        Self {
            command,
            error: Arc::new(error),
            retry_count,
            is_recoverable,
        }
    }

    fn is_error_recoverable(error: &RedisError) -> bool {
        match error.kind() {
            crate::ErrorKind::IoError => true,
            crate::ErrorKind::ResponseError => {
                // Some response errors are recoverable (timeouts, temporary failures)
                let error_str = error.to_string().to_lowercase();
                error_str.contains("timeout")
                    || error_str.contains("loading")
                    || error_str.contains("busy")
            }
            crate::ErrorKind::AuthenticationFailed => false,
            crate::ErrorKind::TypeError => false,
            crate::ErrorKind::ExecAbortError => false,
            crate::ErrorKind::BusyLoadingError => true,
            crate::ErrorKind::NoScriptError => false,
            crate::ErrorKind::InvalidClientConfig => false,
            crate::ErrorKind::Moved => true,
            crate::ErrorKind::Ask => true,
            crate::ErrorKind::TryAgain => true,
            crate::ErrorKind::ClusterDown => true,
            crate::ErrorKind::CrossSlot => false,
            crate::ErrorKind::MasterDown => true,
            crate::ErrorKind::ReadOnly => false,
            crate::ErrorKind::ClientError => false,
            _ => false,
        }
    }
}

impl std::fmt::Display for InitializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Initialization command '{}' failed after {} retries: {}",
            self.command, self.retry_count, self.error
        )
    }
}

impl std::error::Error for InitializationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

// Readiness tracking types
type ReadyResult = CloneableRedisResult<()>;
type ReadySender = tokio::sync::watch::Sender<ReadyResult>;
type ReadyReceiver = tokio::sync::watch::Receiver<ReadyResult>;

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
    /// Configured initialization pipeline (built from config)
    init_pipeline: Option<Pipeline>,
    subscription_tracker: Option<Mutex<SubscriptionTracker>>,
    #[cfg(feature = "cache-aio")]
    cache_manager: Option<CacheManager>,
    _task_handle: HandleContainer,
    security_config: SecurityConfig,
    /// Readiness watch channel
    ready_tx: ReadySender,
    ready_rx: ReadyReceiver,
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
                Self::reconnect(Arc::downgrade(&$self.0), $current);
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
                Self::reconnect(Arc::downgrade(&$self.0), $current);
            }
            return Err(e);
        }
    };
}

impl ConnectionManager {
    /// Wait until the connection is fully initialized (post-connect init completed)
    pub async fn await_ready(&self) -> RedisResult<()> {
        let mut rx = self.0.ready_rx.clone();
        loop {
            // Take a snapshot without holding the borrow across await
            let snapshot = { rx.borrow().clone() };
            match snapshot {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if e.kind() == crate::ErrorKind::ClientError {
                        if rx.changed().await.is_err() {
                            return Err(
                                (crate::ErrorKind::IoError, "readiness channel closed").into()
                            );
                        }
                        continue;
                    } else {
                        return Err((*e).clone_mostly("await_ready"));
                    }
                }
            }
        }
    }

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

    /// Build initialization pipeline based on configuration (safe by default)
    fn build_config_init_pipeline(config: &ConnectionManagerConfig) -> Option<Pipeline> {
        let mut pipeline = Pipeline::new();
        let mut has_any = false;

        // CLIENT SETNAME (validated earlier in setter, but validate again defensively)
        if let Some(name) = &config.client_name {
            if validate_client_name(name, &config.security_config).is_ok() {
                pipeline.cmd("CLIENT").arg("SETNAME").arg(name);
                has_any = true;
            }
        }

        // CLIENT SETINFO key/value pairs
        if let Some(pairs) = &config.client_setinfo_pairs {
            if !pairs.is_empty() {
                let mut cmd = cmd("CLIENT");
                cmd.arg("SETINFO");
                for (k, v) in pairs.iter() {
                    cmd.arg(k).arg(v);
                }
                pipeline.add_command(cmd);
                has_any = true;
            }
        }

        // User-provided pipeline only if arbitrary init is enabled
        if config.allow_arbitrary_init_commands {
            if let Some(user) = &config.user_init_pipeline {
                // Append user commands as-is
                for c in user.cmd_iter() {
                    pipeline.add_command(c.clone());
                    has_any = true;
                }
            }
        }

        if has_any {
            Some(pipeline)
        } else {
            None
        }
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
        connection_config = connection_config.set_tcp_settings(config.tcp_settings.clone());
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

        // Early security validation (e.g., client name)
        if let Some(ref name) = config.client_name {
            validate_client_name(name, &config.security_config)?;
        }

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
            // await_ready will be defined later in impl block, not inside this function body.

            let (internal_sender, internal_receiver) = unbounded_channel();
            components_for_reconnection_on_push = Some((internal_receiver, None));

            connection_config =
                connection_config.set_push_sender_internal(Arc::new(internal_sender));
        }

        // Build initial initialization pipeline from config (no resubscriptions on first connect)
        let initial_init = Self::build_config_init_pipeline(&config);
        let connection = Self::new_connection(
            &client,
            retry_strategy,
            &connection_config,
            initial_init,
            &config.security_config,
        )
        .await?;
        // Provide await_ready on the manager
        // Note: method implemented below in the impl block

        let subscription_tracker = if config.resubscribe_automatically {
            Some(Mutex::new(SubscriptionTracker::default()))
        } else {
            None
        };

        // Initialize readiness channel: start as not ready
        let (ready_tx, ready_rx) = tokio::sync::watch::channel::<ReadyResult>(Err(Arc::new(
            (crate::ErrorKind::ClientError, "not ready").into(),
        )));

        // Built init pipeline from config
        let cfg_init = Self::build_config_init_pipeline(&config);

        // Create initial ready shared future in the required CloneableRedisResult form
        let initial_future: SharedRedisFuture<MultiplexedConnection> =
            futures_util::future::ready(Ok(connection)).boxed().shared();

        let new_self = Self(Arc::new(Internals {
            client,
            connection: ArcSwap::from_pointee(initial_future),
            runtime,
            retry_strategy,
            connection_config,
            init_pipeline: cfg_init,
            subscription_tracker,
            #[cfg(feature = "cache-aio")]
            cache_manager,
            _task_handle,
            security_config: config.security_config,
            ready_tx,
            ready_rx,
        }));

        // Mark as ready if there is no init pipeline; otherwise set OK (already executed in new_connection)
        let _ = new_self.0.ready_tx.send(Ok(()));

        if let Some((internal_receiver, external_sender)) = components_for_reconnection_on_push {
            oneshot_sender
                .send((
                    Arc::downgrade(&new_self.0),
                    internal_receiver,
                    external_sender,
                ))
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
        security_config: &SecurityConfig,
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
            // Execute initialization pipeline with timeout and enhanced error handling
            Self::execute_init_pipeline(&mut conn, pipeline, security_config).await?;
        }
        Ok(conn)
    }

    /// Execute initialization pipeline with proper timeout and error handling
    async fn execute_init_pipeline(
        conn: &mut MultiplexedConnection,
        pipeline: Pipeline,
        security_config: &SecurityConfig,
    ) -> RedisResult<()> {
        let start_time = std::time::Instant::now();

        // Use runtime timeout helper for portability across runtimes
        let runtime = Runtime::locate();
        let result = runtime
            .timeout(security_config.init_timeout, pipeline.exec_async(conn))
            .await
            .map_err(crate::RedisError::from);

        let duration = start_time.elapsed();

        // Handle the result with enhanced error reporting
        match result {
            Ok(_values) => {
                if let Some(monitor) = &security_config.resource_monitor {
                    monitor.on_init_complete(duration, None);
                }
                Ok(())
            }
            Err(e) => {
                let init_error = InitializationError::new(
                    "initialization_pipeline".to_string(),
                    e,
                    0, // retry count handled at higher level
                );

                if let Some(monitor) = &security_config.resource_monitor {
                    monitor.on_init_failed(&init_error, duration);
                }

                Err(crate::RedisError::from((
                    crate::ErrorKind::ClientError,
                    "Initialization pipeline failed",
                    format!("{}", init_error),
                )))
            }
        }
    }

    /// Reconnect and overwrite the old connection.
    ///
    /// The `current` guard points to the shared future that was active
    /// when the connection loss was detected.
    fn reconnect(
        internals: Weak<Internals>,
        current: arc_swap::Guard<Arc<SharedRedisFuture<MultiplexedConnection>>>,
    ) {
        let Some(internals) = internals.upgrade() else {
            return;
        };
        let internals_clone = internals.clone();
        #[cfg(not(feature = "cache-aio"))]
        let connection_config = internals.connection_config.clone();
        #[cfg(feature = "cache-aio")]
        let mut connection_config = internals.connection_config.clone();
        #[cfg(feature = "cache-aio")]
        if let Some(manager) = internals.cache_manager.as_ref() {
            let new_cache_manager = manager.clone_and_increase_epoch();
            connection_config = connection_config.set_cache_manager(new_cache_manager);
        }
        // Set readiness to not ready during reconnect
        let _ = internals.ready_tx.send(Err(Arc::new(
            (crate::ErrorKind::ClientError, "reconnecting").into(),
        )));

        let new_connection: SharedRedisFuture<MultiplexedConnection> = async move {
            // Combine configured init pipeline with resubscriptions
            let cfg_init = internals_clone.init_pipeline.clone();
            let subs_pipe = match &internals_clone.subscription_tracker {
                Some(subscription_tracker) => Some(
                    subscription_tracker
                        .lock()
                        .await
                        .get_subscription_pipeline(),
                ),
                None => None,
            };
            let combined = match (cfg_init, subs_pipe) {
                (None, None) => None,
                (Some(p), None) => Some(p),
                (None, Some(s)) => Some(s),
                (Some(mut p), Some(s)) => {
                    for c in s.cmd_iter() {
                        p.add_command(c.clone());
                    }
                    Some(p)
                }
            };

            let res = Self::new_connection(
                &internals_clone.client,
                internals_clone.retry_strategy,
                &connection_config,
                combined,
                &internals_clone.security_config,
            )
            .await;

            // Update readiness based on result
            match &res {
                Ok(_) => {
                    let _ = internals_clone.ready_tx.send(Ok(()));
                }
                Err(e) => {
                    let _ = internals_clone
                        .ready_tx
                        .send(Err(Arc::new(e.clone_mostly("reconnect"))));
                }
            }

            // Convert to cloneable result for Shared
            res.map_err(Arc::new)
        }
        .boxed()
        .shared();

        // Update the connection in the connection manager
        let new_connection_arc = Arc::new(new_connection.clone());
        let prev = internals
            .connection
            .compare_and_swap(&current, new_connection_arc);

        // If the swap happened...
        if Arc::ptr_eq(&prev, &current) {
            // ...start the connection attempt immediately but do not wait on it.
            internals.runtime.spawn(new_connection.map(|_| ())).detach();
        }
    }

    async fn check_for_disconnect_pushes(
        receiver: oneshot::Receiver<(
            Weak<Internals>,
            UnboundedReceiver<PushInfo>,
            OptionalPushSender,
        )>,
    ) {
        let Ok((this, mut internal_receiver, external_sender)) = receiver.await else {
            return;
        };
        while let Some(push_info) = internal_receiver.recv().await {
            if push_info.kind == PushKind::Disconnection {
                let Some(internals) = this.upgrade() else {
                    return;
                };
                Self::reconnect(Arc::downgrade(&internals), internals.connection.load());
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
        let args = args.to_redis_args().into_iter();
        subscription_tracker
            .lock()
            .await
            .update_with_request(action, args);
    }

    /// Subscribes to a new channel(s).
    ///
    /// Updates from the sender will be sent on the push sender that was passed to the manager.
    /// If the manager was configured without a push sender, the connection won't be able to pass messages back to the user.
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    /// It should be noted that unless [ConnectionManagerConfig::set_automatic_resubscription] was called,
    /// the subscription will be removed on a disconnect and must be re-subscribed.
    ///
    /// ```rust,no_run
    /// # async fn func() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/?protocol=resp3").unwrap();
    /// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    /// let config = redis::aio::ConnectionManagerConfig::new().set_push_sender(tx);
    /// let mut con = client.get_connection_manager_with_config(config).await?;
    /// con.psubscribe("channel*_1").await?;
    /// con.psubscribe(&["channel*_2", "channel*_3"]).await?;
    /// # Ok(())
    /// # }
    /// ```
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
    ///
    /// ```rust,no_run
    /// # async fn func() -> redis::RedisResult<()> {
    /// let client = redis::Client::open("redis://127.0.0.1/?protocol=resp3").unwrap();
    /// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    /// let config = redis::aio::ConnectionManagerConfig::new().set_push_sender(tx);
    /// let mut con = client.get_connection_manager_with_config(config).await?;
    /// con.psubscribe("channel*_1").await?;
    /// con.psubscribe(&["channel*_2", "channel*_3"]).await?;
    /// # Ok(())
    /// # }
    /// ```
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
