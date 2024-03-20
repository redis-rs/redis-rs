#[cfg(feature = "cache")]
use crate::caching::CacheConfig;
use std::time::Duration;

/// In case of reconnection issues, the manager will retry reconnection
/// number_of_retries times, with an exponentially increasing delay, calculated as
/// rand(0 .. factor * (exponent_base ^ current-try)).
#[derive(Clone, Debug)]
pub struct ConnectionManagerBackOffConfig {
    /// Default is 2
    pub exponent_base: u64,
    /// Default is 100
    pub factor: u64,
    /// Default is 6
    pub number_of_retries: usize,
}

impl Default for ConnectionManagerBackOffConfig {
    fn default() -> Self {
        Self {
            exponent_base: 2,
            factor: 100,
            number_of_retries: 6,
        }
    }
}

/// ConnectionConfig is used by both `MultiplexedConnection` and `ConnectionManager`
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Sending commands will time out after `response_timeout` has passed.
    /// Default is Duration::MAX
    pub response_timeout: Duration,
    /// Connection will time out connecting after `connection_timeout` has passed.
    /// Default is Duration::MAX
    pub connection_timeout: Duration,
    #[cfg(feature = "cache")]
    /// CacheConfig to be used by `MultiplexedConnection` and `ConnectionManager`
    pub cache_config: CacheConfig,
    /// BackOff settings used by `ConnectionManager`
    pub back_off: ConnectionManagerBackOffConfig,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        ConnectionConfigBuilder::new().build()
    }
}

/// `ConnectionConfigBuilder` is used to build `ConnectionConfig` easily.
pub struct ConnectionConfigBuilder {
    config: ConnectionConfig,
}

impl ConnectionConfigBuilder {
    /// Creates new `ConnectionConfigBuilder`
    pub fn new() -> Self {
        ConnectionConfigBuilder {
            config: ConnectionConfig {
                response_timeout: Duration::MAX,
                connection_timeout: Duration::MAX,
                #[cfg(feature = "cache")]
                cache_config: CacheConfig::disabled(),
                back_off: ConnectionManagerBackOffConfig::default(),
            },
        }
    }
    /// Sets `response_timeout`
    pub fn response_timeout(mut self, response_timeout: Duration) -> Self {
        self.config.response_timeout = response_timeout;
        self
    }
    /// Sets `connection_timeout`
    pub fn connection_timeout(mut self, connection_timeout: Duration) -> Self {
        self.config.connection_timeout = connection_timeout;
        self
    }
    #[cfg(feature = "cache")]
    /// Sets `cache_config`
    pub fn cache_config(mut self, cache_config: CacheConfig) -> Self {
        self.config.cache_config = cache_config;
        self
    }
    /// Sets `back_off`
    pub fn back_off(mut self, back_off: ConnectionManagerBackOffConfig) -> Self {
        self.config.back_off = back_off;
        self
    }
    /// Builds ConnectionConfig by both default and supplied parameters.
    pub fn build(self) -> ConnectionConfig {
        self.config
    }
}

impl Default for ConnectionConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
