use std::num::NonZeroUsize;
use std::time::Duration;

/// Defines CacheMode of CacheConfig
#[derive(Clone, PartialEq, Debug, Copy)]
pub enum CacheMode {
    /// Try to cache every command.
    All,
    /// Only try to cache commands which explicitly called cache methods.
    OptIn,
}

/// Holds configuration to be used by [`crate::caching::CacheManager`]
#[derive(Clone, Debug, Copy)]
pub struct CacheConfig {
    /// `mode` changes the mode of `CacheManager`
    pub(crate) mode: CacheMode,
    /// `cache_size` defines size of [`crate::caching::cache_manager::LRUCacheShard`].
    pub(crate) cache_size: NonZeroUsize,
    /// `default_client_ttl` is the maximum time for holding redis item in the cache, if it's bigger than server side TTL, server side TTL will be used instead .
    pub(crate) default_client_ttl: Duration,
}

impl CacheConfig {
    /// Creates new CacheConfig with default values (10_000 LRU size and 30 minutes of client ttl)
    pub fn new() -> Self {
        Self {
            mode: CacheMode::All,
            cache_size: NonZeroUsize::new(10_000).unwrap(),
            default_client_ttl: Duration::from_secs(60 * 30),
        }
    }

    /// Sets the cache mode
    pub fn set_mode(mut self, mode: CacheMode) -> Self {
        self.mode = mode;
        self
    }

    /// Sets the total LRU size
    pub fn set_size(mut self, size: NonZeroUsize) -> Self {
        self.cache_size = size;
        self
    }

    /// Sets the client side ttl
    pub fn set_default_client_ttl(mut self, ttl: Duration) -> Self {
        self.default_client_ttl = ttl;
        self
    }
}

impl Default for CacheConfig {
    /// Creates new CacheConfig with default values (10_000 LRU size and 30 minutes of client ttl)
    fn default() -> Self {
        CacheConfig::new()
    }
}
