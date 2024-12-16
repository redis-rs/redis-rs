use std::num::NonZeroUsize;
use std::time::Duration;

/// Defines the behavior of the cache regarding which commands should be cached.
#[derive(Clone, PartialEq, Debug, Copy)]
pub enum CacheMode {
    /// Try to cache every command. This is equivalent to calling [crate::Cmd::set_cache_config]/[crate::Pipeline::set_cache_config] on every command.
    /// Disabling individual commands for caching with this [CacheMode::All] will not work.
    All,
    /// Only try to cache commands which explicitly enabled caching.
    /// If OptIn is used [crate::Cmd::set_cache_config]/[crate::Pipeline::set_cache_config]
    /// must be used for caching to be tried for the command.
    OptIn,
}

/// Configuration for client side caching.
#[derive(Clone, Debug, Copy)]
pub struct CacheConfig {
    pub(crate) mode: CacheMode,
    pub(crate) size: NonZeroUsize,
    pub(crate) default_client_ttl: Duration,
}

impl CacheConfig {
    /// Creates new CacheConfig with default values (10_000 LRU size and 30 minutes of client ttl)
    pub fn new() -> Self {
        Self {
            mode: CacheMode::All,
            size: NonZeroUsize::new(10_000).unwrap(),
            default_client_ttl: Duration::from_secs(60 * 30),
        }
    }

    /// Sets how client side caching should work, default is [`CacheMode::All`].
    pub fn set_mode(mut self, mode: CacheMode) -> Self {
        self.mode = mode;
        self
    }

    /// Sets maximum key count for the cache.
    pub fn set_size(mut self, size: NonZeroUsize) -> Self {
        self.size = size;
        self
    }

    ///  Sets the default client side time to live (TTL) for cached values, when a TTL isn't explicitly passed to [crate::Cmd]/[crate::Pipeline].
    ///  Client side caching mechanism will compare client side TTL and server side TTL and pick minimum to retain in the cache.
    ///  Client side TTL is the maximum time for a key to stay in cache.
    ///  Server side TTL is the maximum time for a key to stay in server, it's requested using `PTTL` command for each key.
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
