use std::time::Duration;

/// Defines CacheMode of CacheConfig
#[derive(Clone, PartialEq, Debug, Copy)]
pub enum CacheMode {
    /// Don't cache
    None,
    /// Try to cache every command.
    All,
    /// Only try to cache commands which explicitly called cache methods.
    OptIn,
}

/// Holds configuration to be used by [`crate::caching::CacheManager`]
#[derive(Clone, Debug, Copy)]
pub struct CacheConfig {
    /// `mode` changes the mode of `CacheManager`
    pub mode: CacheMode,
    /// `cache_size` defines size of [`crate::caching::cache_manager::LRUCacheShard`]. If it's zero, one will be used instead.
    pub cache_size: usize,
    /// `default_client_ttl` is the maximum amount for holding redis item in the cache.
    pub default_client_ttl: Duration,
}

impl CacheConfig {
    /// Enables cache with 10_000 LRU size and 30 minutes of client ttl
    pub fn enabled() -> Self {
        CacheConfig {
            mode: CacheMode::All,
            cache_size: 10_000,
            default_client_ttl: Duration::from_secs(60 * 30),
        }
    }
    /// Disables Cache
    pub fn disabled() -> Self {
        CacheConfig {
            mode: CacheMode::None,
            cache_size: 0,
            default_client_ttl: Duration::default(),
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig::disabled()
    }
}
