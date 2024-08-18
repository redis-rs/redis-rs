//! This module provides Server-assisted client-side caching in Redis.
//! https://redis.io/docs/manual/client-side-caching/

mod statistics;

mod cache_manager;
mod config;

pub(crate) use cache_manager::CacheManager;
pub use config::{CacheConfig, CacheMode};
pub use statistics::{get_global_statistics, reset_global_statistics, CacheStatistics};
