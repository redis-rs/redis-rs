//! This module provides Server-assisted client-side caching in Redis.
//!
//! This implementation of Server-assisted client-side cache, only works with RESP3 connections.
//! Some Redis providers might not enable this feature (e.g. Google Cloud Memorystore).
//! Commands that are cached will be kept in memory, commands won't be deleted from memory immediately,
//! instead they will be removed by LRU or when same key is requested again.
//!
//! For more information please read <https://redis.io/docs/manual/client-side-caching/>

mod cache_manager;
pub(crate) mod cmd;
mod config;
pub(crate) mod sharded_lru;
mod statistics;

pub(crate) use cache_manager::{CacheManager, PrepareCacheResult};
pub use config::{CacheConfig, CacheMode};
pub use statistics::CacheStatistics;
