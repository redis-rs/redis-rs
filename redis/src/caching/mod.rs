//! This module provides Server-assisted client-side caching in Redis.
//!
//! This implementation of Server-assisted client-side cache, only works with RESP3 connections.
//! Some Redis providers might not enable this feature (e.g. Google Cloud Memorystore).
//! Commands that are cached will be kept in memory until client side TTL has passed or server side TTL has passed.
//! When a key's TTL has passed, it won't be deleted from memory immediately, instead it will be removed by LRU or when same key is requested again.
//!
//! For more information please read <https://redis.io/docs/manual/client-side-caching/>

mod statistics;

mod cache_manager;
pub(crate) mod cmd;
mod config;

pub(crate) use cache_manager::{CacheManager, GetWithGuardResponse};
pub use config::{CacheConfig, CacheMode};
pub use statistics::CacheStatistics;
