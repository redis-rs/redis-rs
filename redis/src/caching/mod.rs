//! This module provides **experimental** support for Server-assisted client-side caching in Redis.
//!
//! This implementation of Server-assisted client-side cache, only works with RESP3 connections.
//! Caution: Some RESP compatible databases might not support this feature.
//! Commands that are cached will be kept in memory, commands won't be deleted from memory immediately,
//! instead they will be removed by LRU algorithm, by redis server invalidating the key, or when their time to live (TTL) is expired and the key is requested again.
//!
//! Commands are cached in memory by their redis key and command pairs, when a redis key is removed all related command pairs are also removed.
//! TTL is for key and not per command, when TTL of redis key gets updated, all command pairs of that redis key will use the new one.
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
