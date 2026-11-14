//! Drives the client-side cache without a server, for the cache microbenchmarks.
//!
//! This is not part of the public API: it is only exported, `doc(hidden)`, so that the
//! `bench_cache_manager` harness can reach the crate-private cache.

use super::{CacheConfig, CacheManager, CacheStatistics, PrepCacheItem};
use crate::{Cmd, Pipeline, PushKind, RedisResult, Value};

/// Wraps a [`CacheManager`] and answers its requests with canned replies.
#[doc(hidden)]
pub struct CacheBenchHarness {
    cache_manager: CacheManager,
}

impl CacheBenchHarness {
    /// Creates a harness around an empty cache.
    pub fn new(config: CacheConfig) -> Self {
        Self {
            cache_manager: CacheManager::new(config),
        }
    }

    /// Sends `cmd` through the cache the way `MultiplexedConnection::send_packed_command` does.
    ///
    /// On a hit, the cached value is returned. On a miss, the request is packed and encoded as it
    /// would be for the server, then resolved with `-1` for every `PTTL`, `OK` for
    /// `CLIENT CACHING YES` and `server_reply` for the command itself, which stores it in the cache.
    /// `server_reply` must fit the command that is sent to the server: for `MGET`, that is one
    /// element for every key that wasn't cached.
    pub fn query(&self, cmd: &Cmd, server_reply: &Value) -> RedisResult<Value> {
        match self
            .cache_manager
            .get_cached_cmd(crate::cmd::CmdRef::from_cmd(cmd))
        {
            PrepCacheItem::Cached(value) => Ok(value),
            PrepCacheItem::NotCached(cacheable_command) => {
                let mut pipeline = Pipeline::new();
                cacheable_command.pack_command(&self.cache_manager, &mut pipeline);
                std::hint::black_box(pipeline.get_packed_pipeline());
                let replies = pipeline
                    .cmd_iter()
                    .map(|sent| match sent.arg_idx(0) {
                        Some(b"PTTL") => Value::Int(-1),
                        Some(b"CLIENT") => Value::Okay,
                        _ => server_reply.clone(),
                    })
                    .collect::<Vec<_>>();
                cacheable_command.resolve(&self.cache_manager, replies.into_iter())
            }
            PrepCacheItem::NotCacheable => Ok(server_reply.clone()),
        }
    }

    /// Applies a server invalidation message for `keys`.
    pub fn invalidate(&self, keys: &[&[u8]]) {
        let keys = keys
            .iter()
            .map(|key| Value::BulkString(key.to_vec()))
            .collect();
        self.cache_manager
            .handle_push_value(&PushKind::Invalidate, &[Value::Array(keys)]);
    }

    /// Applies the invalidation message the server sends after `FLUSHALL` / `FLUSHDB`.
    pub fn invalidate_all(&self) {
        self.cache_manager
            .handle_push_value(&PushKind::Invalidate, &[Value::Nil]);
    }

    /// Returns the cache's hit, miss and invalidation counters.
    pub fn statistics(&self) -> CacheStatistics {
        self.cache_manager.statistics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_harness_caches_and_invalidates() {
        let harness = CacheBenchHarness::new(CacheConfig::new());
        let get = crate::cmd("GET").arg("key").take();

        assert_eq!(harness.query(&get, &Value::Int(1)).unwrap(), Value::Int(1));
        assert_eq!(harness.query(&get, &Value::Int(2)).unwrap(), Value::Int(1));
        harness.invalidate(&[b"key"]);
        assert_eq!(harness.query(&get, &Value::Int(3)).unwrap(), Value::Int(3));
        harness.invalidate_all();
        assert_eq!(harness.query(&get, &Value::Int(4)).unwrap(), Value::Int(4));

        let statistics = harness.statistics();
        assert_eq!(statistics.hit, 1);
        assert_eq!(statistics.miss, 3);
    }

    #[test]
    fn test_harness_resolves_mget() {
        let harness = CacheBenchHarness::new(CacheConfig::new());
        let mget = crate::cmd("MGET").arg("a").arg("b").take();
        let reply = Value::Array(vec![Value::Int(1), Value::Int(2)]);

        assert_eq!(harness.query(&mget, &reply).unwrap(), reply);
        assert_eq!(harness.query(&mget, &Value::Nil).unwrap(), reply);
    }

    #[test]
    fn test_harness_opt_in_mode() {
        let harness =
            CacheBenchHarness::new(CacheConfig::new().set_mode(super::super::CacheMode::OptIn));
        let mut get = crate::cmd("GET").arg("key").take();
        get.set_cache_config(crate::CommandCacheConfig::new());

        assert_eq!(harness.query(&get, &Value::Int(1)).unwrap(), Value::Int(1));
        assert_eq!(harness.query(&get, &Value::Int(2)).unwrap(), Value::Int(1));
    }
}
