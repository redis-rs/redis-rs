use crate::caching::statistics::Statistics;
use crate::Value;
use lru::LruCache;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

type RedisCmd = Vec<u8>;
type RedisKey = Vec<u8>;

pub(crate) struct CacheCmdEntry {
    cmd: RedisCmd,
    value: Value,
}

/// CacheItem keeps information about a key's expiry time and cached response for each key, command pair.
pub(crate) struct CacheItem {
    expire_time: Instant,
    value_list: Vec<CacheCmdEntry>,
}

type LRUCacheShard = LruCache<RedisKey, CacheItem>;

pub(crate) struct ShardedLRU {
    shards: Vec<std::sync::Mutex<LRUCacheShard>>,
    pub(crate) statistics: Arc<Statistics>,
}

impl ShardedLRU {
    const MAX_SHARD_COUNT: usize = 32;

    pub(crate) fn new(total_key_size: NonZeroUsize) -> Self {
        // If total cache size is smaller than max shard size then it won't use sharding.
        let (shard_count, shard_size) = if total_key_size.get() >= Self::MAX_SHARD_COUNT {
            (
                Self::MAX_SHARD_COUNT,
                total_key_size.get() / Self::MAX_SHARD_COUNT,
            )
        } else {
            (1, total_key_size.get())
        };

        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let shard = LruCache::new(NonZeroUsize::new(shard_size).unwrap());
            shards.push(std::sync::Mutex::new(shard));
        }
        let statistics = Arc::new(Statistics::default());
        ShardedLRU { shards, statistics }
    }

    /// get_shard will get MutexGuard for a shard determined by key, if lock is poisoned it'll be recovered.
    pub(crate) fn get_shard(&self, key: &[u8]) -> std::sync::MutexGuard<LRUCacheShard> {
        let mut s = DefaultHasher::new();
        s.write(key);
        let lock = &self.shards[s.finish() as usize % self.shards.len()];
        lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn get<'a>(&self, redis_key: &'a [u8], redis_cmd: &'a [u8]) -> Option<Value> {
        let mut lru_cache = self.get_shard(redis_key);
        if let Some(cache_item) = lru_cache.get_mut(redis_key) {
            if Instant::now() > cache_item.expire_time {
                // Key is expired.
                self.statistics
                    .increase_invalidate(cache_item.value_list.len());
                self.statistics.increase_miss(1);
                lru_cache.pop(redis_key);
                return None;
            };
            // Found redis key in cache, but KEY,CMD combination also must be in the cache otherwise, it will be fetched from server.
            for entry in &cache_item.value_list {
                if entry.cmd == redis_cmd {
                    self.statistics.increase_hit(1);
                    return Some(entry.value.clone());
                }
            }
        }
        self.statistics.increase_miss(1);
        None
    }

    pub(crate) fn insert(
        &self,
        redis_key: &[u8],
        cmd_key: &[u8],
        value: Value,
        expire_time: Instant,
    ) {
        let mut lru_cache = self.get_shard(redis_key);
        if let Some(ch) = lru_cache.peek_mut(redis_key) {
            for entry in &mut ch.value_list {
                if entry.cmd == cmd_key {
                    entry.value = value;
                    ch.expire_time = expire_time;
                    return;
                }
            }
            ch.value_list.push(CacheCmdEntry {
                cmd: cmd_key.to_vec(),
                value,
            });
            ch.expire_time = expire_time;
        } else {
            let _ = lru_cache.push(
                redis_key.to_vec(),
                CacheItem {
                    expire_time,
                    value_list: vec![CacheCmdEntry {
                        cmd: cmd_key.to_vec(),
                        value,
                    }],
                },
            );
        }
    }

    pub(crate) fn invalidate(&self, cache_key: &Vec<u8>) {
        if let Some(cache_holder) = self.get_shard(cache_key).pop(cache_key) {
            self.statistics
                .increase_invalidate(cache_holder.value_list.len());
        }
    }

    #[cfg(any(test, feature = "connection-manager"))]
    pub(crate) fn invalidate_all(&self) {
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap();
            self.statistics.increase_invalidate(shard.len());
            shard.clear();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Add;
    use std::time::Duration;
    static CMD_KEY: &[u8] = b"test_cmd_key".as_slice();
    static CMD_KEY_2: &[u8] = b"test_cmd_key_2".as_slice();
    static REDIS_KEY: &[u8] = b"test_redis_key".as_slice();

    #[test]
    fn test_expire() {
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(64).unwrap());

        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            Value::Boolean(true),
            Instant::now().add(Duration::from_secs(10)),
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2),
            None,
            "Using different cmd key must result in cache miss"
        );

        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            Value::Boolean(false),
            Instant::now().add(Duration::from_millis(5)),
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY),
            Some(Value::Boolean(false)),
            "Old value must be overwritten"
        );
        std::thread::sleep(Duration::from_millis(6));
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY),
            None,
            "Cache must be expired"
        );
    }

    #[test]
    fn test_different_cmd_keys() {
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(64).unwrap());

        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            Value::Int(1),
            Instant::now().add(Duration::from_secs(10)),
        );
        // Second insert must override expire of the redis key.
        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY_2,
            Value::Int(2),
            Instant::now().add(Duration::from_millis(5)),
        );

        assert_eq!(sharded_lru.get(REDIS_KEY, CMD_KEY), Some(Value::Int(1)));
        assert_eq!(sharded_lru.get(REDIS_KEY, CMD_KEY_2), Some(Value::Int(2)));

        std::thread::sleep(Duration::from_millis(6));
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY),
            None,
            "Cache must be expired"
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2),
            None,
            "Cache must be expired"
        );
    }

    #[test]
    fn test_invalidate() {
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(64).unwrap());

        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            Value::Boolean(true),
            Instant::now().add(Duration::from_secs(10)),
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2),
            None,
            "Using different cmd key must result in cache miss"
        );

        sharded_lru.invalidate(&REDIS_KEY.to_vec());
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY),
            None,
            "Cache must be invalidated"
        );
    }

    #[test]
    fn test_invalidate_all() {
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(64).unwrap());

        let another_key = "foobar";

        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            Value::Boolean(true),
            Instant::now().add(Duration::from_secs(10)),
        );
        sharded_lru.insert(
            another_key.as_bytes(),
            CMD_KEY,
            Value::Boolean(true),
            Instant::now().add(Duration::from_secs(10)),
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2),
            None,
            "Using different cmd key must result in cache miss"
        );

        sharded_lru.invalidate_all();
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY),
            None,
            "Cache must be invalidated"
        );
        assert_eq!(
            sharded_lru.get(another_key.as_bytes(), CMD_KEY),
            None,
            "Cache must be invalidated"
        );
    }
}
