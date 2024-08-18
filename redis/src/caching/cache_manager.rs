use crate::caching::statistics::Statistics;
use crate::caching::{CacheConfig, CacheMode, CacheStatistics};
use crate::cmd::{CommandCacheInformation, CommandCacheInformationByRef};
use crate::Value;
use lru::LruCache;
use std::cmp::{max, min};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::num::NonZeroUsize;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch::{channel, Receiver, Sender};

type RedisCmd = Vec<u8>;
type RedisKey = Vec<u8>;
type LRUCacheShard = LruCache<RedisKey, CacheItem>;

struct CacheCmdEntry {
    cmd: RedisCmd,
    value: Option<Value>,
    receiver: Receiver<Value>,
}

struct CacheItem {
    ttl: Instant,
    value_list: Vec<CacheCmdEntry>,
}

struct ShardedLRU {
    shards: Vec<std::sync::Mutex<LRUCacheShard>>,
}

impl ShardedLRU {
    fn new(total_key_size: usize) -> Self {
        let shard_size = 32;
        let per_lru_size = total_key_size / shard_size;

        let mut shards = Vec::with_capacity(shard_size);
        for _ in 0..shard_size {
            shards.push(std::sync::Mutex::new(LruCache::new(
                NonZeroUsize::new(max(per_lru_size, 1)).expect("LRU size must be bigger than 0"),
            )));
        }
        ShardedLRU { shards }
    }
    fn get_shard(&self, key: &[u8]) -> &std::sync::Mutex<LRUCacheShard> {
        let mut s = DefaultHasher::new();
        s.write(key);
        &self.shards[s.finish() as usize % self.shards.len()]
    }
}

#[derive(Clone)]
pub(crate) struct CacheManager {
    lru: Arc<ShardedLRU>,
    statistics: Arc<Statistics>,
    pub(crate) cache_config: CacheConfig,
}

impl CacheManager {
    pub fn new(cache_config: CacheConfig) -> Self {
        let cache_size = if cache_config.mode == CacheMode::None {
            1
        } else {
            cache_config.cache_size
        };
        CacheManager {
            statistics: Default::default(),
            lru: Arc::new(ShardedLRU::new(cache_size)),
            cache_config,
        }
    }

    pub(crate) async fn get_with_guard_new<'a>(
        &self,
        cache_information: &CommandCacheInformationByRef<'a>,
    ) -> Result<Option<Value>, Sender<Value>> {
        let mut receiver = {
            let mut lru_cache_better = self
                .lru
                .get_shard(cache_information.redis_key)
                .lock()
                .unwrap();
            if let Some(ch) = lru_cache_better.get_mut(cache_information.redis_key) {
                // We have found redis key in cache, but we need to make sure redis cmd is also here.
                if Instant::now() > ch.ttl {
                    // Key is expired.
                    // It's a cold path, it could be optimized with guard in return of more complex code.
                    self.statistics.increase_invalidate(ch.value_list.len());
                    lru_cache_better.pop(cache_information.redis_key);
                    return Ok(None);
                };
                let mut receiver: Option<Receiver<Value>> = None;
                for entry in &ch.value_list {
                    if entry.cmd == cache_information.cmd {
                        if let Some(val) = entry.value.as_ref() {
                            self.statistics.increase_hit(1);
                            let v = val.clone();
                            return Ok(Some(v));
                        } else {
                            // Value is not ready yet, we will wait for it.
                            receiver = Some(entry.receiver.clone())
                        }
                    }
                }
                match receiver {
                    Some(receiver) => receiver,
                    None => {
                        self.statistics.increase_miss(1);
                        let (tx, rx) = channel(Value::Nil);
                        ch.value_list.push(CacheCmdEntry {
                            cmd: cache_information.cmd.to_vec(),
                            value: None,
                            receiver: rx,
                        });
                        return Err(tx);
                    }
                }
            } else {
                self.statistics.increase_miss(1);
                let (tx, rx) = channel(Value::Nil);
                //Here we are putting guard in cache and marking the `RedisKey`
                let old_value = lru_cache_better.push(
                    cache_information.redis_key.to_vec(),
                    CacheItem {
                        ttl: get_ttl(
                            cache_information.client_side_ttl,
                            self.cache_config.default_client_ttl,
                        ),
                        value_list: vec![CacheCmdEntry {
                            cmd: cache_information.cmd.to_vec(),
                            value: None,
                            receiver: rx,
                        }],
                    },
                );
                if let Some(old_value) = old_value {
                    self.statistics
                        .increase_invalidate(old_value.1.value_list.len());
                }
                return Err(tx);
            }
        };
        if receiver.changed().await.is_ok() {
            self.statistics.increase_hit(1);
            return Ok(Some(receiver.borrow().clone()));
        }
        // the entry we waited for is dropped...
        self.statistics.increase_miss(1);
        Ok(None)
    }
    pub(crate) fn insert_with_guard_by_ref(
        &self,
        cache_information: &CommandCacheInformationByRef,
        sender: &Sender<Value>,
        value: Value,
    ) {
        self.insert_with_guard_inner(
            cache_information.redis_key,
            cache_information.cmd,
            cache_information.client_side_ttl,
            sender,
            value,
        );
    }

    pub(crate) fn insert_with_guard(
        &self,
        cache_information: &CommandCacheInformation,
        sender: &Sender<Value>,
        value: Value,
    ) {
        self.insert_with_guard_inner(
            &cache_information.redis_key,
            &cache_information.cmd,
            cache_information.client_side_ttl,
            sender,
            value,
        );
    }
    fn insert_with_guard_inner(
        &self,
        redis_key: &[u8],
        redis_cmd: &[u8],
        client_side_ttl: Option<Duration>,
        sender: &Sender<Value>,
        value: Value,
    ) {
        let mut lru_cache = self.lru.get_shard(redis_key).lock().unwrap();
        if let Some(ch) = lru_cache.peek_mut(redis_key) {
            for entry in &mut ch.value_list {
                if entry.cmd == redis_cmd {
                    ch.ttl = get_ttl(client_side_ttl, self.cache_config.default_client_ttl);
                    entry.value = Some(value.clone());
                    // The error response of `send` means waiters has been dropped, it's okay!
                    let _ = sender.send(value);
                    return;
                }
            }
        }
    }
    pub(crate) fn invalidate(&self, cache_key: &Vec<u8>) {
        if let Some(cache_holder) = self.lru.get_shard(cache_key).lock().unwrap().pop(cache_key) {
            self.statistics
                .increase_invalidate(cache_holder.value_list.len());
        }
    }
    pub(crate) fn statistics(&self) -> CacheStatistics {
        self.statistics.clone().into()
    }
    pub(crate) fn increase_sent_command_count(&self, val: usize) {
        self.statistics.increase_sent_command_count(val)
    }
}

#[inline]
fn get_ttl(p_ttl: Option<Duration>, default_client_ttl: Duration) -> Instant {
    let ttl_duration = if let Some(p_ttl) = p_ttl {
        min(p_ttl, default_client_ttl)
    } else {
        default_client_ttl
    };
    Instant::now().add(ttl_duration)
}
