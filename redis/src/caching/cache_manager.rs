use crate::caching::statistics::Statistics;
use crate::caching::{CacheConfig, CacheMode, CacheStatistics};
use crate::cmd::{Cmd, CommandCacheInformation, CommandCacheInformationByRef};
use crate::{PushKind, RedisResult, Value};
use lru::LruCache;
use std::cmp::min;
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

impl CacheItem {
    pub fn handle_server_side_ttl(&mut self, ss_ttl_value: &Value) -> RedisResult<()> {
        let pttl: i64 = crate::FromRedisValue::from_redis_value(ss_ttl_value)?;
        if pttl >= 0 {
            let server_side_ttl = Instant::now().add(Duration::from_millis(pttl as u64));
            if self.ttl > server_side_ttl {
                self.ttl = server_side_ttl;
            }
        }
        Ok(())
    }
}

struct ShardedLRU {
    shards: Vec<std::sync::Mutex<LRUCacheShard>>,
}

impl ShardedLRU {
    const MAX_SHARD_SIZE: usize = 32;
    fn new(total_key_size: NonZeroUsize) -> Self {
        // If total cache size is smaller than max shard size then it won't use sharding.
        let (shard_size, per_lru_size) = if total_key_size.get() >= Self::MAX_SHARD_SIZE {
            (
                Self::MAX_SHARD_SIZE,
                total_key_size.get() / Self::MAX_SHARD_SIZE,
            )
        } else {
            (1, total_key_size.get())
        };

        let mut shards = Vec::with_capacity(shard_size);
        for _ in 0..shard_size {
            let shard = LruCache::new(NonZeroUsize::new(per_lru_size).unwrap());
            shards.push(std::sync::Mutex::new(shard));
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
        CacheManager {
            statistics: Default::default(),
            lru: Arc::new(ShardedLRU::new(cache_config.cache_size)),
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
                // Ignoring the return value is because at this point there must be no value with the key
                let _ = lru_cache_better.push(
                    cache_information.redis_key.to_vec(),
                    CacheItem {
                        ttl: get_min_ttl(
                            cache_information.client_side_ttl,
                            self.cache_config.default_client_ttl,
                        ), // This value will be compared with server side TTL.
                        value_list: vec![CacheCmdEntry {
                            cmd: cache_information.cmd.to_vec(),
                            value: None,
                            receiver: rx,
                        }],
                    },
                );
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
        server_side_ttl_value: &Value,
    ) {
        self.insert_with_guard_inner(
            cache_information.redis_key,
            cache_information.cmd,
            sender,
            value,
            server_side_ttl_value,
        );
    }

    pub(crate) fn insert_with_guard(
        &self,
        cache_information: &CommandCacheInformation,
        sender: &Sender<Value>,
        value: Value,
        server_side_ttl_value: &Value,
    ) {
        self.insert_with_guard_inner(
            &cache_information.redis_key,
            &cache_information.cmd,
            sender,
            value,
            server_side_ttl_value,
        );
    }

    fn insert_with_guard_inner(
        &self,
        redis_key: &[u8],
        redis_cmd: &[u8],
        sender: &Sender<Value>,
        value: Value,
        server_side_ttl_value: &Value,
    ) {
        let mut lru_cache = self.lru.get_shard(redis_key).lock().unwrap();
        if let Some(ch) = lru_cache.peek_mut(redis_key) {
            for entry in &mut ch.value_list {
                if entry.cmd == redis_cmd {
                    entry.value = Some(value.clone());
                    // The error response of `send` means waiters has been dropped, it's okay!
                    let _ = sender.send(value);
                    break;
                }
            }
            // It will change TTL of the key
            ch.handle_server_side_ttl(server_side_ttl_value).unwrap();
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

    pub(crate) fn compute_cache_information<'a>(
        &self,
        cmd: &'a Cmd,
    ) -> Option<CommandCacheInformationByRef<'a>> {
        match self.cache_config.mode {
            CacheMode::All => cmd.compute_cache_information(),
            CacheMode::OptIn => {
                if cmd.has_opt_in_cache() {
                    cmd.compute_cache_information()
                } else {
                    None
                }
            }
        }
    }
    pub(crate) fn pack_single_command(
        &self,
        cmd: &Cmd,
        ci: &CommandCacheInformationByRef,
    ) -> (Vec<u8>, usize) {
        let mut request = vec![];
        let mut cmd_count = 1;
        if self.cache_config.mode == CacheMode::OptIn {
            Cmd::new()
                .arg("CLIENT")
                .arg("CACHING")
                .arg("YES")
                .write_packed_command(&mut request);
            cmd_count += 1;
        }
        Cmd::new()
            .arg("PTTL")
            .arg(ci.redis_key)
            .write_packed_command(&mut request);
        cmd.write_packed_command(&mut request);
        cmd_count += 1;
        (request, cmd_count)
    }

    pub(crate) fn handle_push_value(&self, kind: &PushKind, data: &[Value]) {
        if kind == &PushKind::Invalidate {
            if let Some(Value::Array(redis_key)) = data.first() {
                if let Some(redis_key) = redis_key.first() {
                    if let Ok(redis_key) = crate::FromRedisValue::from_redis_value(redis_key) {
                        self.invalidate(&redis_key)
                    }
                }
            }
        }
    }
}

#[inline]
fn get_min_ttl(p_ttl: Option<Duration>, default_client_ttl: Duration) -> Instant {
    let ttl_duration = if let Some(p_ttl) = p_ttl {
        min(p_ttl, default_client_ttl)
    } else {
        default_client_ttl
    };
    Instant::now().add(ttl_duration)
}
