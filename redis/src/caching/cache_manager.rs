use crate::caching::cmd::{CommandCacheInformationByRef, MultipleCommandCacheInformation};
use crate::caching::statistics::Statistics;
use crate::caching::{CacheConfig, CacheMode, CacheStatistics};
use crate::cmd::Cmd;
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
    // Receiver is used for completion when two different command queries same key, command pair.
    receiver: Receiver<Value>,
}

// CacheItem keeps information about a key's expiry time and cached response for each key, command pair.
struct CacheItem {
    expire_time: Instant,
    value_list: Vec<CacheCmdEntry>,
}

impl CacheItem {
    pub fn handle_server_side_ttl(&mut self, ss_ttl_value: &Value) -> RedisResult<()> {
        let pttl: i64 = crate::FromRedisValue::from_redis_value(ss_ttl_value)?;
        if pttl >= 0 {
            let server_side_expire_time = Instant::now().add(Duration::from_millis(pttl as u64));
            if self.expire_time > server_side_expire_time {
                self.expire_time = server_side_expire_time;
            }
        }
        Ok(())
    }
}

struct ShardedLRU {
    shards: Vec<std::sync::Mutex<LRUCacheShard>>,
}

impl ShardedLRU {
    const MAX_SHARD_COUNT: usize = 32;

    fn new(total_key_size: NonZeroUsize) -> Self {
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
        ShardedLRU { shards }
    }

    fn get_shard(&self, key: &[u8]) -> &std::sync::Mutex<LRUCacheShard> {
        let mut s = DefaultHasher::new();
        s.write(key);
        &self.shards[s.finish() as usize % self.shards.len()]
    }
}

pub(crate) enum GetWithGuardResponse {
    /// Value exists in cache and usable.
    Ok(Value),
    /// Value doesn't exists in cache or not usable and it won't be written back to cache.
    None,
    /// Value doesn't exists in cache or not usable but it can be written back to cache with guard.
    Guard(Sender<Value>),
}

enum InternalGuardResponse {
    GuardReceiver(Receiver<Value>),
    Ok(GetWithGuardResponse),
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

    pub(crate) async fn get_with_guard<'a>(
        &self,
        cache_information: &CommandCacheInformationByRef<'a>,
    ) -> GetWithGuardResponse {
        let internal_response = {
            let mut lru_cache = self
                .lru
                .get_shard(cache_information.redis_key)
                .lock()
                .unwrap();
            if let Some(cache_item) = lru_cache.get_mut(cache_information.redis_key) {
                if Instant::now() > cache_item.expire_time {
                    // Key is expired.
                    // It's a cold path, it could be optimized with guard in return of more complex code.
                    self.statistics
                        .increase_invalidate(cache_item.value_list.len());
                    lru_cache.pop(cache_information.redis_key);
                    return GetWithGuardResponse::None;
                };
                self.get_with_guard_existing_key(cache_item, cache_information)
            } else {
                self.get_with_guard_non_existing_key(&mut lru_cache, cache_information)
            }
        };
        match internal_response {
            InternalGuardResponse::Ok(resp) => resp,
            InternalGuardResponse::GuardReceiver(mut receiver) => {
                if receiver.changed().await.is_ok() {
                    self.statistics.increase_hit(1);
                    return GetWithGuardResponse::Ok(receiver.borrow().clone());
                }
                // Sender was dropped.
                // Now function will return None and response of request won't be filled back to the cache.
                // This can be improved in the future.
                self.statistics.increase_miss(1);
                GetWithGuardResponse::None
            }
        }
    }

    fn get_with_guard_existing_key(
        &self,
        cache_item: &mut CacheItem,
        cache_information: &CommandCacheInformationByRef,
    ) -> InternalGuardResponse {
        // Found redis key in cache, but KEY,CMD combination also must be in the cache otherwise, it will be fetched from server.
        let mut receiver: Option<Receiver<Value>> = None;
        for entry in &cache_item.value_list {
            if entry.cmd == cache_information.cmd {
                if let Some(val) = entry.value.as_ref() {
                    self.statistics.increase_hit(1);
                    let v = val.clone();
                    return InternalGuardResponse::Ok(GetWithGuardResponse::Ok(v));
                } else {
                    // Value is not ready yet, request will wait with this receiver.
                    receiver = Some(entry.receiver.clone());
                    break;
                }
            }
        }
        match receiver {
            Some(receiver) => InternalGuardResponse::GuardReceiver(receiver),
            None => {
                // There was no entry about this specific command, now it will be created, and sender guard will be given to the caller.
                self.statistics.increase_miss(1);
                let (tx, rx) = channel(Value::Nil);
                cache_item.value_list.push(CacheCmdEntry {
                    cmd: cache_information.cmd.to_vec(),
                    value: None,
                    receiver: rx,
                });
                InternalGuardResponse::Ok(GetWithGuardResponse::Guard(tx))
            }
        }
    }

    fn get_with_guard_non_existing_key(
        &self,
        lru_cache: &mut LRUCacheShard,
        cache_information: &CommandCacheInformationByRef,
    ) -> InternalGuardResponse {
        self.statistics.increase_miss(1);
        let (tx, rx) = channel(Value::Nil);
        // Putting guard in cache and marking the `RedisKey`, caller will fill the key using the guard.
        let cache_item = CacheItem {
            expire_time: get_min_expire_time(
                cache_information.client_side_ttl,
                self.cache_config.default_client_ttl,
            ), // This value will be compared with server side expire time.
            value_list: vec![CacheCmdEntry {
                cmd: cache_information.cmd.to_vec(),
                value: None,
                receiver: rx,
            }],
        };
        // Ignoring the return value, because at this point there must be no value with the key
        let _ = lru_cache.push(cache_information.redis_key.to_vec(), cache_item);
        InternalGuardResponse::Ok(GetWithGuardResponse::Guard(tx))
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
        cache_information: &MultipleCommandCacheInformation,
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
            // It will change expire time of the key
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
fn get_min_expire_time(p_ttl: Option<Duration>, default_client_ttl: Duration) -> Instant {
    let ttl_duration = if let Some(p_ttl) = p_ttl {
        min(p_ttl, default_client_ttl)
    } else {
        default_client_ttl
    };
    Instant::now().add(ttl_duration)
}
