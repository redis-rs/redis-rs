use crate::Value;
use crate::caching::statistics::Statistics;
use crate::cmd::Arg;
use lru::LruCache;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

type RedisCmd = Vec<u8>;
// Offsets marking the end of each argument in RedisCmd (relative to its start), so that argument boundaries
// are part of the cache key (`HMGET h a b` must not match `HMGET h ab`).
type RedisCmdArgs = Vec<Arg<usize>>;
type RedisKey = Vec<u8>;

/// Borrowed end offsets of each argument in a command key. Commands inside a pipeline keep
/// offsets into the pipeline's shared buffer, so `base` is where the command key starts in it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CmdArgs<'a> {
    offsets: &'a [Arg<usize>],
    base: usize,
}

impl<'a> CmdArgs<'a> {
    pub(crate) const fn new(offsets: &'a [Arg<usize>], base: usize) -> Self {
        CmdArgs { offsets, base }
    }

    fn relative(self) -> impl Iterator<Item = Arg<usize>> + 'a {
        self.offsets.iter().map(move |arg| match *arg {
            Arg::Simple(end) => Arg::Simple(end - self.base),
            Arg::Cursor => Arg::Cursor,
        })
    }

    fn matches(self, stored: &[Arg<usize>]) -> bool {
        stored.iter().cloned().eq(self.relative())
    }
}

pub(crate) struct CacheCmdEntry {
    cmd: RedisCmd,
    cmd_args: RedisCmdArgs,
    value: Value,
    // The other keys read by a command that reads several keys, e.g. `b` for `SINTER a b` cached
    // under `a`. Each of them lists this entry by `id` in the `dependents` of its shard.
    other_keys: Vec<RedisKey>,
    id: usize,
}

/// CacheItem keeps information about a key's expiry time and cached response for each key, command pair.
pub(crate) struct CacheItem {
    expire_time: Instant,
    epoch: usize,
    value_list: Vec<CacheCmdEntry>,
}

pub(crate) struct LRUCacheShard {
    lru: LruCache<RedisKey, CacheItem>,
    // Reverse index of the entries that read a key of this shard but are cached under another key,
    // as (key they're cached under, entry id).
    dependents: HashMap<RedisKey, Vec<(RedisKey, usize)>>,
}

pub(crate) struct ShardedLRU {
    shards: Vec<std::sync::Mutex<LRUCacheShard>>,
    // Number of invalidations applied to each shard, only changed while holding its lock. They are
    // atomics, so that an insert can check the shards of all the keys it reads under one lock.
    invalidations: Vec<AtomicUsize>,
    pub(crate) statistics: Arc<Statistics>,
    last_epoch: AtomicUsize,
    // Ids of entries with other keys start at 1, entries without them use 0.
    next_entry_id: AtomicUsize,
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
            let shard = LRUCacheShard {
                lru: LruCache::new(NonZeroUsize::new(shard_size).unwrap()),
                dependents: HashMap::new(),
            };
            shards.push(std::sync::Mutex::new(shard));
        }
        let statistics = Arc::new(Statistics::default());
        Self {
            shards,
            invalidations: std::iter::repeat_with(|| AtomicUsize::new(0))
                .take(shard_count)
                .collect(),
            statistics,
            last_epoch: AtomicUsize::new(0),
            next_entry_id: AtomicUsize::new(1),
        }
    }

    fn shard_index(&self, key: &[u8]) -> usize {
        let mut s = DefaultHasher::new();
        s.write(key);
        s.finish() as usize % self.shards.len()
    }

    /// get_shard will get MutexGuard for a shard determined by key, if lock is poisoned it'll be recovered.
    pub(crate) fn get_shard(&self, key: &[u8]) -> std::sync::MutexGuard<'_, LRUCacheShard> {
        let lock = &self.shards[self.shard_index(key)];
        lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Returns the number of invalidations applied so far to the shards of `keys`. It must be read
    /// before the command is sent and passed to `insert` along with the command's reply.
    pub(crate) fn invalidation_count<'a>(&self, keys: impl IntoIterator<Item = &'a [u8]>) -> usize {
        // Each shard is counted once, so that repeated keys don't change the count.
        // There are at most MAX_SHARD_COUNT (32) shards.
        let shards = keys
            .into_iter()
            .fold(0u64, |shards, key| shards | (1 << self.shard_index(key)));
        (0..self.shards.len())
            .filter(|index| shards & (1 << index) != 0)
            .map(|index| self.invalidations[index].load(Ordering::Relaxed))
            .fold(0, usize::wrapping_add)
    }

    /// Removes an entry from the `dependents` of its other keys. It must be called without holding
    /// a shard lock.
    fn unindex(&self, other_keys: &[RedisKey], id: usize) {
        for key in other_keys {
            let mut shard = self.get_shard(key);
            if let Some(dependents) = shard.dependents.get_mut(key) {
                dependents.retain(|(_, dependent_id)| *dependent_id != id);
                if dependents.is_empty() {
                    shard.dependents.remove(key);
                }
            }
        }
    }

    fn unindex_item(&self, cache_item: &CacheItem) {
        for entry in &cache_item.value_list {
            self.unindex(&entry.other_keys, entry.id);
        }
    }

    pub(crate) fn get<'a>(
        &self,
        redis_key: &'a [u8],
        redis_cmd: &'a [u8],
        redis_cmd_args: CmdArgs<'a>,
        epoch: usize,
    ) -> Option<Value> {
        let mut shard = self.get_shard(redis_key);
        let lru_cache = &mut shard.lru;
        if let Some(cache_item) = lru_cache.get_mut(redis_key) {
            // If one of following conditions are true, cache item is invalid and can't be trusted to use:
            // Epoch of client is not same, it means cache item is created by another redis connection.
            // Expire time of key has been passed, value could be stale.
            let cache_item_is_invalid =
                cache_item.epoch != epoch || Instant::now() > cache_item.expire_time;
            if cache_item_is_invalid {
                self.statistics
                    .increase_invalidate(cache_item.value_list.len());
                self.statistics.increase_miss(1);
                let cache_item = lru_cache.pop(redis_key);
                drop(shard);
                if let Some(cache_item) = cache_item {
                    self.unindex_item(&cache_item);
                }
                return None;
            }
            // Found redis key in cache, but KEY,CMD combination also must be in the cache otherwise, it will be fetched from server.
            for entry in &cache_item.value_list {
                if entry.cmd == redis_cmd && redis_cmd_args.matches(&entry.cmd_args) {
                    self.statistics.increase_hit(1);
                    return Some(entry.value.clone());
                }
            }
        }
        drop(shard);
        self.statistics.increase_miss(1);
        None
    }

    /// Caches the reply of a command under `redis_key`. `other_keys` are the other keys the command
    /// reads, if any: invalidating one of them evicts the entry too.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert(
        &self,
        redis_key: &[u8],
        cmd_key: &[u8],
        cmd_args: CmdArgs<'_>,
        other_keys: Vec<RedisKey>,
        value: Value,
        expire_time: Instant,
        epoch: usize,
        invalidation_count: usize,
    ) {
        let id = if other_keys.is_empty() {
            0
        } else {
            self.next_entry_id.fetch_add(1, Ordering::Relaxed)
        };
        // Index the entry before adding it, so that an invalidation of one of its other keys either
        // finds it in the index, or is counted by the check below.
        for key in &other_keys {
            let mut shard = self.get_shard(key);
            let dependents = shard.dependents.entry(key.clone()).or_default();
            dependents.push((redis_key.to_vec(), id));
        }
        let mut shard = self.get_shard(redis_key);
        // The invalidation for a write made while the command was in flight can be received before
        // its reply is inserted, in which case the reply is already stale and must not be cached.
        let keys = std::iter::once(redis_key).chain(other_keys.iter().map(Vec::as_slice));
        if self.invalidation_count(keys) != invalidation_count {
            drop(shard);
            self.unindex(&other_keys, id);
            return;
        }
        let lru_cache = &mut shard.lru;
        let mut replaced = None;
        if let Some(ch) = lru_cache.peek_mut(redis_key)
            && ch.epoch == epoch
        {
            ch.expire_time = expire_time;
            if let Some(entry) = ch
                .value_list
                .iter_mut()
                .find(|entry| entry.cmd == cmd_key && cmd_args.matches(&entry.cmd_args))
            {
                entry.value = value;
                replaced = Some((
                    std::mem::replace(&mut entry.other_keys, other_keys),
                    std::mem::replace(&mut entry.id, id),
                ));
            } else {
                ch.value_list.push(CacheCmdEntry {
                    cmd: cmd_key.to_vec(),
                    cmd_args: cmd_args.relative().collect(),
                    value,
                    other_keys,
                    id,
                });
            }
            drop(shard);
            if let Some((other_keys, id)) = replaced {
                self.unindex(&other_keys, id);
            }
            return;
        }
        let evicted = lru_cache.push(
            redis_key.to_vec(),
            CacheItem {
                expire_time,
                value_list: vec![CacheCmdEntry {
                    cmd: cmd_key.to_vec(),
                    cmd_args: cmd_args.relative().collect(),
                    value,
                    other_keys,
                    id,
                }],
                epoch,
            },
        );
        drop(shard);
        if let Some((_, cache_item)) = evicted {
            self.unindex_item(&cache_item);
        }
    }

    pub(crate) fn invalidate(&self, cache_key: &Vec<u8>) {
        // Use a separate binding for this to release the lock guard before calling increase_invalidate.
        let (maybe_cache_holder, dependents) = {
            let index = self.shard_index(cache_key);
            let mut shard = self.shards[index]
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.invalidations[index].fetch_add(1, Ordering::Relaxed);
            (shard.lru.pop(cache_key), shard.dependents.remove(cache_key))
        };
        if let Some(cache_holder) = maybe_cache_holder {
            self.statistics
                .increase_invalidate(cache_holder.value_list.len());
            self.unindex_item(&cache_holder);
        }
        // Evict the entries cached under other keys that read this key.
        for (redis_key, id) in dependents.into_iter().flatten() {
            let entry = {
                let mut shard = self.get_shard(&redis_key);
                let Some(cache_item) = shard.lru.peek_mut(&redis_key) else {
                    continue;
                };
                let Some(position) = cache_item.value_list.iter().position(|e| e.id == id) else {
                    continue;
                };
                let entry = cache_item.value_list.swap_remove(position);
                if cache_item.value_list.is_empty() {
                    shard.lru.pop(&redis_key);
                }
                entry
            };
            self.statistics.increase_invalidate(1);
            self.unindex(&entry.other_keys, entry.id);
        }
    }

    pub(crate) fn invalidate_all(&self) {
        for (index, shard) in self.shards.iter().enumerate() {
            let mut lru_cache = shard
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Replies of commands in flight during the flush must not be cached either.
            self.invalidations[index].fetch_add(1, Ordering::Relaxed);
            let invalidated = lru_cache
                .lru
                .iter()
                .map(|(_, cache_item)| cache_item.value_list.len())
                .sum();
            lru_cache.lru.clear();
            lru_cache.dependents.clear();
            drop(lru_cache);
            self.statistics.increase_invalidate(invalidated);
        }
    }

    pub(crate) fn increase_epoch(&self) -> usize {
        self.last_epoch.fetch_add(1, Ordering::Relaxed)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Add;
    use std::time::Duration;
    static CMD_KEY: &[u8] = b"test_cmd_key".as_slice();
    static CMD_KEY_2: &[u8] = b"test_cmd_key_2".as_slice();
    static CMD_ARGS: CmdArgs<'static> = CmdArgs::new(&[], 0);
    static REDIS_KEY: &[u8] = b"test_redis_key".as_slice();
    static OTHER_KEY: &[u8] = b"test_other_key".as_slice();

    impl ShardedLRU {
        fn dependents_count(&self) -> usize {
            let shards = self.shards.iter().map(|shard| shard.lock().unwrap());
            shards
                .map(|shard| shard.dependents.values().map(Vec::len).sum::<usize>())
                .sum()
        }
    }

    // Caches a command that reads both REDIS_KEY and OTHER_KEY, like `SINTER`.
    fn insert_reading_other_key(sharded_lru: &ShardedLRU, expire_time: Instant) {
        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY_2,
            CMD_ARGS,
            vec![OTHER_KEY.to_vec()],
            Value::Int(2),
            expire_time,
            0,
            sharded_lru.invalidation_count([REDIS_KEY, OTHER_KEY]),
        );
    }

    #[test]
    fn test_expire() {
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(64).unwrap());

        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            CMD_ARGS,
            Vec::new(),
            Value::Boolean(true),
            Instant::now().add(Duration::from_secs(10)),
            0,
            0,
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 0),
            None,
            "Using different cmd key must result in cache miss"
        );

        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            CMD_ARGS,
            Vec::new(),
            Value::Boolean(false),
            Instant::now().add(Duration::from_millis(5)),
            0,
            0,
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
            Some(Value::Boolean(false)),
            "Old value must be overwritten"
        );
        std::thread::sleep(Duration::from_millis(6));
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
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
            CMD_ARGS,
            Vec::new(),
            Value::Int(1),
            Instant::now().add(Duration::from_secs(10)),
            0,
            0,
        );
        // Second insert must override expire of the redis key.
        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY_2,
            CMD_ARGS,
            Vec::new(),
            Value::Int(2),
            Instant::now().add(Duration::from_millis(5)),
            0,
            0,
        );

        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
            Some(Value::Int(1))
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 0),
            Some(Value::Int(2))
        );

        std::thread::sleep(Duration::from_millis(6));
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
            None,
            "Cache must be expired"
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 0),
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
            CMD_ARGS,
            Vec::new(),
            Value::Boolean(true),
            Instant::now().add(Duration::from_secs(10)),
            0,
            0,
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 0),
            None,
            "Using different cmd key must result in cache miss"
        );

        sharded_lru.invalidate(&REDIS_KEY.to_vec());
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
            None,
            "Cache must be invalidated"
        );
    }

    #[test]
    fn test_insert_is_ignored_after_invalidation() {
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(64).unwrap());

        let invalidation_count = sharded_lru.invalidation_count([REDIS_KEY]);
        // The invalidation is received while the command is in flight, before its reply is inserted.
        sharded_lru.invalidate(&REDIS_KEY.to_vec());
        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            CMD_ARGS,
            Vec::new(),
            Value::Boolean(true),
            Instant::now().add(Duration::from_secs(10)),
            0,
            invalidation_count,
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
            None,
            "Reply must not be cached"
        );

        let invalidation_count = sharded_lru.invalidation_count([REDIS_KEY]);
        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            CMD_ARGS,
            Vec::new(),
            Value::Boolean(true),
            Instant::now().add(Duration::from_secs(10)),
            0,
            invalidation_count,
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
            Some(Value::Boolean(true))
        );
    }

    #[test]
    fn test_epoch_change() {
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(64).unwrap());

        let another_key = "foobar";

        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            CMD_ARGS,
            Vec::new(),
            Value::Boolean(true),
            Instant::now().add(Duration::from_secs(10)),
            0,
            0,
        );
        sharded_lru.insert(
            another_key.as_bytes(),
            CMD_KEY,
            CMD_ARGS,
            Vec::new(),
            Value::Boolean(true),
            Instant::now().add(Duration::from_secs(10)),
            0,
            0,
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 0),
            None,
            "Using different cmd key must result in cache miss"
        );

        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 1),
            None,
            "Cache must be invalidated"
        );
        assert_eq!(
            sharded_lru.get(another_key.as_bytes(), CMD_KEY, CMD_ARGS, 1),
            None,
            "Cache must be invalidated"
        );
    }

    #[test]
    fn test_invalidating_other_key() {
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(64).unwrap());
        let expire_time = Instant::now().add(Duration::from_secs(10));

        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            CMD_ARGS,
            Vec::new(),
            Value::Int(1),
            expire_time,
            0,
            0,
        );
        insert_reading_other_key(&sharded_lru, expire_time);
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 0),
            Some(Value::Int(2))
        );

        sharded_lru.invalidate(&OTHER_KEY.to_vec());
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 0),
            None,
            "Command reading the invalidated key must be invalidated"
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
            Some(Value::Int(1)),
            "Command not reading the invalidated key must stay"
        );
        assert_eq!(sharded_lru.dependents_count(), 0);
    }

    #[test]
    fn test_insert_is_ignored_after_invalidating_other_key() {
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(64).unwrap());
        let expire_time = Instant::now().add(Duration::from_secs(10));

        let invalidation_count = sharded_lru.invalidation_count([REDIS_KEY, OTHER_KEY]);
        sharded_lru.invalidate(&OTHER_KEY.to_vec());
        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY_2,
            CMD_ARGS,
            vec![OTHER_KEY.to_vec()],
            Value::Int(2),
            expire_time,
            0,
            invalidation_count,
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 0),
            None,
            "Reply must not be cached"
        );
        assert_eq!(sharded_lru.dependents_count(), 0);

        let invalidation_count = sharded_lru.invalidation_count([REDIS_KEY]);
        // FLUSHALL is received while the command is in flight.
        sharded_lru.invalidate_all();
        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            CMD_ARGS,
            Vec::new(),
            Value::Int(1),
            expire_time,
            0,
            invalidation_count,
        );
        assert_eq!(
            sharded_lru.get(REDIS_KEY, CMD_KEY, CMD_ARGS, 0),
            None,
            "Reply must not be cached"
        );
    }

    #[test]
    fn test_dependents_are_removed_with_their_entries() {
        // A single shard holding a single key.
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(1).unwrap());
        let expire_time = Instant::now().add(Duration::from_secs(10));

        insert_reading_other_key(&sharded_lru, expire_time);
        insert_reading_other_key(&sharded_lru, expire_time);
        assert_eq!(sharded_lru.dependents_count(), 1, "Replaced entry");

        sharded_lru.insert(
            OTHER_KEY,
            CMD_KEY,
            CMD_ARGS,
            Vec::new(),
            Value::Int(1),
            expire_time,
            0,
            sharded_lru.invalidation_count([OTHER_KEY]),
        );
        assert_eq!(sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 0), None);
        assert_eq!(sharded_lru.dependents_count(), 0, "Evicted entry");

        insert_reading_other_key(&sharded_lru, expire_time);
        sharded_lru.invalidate(&REDIS_KEY.to_vec());
        assert_eq!(sharded_lru.dependents_count(), 0, "Invalidated entry");

        insert_reading_other_key(&sharded_lru, Instant::now());
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 0), None);
        assert_eq!(sharded_lru.dependents_count(), 0, "Expired entry");

        insert_reading_other_key(&sharded_lru, expire_time);
        assert_eq!(sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 1), None);
        assert_eq!(sharded_lru.dependents_count(), 0, "Entry of another epoch");

        insert_reading_other_key(&sharded_lru, expire_time);
        sharded_lru.invalidate_all();
        assert_eq!(sharded_lru.get(REDIS_KEY, CMD_KEY_2, CMD_ARGS, 0), None);
        assert_eq!(sharded_lru.dependents_count(), 0, "Flushed entry");
    }
}
