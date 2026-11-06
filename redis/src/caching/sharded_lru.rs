use crate::Value;
use crate::caching::statistics::Statistics;
use crate::cmd::Arg;
use lru::LruCache;
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
    /// The cached reply. `None` marks a dependent entry: the reply of a command that reads
    /// several keys is kept under the first key it reads, and a dependent entry is kept under
    /// each of the other keys, so that the reply stops being served once any of them is
    /// invalidated.
    value: Option<Value>,
}

/// CacheItem keeps information about a key's expiry time and cached response for each key, command pair.
pub(crate) struct CacheItem {
    expire_time: Instant,
    epoch: usize,
    value_list: Vec<CacheCmdEntry>,
}

impl CacheItem {
    /// The number of replies cached in the item, not counting dependent entries.
    fn reply_count(&self) -> usize {
        self.value_list
            .iter()
            .filter(|entry| entry.value.is_some())
            .count()
    }
}

pub(crate) struct LRUCacheShard {
    lru: LruCache<RedisKey, CacheItem>,
    // Number of invalidations applied to this shard.
    invalidations: usize,
}

pub(crate) struct ShardedLRU {
    shards: Vec<std::sync::Mutex<LRUCacheShard>>,
    pub(crate) statistics: Arc<Statistics>,
    last_epoch: AtomicUsize,
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
                invalidations: 0,
            };
            shards.push(std::sync::Mutex::new(shard));
        }
        let statistics = Arc::new(Statistics::default());
        Self {
            shards,
            statistics,
            last_epoch: AtomicUsize::new(0),
        }
    }

    /// get_shard will get MutexGuard for a shard determined by key, if lock is poisoned it'll be recovered.
    pub(crate) fn get_shard(&self, key: &[u8]) -> std::sync::MutexGuard<'_, LRUCacheShard> {
        let mut s = DefaultHasher::new();
        s.write(key);
        let lock = &self.shards[s.finish() as usize % self.shards.len()];
        lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Returns the number of invalidations applied so far to the shard of `key`. It must be read
    /// before the command is sent and passed to `insert` along with the command's reply.
    pub(crate) fn invalidation_count(&self, key: &[u8]) -> usize {
        self.get_shard(key).invalidations
    }

    pub(crate) fn get<'a>(
        &self,
        redis_key: &'a [u8],
        redis_cmd: &'a [u8],
        redis_cmd_args: CmdArgs<'a>,
        epoch: usize,
    ) -> Option<Value> {
        let value = self
            .find(redis_key, redis_cmd, redis_cmd_args, epoch)
            .flatten();
        self.count_lookup(value.is_some());
        value
    }

    /// Returns the reply cached for a command that reads several keys. The reply is kept under
    /// `keys[0]` and a dependent entry under each of the other keys, and it is only served while
    /// every one of them is still cached, so an invalidation of any of the keys is a miss.
    pub(crate) fn get_multi<'a>(
        &self,
        keys: &[&'a [u8]],
        redis_cmd: &'a [u8],
        redis_cmd_args: CmdArgs<'a>,
        epoch: usize,
    ) -> Option<Value> {
        let value = keys.split_first().and_then(|(redis_key, dependent_keys)| {
            let dependents_cached = dependent_keys
                .iter()
                .all(|key| self.find(key, redis_cmd, redis_cmd_args, epoch).is_some());
            if dependents_cached {
                self.find(redis_key, redis_cmd, redis_cmd_args, epoch)
                    .flatten()
            } else {
                None
            }
        });
        self.count_lookup(value.is_some());
        value
    }

    fn count_lookup(&self, hit: bool) {
        if hit {
            self.statistics.increase_hit(1);
        } else {
            self.statistics.increase_miss(1);
        }
    }

    /// Looks `redis_cmd` up under `redis_key`: `None` when it isn't cached, `Some(None)` when
    /// the key holds a dependent entry for it, and `Some(reply)` otherwise.
    fn find<'a>(
        &self,
        redis_key: &'a [u8],
        redis_cmd: &'a [u8],
        redis_cmd_args: CmdArgs<'a>,
        epoch: usize,
    ) -> Option<Option<Value>> {
        let mut shard = self.get_shard(redis_key);
        let lru_cache = &mut shard.lru;
        let cache_item = lru_cache.get_mut(redis_key)?;
        // If one of following conditions are true, cache item is invalid and can't be trusted to use:
        // Epoch of client is not same, it means cache item is created by another redis connection.
        // Expire time of key has been passed, value could be stale.
        let cache_item_is_invalid =
            cache_item.epoch != epoch || Instant::now() > cache_item.expire_time;
        if cache_item_is_invalid {
            let invalidated = cache_item.reply_count();
            lru_cache.pop(redis_key);
            drop(shard);
            self.statistics.increase_invalidate(invalidated);
            return None;
        }
        // Found redis key in cache, but KEY,CMD combination also must be in the cache otherwise, it will be fetched from server.
        cache_item
            .value_list
            .iter()
            .find(|entry| entry.cmd == redis_cmd && redis_cmd_args.matches(&entry.cmd_args))
            .map(|entry| entry.value.clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert(
        &self,
        redis_key: &[u8],
        cmd_key: &[u8],
        cmd_args: CmdArgs<'_>,
        value: Value,
        expire_time: Instant,
        epoch: usize,
        invalidation_count: usize,
    ) {
        self.insert_entry(
            redis_key,
            cmd_key,
            cmd_args,
            Some(value),
            expire_time,
            epoch,
            invalidation_count,
        );
    }

    /// Caches the reply of a command that reads several keys: under `keys[0]` with the reply,
    /// and under each of the other keys as a dependent entry, which is what makes `get_multi`
    /// stop serving the reply once any of the keys is invalidated. `invalidation_counts` holds
    /// the count read for each key before the command was sent.
    ///
    /// The dependent entries are inserted first: a reply whose dependent entry is missing is
    /// never served, while a dependent entry without its reply is harmless.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_multi(
        &self,
        keys: &[&[u8]],
        cmd_key: &[u8],
        cmd_args: CmdArgs<'_>,
        value: Value,
        expire_time: Instant,
        epoch: usize,
        invalidation_counts: &[usize],
    ) {
        let mut entries = keys.iter().zip(invalidation_counts);
        let Some((redis_key, invalidation_count)) = entries.next() else {
            return;
        };
        for (key, invalidation_count) in entries {
            if !self.insert_entry(
                key,
                cmd_key,
                cmd_args,
                None,
                expire_time,
                epoch,
                *invalidation_count,
            ) {
                return;
            }
        }
        self.insert_entry(
            redis_key,
            cmd_key,
            cmd_args,
            Some(value),
            expire_time,
            epoch,
            *invalidation_count,
        );
    }

    /// Returns `false` when the entry wasn't inserted because its key was invalidated while the
    /// command was in flight.
    #[allow(clippy::too_many_arguments)]
    fn insert_entry(
        &self,
        redis_key: &[u8],
        cmd_key: &[u8],
        cmd_args: CmdArgs<'_>,
        value: Option<Value>,
        expire_time: Instant,
        epoch: usize,
        invalidation_count: usize,
    ) -> bool {
        let mut shard = self.get_shard(redis_key);
        // The invalidation for a write made while the command was in flight can be received before
        // its reply is inserted, in which case the reply is already stale and must not be cached.
        if shard.invalidations != invalidation_count {
            return false;
        }
        let lru_cache = &mut shard.lru;
        if let Some(ch) = lru_cache.peek_mut(redis_key)
            && ch.epoch == epoch
        {
            for entry in &mut ch.value_list {
                if entry.cmd == cmd_key && cmd_args.matches(&entry.cmd_args) {
                    entry.value = value;
                    ch.expire_time = expire_time;
                    return true;
                }
            }
            ch.value_list.push(CacheCmdEntry {
                cmd: cmd_key.to_vec(),
                cmd_args: cmd_args.relative().collect(),
                value,
            });
            ch.expire_time = expire_time;
            return true;
        }
        let _ = lru_cache.push(
            redis_key.to_vec(),
            CacheItem {
                expire_time,
                value_list: vec![CacheCmdEntry {
                    cmd: cmd_key.to_vec(),
                    cmd_args: cmd_args.relative().collect(),
                    value,
                }],
                epoch,
            },
        );
        true
    }

    pub(crate) fn invalidate(&self, cache_key: &Vec<u8>) {
        // Use a separate binding for this to release the lock guard before calling increase_invalidate.
        let maybe_cache_holder = {
            let mut shard = self.get_shard(cache_key);
            shard.invalidations = shard.invalidations.wrapping_add(1);
            shard.lru.pop(cache_key)
        };
        if let Some(cache_holder) = maybe_cache_holder {
            self.statistics
                .increase_invalidate(cache_holder.reply_count());
        }
    }

    pub(crate) fn invalidate_all(&self) {
        for shard in &self.shards {
            let mut lru_cache = shard
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let invalidated = lru_cache
                .lru
                .iter()
                .map(|(_, cache_item)| cache_item.reply_count())
                .sum();
            lru_cache.lru.clear();
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

    #[test]
    fn test_multi_key_reply_is_served_only_while_every_key_is_cached() {
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(64).unwrap());
        let keys: [&[u8]; 2] = [b"test_redis_key", b"test_redis_key_2"];
        let expire = Instant::now().add(Duration::from_secs(10));
        let invalidation_counts = |lru: &ShardedLRU| keys.map(|key| lru.invalidation_count(key));

        sharded_lru.insert_multi(
            &keys,
            CMD_KEY,
            CMD_ARGS,
            Value::Int(1),
            expire,
            0,
            &invalidation_counts(&sharded_lru),
        );
        assert_eq!(
            sharded_lru.get_multi(&keys, CMD_KEY, CMD_ARGS, 0),
            Some(Value::Int(1))
        );
        assert_eq!(
            sharded_lru.get(keys[1], CMD_KEY, CMD_ARGS, 0),
            None,
            "The dependent entry must not be served as a reply of its own"
        );

        let counts_before_invalidation = invalidation_counts(&sharded_lru);
        sharded_lru.invalidate(&keys[1].to_vec());
        assert_eq!(
            sharded_lru.get_multi(&keys, CMD_KEY, CMD_ARGS, 0),
            None,
            "Invalidating a dependent key must evict the reply"
        );

        // A key invalidated while the command was in flight keeps its reply out of the cache.
        sharded_lru.insert_multi(
            &keys,
            CMD_KEY,
            CMD_ARGS,
            Value::Int(2),
            expire,
            0,
            &counts_before_invalidation,
        );
        assert_eq!(sharded_lru.get_multi(&keys, CMD_KEY, CMD_ARGS, 0), None);

        sharded_lru.insert_multi(
            &keys,
            CMD_KEY,
            CMD_ARGS,
            Value::Int(2),
            expire,
            0,
            &invalidation_counts(&sharded_lru),
        );
        assert_eq!(
            sharded_lru.get_multi(&keys, CMD_KEY, CMD_ARGS, 0),
            Some(Value::Int(2))
        );
        let statistics = crate::caching::CacheStatistics::from(sharded_lru.statistics);
        assert_eq!((statistics.hit, statistics.miss), (2, 3));
    }

    #[test]
    fn test_expire() {
        let sharded_lru = ShardedLRU::new(NonZeroUsize::new(64).unwrap());

        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            CMD_ARGS,
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

        let invalidation_count = sharded_lru.invalidation_count(REDIS_KEY);
        // The invalidation is received while the command is in flight, before its reply is inserted.
        sharded_lru.invalidate(&REDIS_KEY.to_vec());
        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            CMD_ARGS,
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

        let invalidation_count = sharded_lru.invalidation_count(REDIS_KEY);
        sharded_lru.insert(
            REDIS_KEY,
            CMD_KEY,
            CMD_ARGS,
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
            Value::Boolean(true),
            Instant::now().add(Duration::from_secs(10)),
            0,
            0,
        );
        sharded_lru.insert(
            another_key.as_bytes(),
            CMD_KEY,
            CMD_ARGS,
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
}
