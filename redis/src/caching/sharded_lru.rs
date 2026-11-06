use crate::Value;
use crate::caching::statistics::Statistics;
use crate::cmd::{Arg, CmdRef};
use crate::commands::KeyPositions;
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

/// The keys that a command combines into one reply, sorted and without duplicates.
///
/// This can only be used to tell when a cached reply goes stale, such as which replies an
/// invalidated key affects. It must not be used to find a cached reply, because it loses the order
/// of the keys and some commands depend on it: `SDIFF a b` and `SDIFF b a` have different replies.
/// Replies are found by the whole command.
pub(crate) struct InvalidationKeys(Vec<RedisKey>);

impl InvalidationKeys {
    /// Returns the keys of `cmd`, or `None` if it doesn't combine keys or they aren't at
    /// `key_positions`.
    pub(crate) fn new(cmd: CmdRef<'_>, key_positions: KeyPositions) -> Option<Self> {
        let mut args = cmd.args_iter().skip(1); // Skip the command name
        let key_count = match key_positions {
            // `GET key` has one key, and `MGET key1 key2` caches the value of each key on its own.
            KeyPositions::First | KeyPositions::PerKey => return None,
            KeyPositions::FirstTwo => 2,
            KeyPositions::All => args.len(),
            KeyPositions::AfterKeyCount => match args.next()? {
                Arg::Simple(key_count) => std::str::from_utf8(key_count).ok()?.parse().ok()?,
                Arg::Cursor => return None,
            },
        };
        if key_count == 0 || key_count > args.len() {
            return None;
        }
        let mut keys = args
            .take(key_count)
            .map(|arg| match arg {
                Arg::Simple(key) => Some(key.to_vec()),
                Arg::Cursor => None,
            })
            .collect::<Option<Vec<_>>>()?;
        keys.sort_unstable();
        keys.dedup();
        Some(Self(keys))
    }

    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        self.0
            .binary_search_by(|candidate| candidate.as_slice().cmp(key))
            .is_ok()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.0.iter().map(Vec::as_slice)
    }
}

pub(crate) struct CacheCmdEntry {
    cmd: RedisCmd,
    cmd_args: RedisCmdArgs,
    value: Value,
}

/// CacheItem keeps information about a key's expiry time and cached response for each key, command pair.
pub(crate) struct CacheItem {
    expire_time: Instant,
    epoch: usize,
    value_list: Vec<CacheCmdEntry>,
}

pub(crate) struct LRUCacheShard {
    lru: LruCache<RedisKey, CacheItem>,
    // Number of invalidations applied to this shard.
    invalidations: usize,
}

/// The cached reply of a command that combines several keys, e.g. `SINTER key1 key2`.
struct CombinedReply {
    keys: InvalidationKeys,
    // In an `Arc`, so that a hit can copy the reply after releasing the lock.
    value: Arc<Value>,
    expire_time: Instant,
    epoch: usize,
}

/// The replies of commands that combine several keys. Each one is stored once, with its own expiry,
/// so it takes one slot no matter how many keys it reads, and it doesn't affect the entries of those
/// keys in the shards.
struct CombinedReplies {
    // By packed command, which keeps the order of the keys and the boundaries of the arguments.
    lru: LruCache<Vec<u8>, CombinedReply>,
    // Number of invalidations of any key applied to these replies.
    invalidations: usize,
}

impl CombinedReplies {
    /// Evicts the replies that read `key`, and returns how many there were.
    fn invalidate(&mut self, key: &[u8]) -> usize {
        self.invalidations = self.invalidations.wrapping_add(1);
        let stale: Vec<_> = self
            .lru
            .iter()
            .filter(|(_, reply)| reply.keys.contains(key))
            .map(|(packed_cmd, _)| packed_cmd.clone())
            .collect();
        for packed_cmd in &stale {
            self.lru.pop(packed_cmd);
        }
        stale.len()
    }
}

pub(crate) struct ShardedLRU {
    shards: Vec<std::sync::Mutex<LRUCacheShard>>,
    combined_replies: std::sync::Mutex<CombinedReplies>,
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
        let combined_replies = std::sync::Mutex::new(CombinedReplies {
            lru: LruCache::new(NonZeroUsize::new(shard_size).unwrap()),
            invalidations: 0,
        });
        let statistics = Arc::new(Statistics::default());
        Self {
            shards,
            combined_replies,
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
                lru_cache.pop(redis_key);
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
        let mut shard = self.get_shard(redis_key);
        // The invalidation for a write made while the command was in flight can be received before
        // its reply is inserted, in which case the reply is already stale and must not be cached.
        if shard.invalidations != invalidation_count {
            return;
        }
        let lru_cache = &mut shard.lru;
        if let Some(ch) = lru_cache.peek_mut(redis_key)
            && ch.epoch == epoch
        {
            for entry in &mut ch.value_list {
                if entry.cmd == cmd_key && cmd_args.matches(&entry.cmd_args) {
                    entry.value = value;
                    ch.expire_time = expire_time;
                    return;
                }
            }
            ch.value_list.push(CacheCmdEntry {
                cmd: cmd_key.to_vec(),
                cmd_args: cmd_args.relative().collect(),
                value,
            });
            ch.expire_time = expire_time;
            return;
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
    }

    /// Same as `get_shard`, for the replies of commands that combine several keys.
    fn get_combined_replies(&self) -> std::sync::MutexGuard<'_, CombinedReplies> {
        self.combined_replies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Returns the number of invalidations applied so far to the replies of commands that combine
    /// several keys. It must be read before the command is sent and passed to `insert_combined`
    /// along with the command's reply.
    pub(crate) fn combined_invalidation_count(&self) -> usize {
        self.get_combined_replies().invalidations
    }

    pub(crate) fn get_combined(&self, packed_cmd: &[u8], epoch: usize) -> Option<Value> {
        let value = {
            let mut replies = self.get_combined_replies();
            match replies.lru.get(packed_cmd) {
                Some(reply) if reply.epoch == epoch && Instant::now() <= reply.expire_time => {
                    Some(Arc::clone(&reply.value))
                }
                Some(_) => {
                    replies.lru.pop(packed_cmd);
                    self.statistics.increase_invalidate(1);
                    None
                }
                None => None,
            }
        };
        if value.is_some() {
            self.statistics.increase_hit(1);
        } else {
            self.statistics.increase_miss(1);
        }
        value.map(|value| Value::clone(&value))
    }

    pub(crate) fn insert_combined(
        &self,
        packed_cmd: Vec<u8>,
        keys: InvalidationKeys,
        value: Value,
        expire_time: Instant,
        epoch: usize,
        invalidation_count: usize,
    ) {
        let value = Arc::new(value);
        let mut replies = self.get_combined_replies();
        // Like in `insert`, the reply may already be stale if an invalidation was received while
        // the command was in flight. It may also be for a key the command doesn't read, which only
        // means that the reply isn't cached this time.
        if replies.invalidations != invalidation_count {
            return;
        }
        replies.lru.push(
            packed_cmd,
            CombinedReply {
                keys,
                value,
                expire_time,
                epoch,
            },
        );
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
                .increase_invalidate(cache_holder.value_list.len());
        }
        let invalidated = self.get_combined_replies().invalidate(cache_key);
        self.statistics.increase_invalidate(invalidated);
    }

    pub(crate) fn invalidate_all(&self) {
        for shard in &self.shards {
            let mut lru_cache = shard
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            lru_cache.invalidations = lru_cache.invalidations.wrapping_add(1);
            let invalidated = lru_cache
                .lru
                .iter()
                .map(|(_, cache_item)| cache_item.value_list.len())
                .sum();
            lru_cache.lru.clear();
            drop(lru_cache);
            self.statistics.increase_invalidate(invalidated);
        }
        let mut replies = self.get_combined_replies();
        replies.invalidations = replies.invalidations.wrapping_add(1);
        let invalidated = replies.lru.len();
        replies.lru.clear();
        drop(replies);
        self.statistics.increase_invalidate(invalidated);
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
