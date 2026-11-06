use super::cmd::{
    CacheableCommand, CacheablePipeline, CombinedCachedCommand, MultipleCachedCommandPart,
};
use super::sharded_lru::*;
use super::{CacheConfig, CacheMode, CacheStatistics};
use crate::cmd::{Arg, CmdRef};
use crate::commands::KeyPositions;
use crate::{FromRedisValue, Pipeline, PushKind, Value};
use std::cmp::min;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) enum PrepCacheItem<'a> {
    Cached(Value),
    NotCached(CacheableCommand<'a>),
    NotCacheable,
}

pub(crate) enum CachingResult<'a> {
    Item(PrepCacheItem<'a>),
    Ignored,
}

#[derive(Clone)]
pub(crate) struct CacheManager {
    lru: Arc<ShardedLRU>,
    pub(crate) cache_config: CacheConfig,
    epoch: usize,
}

impl CacheManager {
    pub(crate) fn new(cache_config: CacheConfig) -> Self {
        let lru = Arc::new(ShardedLRU::new(cache_config.size));
        let epoch = lru.increase_epoch();
        Self {
            lru,
            cache_config,
            epoch,
        }
    }

    // Clone the CacheManager and increase epoch from LRU,
    // this will eventually remove all keys created with previous
    // CacheManager's epoch.
    #[cfg(any(feature = "connection-manager", feature = "cluster-async"))]
    pub(crate) fn clone_and_increase_epoch(&self) -> Self {
        Self {
            lru: self.lru.clone(),
            cache_config: self.cache_config,
            epoch: self.lru.increase_epoch(),
        }
    }

    pub(crate) fn get<'a>(
        &self,
        redis_key: &'a [u8],
        redis_cmd: &'a [u8],
        redis_cmd_args: CmdArgs<'a>,
    ) -> Option<Value> {
        self.lru
            .get(redis_key, redis_cmd, redis_cmd_args, self.epoch)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert(
        &self,
        redis_key: &[u8],
        cmd_key: &[u8],
        cmd_args: CmdArgs<'_>,
        value: Value,
        client_side_expire_time: Instant,
        server_side_ttl_value: &Value,
        invalidation_count: usize,
    ) {
        self.lru.insert(
            redis_key,
            cmd_key,
            cmd_args,
            value,
            min_expire_time(client_side_expire_time, server_side_ttl_value),
            self.epoch,
            invalidation_count,
        );
    }

    pub(crate) fn insert_combined(
        &self,
        packed_cmd: Vec<u8>,
        keys: InvalidationKeys,
        value: Value,
        expire_time: Instant,
        invalidation_count: usize,
    ) {
        self.lru.insert_combined(
            packed_cmd,
            keys,
            value,
            expire_time,
            self.epoch,
            invalidation_count,
        );
    }

    pub(crate) fn invalidation_count(&self, redis_key: &[u8]) -> usize {
        self.lru.invalidation_count(redis_key)
    }

    pub(crate) fn statistics(&self) -> CacheStatistics {
        self.lru.statistics.clone().into()
    }

    pub(crate) fn handle_push_value(&self, kind: &PushKind, data: &[Value]) {
        if kind != &PushKind::Invalidate {
            return;
        }
        match data.first() {
            Some(Value::Array(redis_keys)) => {
                for redis_key in redis_keys {
                    if let Ok(redis_key) = FromRedisValue::from_redis_value_ref(redis_key) {
                        self.lru.invalidate(&redis_key);
                    }
                }
            }
            // The server sends a null instead of a list of keys after FLUSHALL / FLUSHDB.
            Some(Value::Nil) => self.lru.invalidate_all(),
            _ => {}
        }
    }

    pub(crate) fn get_cached_cmd<'a>(&self, cmd: CmdRef<'a>) -> PrepCacheItem<'a> {
        match self.cache_config.mode {
            CacheMode::All => self.get_cached_cmd_inner(cmd),
            CacheMode::OptIn => {
                let has_opt_in = cmd
                    .get_cache_config()
                    .as_ref()
                    .is_some_and(|c| c.enable_cache);
                if has_opt_in {
                    self.get_cached_cmd_inner(cmd)
                } else {
                    PrepCacheItem::NotCacheable
                }
            }
        }
    }

    fn calculate_expiration_time(&self, cmd: CmdRef<'_>) -> Instant {
        let client_side_ttl = cmd
            .get_cache_config()
            .as_ref()
            .and_then(|cache| cache.client_side_ttl)
            .unwrap_or(self.cache_config.default_client_ttl);

        Instant::now().add(client_side_ttl)
    }

    /// Builds the command key of the single-key variant (e.g. `GET key` for `MGET`) in the
    /// same layout as `Cmd` keeps it: concatenated arguments plus the end offset of each one,
    /// so that an entry created by `GET` is found by `MGET` and vice versa.
    fn prepare_key_buffer(
        &self,
        buffer: &mut Vec<u8>,
        args: &mut Vec<Arg<usize>>,
        single_command_name: &[u8],
        redis_key: &[u8],
        json_path_key: Option<&[u8]>,
    ) {
        buffer.clear();
        args.clear();
        for part in [Some(single_command_name), Some(redis_key), json_path_key]
            .into_iter()
            .flatten()
        {
            buffer.extend_from_slice(part);
            args.push(Arg::Simple(buffer.len()));
        }
    }

    fn extract_simple_arguments<'a>(&self, cmd: CmdRef<'a>) -> Vec<&'a [u8]> {
        cmd.args_iter()
            .skip(1) // Skip the command name
            .filter_map(|arg| match arg {
                crate::cmd::Arg::Simple(simple_arg) => Some(simple_arg),
                _ => None,
            })
            .collect()
    }

    fn process_multi_key_arguments<'a>(
        &self,
        cmd: CmdRef<'a>,
        is_json_command: bool,
        single_command_name: &[u8],
        commands: &mut Vec<(usize, MultipleCachedCommandPart<'a>)>,
        response: &mut Vec<Value>,
        tail_args: &mut Vec<&'a [u8]>,
    ) {
        let mut arguments = self.extract_simple_arguments(cmd);
        let json_path_key = is_json_command
            .then(|| arguments.pop().inspect(|k| tail_args.push(k)))
            .flatten();
        let mut key_test_buffer: Vec<u8> = Vec::new();
        let mut key_test_args: Vec<Arg<usize>> = Vec::new();

        for (i, redis_key) in arguments.iter().enumerate() {
            self.prepare_key_buffer(
                &mut key_test_buffer,
                &mut key_test_args,
                single_command_name,
                redis_key,
                json_path_key,
            );

            match self.get(redis_key, &key_test_buffer, CmdArgs::new(&key_test_args, 0)) {
                Some(value) => response.push(value),
                None => {
                    response.push(Value::Nil);
                    commands.push((
                        i,
                        MultipleCachedCommandPart {
                            redis_key,
                            cmd_key: key_test_buffer.clone(),
                            cmd_args: key_test_args.clone(),
                            invalidation_count: self.invalidation_count(redis_key),
                        },
                    ));
                }
            }
        }
    }

    fn handle_multi_key_command<'a>(
        &self,
        cmd: CmdRef<'a>,
        command_name_str: &'a str,
        single_command_name: &[u8],
        client_side_expire: Instant,
    ) -> PrepCacheItem<'a> {
        let mut commands = Vec::new();
        let mut tail_args: Vec<&'a [u8]> = Vec::new();
        let mut response = Vec::new();

        let is_json_command = command_name_str.starts_with("JSON");

        self.process_multi_key_arguments(
            cmd,
            is_json_command,
            single_command_name,
            &mut commands,
            &mut response,
            &mut tail_args,
        );

        if commands.is_empty() {
            return PrepCacheItem::Cached(Value::Array(response));
        }

        PrepCacheItem::NotCached(CacheableCommand::Multiple {
            command_name: command_name_str,
            commands,
            response,
            client_side_expire,
            tail_args,
        })
    }

    fn handle_single_key_command<'a>(
        &self,
        cmd: CmdRef<'a>,
        client_side_expire: Instant,
    ) -> PrepCacheItem<'a> {
        let redis_key = match cmd.arg_idx(1) {
            Some(key) => key,
            None => return PrepCacheItem::NotCacheable,
        };

        // The argument offsets are part of the key, otherwise commands whose arguments
        // concatenate to the same bytes (`HMGET h a b` and `HMGET h ab`) share an entry.
        let cmd_key = cmd.data();
        let (offsets, base) = cmd.arg_offsets();
        let cmd_args = CmdArgs::new(offsets, base);

        if let Some(value) = self.get(redis_key, cmd_key, cmd_args) {
            return PrepCacheItem::Cached(value);
        }

        PrepCacheItem::NotCached(CacheableCommand::Single(
            crate::caching::cmd::SingleCachedCommand {
                redis_key,
                cmd_key,
                cmd_args,
                cmd,
                client_side_expire,
                invalidation_count: self.invalidation_count(redis_key),
            },
        ))
    }

    /// Handles a command that combines several keys into one reply, e.g. `SINTER key1 key2`. The
    /// reply is stored once for the whole command, and is evicted when any of the keys is
    /// invalidated.
    fn handle_combined_command<'a>(
        &self,
        cmd: CmdRef<'a>,
        key_positions: KeyPositions,
        client_side_expire: Instant,
    ) -> PrepCacheItem<'a> {
        let packed_cmd = cmd.get_packed_command();
        if let Some(value) = self.lru.get_combined(&packed_cmd, self.epoch) {
            return PrepCacheItem::Cached(value);
        }

        let Some(keys) = InvalidationKeys::new(cmd, key_positions) else {
            return PrepCacheItem::NotCacheable;
        };

        PrepCacheItem::NotCached(CacheableCommand::Combined(CombinedCachedCommand {
            packed_cmd,
            keys,
            cmd,
            client_side_expire,
            invalidation_count: self.lru.combined_invalidation_count(),
        }))
    }

    /// Checks if there is enough information to resolve Cmd exists in cache,
    /// if it exists then returns PrepCacheItem::Cached.
    /// If there isn't enough information in cache but Cmd is cacheable then packs enough information
    /// into CacheableCommand and returns PrepareCacheResult::NotCached.
    /// If Cmd doesn't support client side caching then it returns PrepareCacheResult::NotCacheable.
    fn get_cached_cmd_inner<'a>(&self, cmd: CmdRef<'a>) -> PrepCacheItem<'a> {
        if cmd.encoded_len() < 2 {
            return PrepCacheItem::NotCacheable;
        }

        let command_name = match cmd.arg_idx(0) {
            Some(name) => name,
            None => return PrepCacheItem::NotCacheable,
        };

        let client_side_expire = self.calculate_expiration_time(cmd);

        if let Some((command_name_str, single_command_name)) = is_multi_key(command_name) {
            return self.handle_multi_key_command(
                cmd,
                command_name_str,
                single_command_name,
                client_side_expire,
            );
        }

        match crate::commands::cacheable_key_positions(command_name) {
            Some(KeyPositions::First) => self.handle_single_key_command(cmd, client_side_expire),
            // `EXISTS key` reads a single key, just like `GET key`.
            Some(KeyPositions::All) if cmd.args_iter().len() == 2 => {
                self.handle_single_key_command(cmd, client_side_expire)
            }
            Some(key_positions) => {
                self.handle_combined_command(cmd, key_positions, client_side_expire)
            }
            None => PrepCacheItem::NotCacheable,
        }
    }

    /// Creates new Pipeline and stores enough information in CacheablePipeline
    /// to resolve it when redis responses back.
    pub(crate) fn get_cached_pipeline<'a>(
        &self,
        requested_pipeline: &'a Pipeline,
    ) -> (CacheablePipeline<'a>, Pipeline, (usize, usize)) {
        let mut commands = vec![];
        let transaction_mode = requested_pipeline.transaction_mode;
        let mut packed_pipeline = Pipeline::new();

        for cmd in requested_pipeline.cmd_iter() {
            if cmd.is_ignored() {
                commands.push(CachingResult::Ignored);
                packed_pipeline.add_command_ref(cmd);
                continue;
            }
            let cacheable_command = self.get_cached_cmd(cmd);
            match cacheable_command {
                PrepCacheItem::Cached(_) => {}
                PrepCacheItem::NotCached(ref cc) => {
                    cc.pack_command(self, &mut packed_pipeline);
                }
                PrepCacheItem::NotCacheable => {
                    // It must be added to packed_pipeline manually, since it's not packed via pack_command.
                    packed_pipeline.add_command_ref(cmd);
                }
            }
            commands.push(CachingResult::Item(cacheable_command));
        }

        let pipeline_response_counts = if transaction_mode {
            packed_pipeline.atomic();
            (packed_pipeline.len() + 1, 1)
        } else {
            (0, packed_pipeline.len())
        };
        let cp = CacheablePipeline {
            commands,
            transaction_mode,
        };
        (cp, packed_pipeline, pipeline_response_counts)
    }
}

/// Checks if the given command is cacheable and supports multiple keys.
/// If the command is cacheable, it returns a tuple containing the command name as a string
/// and the single variant of the command (e.g., "MGET" maps to "GET").
fn is_multi_key(command_name: &[u8]) -> Option<(&'static str, &'static [u8])> {
    match command_name {
        b"MGET" => Some(("MGET", b"GET")),
        b"JSON.MGET" => Some(("JSON.MGET", b"JSON.GET")),
        _ => None,
    }
}

/// Returns the earlier of `client_side_expire_time` and the server side expiry in
/// `server_side_ttl_value`.
pub(crate) fn min_expire_time(
    client_side_expire_time: Instant,
    server_side_ttl_value: &Value,
) -> Instant {
    let pttl: Result<i64, _> = FromRedisValue::from_redis_value_ref(server_side_ttl_value);
    match pttl {
        Ok(pttl) if pttl > 0 => {
            let server_side_expire_time = Instant::now().add(Duration::from_millis(pttl as u64));
            min(server_side_expire_time, client_side_expire_time)
        }
        _ => client_side_expire_time,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Cmd;
    use rstest::rstest;

    /// Resolves `cacheable_command` with `server_reply` as the server's reply to it, and `pttl` of
    /// each key as its reply to the PTTL of that key.
    fn resolve_command(
        cache_manager: &CacheManager,
        cacheable_command: CacheableCommand<'_>,
        server_reply: Value,
        pttl: impl Fn(&[u8]) -> i64,
    ) -> Value {
        let mut pipeline = Pipeline::new();
        cacheable_command.pack_command(cache_manager, &mut pipeline);
        let replies: Vec<Value> = pipeline
            .cmd_iter()
            .map(|cmd| match cmd.arg_idx(0) {
                Some(b"PTTL") => Value::Int(pttl(cmd.arg_idx(1).unwrap())),
                _ => server_reply.clone(),
            })
            .collect();
        cacheable_command
            .resolve(cache_manager, replies.into_iter())
            .unwrap()
    }

    /// Runs `cmd` through the cache, with `server_reply` as the server's reply to it and -1 (no
    /// expiry) as its reply to each PTTL.
    fn query_with_cache(cache_manager: &CacheManager, cmd: &Cmd, server_reply: Value) -> Value {
        match cache_manager.get_cached_cmd(CmdRef::from_cmd(cmd)) {
            PrepCacheItem::Cached(value) => value,
            PrepCacheItem::NotCached(cacheable_command) => {
                resolve_command(cache_manager, cacheable_command, server_reply, |_| -1)
            }
            PrepCacheItem::NotCacheable => server_reply,
        }
    }

    /// Returns the command to run `cmd`, which must not be cached yet.
    fn not_cached<'a>(cache_manager: &CacheManager, cmd: &'a Cmd) -> CacheableCommand<'a> {
        match cache_manager.get_cached_cmd(CmdRef::from_cmd(cmd)) {
            PrepCacheItem::NotCached(cacheable_command) => cacheable_command,
            _ => panic!("{cmd:?} must be cacheable and not cached yet"),
        }
    }

    fn invalidate_key(cache_manager: &CacheManager, key: &str) {
        cache_manager.handle_push_value(
            &PushKind::Invalidate,
            &[Value::Array(vec![Value::BulkString(
                key.as_bytes().to_vec(),
            )])],
        );
    }

    #[rstest]
    fn test_reply_is_evicted_when_any_key_it_reads_is_invalidated(
        #[values(
            &["EXISTS", "key_1", "key_2"][..],
            &["LCS", "key_1", "key_2"][..],
            &["SDIFF", "key_1", "key_2"][..],
            &["SINTER", "key_1", "key_2"][..],
            &["SINTERCARD", "2", "key_1", "key_2", "LIMIT", "5"][..],
            &["SUNION", "key_1", "key_2"][..],
            &["SUNION", "key_1", "key_2", "key_1"][..],
            &["ZDIFF", "2", "key_1", "key_2"][..],
            &["ZINTER", "2", "key_1", "key_2"][..],
            &["ZINTERCARD", "2", "key_1", "key_2"][..],
            &["ZUNION", "2", "key_1", "key_2", "WITHSCORES"][..],
        )]
        args: &[&str],
        #[values("key_1", "key_2")] invalidated_key: &str,
    ) {
        let cache_manager = CacheManager::new(CacheConfig::new());
        let cmd = crate::cmd(args[0]).arg(&args[1..]).take();

        assert_eq!(
            query_with_cache(&cache_manager, &cmd, Value::Int(1)),
            Value::Int(1)
        );
        assert_eq!(
            query_with_cache(&cache_manager, &cmd, Value::Int(2)),
            Value::Int(1),
            "{args:?} must be served from the cache"
        );

        invalidate_key(&cache_manager, invalidated_key);
        assert_eq!(
            query_with_cache(&cache_manager, &cmd, Value::Int(3)),
            Value::Int(3),
            "{args:?} must not be served from the cache after {invalidated_key} changed"
        );
        assert_eq!(
            query_with_cache(&cache_manager, &cmd, Value::Int(4)),
            Value::Int(3),
            "{args:?} must be served from the cache"
        );
    }

    #[rstest]
    #[case::single_key(&["GET", "key_1"], Value::Int(1))]
    #[case::combined(&["SINTER", "key_1", "key_2"], Value::Int(1))]
    #[case::per_key(
        &["MGET", "key_1", "key_2"],
        Value::Array(vec![Value::Int(1), Value::Int(1)])
    )]
    fn test_reply_is_not_cached_when_flushed_in_flight(
        #[case] args: &[&str],
        #[case] reply: Value,
    ) {
        let cache_manager = CacheManager::new(CacheConfig::new());
        let cmd = crate::cmd(args[0]).arg(&args[1..]).take();
        let cacheable_command = not_cached(&cache_manager, &cmd);

        cache_manager.handle_push_value(&PushKind::Invalidate, &[Value::Nil]);
        resolve_command(&cache_manager, cacheable_command, reply, |_| -1);

        assert!(
            matches!(
                cache_manager.get_cached_cmd(CmdRef::from_cmd(&cmd)),
                PrepCacheItem::NotCached(_)
            ),
            "{args:?} read before the flush must not be cached"
        );
    }

    #[test]
    fn test_reply_invalidated_in_flight_is_not_stored_at_all() {
        let cache_manager = CacheManager::new(CacheConfig::new());
        // Enough keys to land in several shards.
        let keys: Vec<String> = (0..64).map(|i| format!("key_{i}")).collect();
        let cmd = crate::cmd("SUNION").arg(&keys).take();
        let cacheable_command = not_cached(&cache_manager, &cmd);

        invalidate_key(&cache_manager, &keys[0]);
        resolve_command(&cache_manager, cacheable_command, Value::Int(1), |_| -1);

        // Nothing was stored, so invalidating the other keys doesn't evict anything.
        for key in &keys {
            invalidate_key(&cache_manager, key);
        }
        assert_eq!(cache_manager.statistics().invalidate, 0);
    }

    #[rstest]
    #[case::one_shard(1)]
    #[case::one_entry_per_shard(32)]
    fn test_reply_with_more_keys_than_fit_in_the_cache_is_cached_and_evicts_nothing(
        #[case] size: usize,
    ) {
        let cache_manager = CacheManager::new(
            CacheConfig::new().set_size(std::num::NonZeroUsize::new(size).unwrap()),
        );
        let keys: Vec<String> = (0..64).map(|i| format!("key_{i}")).collect();
        let exists = crate::cmd("EXISTS").arg(&keys).take();
        let get = crate::cmd("GET").arg("other_key").take();

        assert_eq!(
            query_with_cache(&cache_manager, &get, Value::Int(1)),
            Value::Int(1)
        );
        assert_eq!(
            query_with_cache(&cache_manager, &exists, Value::Int(2)),
            Value::Int(2)
        );
        assert_eq!(
            query_with_cache(&cache_manager, &exists, Value::Int(3)),
            Value::Int(2),
            "EXISTS must be served from the cache"
        );
        assert_eq!(
            query_with_cache(&cache_manager, &get, Value::Int(4)),
            Value::Int(1),
            "EXISTS must not evict GET"
        );
    }

    #[test]
    fn test_reply_and_other_commands_on_its_keys_keep_their_own_ttl() {
        let cache_manager = CacheManager::new(CacheConfig::new());
        let short_ttl =
            crate::CommandCacheConfig::new().set_client_side_ttl(Duration::from_millis(20));
        let short_get = crate::cmd("GET")
            .arg("key_1")
            .set_cache_config(short_ttl.clone())
            .take();
        let long_sunion = crate::cmd("SUNION").arg("key_1").arg("key_2").take();
        let long_get = crate::cmd("GET").arg("key_3").take();
        let short_sunion = crate::cmd("SUNION")
            .arg("key_3")
            .arg("key_4")
            .set_cache_config(short_ttl)
            .take();
        for cmd in [&short_get, &long_sunion, &long_get, &short_sunion] {
            query_with_cache(&cache_manager, cmd, Value::Int(1));
        }

        std::thread::sleep(Duration::from_millis(30));

        assert_eq!(
            query_with_cache(&cache_manager, &short_get, Value::Int(2)),
            Value::Int(2),
            "SUNION must not extend the TTL of GET"
        );
        assert_eq!(
            query_with_cache(&cache_manager, &long_sunion, Value::Int(2)),
            Value::Int(1),
            "GET must not shorten the TTL of SUNION"
        );
        assert_eq!(
            query_with_cache(&cache_manager, &long_get, Value::Int(2)),
            Value::Int(1),
            "SUNION must not shorten the TTL of GET"
        );
        assert_eq!(
            query_with_cache(&cache_manager, &short_sunion, Value::Int(2)),
            Value::Int(2),
            "GET must not extend the TTL of SUNION"
        );
    }

    #[rstest]
    fn test_reply_expires_with_the_first_of_its_keys(
        #[values("key_1", "key_2")] expiring_key: &str,
    ) {
        let cache_manager = CacheManager::new(CacheConfig::new());
        let cmd = crate::cmd("SUNION").arg("key_1").arg("key_2").take();

        let cacheable_command = not_cached(&cache_manager, &cmd);
        resolve_command(&cache_manager, cacheable_command, Value::Int(1), |key| {
            if key == expiring_key.as_bytes() {
                5
            } else {
                -1
            }
        });
        std::thread::sleep(Duration::from_millis(10));

        assert_eq!(
            query_with_cache(&cache_manager, &cmd, Value::Int(2)),
            Value::Int(2),
            "SUNION must expire with {expiring_key}"
        );
    }

    #[rstest]
    #[case::key_order(&["SDIFF", "key_1", "key_2"], &["SDIFF", "key_2", "key_1"])]
    #[case::argument_boundaries(&["SINTER", "ab", "c"], &["SINTER", "a", "bc"])]
    fn test_combined_replies_are_found_by_the_whole_command(
        #[case] first: &[&str],
        #[case] second: &[&str],
    ) {
        let cache_manager = CacheManager::new(CacheConfig::new());
        let first = crate::cmd(first[0]).arg(&first[1..]).take();
        let second = crate::cmd(second[0]).arg(&second[1..]).take();

        assert_eq!(
            query_with_cache(&cache_manager, &first, Value::Int(1)),
            Value::Int(1)
        );
        assert_eq!(
            query_with_cache(&cache_manager, &second, Value::Int(2)),
            Value::Int(2)
        );
        assert_eq!(
            query_with_cache(&cache_manager, &first, Value::Int(3)),
            Value::Int(1)
        );
        assert_eq!(
            query_with_cache(&cache_manager, &second, Value::Int(3)),
            Value::Int(2)
        );
    }

    #[test]
    fn test_command_with_one_key_is_cached_like_a_single_key_command() {
        let cache_manager = CacheManager::new(CacheConfig::new());
        let cmd = crate::cmd("EXISTS").arg("key_1").take();

        assert!(matches!(
            not_cached(&cache_manager, &cmd),
            CacheableCommand::Single(_)
        ));
        assert_eq!(
            query_with_cache(&cache_manager, &cmd, Value::Int(1)),
            Value::Int(1)
        );
        assert_eq!(
            query_with_cache(&cache_manager, &cmd, Value::Int(2)),
            Value::Int(1)
        );
        invalidate_key(&cache_manager, "key_1");
        assert_eq!(
            query_with_cache(&cache_manager, &cmd, Value::Int(3)),
            Value::Int(3)
        );
    }

    #[test]
    fn test_combined_reply_in_pipeline() {
        let cache_manager = CacheManager::new(CacheConfig::new());
        let mut pipe = Pipeline::new();
        pipe.cmd("GET")
            .arg("key_1")
            .cmd("SINTER")
            .arg("key_1")
            .arg("key_2");

        let (cacheable_pipeline, packed_pipeline, _) = cache_manager.get_cached_pipeline(&pipe);
        let replies = packed_pipeline
            .cmd_iter()
            .map(|cmd| match cmd.arg_idx(0) {
                Some(b"PTTL") => Value::Int(-1),
                Some(b"GET") => Value::Int(1),
                _ => Value::Int(2),
            })
            .collect();
        assert_eq!(
            cacheable_pipeline
                .resolve(&cache_manager, Value::Array(replies))
                .unwrap(),
            vec![Value::Int(1), Value::Int(2)]
        );

        let (cacheable_pipeline, packed_pipeline, _) = cache_manager.get_cached_pipeline(&pipe);
        assert!(packed_pipeline.is_empty());
        assert_eq!(
            cacheable_pipeline
                .resolve(&cache_manager, Value::Array(Vec::new()))
                .unwrap(),
            vec![Value::Int(1), Value::Int(2)]
        );

        // A command in a pipeline finds the same entry as the command on its own.
        let sinter = crate::cmd("SINTER").arg("key_1").arg("key_2").take();
        assert_eq!(
            query_with_cache(&cache_manager, &sinter, Value::Int(3)),
            Value::Int(2)
        );

        // `GET key_1` is still cached, and `SINTER` is sent along with the PTTL of its two keys.
        invalidate_key(&cache_manager, "key_2");
        let (_, packed_pipeline, _) = cache_manager.get_cached_pipeline(&pipe);
        assert_eq!(packed_pipeline.len(), 3);
    }

    #[test]
    fn test_combined_command_goes_first_in_opt_in_mode() {
        let cache_manager =
            CacheManager::new(CacheConfig::new().set_mode(crate::caching::CacheMode::OptIn));
        let cmd = crate::cmd("SINTER")
            .arg("key_1")
            .arg("key_2")
            .set_cache_config(crate::CommandCacheConfig::new().set_enable_cache(true))
            .take();

        let mut pipeline = Pipeline::new();
        not_cached(&cache_manager, &cmd).pack_command(&cache_manager, &mut pipeline);
        let commands: Vec<&[u8]> = pipeline
            .cmd_iter()
            .map(|cmd| cmd.arg_idx(0).unwrap())
            .collect();
        assert_eq!(
            commands,
            [&b"CLIENT"[..], &b"SINTER"[..], &b"PTTL"[..], &b"PTTL"[..]]
        );

        assert_eq!(
            query_with_cache(&cache_manager, &cmd, Value::Int(1)),
            Value::Int(1)
        );
        assert_eq!(
            query_with_cache(&cache_manager, &cmd, Value::Int(2)),
            Value::Int(1)
        );
    }

    #[test]
    fn test_combined_error_reply_is_not_cached() {
        let cache_manager = CacheManager::new(CacheConfig::new());
        let cmd = crate::cmd("SINTER").arg("key_1").arg("key_2").take();
        let cacheable_command = not_cached(&cache_manager, &cmd);
        let error = crate::parse_redis_value(b"-WRONGTYPE not a set\r\n").unwrap();
        assert!(matches!(error, Value::ServerError(_)));

        assert_eq!(
            resolve_command(&cache_manager, cacheable_command, error.clone(), |_| -1),
            error
        );
        assert!(matches!(
            cache_manager.get_cached_cmd(CmdRef::from_cmd(&cmd)),
            PrepCacheItem::NotCached(_)
        ));
    }

    #[test]
    fn test_combined_reply_statistics() {
        let cache_manager = CacheManager::new(CacheConfig::new());
        let cmd = crate::cmd("SINTER").arg("key_1").arg("key_2").take();

        query_with_cache(&cache_manager, &cmd, Value::Int(1));
        query_with_cache(&cache_manager, &cmd, Value::Int(1));
        let statistics = cache_manager.statistics();
        assert_eq!((statistics.hit, statistics.miss), (1, 1));
        assert_eq!(statistics.invalidate, 0);

        invalidate_key(&cache_manager, "key_1");
        assert_eq!(cache_manager.statistics().invalidate, 1);

        query_with_cache(&cache_manager, &cmd, Value::Int(1));
        cache_manager.handle_push_value(&PushKind::Invalidate, &[Value::Nil]);
        assert_eq!(cache_manager.statistics().invalidate, 2);
    }

    #[test]
    #[cfg(feature = "connection-manager")]
    fn test_combined_reply_is_not_served_across_epochs() {
        let shared_cache_manager = CacheManager::new(CacheConfig::new());
        let cache_manager_1 = shared_cache_manager.clone_and_increase_epoch();
        let cache_manager_2 = shared_cache_manager.clone_and_increase_epoch();
        let cmd = crate::cmd("SINTER").arg("key_1").arg("key_2").take();

        assert_eq!(
            query_with_cache(&cache_manager_1, &cmd, Value::Int(1)),
            Value::Int(1)
        );
        assert_eq!(
            query_with_cache(&cache_manager_1, &cmd, Value::Int(2)),
            Value::Int(1)
        );
        assert_eq!(
            query_with_cache(&cache_manager_2, &cmd, Value::Int(3)),
            Value::Int(3)
        );
    }

    #[rstest]
    #[case::first_two(&["LCS", "key_2", "key_1", "LEN"], Some(&["key_1", "key_2"][..]))]
    #[case::all(&["SINTER", "key_2", "key_1", "key_2"], Some(&["key_1", "key_2"][..]))]
    #[case::after_key_count(
        &["ZUNION", "2", "key_2", "key_1", "WEIGHTS", "1", "2"],
        Some(&["key_1", "key_2"][..])
    )]
    #[case::first(&["GET", "key_1"], None)]
    #[case::per_key(&["MGET", "key_1", "key_2"], None)]
    #[case::per_key_with_path(&["JSON.MGET", "key_1", "key_2", "$"], None)]
    #[case::missing_key(&["LCS", "key_1"], None)]
    #[case::fewer_keys_than_key_count(&["ZUNION", "3", "key_1", "key_2"], None)]
    #[case::zero_key_count(&["ZUNION", "0"], None)]
    #[case::invalid_key_count(&["ZUNION", "two", "key_1", "key_2"], None)]
    fn test_invalidation_keys_are_sorted_without_duplicates(
        #[case] args: &[&str],
        #[case] expected: Option<&[&str]>,
    ) {
        let cmd = crate::cmd(args[0]).arg(&args[1..]).take();
        let key_positions = crate::commands::cacheable_key_positions(args[0].as_bytes()).unwrap();
        let keys = InvalidationKeys::new(CmdRef::from_cmd(&cmd), key_positions);
        assert_eq!(
            keys.as_ref().map(|keys| keys.iter().collect::<Vec<_>>()),
            expected.map(|keys| keys.iter().map(|key| key.as_bytes()).collect())
        );

        if let Some(keys) = keys {
            for key in expected.unwrap() {
                assert!(keys.contains(key.as_bytes()), "{key}");
            }
            for key in ["key_0", "key_3", "key", "key_11"] {
                assert!(!keys.contains(key.as_bytes()), "{key}");
            }
        }
    }

    #[test]
    fn test_custom_command_ttl() {
        let redis_key = b"test_redis_key".as_slice();
        let redis_key_2 = b"test_redis_key_2".as_slice();
        let redis_key_3 = b"test_redis_key_3".as_slice();
        let cmd = crate::cmd("HGET")
            .arg("test_redis_key")
            .arg("field")
            .clone();
        let cmd = CmdRef::from_cmd(&cmd);
        let cmd_key = cmd.data();
        let (offsets, base) = cmd.arg_offsets();
        let cmd_args = CmdArgs::new(offsets, base);

        let cache_manager = CacheManager::new(CacheConfig::new());
        let secs_10 = Instant::now().add(Duration::from_secs(10));
        let ms_5 = Instant::now().add(Duration::from_millis(5));
        let ms_50 = Instant::now().add(Duration::from_millis(50));

        cache_manager.insert(
            redis_key,
            cmd_key,
            cmd_args,
            Value::Int(1),
            secs_10,
            &Value::Int(5),
            0,
        );
        cache_manager.insert(
            redis_key_2,
            cmd_key,
            cmd_args,
            Value::Int(2),
            ms_5,
            &Value::Int(500),
            0,
        );
        cache_manager.insert(
            redis_key_3,
            cmd_key,
            cmd_args,
            Value::Int(3),
            ms_50,
            &Value::Int(500),
            0,
        );

        assert_eq!(
            cache_manager.get(redis_key, cmd_key, cmd_args),
            Some(Value::Int(1))
        );
        assert_eq!(
            cache_manager.get(redis_key_2, cmd_key, cmd_args),
            Some(Value::Int(2))
        );
        assert_eq!(
            cache_manager.get(redis_key_3, cmd_key, cmd_args),
            Some(Value::Int(3))
        );

        std::thread::sleep(Duration::from_millis(6));
        assert_eq!(
            cache_manager.get(redis_key, cmd_key, cmd_args),
            None,
            "Key must be expired, server value must be picked"
        );
        assert_eq!(
            cache_manager.get(redis_key_2, cmd_key, cmd_args),
            None,
            "Key must be expired, client value must be picked"
        );
        assert_eq!(
            cache_manager.get(redis_key_3, cmd_key, cmd_args),
            Some(Value::Int(3)),
            "Key must be alive, client value must be picked"
        );
    }

    #[rstest]
    #[case::hmget(
        crate::cmd("HMGET").arg("hash").arg("a").arg("b").clone(),
        crate::cmd("HMGET").arg("hash").arg("ab").clone()
    )]
    #[case::getrange(
        crate::cmd("GETRANGE").arg("str").arg(1).arg(23).clone(),
        crate::cmd("GETRANGE").arg("str").arg(12).arg(3).clone()
    )]
    fn test_argument_boundaries_are_part_of_cmd_key(#[case] first: Cmd, #[case] second: Cmd) {
        let cache_manager = CacheManager::new(CacheConfig::new());
        let expire = Instant::now().add(Duration::from_secs(10));

        let PrepCacheItem::NotCached(CacheableCommand::Single(scc)) =
            cache_manager.get_cached_cmd(CmdRef::from_cmd(&first))
        else {
            panic!("first command must be cacheable and not cached yet");
        };
        cache_manager.insert(
            scc.redis_key,
            scc.cmd_key,
            scc.cmd_args,
            Value::Int(1),
            expire,
            &Value::Nil,
            scc.invalidation_count,
        );
        assert!(matches!(
            cache_manager.get_cached_cmd(CmdRef::from_cmd(&first)),
            PrepCacheItem::Cached(Value::Int(1))
        ));
        assert!(
            matches!(
                cache_manager.get_cached_cmd(CmdRef::from_cmd(&second)),
                PrepCacheItem::NotCached(_)
            ),
            "{second:?} must not be served from the cache entry of {first:?}"
        );
    }

    #[test]
    fn test_get_and_mget_share_cache_entries() {
        let cache_manager = CacheManager::new(CacheConfig::new());
        let expire = Instant::now().add(Duration::from_secs(10));

        let get = crate::cmd("GET").arg("key").clone();
        let PrepCacheItem::NotCached(CacheableCommand::Single(scc)) =
            cache_manager.get_cached_cmd(CmdRef::from_cmd(&get))
        else {
            panic!("GET must be cacheable and not cached yet");
        };
        cache_manager.insert(
            scc.redis_key,
            scc.cmd_key,
            scc.cmd_args,
            Value::Int(2),
            expire,
            &Value::Nil,
            scc.invalidation_count,
        );
        let mget = crate::cmd("MGET").arg("key").clone();
        assert!(matches!(
            cache_manager.get_cached_cmd(CmdRef::from_cmd(&mget)),
            PrepCacheItem::Cached(Value::Array(values)) if values == vec![Value::Int(2)]
        ));
    }

    #[test]
    fn test_invalidate_push_messages() {
        let redis_key = b"test_redis_key".as_slice();
        let redis_key_2 = b"test_redis_key_2".as_slice();
        let redis_key_3 = b"test_redis_key_3".as_slice();
        let cmd = crate::cmd("HGET")
            .arg("test_redis_key")
            .arg("field")
            .clone();
        let cmd = CmdRef::from_cmd(&cmd);
        let cmd_key = cmd.data();
        let (offsets, base) = cmd.arg_offsets();
        let cmd_args = CmdArgs::new(offsets, base);

        let cache_manager = CacheManager::new(CacheConfig::new());
        let secs_10 = Instant::now().add(Duration::from_secs(10));
        let do_inserts = || {
            for (i, key) in [redis_key, redis_key_2, redis_key_3]
                .into_iter()
                .enumerate()
            {
                cache_manager.insert(
                    key,
                    cmd_key,
                    cmd_args,
                    Value::Int(i as i64),
                    secs_10,
                    &Value::Int(-1),
                    0,
                );
            }
        };

        do_inserts();
        // A single invalidation message can carry several keys.
        cache_manager.handle_push_value(
            &PushKind::Invalidate,
            &[Value::Array(vec![
                Value::BulkString(redis_key.to_vec()),
                Value::BulkString(redis_key_2.to_vec()),
            ])],
        );
        assert_eq!(cache_manager.get(redis_key, cmd_key, cmd_args), None);
        assert_eq!(cache_manager.get(redis_key_2, cmd_key, cmd_args), None);
        assert_eq!(
            cache_manager.get(redis_key_3, cmd_key, cmd_args),
            Some(Value::Int(2))
        );

        do_inserts();
        // FLUSHALL / FLUSHDB are reported with a null instead of a list of keys.
        cache_manager.handle_push_value(&PushKind::Invalidate, &[Value::Nil]);
        assert_eq!(cache_manager.get(redis_key, cmd_key, cmd_args), None);
        assert_eq!(cache_manager.get(redis_key_2, cmd_key, cmd_args), None);
        assert_eq!(cache_manager.get(redis_key_3, cmd_key, cmd_args), None);
    }

    #[test]
    #[cfg(feature = "connection-manager")]
    fn test_epoch_on_shared_cache_managers() {
        let redis_key = b"test_redis_key".as_slice();
        let redis_key_2 = b"test_redis_key_2".as_slice();
        let redis_key_3 = b"test_redis_key_3".as_slice();
        let cmd = crate::cmd("HGET")
            .arg("test_redis_key")
            .arg("field")
            .clone();
        let cmd = CmdRef::from_cmd(&cmd);
        let cmd_key = cmd.data();
        let (offsets, base) = cmd.arg_offsets();
        let cmd_args = CmdArgs::new(offsets, base);

        let shared_cache_manager = CacheManager::new(CacheConfig::new());

        let cache_manager_1 = shared_cache_manager.clone_and_increase_epoch();
        let cache_manager_2 = shared_cache_manager.clone_and_increase_epoch();
        let cache_manager_3 = shared_cache_manager.clone_and_increase_epoch();

        let secs_10 = Instant::now().add(Duration::from_secs(10));

        let do_inserts = |cm1: &CacheManager, cm2: &CacheManager, cm3: &CacheManager| {
            cm1.insert(
                redis_key,
                cmd_key,
                cmd_args,
                Value::Int(1),
                secs_10,
                &Value::Int(5),
                0,
            );
            cm2.insert(
                redis_key_2,
                cmd_key,
                cmd_args,
                Value::Int(2),
                secs_10,
                &Value::Int(5),
                0,
            );
            cm3.insert(
                redis_key_3,
                cmd_key,
                cmd_args,
                Value::Int(3),
                secs_10,
                &Value::Int(5),
                0,
            );
        };

        let do_hit_gets = |cm1: &CacheManager, cm2: &CacheManager, cm3: &CacheManager| {
            assert_eq!(cm1.get(redis_key, cmd_key, cmd_args), Some(Value::Int(1)));
            assert_eq!(cm2.get(redis_key_2, cmd_key, cmd_args), Some(Value::Int(2)));
            assert_eq!(cm3.get(redis_key_3, cmd_key, cmd_args), Some(Value::Int(3)));
        };

        let do_miss_gets = |cm1: &CacheManager, cm2: &CacheManager, cm3: &CacheManager| {
            assert_eq!(cm1.get(redis_key, cmd_key, cmd_args), None);
            assert_eq!(cm2.get(redis_key_2, cmd_key, cmd_args), None);
            assert_eq!(cm3.get(redis_key_3, cmd_key, cmd_args), None);
        };

        do_inserts(&cache_manager_1, &cache_manager_2, &cache_manager_3);
        do_hit_gets(&cache_manager_1, &cache_manager_2, &cache_manager_3);
        // Different CacheManagers has different epochs so all must return None
        do_miss_gets(&cache_manager_2, &cache_manager_3, &cache_manager_1);

        // Check when only one CacheManager has increased the epoch
        do_inserts(&cache_manager_1, &cache_manager_2, &cache_manager_3);
        do_hit_gets(&cache_manager_1, &cache_manager_2, &cache_manager_3);

        let cache_manager_1 = cache_manager_1.clone_and_increase_epoch();
        assert_eq!(cache_manager_1.get(redis_key, cmd_key, cmd_args), None);

        assert_eq!(
            cache_manager_2.get(redis_key_2, cmd_key, cmd_args),
            Some(Value::Int(2))
        );
        assert_eq!(
            cache_manager_3.get(redis_key_3, cmd_key, cmd_args),
            Some(Value::Int(3))
        );
    }
}
