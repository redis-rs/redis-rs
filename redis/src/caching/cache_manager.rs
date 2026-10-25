use super::cmd::{CacheableCommand, CacheablePipeline, MultipleCachedCommandPart};
use super::sharded_lru::*;
use super::{CacheConfig, CacheMode, CacheStatistics};
use crate::cmd::{Arg, CmdRef};
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

    pub(crate) fn insert(
        &self,
        redis_key: &[u8],
        cmd_key: &[u8],
        cmd_args: CmdArgs<'_>,
        value: Value,
        client_side_expire_time: Instant,
        server_side_ttl_value: &Value,
    ) {
        let pttl: Result<i64, _> = FromRedisValue::from_redis_value_ref(server_side_ttl_value);
        let expire_time = match pttl {
            Ok(pttl) if pttl > 0 => {
                let server_side_expire_time =
                    Instant::now().add(Duration::from_millis(pttl as u64));
                min(server_side_expire_time, client_side_expire_time)
            }
            _ => client_side_expire_time,
        };
        self.lru
            .insert(redis_key, cmd_key, cmd_args, value, expire_time, self.epoch);
    }

    pub(crate) fn statistics(&self) -> CacheStatistics {
        self.lru.statistics.clone().into()
    }

    pub(crate) fn handle_push_value(&self, kind: &PushKind, data: &[Value]) {
        if kind == &PushKind::Invalidate
            && let Some(Value::Array(redis_key)) = data.first()
            && let Some(redis_key) = redis_key.first()
            && let Ok(redis_key) = FromRedisValue::from_redis_value_ref(redis_key)
        {
            self.lru.invalidate(&redis_key);
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
            },
        ))
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

        if crate::commands::is_cachable_cmd(command_name) {
            return self.handle_single_key_command(cmd, client_side_expire);
        }

        PrepCacheItem::NotCacheable
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Cmd;
    use rstest::rstest;

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
        );
        cache_manager.insert(
            redis_key_2,
            cmd_key,
            cmd_args,
            Value::Int(2),
            ms_5,
            &Value::Int(500),
        );
        cache_manager.insert(
            redis_key_3,
            cmd_key,
            cmd_args,
            Value::Int(3),
            ms_50,
            &Value::Int(500),
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
        );
        let mget = crate::cmd("MGET").arg("key").clone();
        assert!(matches!(
            cache_manager.get_cached_cmd(CmdRef::from_cmd(&mget)),
            PrepCacheItem::Cached(Value::Array(values)) if values == vec![Value::Int(2)]
        ));
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
            );
            cm2.insert(
                redis_key_2,
                cmd_key,
                cmd_args,
                Value::Int(2),
                secs_10,
                &Value::Int(5),
            );
            cm3.insert(
                redis_key_3,
                cmd_key,
                cmd_args,
                Value::Int(3),
                secs_10,
                &Value::Int(5),
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
