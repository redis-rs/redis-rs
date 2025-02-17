use super::cmd::{CacheableCommand, CacheablePipeline, MultipleCachedCommandPart};
use super::sharded_lru::*;
use super::{CacheConfig, CacheMode, CacheStatistics};
use crate::cmd::{cmd_len, Cmd};
use crate::commands::is_readonly_cmd;
use crate::{Pipeline, PushKind, RedisResult, Value};
use std::cmp::min;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) enum PrepareCacheResult<'a> {
    Cached(Value),
    NotCached(CacheableCommand<'a>),
    NotCacheable,
    Ignored,
}

#[derive(Clone)]
pub(crate) struct CacheManager {
    lru: Arc<ShardedLRU>,
    pub(crate) cache_config: CacheConfig,
}

impl CacheManager {
    pub(crate) fn new(cache_config: CacheConfig) -> Self {
        CacheManager {
            lru: Arc::new(ShardedLRU::new(cache_config.size)),
            cache_config,
        }
    }

    pub(crate) fn get<'a>(&self, redis_key: &'a [u8], redis_cmd: &'a [u8]) -> Option<Value> {
        self.lru.get(redis_key, redis_cmd)
    }

    pub(crate) fn insert(
        &self,
        redis_key: &[u8],
        cmd_key: &[u8],
        value: Value,
        client_side_expire_time: Instant,
        server_side_ttl_value: &Value,
    ) {
        let pttl: RedisResult<i64> = crate::FromRedisValue::from_redis_value(server_side_ttl_value);
        let expire_time = match pttl {
            Ok(pttl) if pttl > 0 => {
                let server_side_expire_time =
                    Instant::now().add(Duration::from_millis(pttl as u64));
                min(server_side_expire_time, client_side_expire_time)
            }
            _ => client_side_expire_time,
        };
        self.lru.insert(redis_key, cmd_key, value, expire_time);
    }

    pub(crate) fn statistics(&self) -> CacheStatistics {
        self.lru.statistics.clone().into()
    }

    pub(crate) fn handle_push_value(&self, kind: &PushKind, data: &[Value]) {
        if kind == &PushKind::Invalidate {
            if let Some(Value::Array(redis_key)) = data.first() {
                if let Some(redis_key) = redis_key.first() {
                    if let Ok(redis_key) = crate::FromRedisValue::from_redis_value(redis_key) {
                        self.lru.invalidate(&redis_key)
                    }
                }
            }
        }
    }

    pub(crate) fn get_cached_cmd<'a>(&self, cmd: &'a Cmd) -> PrepareCacheResult<'a> {
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
                    PrepareCacheResult::NotCacheable
                }
            }
        }
    }

    fn calculate_expiration_time(&self, cmd: &Cmd) -> Instant {
        let client_side_ttl = cmd
            .get_cache_config()
            .as_ref()
            .and_then(|cache| cache.client_side_ttl)
            .unwrap_or(self.cache_config.default_client_ttl);

        Instant::now().add(client_side_ttl)
    }

    fn prepare_key_buffer(
        &self,
        buffer: &mut Vec<u8>,
        single_command_name: &[u8],
        redis_key: &[u8],
        json_path_key: Option<&[u8]>,
    ) {
        buffer.clear();
        buffer.extend_from_slice(single_command_name);
        buffer.extend_from_slice(redis_key);
        if let Some(json_path_key) = json_path_key {
            buffer.extend_from_slice(json_path_key);
        }
    }

    fn extract_simple_arguments<'a>(&self, cmd: &'a Cmd) -> Vec<&'a [u8]> {
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
        cmd: &'a Cmd,
        is_json_command: bool,
        single_command_name: &[u8],
        commands: &mut Vec<(usize, MultipleCachedCommandPart<'a>)>,
        response: &mut Vec<Value>,
        tail_args: &mut Vec<&'a [u8]>,
    ) {
        let mut arguments = self.extract_simple_arguments(cmd);
        let json_path_key = is_json_command
            .then(|| {
                arguments.pop().map(|k| {
                    tail_args.push(k);
                    k
                })
            })
            .flatten();
        let mut key_test_buffer: Vec<u8> = Vec::new();

        for (i, redis_key) in arguments.iter().enumerate() {
            self.prepare_key_buffer(
                &mut key_test_buffer,
                single_command_name,
                redis_key,
                json_path_key,
            );

            match self.get(redis_key, &key_test_buffer) {
                Some(value) => response.push(value),
                None => {
                    response.push(Value::Nil);
                    commands.push((
                        i,
                        MultipleCachedCommandPart {
                            redis_key,
                            cmd_key: key_test_buffer.clone(),
                        },
                    ));
                }
            }
        }
    }

    fn handle_multi_key_command<'a>(
        &self,
        cmd: &'a Cmd,
        command_name_str: &'a str,
        single_command_name: &[u8],
        client_side_expire: Instant,
    ) -> PrepareCacheResult<'a> {
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
            return PrepareCacheResult::Cached(Value::Array(response));
        }

        PrepareCacheResult::NotCached(CacheableCommand::Multiple {
            command_name: command_name_str,
            commands,
            response,
            client_side_expire,
            tail_args,
        })
    }

    fn handle_single_key_command<'a>(
        &self,
        cmd: &'a Cmd,
        client_side_expire: Instant,
    ) -> PrepareCacheResult<'a> {
        let redis_key = match cmd.arg_idx(1) {
            Some(key) => key,
            None => return PrepareCacheResult::NotCacheable,
        };

        let cmd_key = cmd.data.as_slice();

        if let Some(value) = self.get(redis_key, cmd_key) {
            return PrepareCacheResult::Cached(value);
        }

        PrepareCacheResult::NotCached(CacheableCommand::Single(
            crate::caching::cmd::SingleCachedCommand {
                redis_key,
                cmd_key,
                cmd,
                client_side_expire,
            },
        ))
    }

    /// Checks if there is enough information to resolve Cmd exists in cache,
    /// if it exists then returns PrepareCacheResult::Cached.
    /// If there isn't enough information in cache but Cmd is cacheable then packs enough information
    /// into CacheableCommand and returns PrepareCacheResult::NotCached.
    /// If Cmd doesn't support client side caching then it returns PrepareCacheResult::NotCacheable.
    fn get_cached_cmd_inner<'a>(&self, cmd: &'a Cmd) -> PrepareCacheResult<'a> {
        if cmd_len(cmd) < 2 {
            return PrepareCacheResult::NotCacheable;
        }

        let command_name = match cmd.arg_idx(0) {
            Some(name) => name,
            None => return PrepareCacheResult::NotCacheable,
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

        if is_readonly_cmd(command_name) {
            return self.handle_single_key_command(cmd, client_side_expire);
        }

        PrepareCacheResult::NotCacheable
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

        for (idx, cmd) in requested_pipeline.commands.iter().enumerate() {
            if requested_pipeline.ignored_commands.contains(&idx) {
                commands.push(PrepareCacheResult::Ignored);
                packed_pipeline.add_command(cmd.clone());
                continue;
            }
            let cacheable_command = self.get_cached_cmd(cmd);
            match cacheable_command {
                PrepareCacheResult::Cached(_) => {}
                PrepareCacheResult::NotCached(ref cc) => {
                    cc.pack_command(self, &mut packed_pipeline);
                }
                PrepareCacheResult::NotCacheable => {
                    // It must be added to packed_pipeline manually, since it's not packed via pack_command.
                    packed_pipeline.add_command(cmd.clone());
                }
                // PrepareCacheResult::Ignored shouldn't return by get_cached_cmd
                _ => panic!("Unexpected result is given from get_cached_cmd"),
            };
            commands.push(cacheable_command);
        }

        let pipeline_response_counts = if transaction_mode {
            packed_pipeline.atomic();
            (packed_pipeline.commands.len() + 1, 1)
        } else {
            (0, packed_pipeline.commands.len())
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

    #[test]
    fn test_custom_command_ttl() {
        let redis_key = b"test_redis_key".as_slice();
        let redis_key_2 = b"test_redis_key_2".as_slice();
        let redis_key_3 = b"test_redis_key_3".as_slice();
        let cmd_key = b"test_cmd_key".as_slice();

        let cache_manager = CacheManager::new(CacheConfig::new());
        let secs_10 = Instant::now().add(Duration::from_secs(10));
        let ms_5 = Instant::now().add(Duration::from_millis(5));
        let ms_50 = Instant::now().add(Duration::from_millis(50));

        cache_manager.insert(redis_key, cmd_key, Value::Int(1), secs_10, &Value::Int(5));
        cache_manager.insert(redis_key_2, cmd_key, Value::Int(2), ms_5, &Value::Int(500));
        cache_manager.insert(redis_key_3, cmd_key, Value::Int(3), ms_50, &Value::Int(500));

        assert_eq!(cache_manager.get(redis_key, cmd_key), Some(Value::Int(1)));
        assert_eq!(cache_manager.get(redis_key_2, cmd_key), Some(Value::Int(2)));
        assert_eq!(cache_manager.get(redis_key_3, cmd_key), Some(Value::Int(3)));

        std::thread::sleep(Duration::from_millis(6));
        assert_eq!(
            cache_manager.get(redis_key, cmd_key),
            None,
            "Key must be expired, server value must be picked"
        );
        assert_eq!(
            cache_manager.get(redis_key_2, cmd_key),
            None,
            "Key must be expired, client value must be picked"
        );
        assert_eq!(
            cache_manager.get(redis_key_3, cmd_key),
            Some(Value::Int(3)),
            "Key must be alive, client value must be picked"
        );
    }
}
