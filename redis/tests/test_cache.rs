#![cfg(all(feature = "aio", feature = "cache-aio"))]

use crate::support::*;
use futures_time::task::sleep;
use redis::CommandCacheConfig;
use redis::{caching::CacheConfig, AsyncCommands, ProtocolVersion, RedisError};
use redis_test::server::Module;
use rstest::rstest;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

mod support;

macro_rules! assert_hit {
    ($con:expr, $val:expr) => {
        assert_eq!($con.get_cache_statistics().unwrap().hit, $val);
    };
}

macro_rules! assert_miss {
    ($con:expr, $val:expr) => {
        assert_eq!($con.get_cache_statistics().unwrap().miss, $val);
    };
}

macro_rules! assert_invalidate {
    ($con:expr, $val:expr) => {
        assert_eq!($con.get_cache_statistics().unwrap().invalidate, $val);
    };
}

// Basic testing should work with both CacheMode::All and CacheMode::OptIn if commands has called cache()
#[rstest]
#[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
#[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
fn test_cache_basic(#[case] runtime: RuntimeType, #[values(true, false)] test_with_optin: bool) {
    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }
    block_on_all(
        async move {
            let cache_config = if test_with_optin {
                CacheConfig::new().set_mode(redis::caching::CacheMode::OptIn)
            } else {
                CacheConfig::default()
            };
            let mut con = ctx.async_connection_with_cache_config(cache_config).await?;
            let val: Option<String> = get_cmd("GET", test_with_optin)
                .arg("key_1")
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(val, None);
            assert_hit!(&con, 0);
            assert_miss!(&con, 1);

            let val: Option<String> = get_cmd("GET", test_with_optin)
                .arg("key_1")
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(val, None);
            // key_1's value should be returned from cache even if it doesn't exist in server yet.
            assert_hit!(&con, 1);
            assert_miss!(&con, 1);

            let _: () = get_cmd("SET", test_with_optin)
                .arg("key_1")
                .arg("1")
                .query_async(&mut con)
                .await
                .unwrap();
            sleep(Duration::from_millis(50).into()).await; // Give time for push message to be received after invalidating key_1.
            assert_hit!(&con, 1);
            assert_miss!(&con, 1);
            assert_invalidate!(&con, 1);

            let val: String = get_cmd("GET", test_with_optin)
                .arg("key_1")
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(val, "1");
            // After invalidating key_1, now it misses the key from cache
            assert_hit!(&con, 1);
            assert_miss!(&con, 2);
            assert_invalidate!(&con, 1);
            Ok::<_, RedisError>(())
        },
        runtime,
    )
    .unwrap();
}

#[rstest]
#[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
#[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
fn test_cache_mget(#[case] runtime: RuntimeType) {
    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }

    block_on_all(
        async move {
            let mut con = ctx.async_connection_with_cache().await?;
            // Caching must work with both MGET and GET
            let _: () = get_pipe(true)
                .cmd("SET")
                .arg("key_1")
                .arg(41)
                .ignore()
                .cmd("SET")
                .arg("key_3")
                .arg(43)
                .ignore()
                .query_async(&mut con)
                .await?;

            let res1: Vec<Option<String>> = redis::cmd("MGET")
                .arg("key_1")
                .arg("key_2")
                .query_async(&mut con)
                .await?;
            assert_hit!(&con, 0);
            assert_miss!(&con, 2);
            assert_eq!(res1, vec![Some("41".to_string()), None]);

            let res2: Vec<Option<String>> = redis::cmd("MGET")
                .arg("key_1")
                .arg("key_3")
                .arg("key_2")
                .query_async(&mut con)
                .await?;
            assert_hit!(&con, 2);
            assert_miss!(&con, 3);
            assert_eq!(
                res2,
                vec![Some("41".to_string()), Some("43".to_string()), None]
            );

            let _: Option<String> = redis::cmd("GET").arg("key_1").query_async(&mut con).await?;
            assert_hit!(&con, 3);
            assert_miss!(&con, 3);
            Ok::<_, RedisError>(())
        },
        runtime,
    )
    .unwrap();
}

#[rstest]
#[cfg(feature = "json")]
#[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
#[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
fn test_module_cache_json_get_mget(#[case] runtime: RuntimeType) {
    let ctx = TestContext::with_modules(&[Module::Json], false);
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }

    block_on_all(
        async move {
            let cache_config = CacheConfig::new().set_mode(redis::caching::CacheMode::OptIn);
            let mut con = ctx.async_connection_with_cache_config(cache_config).await?;

            let value_1 = serde_json::to_string(&json!({"value":41i64}))?;
            let value_3 = serde_json::to_string(&json!({"value":43i64}))?;

            let _: () = get_pipe(true)
                .cmd("JSON.SET")
                .arg("key_1")
                .arg("$")
                .arg(value_1.clone())
                .ignore()
                .cmd("JSON.SET")
                .arg("key_3")
                .arg("$")
                .arg(value_3.clone())
                .ignore()
                .query_async(&mut con)
                .await?;

            let res1: Vec<Option<String>> = get_cmd("JSON.MGET", true)
                .arg(&["key_1", "key_2"])
                .arg(".")
                .query_async(&mut con)
                .await?;

            assert_eq!(res1.len(), 2);
            assert_eq!(res1, vec![Some(value_1.clone()), None]);
            assert_hit!(&con, 0);
            assert_miss!(&con, 2);

            let res2: Vec<Option<String>> = get_cmd("JSON.MGET", true)
                .arg("key_1")
                .arg("key_3")
                .arg("key_2")
                .arg(".")
                .query_async(&mut con)
                .await?;

            assert_eq!(res2.len(), 3);
            assert_eq!(res2, vec![Some(value_1.clone()), Some(value_3), None]);
            assert_hit!(&con, 2);
            assert_miss!(&con, 3);

            let res3: Option<String> = get_cmd("JSON.GET", true)
                .arg("key_1")
                .arg(".")
                .query_async(&mut con)
                .await?;

            assert_eq!(res3, Some(value_1.clone()));
            assert_hit!(&con, 3);
            assert_miss!(&con, 3);

            Ok::<_, RedisError>(())
        },
        runtime,
    )
    .unwrap();
}

#[rstest]
#[cfg(feature = "json")]
#[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
#[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
fn test_module_cache_json_get_mget_different_paths(#[case] runtime: RuntimeType) {
    let ctx = TestContext::with_modules(&[Module::Json], false);
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }

    block_on_all(
        async move {
            let cache_config = CacheConfig::new().set_mode(redis::caching::CacheMode::OptIn);
            let mut con = ctx.async_connection_with_cache_config(cache_config).await?;

            let value_1 = serde_json::to_string(&json!({"value":41i64, "id":1i8}))?;
            let value_3 = serde_json::to_string(&json!({"value":43i64, "id":3i8}))?;

            let _: () = get_pipe(true)
                .cmd("JSON.SET")
                .arg("key_1")
                .arg("$")
                .arg(value_1.clone())
                .ignore()
                .cmd("JSON.SET")
                .arg("key_3")
                .arg("$")
                .arg(value_3.clone())
                .ignore()
                .query_async(&mut con)
                .await?;

            let res1: Vec<Option<i64>> = get_cmd("JSON.MGET", true)
                .arg(&["key_1", "key_2"])
                .arg(".value")
                .query_async(&mut con)
                .await?;

            assert_eq!(res1.len(), 2);
            assert_eq!(res1, vec![Some(41), None]);
            assert_hit!(&con, 0);
            assert_miss!(&con, 2);

            let res2: Vec<Option<i64>> = get_cmd("JSON.MGET", true)
                .arg(&["key_1", "key_2"])
                .arg(".value")
                .query_async(&mut con)
                .await?;

            assert_eq!(res2.len(), 2);
            assert_eq!(res2, vec![Some(41), None]);
            assert_hit!(&con, 2);
            assert_miss!(&con, 2);

            let res3: Vec<Option<i64>> = get_cmd("JSON.MGET", true)
                .arg(&["key_1", "key_3"])
                .arg(".id")
                .query_async(&mut con)
                .await?;

            assert_eq!(res3.len(), 2);
            assert_eq!(res3, vec![Some(1), Some(3)]);
            assert_hit!(&con, 2);
            assert_miss!(&con, 4);

            let res4: Vec<Option<i64>> = get_cmd("JSON.MGET", true)
                .arg(&["key_2", "key_3"])
                .arg(".id")
                .query_async(&mut con)
                .await?;

            assert_eq!(res4.len(), 2);
            assert_eq!(res4, vec![None, Some(3)]);
            assert_hit!(&con, 3);
            assert_miss!(&con, 5);

            let res5: Option<i64> = get_cmd("JSON.GET", true)
                .arg("key_1")
                .arg(".value")
                .query_async(&mut con)
                .await?;

            assert_eq!(res5, Some(41));
            assert_hit!(&con, 4);
            assert_miss!(&con, 5);

            let res6: Option<i64> = get_cmd("JSON.GET", true)
                .arg("key_1")
                .arg(".value")
                .query_async(&mut con)
                .await?;

            assert_eq!(res6, Some(41));
            assert_hit!(&con, 5);
            assert_miss!(&con, 5);

            let res7: Option<i8> = get_cmd("JSON.GET", true)
                .arg("key_1")
                .arg(".id")
                .query_async(&mut con)
                .await?;

            assert_eq!(res7, Some(1));
            assert_hit!(&con, 6);
            assert_miss!(&con, 5);

            Ok::<_, RedisError>(())
        },
        runtime,
    )
    .unwrap();
}

#[rstest]
#[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
#[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
fn test_cache_is_not_target_type_dependent(#[case] runtime: RuntimeType) {
    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }

    block_on_all(
        async move {
            let mut con = ctx.async_connection_with_cache().await?;
            let _: () = con.set("KEY", "77").await?;
            let x: u32 = con.get("KEY").await?;
            assert_eq!(x, 77);
            let x: String = con.get("KEY").await?;
            assert_eq!(x, "77");
            let x: u8 = con.get("KEY").await?;
            assert_eq!(x, 77);
            assert_hit!(&con, 2);
            assert_miss!(&con, 1);
            Ok::<_, RedisError>(())
        },
        runtime,
    )
    .unwrap();
}

#[rstest]
#[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
#[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
fn test_cache_with_pipeline(#[case] runtime: RuntimeType, #[values(true, false)] atomic: bool) {
    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }
    block_on_all(
        async move {
            let mut con = ctx.async_connection_with_cache().await?;
            // Test cache for both atomic and non-atomic Pipeline and mix MGET,GET,ignore in the pipeline.
            let (mget_k1_k2,): ((i32, i32),) = get_pipe(atomic)
                .cmd("SET")
                .arg("key_1")
                .arg(41)
                .ignore()
                .cmd("SET")
                .arg("key_2")
                .arg(42)
                .ignore()
                .cmd("MGET")
                .arg(&["key_1", "key_2"])
                .query_async(&mut con)
                .await?;

            assert_eq!(mget_k1_k2, (41, 42));
            // There are 2 miss for key_1, key_2 used with MGET
            assert_hit!(&con, 0);
            assert_miss!(&con, 2);

            let (k1, mget_k1_k2, k_unknown): (i32, (i32, i32), Option<i32>) = get_pipe(atomic)
                .cmd("GET")
                .arg("key_1")
                .cmd("MGET")
                .arg(&["key_1", "key_2"])
                .cmd("GET")
                .arg("key_doesnt_exists")
                .query_async(&mut con)
                .await?;

            assert_eq!(k1, 41);
            assert_eq!(mget_k1_k2, (41, 42));
            assert_eq!(k_unknown, Option::None);
            assert_hit!(&con, 3);
            assert_miss!(&con, 3);

            Ok::<_, RedisError>(())
        },
        runtime,
    )
    .unwrap();
}

#[rstest]
#[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
#[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
fn test_cache_basic_partial_opt_in(#[case] runtime: RuntimeType) {
    // In OptIn mode cache must not be utilized without explicit per command configuration.
    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }
    block_on_all(
        async move {
            let cache_config = CacheConfig::new().set_mode(redis::caching::CacheMode::OptIn);
            let mut con = ctx.async_connection_with_cache_config(cache_config).await?;
            let val: Option<String> = redis::cmd("GET")
                .arg("key_1")
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(val, None);
            // GET is not marked with cache(), there should be no MISS/HIT
            assert_hit!(&con, 0);
            assert_miss!(&con, 0);

            let _: () = redis::cmd("SET")
                .arg("key_1")
                .arg("1")
                .query_async(&mut con)
                .await
                .unwrap();
            // There should be no invalidation since cache is not used.
            assert_hit!(&con, 0);
            assert_miss!(&con, 0);
            assert_invalidate!(&con, 0);

            let val: String = redis::cmd("GET")
                .arg("key_1")
                .set_cache_config(CommandCacheConfig::new().set_enable_cache(true))
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(val, "1");
            assert_hit!(&con, 0);
            assert_miss!(&con, 1);

            let val: String = redis::cmd("GET")
                .arg("key_1")
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(val, "1");
            // Since cache is not used, hit should still be 0
            assert_hit!(&con, 0);
            assert_miss!(&con, 1);

            let val: String = redis::cmd("GET")
                .arg("key_1")
                .set_cache_config(CommandCacheConfig::new().set_enable_cache(true))
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(val, "1");
            assert_hit!(&con, 1);
            assert_miss!(&con, 1);
            assert_invalidate!(&con, 0);
            Ok::<_, RedisError>(())
        },
        runtime,
    )
    .unwrap();
}

#[rstest]
#[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
#[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
fn test_cache_pipeline_partial_opt_in(
    #[case] runtime: RuntimeType,
    #[values(true, false)] atomic: bool,
) {
    // In OptIn mode cache must not be utilized without explicit per command configuration.
    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }
    block_on_all(
        async move {
            let cache_config = CacheConfig::new().set_mode(redis::caching::CacheMode::OptIn);
            let mut con = ctx.async_connection_with_cache_config(cache_config).await?;
            // Test cache for both atomic and non-atomic Pipeline and mix MGET,GET,ignore in the pipeline.
            let (mget_k1_k2,): ((i32, i32),) = get_pipe(atomic)
                .cmd("SET")
                .arg("key_1")
                .arg(42)
                .ignore()
                .cmd("SET")
                .arg("key_2")
                .arg(43)
                .ignore()
                .cmd("MGET")
                .arg(&["key_1", "key_2"])
                .query_async(&mut con)
                .await?;

            assert_eq!(mget_k1_k2, (42, 43));
            // Since CacheMode::OptIn is enabled, so there should be no miss or hit
            assert_hit!(&con, 0);
            assert_miss!(&con, 0);

            for _ in 0..2 {
                let (mget_k1_k2, k1, k_unknown): ((i32, i32), i32, Option<i32>) = get_pipe(atomic)
                    .cmd("MGET")
                    .set_cache_config(CommandCacheConfig::new().set_enable_cache(true))
                    .arg(&["key_1", "key_2"])
                    .cmd("GET")
                    .arg("key_1")
                    .cmd("GET")
                    .arg("key_doesnt_exists")
                    .query_async(&mut con)
                    .await?;

                assert_eq!(mget_k1_k2, (42, 43));
                assert_eq!(k1, 42);
                assert_eq!(k_unknown, Option::None);
            }
            // Only MGET should be use cache path, since pipeline used twice there should be one miss and one hit.
            assert_hit!(&con, 2);
            assert_miss!(&con, 2);

            Ok::<_, RedisError>(())
        },
        runtime,
    )
    .unwrap();
}

#[rstest]
#[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
#[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
fn test_cache_different_commands(
    #[case] runtime: RuntimeType,
    #[values(true, false)] test_with_opt_in: bool,
) {
    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }
    block_on_all(
        async move {
            let cache_config = if test_with_opt_in {
                CacheConfig::new().set_mode(redis::caching::CacheMode::OptIn)
            } else {
                CacheConfig::default()
            };
            let mut con = ctx.async_connection_with_cache_config(cache_config).await?;
            let _: () = get_cmd("HSET", test_with_opt_in)
                .arg("user")
                .arg("health")
                .arg("100")
                .query_async(&mut con)
                .await
                .unwrap();

            let val: usize = get_cmd("HGET", test_with_opt_in)
                .arg("user")
                .arg("health")
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(val, 100);
            assert_hit!(&con, 0);
            assert_miss!(&con, 1);

            let val: Option<usize> = get_cmd("HGET", test_with_opt_in)
                .arg("user")
                .arg("non_existent_key")
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(val, None);
            assert_hit!(&con, 0);
            assert_miss!(&con, 2);

            let val: HashMap<String, usize> = get_cmd("HGETALL", test_with_opt_in)
                .arg("user")
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(val.get("health"), Some(100).as_ref());
            assert_hit!(&con, 0);
            assert_miss!(&con, 3);

            let val: HashMap<String, usize> = get_cmd("HGETALL", test_with_opt_in)
                .arg("user")
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(val.get("health"), Some(100).as_ref());
            assert_hit!(&con, 1);
            assert_miss!(&con, 3);
            Ok::<_, RedisError>(())
        },
        runtime,
    )
    .unwrap();
}

#[rstest]
#[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
#[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
#[cfg(feature = "connection-manager")]
fn test_connection_manager_maintains_statistics_after_crashes(
    #[case] runtime: RuntimeType,
    #[values(true, false)] test_with_optin: bool,
) {
    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }
    block_on_all(
        async move {
            let cache_config = if test_with_optin {
                CacheConfig::new().set_mode(redis::caching::CacheMode::OptIn)
            } else {
                CacheConfig::default()
            };

            let config = redis::aio::ConnectionManagerConfig::new()
                .set_max_delay(1)
                .set_cache_config(cache_config);

            let mut get = get_cmd("GET", test_with_optin);
            get.arg("key_1");

            let mut manager = ctx
                .client
                .get_connection_manager_with_config(config)
                .await
                .unwrap();

            let val: Option<String> = get.query_async(&mut manager).await.unwrap();
            assert_eq!(val, None);
            assert_hit!(&manager, 0);
            assert_miss!(&manager, 1);

            let val: Option<String> = get.query_async(&mut manager).await.unwrap();
            assert_eq!(val, None);
            assert_hit!(&manager, 1);
            assert_miss!(&manager, 1);

            let addr = ctx.server.client_addr().clone();
            drop(ctx);

            let result: Result<redis::Value, RedisError> =
                manager.send_packed_command(&redis::cmd("PING")).await;
            assert!(result.unwrap_err().is_unrecoverable_error());

            let _server =
                redis_test::server::RedisServer::new_with_addr_and_modules(addr, &[], false);

            loop {
                if manager.send_packed_command(&get).await.is_ok() {
                    break;
                }
            }

            assert_eq!(val, None);
            // The key should've been invalidated after the disconnect
            assert_hit!(&manager, 1);
            assert_miss!(&manager, 2);

            Ok(())
        },
        runtime,
    )
    .unwrap();
}

// Support function for testing pipelines
fn get_pipe(atomic: bool) -> redis::Pipeline {
    if atomic {
        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe
    } else {
        redis::pipe()
    }
}

// Support function for testing cases where CacheMode::All == CacheMode::OptIn
fn get_cmd(name: &str, enable_opt_in: bool) -> redis::Cmd {
    let mut cmd = redis::cmd(name);
    if enable_opt_in {
        cmd.set_cache_config(CommandCacheConfig::new().set_enable_cache(true));
    }
    cmd
}
