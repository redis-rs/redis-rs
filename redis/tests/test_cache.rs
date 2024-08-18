#![cfg(all(feature = "aio", feature = "cache"))]

use redis::caching::CacheConfig;
use redis::{AsyncCommands, RedisError, RedisFuture};
use std::time::Duration;

use crate::support::*;

mod support;

#[test]
fn test_cache() {
    use redis::ProtocolVersion;

    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }

    block_on_all(async move {
        let mut con = ctx.multiplexed_async_connection_with_cache().await?;
        let val: Option<String> = redis::cmd("GET")
            .arg("key_1")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(val, None);
        {
            let s = con.get_cache_statistics();
            assert_eq!(s.hit, 0);
            assert_eq!(s.miss, 1);
        }
        let val: Option<String> = redis::cmd("GET")
            .arg("key_1")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(val, None);
        {
            let s = con.get_cache_statistics();
            assert_eq!(s.hit, 1);
            assert_eq!(s.miss, 1);
        }
        let _: () = redis::cmd("SET")
            .arg("key_1")
            .arg("1")
            .query_async(&mut con)
            .await
            .unwrap();
        {
            let s = con.get_cache_statistics();
            assert_eq!(s.hit, 1);
            assert_eq!(s.miss, 1);
            assert_eq!(s.invalidate, 1);
        }
        let val: String = redis::cmd("GET")
            .arg("key_1")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(val, "1");
        {
            let s = con.get_cache_statistics();
            assert_eq!(s.hit, 1);
            assert_eq!(s.miss, 2);
            assert_eq!(s.invalidate, 1);
        }
        Ok::<_, RedisError>(())
    })
    .unwrap();
}

#[test]
fn test_mget() {
    use redis::ProtocolVersion;

    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }

    block_on_all(async move {
        let mut con = ctx.multiplexed_async_connection_with_cache().await?;
        let _: Vec<Option<String>> = redis::cmd("MGET")
            .arg("key_1")
            .arg("key_2")
            .query_async(&mut con)
            .await?;
        {
            let s = con.get_cache_statistics();
            assert_eq!(s.hit, 0);
            assert_eq!(s.miss, 2);
        }

        let _: Vec<Option<String>> = redis::cmd("MGET")
            .arg("key_1")
            .arg("key_2")
            .query_async(&mut con)
            .await?;
        {
            let s = con.get_cache_statistics();
            assert_eq!(s.hit, 2);
            assert_eq!(s.miss, 2);
        }
        let _: Option<String> = redis::cmd("GET").arg("key_1").query_async(&mut con).await?;
        {
            let s = con.get_cache_statistics();
            assert_eq!(s.hit, 3);
            assert_eq!(s.miss, 2);
        }
        Ok::<_, RedisError>(())
    })
    .unwrap();
}

#[test]
fn test_drops_and_guards() {
    use redis::ProtocolVersion;

    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }

    block_on_all(async move {
        let con = ctx
            .multiplexed_async_connection_tokio_with_cache_config(CacheConfig::enabled())
            .await?;

        redis::caching::reset_global_statistics();
        let mut v = vec![];
        for i in 0..1000 {
            let mut c = con.clone();
            v.push(async move {
                let x: RedisFuture<Option<u32>> = c.get("KEY");
                tokio::time::timeout(Duration::from_micros(i), x).await
            })
        }
        futures::future::join_all(v).await;
        let s = con.get_cache_statistics();
        assert_eq!(s.miss, 1000);
        Ok::<_, RedisError>(())
    })
    .unwrap();
}

#[test]
fn test_guards_and_different_types() {
    use redis::ProtocolVersion;

    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }

    block_on_all(async move {
        let mut con = ctx
            .multiplexed_async_connection_tokio_with_cache_config(CacheConfig::enabled())
            .await?;
        con.set("KEY", "77").await?;
        redis::caching::reset_global_statistics();
        let mut v = vec![];
        for _ in 0..5 {
            let mut c = con.clone();
            v.push(tokio::spawn(async move {
                let x: u32 = c.get("KEY").await.unwrap();
                assert_eq!(x, 77);
            }));
            let mut c = con.clone();
            v.push(tokio::spawn(async move {
                let x: String = c.get("KEY").await.unwrap();
                assert_eq!(x, "77");
            }));
            let mut c = con.clone();
            v.push(tokio::spawn(async move {
                let x: u8 = c.get("KEY").await.unwrap();
                assert_eq!(x, 77);
            }));
        }
        futures::future::join_all(v).await;
        let s = redis::caching::get_global_statistics();
        // It should get value once and miss then it should use cache.
        assert_eq!(s.hit, 14);
        assert_eq!(s.miss, 1);
        // 1 GET + 1 PTTL
        assert_eq!(s.sent_command_count, 2);
        Ok::<_, RedisError>(())
    })
    .unwrap();
}

#[test]
fn test_guards_and_nil_values() {
    // Testing guard will work even with Nil Value is returned from server
    use redis::ProtocolVersion;

    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }

    block_on_all(async move {
        let con = ctx
            .multiplexed_async_connection_tokio_with_cache_config(CacheConfig::enabled())
            .await?;
        redis::caching::reset_global_statistics();
        let mut v = vec![];
        for _ in 0..5 {
            let mut c = con.clone();
            v.push(tokio::spawn(async move {
                let x: Option<u32> = c.get("KEY").await.unwrap();
                assert_eq!(x, None);
            }));
            let mut c = con.clone();
            v.push(tokio::spawn(async move {
                let x: Option<String> = c.get("KEY").await.unwrap();
                assert_eq!(x, None);
            }));
            let mut c = con.clone();
            v.push(tokio::spawn(async move {
                let x: Option<u8> = c.get("KEY").await.unwrap();
                assert_eq!(x, None);
            }));
        }
        futures::future::join_all(v).await;
        let s = redis::caching::get_global_statistics();
        // It should get value once and miss then it should use cache.
        assert_eq!(s.hit, 14);
        assert_eq!(s.miss, 1);
        // 1 GET + 1 PTTL
        assert_eq!(s.sent_command_count, 2);
        Ok::<_, RedisError>(())
    })
    .unwrap();
}

#[test]
fn test_verify() {
    use redis::ProtocolVersion;

    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }

    block_on_all(async move {
        let mut con = ctx
            .multiplexed_async_connection_tokio_with_cache_config(CacheConfig::enabled())
            .await?;
        let c2 = con.clone();
        let join_handle = tokio::spawn(async move {
            let con = c2.clone();
            let mut v = vec![];
            for i in 0..100000 {
                let mut c = con.clone();
                v.push(async move {
                    let x: RedisFuture<Option<u32>> = c.get("KEY");
                    let _ = tokio::time::timeout(Duration::from_micros(i * 10), x).await;
                })
            }
            futures::future::join_all(v).await;
        });
        for _ in 0..10000 {
            let _: Option<u32> = con.get("KEY").await.unwrap();
        }
        join_handle.await.unwrap();
        Ok::<_, RedisError>(())
    })
    .unwrap();
}

#[test]
fn test_invalidations() {
    use redis::ProtocolVersion;

    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }

    block_on_all(async move {
        let mut con = ctx
            .multiplexed_async_connection_tokio_with_cache_config(CacheConfig::enabled())
            .await?;
        let c2 = con.clone();
        let join_handle = tokio::spawn(async move {
            let con = c2.clone();
            let mut v = vec![];
            for i in 0..100000 {
                let mut c = con.clone();
                v.push(async move {
                    let x: RedisFuture<Option<u32>> = c.get("KEY");
                    let _ = tokio::time::timeout(Duration::from_micros(i * 10), x).await;
                })
            }
            futures::future::join_all(v).await;
        });
        for _ in 0..10000 {
            let _: Option<u32> = con.get("KEY").await.unwrap();
        }
        join_handle.await.unwrap();
        Ok::<_, RedisError>(())
    })
    .unwrap();
}
