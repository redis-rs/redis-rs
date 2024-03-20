#![cfg(all(feature = "cluster-async", feature = "cache"))]

use redis::{caching, cmd, ProtocolVersion, RedisError};

use crate::support::*;

mod support;

#[test]
fn test_async_cluster_cache() {
    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }
    caching::reset_global_statistics();
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection_with_cache().await;
        cmd("SET")
            .arg("test")
            .arg("test_data")
            .query_async(&mut connection)
            .await?;
        let res: String = cmd("GET").arg("test").query_async(&mut connection).await?;
        assert_eq!(res, "test_data");
        {
            let cs = caching::get_global_statistics();
            assert_eq!(cs.hit, 0);
            assert_eq!(cs.miss, 1);
        }
        let res: String = cmd("GET").arg("test").query_async(&mut connection).await?;
        assert_eq!(res, "test_data");
        {
            let cs = caching::get_global_statistics();
            assert_eq!(cs.hit, 1);
            assert_eq!(cs.miss, 1);
        }
        cmd("DEL").arg("test").query_async(&mut connection).await?;
        let res: Option<String> = cmd("GET").arg("test").query_async(&mut connection).await?;
        {
            let cs = caching::get_global_statistics();
            assert_eq!(cs.hit, 1);
            assert_eq!(cs.miss, 2);
        }
        assert_eq!(res, None);
        let _: Option<String> = cmd("GET").arg("test2").query_async(&mut connection).await?;
        {
            let cs = caching::get_global_statistics();
            assert_eq!(cs.hit, 1);
            assert_eq!(cs.miss, 3);
        }
        Ok::<_, RedisError>(())
    })
    .unwrap();
}

#[test]
fn test_cache_across_nodes() {
    let ctx = TestContext::new();
    if ctx.protocol == ProtocolVersion::RESP2 {
        return;
    }
    caching::reset_global_statistics();
    let cluster = TestClusterContext::new(3, 0);
    let key_num = 100;
    block_on_all(async move {
        let mut connection = cluster.async_connection_with_cache().await;
        for i in 0..key_num {
            cmd("SET")
                .arg(i.to_string())
                .arg(i)
                .query_async(&mut connection)
                .await?;
        }
        {
            let cs = caching::get_global_statistics();
            assert_eq!(cs.hit, 0);
            assert_eq!(cs.miss, 0);
        }
        for i in 0..key_num {
            let num: usize = cmd("GET")
                .arg(i.to_string())
                .query_async(&mut connection)
                .await?;
            assert_eq!(num, i);
        }
        {
            let cs = caching::get_global_statistics();
            assert_eq!(cs.hit, 0);
            assert_eq!(cs.miss, key_num);
        }

        for i in 0..key_num {
            let num: usize = cmd("GET")
                .arg(i.to_string())
                .query_async(&mut connection)
                .await?;
            assert_eq!(num, i);
        }
        {
            let cs = caching::get_global_statistics();
            assert_eq!(cs.hit, key_num);
            assert_eq!(cs.miss, key_num);
        }
        for i in 0..key_num {
            cmd("SET")
                .arg(i.to_string())
                .arg(i)
                .query_async(&mut connection)
                .await?;
        }
        {
            let cs = caching::get_global_statistics();
            assert_eq!(cs.hit, key_num);
            assert_eq!(cs.miss, key_num);
            assert_eq!(cs.invalidate, key_num);
        }
        for i in 0..key_num {
            let num: usize = cmd("GET")
                .arg(i.to_string())
                .query_async(&mut connection)
                .await?;
            assert_eq!(num, i);
        }
        {
            let cs = caching::get_global_statistics();
            assert_eq!(cs.hit, key_num);
            assert_eq!(cs.miss, key_num * 2);
            assert_eq!(cs.invalidate, key_num);
        }
        Ok::<_, RedisError>(())
    })
    .unwrap();
}
