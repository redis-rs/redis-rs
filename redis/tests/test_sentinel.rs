mod support;

use redis::sentinel::SentinelClient;

use crate::support::*;

#[test]
fn test_sentinel_basics() {
    let cluster = TestSentinelContext::new(3, 1, 3);
    let sentinel = cluster.sentinel();

    let mut master_con = sentinel
        .master_for("master1")
        .unwrap()
        .get_connection()
        .unwrap();

    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(&mut master_con);
    redis::cmd("SET")
        .arg(&["{x}key2", "bar"])
        .execute(&mut master_con);

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["{x}key1", "{x}key2"])
            .query(&mut master_con),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

#[test]
fn test_sentinel_read_from_random_replica() {
    let cluster = TestSentinelContext::new(3, 1, 3);
    let sentinel = cluster.sentinel();

    let mut master_con = sentinel
        .master_for("master1")
        .unwrap()
        .get_connection()
        .unwrap();

    let mut replica_con = sentinel
        .replica_for("master1")
        .unwrap()
        .get_connection()
        .unwrap();

    // Write commands would go to the primary nodes
    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(&mut master_con);
    redis::cmd("SET")
        .arg(&["{x}key2", "bar"])
        .execute(&mut master_con);

    // Read commands would go to the replica nodes
    assert_eq!(
        redis::cmd("MGET")
            .arg(&["{x}key1", "{x}key2"])
            .query(&mut replica_con),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

#[test]
fn test_sentinel_read_from_multiple_replicas() {
    let mut cluster = TestSentinelContext::new(3, 1, 3);
    let sentinel = cluster.sentinel_mut();

    let mut master_con = sentinel
        .master_for("master1")
        .unwrap()
        .get_connection()
        .unwrap();

    // Write commands would go to the primary nodes
    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(&mut master_con);
    redis::cmd("SET")
        .arg(&["{x}key2", "bar"])
        .execute(&mut master_con);

    for _ in 0..20 {
        let mut replica_con = sentinel
            .replica_rotate_for("master1")
            .unwrap()
            .get_connection()
            .unwrap();

        assert_eq!(
            redis::cmd("MGET")
                .arg(&["{x}key1", "{x}key2"])
                .query(&mut replica_con),
            Ok(("foo".to_string(), b"bar".to_vec()))
        );
    }
}

#[test]
fn test_sentinel_server_down() {
    let mut context = TestSentinelContext::new(3, 1, 3);
    let sentinel = context.sentinel_mut();

    let mut master_con = sentinel
        .master_for("master1")
        .unwrap()
        .get_connection()
        .unwrap();

    // Write commands would go to the primary nodes
    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(&mut master_con);
    redis::cmd("SET")
        .arg(&["{x}key2", "bar"])
        .execute(&mut master_con);

    context.cluster.sentinel_servers[0].stop();
    std::thread::sleep(std::time::Duration::from_millis(25));

    let sentinel = context.sentinel_mut();

    for _ in 0..20 {
        let mut replica_con = sentinel
            .replica_rotate_for("master1")
            .unwrap()
            .get_connection()
            .unwrap();

        assert_eq!(
            redis::cmd("MGET")
                .arg(&["{x}key1", "{x}key2"])
                .query(&mut replica_con),
            Ok(("foo".to_string(), b"bar".to_vec()))
        );
    }
}

#[test]
fn test_sentinel_client() {
    let context = TestSentinelContext::new(3, 1, 3);
    let master_client = SentinelClient::build(
        context.sentinels_connection_info().clone(),
        String::from("master1"),
        redis::sentinel::SentinelServerType::Master,
    )
    .unwrap();

    let replica_client = SentinelClient::build(
        context.sentinels_connection_info().clone(),
        String::from("master1"),
        redis::sentinel::SentinelServerType::Replica,
    )
    .unwrap();

    let mut master_con = master_client.get_connection().unwrap();

    // Write commands would go to the primary nodes
    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(&mut master_con);
    redis::cmd("SET")
        .arg(&["{x}key2", "bar"])
        .execute(&mut master_con);

    for _ in 0..20 {
        let mut replica_con = replica_client.get_connection().unwrap();

        assert_eq!(
            redis::cmd("MGET")
                .arg(&["{x}key1", "{x}key2"])
                .query(&mut replica_con),
            Ok(("foo".to_string(), b"bar".to_vec()))
        );
    }
}

#[cfg(feature = "aio")]
pub mod async_tests {
    use redis::{sentinel::SentinelClient, RedisError, RedisResult};

    use crate::support::*;

    #[test]
    fn test_sentinel_basics_async() {
        let cluster = TestSentinelContext::new(3, 1, 3);
        let sentinel = cluster.sentinel();

        block_on_all(async move {
            let mut master_con = sentinel
                .async_master_for("master1")
                .await?
                .get_async_connection()
                .await?;

            redis::cmd("SET")
                .arg("{x}key1")
                .arg(b"foo")
                .query_async(&mut master_con)
                .await?;
            redis::cmd("SET")
                .arg(&["{x}key2", "bar"])
                .query_async(&mut master_con)
                .await?;

            let mget_result: RedisResult<(String, Vec<u8>)> = redis::cmd("MGET")
                .arg(&["{x}key1", "{x}key2"])
                .query_async(&mut master_con)
                .await;
            assert_eq!(mget_result, Ok(("foo".to_string(), b"bar".to_vec())));

            Ok::<(), RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_sentinel_read_from_random_replica_async() {
        let cluster = TestSentinelContext::new(3, 1, 3);
        let sentinel = cluster.sentinel();

        block_on_all(async move {
            let mut master_con = sentinel
                .async_master_for("master1")
                .await?
                .get_async_connection()
                .await?;

            let mut replica_con = sentinel
                .async_replica_for("master1")
                .await?
                .get_async_connection()
                .await?;

            // Write commands would go to the primary nodes
            redis::cmd("SET")
                .arg("{x}key1")
                .arg(b"foo")
                .query_async(&mut master_con)
                .await?;
            redis::cmd("SET")
                .arg(&["{x}key2", "bar"])
                .query_async(&mut master_con)
                .await?;

            // Read commands to the replica node
            let mget_result: RedisResult<(String, Vec<u8>)> = redis::cmd("MGET")
                .arg(&["{x}key1", "{x}key2"])
                .query_async(&mut replica_con)
                .await;
            assert_eq!(mget_result, Ok(("foo".to_string(), b"bar".to_vec())));

            Ok::<(), RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_sentinel_read_from_multiple_replicas_async() {
        let mut cluster = TestSentinelContext::new(3, 1, 3);
        let sentinel = cluster.sentinel_mut();

        block_on_all(async move {
            let mut master_con = sentinel
                .async_master_for("master1")
                .await?
                .get_async_connection()
                .await?;

            // Write commands would go to the primary nodes
            redis::cmd("SET")
                .arg("{x}key1")
                .arg(b"foo")
                .query_async(&mut master_con)
                .await?;
            redis::cmd("SET")
                .arg(&["{x}key2", "bar"])
                .query_async(&mut master_con)
                .await?;

            // Read commands to the replica node
            for _ in 0..20 {
                let mut replica_con = sentinel
                    .async_replica_rotate_for("master1")
                    .await?
                    .get_async_connection()
                    .await?;

                let mget_result: RedisResult<(String, Vec<u8>)> = redis::cmd("MGET")
                    .arg(&["{x}key1", "{x}key2"])
                    .query_async(&mut replica_con)
                    .await;
                assert_eq!(mget_result, Ok(("foo".to_string(), b"bar".to_vec())));
            }

            Ok::<(), RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_sentinel_server_down_async() {
        let mut context = TestSentinelContext::new(3, 1, 3);

        block_on_all(async move {
            let sentinel = context.sentinel_mut();

            let mut master_con = sentinel
                .async_master_for("master1")
                .await?
                .get_async_connection()
                .await?;

            // Write commands would go to the primary nodes
            redis::cmd("SET")
                .arg("{x}key1")
                .arg(b"foo")
                .query_async(&mut master_con)
                .await?;
            redis::cmd("SET")
                .arg(&["{x}key2", "bar"])
                .query_async(&mut master_con)
                .await?;

            context.cluster.sentinel_servers[0].stop();
            std::thread::sleep(std::time::Duration::from_millis(25));

            let sentinel = context.sentinel_mut();

            // Read commands to the replica node
            for _ in 0..20 {
                let mut replica_con = sentinel
                    .async_replica_rotate_for("master1")
                    .await?
                    .get_async_connection()
                    .await?;

                let mget_result: RedisResult<(String, Vec<u8>)> = redis::cmd("MGET")
                    .arg(&["{x}key1", "{x}key2"])
                    .query_async(&mut replica_con)
                    .await;
                assert_eq!(mget_result, Ok(("foo".to_string(), b"bar".to_vec())));
            }

            Ok::<(), RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_sentinel_client_async() {
        let context = TestSentinelContext::new(3, 1, 3);
        let master_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from("master1"),
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();

        let replica_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from("master1"),
            redis::sentinel::SentinelServerType::Replica,
        )
        .unwrap();

        block_on_all(async move {
            let mut master_con = master_client.get_async_connection().await?;

            // Write commands would go to the primary nodes
            redis::cmd("SET")
                .arg("{x}key1")
                .arg(b"foo")
                .query_async(&mut master_con)
                .await?;
            redis::cmd("SET")
                .arg(&["{x}key2", "bar"])
                .query_async(&mut master_con)
                .await?;

            // Read commands to the replica node
            for _ in 0..20 {
                let mut replica_con = replica_client.get_async_connection().await?;

                let mget_result: RedisResult<(String, Vec<u8>)> = redis::cmd("MGET")
                    .arg(&["{x}key1", "{x}key2"])
                    .query_async(&mut replica_con)
                    .await;
                assert_eq!(mget_result, Ok(("foo".to_string(), b"bar".to_vec())));
            }

            Ok::<(), RedisError>(())
        })
        .unwrap();
    }
}
