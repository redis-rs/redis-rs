use std::collections::HashMap;

use futures::{prelude::*, StreamExt};
use redis::{
    aio::{ConnectionLike, MultiplexedConnection},
    cmd, pipe, AsyncCommands, ErrorKind, RedisResult,
};

use crate::support::*;
mod support;

#[test]
fn test_args() {
    let ctx = TestContext::new();
    let connect = ctx.async_connection();

    block_on_all(connect.and_then(|mut con| async move {
        redis::cmd("SET")
            .arg("key1")
            .arg(b"foo")
            .query_async(&mut con)
            .await?;
        redis::cmd("SET")
            .arg(&["key2", "bar"])
            .query_async(&mut con)
            .await?;
        let result = redis::cmd("MGET")
            .arg(&["key1", "key2"])
            .query_async(&mut con)
            .await;
        assert_eq!(result, Ok(("foo".to_string(), b"bar".to_vec())));
        result
    }))
    .unwrap();
}

#[test]
fn test_nice_hash_api() {
    let ctx = TestContext::new();

    block_on_all(async move {
        let mut connection = ctx.async_connection().await.unwrap();

        assert_eq!(
            connection
                .hset_multiple("my_hash", &[("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8)])
                .await,
            Ok(())
        );

        let hm: HashMap<String, isize> = connection.hgetall("my_hash").await.unwrap();
        assert_eq!(hm.len(), 4);
        assert_eq!(hm.get("f1"), Some(&1));
        assert_eq!(hm.get("f2"), Some(&2));
        assert_eq!(hm.get("f3"), Some(&4));
        assert_eq!(hm.get("f4"), Some(&8));
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_nice_hash_api_in_pipe() {
    let ctx = TestContext::new();

    block_on_all(async move {
        let mut connection = ctx.async_connection().await.unwrap();

        assert_eq!(
            connection
                .hset_multiple("my_hash", &[("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8)])
                .await,
            Ok(())
        );

        let mut pipe = redis::pipe();
        pipe.cmd("HGETALL").arg("my_hash");
        let mut vec: Vec<HashMap<String, isize>> = pipe.query_async(&mut connection).await.unwrap();
        assert_eq!(vec.len(), 1);
        let hash = vec.pop().unwrap();
        assert_eq!(hash.len(), 4);
        assert_eq!(hash.get("f1"), Some(&1));
        assert_eq!(hash.get("f2"), Some(&2));
        assert_eq!(hash.get("f3"), Some(&4));
        assert_eq!(hash.get("f4"), Some(&8));

        Ok(())
    })
    .unwrap();
}

#[test]
fn dont_panic_on_closed_multiplexed_connection() {
    let ctx = TestContext::new();
    let client = ctx.client.clone();
    let connect = client.get_multiplexed_async_connection();
    drop(ctx);

    block_on_all(async move {
        connect
            .and_then(|con| async move {
                let cmd = move || {
                    let mut con = con.clone();
                    async move {
                        redis::cmd("SET")
                            .arg("key1")
                            .arg(b"foo")
                            .query_async(&mut con)
                            .await
                    }
                };
                let result: RedisResult<()> = cmd().await;
                assert_eq!(
                    result.as_ref().unwrap_err().kind(),
                    redis::ErrorKind::IoError,
                    "{}",
                    result.as_ref().unwrap_err()
                );
                cmd().await
            })
            .map(|result| {
                assert_eq!(
                    result.as_ref().unwrap_err().kind(),
                    redis::ErrorKind::IoError,
                    "{}",
                    result.as_ref().unwrap_err()
                );
            })
            .await;
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_pipeline_transaction() {
    let ctx = TestContext::new();
    block_on_all(async move {
        let mut con = ctx.async_connection().await?;
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("SET")
            .arg("key_1")
            .arg(42)
            .ignore()
            .cmd("SET")
            .arg("key_2")
            .arg(43)
            .ignore()
            .cmd("MGET")
            .arg(&["key_1", "key_2"]);
        pipe.query_async(&mut con)
            .map_ok(|((k1, k2),): ((i32, i32),)| {
                assert_eq!(k1, 42);
                assert_eq!(k2, 43);
            })
            .await
    })
    .unwrap();
}

#[test]
fn test_pipeline_transaction_with_errors() {
    use redis::RedisError;
    let ctx = TestContext::new();

    block_on_all(async move {
        let mut con = ctx.async_connection().await?;
        con.set::<_, _, ()>("x", 42).await.unwrap();

        // Make Redis a replica of a nonexistent master, thereby making it read-only.
        redis::cmd("slaveof")
            .arg("1.1.1.1")
            .arg("1")
            .query_async::<_, ()>(&mut con)
            .await
            .unwrap();

        // Ensure that a write command fails with a READONLY error
        let err: RedisResult<()> = redis::pipe()
            .atomic()
            .set("x", 142)
            .ignore()
            .get("x")
            .query_async(&mut con)
            .await;

        assert_eq!(err.unwrap_err().kind(), ErrorKind::ReadOnly);

        let x: i32 = con.get("x").await.unwrap();
        assert_eq!(x, 42);

        Ok::<_, RedisError>(())
    })
    .unwrap();
}

fn test_cmd(con: &MultiplexedConnection, i: i32) -> impl Future<Output = RedisResult<()>> + Send {
    let mut con = con.clone();
    async move {
        let key = format!("key{i}");
        let key_2 = key.clone();
        let key2 = format!("key{i}_2");
        let key2_2 = key2.clone();

        let foo_val = format!("foo{i}");

        redis::cmd("SET")
            .arg(&key[..])
            .arg(foo_val.as_bytes())
            .query_async(&mut con)
            .await?;
        redis::cmd("SET")
            .arg(&[&key2, "bar"])
            .query_async(&mut con)
            .await?;
        redis::cmd("MGET")
            .arg(&[&key_2, &key2_2])
            .query_async(&mut con)
            .map(|result| {
                assert_eq!(Ok((foo_val, b"bar".to_vec())), result);
                Ok(())
            })
            .await
    }
}

fn test_error(con: &MultiplexedConnection) -> impl Future<Output = RedisResult<()>> {
    let mut con = con.clone();
    async move {
        redis::cmd("SET")
            .query_async(&mut con)
            .map(|result| match result {
                Ok(()) => panic!("Expected redis to return an error"),
                Err(_) => Ok(()),
            })
            .await
    }
}

#[test]
fn test_pipe_over_multiplexed_connection() {
    let ctx = TestContext::new();
    block_on_all(async move {
        let mut con = ctx.multiplexed_async_connection().await?;
        let mut pipe = pipe();
        pipe.zrange("zset", 0, 0);
        pipe.zrange("zset", 0, 0);
        let frames = con.send_packed_commands(&pipe, 0, 2).await?;
        assert_eq!(frames.len(), 2);
        assert!(matches!(frames[0], redis::Value::Bulk(_)));
        assert!(matches!(frames[1], redis::Value::Bulk(_)));
        RedisResult::Ok(())
    })
    .unwrap();
}

#[test]
fn test_args_multiplexed_connection() {
    let ctx = TestContext::new();
    block_on_all(async move {
        ctx.multiplexed_async_connection()
            .and_then(|con| {
                let cmds = (0..100).map(move |i| test_cmd(&con, i));
                future::try_join_all(cmds).map_ok(|results| {
                    assert_eq!(results.len(), 100);
                })
            })
            .map_err(|err| panic!("{}", err))
            .await
    })
    .unwrap();
}

#[test]
fn test_args_with_errors_multiplexed_connection() {
    let ctx = TestContext::new();
    block_on_all(async move {
        ctx.multiplexed_async_connection()
            .and_then(|con| {
                let cmds = (0..100).map(move |i| {
                    let con = con.clone();
                    async move {
                        if i % 2 == 0 {
                            test_cmd(&con, i).await
                        } else {
                            test_error(&con).await
                        }
                    }
                });
                future::try_join_all(cmds).map_ok(|results| {
                    assert_eq!(results.len(), 100);
                })
            })
            .map_err(|err| panic!("{}", err))
            .await
    })
    .unwrap();
}

#[test]
fn test_transaction_multiplexed_connection() {
    let ctx = TestContext::new();
    block_on_all(async move {
        ctx.multiplexed_async_connection()
            .and_then(|con| {
                let cmds = (0..100).map(move |i| {
                    let mut con = con.clone();
                    async move {
                        let foo_val = i;
                        let bar_val = format!("bar{i}");

                        let mut pipe = redis::pipe();
                        pipe.atomic()
                            .cmd("SET")
                            .arg("key")
                            .arg(foo_val)
                            .ignore()
                            .cmd("SET")
                            .arg(&["key2", &bar_val[..]])
                            .ignore()
                            .cmd("MGET")
                            .arg(&["key", "key2"]);

                        pipe.query_async(&mut con)
                            .map(move |result| {
                                assert_eq!(Ok(((foo_val, bar_val.into_bytes()),)), result);
                                result
                            })
                            .await
                    }
                });
                future::try_join_all(cmds)
            })
            .map_ok(|results| {
                assert_eq!(results.len(), 100);
            })
            .map_err(|err| panic!("{}", err))
            .await
    })
    .unwrap();
}

fn test_async_scanning(batch_size: usize) {
    let ctx = TestContext::new();
    block_on_all(async move {
        ctx.multiplexed_async_connection()
            .and_then(|mut con| {
                async move {
                    let mut unseen = std::collections::HashSet::new();

                    for x in 0..batch_size {
                        redis::cmd("SADD")
                            .arg("foo")
                            .arg(x)
                            .query_async(&mut con)
                            .await?;
                        unseen.insert(x);
                    }

                    let mut iter = redis::cmd("SSCAN")
                        .arg("foo")
                        .cursor_arg(0)
                        .clone()
                        .iter_async(&mut con)
                        .await
                        .unwrap();

                    while let Some(x) = iter.next_item().await {
                        // type inference limitations
                        let x: usize = x;
                        // if this assertion fails, too many items were returned by the iterator.
                        assert!(unseen.remove(&x));
                    }

                    assert_eq!(unseen.len(), 0);
                    Ok(())
                }
            })
            .map_err(|err| panic!("{}", err))
            .await
    })
    .unwrap();
}

#[test]
fn test_async_scanning_big_batch() {
    test_async_scanning(1000)
}

#[test]
fn test_async_scanning_small_batch() {
    test_async_scanning(2)
}

#[test]
fn test_response_timeout_multiplexed_connection() {
    let ctx = TestContext::new();
    block_on_all(async move {
        let mut connection = ctx.multiplexed_async_connection().await.unwrap();
        connection.set_response_timeout(std::time::Duration::from_millis(1));
        let mut cmd = redis::Cmd::new();
        cmd.arg("BLPOP").arg("foo").arg(0); // 0 timeout blocks indefinitely
        let result = connection.req_packed_command(&cmd).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_timeout());
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg(feature = "script")]
fn test_script() {
    use redis::RedisError;

    // Note this test runs both scripts twice to test when they have already been loaded
    // into Redis and when they need to be loaded in
    let script1 = redis::Script::new("return redis.call('SET', KEYS[1], ARGV[1])");
    let script2 = redis::Script::new("return redis.call('GET', KEYS[1])");
    let script3 = redis::Script::new("return redis.call('KEYS', '*')");

    let ctx = TestContext::new();

    block_on_all(async move {
        let mut con = ctx.multiplexed_async_connection().await?;
        script1
            .key("key1")
            .arg("foo")
            .invoke_async(&mut con)
            .await?;
        let val: String = script2.key("key1").invoke_async(&mut con).await?;
        assert_eq!(val, "foo");
        let keys: Vec<String> = script3.invoke_async(&mut con).await?;
        assert_eq!(keys, ["key1"]);
        script1
            .key("key1")
            .arg("bar")
            .invoke_async(&mut con)
            .await?;
        let val: String = script2.key("key1").invoke_async(&mut con).await?;
        assert_eq!(val, "bar");
        let keys: Vec<String> = script3.invoke_async(&mut con).await?;
        assert_eq!(keys, ["key1"]);
        Ok::<_, RedisError>(())
    })
    .unwrap();
}

#[test]
#[cfg(feature = "script")]
fn test_script_load() {
    let ctx = TestContext::new();
    let script = redis::Script::new("return 'Hello World'");

    block_on_all(async move {
        let mut con = ctx.multiplexed_async_connection().await.unwrap();

        let hash = script.prepare_invoke().load_async(&mut con).await.unwrap();
        assert_eq!(hash, script.get_hash().to_string());
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg(feature = "script")]
fn test_script_returning_complex_type() {
    let ctx = TestContext::new();
    block_on_all(async {
        let mut con = ctx.multiplexed_async_connection().await?;
        redis::Script::new("return {1, ARGV[1], true}")
            .arg("hello")
            .invoke_async(&mut con)
            .map_ok(|(i, s, b): (i32, String, bool)| {
                assert_eq!(i, 1);
                assert_eq!(s, "hello");
                assert!(b);
            })
            .await
    })
    .unwrap();
}

// Allowing `nth(0)` for similarity with the following `nth(1)`.
// Allowing `let ()` as `query_async` requries the type it converts the result to.
#[allow(clippy::let_unit_value, clippy::iter_nth_zero)]
#[tokio::test]
async fn io_error_on_kill_issue_320() {
    let ctx = TestContext::new();

    let mut conn_to_kill = ctx.async_connection().await.unwrap();
    cmd("CLIENT")
        .arg("SETNAME")
        .arg("to-kill")
        .query_async::<_, ()>(&mut conn_to_kill)
        .await
        .unwrap();

    let client_list: String = cmd("CLIENT")
        .arg("LIST")
        .query_async(&mut conn_to_kill)
        .await
        .unwrap();

    eprintln!("{client_list}");
    let client_to_kill = client_list
        .split('\n')
        .find(|line| line.contains("to-kill"))
        .expect("line")
        .split(' ')
        .nth(0)
        .expect("id")
        .split('=')
        .nth(1)
        .expect("id value");

    let mut killer_conn = ctx.async_connection().await.unwrap();
    let () = cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(client_to_kill)
        .query_async(&mut killer_conn)
        .await
        .unwrap();
    let mut killed_client = conn_to_kill;

    let err = loop {
        match killed_client.get::<_, Option<String>>("a").await {
            // We are racing against the server being shutdown so try until we a get an io error
            Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            Err(err) => break err,
        }
    };
    assert_eq!(err.kind(), ErrorKind::IoError); // Shouldn't this be IoError?
}

#[tokio::test]
async fn invalid_password_issue_343() {
    let ctx = TestContext::new();
    let coninfo = redis::ConnectionInfo {
        addr: ctx.server.client_addr().clone(),
        redis: redis::RedisConnectionInfo {
            db: 0,
            username: None,
            password: Some("asdcasc".to_string()),
        },
    };
    let client = redis::Client::open(coninfo).unwrap();

    let err = client
        .get_multiplexed_tokio_connection()
        .await
        .err()
        .unwrap();
    assert_eq!(
        err.kind(),
        ErrorKind::AuthenticationFailed,
        "Unexpected error: {err}",
    );
}

// Test issue of Stream trait blocking if we try to iterate more than 10 items
// https://github.com/mitsuhiko/redis-rs/issues/537 and https://github.com/mitsuhiko/redis-rs/issues/583
#[tokio::test]
async fn test_issue_stream_blocks() {
    let ctx = TestContext::new();
    let mut con = ctx.multiplexed_async_connection().await.unwrap();
    for i in 0..20usize {
        let _: () = con.append(format!("test/{i}"), i).await.unwrap();
    }
    let values = con.scan_match::<&str, String>("test/*").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(100), async move {
        let values: Vec<_> = values.collect().await;
        assert_eq!(values.len(), 20);
    })
    .await
    .unwrap();
}

// Test issue of AsyncCommands::scan returning the wrong number of keys
// https://github.com/redis-rs/redis-rs/issues/759
#[tokio::test]
async fn test_issue_async_commands_scan_broken() {
    let ctx = TestContext::new();
    let mut con = ctx.async_connection().await.unwrap();
    let mut keys: Vec<String> = (0..100).map(|k| format!("async-key{k}")).collect();
    keys.sort();
    for key in &keys {
        let _: () = con.set(key, b"foo").await.unwrap();
    }

    let iter: redis::AsyncIter<String> = con.scan().await.unwrap();
    let mut keys_from_redis: Vec<_> = iter.collect().await;
    keys_from_redis.sort();
    assert_eq!(keys, keys_from_redis);
    assert_eq!(keys.len(), 100);
}

mod pub_sub {
    use std::time::Duration;

    use super::*;

    #[test]
    fn pub_sub_subscription() {
        use redis::RedisError;

        let ctx = TestContext::new();
        block_on_all(async move {
            let mut pubsub_conn = ctx.async_pubsub().await?;
            pubsub_conn.subscribe("phonewave").await?;
            let mut pubsub_stream = pubsub_conn.on_message();
            let mut publish_conn = ctx.async_connection().await?;
            publish_conn.publish("phonewave", "banana").await?;

            let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
            assert_eq!("banana".to_string(), msg_payload);

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn pub_sub_unsubscription() {
        use redis::RedisError;

        const SUBSCRIPTION_KEY: &str = "phonewave-pub-sub-unsubscription";

        let ctx = TestContext::new();
        block_on_all(async move {
            let mut pubsub_conn = ctx.async_pubsub().await?;
            pubsub_conn.subscribe(SUBSCRIPTION_KEY).await?;
            pubsub_conn.unsubscribe(SUBSCRIPTION_KEY).await?;

            let mut conn = ctx.async_connection().await?;
            let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                .arg("NUMSUB")
                .arg(SUBSCRIPTION_KEY)
                .query_async(&mut conn)
                .await?;
            let subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
            assert_eq!(subscription_count, 0);

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn automatic_unsubscription() {
        use redis::RedisError;

        const SUBSCRIPTION_KEY: &str = "phonewave-automatic-unsubscription";

        let ctx = TestContext::new();
        block_on_all(async move {
            let mut pubsub_conn = ctx.async_pubsub().await?;
            pubsub_conn.subscribe(SUBSCRIPTION_KEY).await?;
            drop(pubsub_conn);

            let mut conn = ctx.async_connection().await?;
            let mut subscription_count = 1;
            // Allow for the unsubscription to occur within 5 seconds
            for _ in 0..100 {
                let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                    .arg("NUMSUB")
                    .arg(SUBSCRIPTION_KEY)
                    .query_async(&mut conn)
                    .await?;
                subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
                if subscription_count == 0 {
                    break;
                }

                std::thread::sleep(Duration::from_millis(50));
            }
            assert_eq!(subscription_count, 0);

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn pub_sub_conn_reuse() {
        use redis::RedisError;

        let ctx = TestContext::new();
        block_on_all(async move {
            let mut pubsub_conn = ctx.async_pubsub().await?;
            pubsub_conn.subscribe("phonewave").await?;
            pubsub_conn.psubscribe("*").await?;

            #[allow(deprecated)]
            let mut conn = pubsub_conn.into_connection().await;
            redis::cmd("SET")
                .arg("foo")
                .arg("bar")
                .query_async(&mut conn)
                .await?;

            let res: String = redis::cmd("GET").arg("foo").query_async(&mut conn).await?;
            assert_eq!(&res, "bar");

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn pipe_errors_do_not_affect_subsequent_commands() {
        use redis::RedisError;

        let ctx = TestContext::new();
        block_on_all(async move {
            let mut conn = ctx.multiplexed_async_connection().await?;

            conn.lpush::<&str, &str, ()>("key", "value").await?;

            let res: Result<(String, usize), redis::RedisError> = redis::pipe()
                .get("key") // WRONGTYPE
                .llen("key")
                .query_async(&mut conn)
                .await;

            assert!(res.is_err());

            let list: Vec<String> = conn.lrange("key", 0, -1).await?;

            assert_eq!(list, vec!["value".to_owned()]);

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }
}

#[test]
fn test_async_basic_pipe_with_parsing_error() {
    // Tests a specific case involving repeated errors in transactions.
    let ctx = TestContext::new();

    block_on_all(async move {
        let mut conn = ctx.multiplexed_async_connection().await?;

        // create a transaction where 2 errors are returned.
        // we call EVALSHA twice with no loaded script, thus triggering 2 errors.
        redis::pipe()
            .atomic()
            .cmd("EVALSHA")
            .arg("foobar")
            .arg(0)
            .cmd("EVALSHA")
            .arg("foobar")
            .arg(0)
            .query_async::<_, ((), ())>(&mut conn)
            .await
            .expect_err("should return an error");

        assert!(
            // Arbitrary Redis command that should not return an error.
            redis::cmd("SMEMBERS")
                .arg("nonexistent_key")
                .query_async::<_, Vec<String>>(&mut conn)
                .await
                .is_ok(),
            "Failed transaction should not interfere with future calls."
        );

        Ok::<_, redis::RedisError>(())
    })
    .unwrap()
}

#[cfg(feature = "connection-manager")]
async fn wait_for_server_to_become_ready(client: redis::Client) {
    let millisecond = std::time::Duration::from_millis(1);
    let mut retries = 0;
    loop {
        match client.get_multiplexed_async_connection().await {
            Err(err) => {
                if err.is_connection_refusal() {
                    tokio::time::sleep(millisecond).await;
                    retries += 1;
                    if retries > 100000 {
                        panic!("Tried to connect too many times, last error: {err}");
                    }
                } else {
                    panic!("Could not connect: {err}");
                }
            }
            Ok(mut con) => {
                let _: RedisResult<()> = redis::cmd("FLUSHDB").query_async(&mut con).await;
                break;
            }
        }
    }
}

#[test]
#[cfg(feature = "connection-manager")]
fn test_connection_manager_reconnect_after_delay() {
    let tempdir = tempfile::Builder::new()
        .prefix("redis")
        .tempdir()
        .expect("failed to create tempdir");
    let tls_files = build_keys_and_certs_for_tls(&tempdir);

    let ctx = TestContext::with_tls(tls_files.clone(), false);
    block_on_all(async move {
        let mut manager = redis::aio::ConnectionManager::new(ctx.client.clone())
            .await
            .unwrap();
        let server = ctx.server;
        let addr = server.client_addr().clone();
        drop(server);

        let _result: RedisResult<redis::Value> = manager.set("foo", "bar").await; // one call is ignored because it's required to trigger the connection manager's reconnect.

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let _new_server = RedisServer::new_with_addr_and_modules(addr.clone(), &[], false);
        wait_for_server_to_become_ready(ctx.client.clone()).await;

        let result: redis::Value = manager.set("foo", "bar").await.unwrap();
        assert_eq!(result, redis::Value::Okay);
        Ok(())
    })
    .unwrap();
}

#[cfg(feature = "tls-rustls")]
mod mtls_test {
    use super::*;

    #[test]
    fn test_should_connect_mtls() {
        let ctx = TestContext::new_with_mtls();

        let client =
            build_single_client(ctx.server.connection_info(), &ctx.server.tls_paths, true).unwrap();
        let connect = client.get_multiplexed_async_connection();
        block_on_all(connect.and_then(|mut con| async move {
            redis::cmd("SET")
                .arg("key1")
                .arg(b"foo")
                .query_async(&mut con)
                .await?;
            let result = redis::cmd("GET").arg(&["key1"]).query_async(&mut con).await;
            assert_eq!(result, Ok("foo".to_string()));
            result
        }))
        .unwrap();
    }

    #[test]
    fn test_should_not_connect_if_tls_active() {
        let ctx = TestContext::new_with_mtls();

        let client =
            build_single_client(ctx.server.connection_info(), &ctx.server.tls_paths, false)
                .unwrap();
        let connect = client.get_multiplexed_async_connection();
        let result = block_on_all(connect.and_then(|mut con| async move {
            redis::cmd("SET")
                .arg("key1")
                .arg(b"foo")
                .query_async(&mut con)
                .await?;
            let result = redis::cmd("GET").arg(&["key1"]).query_async(&mut con).await;
            assert_eq!(result, Ok("foo".to_string()));
            result
        }));

        // depends on server type set (REDISRS_SERVER_TYPE)
        match ctx.server.connection_info() {
            redis::ConnectionInfo {
                addr: redis::ConnectionAddr::TcpTls { .. },
                ..
            } => {
                if result.is_ok() {
                    panic!("Must NOT be able to connect without client credentials if server accepts TLS");
                }
            }
            _ => {
                if result.is_err() {
                    panic!("Must be able to connect without client credentials if server does NOT accept TLS");
                }
            }
        }
    }
}
