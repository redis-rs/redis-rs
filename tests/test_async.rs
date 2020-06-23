use futures::{future, prelude::*};
use redis::{aio::MultiplexedConnection, cmd, AsyncCommands, ErrorKind, RedisResult};

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
fn dont_panic_on_closed_multiplexed_connection() {
    let ctx = TestContext::new();
    let connect = ctx.multiplexed_async_connection();
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
            .await
    });
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

fn test_cmd(con: &MultiplexedConnection, i: i32) -> impl Future<Output = RedisResult<()>> + Send {
    let mut con = con.clone();
    async move {
        let key = format!("key{}", i);
        let key_2 = key.clone();
        let key2 = format!("key{}_2", i);
        let key2_2 = key2.clone();

        let foo_val = format!("foo{}", i);

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
                        let bar_val = format!("bar{}", i);

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

#[test]
fn test_async_scanning() {
    let ctx = TestContext::new();
    block_on_all(async move {
        ctx.multiplexed_async_connection()
            .and_then(|mut con| {
                async move {
                    let mut unseen = std::collections::HashSet::new();

                    for x in 0..1000 {
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
                        unseen.remove(&x);
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
#[cfg(feature = "script")]
fn test_script() {
    use redis::RedisError;

    // Note this test runs both scripts twice to test when they have already been loaded
    // into Redis and when they need to be loaded in
    let script1 = redis::Script::new("return redis.call('SET', KEYS[1], ARGV[1])");
    let script2 = redis::Script::new("return redis.call('GET', KEYS[1])");

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
        script1
            .key("key1")
            .arg("bar")
            .invoke_async(&mut con)
            .await?;
        let val: String = script2.key("key1").invoke_async(&mut con).await?;
        assert_eq!(val, "bar");
        Ok(())
    })
    .map_err(|err: RedisError| err)
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
                assert_eq!(b, true);
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

    eprintln!("{}", client_list);
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
            Ok(_) => tokio::time::delay_for(std::time::Duration::from_millis(50)).await,
            Err(err) => break err,
        }
    };
    assert_eq!(err.kind(), ErrorKind::IoError); // Shouldn't this be IoError?
}

#[tokio::test]
async fn invalid_password_issue_343() {
    let ctx = TestContext::new();
    let coninfo = redis::ConnectionInfo {
        addr: Box::new(ctx.server.get_client_addr().clone()),
        db: 0,
        username: None,
        passwd: Some("asdcasc".to_string()),
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
        "Unexpected error: {}",
        err
    );
}

mod pub_sub {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;

    #[test]
    fn pub_sub_subscription() {
        use redis::RedisError;

        let ctx = TestContext::new();
        block_on_all(async move {
            let mut pubsub_conn = ctx.async_connection().await?.into_pubsub();
            pubsub_conn.subscribe("phonewave").await?;
            let mut pubsub_stream = pubsub_conn.on_message();
            let mut publish_conn = ctx.async_connection().await?;
            publish_conn.publish("phonewave", "banana").await?;

            let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
            assert_eq!("banana".to_string(), msg_payload);

            Ok(())
        })
        .map_err(|err: RedisError| err)
        .unwrap();
    }

    #[test]
    fn pub_sub_unsubscription() {
        use redis::RedisError;

        const SUBSCRIPTION_KEY: &str = "phonewave-pub-sub-unsubscription";

        let ctx = TestContext::new();
        block_on_all(async move {
            let mut pubsub_conn = ctx.async_connection().await?.into_pubsub();
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

            Ok(())
        })
        .map_err(|err: RedisError| err)
        .unwrap();
    }

    #[test]
    fn automatic_unsubscription() {
        use redis::RedisError;

        const SUBSCRIPTION_KEY: &str = "phonewave-automatic-unsubscription";

        let ctx = TestContext::new();
        block_on_all(async move {
            let mut pubsub_conn = ctx.async_connection().await?.into_pubsub();
            pubsub_conn.subscribe(SUBSCRIPTION_KEY).await?;
            drop(pubsub_conn);

            std::thread::sleep(Duration::from_millis(50));

            let mut conn = ctx.async_connection().await?;
            let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                .arg("NUMSUB")
                .arg(SUBSCRIPTION_KEY)
                .query_async(&mut conn)
                .await?;
            let subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
            assert_eq!(subscription_count, 0);

            Ok(())
        })
        .map_err(|err: RedisError| err)
        .unwrap();
    }

    #[test]
    fn pub_sub_conn_reuse() {
        use redis::RedisError;

        let ctx = TestContext::new();
        block_on_all(async move {
            let mut pubsub_conn = ctx.async_connection().await?.into_pubsub();
            pubsub_conn.subscribe("phonewave").await?;
            pubsub_conn.psubscribe("*").await?;

            let mut conn = pubsub_conn.into_connection().await;
            redis::cmd("SET")
                .arg("foo")
                .arg("bar")
                .query_async(&mut conn)
                .await?;

            let res: String = redis::cmd("GET").arg("foo").query_async(&mut conn).await?;
            assert_eq!(&res, "bar");

            Ok(())
        })
        .map_err(|err: RedisError| err)
        .unwrap();
    }
}
