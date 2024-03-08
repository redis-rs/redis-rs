use futures::prelude::*;

use crate::support::*;

use redis::{aio::MultiplexedConnection, RedisResult};

mod support;

#[test]
fn test_args() {
    let ctx = TestContext::new();
    let connect = ctx.async_connection_async_std();

    block_on_all_using_async_std(connect.and_then(|mut con| async move {
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
fn test_args_async_std() {
    let ctx = TestContext::new();
    let connect = ctx.async_connection_async_std();

    block_on_all_using_async_std(connect.and_then(|mut con| async move {
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
    let client = ctx.client.clone();
    let connect = client.get_multiplexed_async_std_connection();
    drop(ctx);

    block_on_all_using_async_std(async move {
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
    block_on_all_using_async_std(async move {
        let mut con = ctx.async_connection_async_std().await?;
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
fn test_args_multiplexed_connection() {
    let ctx = TestContext::new();
    block_on_all_using_async_std(async move {
        ctx.multiplexed_async_connection_async_std()
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
    block_on_all_using_async_std(async move {
        ctx.multiplexed_async_connection_async_std()
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
    block_on_all_using_async_std(async move {
        ctx.multiplexed_async_connection_async_std()
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

    block_on_all_using_async_std(async move {
        let mut con = ctx.multiplexed_async_connection_async_std().await?;
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
        let mut con = ctx.multiplexed_async_connection_async_std().await.unwrap();

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
    block_on_all_using_async_std(async {
        let mut con = ctx.multiplexed_async_connection_async_std().await?;
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
