use redis;

use futures;


use futures::{future, Future};

use crate::support::*;

use redis::aio::SharedConnection;
use redis::RedisError;

use tokio::runtime::current_thread::block_on_all;

mod support;

#[test]
fn test_args() {
    let ctx = TestContext::new();
    let connect = ctx.async_connection();

    block_on_all(connect.and_then(|con| {
        redis::cmd("SET")
            .arg("key1")
            .arg(b"foo")
            .query_async(con)
            .and_then(|(con, ())| redis::cmd("SET").arg(&["key2", "bar"]).query_async(con))
            .and_then(|(con, ())| {
                redis::cmd("MGET")
                    .arg(&["key1", "key2"])
                    .query_async(con)
                    .map(|t| t.1)
            })
            .then(|result| {
                assert_eq!(result, Ok(("foo".to_string(), b"bar".to_vec())));
                result
            })
    }))
    .unwrap();
}

#[test]
fn dont_panic_on_closed_shared_connection() {
    let ctx = TestContext::new();
    let connect = ctx.shared_async_connection();
    drop(ctx);

    block_on_all(future::lazy(|| {
        connect
            .and_then(|con| {
                let cmd = move || {
                    redis::cmd("SET")
                        .arg("key1")
                        .arg(b"foo")
                        .query_async(con.clone())
                        .map(|(_, ())| ())
                };
                cmd().then(move |result| {
                    assert_eq!(
                        result.as_ref().unwrap_err().kind(),
                        redis::ErrorKind::IoError,
                        "{}",
                        result.as_ref().unwrap_err()
                    );
                    cmd()
                })
            })
            .then(|result| -> Result<(), ()> {
                assert_eq!(
                    result.as_ref().unwrap_err().kind(),
                    redis::ErrorKind::IoError,
                    "{}",
                    result.as_ref().unwrap_err()
                );
                Ok(())
            })
    }))
    .unwrap();
}

#[test]
fn test_pipeline_transaction() {
    let ctx = TestContext::new();
    block_on_all(ctx.async_connection().and_then(|con| {
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
        pipe.query_async(con)
            .and_then(|(_con, ((k1, k2),)): (_, ((i32, i32),))| {
                assert_eq!(k1, 42);
                assert_eq!(k2, 43);
                Ok(())
            })
    }))
    .unwrap();
}

fn test_cmd(
    con: &SharedConnection,
    i: i32,
) -> Box<dyn Future<Item = (), Error = RedisError> + Send> {
    let key = format!("key{}", i);
    let key_2 = key.clone();
    let key2 = format!("key{}_2", i);
    let key2_2 = key2.clone();

    let foo = format!("foo{}", i);

    let con1 = con.clone();
    let con2 = con.clone();
    Box::new(
        redis::cmd("SET")
            .arg(&key[..])
            .arg(foo.as_bytes())
            .query_async(con.clone())
            .and_then(move |(_, ())| redis::cmd("SET").arg(&[&key2, "bar"]).query_async(con1))
            .and_then(move |(_, ())| {
                redis::cmd("MGET")
                    .arg(&[&key_2, &key2_2])
                    .query_async(con2)
                    .map(|t| t.1)
                    .then(|result| {
                        assert_eq!(Ok((foo, b"bar".to_vec())), result);
                        Ok(())
                    })
            }),
    )
}

fn test_error(con: &SharedConnection) -> Box<dyn Future<Item = (), Error = RedisError> + Send> {
    Box::new(
        redis::cmd("SET")
            .query_async(con.clone())
            .then(|result| match result {
                Ok((_, ())) => panic!("Expected redis to return an error"),
                Err(_) => Ok(()),
            }),
    )
}

#[test]
fn test_args_shared_connection() {
    let ctx = TestContext::new();
    block_on_all(future::lazy(|| {
        ctx.shared_async_connection()
            .and_then(|con| {
                let cmds = (0..100).map(move |i| test_cmd(&con, i));
                future::join_all(cmds).map(|results| {
                    assert_eq!(results.len(), 100);
                })
            })
            .map_err(|err| panic!("{}", err))
    }))
    .unwrap();
}

#[test]
fn test_args_with_errors_shared_connection() {
    let ctx = TestContext::new();
    block_on_all(future::lazy(|| {
        ctx.shared_async_connection()
            .and_then(|con| {
                let cmds = (0..100).map(move |i| {
                    if i % 2 == 0 {
                        test_cmd(&con, i)
                    } else {
                        test_error(&con)
                    }
                });
                future::join_all(cmds).map(|results| {
                    assert_eq!(results.len(), 100);
                })
            })
            .map_err(|err| panic!("{}", err))
    }))
    .unwrap();
}

#[test]
fn test_transaction_shared_connection() {
    let ctx = TestContext::new();
    block_on_all(future::lazy(|| {
        ctx.shared_async_connection()
            .and_then(|con| {
                let cmds = (0..100).map(move |i| {
                    let foo = i;
                    let bar = format!("bar{}", i);

                    let mut pipe = redis::pipe();
                    pipe.atomic()
                        .cmd("SET")
                        .arg("key")
                        .arg(foo)
                        .ignore()
                        .cmd("SET")
                        .arg(&["key2", &bar[..]])
                        .ignore()
                        .cmd("MGET")
                        .arg(&["key", "key2"]);

                    pipe.query_async(con.clone())
                        .map(|t| t.1)
                        .then(move |result| {
                            assert_eq!(Ok(((foo, bar.clone().into_bytes()),)), result);
                            result
                        })
                });
                future::join_all(cmds)
            })
            .and_then(|results| {
                assert_eq!(results.len(), 100);
                Ok(())
            })
            .map_err(|err| panic!("{}", err))
    }))
    .unwrap();
}

#[test]
fn test_script() {
    // Note this test runs both scripts twice to test when they have already been loaded
    // into Redis and when they need to be loaded in
    let script1 = redis::Script::new("return redis.call('SET', KEYS[1], ARGV[1])");
    let script2 = redis::Script::new("return redis.call('GET', KEYS[1])");

    let ctx = TestContext::new();

    block_on_all(future::lazy(|| {
        ctx.shared_async_connection().and_then(|con| {
            script1
                .key("key1")
                .arg("foo")
                .invoke_async(con)
                .and_then(|(con, ())| script2.key("key1").invoke_async(con))
                .and_then(|(con, val): (SharedConnection, String)| {
                    assert_eq!(val, "foo");
                    script1.key("key1").arg("bar").invoke_async(con)
                })
                .and_then(|(con, ())| script2.key("key1").invoke_async(con))
                .and_then(|(_con, val): (SharedConnection, String)| {
                    assert_eq!(val, "bar");
                    Ok(())
                })
        })
    }))
    .unwrap();
}
