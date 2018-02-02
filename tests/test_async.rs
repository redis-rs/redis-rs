extern crate redis;

extern crate futures;
extern crate tokio;

use futures::{future, Future};

use support::*;

use tokio::executor::current_thread::block_on_all;

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
    })).unwrap();
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
    })).unwrap();
}

#[test]
fn test_args_shared_connection() {
    let ctx = TestContext::new();
    tokio::run(
        ctx.shared_async_connection()
            .and_then(|con| {
                let cmds = (0..100).map(move |i| {
                    let key = format!("key{}", i);
                    let key_2 = key.clone();
                    let key2 = format!("key{}_2", i);
                    let key2_2 = key2.clone();

                    let foo = format!("foo{}", i);

                    let con1 = con.clone();
                    let con2 = con.clone();
                    redis::cmd("SET")
                        .arg(&key[..])
                        .arg(foo.as_bytes())
                        .query_async(con.clone())
                        .and_then(move |(_, ())| {
                            redis::cmd("SET").arg(&[&key2, "bar"]).query_async(con1)
                        })
                        .and_then(move |(_, ())| {
                            redis::cmd("MGET")
                                .arg(&[&key_2, &key2_2])
                                .query_async(con2)
                                .map(|t| t.1)
                                .then(|result| {
                                    assert_eq!(Ok((foo, b"bar".to_vec())), result);
                                    Ok(())
                                })
                        })
                });
                future::join_all(cmds).map(|results| {
                    assert_eq!(results.len(), 100);
                })
            })
            .map_err(|err| panic!("{}", err)),
    );
}

#[test]
fn test_transaction_shared_connection() {
    let ctx = TestContext::new();
    tokio::run(
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
            .map_err(|err| panic!("{}", err)),
    );
}
