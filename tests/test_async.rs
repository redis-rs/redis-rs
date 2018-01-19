#![cfg(not(any(feature = "with-unix-sockets", feature = "with-system-unix-sockets")))]
extern crate redis;

extern crate futures;
extern crate tokio;

use futures::Future;

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
