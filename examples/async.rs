use redis;

use futures;

use futures::{Future};

use tokio::runtime::current_thread::block_on_all;

fn do_redis_async_code(url: &str) -> redis::RedisResult<()> {
    let client = redis::Client::open(url)?;
    let connect = client.get_async_connection();

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
    Ok(())
}

fn main() {
    let url = if std::env::args().nth(1) == Some("--tls".into()) {
        "redis+tls://127.0.0.1:6380/#insecure"
    } else {
        "redis://127.0.0.1:6379/"
    };
    // at this point the errors are fatal, let's just fail hard.
    match do_redis_async_code(url) {
        Err(err) => {
            println!("Could not execute example:");
            println!("  {:?}", err);
        }
        Ok(()) => {
            println!("success!");
        }
    }
}
