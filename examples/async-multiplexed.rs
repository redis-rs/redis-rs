extern crate futures;
extern crate redis;
use futures::{future, prelude::*};
use redis::{aio::MultiplexedConnection, RedisResult};

async fn test_cmd(con: &MultiplexedConnection, i: i32) -> RedisResult<()> {
    let mut con = con.clone();
    let key = format!("key{}", i);
    let key_2 = key.clone();
    let key2 = format!("key{}_2", i);
    let key2_2 = key2.clone();

    let foo_val = format!("foo{}", i);

    let () = redis::cmd("SET")
        .arg(&key[..])
        .arg(foo_val.as_bytes())
        .query_async(&mut con)
        .await?;
    let () = redis::cmd("SET")
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

#[tokio::main]
async fn main() {
    println!("Main starting...");

    let client = redis::Client::open("redis://127.0.0.1/").unwrap();

    // features = ["tokio-rt-core"] must be specified on redis crate
    let con = client.get_multiplexed_tokio_connection().await.unwrap();

    let cmds = (0..100).map(|i| test_cmd(&con, i));
    let result = future::try_join_all(cmds).await.unwrap();
    assert_eq!(100, result.len());

    println!("Main done.");
}
