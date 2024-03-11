use futures::prelude::*;
use redis::{aio::MultiplexedConnection, RedisResult};

async fn test_cmd(con: &MultiplexedConnection, i: i32) -> RedisResult<()> {
    let mut con = con.clone();

    let key = format!("key{i}");
    let key2 = format!("key{i}_2");
    let value = format!("foo{i}");

    redis::cmd("SET")
        .arg(&key)
        .arg(&value)
        .query_async(&mut con)
        .await?;

    redis::cmd("SET")
        .arg(&[&key2, "bar"])
        .query_async(&mut con)
        .await?;

    redis::cmd("MGET")
        .arg(&[&key, &key2])
        .query_async(&mut con)
        .map(|result| {
            assert_eq!(Ok((value, b"bar".to_vec())), result);
            Ok(())
        })
        .await
}

#[tokio::main]
async fn main() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();

    let con = client.get_multiplexed_tokio_connection().await.unwrap();

    let cmds = (0..100).map(|i| test_cmd(&con, i));
    let result = future::try_join_all(cmds).await.unwrap();

    assert_eq!(100, result.len());
}
