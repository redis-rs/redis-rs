extern crate futures;
extern crate redis;
extern crate tokio;
use futures::{future, prelude::*};
use redis::{aio::MultiplexedConnection, RedisResult};

fn test_cmd(con: &MultiplexedConnection, i: i32) -> impl Future<Output = RedisResult<()>> + Send {
    let mut con = con.clone();
    async move {
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
}

async fn run_multiplexed() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let (con, driver) = client.get_multiplexed_async_connection().await.unwrap();

    tokio::spawn(driver);
    let cmds = (0..100).map(move |i| test_cmd(&con, i));
    future::try_join_all(cmds).await.unwrap();
}

#[tokio::main]
async fn main() {
    println!("Main starting...");
    tokio::spawn(async move {
        run_multiplexed().await;
    })
    .await
    .unwrap();
    println!("Main done.");
}
