use redis::{caching::CacheConfig, AsyncConnectionConfig};

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/?protocol=resp3").unwrap();

    let conf = AsyncConnectionConfig::new().set_cache_config(CacheConfig::default());
    let mut con = client
        .get_multiplexed_async_connection_with_config(&conf)
        .await?;

    redis::cmd("SET")
        .arg("KEY")
        .arg("7")
        .exec_async(&mut con)
        .await?;

    for _ in 0..10_000_000 {
        let result: i32 = redis::cmd("GET").arg("KEY").query_async(&mut con).await?;
        assert_eq!(result, 7);
    }
    // Result should be CacheStatistics { hit: 9999999, miss: 1, invalidate: 0 }
    println!("caching result: {:?}", con.get_cache_statistics());
    Ok(())
}
