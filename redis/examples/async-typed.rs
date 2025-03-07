use redis::AsyncTypedCommands;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut con = client.get_multiplexed_async_connection().await?;

    con.set("hello", "world").await?;

    let hello = con.get("hello").await?;

    assert_eq!(hello, Some("world".to_string()));

    con.set("counter", 0).await?;

    con.incr("counter", 1).await?;

    let new_value = con.incr("counter", 3).await?;
    assert_eq!(new_value, 4);

    con.set("goodbye", "world").await?;

    let num_deleted = con.del("goodbye").await?;

    assert_eq!(num_deleted, 1);

    let value_type = con.key_type("hello").await?;
    assert_eq!(value_type, redis::ValueType::String);

    con.lpush("mylist", "a").await?;
    con.lpush("mylist", "b").await?;
    con.lpush("mylist", "c").await?;

    let list = con.lrange("mylist", 0, -1).await?;
    assert_eq!(list, vec!["c", "b", "a"]);

    Ok(())
}
