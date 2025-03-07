use redis::TypedCommands;

fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut con = client.get_connection()?;

    con.set("hello", "world")?;

    let hello = con.get("hello")?;

    assert_eq!(hello, Some("world".to_string()));

    con.set("counter", 0)?;

    con.incr("counter", 1)?;

    let new_value = con.incr("counter", 3)?;
    assert_eq!(new_value, 4);

    con.set("goodbye", "world")?;

    let num_deleted = con.del("goodbye")?;

    assert_eq!(num_deleted, 1);

    let value_type = con.key_type("hello")?;
    assert_eq!(value_type, redis::ValueType::String);

    con.del("mylist")?;
    
    con.lpush("mylist", "a")?;
    con.lpush("mylist", "b")?;
    con.lpush("mylist", "c")?;

    let list = con.lrange("mylist", 0, -1)?;
    assert_eq!(list, vec!["c", "b", "a"]);

    Ok(())
}
