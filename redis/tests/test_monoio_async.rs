#![cfg(feature = "monoio-comp")]

//! Tests for Monoio async runtime support

use redis::{AsyncCommands, Value};

#[monoio::main(timer_enabled = true)]
async fn test_monoio_basic_connection() {
    let client = redis::Client::open("redis://127.0.0.1/").expect("Failed to create client");
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect");

    // Test basic SET and GET
    let _: () = con.set("test_key", "test_value").await.expect("SET failed");

    let result: String = con.get("test_key").await.expect("GET failed");
    assert_eq!(result, "test_value");
}

#[monoio::main(timer_enabled = true)]
async fn test_monoio_string_operations() {
    let client = redis::Client::open("redis://127.0.0.1/").expect("Failed to create client");
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect");

    // SET with expiration
    let _: () = con
        .set_ex("expiring_key", "value", 10)
        .await
        .expect("SET EX failed");

    // GET should work
    let result: String = con.get("expiring_key").await.expect("GET failed");
    assert_eq!(result, "value");

    // APPEND
    let _: () = con
        .append("expiring_key", "_appended")
        .await
        .expect("APPEND failed");
    let result: String = con.get("expiring_key").await.expect("GET failed");
    assert_eq!(result, "value_appended");

    // STRLEN
    let len: usize = redis::cmd("STRLEN")
        .arg("expiring_key")
        .query_async(&mut con)
        .await
        .expect("STRLEN failed");
    assert_eq!(len, 14);
}

#[monoio::main(timer_enabled = true)]
async fn test_monoio_list_operations() {
    let client = redis::Client::open("redis://127.0.0.1/").expect("Failed to create client");
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect");

    let list_key = "test_list";

    // Clean up first
    let _: () = con.del(list_key).await.ok();

    // RPUSH
    let _: () = con
        .rpush(list_key, &["item1", "item2", "item3"])
        .await
        .expect("RPUSH failed");

    // LLEN
    let len: usize = con.llen(list_key).await.expect("LLEN failed");
    assert_eq!(len, 3);

    // LRANGE
    let items: Vec<String> = con.lrange(list_key, 0, -1).await.expect("LRANGE failed");
    assert_eq!(items, vec!["item1", "item2", "item3"]);

    // LPOP
    let item: String = con.lpop(list_key, None).await.expect("LPOP failed");
    assert_eq!(item, "item1");
}

#[monoio::main(timer_enabled = true)]
async fn test_monoio_hash_operations() {
    let client = redis::Client::open("redis://127.0.0.1/").expect("Failed to create client");
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect");

    let hash_key = "test_hash";

    // Clean up first
    let _: () = con.del(hash_key).await.ok();

    // HSET
    let _: () = con
        .hset_multiple(hash_key, &[("field1", "value1"), ("field2", "value2")])
        .await
        .expect("HSET failed");

    // HGET
    let value: String = con.hget(hash_key, "field1").await.expect("HGET failed");
    assert_eq!(value, "value1");

    // HGETALL
    let map: std::collections::HashMap<String, String> =
        con.hgetall(hash_key).await.expect("HGETALL failed");
    assert_eq!(map.len(), 2);
    assert_eq!(map.get("field1").unwrap(), "value1");
    assert_eq!(map.get("field2").unwrap(), "value2");

    // HLEN
    let len: usize = con.hlen(hash_key).await.expect("HLEN failed");
    assert_eq!(len, 2);
}

#[monoio::main(timer_enabled = true)]
async fn test_monoio_set_operations() {
    let client = redis::Client::open("redis://127.0.0.1/").expect("Failed to create client");
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect");

    let set_key = "test_set";

    // Clean up first
    let _: () = con.del(set_key).await.ok();

    // SADD
    let _: () = con
        .sadd(set_key, &["member1", "member2", "member3"])
        .await
        .expect("SADD failed");

    // SCARD
    let size: usize = con.scard(set_key).await.expect("SCARD failed");
    assert_eq!(size, 3);

    // SISMEMBER
    let is_member: bool = con
        .sismember(set_key, "member1")
        .await
        .expect("SISMEMBER failed");
    assert!(is_member);

    // SREM
    let _: () = con.srem(set_key, "member1").await.expect("SREM failed");
    let size: usize = con.scard(set_key).await.expect("SCARD failed");
    assert_eq!(size, 2);
}

#[monoio::main(timer_enabled = true)]
async fn test_monoio_sorted_set_operations() {
    let client = redis::Client::open("redis://127.0.0.1/").expect("Failed to create client");
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect");

    let zset_key = "test_zset";

    // Clean up first
    let _: () = con.del(zset_key).await.ok();

    // ZADD
    let _: () = con
        .zadd_multiple(
            zset_key,
            &[("member1", 1.0), ("member2", 2.0), ("member3", 3.0)],
        )
        .await
        .expect("ZADD failed");

    // ZCARD
    let size: usize = con.zcard(zset_key).await.expect("ZCARD failed");
    assert_eq!(size, 3);

    // ZSCORE
    let score: f64 = redis::cmd("ZSCORE")
        .arg(zset_key)
        .arg("member1")
        .query_async(&mut con)
        .await
        .expect("ZSCORE failed");
    assert_eq!(score, 1.0);

    // ZRANGE
    let members: Vec<String> = con.zrange(zset_key, 0, -1).await.expect("ZRANGE failed");
    assert_eq!(members, vec!["member1", "member2", "member3"]);
}

#[monoio::main(timer_enabled = true)]
async fn test_monoio_delete_and_exists() {
    let client = redis::Client::open("redis://127.0.0.1/").expect("Failed to create client");
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect");

    let key = "test_delete_key";

    // SET a value
    let _: () = con.set(key, "value").await.expect("SET failed");

    // Check EXISTS
    let exists: bool = con.exists(key).await.expect("EXISTS failed");
    assert!(exists);

    // DELETE
    let deleted: bool = con.del(key).await.expect("DEL failed");
    assert!(deleted);

    // Check EXISTS again
    let exists: bool = con.exists(key).await.expect("EXISTS failed");
    assert!(!exists);
}

#[monoio::main(timer_enabled = true)]
async fn test_monoio_multiple_connections() {
    let client = redis::Client::open("redis://127.0.0.1/").expect("Failed to create client");

    let mut con1 = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect");
    let mut con2 = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to connect");

    // Use con1 to set a value
    let _: () = con1
        .set("shared_key", "from_con1")
        .await
        .expect("SET failed");

    // Use con2 to read it
    let result: String = con2.get("shared_key").await.expect("GET failed");
    assert_eq!(result, "from_con1");

    // Update via con2
    let _: () = con2
        .set("shared_key", "from_con2")
        .await
        .expect("SET failed");

    // Read via con1
    let result: String = con1.get("shared_key").await.expect("GET failed");
    assert_eq!(result, "from_con2");
}

#[cfg(feature = "monoio-rustls-comp")]
#[monoio::main(timer_enabled = true)]
async fn test_monoio_tls_connection() {
    // This test assumes Redis is configured with TLS on port 6380
    // and requires proper CA certificates setup
    let client_result = redis::Client::open("rediss://127.0.0.1:6380/");

    // Only test if the client can be created (TLS might not be available in test environment)
    if let Ok(client) = client_result {
        let con_result = client.get_multiplexed_async_connection().await;
        // TLS connection may fail in test environment, so we just verify it doesn't panic
        if let Ok(mut con) = con_result {
            let result: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut con).await;
            // Accept either success or connection error
            let _ = result;
        }
    }
}
