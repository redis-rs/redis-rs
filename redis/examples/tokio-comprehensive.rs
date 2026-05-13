//! Comprehensive Redis example using the tokio runtime.
//!
//! Demonstrates the four major Redis features:
//! 1. Saving/deleting/querying various data types (String, Hash, List, Set, Sorted Set)
//! 2. Pub/Sub publishing and receiving messages
//! 3. Keyspace notification listening for key changes
//! 4. Stream data collection

#![cfg(feature = "tokio-comp")]

use redis::{
    AsyncTypedCommands,
    streams::{StreamId, StreamKey, StreamReadReply},
    RedisResult,
};
use futures_util::StreamExt;
use std::time::Duration;

#[tokio::main]
async fn main() -> RedisResult<()> {
    println!("🚀 Redis Tokio Comprehensive Example\n");

    // 1. Demonstrate various data types operations
    demo_data_types().await?;

    // 2. Demonstrate Pub/Sub
    demo_pubsub().await?;

    // 3. Demonstrate Keyspace notifications
    demo_keyspace_notifications().await?;

    // 4. Demonstrate Streams
    demo_streams().await?;

    println!("\n✅ All examples completed!");
    Ok(())
}

/// 1. Demonstrate saving/deleting/querying various data types
async fn demo_data_types() -> RedisResult<()> {
    println!("📦 1. Data Types Operations");
    println!("{}", "=".repeat(50));

    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_multiplexed_async_connection().await?;

    // String type
    println!("\n🔹 String type (SET → GET → UPDATE → GET):");
    AsyncTypedCommands::set(&mut con, "user:name", "Alice").await?;
    AsyncTypedCommands::set(&mut con, "user:age", 30).await?;
    let name: Option<String> = AsyncTypedCommands::get(&mut con, "user:name").await?;
    let age: Option<String> = AsyncTypedCommands::get(&mut con, "user:age").await?;
    println!("  Initial - name: {:?}, age: {:?}", name, age);

    // Update
    AsyncTypedCommands::set(&mut con, "user:name", "Alice Smith").await?;
    let _: isize = AsyncTypedCommands::incr(&mut con, "user:age", 1).await?;
    let name_updated: Option<String> = AsyncTypedCommands::get(&mut con, "user:name").await?;
    let age_updated: Option<String> = AsyncTypedCommands::get(&mut con, "user:age").await?;
    println!("  Updated - name: {:?}, age: {:?}", name_updated, age_updated);

    // Hash type
    println!("\n🔹 Hash type (HSET → HGETALL → UPDATE → HGETALL):");
    AsyncTypedCommands::hset(&mut con, "user:1000", "name", "Bob").await?;
    AsyncTypedCommands::hset(&mut con, "user:1000", "email", "bob@example.com").await?;
    AsyncTypedCommands::hset(&mut con, "user:1000", "age", "25").await?;
    let user: std::collections::HashMap<String, String> = AsyncTypedCommands::hgetall(&mut con, "user:1000").await?;
    println!("  Initial: {:?}", user);

    // Update Hash fields
    AsyncTypedCommands::hset(&mut con, "user:1000", "email", "bob.updated@example.com").await?;
    AsyncTypedCommands::hset(&mut con, "user:1000", "status", "active").await?;
    let user_updated: std::collections::HashMap<String, String> = AsyncTypedCommands::hgetall(&mut con, "user:1000").await?;
    println!("  Updated: {:?}", user_updated);

    // Get single Hash field
    let email: Option<String> = AsyncTypedCommands::hget(&mut con, "user:1000", "email").await?;
    println!("  HGET email: {:?}", email);

    // List type
    println!("\n🔹 List type (RPUSH → LRANGE → LPUSH → LRANGE):");
    AsyncTypedCommands::del(&mut con, "tasks").await?;
    AsyncTypedCommands::rpush(&mut con, "tasks", "task1").await?;
    AsyncTypedCommands::rpush(&mut con, "tasks", "task2").await?;
    AsyncTypedCommands::rpush(&mut con, "tasks", "task3").await?;
    let tasks: Vec<String> = AsyncTypedCommands::lrange(&mut con, "tasks", 0, -1).await?;
    println!("  Initial list: {:?}", tasks);

    // Insert from left and remove from right
    AsyncTypedCommands::lpush(&mut con, "tasks", "urgent_task").await?;
    let _popped: Option<String> = AsyncTypedCommands::rpop(&mut con, "tasks", None).await?;
    let tasks_updated: Vec<String> = AsyncTypedCommands::lrange(&mut con, "tasks", 0, -1).await?;
    println!("  Updated list: {:?}", tasks_updated);

    // Get list length
    let len: usize = AsyncTypedCommands::llen(&mut con, "tasks").await?;
    println!("  List length: {}", len);

    // Set type
    println!("\n🔹 Set type (SADD → SMEMBERS → UPDATE → SMEMBERS):");
    AsyncTypedCommands::del(&mut con, "tags").await?;
    AsyncTypedCommands::sadd(&mut con, "tags", "rust").await?;
    AsyncTypedCommands::sadd(&mut con, "tags", "redis").await?;
    AsyncTypedCommands::sadd(&mut con, "tags", "tokio").await?;
    let tags: std::collections::HashSet<String> = AsyncTypedCommands::smembers(&mut con, "tags").await?;
    println!("  Initial set: {:?}", tags);

    // Add and remove elements
    AsyncTypedCommands::sadd(&mut con, "tags", "async").await?;
    AsyncTypedCommands::srem(&mut con, "tags", "tokio").await?;
    let tags_updated: std::collections::HashSet<String> = AsyncTypedCommands::smembers(&mut con, "tags").await?;
    println!("  Updated set: {:?}", tags_updated);

    // Check if member exists
    let exists: bool = AsyncTypedCommands::sismember(&mut con, "tags", "rust").await?;
    println!("  'rust' is in set: {}", exists);

    // Sorted Set type
    println!("\n🔹 Sorted Set type (ZADD → ZREVRANGE → UPDATE → ZREVRANGE):");
    AsyncTypedCommands::del(&mut con, "leaderboard").await?;
    AsyncTypedCommands::zadd(&mut con, "leaderboard", "player1", 100).await?;
    AsyncTypedCommands::zadd(&mut con, "leaderboard", "player2", 200).await?;
    AsyncTypedCommands::zadd(&mut con, "leaderboard", "player3", 150).await?;
    let top: Vec<String> = AsyncTypedCommands::zrevrange(&mut con, "leaderboard", 0, 2).await?;
    println!("  Initial top 3 players: {:?}", top);

    // Update scores
    AsyncTypedCommands::zadd(&mut con, "leaderboard", "player1", 250).await?;
    let _: f64 = redis::cmd("ZINCRBY")
        .arg("leaderboard")
        .arg(50)
        .arg("player3")
        .query_async(&mut con)
        .await?;
    let top_updated: Vec<String> = AsyncTypedCommands::zrevrange(&mut con, "leaderboard", 0, 2).await?;
    println!("  Updated top 3 players: {:?}", top_updated);

    // Get member score
    let score: Option<f64> = AsyncTypedCommands::zscore(&mut con, "leaderboard", "player1").await?;
    println!("  player1 score: {:?}", score);

    // Delete operations
    println!("\n🔹 Delete operations:");
    let deleted: usize = AsyncTypedCommands::del(&mut con, "user:name").await?;
    println!("  DEL user:name: {} key deleted", deleted);

    // Verify deletion
    let exists: bool = AsyncTypedCommands::exists(&mut con, "user:name").await?;
    println!("  user:name exists: {}", exists);

    // Batch delete
    let deleted_multiple: usize = AsyncTypedCommands::del(&mut con, &["tasks", "tags", "leaderboard"]).await?;
    println!("  Batch delete {} keys", deleted_multiple);

    println!("\n✅ Data types operations completed\n");
    Ok(())
}

/// 2. Demonstrate Pub/Sub publishing and receiving
async fn demo_pubsub() -> RedisResult<()> {
    use std::time::Instant;

    println!("📢 2. Pub/Sub Demo");
    println!("{}", "=".repeat(50));

    let start_time = Instant::now();

    let client = redis::Client::open("redis://127.0.0.1/")?;

    // Create publisher connection
    let mut pub_conn = client.get_multiplexed_async_connection().await?;

    // Create subscriber connection
    let mut sub_conn = client.get_async_pubsub().await?;

    // Subscribe to channels
    let subscribe_start = Instant::now();
    sub_conn.subscribe("news").await?;
    sub_conn.subscribe("sports").await?;
    let subscribe_elapsed = subscribe_start.elapsed();
    println!("  ✓ Subscribed to channels: news, sports (elapsed: {:?})", subscribe_elapsed);

    // Start subscription task
    let sub_task = tokio::spawn(async move {
        let mut stream = sub_conn.on_message();
        println!("\n  📥 Starting to receive messages...");
        let mut msg_count = 0;
        let first_msg_time = Instant::now();
        for _ in 0..4 {
            if let Some(msg) = stream.next().await {
                msg_count += 1;
                let _elapsed = first_msg_time.elapsed();
                let _channel: String = msg.get_channel_name().to_string();
                let _payload: String = msg.get_payload().unwrap_or_default();
            }
        }
        println!("  ⏱️  Received 4 messages in: {:?}", first_msg_time.elapsed());
    });

    // Wait a moment to ensure subscription is established
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Publish messages
    println!("\n  📤 Publishing messages...");
    let publish_start = Instant::now();
    AsyncTypedCommands::publish(&mut pub_conn, "news", "Breaking: Redis with Tokio!").await?;
    AsyncTypedCommands::publish(&mut pub_conn, "sports", "Game starts at 8pm").await?;
    AsyncTypedCommands::publish(&mut pub_conn, "news", "Latest update available").await?;
    AsyncTypedCommands::publish(&mut pub_conn, "sports", "Final score: 3-1").await?;
    let publish_elapsed = publish_start.elapsed();
    println!("  ⏱️  Published 4 messages in: {:?}", publish_elapsed);

    // Wait for subscription task to complete
    sub_task.await.ok();

    let total_elapsed = start_time.elapsed();
    println!("\n✅ Pub/Sub demo completed (total elapsed: {:?})\n", total_elapsed);
    Ok(())
}

/// 3. Demonstrate Keyspace notification listening
async fn demo_keyspace_notifications() -> RedisResult<()> {
    use std::time::Instant;

    println!("🔔 3. Keyspace Notifications Demo");
    println!("{}", "=".repeat(50));

    let start_time = Instant::now();

    let client = redis::Client::open("redis://127.0.0.1/")?;

    // Enable keyspace notifications
    let mut config_conn = client.get_multiplexed_async_connection().await?;
    let config_start = Instant::now();
    let _: () = redis::cmd("CONFIG")
        .arg(&["SET", "notify-keyspace-events", "KEA"])
        .query_async(&mut config_conn)
        .await?;
    println!("  ✓ Enabled keyspace notifications (elapsed: {:?})", config_start.elapsed());

    // Create notification connection
    let mut notify_conn = client.get_async_pubsub().await?;

    // Subscribe to keyspace events
    let subscribe_start = Instant::now();
    notify_conn.psubscribe("__keyspace@0__:*").await?;
    println!("  ✓ Subscribed to keyspace events (elapsed: {:?})", subscribe_start.elapsed());

    // Start notification task
    let notify_task = tokio::spawn(async move {
        let mut stream = notify_conn.on_message();
        println!("\n  👂 Starting to listen for keyspace events...");
        let mut event_count = 0;
        let first_event_time = Instant::now();
        for _ in 0..6 {
            if let Some(msg) = stream.next().await {
                event_count += 1;
                let _elapsed = first_event_time.elapsed();
                let channel: String = msg.get_channel_name().to_string();
                let _payload: String = msg.get_payload().unwrap_or_default();
                if let Some(_key) = channel.strip_prefix("__keyspace@0__:") {
                    // Key event notification
                }
            }
        }
        println!("  ⏱️  Received 6 events in: {:?}", first_event_time.elapsed());
    });

    // Wait a moment to ensure subscription is established
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Perform operations to trigger events
    println!("\n  🔧 Performing operations to trigger events...");
    let mut op_conn = client.get_multiplexed_async_connection().await?;
    let ops_start = Instant::now();
    AsyncTypedCommands::set(&mut op_conn, "monitored:key1", "value1").await?;
    AsyncTypedCommands::set(&mut op_conn, "monitored:key2", "value2").await?;
    AsyncTypedCommands::hset(&mut op_conn, "monitored:hash", "field", "value").await?;
    AsyncTypedCommands::del(&mut op_conn, "monitored:key1").await?;
    AsyncTypedCommands::expire(&mut op_conn, "monitored:key2", 10).await?;
    AsyncTypedCommands::set(&mut op_conn, "monitored:key3", "value3").await?;
    let ops_elapsed = ops_start.elapsed();
    println!("  ⏱️  Performed 6 operations in: {:?}", ops_elapsed);

    // Wait for notification task to complete
    notify_task.await.ok();

    let total_elapsed = start_time.elapsed();
    println!("\n✅ Keyspace notifications demo completed (total elapsed: {:?})\n", total_elapsed);
    Ok(())
}

/// 4. Demonstrate Stream operations
async fn demo_streams() -> RedisResult<()> {
    use std::time::Instant;

    println!("🌊 4. Stream Data Demo");
    println!("{}", "=".repeat(50));

    let start_time = Instant::now();

    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_multiplexed_async_connection().await?;

    let stream_key = "events:stream";

    // Clean up old data
    let _: usize = AsyncTypedCommands::del(&mut con, stream_key).await?;

    // Add data to stream
    println!("\n  📝 Adding data to stream...");
    let add_start = Instant::now();
    let id1: String = redis::cmd("XADD")
        .arg(&[stream_key, "*", "event", "login", "user", "alice"])
        .query_async(&mut con)
        .await?;
    println!("  ✓ Added event ID: {} (elapsed: {:?})", id1, add_start.elapsed());

    let add2_start = Instant::now();
    let id2: String = redis::cmd("XADD")
        .arg(&[stream_key, "*", "event", "purchase", "amount", "99.99"])
        .query_async(&mut con)
        .await?;
    println!("  ✓ Added event ID: {} (elapsed: {:?})", id2, add2_start.elapsed());

    let add3_start = Instant::now();
    let id3: String = redis::cmd("XADD")
        .arg(&[stream_key, "*", "event", "logout", "user", "alice"])
        .query_async(&mut con)
        .await?;
    println!("  ✓ Added event ID: {} (elapsed: {:?})", id3, add3_start.elapsed());

    // Read stream data
    println!("\n  📖 Reading stream data...");
    let read_start = Instant::now();
    let reply: StreamReadReply = redis::cmd("XREAD")
        .arg(&["COUNT", "10"])
        .arg(&["STREAMS", stream_key, "0"])
        .query_async(&mut con)
        .await?;
    let read_elapsed = read_start.elapsed();

    for StreamKey { key, ids } in reply.keys {
        println!("  Stream: {} (elapsed: {:?})", key, read_elapsed);
        for StreamId { id, map } in ids {
            println!("    ID: {}", id);
            for (field, value) in map {
                println!("      {}: {:?}", field, value);
            }
        }
    }

    // Blocking read for new data
    println!("\n  ⏳ Blocking read for new data (5 second timeout)...");
    let mut read_conn = client.get_multiplexed_async_connection().await?;
    let stream_key_clone = stream_key.to_string();
    let block_read_start = Instant::now();
    let read_task = tokio::spawn(async move {
        let task_start = Instant::now();
        let reply: RedisResult<StreamReadReply> = redis::cmd("XREAD")
            .arg(&["COUNT", "10"])
            .arg(&["BLOCK", "5000"])
            .arg(&["STREAMS", &stream_key_clone, "$"])
            .query_async(&mut read_conn)
            .await;

        match reply {
            Ok(reply) => {
                let elapsed = task_start.elapsed();
                for StreamKey { key, ids } in reply.keys {
                    println!("    📨 Received new data from {} (elapsed: {:?})", key, elapsed);
                    for StreamId { id, map } in ids {
                        println!("      ID: {}", id);
                        for (field, value) in map {
                            println!("        {}: {:?}", field, value);
                        }
                    }
                }
            }
            Err(e) => {
                println!("    ⏱️  Timeout or error: {:?} (elapsed: {:?})", e, task_start.elapsed());
            }
        }
    });

    // Wait a moment before adding new data to allow blocking read to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut add_conn = client.get_multiplexed_async_connection().await?;
    let add_new_start = Instant::now();
    let _: String = redis::cmd("XADD")
        .arg(&[stream_key, "*"])
        .arg(&["event", "notification"])
        .arg(&["message", "Hello from stream!"])
        .query_async(&mut add_conn)
        .await?;
    println!("  ✓ Added new event to stream (elapsed: {:?})", add_new_start.elapsed());

    // Wait for read task to complete
    read_task.await.ok();
    let block_read_total = block_read_start.elapsed();
    println!("  ⏱️  Blocking read operation total elapsed: {:?}", block_read_total);

    let total_elapsed = start_time.elapsed();
    println!("\n✅ Stream demo completed (total elapsed: {:?})\n", total_elapsed);
    Ok(())
}
