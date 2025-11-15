//! 使用 monoio 运行时的 Redis 综合示例
//! 
//! 演示 Redis 的四大核心功能：
//! 1. 保存/删除/查询各种数据类型（String, Hash, List, Set, Sorted Set）
//! 2. Pub/Sub 发布与接收数据
//! 3. Keyspace 监听数据变化
//! 4. Stream 收取数据流

#![cfg(feature = "monoio-comp")]

use redis::{
    AsyncTypedCommands,
    streams::{StreamId, StreamKey, StreamReadReply},
    RedisResult,
};
use futures_util::StreamExt;
use std::time::Duration;

#[monoio::main(timer_enabled = true)]
async fn main() -> RedisResult<()> {
    println!("🚀 Redis Monoio 综合示例\n");

    // 1. 演示各种数据类型的操作
    demo_data_types().await?;
    
    // 2. 演示 Pub/Sub
    demo_pubsub().await?;
    
    // 3. 演示 Keyspace 监听
    demo_keyspace_notifications().await?;
    
    // 4. 演示 Stream
    demo_streams().await?;

    println!("\n✅ 所有示例完成！");
    Ok(())
}

/// 1. 演示各种数据类型的保存/删除/查询
async fn demo_data_types() -> RedisResult<()> {
    println!("📦 1. 数据类型操作演示");
    println!("{}", "=".repeat(50));

    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_multiplexed_async_connection().await?;

    // String 类型
    println!("\n🔹 String 类型 (SET → GET → UPDATE → GET):");
    AsyncTypedCommands::set(&mut con, "user:name", "Alice").await?;
    AsyncTypedCommands::set(&mut con, "user:age", 30).await?;
    let name: Option<String> = AsyncTypedCommands::get(&mut con, "user:name").await?;
    let age: Option<String> = AsyncTypedCommands::get(&mut con, "user:age").await?;
    println!("  初始值 - name: {:?}, age: {:?}", name, age);

    // 更新
    AsyncTypedCommands::set(&mut con, "user:name", "Alice Smith").await?;
    let _: isize = AsyncTypedCommands::incr(&mut con, "user:age", 1).await?; // age + 1
    let name_updated: Option<String> = AsyncTypedCommands::get(&mut con, "user:name").await?;
    let age_updated: Option<String> = AsyncTypedCommands::get(&mut con, "user:age").await?;
    println!("  更新后 - name: {:?}, age: {:?}", name_updated, age_updated);

    // Hash 类型
    println!("\n🔹 Hash 类型 (HSET → HGETALL → UPDATE → HGETALL):");
    AsyncTypedCommands::hset(&mut con, "user:1000", "name", "Bob").await?;
    AsyncTypedCommands::hset(&mut con, "user:1000", "email", "bob@example.com").await?;
    AsyncTypedCommands::hset(&mut con, "user:1000", "age", "25").await?;
    let user: std::collections::HashMap<String, String> = AsyncTypedCommands::hgetall(&mut con, "user:1000").await?;
    println!("  初始值: {:?}", user);

    // 更新 Hash 字段
    AsyncTypedCommands::hset(&mut con, "user:1000", "email", "bob.updated@example.com").await?;
    AsyncTypedCommands::hset(&mut con, "user:1000", "status", "active").await?; // 新增字段
    let user_updated: std::collections::HashMap<String, String> = AsyncTypedCommands::hgetall(&mut con, "user:1000").await?;
    println!("  更新后: {:?}", user_updated);

    // 获取单个 Hash 字段
    let email: Option<String> = AsyncTypedCommands::hget(&mut con, "user:1000", "email").await?;
    println!("  HGET email: {:?}", email);

    // List 类型
    println!("\n🔹 List 类型 (RPUSH → LRANGE → LPUSH → LRANGE):");
    AsyncTypedCommands::del(&mut con, "tasks").await?;
    AsyncTypedCommands::rpush(&mut con, "tasks", "task1").await?;
    AsyncTypedCommands::rpush(&mut con, "tasks", "task2").await?;
    AsyncTypedCommands::rpush(&mut con, "tasks", "task3").await?;
    let tasks: Vec<String> = AsyncTypedCommands::lrange(&mut con, "tasks", 0, -1).await?;
    println!("  初始列表: {:?}", tasks);

    // 从左侧插入和从右侧移除
    AsyncTypedCommands::lpush(&mut con, "tasks", "urgent_task").await?;
    let _popped: Option<String> = AsyncTypedCommands::rpop(&mut con, "tasks", None).await?; // 移除最后一个
    let tasks_updated: Vec<String> = AsyncTypedCommands::lrange(&mut con, "tasks", 0, -1).await?;
    println!("  更新后列表: {:?}", tasks_updated);

    // 获取列表长度
    let len: usize = AsyncTypedCommands::llen(&mut con, "tasks").await?;
    println!("  列表长度: {}", len);

    // Set 类型
    println!("\n🔹 Set 类型 (SADD → SMEMBERS → UPDATE → SMEMBERS):");
    AsyncTypedCommands::del(&mut con, "tags").await?;
    AsyncTypedCommands::sadd(&mut con, "tags", "rust").await?;
    AsyncTypedCommands::sadd(&mut con, "tags", "redis").await?;
    AsyncTypedCommands::sadd(&mut con, "tags", "monoio").await?;
    let tags: std::collections::HashSet<String> = AsyncTypedCommands::smembers(&mut con, "tags").await?;
    println!("  初始集合: {:?}", tags);

    // 添加和删除元素
    AsyncTypedCommands::sadd(&mut con, "tags", "async").await?;
    AsyncTypedCommands::srem(&mut con, "tags", "monoio").await?;
    let tags_updated: std::collections::HashSet<String> = AsyncTypedCommands::smembers(&mut con, "tags").await?;
    println!("  更新后集合: {:?}", tags_updated);

    // 检查成员是否存在
    let exists: bool = AsyncTypedCommands::sismember(&mut con, "tags", "rust").await?;
    println!("  'rust' 存在于集合中: {}", exists);

    // Sorted Set 类型
    println!("\n🔹 Sorted Set 类型 (ZADD → ZREVRANGE → UPDATE → ZREVRANGE):");
    AsyncTypedCommands::del(&mut con, "leaderboard").await?;
    AsyncTypedCommands::zadd(&mut con, "leaderboard", "player1", 100).await?;
    AsyncTypedCommands::zadd(&mut con, "leaderboard", "player2", 200).await?;
    AsyncTypedCommands::zadd(&mut con, "leaderboard", "player3", 150).await?;
    let top: Vec<String> = AsyncTypedCommands::zrevrange(&mut con, "leaderboard", 0, 2).await?;
    println!("  初始排行榜前3名: {:?}", top);

    // 更新分数
    AsyncTypedCommands::zadd(&mut con, "leaderboard", "player1", 250).await?; // player1 score = 250
    let _: f64 = redis::cmd("ZINCRBY")
        .arg("leaderboard")
        .arg(50)
        .arg("player3")
        .query_async(&mut con)
        .await?; // player3 score + 50
    let top_updated: Vec<String> = AsyncTypedCommands::zrevrange(&mut con, "leaderboard", 0, 2).await?;
    println!("  更新后排行榜前3名: {:?}", top_updated);

    // 获取成员分数
    let score: Option<f64> = AsyncTypedCommands::zscore(&mut con, "leaderboard", "player1").await?;
    println!("  player1 分数: {:?}", score);

    // 删除操作
    println!("\n🔹 删除操作:");
    let deleted: usize = AsyncTypedCommands::del(&mut con, "user:name").await?;
    println!("  DEL user:name: {} key deleted", deleted);

    // 验证删除
    let exists: bool = AsyncTypedCommands::exists(&mut con, "user:name").await?;
    println!("  user:name 是否存在: {}", exists);

    // 批量删除
    let deleted_multiple: usize = AsyncTypedCommands::del(&mut con, &["tasks", "tags", "leaderboard"]).await?;
    println!("  批量删除 {} 个键", deleted_multiple);

    println!("\n✅ 数据类型操作完成\n");
    Ok(())
}

/// 2. 演示 Pub/Sub 发布与接收
async fn demo_pubsub() -> RedisResult<()> {
    use std::time::Instant;

    println!("📢 2. Pub/Sub 演示");
    println!("{}", "=".repeat(50));

    let start_time = Instant::now();

    let client = redis::Client::open("redis://127.0.0.1/")?;

    // 创建发布连接
    let mut pub_conn = client.get_multiplexed_async_connection().await?;

    // 创建订阅连接
    let mut sub_conn = client.get_async_pubsub().await?;

    // 订阅频道
    let subscribe_start = Instant::now();
    sub_conn.subscribe("news").await?;
    sub_conn.subscribe("sports").await?;
    let subscribe_elapsed = subscribe_start.elapsed();
    println!("  ✓ 已订阅频道: news, sports (耗时: {:?})", subscribe_elapsed);

    // 启动订阅任务 - on_message() 会 move sub_conn
    let sub_task = monoio::spawn(async move {
        let mut stream = sub_conn.on_message();
        println!("\n  📥 开始接收消息...");
        let mut msg_count = 0;
        let first_msg_time = Instant::now();
        for _ in 0..4 {
            if let Some(msg) = stream.next().await {
                msg_count += 1;
                let elapsed = first_msg_time.elapsed();
                let channel: String = msg.get_channel_name().to_string();
                let payload: String = msg.get_payload().unwrap_or_default();
                // println!("  📨 [+{:?}] 收到消息 #{} [{}]: {}", elapsed, msg_count, channel, payload);
            }
        }
        println!("  ⏱️  接收 4 条消息总耗时: {:?}", first_msg_time.elapsed());
    });

    // 等待一下确保订阅已建立（只需要很短的时间）
    monoio::time::sleep(Duration::from_micros(100)).await;  // 100微秒，而非100毫秒

    // 发布消息
    println!("\n  📤 发布消息...");
    let publish_start = Instant::now();
    AsyncTypedCommands::publish(&mut pub_conn, "news", "Breaking: Redis with Monoio!").await?;
    AsyncTypedCommands::publish(&mut pub_conn, "sports", "Game starts at 8pm").await?;
    AsyncTypedCommands::publish(&mut pub_conn, "news", "Latest update available").await?;
    AsyncTypedCommands::publish(&mut pub_conn, "sports", "Final score: 3-1").await?;
    let publish_elapsed = publish_start.elapsed();
    println!("  ⏱️  发布 4 条消息耗时: {:?}", publish_elapsed);

    // 等待订阅任务完成
    sub_task.await;

    let total_elapsed = start_time.elapsed();
    println!("\n✅ Pub/Sub 演示完成 (总耗时: {:?})\n", total_elapsed);
    Ok(())
}

/// 3. 演示 Keyspace 监听数据变化
async fn demo_keyspace_notifications() -> RedisResult<()> {
    use std::time::Instant;

    println!("🔔 3. Keyspace 监听演示");
    println!("{}", "=".repeat(50));

    let start_time = Instant::now();

    let client = redis::Client::open("redis://127.0.0.1/")?;

    // 启用 keyspace notifications（需要在 Redis 配置中设置，这里假设已启用）
    // CONFIG SET notify-keyspace-events KEA
    let mut config_conn = client.get_multiplexed_async_connection().await?;
    let config_start = Instant::now();
    let _: () = redis::cmd("CONFIG")
        .arg(&["SET", "notify-keyspace-events", "KEA"])
        .query_async(&mut config_conn)
        .await?;
    println!("  ✓ 已启用 keyspace notifications (耗时: {:?})", config_start.elapsed());

    // 创建监听连接
    let mut notify_conn = client.get_async_pubsub().await?;

    // 订阅 keyspace 事件
    // __keyspace@0__:* 监听所有键空间事件
    let subscribe_start = Instant::now();
    notify_conn.psubscribe("__keyspace@0__:*").await?;
    println!("  ✓ 已订阅 keyspace 事件 (耗时: {:?})", subscribe_start.elapsed());

    // 启动监听任务 - on_message() 会 move notify_conn
    let notify_task = monoio::spawn(async move {
        let mut stream = notify_conn.on_message();
        println!("\n  👂 开始监听 keyspace 事件...");
        let mut event_count = 0;
        let first_event_time = Instant::now();
        for _ in 0..6 {
            if let Some(msg) = stream.next().await {
                event_count += 1;
                let elapsed = first_event_time.elapsed();
                let channel: String = msg.get_channel_name().to_string();
                let payload: String = msg.get_payload().unwrap_or_default();
                // 解析键名（从 __keyspace@0__:keyname 中提取）
                if let Some(key) = channel.strip_prefix("__keyspace@0__:") {
                    // println!("  🔔 [+{:?}] 事件 #{} - 键 '{}' 发生事件: {}", elapsed, event_count, key, payload);
                }
            }
        }
        println!("  ⏱️  接收 6 个事件总耗时: {:?}", first_event_time.elapsed());
    });

    // 等待一下确保订阅已建立（只需要很短的时间）
    monoio::time::sleep(Duration::from_micros(100)).await;  // 100微秒，而非100毫秒

    // 执行一些操作触发事件
    println!("\n  🔧 执行操作触发事件...");
    let mut op_conn = client.get_multiplexed_async_connection().await?;
    let ops_start = Instant::now();
    AsyncTypedCommands::set(&mut op_conn, "monitored:key1", "value1").await?;
    AsyncTypedCommands::set(&mut op_conn, "monitored:key2", "value2").await?;
    AsyncTypedCommands::hset(&mut op_conn, "monitored:hash", "field", "value").await?;
    AsyncTypedCommands::del(&mut op_conn, "monitored:key1").await?;
    AsyncTypedCommands::expire(&mut op_conn, "monitored:key2", 10).await?;
    AsyncTypedCommands::set(&mut op_conn, "monitored:key3", "value3").await?;
    let ops_elapsed = ops_start.elapsed();
    println!("  ⏱️  执行 6 个操作耗时: {:?}", ops_elapsed);

    // 等待监听任务完成
    notify_task.await;

    let total_elapsed = start_time.elapsed();
    println!("\n✅ Keyspace 监听演示完成 (总耗时: {:?})\n", total_elapsed);
    Ok(())
}

/// 4. 演示 Stream 数据流
async fn demo_streams() -> RedisResult<()> {
    use std::time::Instant;

    println!("🌊 4. Stream 数据流演示");
    println!("{}", "=".repeat(50));

    let start_time = Instant::now();

    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_multiplexed_async_connection().await?;

    let stream_key = "events:stream";

    // 清理旧数据
    let _: usize = AsyncTypedCommands::del(&mut con, stream_key).await?;

    // 添加数据到 stream
    println!("\n  📝 添加数据到 stream...");
    let add_start = Instant::now();
    let id1: String = redis::cmd("XADD")
        .arg(&[stream_key, "*", "event", "login", "user", "alice"])
        .query_async(&mut con)
        .await?;
    println!("  ✓ 添加事件 ID: {} (耗时: {:?})", id1, add_start.elapsed());

    let add2_start = Instant::now();
    let id2: String = redis::cmd("XADD")
        .arg(&[stream_key, "*", "event", "purchase", "amount", "99.99"])
        .query_async(&mut con)
        .await?;
    println!("  ✓ 添加事件 ID: {} (耗时: {:?})", id2, add2_start.elapsed());

    let add3_start = Instant::now();
    let id3: String = redis::cmd("XADD")
        .arg(&[stream_key, "*", "event", "logout", "user", "alice"])
        .query_async(&mut con)
        .await?;
    println!("  ✓ 添加事件 ID: {} (耗时: {:?})", id3, add3_start.elapsed());

    // 读取 stream 数据
    println!("\n  📖 读取 stream 数据...");
    let read_start = Instant::now();
    let reply: StreamReadReply = redis::cmd("XREAD")
        .arg(&["COUNT", "10"])
        .arg(&["STREAMS", stream_key, "0"])
        .query_async(&mut con)
        .await?;
    let read_elapsed = read_start.elapsed();

    for StreamKey { key, ids } in reply.keys {
        println!("  Stream: {} (读取耗时: {:?})", key, read_elapsed);
        for StreamId { id, map } in ids {
            println!("    ID: {}", id);
            for (field, value) in map {
                println!("      {}: {:?}", field, value);
            }
        }
    }

    // 阻塞读取新数据
    println!("\n  ⏳ 阻塞读取新数据（5秒超时）...");
    let mut read_conn = client.get_multiplexed_async_connection().await?;
    let stream_key_clone = stream_key.to_string();
    let block_read_start = Instant::now();
    let read_task = monoio::spawn(async move {
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
                    println!("    📨 收到新数据 from {} (等待耗时: {:?})", key, elapsed);
                    for StreamId { id, map } in ids {
                        println!("      ID: {}", id);
                        for (field, value) in map {
                            println!("        {}: {:?}", field, value);
                        }
                    }
                }
            }
            Err(e) => {
                println!("    ⏱️  超时或错误: {:?} (耗时: {:?})", e, task_start.elapsed());
            }
        }
    });

    // 等待一小段时间然后添加新数据（让阻塞读取先启动）
    monoio::time::sleep(Duration::from_millis(100)).await;
    let mut add_conn = client.get_multiplexed_async_connection().await?;
    let add_new_start = Instant::now();
    let _: String = redis::cmd("XADD")
        .arg(&[stream_key, "*"])
        .arg(&["event", "notification"])
        .arg(&["message", "Hello from stream!"])
        .query_async(&mut add_conn)
        .await?;
    println!("  ✓ 已添加新事件到 stream (耗时: {:?})", add_new_start.elapsed());

    // 等待读取任务完成
    read_task.await;
    let block_read_total = block_read_start.elapsed();
    println!("  ⏱️  阻塞读取操作总耗时: {:?}", block_read_total);

    let total_elapsed = start_time.elapsed();
    println!("\n✅ Stream 演示完成 (总耗时: {:?})\n", total_elapsed);
    Ok(())
}

