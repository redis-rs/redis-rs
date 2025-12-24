use redis::{AsyncTypedCommands, from_redis_value};

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/?protocol=3").unwrap();
    let mut publish_conn = client.get_multiplexed_async_connection().await?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let config = redis::aio::ConnectionManagerConfig::new()
        .set_push_sender(tx)
        .set_automatic_resubscription();

    let mut cm = client.get_connection_manager_with_config(config).await?;
    cm.subscribe("wavephone").await?;

    publish_conn.publish("wavephone", "banana").await?;

    _ = rx.recv().await.unwrap(); // ignore the first message, its the subscription notice
    let mut pubsub_msg = rx.recv().await.unwrap();
    println!("Received {pubsub_msg:?}");
    let message: String = from_redis_value(pubsub_msg.data.pop().unwrap()).unwrap();
    assert_eq!(&message, "banana");

    Ok(())
}
