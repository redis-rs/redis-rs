use futures_util::StreamExt as _;
use redis::AsyncCommands;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut publish_conn = client.get_async_connection().await?;
    let (mut sink, mut stream) = client.get_async_connection().await?.into_pubsub();

    sink.subscribe("wavephone").await?;
    publish_conn.publish("wavephone", "banana").await?;

    // consume the response to the subscription
    let _ = stream.next().await;

    let pubsub_msg: String = stream.next().await.unwrap()?.unwrap().get_payload()?;
    assert_eq!(&pubsub_msg, "banana");

    println!("Received the message: {}", pubsub_msg);

    Ok(())
}
