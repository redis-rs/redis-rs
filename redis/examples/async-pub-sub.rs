use futures_util::StreamExt;
use redis::AsyncCommands;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut publish_conn = client.get_async_connection().await?;
    let mut pubsub_conn = client.get_async_connection().await?.into_pubsub();

    pubsub_conn.subscribe("wavephone").await?;
    publish_conn.publish("wavephone", "banana").await?;

    let pubsub_msg: String = pubsub_conn.next().await.unwrap().get_payload()?;
    assert_eq!(&pubsub_msg, "banana");

    let (mut sink, mut stream) = pubsub_conn.split();

    let pubsub_msg = tokio::spawn(async move { stream.next().await });

    sink.subscribe("wavephone2").await?;
    publish_conn.publish("wavephone2", "apple").await?;

    let pubsub_payload: String = pubsub_msg.await.unwrap().unwrap().get_payload::<String>()?;
    assert_eq!(&pubsub_payload, "apple");

    Ok(())
}
