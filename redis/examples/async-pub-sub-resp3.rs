use redis::AsyncCommands;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    // ?resp3=true enables RESP3 for this connection.
    // Since RESP3 protocol is used there is no need for creating different connections when using PubSub commands.
    let client = redis::Client::open("redis://127.0.0.1/?resp3=true").unwrap();
    let mut conn = client.get_multiplexed_async_connection().await?;
    // In this example we are producing, consuming and using commands in same connection.
    let new_user_name = "jack";
    let mut conn_clone = conn.clone();
    tokio::spawn(async move {
        let mut msg_stream = conn_clone.create_msg_stream();
        conn_clone.subscribe("new_users").await.unwrap();

        let pubsub_msg: String = msg_stream.next().await.unwrap().get_payload().unwrap();
        assert_eq!(&pubsub_msg, &new_user_name);

        let user_info: String = conn_clone.get(pubsub_msg).await.unwrap();
        assert_eq!("dolphin", user_info);
    });
    conn.set(new_user_name, "dolphin").await?;
    conn.publish("new_users", new_user_name).await?;

    Ok(())
}
