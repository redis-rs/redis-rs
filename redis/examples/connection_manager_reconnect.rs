//! This example demonstrates the automatic reconnection and resubscription
//! capabilities of the `ConnectionManager`.

#[cfg(all(feature = "connection-manager", feature = "tokio-comp"))]
#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    use redis::aio::ConnectionManagerConfig;
    use redis::{AsyncCommands, Client};
    use std::time::Duration;
    use tokio::sync::mpsc::unbounded_channel;

    // This example requires a running Redis server with RESP3 support.
    // Connect to the Redis node using its service name, which is resolvable
    // within the podman network.
    let client = Client::open("redis://redis-node-1:6379/?protocol=resp3")?;

    let (tx, mut rx) = unbounded_channel();

    // Configure the connection manager to use a push sender and automatically resubscribe.
    let config = ConnectionManagerConfig::new()
        .set_push_sender(tx)
        .set_automatic_resubscription();

    let mut manager = client.get_connection_manager_with_config(config).await?;

    // Subscribe to a channel.
    manager.subscribe("my-channel").await?;

    // Publish a message.
    let mut pub_conn = client.get_connection_manager().await?;
    pub_conn
        .publish::<_, _, ()>("my-channel", "first message")
        .await?;

    // Wait for the message to be received.
    let msg = rx.recv().await.unwrap();
    println!("Received message: {:?}", msg);

    // Simulate a disconnection by killing the client's connection.
    println!("Simulating disconnection to trigger automatic resubscription...");
    let mut killer_conn = client.get_connection_manager().await?;
    redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("normal")
        .query_async::<()>(&mut killer_conn)
        .await?;

    // The connection manager will automatically reconnect and resubscribe.
    // Give it a moment to do so.
    println!("Allowing 1s for reconnect and resubscription...");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Publish another message.
    pub_conn
        .publish::<_, _, ()>("my-channel", "second message")
        .await?;

    // The message should be received on the same channel.
    let msg = rx.recv().await.unwrap();
    println!("Received message after reconnect: {:?}", msg);

    Ok(())
}

#[cfg(not(all(feature = "connection-manager", feature = "tokio-comp")))]
fn main() {}
