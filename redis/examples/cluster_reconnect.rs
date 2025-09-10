//! This example demonstrates the automatic reconnection and resubscription
//! capabilities of the `ClusterClient`.

#[cfg(all(feature = "cluster-async", feature = "tokio-comp"))]
#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    use std::time::Duration;
    use redis::{cluster::ClusterClient, AsyncCommands};
    use tokio::sync::mpsc::unbounded_channel;

    // Add a delay to allow the cluster to initialize before connecting.
    println!("Waiting for the cluster to stabilize for 5s...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // This example requires a running Redis cluster with RESP3 support.
    let nodes = vec![
        "redis://redis-node-1:6379/?protocol=resp3",
        "redis://redis-node-2:6379/?protocol=resp3",
        "redis://redis-node-3:6379/?protocol=resp3",
    ];
    let (tx, mut rx) = unbounded_channel();

    // Build a client with a push sender to enable pub/sub and push-based reconnections.
    let client = ClusterClient::builder(nodes).push_sender(tx).build()?;

    let mut con = client.get_async_connection().await?;

    // Subscribe to a channel. The key `{foo}` ensures the command is routed to the same node.
    con.subscribe("{foo}my-channel").await?;

    // Publish a message.
    let mut pub_conn = client.get_async_connection().await?;
    pub_conn
        .publish::<_, _, ()>("{foo}my-channel", "first message")
        .await?;

    // Wait for the message to be received.
    let msg = rx.recv().await.unwrap();
    println!("Received message: {:?}", msg);

    // Pause to allow for manual intervention.
    println!("\n*** Successfully received first message. ***");
    println!("*** Now is the time to manually stop a Redis primary node (e.g., podman stop redis-cluster-test-env_redis-node-1_1). ***");
    println!("*** After stopping, wait a few seconds and restart it (e.g., podman start redis-cluster-test-env_redis-node-1_1). ***");
    println!("*** The test will resume in 30 seconds... ***");
    tokio::time::sleep(Duration::from_secs(30)).await;

    // Publish another message.
    pub_conn
        .publish::<_, _, ()>("{foo}my-channel", "second message")
        .await?;

    // The message should be received on the same channel.
    let msg = rx.recv().await.unwrap();
    println!("Received message after reconnect: {:?}", msg);

    Ok(())
}

#[cfg(not(all(feature = "cluster-async", feature = "tokio-comp")))]
fn main() {}
