use redis::AsyncCommands;
use tokio::time;

// use SIGTSTP (Ctrl+Z) to stop the redis server in order to induce a timeout here

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;

    let mut con = client
        .get_tokio_connection_manager_timeout(time::Duration::from_secs(5))
        .await?;

    match con.set_ex::<_, _, ()>("async-timeout", 1, 30).await {
        Ok(_) => (),
        Err(e) => eprintln!("redis error: {}", e),
    }

    Ok(())
}
