//! This example will connect to Redis in one of three modes:
//!
//! - Regular async connection
//! - Async multiplexed connection
//! - Async connection manager
//!
//! It will then send a PING every second and print the result.

use std::env;
use std::process;
use std::time::Duration;

use redis::aio::ConnectionLike;
use tokio::time::interval;

enum Mode {
    Default,
    Multiplexed,
    Reconnect,
}

async fn run<C: ConnectionLike>(mut con: C) -> redis::RedisResult<()> {
    let mut interval = interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        println!("PING");
        let result: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut con).await;
        println!("Query result: {:?}", result);
    }
}

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let mode = match env::args().skip(1).next().as_ref().map(String::as_str) {
        Some("default") => {
            println!("Using default connection mode\n");
            Mode::Default
        }
        Some("multiplexed") => {
            println!("Using multiplexed connection mode\n");
            Mode::Multiplexed
        }
        Some("reconnect") => {
            println!("Using reconnect manager mode\n");
            Mode::Reconnect
        }
        Some(_) | None => {
            println!("Usage: reconnect-manager (default|multiplexed|reconnect)");
            process::exit(1);
        }
    };

    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    match mode {
        Mode::Default => run(client.get_async_connection().await?).await?,
        Mode::Multiplexed => run(client.get_multiplexed_tokio_connection().await?).await?,
        Mode::Reconnect => run(client.get_tokio_connection_manager().await?).await?,
    };
    Ok(())
}
