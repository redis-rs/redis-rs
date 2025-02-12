//! This command demonstrates using a commit hook to bypass some network issues to reach a Redis
//! cluster. It works without disabling SSL strict hostname checking or other security features.
//!
//! This program uses a `connect_hook` to rewrite addresses as we connect. I used this on my
//! machine to make the client ignore the IP addresses the cluster tells it to use (which for
//! networking reasons aren't reachable from my dev machine) and instead use a hostname that works.

use redis::cluster::ClusterClient;
use redis::Commands;

fn main() -> redis::RedisResult<()> {
    let password = std::env::var("REDISCLI_AUTH")
        .expect("export REDISCLI_AUTH=<password> before running this");
    let redis_ip =
        std::env::var("REDIS_IP").expect("set REDIS_IP to an IP the cluster will tell us to use");
    let redis_host =
        std::env::var("REDIS_HOST").expect("set REDIS_HOST to the cluster host we should use");
    let init_port = std::env::var("REDIS_PORT").unwrap_or("6380".to_string());
    let key = std::env::var("KEY").expect("set KEY to a key to query");

    // connect to redis
    let client =
        ClusterClient::builder([format!("rediss://:{password}@{redis_host}:{init_port}/")])
            .connect_hook(move |mut host, port| {
                if host == redis_ip {
                    host = redis_host.to_string();
                }
                eprintln!("connecting to: {host}:{port}");
                Ok((host, port))
            })
            .build()?;

    let mut con = client.get_connection()?;
    let result: redis::Value = con.get(key.as_str())?;
    println!("ok: {result:#?}");
    Ok(())
}
