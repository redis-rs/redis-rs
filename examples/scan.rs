use std::time::Instant;

use redis::{Commands, Pipeline};

fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;

    let mut pipe = Pipeline::new();
    for i in 0..1_000_000 {
        let key = format!("key:{}", i);
        pipe.cmd("SET").arg(key).arg(i).ignore();
    }
    pipe.query::<()>(&mut con)?;

    let instant_scan = Instant::now();
    let count_scan = con.scan_match::<&str, String>("key:*")?.count();
    let instant_scan = instant_scan.elapsed().as_millis();

    let instant_scan_count = Instant::now();
    let count = con
        .scan_match_count::<&str, String>("key:*", 100_000)?
        .count();
    let instant_scan_count = instant_scan_count.elapsed().as_millis();

    assert_eq!(count, count_scan);
    assert_eq!(count, 1_000_000);
    // 20 times
    println!("{} times", instant_scan / instant_scan_count);
    Ok(())
}
