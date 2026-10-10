//! Reproduces the memory-usage difference this crate's `client_list_iter()`
//! is meant to fix (<https://github.com/redis-rs/redis-rs/issues/2396>):
//! the generic `CLIENT LIST` reply path materializes the whole reply into
//! one owned buffer before any of it is usable, so calling it against a
//! server with a large number of connected clients causes memory to spike
//! roughly in proportion to the client count. `client_list_iter()` streams
//! the same reply instead.
//!
//! This opens a configurable number of idle client connections, then times
//! and measures this process's own RSS across both a generic `CLIENT LIST`
//! call (via `query::<String>()`) and an equivalent `client_list_iter()`
//! call against the identical reply.
//!
//! RSS is read from `/proc/self/status`, so this only runs on Linux -- it's
//! a manual reproduction tool for the numbers quoted in the PR, not part of
//! the automated, cross-platform test suite.
//!
//! **Warning:** this opens real, held-open TCP connections -- tens of
//! thousands by default -- against whatever server address you point it
//! at. Only run it against a local or otherwise disposable server you
//! control, never a shared, staging, or production one.
//!
//! Usage:
//!   cargo run --release --example client_list_memory -- [addr] [num_connections]
//! Defaults: addr=127.0.0.1:6379, num_connections=20000
//!
//! The target server must already be running, reachable at `addr`, and
//! configured with `maxclients` comfortably above `num_connections`.

use std::env;
use std::net::TcpStream;
use std::time::Instant;

#[cfg(target_os = "linux")]
fn rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.trim().trim_end_matches(" kB").trim().parse().ok();
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn rss_kb() -> Option<u64> {
    None
}

fn main() {
    let mut args = env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:6379".to_string());
    let n: usize = args
        .next()
        .map(|s| s.parse().expect("num_connections must be a number"))
        .unwrap_or(20_000);

    let Some(baseline) = rss_kb() else {
        eprintln!(
            "This example reads RSS from /proc/self/status and only runs on Linux; \
             nothing else to demonstrate on this platform."
        );
        std::process::exit(1);
    };
    println!("baseline RSS: {baseline} kB");

    println!("opening {n} idle client connections to {addr} ...");
    let t0 = Instant::now();
    let mut conns = Vec::with_capacity(n);
    for i in 0..n {
        match TcpStream::connect(&addr) {
            Ok(stream) => conns.push(stream),
            Err(err) => {
                eprintln!("failed to open connection {i}: {err}");
                break;
            }
        }
        if i > 0 && i % 5000 == 0 {
            println!("  ... {i} connections opened ({:?})", t0.elapsed());
        }
    }
    println!(
        "opened {} connections in {:?}, RSS now: {} kB",
        conns.len(),
        t0.elapsed(),
        rss_kb().unwrap()
    );

    let url = format!("redis://{addr}/");
    let client = redis::Client::open(url).expect("invalid redis URL");
    let mut con = client.get_connection().expect("failed to connect");

    // --- (a) generic CLIENT LIST via query(): buffers the whole reply ---
    let before = rss_kb().unwrap();
    let t0 = Instant::now();
    let full: String = redis::cmd("CLIENT").arg("LIST").query(&mut con).unwrap();
    let elapsed = t0.elapsed();
    let after = rss_kb().unwrap();
    println!(
        "[generic query]    reply {} bytes, {} lines, RSS {before} -> {after} kB \
         (delta {} kB), {elapsed:?}",
        full.len(),
        full.lines().count(),
        after as i64 - before as i64,
    );
    drop(full);

    // --- (b) client_list_iter(): streams the reply ---
    let before = rss_kb().unwrap();
    let t0 = Instant::now();
    let mut count = 0usize;
    let mut max_line = 0usize;
    for line in con.client_list_iter().unwrap() {
        let line = line.unwrap();
        max_line = max_line.max(line.len());
        count += 1;
    }
    let elapsed = t0.elapsed();
    let after = rss_kb().unwrap();
    println!(
        "[client_list_iter] {count} lines, longest {max_line} bytes, RSS {before} -> {after} kB \
         (delta {} kB), {elapsed:?}",
        after as i64 - before as i64,
    );

    drop(conns);
}
