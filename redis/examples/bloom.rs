#![cfg(feature = "bloom")]

//! Demonstrates basic operations and dumping/loading Bloom filters.

use redis::bloom::{BloomFilterDumpChunk, BloomFilterDumpIterator};
use redis::{Connection, RedisResult, TypedCommands};

/// The key to use in the example
const KEY: &str = "example_bloom_1";

/// Adds "foo", "bar", and "baz" (but not "quux") to `KEY`
fn add_elements(con: &mut Connection) -> RedisResult<()> {
    println!("Adding some elements to {KEY} ...");

    // Single element
    con.bf_add(KEY, "foo")?;

    // Multiple elements
    con.bf_madd(KEY, &["bar", "baz"])?;

    Ok(())
}

/// Prints whether `KEY` contains "foo", "bar", "baz", or "quux"
fn check_membership(con: &mut Connection) -> RedisResult<()> {
    println!("Checking containment ...");
    for candidate in &["foo", "bar", "baz", "quux"] {
        let exists = con.bf_exists(KEY, candidate)?;
        println!("* {candidate}: {exists}");
    }

    Ok(())
}

/// Returns a dump of `KEY`
fn dump(con: &mut Connection) -> Vec<BloomFilterDumpChunk> {
    println!("Dumping {KEY} ...");

    BloomFilterDumpIterator::new(con, KEY)
        .map(|r| r.expect("dump should succeed"))
        .collect::<Vec<_>>()
}

/// Loads a dump of `KEY`
fn load(con: &mut Connection, dump: Vec<BloomFilterDumpChunk>) -> RedisResult<()> {
    println!("Loading dump into {KEY} ...");
    for chunk in dump {
        con.bf_loadchunk(KEY, chunk)?;
    }

    Ok(())
}

/// Deletes `KEY`
fn cleanup(con: &mut Connection) -> RedisResult<()> {
    println!("Deleting {KEY} ...");
    let deleted_count = con.del(KEY)?;
    println!("Deleted {deleted_count} key");

    Ok(())
}

fn main() -> RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;

    // Add some elements to the Bloom filter
    add_elements(&mut con)?;
    // Check membership of a few items
    check_membership(&mut con)?;

    // Dump/Load-round-trip the Bloom filter
    let dump = dump(&mut con);
    cleanup(&mut con)?;
    load(&mut con, dump)?;
    check_membership(&mut con)?;

    // Final cleanup
    cleanup(&mut con)?;

    Ok(())
}
