#![cfg(feature = "streams")]

use redis::streams::{StreamId, StreamKey, StreamMaxlen, StreamReadOptions, StreamReadReply};

use redis::{Commands, RedisResult, Value};

use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

const DOG_STREAM: &str = "example-dog";
const CAT_STREAM: &str = "example-cat";
const DUCK_STREAM: &str = "example-duck";

const STREAMS: &[&str] = &[DOG_STREAM, CAT_STREAM, DUCK_STREAM];

const SLOWNESSES: &[u8] = &[2, 3, 4];

/// This program generates an arbitrary set of records across three
/// different streams.  It then reads the data back in such a way
/// that demonstrates basic usage of both the XREAD and XREADGROUP
/// commands.
fn main() {
    let client = redis::Client::open("redis://127.0.0.1/").expect("client");

    println!("Demonstrating XADD followed by XREAD, single threaded\n");

    add_records(&client).expect("contrived record generation");

    read_records(&client).expect("simple read");

    demo_group_reads(&client);

    clean_up(&client)
}

fn demo_group_reads(client: &redis::Client) {
    println!("\n\nDemonstrating a longer stream of data flowing\nin over time, consumed by multiple threads using XREADGROUP\n");

    let mut handles = vec![];

    let cc = client.clone();
    // Launch a producer thread which repeatedly adds records,
    // with only a small delay between writes.
    handles.push(thread::spawn(move || {
        let repeat = 30;
        let slowness = 1;
        for _ in 0..repeat {
            add_records(&cc).expect("add");
            thread::sleep(Duration::from_millis(random_wait_millis(slowness)))
        }
    }));

    // Launch consumer threads which repeatedly read from the
    // streams at various speeds.  They'll effectively compete
    // to consume the stream.
    //
    // Consumer groups are only appropriate for cases where you
    // do NOT want each consumer to read ALL of the data.  This
    // example is a contrived scenario so that each consumer
    // receives its own, specific chunk of data.
    //
    // Once the data is read, the redis-rs lib will automatically
    // acknowledge its receipt via XACK.
    //
    // Read more about reading with consumer groups here:
    // https://redis.io/commands/xreadgroup
    for slowness in SLOWNESSES {
        let repeat = 5;
        let ca = client.clone();
        handles.push(thread::spawn(move || {
            let mut con = ca.get_connection().expect("con");

            // We must create each group and each consumer
            // See https://redis.io/commands/xreadgroup#differences-between-xread-and-xreadgroup

            for key in STREAMS {
                let created: Result<(), _> = con.xgroup_create_mkstream(*key, GROUP_NAME, "$");
                if let Err(e) = created {
                    println!("Group already exists: {e:?}")
                }
            }

            for _ in 0..repeat {
                let read_reply = read_group_records(&ca, *slowness).expect("group read");

                // fake some expensive work
                for StreamKey { key, ids } in read_reply.keys {
                    for StreamId { id, map: _ } in &ids {
                        thread::sleep(Duration::from_millis(random_wait_millis(*slowness)));
                        println!(
                            "Stream {} ID {} Consumer slowness {} SysTime {}",
                            key,
                            id,
                            slowness,
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("time")
                                .as_millis()
                        );
                    }

                    // acknowledge each stream and message ID once all messages are
                    // correctly processed
                    let id_strs: Vec<&String> =
                        ids.iter().map(|StreamId { id, map: _ }| id).collect();
                    con.xack(key, GROUP_NAME, &id_strs).expect("ack")
                }
            }
        }))
    }

    for h in handles {
        h.join().expect("Join")
    }
}

/// Generate some contrived records and add them to various
/// streams.
fn add_records(client: &redis::Client) -> RedisResult<()> {
    let mut con = client.get_connection().expect("conn");

    let maxlen = StreamMaxlen::Approx(1000);

    // a stream whose records have two fields
    for _ in 0..thrifty_rand() {
        con.xadd_maxlen(
            DOG_STREAM,
            maxlen,
            "*",
            &[("bark", arbitrary_value()), ("groom", arbitrary_value())],
        )?;
    }

    // a streams whose records have three fields
    for _ in 0..thrifty_rand() {
        con.xadd_maxlen(
            CAT_STREAM,
            maxlen,
            "*",
            &[
                ("meow", arbitrary_value()),
                ("groom", arbitrary_value()),
                ("hunt", arbitrary_value()),
            ],
        )?;
    }

    // a streams whose records have four fields
    for _ in 0..thrifty_rand() {
        con.xadd_maxlen(
            DUCK_STREAM,
            maxlen,
            "*",
            &[
                ("quack", arbitrary_value()),
                ("waddle", arbitrary_value()),
                ("splash", arbitrary_value()),
                ("flap", arbitrary_value()),
            ],
        )?;
    }

    Ok(())
}

/// An approximation of randomness, without leaving the stdlib.
fn thrifty_rand() -> u8 {
    let penultimate_num = 2;
    (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time travel")
        .as_nanos()
        % penultimate_num) as u8
        + 1
}

const MAGIC: u64 = 11;
fn random_wait_millis(slowness: u8) -> u64 {
    thrifty_rand() as u64 * thrifty_rand() as u64 * MAGIC * slowness as u64
}

/// Generate a potentially unique value.
fn arbitrary_value() -> String {
    format!(
        "{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time travel")
            .as_nanos()
    )
}

/// Block the thread for this many milliseconds while
/// waiting for data to arrive on the stream.
const BLOCK_MILLIS: usize = 5000;

/// Read back records from all three streams, if they're available.
/// Doesn't bother with consumer groups.  Generally the user
/// would be responsible for keeping track of the most recent
/// ID from which they need to read, but in this example, we
/// just go back to the beginning of time and ask for all the
/// records in the stream.
fn read_records(client: &redis::Client) -> RedisResult<()> {
    let mut con = client.get_connection().expect("conn");

    let opts = StreamReadOptions::default().block(BLOCK_MILLIS);

    // Oldest known time index
    let starting_id = "0-0";
    // Same as above
    let another_form = "0";

    let srr: StreamReadReply = con
        .xread_options(STREAMS, &[starting_id, another_form, starting_id], &opts)
        .expect("read");

    for StreamKey { key, ids } in srr.keys {
        println!("Stream {key}");
        for StreamId { id, map } in ids {
            println!("\tID {id}");
            for (n, s) in map {
                if let Value::Data(bytes) = s {
                    println!("\t\t{}: {}", n, String::from_utf8(bytes).expect("utf8"))
                } else {
                    panic!("Weird data")
                }
            }
        }
    }

    Ok(())
}

fn consumer_name(slowness: u8) -> String {
    format!("example-consumer-{slowness}")
}

const GROUP_NAME: &str = "example-group-aaa";

fn read_group_records(client: &redis::Client, slowness: u8) -> RedisResult<StreamReadReply> {
    let mut con = client.get_connection().expect("conn");

    let opts = StreamReadOptions::default()
        .block(BLOCK_MILLIS)
        .count(3)
        .group(GROUP_NAME, consumer_name(slowness));

    let srr: StreamReadReply = con
        .xread_options(
            &[DOG_STREAM, CAT_STREAM, DUCK_STREAM],
            &[">", ">", ">"],
            &opts,
        )
        .expect("records");

    Ok(srr)
}

fn clean_up(client: &redis::Client) {
    let mut con = client.get_connection().expect("con");
    for k in STREAMS {
        let trimmed: RedisResult<()> = con.xtrim(*k, StreamMaxlen::Equals(0));
        trimmed.expect("trim");

        let destroyed: RedisResult<()> = con.xgroup_destroy(*k, GROUP_NAME);
        destroyed.expect("xgroup destroy");
    }
}
