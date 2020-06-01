use redis::streams::{StreamId, StreamKey, StreamMaxlen, StreamReadOptions, StreamReadReply};
use redis::{Commands, RedisResult, Value};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

/// This program generates an arbitrary set of records across three
/// different streams.  It then reads the data back in such a way
/// that demonstrates basic usage of both the XREAD and XREADGROUP
/// commands.
fn main() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();

    println!("Demonstrating XADD followed by XREAD, single threaded\n");

    xadd_records(&client).expect("contrived record generation");

    let read_reply_simple = xread_records(&client).expect("simple read");
    print_records(read_reply_simple);

    println!("\n\nDemonstrating a longer stream of data flowing\nin over time, consumed by multiple threads using XREADGROUP\n");

    let mut handles = vec![];

    // Launch a producer thread which repeatedly adds records,
    // with only a small delay between writes.
    handles.push(thread::spawn(move || {
        let repeat = 30;
        let slowness = 1;
        for _ in 0..repeat {
            xadd_records(&client).expect("add");
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
    for slowness in 2..4 {
        let repeat = 5;
        handles.push(thread::spawn(move || {
            let c = redis::Client::open("redis://127.0.0.1/").unwrap();

            // We must create each group and each consumer
            // See https://redis.io/commands/xreadgroup#differences-between-xread-and-xreadgroup
            let mut con = c.get_connection().expect("con");

            for key in vec![DOG_STREAM, CAT_STREAM, DUCK_STREAM] {
                let created: Result<(), _> =
                    con.xgroup_create_mkstream(key, group_name(slowness), "$");
                if let Err(e) = created {
                    println!("Group already exists: {:?}", e)
                }
            }

            for _ in 0..repeat {
                let read_reply_group = xreadgroup_records(&c, slowness).expect("group read");

                print_records(read_reply_group);
                thread::sleep(Duration::from_millis(random_wait_millis(slowness)))
            }
        }))
    }

    for h in handles {
        h.join().expect("Join")
    }
}

const DOG_STREAM: &str = "example-dog";
const CAT_STREAM: &str = "example-cat";
const DUCK_STREAM: &str = "example-duck";

/// Generate some contrived records and add them to various
/// streams.
#[cfg(feature = "streams")]
fn xadd_records(client: &redis::Client) -> RedisResult<()> {
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

fn random_wait_millis(slowness: u8) -> u64 {
    thrifty_rand() as u64 * thrifty_rand() as u64 * 35 * slowness as u64
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
#[cfg(feature = "streams")]
fn xread_records(client: &redis::Client) -> RedisResult<StreamReadReply> {
    let mut con = client.get_connection().expect("conn");

    let opts = StreamReadOptions::default().block(BLOCK_MILLIS);

    // Oldest known time index
    let starting_id = "0-0";
    // Same as above
    let another_form = "0";

    con.xread_options(
        &[DOG_STREAM, CAT_STREAM, DUCK_STREAM],
        &[starting_id, another_form, starting_id],
        opts,
    )
}

fn group_name(slowness: u8) -> String {
    format!("example-group-{}", slowness)
}

fn consumer_name(slowness: u8) -> String {
    format!("example-consumer-{}", slowness)
}

#[cfg(feature = "streams")]
fn xreadgroup_records(client: &redis::Client, slowness: u8) -> RedisResult<StreamReadReply> {
    let mut con = client.get_connection().expect("conn");

    let opts = StreamReadOptions::default()
        .block(BLOCK_MILLIS)
        .count(3)
        .group(group_name(slowness), consumer_name(slowness));

    con.xread_options(
        &[DOG_STREAM, CAT_STREAM, DUCK_STREAM],
        &[">", ">", ">"],
        opts,
    )
}

fn print_records(srr: StreamReadReply) {
    for StreamKey { key, ids } in srr.keys {
        println!("Stream {}", key);
        for StreamId { id, map } in ids {
            println!("\tID {}", id);
            for (n, s) in map {
                if let Value::Data(bytes) = s {
                    println!("\t\t{}: {}", n, String::from_utf8(bytes).expect("utf8"))
                } else {
                    panic!("Weird data")
                }
            }
        }
    }
}

#[cfg(not(feature = "streams"))]
fn xadd_records(client: &redis::Client) -> RedisResult<()> {
    Ok(())
}

#[cfg(not(feature = "streams"))]
fn xread_records(client: &redis::Client) -> RedisResult<StreamReadyReply> {
    Ok(())
}

#[cfg(not(feature = "streams"))]
fn xreadgroup_records(client: &redis::Client, ids: ExampleIds) -> RedisResult<StreamReadReply> {
    Ok(())
}
