#![cfg(feature = "streams")]

use redis::streams::*;
use redis::{Connection, ToRedisArgs, TypedCommands};

#[macro_use]
mod support;
use crate::support::*;

use std::collections::BTreeMap;
use std::slice;
use std::str;
use std::thread::sleep;
use std::time::Duration;

fn xadd(con: &mut Connection) {
    con.xadd("k1", "1000-0", &[("hello", "world"), ("redis", "streams")])
        .unwrap();
    con.xadd("k1", "1000-1", &[("hello", "world2")]).unwrap();
    con.xadd("k2", "2000-0", &[("hello", "world")]).unwrap();
    con.xadd("k2", "2000-1", &[("hello", "world2")]).unwrap();
}

fn xadd_keyrange(con: &mut Connection, key: &str, start: i32, end: i32) {
    for _i in start..end {
        con.xadd(key, "*", &[("h", "w")]).unwrap();
    }
}

#[test]
fn test_cmd_options() {
    // Tests the following command option builders....
    // xclaim_options
    // xread_options
    // maxlen enum

    // test read options

    let empty = StreamClaimOptions::default();
    assert_eq!(ToRedisArgs::to_redis_args(&empty).len(), 0);

    let empty = StreamReadOptions::default();
    assert_eq!(ToRedisArgs::to_redis_args(&empty).len(), 0);

    let opts = StreamClaimOptions::default()
        .idle(50)
        .time(500)
        .retry(3)
        .with_force()
        .with_justid();

    assert_args!(
        &opts,
        "IDLE",
        "50",
        "TIME",
        "500",
        "RETRYCOUNT",
        "3",
        "FORCE",
        "JUSTID"
    );

    // test maxlen options

    assert_args!(StreamMaxlen::Approx(10), "MAXLEN", "~", "10");
    assert_args!(StreamMaxlen::Equals(10), "MAXLEN", "=", "10");

    // test read options

    let opts = StreamReadOptions::default()
        .noack()
        .block(100)
        .count(200)
        .group("group-name", "consumer-name");

    assert_args!(
        &opts,
        "GROUP",
        "group-name",
        "consumer-name",
        "BLOCK",
        "100",
        "COUNT",
        "200",
        "NOACK"
    );

    // should skip noack because of missing group(,)
    let opts = StreamReadOptions::default().noack().block(100).count(200);

    assert_args!(&opts, "BLOCK", "100", "COUNT", "200");
}

#[test]
fn test_assorted_1() {
    // Tests the following commands....
    // xadd
    // xadd_map (skip this for now)
    // xadd_maxlen
    // xread
    // xlen

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    xadd(&mut con);

    // smoke test that we get the same id back
    let result = con.xadd("k0", "1000-0", &[("x", "y")]).unwrap();
    assert_eq!(result.unwrap(), "1000-0");

    // xread reply
    let reply = con
        .xread(&["k1", "k2", "k3"], &["0", "0", "0"])
        .unwrap()
        .unwrap();

    // verify reply contains 2 keys even though we asked for 3
    assert_eq!(&reply.keys.len(), &2usize);

    // verify first key & first id exist
    assert_eq!(&reply.keys[0].key, "k1");
    assert_eq!(&reply.keys[0].ids.len(), &2usize);
    assert_eq!(&reply.keys[0].ids[0].id, "1000-0");

    // lookup the key in StreamId map
    let hello = reply.keys[0].ids[0].get("hello");
    assert_eq!(hello, Some("world".to_string()));

    // verify the second key was written
    assert_eq!(&reply.keys[1].key, "k2");
    assert_eq!(&reply.keys[1].ids.len(), &2usize);
    assert_eq!(&reply.keys[1].ids[0].id, "2000-0");

    // test xadd_map
    let mut map = BTreeMap::new();
    map.insert("ab", "cd");
    map.insert("ef", "gh");
    map.insert("ij", "kl");
    con.xadd_map("k3", "3000-0", map).unwrap();

    let reply = con.xrange_all("k3").unwrap();
    assert!(reply.ids[0].contains_key("ab"));
    assert!(reply.ids[0].contains_key("ef"));
    assert!(reply.ids[0].contains_key("ij"));

    // test xadd w/ maxlength below...

    // add 100 things to k4
    xadd_keyrange(&mut con, "k4", 0, 100);

    // test xlen.. should have 100 items
    let result = con.xlen("k4");
    assert_eq!(result, Ok(100));

    // test xadd_maxlen

    con.xadd_maxlen("k4", StreamMaxlen::Equals(10), "*", &[("h", "w")])
        .unwrap();
    let result = con.xlen("k4");
    assert_eq!(result, Ok(10));
}

#[test]
fn test_xgroup_create() {
    // Tests the following commands....
    // xadd
    // xinfo_stream
    // xgroup_create
    // xinfo_groups

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    xadd(&mut con);

    // no key exists... this call breaks the connection pipe for some reason
    let reply = con.xinfo_stream("k10");
    assert!(
        matches!(&reply, Err(e) if e.kind() == redis::ServerErrorKind::ResponseError.into()
            && e.code() == Some("ERR")
            && e.detail() == Some("no such key"))
    );

    // redo the connection because the above error
    con = ctx.connection();

    // key should exist
    let reply = con.xinfo_stream("k1").unwrap();
    assert_eq!(&reply.first_entry.id, "1000-0");
    assert_eq!(&reply.last_entry.id, "1000-1");
    assert_eq!(&reply.last_generated_id, "1000-1");

    // xgroup create (existing stream)
    let result = con.xgroup_create("k1", "g1", "$");
    assert!(result.is_ok());

    // xinfo groups (existing stream)
    let result = con.xinfo_groups("k1");
    assert!(result.is_ok());
    let reply = result.unwrap();
    assert_eq!(&reply.groups.len(), &1);
    assert_eq!(&reply.groups[0].name, &"g1");
}

#[test]
fn test_xgroup_createconsumer() {
    // Tests the following command....
    // xgroup_createconsumer

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    xadd(&mut con);

    // key should exist
    let reply = con.xinfo_stream("k1").unwrap();
    assert_eq!(&reply.first_entry.id, "1000-0");
    assert_eq!(&reply.last_entry.id, "1000-1");
    assert_eq!(&reply.last_generated_id, "1000-1");

    // xgroup create (existing stream)
    let result = con.xgroup_create("k1", "g1", "$");
    assert!(result.is_ok());

    // xinfo groups (existing stream)
    let result = con.xinfo_groups("k1");
    assert!(result.is_ok());
    let reply = result.unwrap();
    assert_eq!(&reply.groups.len(), &1);
    assert_eq!(&reply.groups[0].name, &"g1");

    // xinfo consumers (consumer does not exist)
    let result = con.xinfo_consumers("k1", "g1");
    assert!(result.is_ok());
    let reply = result.unwrap();
    assert_eq!(&reply.consumers.len(), &0);

    // xgroup_createconsumer
    let result = con.xgroup_createconsumer("k1", "g1", "c1");
    assert!(matches!(result, Ok(true)));

    // xinfo consumers (consumer was created)
    let result = con.xinfo_consumers("k1", "g1");
    assert!(result.is_ok());
    let reply = result.unwrap();
    assert_eq!(&reply.consumers.len(), &1);
    assert_eq!(&reply.consumers[0].name, &"c1");

    // second call will not create consumer
    let result = con.xgroup_createconsumer("k1", "g1", "c1");
    assert!(matches!(result, Ok(false)));

    // xinfo consumers (consumer still exists)
    let result = con.xinfo_consumers("k1", "g1");
    assert!(result.is_ok());
    let reply = result.unwrap();
    assert_eq!(&reply.consumers.len(), &1);
    assert_eq!(&reply.consumers[0].name, &"c1");
}

#[test]
fn test_assorted_2() {
    // Tests the following commands....
    // xadd
    // xinfo_stream
    // xinfo_groups
    // xinfo_consumer
    // xgroup_create_mkstream
    // xread_options
    // xack
    // xpending
    // xpending_count
    // xpending_consumer_count

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    xadd(&mut con);

    // test xgroup create w/ mkstream @ 0
    let result = con.xgroup_create_mkstream("k99", "g99", "0");
    assert!(result.is_ok());

    // Since nothing exists on this stream yet,
    // it should have the defaults returned by the client
    let result = con.xinfo_groups("k99");
    assert!(result.is_ok());
    let reply = result.unwrap();
    assert_eq!(&reply.groups.len(), &1);
    assert_eq!(&reply.groups[0].name, &"g99");
    assert_eq!(&reply.groups[0].last_delivered_id, &"0-0");
    if let Some(lag) = reply.groups[0].lag {
        assert_eq!(lag, 0);
    }

    // call xadd on k99 just so we can read from it
    // using consumer g99 and test xinfo_consumers
    let _ = con.xadd("k99", "1000-0", &[("a", "b"), ("c", "d")]);
    let _ = con.xadd("k99", "1000-1", &[("e", "f"), ("g", "h")]);

    // Two messages have been added but not acked:
    // this should give us a `lag` of 2 (if the server supports it)
    let result = con.xinfo_groups("k99");
    assert!(result.is_ok());
    let reply = result.unwrap();
    assert_eq!(&reply.groups.len(), &1);
    assert_eq!(&reply.groups[0].name, &"g99");
    if let Some(lag) = reply.groups[0].lag {
        assert_eq!(lag, 2);
    }

    // test empty PEL
    let empty_reply = con.xpending("k99", "g99").unwrap();

    assert_eq!(empty_reply.count(), 0);
    if let StreamPendingReply::Empty = empty_reply {
        // looks good
    } else {
        panic!("Expected StreamPendingReply::Empty but got Data");
    }

    // passing options  w/ group triggers XREADGROUP
    // using ID=">" means all undelivered ids
    // otherwise, ID="0 | ms-num" means all pending already
    // sent to this client
    let reply = con
        .xread_options(
            &["k99"],
            &[">"],
            &StreamReadOptions::default().group("g99", "c99"),
        )
        .unwrap()
        .unwrap();
    assert_eq!(reply.keys[0].ids.len(), 2);

    // read xinfo consumers again, should have 2 messages for the c99 consumer
    let reply = con.xinfo_consumers("k99", "g99").unwrap();
    assert_eq!(reply.consumers[0].pending, 2);

    // ack one of these messages
    let result = con.xack("k99", "g99", &["1000-0"]);
    assert_eq!(result, Ok(1));

    // get pending messages already seen by this client
    // we should only have one now..
    let reply = con
        .xread_options(
            &["k99"],
            &["0"],
            &StreamReadOptions::default().group("g99", "c99"),
        )
        .unwrap()
        .unwrap();
    assert_eq!(reply.keys.len(), 1);

    // we should also have one pending here...
    let reply = con.xinfo_consumers("k99", "g99").unwrap();
    assert_eq!(reply.consumers[0].pending, 1);

    // add more and read so we can test xpending
    let _ = con.xadd("k99", "1001-0", &[("i", "j"), ("k", "l")]);
    let _ = con.xadd("k99", "1001-1", &[("m", "n"), ("o", "p")]);
    let _ = con
        .xread_options(
            &["k99"],
            &[">"],
            &StreamReadOptions::default().group("g99", "c99"),
        )
        .unwrap();

    // call xpending here...
    // this has a different reply from what the count variations return
    let data_reply = con.xpending("k99", "g99").unwrap();

    assert_eq!(data_reply.count(), 3);

    if let StreamPendingReply::Data(data) = data_reply {
        assert_stream_pending_data(data)
    } else {
        panic!("Expected StreamPendingReply::Data but got Empty");
    }

    // both count variations have the same reply types
    let reply = con.xpending_count("k99", "g99", "-", "+", 10).unwrap();
    assert_eq!(reply.ids.len(), 3);

    let reply = con
        .xpending_consumer_count("k99", "g99", "-", "+", 10, "c99")
        .unwrap();
    assert_eq!(reply.ids.len(), 3);

    for StreamPendingId {
        id,
        consumer,
        times_delivered,
        last_delivered_ms: _,
    } in reply.ids
    {
        assert!(!id.is_empty());
        assert!(!consumer.is_empty());
        assert!(times_delivered > 0);
    }
}

fn assert_stream_pending_data(data: StreamPendingData) {
    assert_eq!(data.start_id, "1000-1");
    assert_eq!(data.end_id, "1001-1");
    assert_eq!(data.consumers.len(), 1);
    assert_eq!(data.consumers[0].name, "c99");
}

#[test]
fn test_xadd_maxlen_map() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    for i in 0..10 {
        let mut map = BTreeMap::new();
        let idx = i.to_string();
        map.insert("idx", &idx);
        let _ = con.xadd_maxlen_map("maxlen_map", StreamMaxlen::Equals(3), "*", map);
    }

    let result = con.xlen("maxlen_map");
    assert_eq!(result, Ok(3));
    let reply = con.xrange_all("maxlen_map").unwrap();

    assert_eq!(reply.ids[0].get("idx"), Some("7".to_string()));
    assert_eq!(reply.ids[1].get("idx"), Some("8".to_string()));
    assert_eq!(reply.ids[2].get("idx"), Some("9".to_string()));
}

#[test]
fn test_xadd_options() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // NoMKStream will return a nil when the stream does not exist
    let result = con.xadd_options(
        "k1",
        "*",
        &[("h", "w")],
        &StreamAddOptions::default().nomkstream(),
    );
    assert_eq!(result, Ok(None));

    let result = con.xinfo_stream("k1");
    assert!(
        matches!(&result, Err(e) if e.kind() == redis::ServerErrorKind::ResponseError.into()
            && e.code() == Some("ERR")
            && e.detail() == Some("no such key"))
    );

    fn setup_stream(con: &mut Connection) {
        let _ = con.del("k1");

        for i in 0..10 {
            let _ = con.xadd_options(
                "k1",
                format!("1-{i}"),
                &[("h", "w")],
                &StreamAddOptions::default(),
            );
        }
    }

    // test trim by maxlen
    setup_stream(&mut con);

    let _ = con.xadd_options(
        "k1",
        "2-1",
        &[("h", "w")],
        &StreamAddOptions::default().trim(StreamTrimStrategy::maxlen(StreamTrimmingMode::Exact, 4)),
    );

    let info = con.xinfo_stream("k1").unwrap();
    assert_eq!(info.length, 4);
    assert_eq!(info.first_entry.id, "1-7");

    // test with trim by minid
    setup_stream(&mut con);

    let _ = con.xadd_options(
        "k1",
        "2-1",
        &[("h", "w")],
        &StreamAddOptions::default()
            .trim(StreamTrimStrategy::minid(StreamTrimmingMode::Exact, "1-5")),
    );
    let info = con.xinfo_stream("k1").unwrap();
    assert_eq!(info.length, 6);
    assert_eq!(info.first_entry.id, "1-5");

    // test adding from a map
    let mut map = BTreeMap::new();
    map.insert("ab", "cd");
    map.insert("ef", "gh");
    map.insert("ij", "kl");
    let _ = con.xadd_options("k1", "3-1", map, &StreamAddOptions::default());

    let info = con.xinfo_stream("k1").unwrap();
    assert_eq!(info.length, 7);
    assert_eq!(info.first_entry.id, "1-5");
    assert_eq!(info.last_entry.id, "3-1");
}

#[test]
fn test_xread_options_deleted_pel_entry() {
    // Test xread_options behaviour with deleted entry
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let result = con.xgroup_create_mkstream("k1", "g1", "$");
    assert!(result.is_ok());
    let _ = con.xadd_maxlen("k1", StreamMaxlen::Equals(1), "*", &[("h1", "w1")]);
    // read the pending items for this key & group
    let result = con
        .xread_options(
            &["k1"],
            &[">"],
            &StreamReadOptions::default().group("g1", "c1"),
        )
        .unwrap()
        .unwrap();

    let _ = con.xadd_maxlen("k1", StreamMaxlen::Equals(1), "*", &[("h2", "w2")]);
    let result_deleted_entry = con
        .xread_options(
            &["k1"],
            &["0"],
            &StreamReadOptions::default().group("g1", "c1"),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        result.keys[0].ids.len(),
        result_deleted_entry.keys[0].ids.len()
    );
    assert_eq!(
        result.keys[0].ids[0].id,
        result_deleted_entry.keys[0].ids[0].id
    );
}

fn create_group_add_and_read(con: &mut Connection) -> StreamReadReply {
    con.flushall().unwrap();
    let result = con.xgroup_create_mkstream("k1", "g1", "$");
    assert!(result.is_ok());

    xadd_keyrange(con, "k1", 0, 10);

    let reply = con
        .xread_options(
            &["k1"],
            &[">"],
            &StreamReadOptions::default().group("g1", "c1"),
        )
        .unwrap()
        .unwrap();
    assert_eq!(reply.keys[0].ids.len(), 10);
    reply
}

#[test]
fn test_xautoclaim() {
    // Tests the following command....
    // xautoclaim_options
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // xautoclaim test basic idea:
    // 1. we need to test adding messages to a group
    // 2. then xreadgroup needs to define a consumer and read pending
    //    messages without acking them
    // 3. then we need to sleep 5ms and call xautoclaim to claim message
    //    past the idle time and read them from a different consumer

    let reply = create_group_add_and_read(&mut con);

    // save this StreamId for later
    let claim = &reply.keys[0].ids[0];
    let claim_1 = &reply.keys[0].ids[1];

    // sleep for 5ms
    sleep(Duration::from_millis(10));

    // grab this id if > 4ms
    let reply = con
        .xautoclaim_options(
            "k1",
            "g1",
            "c2",
            4,
            claim.id.clone(),
            StreamAutoClaimOptions::default().count(2),
        )
        .unwrap();
    assert_eq!(reply.claimed.len(), 2);
    assert_eq!(reply.claimed[0].id, claim.id);
    assert!(!reply.claimed[0].map.is_empty());
    assert_eq!(reply.claimed[1].id, claim_1.id);
    assert!(!reply.claimed[1].map.is_empty());

    // sleep for 5ms
    sleep(Duration::from_millis(5));

    // let's test some of the xautoclaim_options
    // call force on the same claim.id
    let reply = con
        .xautoclaim_options(
            "k1",
            "g1",
            "c3",
            4,
            claim.id.clone(),
            StreamAutoClaimOptions::default().count(5).with_justid(),
        )
        .unwrap();

    // we just claimed the first original 5 ids
    // and only returned the ids
    assert_eq!(reply.claimed.len(), 5);
    assert_eq!(reply.claimed[0].id, claim.id);
    assert!(reply.claimed[0].map.is_empty());
    assert_eq!(reply.claimed[1].id, claim_1.id);
    assert!(reply.claimed[1].map.is_empty());
}

#[test]
fn test_xclaim() {
    // Tests the following commands....
    // xclaim
    // xclaim_options
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // xclaim test basic idea:
    // 1. we need to test adding messages to a group
    // 2. then xreadgroup needs to define a consumer and read pending
    //    messages without acking them
    // 3. then we need to sleep 5ms and call xpending
    // 4. from here we should be able to claim message
    //    past the idle time and read them from a different consumer

    let reply = create_group_add_and_read(&mut con);

    // save this StreamId for later
    let claim = &reply.keys[0].ids[0];
    let claim_justids = &reply.keys[0]
        .ids
        .iter()
        .map(|msg| &msg.id)
        .collect::<Vec<&String>>();

    // sleep for 5ms
    sleep(Duration::from_millis(5));

    // grab this id if > 4ms
    let reply = con
        .xclaim("k1", "g1", "c2", 4, slice::from_ref(&claim.id))
        .unwrap();
    assert_eq!(reply.ids.len(), 1);
    assert_eq!(reply.ids[0].id, claim.id);

    // grab all pending ids for this key...
    // we should 9 in c1 and 1 in c2
    let reply = con.xpending("k1", "g1").unwrap();
    if let StreamPendingReply::Data(data) = reply {
        assert_eq!(data.consumers[0].name, "c1");
        assert_eq!(data.consumers[0].pending, 9);
        assert_eq!(data.consumers[1].name, "c2");
        assert_eq!(data.consumers[1].pending, 1);
    }

    // sleep for 5ms
    sleep(Duration::from_millis(5));

    // lets test some of the xclaim_options
    // call force on the same claim.id
    let _: StreamClaimReply = con
        .xclaim_options(
            "k1",
            "g1",
            "c3",
            4,
            slice::from_ref(&claim.id),
            StreamClaimOptions::default().with_force(),
        )
        .unwrap();

    let reply = con.xpending("k1", "g1").unwrap();
    // we should have 9 w/ c1 and 1 w/ c3 now
    if let StreamPendingReply::Data(data) = reply {
        assert_eq!(data.consumers[1].name, "c3");
        assert_eq!(data.consumers[1].pending, 1);
    }

    // sleep for 5ms
    sleep(Duration::from_millis(5));

    // claim and only return JUSTID
    let claimed: Vec<String> = con
        .xclaim_options(
            "k1",
            "g1",
            "c5",
            4,
            claim_justids,
            StreamClaimOptions::default().with_force().with_justid(),
        )
        .unwrap();
    // we just claimed the original 10 ids
    // and only returned the ids
    assert_eq!(claimed.len(), 10);
}

#[test]
fn test_xclaim_last_id() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let result = con.xgroup_create_mkstream("k1", "g1", "$");
    assert!(result.is_ok());

    // add some keys
    xadd_keyrange(&mut con, "k1", 0, 10);

    let reply = con
        .xread_options(&["k1"], &["0"], &StreamReadOptions::default())
        .unwrap()
        .unwrap();
    // verify we have 10 ids
    assert_eq!(reply.keys[0].ids.len(), 10);

    let claim_early_id = &reply.keys[0].ids[3];
    let claim_middle_id = &reply.keys[0].ids[5];
    let claim_late_id = &reply.keys[0].ids[8];

    // get read up to the middle record
    let _ = con
        .xread_options(
            &["k1"],
            &[">"],
            &StreamReadOptions::default().count(6).group("g1", "c1"),
        )
        .unwrap();

    let info = con.xinfo_groups("k1").unwrap();
    assert_eq!(info.groups[0].last_delivered_id, claim_middle_id.id.clone());

    // sleep for 5ms
    sleep(Duration::from_millis(5));

    let _: Vec<String> = con
        .xclaim_options(
            "k1",
            "g1",
            "c2",
            4,
            slice::from_ref(&claim_middle_id.id),
            StreamClaimOptions::default()
                .with_justid()
                .with_lastid(claim_early_id.id.as_str()),
        )
        .unwrap();

    // lastid is kept at the 6th entry as the 4th entry is OLDER than the last_delivered_id
    let info = con.xinfo_groups("k1").unwrap();
    assert_eq!(info.groups[0].last_delivered_id, claim_middle_id.id.clone());

    // sleep for 5ms
    sleep(Duration::from_millis(5));

    let _: Vec<String> = con
        .xclaim_options(
            "k1",
            "g1",
            "c1",
            4,
            slice::from_ref(&claim_middle_id.id),
            StreamClaimOptions::default()
                .with_justid()
                .with_lastid(claim_late_id.id.as_str()),
        )
        .unwrap();

    // lastid is moved to the 8th entry as it is NEWER than the last_delivered_id
    let info = con.xinfo_groups("k1").unwrap();
    assert_eq!(info.groups[0].last_delivered_id, claim_late_id.id.clone());
}

#[test]
fn test_xreadgroup_with_claim_option() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
    let mut con = ctx.connection();

    let stream_name = "test_stream";
    let group_name = "test_group";
    let consumer1 = "consumer1";
    let consumer2 = "consumer2";

    // Create a consumer group and add 10 messages to the stream
    assert!(con
        .xgroup_create_mkstream(stream_name, group_name, "$")
        .is_ok());
    xadd_keyrange(&mut con, stream_name, 0, 10);

    // Consumer1 reads all messages without acking them
    let reply = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, consumer1)
                .count(10),
        )
        .unwrap()
        .unwrap();
    assert_eq!(reply.keys[0].ids.len(), 10);
    let message_identifiers: Vec<String> =
        reply.keys[0].ids.iter().map(|msg| msg.id.clone()).collect();

    // Verify that consumer1 has 10 pending messages
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 10);
    if let StreamPendingReply::Data(data) = pending {
        assert_eq!(data.consumers.len(), 1);
        assert_eq!(data.consumers[0].name, consumer1);
        assert_eq!(data.consumers[0].pending, 10);
    }

    // Sleep to make messages idle
    sleep(Duration::from_millis(10));

    // Consumer2 uses XREADGROUP with CLAIM option to claim idle messages
    let claim_reply: Option<StreamReadReply> = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, consumer2)
                .claim(5) // Claim messages idle for at least 5ms
                .count(5),
        )
        .unwrap();
    let claim_reply = claim_reply.unwrap();

    // Verify that consumer2 claimed up to 5 messages from consumer1
    assert_eq!(claim_reply.keys.len(), 1);
    assert_eq!(claim_reply.keys[0].key, stream_name);
    assert!(!claim_reply.keys[0].ids.is_empty());
    assert!(claim_reply.keys[0].ids.len() <= 5);

    // Verify that the claimed messages have additional PEL information
    for stream_id in &claim_reply.keys[0].ids {
        assert!(stream_id.milliseconds_elapsed_from_delivery.unwrap() > 0);
        assert!(stream_id.delivered_count.unwrap() > 0);
    }

    // Verify that the claimed messages are the ones that were pending for consumer1
    let claimed_ids: Vec<String> = claim_reply.keys[0]
        .ids
        .iter()
        .map(|id| id.id.clone())
        .collect();
    for claimed_id in &claimed_ids {
        assert!(message_identifiers.contains(claimed_id));
    }

    // Check the PEL to verify ownership transfer
    let pending_info = con
        .xpending_count(stream_name, group_name, "-", "+", 10)
        .unwrap();
    let mut consumer1_count = 0;
    let mut consumer2_count = 0;
    for pending_id in &pending_info.ids {
        if pending_id.consumer == consumer1 {
            consumer1_count += 1;
        } else {
            consumer2_count += 1;
        }
    }

    // Consumer1 should have fewer messages than before
    assert!(consumer1_count < 10);
    // Consumer2 should now own the claimed messages
    assert!(consumer2_count > 0);
    // The total number of messages should still be 10 as no messages were acked
    assert_eq!(consumer1_count + consumer2_count, 10);
}

#[test]
fn test_xreadgroup_claim_with_idle_and_incoming_messages() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
    let mut con = ctx.connection();

    let stream_name = "test_stream_claim_with_idle_and_incoming_messages";
    let group_name = "test_group";
    let consumer1 = "consumer1";
    let consumer2 = "consumer2";

    // Create a consumer group and add 2 messages to the stream
    assert!(con
        .xgroup_create_mkstream(stream_name, group_name, "$")
        .is_ok());
    xadd_keyrange(&mut con, stream_name, 0, 2);

    // Consumer1 reads the 2 messages without acking them (these will become idle pending messages)
    let initial_reply = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, consumer1)
                .count(2),
        )
        .unwrap()
        .unwrap();
    assert_eq!(initial_reply.keys[0].ids.len(), 2);
    let idle_pending_ids: Vec<String> = initial_reply.keys[0]
        .ids
        .iter()
        .map(|id| id.id.clone())
        .collect();

    // Sleep to make the first 2 messages idle
    sleep(Duration::from_millis(20));

    // Add 20 new incoming messages to the stream
    xadd_keyrange(&mut con, stream_name, 2, 22);

    // Consumer2 uses XREADGROUP with CLAIM and COUNT 10 and should receive 10 messages (2 idle + 8 incoming)
    let claim_reply: Option<StreamReadReply> = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, consumer2)
                .claim(5)  // Claim messages idle for at least 5ms
                .count(10),
        )
        .unwrap();
    let claim_reply = claim_reply.unwrap();

    // Verify that consumer2 got exactly 10 messages
    assert_eq!(claim_reply.keys.len(), 1);
    assert_eq!(claim_reply.keys[0].key, stream_name);
    assert_eq!(claim_reply.keys[0].ids.len(), 10);

    // Verify the ordering: idle pending messages are first, followed by the incoming ones
    // First 2 messages should be the idle pending messages and they should have additional PEL information:
    //  - milliseconds_elapsed_from_delivery > 0
    //  - delivered_count > 0
    assert_eq!(idle_pending_ids[0], claim_reply.keys[0].ids[0].id);
    assert_eq!(idle_pending_ids[1], claim_reply.keys[0].ids[1].id);
    for i in 0..2 {
        let stream_id = &claim_reply.keys[0].ids[i];
        assert!(stream_id.milliseconds_elapsed_from_delivery.unwrap() > 0);
        assert!(stream_id.delivered_count.unwrap() > 0);
    }
    // Verify that the remaining 8 messages are new, incoming messages (not previously delivered)
    for i in 2..10 {
        let stream_id = &claim_reply.keys[0].ids[i];
        assert_eq!(stream_id.milliseconds_elapsed_from_delivery.unwrap(), 0);
        assert_eq!(stream_id.delivered_count.unwrap(), 0);
        // These messages should NOT be in the idle pending list
        assert!(!idle_pending_ids.contains(&stream_id.id));
    }

    // Verify ownership in PEL
    let pending_info = con
        .xpending_count(stream_name, group_name, "-", "+", 30)
        .unwrap();
    let mut consumer1_count = 0;
    let mut consumer2_count = 0;
    for pending_id in &pending_info.ids {
        if pending_id.consumer == consumer1 {
            consumer1_count += 1;
        } else {
            consumer2_count += 1;
        }
    }

    // Consumer1 should have 0 messages (they were claimed by consumer2)
    assert_eq!(consumer1_count, 0);
    // Consumer2 should have 10 messages (2 claimed + 8 new)
    assert_eq!(consumer2_count, 10);

    // Sleep to make all messages idle
    sleep(Duration::from_millis(10));

    // Test without COUNT to verify that all messages are returned
    let claim_all_reply: Option<StreamReadReply> = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, consumer1)
                .claim(5), // No COUNT specified
        )
        .unwrap();
    let claim_all_reply = claim_all_reply.unwrap();
    // Should get: 10 idle pending (from consumer2) + 12 remaining incoming = 22 total
    assert_eq!(claim_all_reply.keys.len(), 1);
    assert_eq!(claim_all_reply.keys[0].ids.len(), 22);

    // Verify that the first 10 are the idle pending messages
    for i in 0..10 {
        let stream_id = &claim_all_reply.keys[0].ids[i];
        assert!(stream_id.milliseconds_elapsed_from_delivery.unwrap() > 0);
        assert!(stream_id.delivered_count.unwrap() > 0);
    }

    // Verify that the rest are new, incoming messages
    for i in 10..22 {
        let stream_id = &claim_all_reply.keys[0].ids[i];
        assert_eq!(stream_id.milliseconds_elapsed_from_delivery.unwrap(), 0);
        assert_eq!(stream_id.delivered_count.unwrap(), 0);
    }
}

#[test]
fn test_xreadgroup_claim_multiple_streams() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
    let mut con = ctx.connection();

    let stream1 = "test_stream_claim_multi_1";
    let stream2 = "test_stream_claim_multi_2";
    let stream3 = "test_stream_claim_multi_3";
    let group_name = "test_group";
    let consumer1 = "consumer1";
    let consumer2 = "consumer2";

    // Create consumer groups for all three streams with 5 messages in each stream
    for stream_name in [stream1, stream2, stream3] {
        assert!(con
            .xgroup_create_mkstream(stream_name, group_name, "$")
            .is_ok());

        xadd_keyrange(&mut con, stream_name, 0, 5);
    }

    // Consumer1 reads from all streams without acking (these will become idle pending)
    let initial_reply: Option<StreamReadReply> = con
        .xread_options(
            &[stream1, stream2, stream3],
            &[">", ">", ">"],
            &StreamReadOptions::default()
                .group(group_name, consumer1)
                .count(15),
        )
        .unwrap();
    let initial_reply = initial_reply.unwrap();
    assert_eq!(initial_reply.keys.len(), 3);

    // Sleep to make messages idle
    sleep(Duration::from_millis(20));

    // Consumer2 claims from all streams
    let claim_reply: Option<StreamReadReply> = con
        .xread_options(
            &[stream1, stream2, stream3],
            &[">", ">", ">"],
            &StreamReadOptions::default()
                .group(group_name, consumer2)
                .claim(5), // Claim messages idle for at least 5ms
        )
        .unwrap();
    let claim_reply = claim_reply.unwrap();
    // Verify that the results are from all three streams
    assert_eq!(claim_reply.keys.len(), 3);

    // Verify that each stream has the expected number of messages
    for stream_key in &claim_reply.keys {
        assert_eq!(stream_key.ids.len(), 5);
        for stream_id in &stream_key.ids {
            assert!(stream_id.milliseconds_elapsed_from_delivery.unwrap() > 0);
            assert!(stream_id.delivered_count.unwrap() > 0);
        }
    }

    // Verify ownership transfer in PEL
    // Consumer1 should have no pending messages
    let consumer1_pending: StreamPendingCountReply = con
        .xpending_consumer_count(stream1, group_name, "-", "+", 10, consumer1)
        .unwrap();
    assert_eq!(consumer1_pending.ids.len(), 0);

    // Consumer2 should have all messages from stream1
    let consumer2_pending: StreamPendingCountReply = con
        .xpending_consumer_count(stream1, group_name, "-", "+", 10, consumer2)
        .unwrap();
    assert_eq!(consumer2_pending.ids.len(), 5);
}

#[test]
fn test_xdel() {
    // Tests the following commands....
    // xdel
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // add some keys
    xadd(&mut con);

    // delete the first stream item for this key
    let result = con.xdel("k1", &["1000-0"]);
    // returns the number of items deleted
    assert_eq!(result, Ok(1));

    let result = con.xdel("k2", &["2000-0", "2000-1", "2000-2"]);
    // should equal 2 since the last id doesn't exist
    assert_eq!(result, Ok(2));
}

#[test]
fn test_xadd_options_deletion_policy_keepref() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_2);
    let mut con = ctx.connection();
    let _: () = con.flushdb().unwrap();

    let stream_name = "test_stream_xadd_keepref";
    let group_name = "test_group";
    let consumer_name = "consumer";

    let initial_stream_entries = [
        ("field1", "value1"),
        ("field2", "value2"),
        ("field3", "value3"),
    ];

    let stream_entries: [(&str, &str); 3] = initial_stream_entries[..3].try_into().unwrap();
    let [id1, id2, id3]: [String; 3] =
        stream_entries.map(|entry| con.xadd(stream_name, "*", &[entry]).unwrap().unwrap());

    // Create a consumer group and read all initial messages to create PEL entries.
    let _: () = con
        .xgroup_create_mkstream(stream_name, group_name, "0")
        .unwrap();
    let _: Option<StreamReadReply> = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, consumer_name)
                .count(initial_stream_entries.len() + 1),
        )
        .unwrap();
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), initial_stream_entries.len());

    // Add a new entry with the following options:
    // - Apply a trimming strategy to ensure the number of entries does not exceed the initial number of entries
    // - Apply the KeepRef deletion policy to keep existing references to entries that will be removed as a result of trimming
    let id4 = con
        .xadd_options(
            stream_name,
            "*",
            &[("field4", "value4")],
            &StreamAddOptions::default()
                .trim(StreamTrimStrategy::maxlen(
                    StreamTrimmingMode::Exact,
                    initial_stream_entries.len(),
                ))
                .set_deletion_policy(StreamDeletionPolicy::KeepRef),
        )
        .unwrap()
        .unwrap();

    // The stream should have been trimmed as its entries exceeded the maximum length.
    // As a result, the first entry should have been removed.
    assert_eq!(con.xlen(stream_name).unwrap(), initial_stream_entries.len());
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.first_entry.id, id2);
    assert_eq!(info.last_generated_id, id4);

    // The PEL should still have references to the initial entries.
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), initial_stream_entries.len());
    // Check that these references are indeed pointing to the initial entries.
    let reply = con
        .xpending_consumer_count(
            stream_name,
            group_name,
            "-",
            "+",
            initial_stream_entries.len(),
            consumer_name,
        )
        .unwrap();
    assert_eq!(
        vec![id1, id2, id3],
        reply
            .ids
            .iter()
            .map(|id| id.id.clone())
            .collect::<Vec<String>>()
    );
}

#[test]
fn test_xadd_options_deletion_policy_delref() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_2);
    let mut con = ctx.connection();
    let _: () = con.flushdb().unwrap();

    let stream_name = "test_stream_xadd_delref";
    let group_name = "test_group";
    let consumer_name = "consumer";

    let initial_stream_entries = [
        ("field1", "value1"),
        ("field2", "value2"),
        ("field3", "value3"),
    ];

    let stream_entries: [(&str, &str); 3] = initial_stream_entries[..3].try_into().unwrap();
    let [_, id2, id3]: [String; 3] =
        stream_entries.map(|entry| con.xadd(stream_name, "*", &[entry]).unwrap().unwrap());

    // Create a consumer group and read all initial messages to create PEL entries.
    let _: () = con
        .xgroup_create_mkstream(stream_name, group_name, "0")
        .unwrap();
    let _: Option<StreamReadReply> = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, consumer_name)
                .count(initial_stream_entries.len() + 1),
        )
        .unwrap();
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), initial_stream_entries.len());

    // Add a new entry with the following options:
    // - Apply a trimming strategy to ensure the number of entries does not exceed the initial number of entries
    // - Apply the DelRef deletion policy to erase any existing references to entries that will be removed as a result of trimming
    let id4 = con
        .xadd_options(
            stream_name,
            "*",
            &[("field4", "value4")],
            &StreamAddOptions::default()
                .trim(StreamTrimStrategy::maxlen(
                    StreamTrimmingMode::Exact,
                    initial_stream_entries.len(),
                ))
                .set_deletion_policy(StreamDeletionPolicy::DelRef),
        )
        .unwrap()
        .unwrap();

    // The stream should have been trimmed as its entries exceeded the maximum length.
    // As a result, the first entry should have been removed.
    assert_eq!(con.xlen(stream_name).unwrap(), initial_stream_entries.len());
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.first_entry.id, id2);
    assert_eq!(info.last_generated_id, id4);

    // The PEL should now hold one less reference than the initial number of references as the first one was removed by the deletion policy.
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), initial_stream_entries.len() - 1);
    // Check that the remaining references point to the remaining initial entries.
    let reply = con
        .xpending_consumer_count(
            stream_name,
            group_name,
            "-",
            "+",
            initial_stream_entries.len(),
            consumer_name,
        )
        .unwrap();
    assert_eq!(
        vec![id2, id3],
        reply
            .ids
            .iter()
            .map(|id| id.id.clone())
            .collect::<Vec<String>>()
    );
}

#[test]
fn test_xadd_options_deletion_policy_acked() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_2);
    let mut con = ctx.connection();
    let _: () = con.flushdb().unwrap();

    let stream_name = "test_stream_xadd_acked";
    let group_name = "test_group";
    let consumer_name = "consumer";

    let initial_stream_entries = [
        ("field1", "value1"),
        ("field2", "value2"),
        ("field3", "value3"),
    ];

    let stream_entries: [(&str, &str); 3] = initial_stream_entries[..3].try_into().unwrap();
    let [id1, id2, id3]: [String; 3] =
        stream_entries.map(|entry| con.xadd(stream_name, "*", &[entry]).unwrap().unwrap());

    // Create a consumer group and read all initial messages to create PEL entries.
    let _: () = con
        .xgroup_create_mkstream(stream_name, group_name, "0")
        .unwrap();
    let _: Option<StreamReadReply> = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, consumer_name)
                .count(initial_stream_entries.len() + 1),
        )
        .unwrap();
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), initial_stream_entries.len());

    // Add a new entry with the following options:
    // - Apply a trimming strategy to ensure the number of entries does not exceed the initial number of entries
    // - Apply the Acked deletion policy, which removes entries and their references only if they have been delivered to a consumer and acknowledged
    let id4 = con
        .xadd_options(
            stream_name,
            "*",
            &[("field4", "value4")],
            &StreamAddOptions::default()
                .trim(StreamTrimStrategy::maxlen(
                    StreamTrimmingMode::Exact,
                    initial_stream_entries.len(),
                ))
                .set_deletion_policy(StreamDeletionPolicy::Acked),
        )
        .unwrap()
        .unwrap();

    // The stream should NOT have been trimmed even though its entries exceeded the maximum length.
    // The first entry should still be present, in accordance with the deletion policy.
    assert_eq!(
        con.xlen(stream_name).unwrap(),
        initial_stream_entries.len() + 1
    );
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.first_entry.id, id1);
    assert_eq!(info.last_generated_id, id4);

    // The PEL should still have references to the initial entries.
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), initial_stream_entries.len());
    // Check that these references are indeed pointing to the initial entries.
    let reply = con
        .xpending_consumer_count(
            stream_name,
            group_name,
            "-",
            "+",
            initial_stream_entries.len(),
            consumer_name,
        )
        .unwrap();
    assert_eq!(
        vec![id1, id2, id3],
        reply
            .ids
            .iter()
            .map(|id| id.id.clone())
            .collect::<Vec<String>>()
    );
}

#[test]
fn test_xtrim_options_deletion_policy_keepref() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_2);
    let mut con = ctx.connection();
    let _: () = con.flushdb().unwrap();

    let stream_name = "test_stream_xtrim_keepref";
    let group_name = "test_group";
    let consumer_name = "consumer";

    let initial_stream_entries = [
        ("field1", "value1"),
        ("field2", "value2"),
        ("field3", "value3"),
    ];

    let stream_entries: [(&str, &str); 3] = initial_stream_entries[..3].try_into().unwrap();
    let [id1, id2, id3]: [String; 3] =
        stream_entries.map(|entry| con.xadd(stream_name, "*", &[entry]).unwrap().unwrap());

    // Create a consumer group and read all initial messages to create PEL entries.
    let _: () = con
        .xgroup_create_mkstream(stream_name, group_name, "0")
        .unwrap();
    let _: Option<StreamReadReply> = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, consumer_name)
                .count(initial_stream_entries.len() + 1),
        )
        .unwrap();
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), initial_stream_entries.len());

    // Trim the stream to remove the first entry with the following options:
    // - Apply a trimming strategy to remove the first entry
    // - Apply the KeepRef deletion policy to keep existing references to entries that will be removed as a result of trimming
    let _: usize = con
        .xtrim_options(
            stream_name,
            &StreamTrimOptions::minid(StreamTrimmingMode::Exact, id2.clone())
                .set_deletion_policy(StreamDeletionPolicy::KeepRef),
        )
        .unwrap();
    // The stream should have been trimmed.
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, initial_stream_entries.len() - 1);
    assert_eq!(info.first_entry.id, id2);
    // Check that the PEL entries have been preserved.
    let reply = con
        .xpending_consumer_count(
            stream_name,
            group_name,
            "-",
            "+",
            initial_stream_entries.len(),
            consumer_name,
        )
        .unwrap();
    assert_eq!(
        vec![id1, id2, id3],
        reply
            .ids
            .iter()
            .map(|id| id.id.clone())
            .collect::<Vec<String>>()
    );
}

#[test]
fn test_xtrim_options_deletion_policy_delref() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_2);
    let mut con = ctx.connection();
    let _: () = con.flushdb().unwrap();

    let stream_name = "test_stream_xtrim_delref";
    let group_name = "test_group";
    let consumer_name = "consumer";

    let initial_stream_entries = [
        ("field1", "value1"),
        ("field2", "value2"),
        ("field3", "value3"),
    ];

    let stream_entries: [(&str, &str); 3] = initial_stream_entries[..3].try_into().unwrap();
    let [_, id2, id3]: [String; 3] =
        stream_entries.map(|entry| con.xadd(stream_name, "*", &[entry]).unwrap().unwrap());

    // Create a consumer group and read all initial messages to create PEL entries.
    let _: () = con
        .xgroup_create_mkstream(stream_name, group_name, "0")
        .unwrap();
    let _: Option<StreamReadReply> = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, consumer_name)
                .count(initial_stream_entries.len() + 1),
        )
        .unwrap();
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), initial_stream_entries.len());

    // Trim the stream to remove the first entry with the following options:
    // - Apply a trimming strategy to remove the first entry
    // - Apply the DelRef deletion policy to remove any existing references to entries that will be removed as a result of trimming
    let _: usize = con
        .xtrim_options(
            stream_name,
            &StreamTrimOptions::minid(StreamTrimmingMode::Exact, id2.clone())
                .set_deletion_policy(StreamDeletionPolicy::DelRef),
        )
        .unwrap();
    // The stream should have been trimmed.
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, initial_stream_entries.len() - 1);
    assert_eq!(info.first_entry.id, id2);
    // Check that the PEL entry for the first entry has been removed.
    let reply = con
        .xpending_consumer_count(
            stream_name,
            group_name,
            "-",
            "+",
            initial_stream_entries.len(),
            consumer_name,
        )
        .unwrap();
    assert_eq!(
        vec![id2, id3],
        reply
            .ids
            .iter()
            .map(|id| id.id.clone())
            .collect::<Vec<String>>()
    );
}

#[test]
fn test_xtrim_options_deletion_policy_acked() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_2);
    let mut con = ctx.connection();
    let _: () = con.flushdb().unwrap();

    let stream_name = "test_stream_xtrim_acked";
    let group_name = "test_group";
    let consumer_name = "consumer";

    let initial_stream_entries = [
        ("field1", "value1"),
        ("field2", "value2"),
        ("field3", "value3"),
    ];

    let stream_entries: [(&str, &str); 3] = initial_stream_entries[..3].try_into().unwrap();
    let [id1, id2, id3]: [String; 3] =
        stream_entries.map(|entry| con.xadd(stream_name, "*", &[entry]).unwrap().unwrap());

    // Create a consumer group and read all initial messages to create PEL entries.
    let _: () = con
        .xgroup_create_mkstream(stream_name, group_name, "0")
        .unwrap();
    let _: Option<StreamReadReply> = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, consumer_name)
                .count(initial_stream_entries.len() + 1),
        )
        .unwrap();
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), initial_stream_entries.len());

    // Trim the stream to remove the first entry with the following options:
    // - Apply a trimming strategy to remove the first entry
    // - Apply the Acked deletion policy to remove the entry only if it has been acknowledged
    let _: usize = con
        .xtrim_options(
            stream_name,
            &StreamTrimOptions::minid(StreamTrimmingMode::Exact, id2.clone())
                .set_deletion_policy(StreamDeletionPolicy::Acked),
        )
        .unwrap();
    // The stream should NOT have been trimmed, as the entry to be trimmed was not acknowledged.
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, initial_stream_entries.len());
    assert_eq!(info.first_entry.id, id1);
    // PEL entries should remain unchanged because no stream trimming was performed.
    let reply = con
        .xpending_consumer_count(
            stream_name,
            group_name,
            "-",
            "+",
            initial_stream_entries.len(),
            consumer_name,
        )
        .unwrap();
    assert_eq!(
        vec![id1, id2, id3],
        reply
            .ids
            .iter()
            .map(|id| id.id.clone())
            .collect::<Vec<String>>()
    );
}

#[test]
fn test_xdel_ex() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_2);
    let mut con = ctx.connection();
    let _: () = con.flushdb().unwrap();

    let stream_name = "test_stream_xdel_ex";
    let group_name = "test_group";

    let stream_entries = [
        ("field1", "value1"),
        ("field2", "value2"),
        ("field3", "value3"),
        ("field4", "value4"),
        ("field5", "value5"),
        ("field6", "value6"),
    ];
    let non_existent_id = "9999999999-0";

    let first_three_entries: [(&str, &str); 3] = stream_entries[..3].try_into().unwrap();
    let [id1, id2, id3]: [String; 3] =
        first_three_entries.map(|entry| con.xadd(stream_name, "*", &[entry]).unwrap().unwrap());

    // Create a consumer group and read some messages to create PEL entries.
    let _: () = con
        .xgroup_create_mkstream(stream_name, group_name, "0")
        .unwrap();
    // Create 2 consumers within the group and read 1 message with each of them.
    for i in 1..3 {
        let _: Option<StreamReadReply> = con
            .xread_options(
                &[stream_name],
                &[">"],
                &StreamReadOptions::default()
                    .group(group_name, format!("consumer{i}"))
                    .count(1),
            )
            .unwrap();

        // Read the PEL entries.
        let pending = con.xpending(stream_name, group_name).unwrap();
        assert_eq!(pending.count(), i);
        if let StreamPendingReply::Data(data) = pending {
            assert_eq!(data.consumers.len(), i);
            for j in 0..i {
                assert_eq!(data.consumers[j].name, format!("consumer{}", j + 1));
                assert_eq!(data.consumers[j].pending, 1);
            }
        } else {
            panic!("Expected StreamPendingReply::Data");
        }
    }
    // Check the number of entries in the stream.
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, first_three_entries.len());

    // Test XDELEX with ACKED policy
    // Entries are not deleted unless they are acknowledged by all groups.
    let result = con.xdel_ex(stream_name, &[&id1], StreamDeletionPolicy::Acked);
    assert_eq!(
        result,
        Ok(vec![
            XDelExStatusCode::NotDeletedUnacknowledgedOrStillReferenced
        ])
    );
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 2);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 3);
    // After acknowledging a message, deleting it should be possible.
    let _: usize = con.xack(stream_name, group_name, &[&id1]).unwrap();
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 1);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 3);
    let result = con.xdel_ex(stream_name, &[&id1], StreamDeletionPolicy::Acked);
    assert_eq!(result, Ok(vec![XDelExStatusCode::Deleted]));
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 1);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 2);

    // Test XDELEX with KEEPREF policy
    // Should delete entries but preserve references in PEL.
    let result = con.xdel_ex(stream_name, &[&id2], StreamDeletionPolicy::KeepRef);
    assert_eq!(result, Ok(vec![XDelExStatusCode::Deleted]));
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 1);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 1);

    // Test XDELEX with ACKED policy
    // Should fail on an entry that is still referenced by a consumer group, even though it was deleted from the stream.
    let result = con.xdel_ex(stream_name, &[&id2], StreamDeletionPolicy::Acked);
    assert_eq!(
        result,
        Ok(vec![
            XDelExStatusCode::NotDeletedUnacknowledgedOrStillReferenced
        ])
    );
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 1);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 1);
    assert_eq!(info.first_entry.id, id3);

    // Test XDELEX with ACKED policy
    // Entries that were not delivered to any consumer cannot be deleted even though they are not referenced by any consumer group.
    // Such entries can be removed with the DELREF policy.
    let fourth_entry: [(&str, &str); 1] = stream_entries[3..4].try_into().unwrap();
    let id4: String = con.xadd(stream_name, "*", &fourth_entry).unwrap().unwrap();
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 1);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 2);

    let result = con.xdel_ex(stream_name, &[&id4], StreamDeletionPolicy::Acked);
    assert_eq!(
        result,
        Ok(vec![
            XDelExStatusCode::NotDeletedUnacknowledgedOrStillReferenced
        ])
    );
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 1);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 2);

    let result = con.xdel_ex(stream_name, &[&id4], StreamDeletionPolicy::DelRef);
    assert_eq!(result, Ok(vec![XDelExStatusCode::Deleted]));
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 1);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 1);

    // Test XDELEX with DELREF policy
    // Deletes entries and their references in PEL.
    // Bring id3 into the PEL by reading it with a new consumer.
    let _: Option<StreamReadReply> = con
        .xread_options(
            &[stream_name],
            &[">"],
            &StreamReadOptions::default()
                .group(group_name, "consumer3")
                .count(1),
        )
        .unwrap();

    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 2);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 1);
    // Verify that the DELREF policy can delete entries in the PEL even when they are no longer part of the stream.
    // These are dangling references in the PEL.
    let result = con.xdel_ex(stream_name, &[&id2], StreamDeletionPolicy::DelRef);
    assert_eq!(result, Ok(vec![XDelExStatusCode::IdNotFound]));
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 1);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 1);
    // Verify that the DELREF policy deletes entries that exist both in the PEL and in the stream.
    let result = con.xdel_ex(stream_name, &[&id3], StreamDeletionPolicy::DelRef);
    assert_eq!(result, Ok(vec![XDelExStatusCode::Deleted]));
    let pending = con.xpending(stream_name, group_name).unwrap();
    assert_eq!(pending.count(), 0);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 0);

    // Test multiple IDs at once
    let last_two_entries: [(&str, &str); 2] = stream_entries[4..6].try_into().unwrap();
    let [id5, id6]: [String; 2] =
        last_two_entries.map(|entry| con.xadd(stream_name, "*", &[entry]).unwrap().unwrap());

    let result = con.xdel_ex(
        stream_name,
        &[id5.as_str(), id6.as_str(), non_existent_id],
        StreamDeletionPolicy::DelRef,
    );
    assert_eq!(
        result,
        Ok(vec![
            XDelExStatusCode::Deleted,
            XDelExStatusCode::Deleted,
            XDelExStatusCode::IdNotFound,
        ])
    );

    // Test XDELEX with non-existent ID
    let result = con.xdel_ex(
        stream_name,
        &[non_existent_id],
        StreamDeletionPolicy::DelRef,
    );
    assert_eq!(result, Ok(vec![XDelExStatusCode::IdNotFound]));

    // Test with invalid ID format
    let result = con.xdel_ex(stream_name, &["invalid-0"], StreamDeletionPolicy::DelRef);
    assert!(matches!(result, Err(e) if e.to_string().contains("Invalid stream ID")));
}

#[test]
fn test_xack_del() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_2);
    let mut con = ctx.connection();
    let _: () = con.flushdb().unwrap();

    let stream_name = "test_stream_xack_del";
    let first_group_name = "test_group1";
    let second_group_name = "test_group2";

    let stream_entries = [
        ("field1", "value1"),
        ("field2", "value2"),
        ("field3", "value3"),
        ("field4", "value4"),
        ("field5", "value5"),
        ("field6", "value6"),
    ];
    let non_existent_id = "9999999999-0";

    // Create a stream with some entries.
    let first_three_entries: [(&str, &str); 3] = stream_entries[..3].try_into().unwrap();
    let [id1, id2, id3]: [String; 3] =
        first_three_entries.map(|entry| con.xadd(stream_name, "*", &[entry]).unwrap().unwrap());

    // Create a consumer group and an individual consumer for each message,
    // then read the messages to create PEL entries.
    let _: () = con
        .xgroup_create_mkstream(stream_name, first_group_name, "0")
        .unwrap();

    for i in 1..4 {
        let _: Option<StreamReadReply> = con
            .xread_options(
                &[stream_name],
                &[">"],
                &StreamReadOptions::default()
                    .group(first_group_name, format!("consumer{i}"))
                    .count(1),
            )
            .unwrap();

        // Read the PEL entries.
        let pending = con.xpending(stream_name, first_group_name).unwrap();
        assert_eq!(pending.count(), i);
        if let StreamPendingReply::Data(data) = pending {
            assert_eq!(data.consumers.len(), i);
            for j in 0..i {
                assert_eq!(data.consumers[j].name, format!("consumer{}", j + 1));
                assert_eq!(data.consumers[j].pending, 1);
            }
        } else {
            panic!("Expected StreamPendingReply::Data");
        }
    }
    // Check the number of entries in the stream.
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, first_three_entries.len());
    // Check that all of the messages were inserted in the PEL.
    let pending = con.xpending(stream_name, first_group_name).unwrap();
    assert_eq!(pending.count(), first_three_entries.len());

    let mut remaining_entries = first_three_entries.len();

    // Test XACKDEL with all of the policies, when there is just one group
    // As only one group is holding a reference to any of the messages, they should be deletable regardless of the applied policy.
    let ids = [&id1, &id2, &id3];
    let policies = [
        StreamDeletionPolicy::KeepRef,
        StreamDeletionPolicy::DelRef,
        StreamDeletionPolicy::Acked,
    ];

    for (&id, policy) in ids.iter().zip(policies.iter()) {
        let result = con.xack_del(stream_name, first_group_name, &[id], policy.clone());
        assert_eq!(result, Ok(vec![XAckDelStatusCode::AcknowledgedAndDeleted]));
        remaining_entries -= 1;

        let pending = con.xpending(stream_name, first_group_name).unwrap();
        assert_eq!(pending.count(), remaining_entries);
        let info = con.xinfo_stream(stream_name).unwrap();
        assert_eq!(info.length, remaining_entries);
    }

    // Insert a new entry and create a second group so it could be read by both groups.
    let fourth_entry: [(&str, &str); 1] = stream_entries[3..4].try_into().unwrap();
    let id4: String = con.xadd(stream_name, "*", &fourth_entry).unwrap().unwrap();
    let _: () = con
        .xgroup_create_mkstream(stream_name, second_group_name, "0")
        .unwrap();

    // Before reading it with the consumers attempt to acknowledge and delete it.
    // XACKDEL should not be able to find the entry in a PEL and thus should return IdNotFound.
    let result = con.xack_del(
        stream_name,
        first_group_name,
        &[&id4],
        StreamDeletionPolicy::Acked,
    );
    assert_eq!(result, Ok(vec![XAckDelStatusCode::IdNotFound]));

    // Read the new entry with both groups.
    for &group_name in &[first_group_name, second_group_name] {
        let _: Option<StreamReadReply> = con
            .xread_options(
                &[stream_name],
                &[">"],
                &StreamReadOptions::default()
                    .group(group_name, "consumer1")
                    .count(1),
            )
            .unwrap();
    }
    let first_group_pending_messages = con.xpending(stream_name, first_group_name).unwrap();
    let second_group_pending_messages = con.xpending(stream_name, second_group_name).unwrap();
    assert_eq!(first_group_pending_messages.count(), 1);
    assert_eq!(
        first_group_pending_messages.count(),
        second_group_pending_messages.count()
    );
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 1);

    // Test XACKDEL with KeepRef policy
    // Should acknowledge and delete the entry in the PEL of the specified group, but not in the PEL of the other group.
    let result = con.xack_del(
        stream_name,
        first_group_name,
        &[&id4],
        StreamDeletionPolicy::KeepRef,
    );
    assert_eq!(result, Ok(vec![XAckDelStatusCode::AcknowledgedAndDeleted]));
    let first_group_pending_messages = con.xpending(stream_name, first_group_name).unwrap();
    assert_eq!(first_group_pending_messages.count(), 0);
    let second_group_pending_messages = con.xpending(stream_name, second_group_name).unwrap();
    assert_eq!(second_group_pending_messages.count(), 1);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 0);

    // Test XACKDEL with DelRef policy
    // Should acknowledge and delete the entry and its references even if they are dangling ones.
    let result = con.xack_del(
        stream_name,
        second_group_name,
        &[&id4],
        StreamDeletionPolicy::DelRef,
    );
    assert_eq!(result, Ok(vec![XAckDelStatusCode::AcknowledgedAndDeleted]));
    let second_group_pending_messages = con.xpending(stream_name, second_group_name).unwrap();
    assert_eq!(second_group_pending_messages.count(), 0);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 0);

    // Test XACKDEL with DelRef policy
    // Should acknowledge and delete the entries and their references from all PELs.
    let last_two_entries: [(&str, &str); 2] = stream_entries[4..6].try_into().unwrap();
    let [id5, id6]: [String; 2] =
        last_two_entries.map(|entry| con.xadd(stream_name, "*", &[entry]).unwrap().unwrap());

    for &group_name in &[first_group_name, second_group_name] {
        let _: Option<StreamReadReply> = con
            .xread_options(
                &[stream_name],
                &[">"],
                &StreamReadOptions::default()
                    .group(group_name, "consumer1")
                    .count(2),
            )
            .unwrap();
    }

    let first_group_pending_messages = con.xpending(stream_name, first_group_name).unwrap();
    assert_eq!(first_group_pending_messages.count(), 2);
    let second_group_pending_messages = con.xpending(stream_name, second_group_name).unwrap();
    assert_eq!(second_group_pending_messages.count(), 2);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 2);

    let result = con.xack_del(
        stream_name,
        first_group_name,
        &[&id5, &id6],
        StreamDeletionPolicy::DelRef,
    );
    assert_eq!(
        result,
        Ok(vec![
            XAckDelStatusCode::AcknowledgedAndDeleted,
            XAckDelStatusCode::AcknowledgedAndDeleted,
        ])
    );
    let first_group_pending_messages = con.xpending(stream_name, first_group_name).unwrap();
    assert_eq!(first_group_pending_messages.count(), 0);
    let second_group_pending_messages = con.xpending(stream_name, second_group_name).unwrap();
    assert_eq!(second_group_pending_messages.count(), 0);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 0);

    let result = con.xack_del(
        stream_name,
        second_group_name,
        &[&id5, &id6],
        StreamDeletionPolicy::DelRef,
    );
    assert_eq!(
        result,
        Ok(vec![
            XAckDelStatusCode::IdNotFound,
            XAckDelStatusCode::IdNotFound,
        ])
    );

    // Test XACKDEL with ACKED policy
    // Entries are not deleted unless they are acknowledged by all groups.
    let last_two_entries: [(&str, &str); 2] = stream_entries[4..6].try_into().unwrap();
    let [id5, id6]: [String; 2] =
        last_two_entries.map(|entry| con.xadd(stream_name, "*", &[entry]).unwrap().unwrap());

    for &group_name in &[first_group_name, second_group_name] {
        let _: Option<StreamReadReply> = con
            .xread_options(
                &[stream_name],
                &[">"],
                &StreamReadOptions::default()
                    .group(group_name, "consumer1")
                    .count(2),
            )
            .unwrap();
    }
    let first_group_pending_messages = con.xpending(stream_name, first_group_name).unwrap();
    assert_eq!(first_group_pending_messages.count(), 2);
    let second_group_pending_messages = con.xpending(stream_name, second_group_name).unwrap();
    assert_eq!(second_group_pending_messages.count(), 2);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 2);

    // ACKED policy should not delete the entries because they are still referenced by the other group.
    let result = con.xack_del(
        stream_name,
        first_group_name,
        &[&id5, &id6],
        StreamDeletionPolicy::Acked,
    );
    assert_eq!(
        result,
        Ok(vec![
            XAckDelStatusCode::AcknowledgedNotDeletedStillReferenced,
            XAckDelStatusCode::AcknowledgedNotDeletedStillReferenced,
        ])
    );

    let first_group_pending_messages = con.xpending(stream_name, first_group_name).unwrap();
    assert_eq!(first_group_pending_messages.count(), 0);
    let second_group_pending_messages = con.xpending(stream_name, second_group_name).unwrap();
    assert_eq!(second_group_pending_messages.count(), 2);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 2);

    // ACKED policy should delete the entries because they are no longer referenced by any other group.
    let result = con.xack_del(
        stream_name,
        second_group_name,
        &[&id5, &id6],
        StreamDeletionPolicy::Acked,
    );
    assert_eq!(
        result,
        Ok(vec![
            XAckDelStatusCode::AcknowledgedAndDeleted,
            XAckDelStatusCode::AcknowledgedAndDeleted,
        ])
    );
    let first_group_pending_messages = con.xpending(stream_name, first_group_name).unwrap();
    assert_eq!(first_group_pending_messages.count(), 0);
    let second_group_pending_messages = con.xpending(stream_name, second_group_name).unwrap();
    assert_eq!(second_group_pending_messages.count(), 0);
    let info = con.xinfo_stream(stream_name).unwrap();
    assert_eq!(info.length, 0);

    // Test XACKDEL with non-existent ID
    let result = con.xack_del(
        stream_name,
        first_group_name,
        &[non_existent_id],
        StreamDeletionPolicy::DelRef,
    );
    assert_eq!(result, Ok(vec![XAckDelStatusCode::IdNotFound]));

    // Test with invalid ID format
    let result = con.xack_del(
        stream_name,
        first_group_name,
        &["invalid-0"],
        StreamDeletionPolicy::DelRef,
    );
    assert!(matches!(result, Err(e) if e.to_string().contains("Invalid stream ID")));
}

#[test]
fn test_xtrim() {
    // Tests the following commands....
    // xtrim
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // add some keys
    xadd_keyrange(&mut con, "k1", 0, 100);

    // trim key to 50
    // returns the number of items remaining in the stream
    let result = con.xtrim("k1", StreamMaxlen::Equals(50));
    assert_eq!(result, Ok(50));
    // we should end up with 40 after this call
    let result = con.xtrim("k1", StreamMaxlen::Equals(10));
    assert_eq!(result, Ok(40));
}

#[test]
fn test_xtrim_options() {
    // Tests the following commands....
    // xtrim_options
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // add some keys
    xadd_keyrange(&mut con, "k1", 0, 100);

    // trim key to 50
    // returns the number of items deleted from the stream
    let result = con.xtrim_options(
        "k1",
        &StreamTrimOptions::maxlen(StreamTrimmingMode::Exact, 50),
    );
    assert_eq!(result, Ok(50));

    // we should end up with 40 deleted this call
    let result = con.xtrim_options(
        "k1",
        &StreamTrimOptions::maxlen(StreamTrimmingMode::Exact, 10),
    );
    assert_eq!(result, Ok(40));

    let _ = con.del("k1");

    for i in 1..100 {
        let _ = con.xadd("k1", format!("1-{i}"), &[("h", "w")]);
    }

    // Trim to id 1-26
    let result = con.xtrim_options(
        "k1",
        &StreamTrimOptions::minid(StreamTrimmingMode::Exact, "1-26"),
    );
    assert_eq!(result, Ok(25));

    // we should end up with 50 deleted this call
    let result = con.xtrim_options(
        "k1",
        &StreamTrimOptions::minid(StreamTrimmingMode::Exact, "1-76"),
    );
    assert_eq!(result, Ok(50));
}

#[test]
fn test_xgroup() {
    // Tests the following commands....
    // xgroup_create_mkstream
    // xgroup_destroy
    // xgroup_delconsumer

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // test xgroup create w/ mkstream @ 0
    let result = con.xgroup_create_mkstream("k1", "g1", "0");
    assert!(result.is_ok());

    // destroy this new stream group
    let result = con.xgroup_destroy("k1", "g1");
    assert_eq!(result, Ok(true));

    // add some keys
    xadd(&mut con);

    // create the group again using an existing stream
    let result = con.xgroup_create("k1", "g1", "0");
    assert!(result.is_ok());

    // read from the group so we can register the consumer
    let reply = con
        .xread_options(
            &["k1"],
            &[">"],
            &StreamReadOptions::default().group("g1", "c1"),
        )
        .unwrap()
        .unwrap();
    assert_eq!(reply.keys[0].ids.len(), 2);

    let result = con.xgroup_delconsumer("k1", "g1", "c1");
    // returns the number of pending message this client had open
    assert_eq!(result, Ok(2));

    let result = con.xgroup_destroy("k1", "g1");
    assert_eq!(result, Ok(true));
}

#[test]
fn test_xrange() {
    // Tests the following commands....
    // xrange (-/+ variations)
    // xrange_all
    // xrange_count

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    xadd(&mut con);

    // xrange replies
    let reply = con.xrange_all("k1").unwrap();
    assert_eq!(reply.ids.len(), 2);

    let reply = con.xrange("k1", "1000-1", "+").unwrap();
    assert_eq!(reply.ids.len(), 1);

    let reply = con.xrange("k1", "-", "1000-0").unwrap();
    assert_eq!(reply.ids.len(), 1);

    let reply = con.xrange_count("k1", "-", "+", 1).unwrap();
    assert_eq!(reply.ids.len(), 1);
}

#[test]
fn test_xrevrange() {
    // Tests the following commands....
    // xrevrange (+/- variations)
    // xrevrange_all
    // xrevrange_count

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    xadd(&mut con);

    // xrange replies
    let reply = con.xrevrange_all("k1").unwrap();
    assert_eq!(reply.ids.len(), 2);

    let reply = con.xrevrange("k1", "1000-1", "-").unwrap();
    assert_eq!(reply.ids.len(), 2);

    let reply = con.xrevrange("k1", "+", "1000-1").unwrap();
    assert_eq!(reply.ids.len(), 1);

    let reply = con.xrevrange_count("k1", "+", "-", 1).unwrap();
    assert_eq!(reply.ids.len(), 1);
}

#[test]
fn test_xautoclaim_invalid_pel_entries_claiming_full_entries() {
    // The Redis PEL can include stale entries that have been deleted from the stream,
    // due to either data corruption or client error.
    // Redis v6 behaves differently from Redis v7 when encountering these invalid entries.
    // We support v6, so we must ensure that stale entries do not cause a deserialization error.
    // See https://github.com/redis-rs/redis-rs/issues/1798
    // Note that this issue also applies to xclaim.

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // xautoclaim-invalid basic idea:
    // 1. add messages to a group
    // 2. read the messages, but do not xack them
    // 3. delete the messages from the stream
    // 4. call xautoclaim

    let reply = create_group_add_and_read(&mut con);

    // save this StreamId for later
    let claim = &reply.keys[0].ids[0];
    let claim_1 = &reply.keys[0].ids[1];
    let claimed_entries = vec![reply.keys[0].ids[2].clone(), reply.keys[0].ids[3].clone()];

    // delete the messages from the stream
    let _ = con.xdel("k1", &[claim.id.clone(), claim_1.id.clone()]);
    sleep(Duration::from_millis(5));

    let reply = con
        .xautoclaim_options(
            "k1",
            "g1",
            "c2",
            1,
            claim.id.clone(),
            StreamAutoClaimOptions::default().count(4),
        )
        .unwrap();
    assert_eq!(reply.claimed, claimed_entries);

    if ctx.get_version().0 > 6 {
        assert_eq!(
            reply.deleted_ids,
            vec![claim.id.clone(), claim_1.id.clone()]
        );
    } else {
        assert_eq!(reply.deleted_ids.len(), 0);
        assert!(reply.invalid_entries);
    }
}

#[test]
fn test_xautoclaim_invalid_pel_entries_claiming_just_ids() {
    // The Redis PEL can include stale entries that have been deleted from the stream,
    // due to either data corruption or client error.
    // Redis v6 behaves differently from Redis v7 when encountering these invalid entries.
    // We support v6, so we must ensure that stale entries do not cause a deserialization error.
    // See https://github.com/redis-rs/redis-rs/issues/1798
    // Note that this issue also applies to xclaim.

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // xautoclaim-invalid basic idea:
    // 1. add messages to a group
    // 2. read the messages, but do not xack them
    // 3. delete the messages from the stream
    // 4. call xautoclaim

    let reply = create_group_add_and_read(&mut con);

    // save this StreamId for later
    let claim = &reply.keys[0].ids[0];
    let claim_1 = &reply.keys[0].ids[1];
    let mut claimed_entries = vec![reply.keys[0].ids[2].clone(), reply.keys[0].ids[3].clone()];

    // delete the messages from the stream
    let _ = con.xdel("k1", &[claim.id.clone(), claim_1.id.clone()]);
    sleep(Duration::from_millis(5));

    let reply = con
        .xautoclaim_options(
            "k1",
            "g1",
            "c2",
            1,
            claim.id.clone(),
            StreamAutoClaimOptions::default().count(4).with_justid(),
        )
        .unwrap();
    // we expect to receive just IDs, so we remove the maps
    std::mem::take(&mut claimed_entries[0].map);
    std::mem::take(&mut claimed_entries[1].map);

    if ctx.get_version().0 > 6 {
        assert_eq!(reply.claimed, claimed_entries);
        assert_eq!(
            reply.deleted_ids,
            vec![claim.id.clone(), claim_1.id.clone()]
        );
    } else {
        // on redis 6, the deleted entries appear when passing JUSTID
        claimed_entries.insert(
            0,
            StreamId {
                id: claim.id.clone(),
                map: Default::default(),
                milliseconds_elapsed_from_delivery: None,
                delivered_count: None,
            },
        );
        claimed_entries.insert(
            1,
            StreamId {
                id: claim_1.id.clone(),
                map: Default::default(),
                milliseconds_elapsed_from_delivery: None,
                delivered_count: None,
            },
        );
        assert_eq!(reply.claimed, claimed_entries);
        assert_eq!(reply.deleted_ids.len(), 0);
    }
}
