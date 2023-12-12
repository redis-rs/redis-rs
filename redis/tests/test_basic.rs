#![allow(clippy::let_unit_value)]

use redis::{
    Commands, ConnectionInfo, ConnectionLike, ControlFlow, ErrorKind, ExistenceCheck, Expiry,
    PubSubCommands, RedisResult, SetExpiry, SetOptions, ToRedisArgs,
};

use std::collections::{BTreeMap, BTreeSet};
use std::collections::{HashMap, HashSet};
use std::thread::{sleep, spawn};
use std::time::Duration;
use std::vec;

use crate::support::*;

mod support;

#[test]
fn test_parse_redis_url() {
    let redis_url = "redis://127.0.0.1:1234/0".to_string();
    redis::parse_redis_url(&redis_url).unwrap();
    redis::parse_redis_url("unix:/var/run/redis/redis.sock").unwrap();
    assert!(redis::parse_redis_url("127.0.0.1").is_none());
}

#[test]
fn test_redis_url_fromstr() {
    let _info: ConnectionInfo = "redis://127.0.0.1:1234/0".parse().unwrap();
}

#[test]
fn test_args() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SET").arg("key1").arg(b"foo").execute(&mut con);
    redis::cmd("SET").arg(&["key2", "bar"]).execute(&mut con);

    assert_eq!(
        redis::cmd("MGET").arg(&["key1", "key2"]).query(&mut con),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

#[test]
fn test_getset() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
    assert_eq!(redis::cmd("GET").arg("foo").query(&mut con), Ok(42));

    redis::cmd("SET").arg("bar").arg("foo").execute(&mut con);
    assert_eq!(
        redis::cmd("GET").arg("bar").query(&mut con),
        Ok(b"foo".to_vec())
    );
}

//unit test for key_type function
#[test]
fn test_key_type() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    //The key is a simple value
    redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
    let string_key_type: String = con.key_type("foo").unwrap();
    assert_eq!(string_key_type, "string");

    //The key is a list
    redis::cmd("LPUSH")
        .arg("list_bar")
        .arg("foo")
        .execute(&mut con);
    let list_key_type: String = con.key_type("list_bar").unwrap();
    assert_eq!(list_key_type, "list");

    //The key is a set
    redis::cmd("SADD")
        .arg("set_bar")
        .arg("foo")
        .execute(&mut con);
    let set_key_type: String = con.key_type("set_bar").unwrap();
    assert_eq!(set_key_type, "set");

    //The key is a sorted set
    redis::cmd("ZADD")
        .arg("sorted_set_bar")
        .arg("1")
        .arg("foo")
        .execute(&mut con);
    let zset_key_type: String = con.key_type("sorted_set_bar").unwrap();
    assert_eq!(zset_key_type, "zset");

    //The key is a hash
    redis::cmd("HSET")
        .arg("hset_bar")
        .arg("hset_key_1")
        .arg("foo")
        .execute(&mut con);
    let hash_key_type: String = con.key_type("hset_bar").unwrap();
    assert_eq!(hash_key_type, "hash");
}

#[test]
fn test_incr() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
    assert_eq!(redis::cmd("INCR").arg("foo").query(&mut con), Ok(43usize));
}

#[test]
fn test_getdel() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(42).execute(&mut con);

    assert_eq!(con.get_del("foo"), Ok(42usize));

    assert_eq!(
        redis::cmd("GET").arg("foo").query(&mut con),
        Ok(None::<usize>)
    );
}

#[test]
fn test_getex() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(42usize).execute(&mut con);

    // Return of get_ex must match set value
    let ret_value = con.get_ex::<_, usize>("foo", Expiry::EX(1)).unwrap();
    assert_eq!(ret_value, 42usize);

    // Get before expiry time must also return value
    sleep(Duration::from_millis(100));
    let delayed_get = con.get::<_, usize>("foo").unwrap();
    assert_eq!(delayed_get, 42usize);

    // Get after expiry time mustn't return value
    sleep(Duration::from_secs(1));
    let after_expire_get = con.get::<_, Option<usize>>("foo").unwrap();
    assert_eq!(after_expire_get, None);

    // Persist option test prep
    redis::cmd("SET").arg("foo").arg(420usize).execute(&mut con);

    // Return of get_ex with persist option must match set value
    let ret_value = con.get_ex::<_, usize>("foo", Expiry::PERSIST).unwrap();
    assert_eq!(ret_value, 420usize);

    // Get after persist get_ex must return value
    sleep(Duration::from_millis(200));
    let delayed_get = con.get::<_, usize>("foo").unwrap();
    assert_eq!(delayed_get, 420usize);
}

#[test]
fn test_info() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let info: redis::InfoDict = redis::cmd("INFO").query(&mut con).unwrap();
    assert_eq!(
        info.find(&"role"),
        Some(&redis::Value::Status("master".to_string()))
    );
    assert_eq!(info.get("role"), Some("master".to_string()));
    assert_eq!(info.get("loading"), Some(false));
    assert!(!info.is_empty());
    assert!(info.contains_key(&"role"));
}

#[test]
fn test_hash_ops() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("HSET")
        .arg("foo")
        .arg("key_1")
        .arg(1)
        .execute(&mut con);
    redis::cmd("HSET")
        .arg("foo")
        .arg("key_2")
        .arg(2)
        .execute(&mut con);

    let h: HashMap<String, i32> = redis::cmd("HGETALL").arg("foo").query(&mut con).unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));

    let h: BTreeMap<String, i32> = redis::cmd("HGETALL").arg("foo").query(&mut con).unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));
}

// Requires redis-server >= 4.0.0.
// Not supported with the current appveyor/windows binary deployed.
#[cfg(not(target_os = "windows"))]
#[test]
fn test_unlink() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
    assert_eq!(redis::cmd("GET").arg("foo").query(&mut con), Ok(42));
    assert_eq!(con.unlink("foo"), Ok(1));

    redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
    redis::cmd("SET").arg("bar").arg(42).execute(&mut con);
    assert_eq!(con.unlink(&["foo", "bar"]), Ok(2));
}

#[test]
fn test_set_ops() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.sadd("foo", &[1, 2, 3]), Ok(3));

    let mut s: Vec<i32> = con.smembers("foo").unwrap();
    s.sort_unstable();
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);

    let set: HashSet<i32> = con.smembers("foo").unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));

    let set: BTreeSet<i32> = con.smembers("foo").unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));
}

#[test]
fn test_scan() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.sadd("foo", &[1, 2, 3]), Ok(3));

    let (cur, mut s): (i32, Vec<i32>) = redis::cmd("SSCAN")
        .arg("foo")
        .arg(0)
        .query(&mut con)
        .unwrap();
    s.sort_unstable();
    assert_eq!(cur, 0i32);
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);
}

#[test]
fn test_optionals() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(1).execute(&mut con);

    let (a, b): (Option<i32>, Option<i32>) = redis::cmd("MGET")
        .arg("foo")
        .arg("missing")
        .query(&mut con)
        .unwrap();
    assert_eq!(a, Some(1i32));
    assert_eq!(b, None);

    let a = redis::cmd("GET")
        .arg("missing")
        .query(&mut con)
        .unwrap_or(0i32);
    assert_eq!(a, 0i32);
}

#[test]
fn test_scanning() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let mut unseen = HashSet::new();

    for x in 0..1000 {
        redis::cmd("SADD").arg("foo").arg(x).execute(&mut con);
        unseen.insert(x);
    }

    let iter = redis::cmd("SSCAN")
        .arg("foo")
        .cursor_arg(0)
        .clone()
        .iter(&mut con)
        .unwrap();

    for x in iter {
        // type inference limitations
        let x: usize = x;
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
}

#[test]
fn test_filtered_scanning() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let mut unseen = HashSet::new();

    for x in 0..3000 {
        let _: () = con
            .hset("foo", format!("key_{}_{}", x % 100, x), x)
            .unwrap();
        if x % 100 == 0 {
            unseen.insert(x);
        }
    }

    let iter = con
        .hscan_match::<&str, &str, (String, usize)>("foo", "key_0_*")
        .unwrap();

    for (_field, value) in iter {
        unseen.remove(&value);
    }

    assert_eq!(unseen.len(), 0);
}

#[test]
fn test_pipeline() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let ((k1, k2),): ((i32, i32),) = redis::pipe()
        .cmd("SET")
        .arg("key_1")
        .arg(42)
        .ignore()
        .cmd("SET")
        .arg("key_2")
        .arg(43)
        .ignore()
        .cmd("MGET")
        .arg(&["key_1", "key_2"])
        .query(&mut con)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

#[test]
fn test_pipeline_with_err() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let _: () = redis::cmd("SET")
        .arg("x")
        .arg("x-value")
        .query(&mut con)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("y")
        .arg("y-value")
        .query(&mut con)
        .unwrap();

    let _: () = redis::cmd("SLAVEOF")
        .arg("1.1.1.1")
        .arg("99")
        .query(&mut con)
        .unwrap();

    let res = redis::pipe()
        .set("x", "another-x-value")
        .ignore()
        .get("y")
        .query::<()>(&mut con);
    assert!(res.is_err() && res.unwrap_err().kind() == ErrorKind::ReadOnly);

    // Make sure we don't get leftover responses from the pipeline ("y-value"). See #436.
    let res = redis::cmd("GET")
        .arg("x")
        .query::<String>(&mut con)
        .unwrap();
    assert_eq!(res, "x-value");
}

#[test]
fn test_empty_pipeline() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let _: () = redis::pipe().cmd("PING").ignore().query(&mut con).unwrap();

    let _: () = redis::pipe().query(&mut con).unwrap();
}

#[test]
fn test_pipeline_transaction() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let ((k1, k2),): ((i32, i32),) = redis::pipe()
        .atomic()
        .cmd("SET")
        .arg("key_1")
        .arg(42)
        .ignore()
        .cmd("SET")
        .arg("key_2")
        .arg(43)
        .ignore()
        .cmd("MGET")
        .arg(&["key_1", "key_2"])
        .query(&mut con)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

#[test]
fn test_pipeline_transaction_with_errors() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let _: () = con.set("x", 42).unwrap();

    // Make Redis a replica of a nonexistent master, thereby making it read-only.
    let _: () = redis::cmd("slaveof")
        .arg("1.1.1.1")
        .arg("1")
        .query(&mut con)
        .unwrap();

    // Ensure that a write command fails with a READONLY error
    let err: RedisResult<()> = redis::pipe()
        .atomic()
        .set("x", 142)
        .ignore()
        .get("x")
        .query(&mut con);

    assert_eq!(err.unwrap_err().kind(), ErrorKind::ReadOnly);

    let x: i32 = con.get("x").unwrap();
    assert_eq!(x, 42);
}

#[test]
fn test_pipeline_reuse_query() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let mut pl = redis::pipe();

    let ((k1,),): ((i32,),) = pl
        .cmd("SET")
        .arg("pkey_1")
        .arg(42)
        .ignore()
        .cmd("MGET")
        .arg(&["pkey_1"])
        .query(&mut con)
        .unwrap();

    assert_eq!(k1, 42);

    redis::cmd("DEL").arg("pkey_1").execute(&mut con);

    // The internal commands vector of the pipeline still contains the previous commands.
    let ((k1,), (k2, k3)): ((i32,), (i32, i32)) = pl
        .cmd("SET")
        .arg("pkey_2")
        .arg(43)
        .ignore()
        .cmd("MGET")
        .arg(&["pkey_1"])
        .arg(&["pkey_2"])
        .query(&mut con)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 42);
    assert_eq!(k3, 43);
}

#[test]
fn test_pipeline_reuse_query_clear() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let mut pl = redis::pipe();

    let ((k1,),): ((i32,),) = pl
        .cmd("SET")
        .arg("pkey_1")
        .arg(44)
        .ignore()
        .cmd("MGET")
        .arg(&["pkey_1"])
        .query(&mut con)
        .unwrap();
    pl.clear();

    assert_eq!(k1, 44);

    redis::cmd("DEL").arg("pkey_1").execute(&mut con);

    let ((k1, k2),): ((bool, i32),) = pl
        .cmd("SET")
        .arg("pkey_2")
        .arg(45)
        .ignore()
        .cmd("MGET")
        .arg(&["pkey_1"])
        .arg(&["pkey_2"])
        .query(&mut con)
        .unwrap();
    pl.clear();

    assert!(!k1);
    assert_eq!(k2, 45);
}

#[test]
fn test_real_transaction() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let key = "the_key";
    let _: () = redis::cmd("SET").arg(key).arg(42).query(&mut con).unwrap();

    loop {
        let _: () = redis::cmd("WATCH").arg(key).query(&mut con).unwrap();
        let val: isize = redis::cmd("GET").arg(key).query(&mut con).unwrap();
        let response: Option<(isize,)> = redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(key)
            .arg(val + 1)
            .ignore()
            .cmd("GET")
            .arg(key)
            .query(&mut con)
            .unwrap();

        match response {
            None => {
                continue;
            }
            Some(response) => {
                assert_eq!(response, (43,));
                break;
            }
        }
    }
}

#[test]
fn test_real_transaction_highlevel() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let key = "the_key";
    let _: () = redis::cmd("SET").arg(key).arg(42).query(&mut con).unwrap();

    let response: (isize,) = redis::transaction(&mut con, &[key], |con, pipe| {
        let val: isize = redis::cmd("GET").arg(key).query(con)?;
        pipe.cmd("SET")
            .arg(key)
            .arg(val + 1)
            .ignore()
            .cmd("GET")
            .arg(key)
            .query(con)
    })
    .unwrap();

    assert_eq!(response, (43,));
}

#[test]
fn test_pubsub() {
    use std::sync::{Arc, Barrier};
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // Connection for subscriber api
    let mut pubsub_con = ctx.connection();

    // Barrier is used to make test thread wait to publish
    // until after the pubsub thread has subscribed.
    let barrier = Arc::new(Barrier::new(2));
    let pubsub_barrier = barrier.clone();

    let thread = spawn(move || {
        let mut pubsub = pubsub_con.as_pubsub();
        pubsub.subscribe("foo").unwrap();

        let _ = pubsub_barrier.wait();

        let msg = pubsub.get_message().unwrap();
        assert_eq!(msg.get_channel(), Ok("foo".to_string()));
        assert_eq!(msg.get_payload(), Ok(42));

        let msg = pubsub.get_message().unwrap();
        assert_eq!(msg.get_channel(), Ok("foo".to_string()));
        assert_eq!(msg.get_payload(), Ok(23));
    });

    let _ = barrier.wait();
    redis::cmd("PUBLISH").arg("foo").arg(42).execute(&mut con);
    // We can also call the command directly
    assert_eq!(con.publish("foo", 23), Ok(1));

    thread.join().expect("Something went wrong");
}

#[test]
fn test_pubsub_unsubscribe() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    {
        let mut pubsub = con.as_pubsub();
        pubsub.subscribe("foo").unwrap();
        pubsub.subscribe("bar").unwrap();
        pubsub.subscribe("baz").unwrap();
        pubsub.psubscribe("foo*").unwrap();
        pubsub.psubscribe("bar*").unwrap();
        pubsub.psubscribe("baz*").unwrap();
    }

    // Connection should be usable again for non-pubsub commands
    let _: redis::Value = con.set("foo", "bar").unwrap();
    let value: String = con.get("foo").unwrap();
    assert_eq!(&value[..], "bar");
}

#[test]
fn test_pubsub_subscribe_while_messages_are_sent() {
    let ctx = TestContext::new();
    let mut conn_external = ctx.connection();
    let mut conn_internal = ctx.connection();
    let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    // receive message from foo channel
    let thread = std::thread::spawn(move || {
        let mut pubsub = conn_internal.as_pubsub();
        pubsub.subscribe("foo").unwrap();
        sender.send(()).unwrap();
        loop {
            let msg = pubsub.get_message().unwrap();
            let channel = msg.get_channel_name();
            let content: i32 = msg.get_payload().unwrap();
            received
                .lock()
                .unwrap()
                .push(format!("{channel}:{content}"));
            if content == -1 {
                return;
            }
            if content == 5 {
                // subscribe bar channel using the same pubsub
                pubsub.subscribe("bar").unwrap();
                sender.send(()).unwrap();
            }
        }
    });
    receiver.recv().unwrap();

    // send message to foo channel after channel is ready.
    for index in 0..10 {
        println!("publishing on foo {index}");
        redis::cmd("PUBLISH")
            .arg("foo")
            .arg(index)
            .query::<i32>(&mut conn_external)
            .unwrap();
    }
    receiver.recv().unwrap();
    redis::cmd("PUBLISH")
        .arg("bar")
        .arg(-1)
        .query::<i32>(&mut conn_external)
        .unwrap();
    thread.join().unwrap();
    assert_eq!(
        *received_clone.lock().unwrap(),
        (0..10)
            .map(|index| format!("foo:{}", index))
            .chain(std::iter::once("bar:-1".to_string()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_pubsub_unsubscribe_no_subs() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    {
        let _pubsub = con.as_pubsub();
    }

    // Connection should be usable again for non-pubsub commands
    let _: redis::Value = con.set("foo", "bar").unwrap();
    let value: String = con.get("foo").unwrap();
    assert_eq!(&value[..], "bar");
}

#[test]
fn test_pubsub_unsubscribe_one_sub() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    {
        let mut pubsub = con.as_pubsub();
        pubsub.subscribe("foo").unwrap();
    }

    // Connection should be usable again for non-pubsub commands
    let _: redis::Value = con.set("foo", "bar").unwrap();
    let value: String = con.get("foo").unwrap();
    assert_eq!(&value[..], "bar");
}

#[test]
fn test_pubsub_unsubscribe_one_sub_one_psub() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    {
        let mut pubsub = con.as_pubsub();
        pubsub.subscribe("foo").unwrap();
        pubsub.psubscribe("foo*").unwrap();
    }

    // Connection should be usable again for non-pubsub commands
    let _: redis::Value = con.set("foo", "bar").unwrap();
    let value: String = con.get("foo").unwrap();
    assert_eq!(&value[..], "bar");
}

#[test]
fn scoped_pubsub() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // Connection for subscriber api
    let mut pubsub_con = ctx.connection();

    let thread = spawn(move || {
        let mut count = 0;
        pubsub_con
            .subscribe(&["foo", "bar"], |msg| {
                count += 1;
                match count {
                    1 => {
                        assert_eq!(msg.get_channel(), Ok("foo".to_string()));
                        assert_eq!(msg.get_payload(), Ok(42));
                        ControlFlow::Continue
                    }
                    2 => {
                        assert_eq!(msg.get_channel(), Ok("bar".to_string()));
                        assert_eq!(msg.get_payload(), Ok(23));
                        ControlFlow::Break(())
                    }
                    _ => ControlFlow::Break(()),
                }
            })
            .unwrap();

        pubsub_con
    });

    // Can't use a barrier in this case since there's no opportunity to run code
    // between channel subscription and blocking for messages.
    sleep(Duration::from_millis(100));

    redis::cmd("PUBLISH").arg("foo").arg(42).execute(&mut con);
    assert_eq!(con.publish("bar", 23), Ok(1));

    // Wait for thread
    let mut pubsub_con = thread.join().expect("pubsub thread terminates ok");

    // Connection should be usable again for non-pubsub commands
    let _: redis::Value = pubsub_con.set("foo", "bar").unwrap();
    let value: String = pubsub_con.get("foo").unwrap();
    assert_eq!(&value[..], "bar");
}

#[test]
#[cfg(feature = "script")]
fn test_script() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let script = redis::Script::new(
        r"
       return {redis.call('GET', KEYS[1]), ARGV[1]}
    ",
    );

    let _: () = redis::cmd("SET")
        .arg("my_key")
        .arg("foo")
        .query(&mut con)
        .unwrap();
    let response = script.key("my_key").arg(42).invoke(&mut con);

    assert_eq!(response, Ok(("foo".to_string(), 42)));
}

#[test]
#[cfg(feature = "script")]
fn test_script_load() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let script = redis::Script::new("return 'Hello World'");

    let hash = script.prepare_invoke().load(&mut con);

    assert_eq!(hash, Ok(script.get_hash().to_string()));
}

#[test]
fn test_tuple_args() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("HMSET")
        .arg("my_key")
        .arg(&[("field_1", 42), ("field_2", 23)])
        .execute(&mut con);

    assert_eq!(
        redis::cmd("HGET")
            .arg("my_key")
            .arg("field_1")
            .query(&mut con),
        Ok(42)
    );
    assert_eq!(
        redis::cmd("HGET")
            .arg("my_key")
            .arg("field_2")
            .query(&mut con),
        Ok(23)
    );
}

#[test]
fn test_nice_api() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.set("my_key", 42), Ok(()));
    assert_eq!(con.get("my_key"), Ok(42));

    let (k1, k2): (i32, i32) = redis::pipe()
        .atomic()
        .set("key_1", 42)
        .ignore()
        .set("key_2", 43)
        .ignore()
        .get("key_1")
        .get("key_2")
        .query(&mut con)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

#[test]
fn test_auto_m_versions() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.mset(&[("key1", 1), ("key2", 2)]), Ok(()));
    assert_eq!(con.get(&["key1", "key2"]), Ok((1, 2)));
    assert_eq!(con.get(vec!["key1", "key2"]), Ok((1, 2)));
    assert_eq!(con.get(&vec!["key1", "key2"]), Ok((1, 2)));
}

#[test]
fn test_nice_hash_api() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(
        con.hset_multiple("my_hash", &[("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8)]),
        Ok(())
    );

    let hm: HashMap<String, isize> = con.hgetall("my_hash").unwrap();
    assert_eq!(hm.get("f1"), Some(&1));
    assert_eq!(hm.get("f2"), Some(&2));
    assert_eq!(hm.get("f3"), Some(&4));
    assert_eq!(hm.get("f4"), Some(&8));
    assert_eq!(hm.len(), 4);

    let hm: BTreeMap<String, isize> = con.hgetall("my_hash").unwrap();
    assert_eq!(hm.get("f1"), Some(&1));
    assert_eq!(hm.get("f2"), Some(&2));
    assert_eq!(hm.get("f3"), Some(&4));
    assert_eq!(hm.get("f4"), Some(&8));
    assert_eq!(hm.len(), 4);

    let v: Vec<(String, isize)> = con.hgetall("my_hash").unwrap();
    assert_eq!(
        v,
        vec![
            ("f1".to_string(), 1),
            ("f2".to_string(), 2),
            ("f3".to_string(), 4),
            ("f4".to_string(), 8),
        ]
    );

    assert_eq!(con.hget("my_hash", &["f2", "f4"]), Ok((2, 8)));
    assert_eq!(con.hincr("my_hash", "f1", 1), Ok(2));
    assert_eq!(con.hincr("my_hash", "f2", 1.5f32), Ok(3.5f32));
    assert_eq!(con.hexists("my_hash", "f2"), Ok(true));
    assert_eq!(con.hdel("my_hash", &["f1", "f2"]), Ok(()));
    assert_eq!(con.hexists("my_hash", "f2"), Ok(false));

    let iter: redis::Iter<'_, (String, isize)> = con.hscan("my_hash").unwrap();
    let mut found = HashSet::new();
    for item in iter {
        found.insert(item);
    }

    assert_eq!(found.len(), 2);
    assert!(found.contains(&("f3".to_string(), 4)));
    assert!(found.contains(&("f4".to_string(), 8)));
}

#[test]
fn test_nice_list_api() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.rpush("my_list", &[1, 2, 3, 4]), Ok(4));
    assert_eq!(con.rpush("my_list", &[5, 6, 7, 8]), Ok(8));
    assert_eq!(con.llen("my_list"), Ok(8));

    assert_eq!(con.lpop("my_list", Default::default()), Ok(1));
    assert_eq!(con.llen("my_list"), Ok(7));

    assert_eq!(con.lrange("my_list", 0, 2), Ok((2, 3, 4)));

    assert_eq!(con.lset("my_list", 0, 4), Ok(true));
    assert_eq!(con.lrange("my_list", 0, 2), Ok((4, 3, 4)));

    #[cfg(not(windows))]
    //Windows version of redis is limited to v3.x
    {
        let my_list: Vec<u8> = con.lrange("my_list", 0, 10).expect("To get range");
        assert_eq!(
            con.lpop("my_list", core::num::NonZeroUsize::new(10)),
            Ok(my_list)
        );
    }
}

#[test]
fn test_tuple_decoding_regression() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.del("my_zset"), Ok(()));
    assert_eq!(con.zadd("my_zset", "one", 1), Ok(1));
    assert_eq!(con.zadd("my_zset", "two", 2), Ok(1));

    let vec: Vec<(String, u32)> = con.zrangebyscore_withscores("my_zset", 0, 10).unwrap();
    assert_eq!(vec.len(), 2);

    assert_eq!(con.del("my_zset"), Ok(1));

    let vec: Vec<(String, u32)> = con.zrangebyscore_withscores("my_zset", 0, 10).unwrap();
    assert_eq!(vec.len(), 0);
}

#[test]
fn test_bit_operations() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.setbit("bitvec", 10, true), Ok(false));
    assert_eq!(con.getbit("bitvec", 10), Ok(true));
}

#[test]
fn test_redis_server_down() {
    let mut ctx = TestContext::new();
    let mut con = ctx.connection();

    let ping = redis::cmd("PING").query::<String>(&mut con);
    assert_eq!(ping, Ok("PONG".into()));

    ctx.stop_server();

    let ping = redis::cmd("PING").query::<String>(&mut con);

    assert!(ping.is_err());
    eprintln!("{}", ping.unwrap_err());
    assert!(!con.is_open());
}

#[test]
fn test_zinterstore_weights() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let _: () = con
        .zadd_multiple("zset1", &[(1, "one"), (2, "two"), (4, "four")])
        .unwrap();
    let _: () = con
        .zadd_multiple("zset2", &[(1, "one"), (2, "two"), (3, "three")])
        .unwrap();

    // zinterstore_weights
    assert_eq!(
        con.zinterstore_weights("out", &[("zset1", 2), ("zset2", 3)]),
        Ok(2)
    );

    assert_eq!(
        con.zrange_withscores("out", 0, -1),
        Ok(vec![
            ("one".to_string(), "5".to_string()),
            ("two".to_string(), "10".to_string())
        ])
    );

    // zinterstore_min_weights
    assert_eq!(
        con.zinterstore_min_weights("out", &[("zset1", 2), ("zset2", 3)]),
        Ok(2)
    );

    assert_eq!(
        con.zrange_withscores("out", 0, -1),
        Ok(vec![
            ("one".to_string(), "2".to_string()),
            ("two".to_string(), "4".to_string()),
        ])
    );

    // zinterstore_max_weights
    assert_eq!(
        con.zinterstore_max_weights("out", &[("zset1", 2), ("zset2", 3)]),
        Ok(2)
    );

    assert_eq!(
        con.zrange_withscores("out", 0, -1),
        Ok(vec![
            ("one".to_string(), "3".to_string()),
            ("two".to_string(), "6".to_string()),
        ])
    );
}

#[test]
fn test_zunionstore_weights() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let _: () = con
        .zadd_multiple("zset1", &[(1, "one"), (2, "two")])
        .unwrap();
    let _: () = con
        .zadd_multiple("zset2", &[(1, "one"), (2, "two"), (3, "three")])
        .unwrap();

    // zunionstore_weights
    assert_eq!(
        con.zunionstore_weights("out", &[("zset1", 2), ("zset2", 3)]),
        Ok(3)
    );

    assert_eq!(
        con.zrange_withscores("out", 0, -1),
        Ok(vec![
            ("one".to_string(), "5".to_string()),
            ("three".to_string(), "9".to_string()),
            ("two".to_string(), "10".to_string())
        ])
    );

    // zunionstore_min_weights
    assert_eq!(
        con.zunionstore_min_weights("out", &[("zset1", 2), ("zset2", 3)]),
        Ok(3)
    );

    assert_eq!(
        con.zrange_withscores("out", 0, -1),
        Ok(vec![
            ("one".to_string(), "2".to_string()),
            ("two".to_string(), "4".to_string()),
            ("three".to_string(), "9".to_string())
        ])
    );

    // zunionstore_max_weights
    assert_eq!(
        con.zunionstore_max_weights("out", &[("zset1", 2), ("zset2", 3)]),
        Ok(3)
    );

    assert_eq!(
        con.zrange_withscores("out", 0, -1),
        Ok(vec![
            ("one".to_string(), "3".to_string()),
            ("two".to_string(), "6".to_string()),
            ("three".to_string(), "9".to_string())
        ])
    );
}

#[test]
fn test_zrembylex() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let setname = "myzset";
    assert_eq!(
        con.zadd_multiple(
            setname,
            &[
                (0, "apple"),
                (0, "banana"),
                (0, "carrot"),
                (0, "durian"),
                (0, "eggplant"),
                (0, "grapes"),
            ],
        ),
        Ok(6)
    );

    // Will remove "banana", "carrot", "durian" and "eggplant"
    let num_removed: u32 = con.zrembylex(setname, "[banana", "[eggplant").unwrap();
    assert_eq!(4, num_removed);

    let remaining: Vec<String> = con.zrange(setname, 0, -1).unwrap();
    assert_eq!(remaining, vec!["apple".to_string(), "grapes".to_string()]);
}

// Requires redis-server >= 6.2.0.
// Not supported with the current appveyor/windows binary deployed.
#[cfg(not(target_os = "windows"))]
#[test]
fn test_zrandmember() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let setname = "myzrandset";
    let () = con.zadd(setname, "one", 1).unwrap();

    let result: String = con.zrandmember(setname, None).unwrap();
    assert_eq!(result, "one".to_string());

    let result: Vec<String> = con.zrandmember(setname, Some(1)).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], "one".to_string());

    let result: Vec<String> = con.zrandmember(setname, Some(2)).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], "one".to_string());

    assert_eq!(
        con.zadd_multiple(
            setname,
            &[(2, "two"), (3, "three"), (4, "four"), (5, "five")]
        ),
        Ok(4)
    );

    let results: Vec<String> = con.zrandmember(setname, Some(5)).unwrap();
    assert_eq!(results.len(), 5);

    let results: Vec<String> = con.zrandmember(setname, Some(-5)).unwrap();
    assert_eq!(results.len(), 5);

    let results: Vec<String> = con.zrandmember_withscores(setname, 5).unwrap();
    assert_eq!(results.len(), 10);

    let results: Vec<String> = con.zrandmember_withscores(setname, -5).unwrap();
    assert_eq!(results.len(), 10);
}

#[test]
fn test_sismember() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let setname = "myset";
    assert_eq!(con.sadd(setname, &["a"]), Ok(1));

    let result: bool = con.sismember(setname, &["a"]).unwrap();
    assert!(result);

    let result: bool = con.sismember(setname, &["b"]).unwrap();
    assert!(!result);
}

// Requires redis-server >= 6.2.0.
// Not supported with the current appveyor/windows binary deployed.
#[cfg(not(target_os = "windows"))]
#[test]
fn test_smismember() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let setname = "myset";
    assert_eq!(con.sadd(setname, &["a", "b", "c"]), Ok(3));
    let results: Vec<bool> = con.smismember(setname, &["0", "a", "b", "c", "x"]).unwrap();
    assert_eq!(results, vec![false, true, true, true, false]);
}

#[test]
fn test_object_commands() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let _: () = con.set("object_key_str", "object_value_str").unwrap();
    let _: () = con.set("object_key_int", 42).unwrap();

    assert_eq!(
        con.object_encoding::<_, String>("object_key_str").unwrap(),
        "embstr"
    );

    assert_eq!(
        con.object_encoding::<_, String>("object_key_int").unwrap(),
        "int"
    );

    assert!(con.object_idletime::<_, i32>("object_key_str").unwrap() <= 1);
    assert_eq!(con.object_refcount::<_, i32>("object_key_str").unwrap(), 1);

    // Needed for OBJECT FREQ and can't be set before object_idletime
    // since that will break getting the idletime before idletime adjuts
    redis::cmd("CONFIG")
        .arg("SET")
        .arg(b"maxmemory-policy")
        .arg("allkeys-lfu")
        .execute(&mut con);

    let _: () = con.get("object_key_str").unwrap();
    // since maxmemory-policy changed, freq should reset to 1 since we only called
    // get after that
    assert_eq!(con.object_freq::<_, i32>("object_key_str").unwrap(), 1);
}

#[test]
fn test_mget() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let _: () = con.set(1, "1").unwrap();
    let data: Vec<String> = con.mget(&[1]).unwrap();
    assert_eq!(data, vec!["1"]);

    let _: () = con.set(2, "2").unwrap();
    let data: Vec<String> = con.mget(&[1, 2]).unwrap();
    assert_eq!(data, vec!["1", "2"]);

    let data: Vec<Option<String>> = con.mget(&[4]).unwrap();
    assert_eq!(data, vec![None]);

    let data: Vec<Option<String>> = con.mget(&[2, 4]).unwrap();
    assert_eq!(data, vec![Some("2".to_string()), None]);
}

#[test]
fn test_variable_length_get() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let _: () = con.set(1, "1").unwrap();
    let keys = vec![1];
    assert_eq!(keys.len(), 1);
    let data: Vec<String> = con.get(&keys).unwrap();
    assert_eq!(data, vec!["1"]);
}

#[test]
fn test_multi_generics() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.sadd(b"set1", vec![5, 42]), Ok(2));
    assert_eq!(con.sadd(999_i64, vec![42, 123]), Ok(2));
    let _: () = con.rename(999_i64, b"set2").unwrap();
    assert_eq!(con.sunionstore("res", &[b"set1", b"set2"]), Ok(3));
}

#[test]
fn test_set_options_with_get() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let opts = SetOptions::default().get(true);
    let data: Option<String> = con.set_options(1, "1", opts).unwrap();
    assert_eq!(data, None);

    let opts = SetOptions::default().get(true);
    let data: Option<String> = con.set_options(1, "1", opts).unwrap();
    assert_eq!(data, Some("1".to_string()));
}

#[test]
fn test_set_options_options() {
    let empty = SetOptions::default();
    assert_eq!(ToRedisArgs::to_redis_args(&empty).len(), 0);

    let opts = SetOptions::default()
        .conditional_set(ExistenceCheck::NX)
        .get(true)
        .with_expiration(SetExpiry::PX(1000));

    assert_args!(&opts, "NX", "GET", "PX", "1000");

    let opts = SetOptions::default()
        .conditional_set(ExistenceCheck::XX)
        .get(true)
        .with_expiration(SetExpiry::PX(1000));

    assert_args!(&opts, "XX", "GET", "PX", "1000");

    let opts = SetOptions::default()
        .conditional_set(ExistenceCheck::XX)
        .with_expiration(SetExpiry::KEEPTTL);

    assert_args!(&opts, "XX", "KEEPTTL");

    let opts = SetOptions::default()
        .conditional_set(ExistenceCheck::XX)
        .with_expiration(SetExpiry::EXAT(100));

    assert_args!(&opts, "XX", "EXAT", "100");

    let opts = SetOptions::default().with_expiration(SetExpiry::EX(1000));

    assert_args!(&opts, "EX", "1000");
}

#[test]
fn test_blocking_sorted_set_api() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // setup version & input data followed by assertions that take into account Redis version
    // BZPOPMIN & BZPOPMAX are available from Redis version 5.0.0
    // BZMPOP is available from Redis version 7.0.0

    let redis_version = ctx.get_version();
    assert!(redis_version.0 >= 5);

    assert_eq!(con.zadd("a", "1a", 1), Ok(()));
    assert_eq!(con.zadd("b", "2b", 2), Ok(()));
    assert_eq!(con.zadd("c", "3c", 3), Ok(()));
    assert_eq!(con.zadd("d", "4d", 4), Ok(()));
    assert_eq!(con.zadd("a", "5a", 5), Ok(()));
    assert_eq!(con.zadd("b", "6b", 6), Ok(()));
    assert_eq!(con.zadd("c", "7c", 7), Ok(()));
    assert_eq!(con.zadd("d", "8d", 8), Ok(()));

    let min = con.bzpopmin::<&str, (String, String, String)>("b", 0.0);
    let max = con.bzpopmax::<&str, (String, String, String)>("b", 0.0);

    assert_eq!(
        min.unwrap(),
        (String::from("b"), String::from("2b"), String::from("2"))
    );
    assert_eq!(
        max.unwrap(),
        (String::from("b"), String::from("6b"), String::from("6"))
    );

    if redis_version.0 >= 7 {
        let min = con.bzmpop_min::<&str, (String, Vec<Vec<(String, String)>>)>(
            0.0,
            vec!["a", "b", "c", "d"].as_slice(),
            1,
        );
        let max = con.bzmpop_max::<&str, (String, Vec<Vec<(String, String)>>)>(
            0.0,
            vec!["a", "b", "c", "d"].as_slice(),
            1,
        );

        assert_eq!(
            min.unwrap().1[0][0],
            (String::from("1a"), String::from("1"))
        );
        assert_eq!(
            max.unwrap().1[0][0],
            (String::from("5a"), String::from("5"))
        );
    }
}
