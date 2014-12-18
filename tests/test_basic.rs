#![feature(slicing_syntax)]

extern crate redis;
extern crate libc;
extern crate serialize;

use redis::{Commands, PipelineCommands};

use std::io::process;
use std::time::Duration;
use std::sync::Future;
use std::io::timer::sleep;
use std::collections::{HashMap, HashSet};

pub static SERVER_PORT: u16 = 38991;

pub struct RedisServer {
    pub process: process::Process,
}

impl RedisServer {

    pub fn new() -> RedisServer {
        let mut process = process::Command::new("redis-server")
            .arg("-")
            .stdout(process::Ignored)
            .stderr(process::Ignored)
            .spawn().unwrap();
        {
            let mut stdin = process.stdin.take().unwrap();
            stdin.write_str(format!("
                bind 127.0.0.1
                port {port}
            ", port=SERVER_PORT).as_slice()).unwrap();
        }
        RedisServer { process: process }
    }

    pub fn wait(&mut self) {
        self.process.wait().unwrap();
    }

    pub fn foo(&mut self) {
    }
}

impl Drop for RedisServer {

    fn drop(&mut self) {
        let _ = self.process.signal_exit();
    }
}

pub struct TestContext {
    pub server: RedisServer,
    pub client: redis::Client,
}

impl TestContext {

    fn new() -> TestContext {
        let server = RedisServer::new();

        let client = redis::Client::open(redis::ConnectionInfo {
            host: "127.0.0.1".to_string(),
            port: SERVER_PORT,
            db: 0,
            passwd: None,
        }).unwrap();
        let con;

        loop {
            match client.get_connection() {
                Err(err) => {
                    if err.is_connection_refusal() {
                        sleep(Duration::milliseconds(1));
                    } else {
                        panic!("Could not connect: {}", err);
                    }
                },
                Ok(x) => { con = x; break; },
            }
        }
        redis::cmd("FLUSHDB").execute(&con);

        TestContext {
            server: server,
            client: client,
        }
    }

    fn connection(&self) -> redis::Connection {
        self.client.get_connection().unwrap()
    }

    fn pubsub(&self) -> redis::PubSub {
        self.client.get_pubsub().unwrap()
    }
}


#[test]
fn test_args() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SET").arg("key1").arg(b"foo").execute(&con);
    redis::cmd("SET").arg(["key2", "bar"][]).execute(&con);

    assert_eq!(redis::cmd("MGET").arg(["key1", "key2"][]).query(&con),
               Ok(("foo".to_string(), b"bar".to_vec())));
}

#[test]
fn test_getset() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(42i).execute(&con);
    assert_eq!(redis::cmd("GET").arg("foo").query(&con), Ok(42i));

    redis::cmd("SET").arg("bar").arg("foo").execute(&con);
    assert_eq!(redis::cmd("GET").arg("bar").query(&con), Ok(b"foo".to_vec()));
}

#[test]
fn test_incr() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(42i).execute(&con);
    assert_eq!(redis::cmd("INCR").arg("foo").query(&con), Ok(43i));
}

#[test]
fn test_info() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let info : redis::InfoDict = redis::cmd("INFO").query(&con).unwrap();
    assert_eq!(info.find(&"role"), Some(&redis::Value::Status("master".to_string())));
    assert_eq!(info.get("role"), Some("master".to_string()));
    assert_eq!(info.get("loading"), Some(false));
    assert!(info.len() > 0);
    assert!(info.contains_key(&"role"));
}

#[test]
fn test_hash_ops() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("HSET").arg("foo").arg("key_1").arg(1i).execute(&con);
    redis::cmd("HSET").arg("foo").arg("key_2").arg(2i).execute(&con);

    let h : HashMap<String, i32> = redis::cmd("HGETALL").arg("foo").query(&con).unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));
}

#[test]
fn test_set_ops() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SADD").arg("foo").arg(1i).execute(&con);
    redis::cmd("SADD").arg("foo").arg(2i).execute(&con);
    redis::cmd("SADD").arg("foo").arg(3i).execute(&con);

    let mut s : Vec<i32> = redis::cmd("SMEMBERS").arg("foo").query(&con).unwrap();
    s.sort();
    assert_eq!(s.len(), 3);
    assert_eq!(s[], [1, 2, 3][]);

    let set : HashSet<i32> = redis::cmd("SMEMBERS").arg("foo").query(&con).unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));
}

#[test]
fn test_scan() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SADD").arg("foo").arg(1i).execute(&con);
    redis::cmd("SADD").arg("foo").arg(2i).execute(&con);
    redis::cmd("SADD").arg("foo").arg(3i).execute(&con);

    let (cur, mut s) : (i32, Vec<i32>) = redis::cmd("SSCAN").arg("foo").arg(0i).query(&con).unwrap();
    s.sort();
    assert_eq!(cur, 0i32);
    assert_eq!(s.len(), 3);
    assert_eq!(s[], [1, 2, 3][]);
}

#[test]
fn test_optionals() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(1i).execute(&con);

    let (a, b) : (Option<i32>, Option<i32>) = redis::cmd("MGET")
        .arg("foo").arg("missing").query(&con).unwrap();
    assert_eq!(a, Some(1i32));
    assert_eq!(b, None);

    let a = redis::cmd("GET").arg("missing").query(&con).unwrap_or(0i32);
    assert_eq!(a, 0i32);
}

#[test]
fn test_json() {
    use serialize::json::Json;

    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SET").arg("foo").arg("[1, 2, 3]").execute(&con);

    let json : Json = redis::cmd("GET").arg("foo").query(&con).unwrap();
    assert_eq!(json, Json::Array(vec![
        Json::U64(1),
        Json::U64(2),
        Json::U64(3),
    ]));
}

#[test]
fn test_scanning() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let mut unseen = HashSet::new();

    for x in range(0u, 1000u) {
        redis::cmd("SADD").arg("foo").arg(x).execute(&con);
        unseen.insert(x);
    }

    let mut iter = redis::cmd("SSCAN").arg("foo").cursor_arg(0).iter(&con).unwrap();

    for x in iter {
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
}

#[test]
fn test_filtered_scanning() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let mut unseen = HashSet::new();

    for x in range(0u, 3000u) {
        let _ : () = con.hset("foo", format!("key_{}_{}", x % 100, x), x).unwrap();
        if x % 100 == 0 {
            unseen.insert(x);
        }
    }

    let mut iter = con.hscan_match("foo", "key_0_*").unwrap();

    for x in iter {
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
}

#[test]
fn test_pipeline() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let ((k1, k2),) : ((i32, i32),) = redis::pipe()
        .cmd("SET").arg("key_1").arg(42i).ignore()
        .cmd("SET").arg("key_2").arg(43i).ignore()
        .cmd("MGET").arg(["key_1", "key_2"][]).query(&con).unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

#[test]
fn test_empty_pipeline() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let _ : () = redis::pipe()
        .cmd("PING").ignore()
        .query(&con).unwrap();

    let _ : () = redis::pipe().query(&con).unwrap();
}

#[test]
fn test_pipeline_transaction() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let ((k1, k2),) : ((i32, i32),) = redis::pipe()
        .atomic()
        .cmd("SET").arg("key_1").arg(42i).ignore()
        .cmd("SET").arg("key_2").arg(43i).ignore()
        .cmd("MGET").arg(["key_1", "key_2"][]).query(&con).unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

#[test]
fn test_real_transaction() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let key = "the_key";
    let _ : () = redis::cmd("SET").arg(key).arg(42i).query(&con).unwrap();

    loop {
        let _ : () = redis::cmd("WATCH").arg(key).query(&con).unwrap();
        let val : int = redis::cmd("GET").arg(key).query(&con).unwrap();
        let response : Option<(int,)> = redis::pipe()
            .atomic()
            .cmd("SET").arg(key).arg(val + 1).ignore()
            .cmd("GET").arg(key)
            .query(&con).unwrap();

        match response {
            None => { continue; }
            Some(response) => {
                assert_eq!(response, (43i,));
                break;
            }
        }
    }
}

#[test]
fn test_real_transaction_highlevel() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let key = "the_key";
    let _ : () = redis::cmd("SET").arg(key).arg(42i).query(&con).unwrap();

    let response : (int,) = redis::transaction(&con, [key][], |pipe| {
        let val : int = try!(redis::cmd("GET").arg(key).query(&con));
        pipe
            .cmd("SET").arg(key).arg(val + 1).ignore()
            .cmd("GET").arg(key).query(&con)
    }).unwrap();

    assert_eq!(response, (43i,));
}

#[test]
fn test_pubsub() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let mut pubsub = ctx.pubsub();
    pubsub.subscribe("foo").unwrap();

    let mut rv = Future::spawn(move || {
        sleep(Duration::milliseconds(100));

        let msg = pubsub.get_message().unwrap();
        assert_eq!(msg.get_channel(), Ok("foo".to_string()));
        assert_eq!(msg.get_payload(), Ok(42i));

        let msg = pubsub.get_message().unwrap();
        assert_eq!(msg.get_channel(), Ok("foo".to_string()));
        assert_eq!(msg.get_payload(), Ok(23i));

        true
    });

    redis::cmd("PUBLISH").arg("foo").arg(42i).execute(&con);
    redis::cmd("PUBLISH").arg("foo").arg(23i).execute(&con);
    assert_eq!(rv.get(), true);
}

#[test]
fn test_script() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let script = redis::Script::new(r"
       return {redis.call('GET', KEYS[1]), ARGV[1]}
    ");

    let _ : () = redis::cmd("SET").arg("my_key").arg("foo").query(&con).unwrap();
    let response = script.key("my_key").arg(42i).invoke(&con);

    assert_eq!(response, Ok(("foo".to_string(), 42i)));
}

#[test]
fn test_tuple_args() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("HMSET").arg("my_key").arg([
        ("field_1", 42i),
        ("field_2", 23i),
    ][]).execute(&con);

    assert_eq!(redis::cmd("HGET").arg("my_key").arg("field_1").query(&con), Ok(42i));
    assert_eq!(redis::cmd("HGET").arg("my_key").arg("field_2").query(&con), Ok(23i));
}

#[test]
fn test_nice_api() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.set("my_key", 42i), Ok(()));
    assert_eq!(con.get("my_key"), Ok(42i));

    let (k1, k2) : (i32, i32) = redis::pipe()
        .atomic()
        .set("key_1", 42i).ignore()
        .set("key_2", 43i).ignore()
        .get("key_1")
        .get("key_2").query(&con).unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

#[test]
fn test_auto_m_versions() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.set_multiple([("key1", 1i), ("key2", 2i)][]), Ok(()));
    assert_eq!(con.get(["key1", "key2"][]), Ok((1i, 2i)));
}

#[test]
fn test_nice_hash_api() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.hset_multiple("my_hash", [
        ("f1", 1i),
        ("f2", 2i),
        ("f3", 4i),
        ("f4", 8i),
    ][]), Ok(()));

    let hm : HashMap<String, int> = con.hgetall("my_hash").unwrap();
    assert_eq!(hm.get("f1"), Some(&1i));
    assert_eq!(hm.get("f2"), Some(&2i));
    assert_eq!(hm.get("f3"), Some(&4i));
    assert_eq!(hm.get("f4"), Some(&8i));
    assert_eq!(hm.len(), 4);

    let v : Vec<(String, int)> = con.hgetall("my_hash").unwrap();
    assert_eq!(v, vec![
        ("f1".to_string(), 1i),
        ("f2".to_string(), 2i),
        ("f3".to_string(), 4i),
        ("f4".to_string(), 8i),
    ]);

    assert_eq!(con.hget("my_hash", ["f2", "f4"][]), Ok((2i, 8i)));
    assert_eq!(con.hincr("my_hash", "f1", 1i), Ok((2i)));
    assert_eq!(con.hincr("my_hash", "f2", 1.5f32), Ok((3.5f32)));
    assert_eq!(con.hexists("my_hash", "f2"), Ok(true));
    assert_eq!(con.hdel("my_hash", ["f1", "f2"][]), Ok(()));
    assert_eq!(con.hexists("my_hash", "f2"), Ok(false));

    let mut iter : redis::Iter<(String, int)> = con.hscan("my_hash").unwrap();
    let mut found = HashSet::new();
    for item in iter {
        found.insert(item);
    }

    assert_eq!(found.len(), 2);
    assert_eq!(found.contains(&("f3".to_string(), 4i)), true);
    assert_eq!(found.contains(&("f4".to_string(), 8i)), true);
}

#[test]
fn test_nice_list_api() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.rpush("my_list", [1i, 2i, 3i, 4i][]), Ok(4i));
    assert_eq!(con.rpush("my_list", [5i, 6i, 7i, 8i][]), Ok(8i));
    assert_eq!(con.llen("my_list"), Ok(8i));

    assert_eq!(con.lpop("my_list"), Ok(1i));
    assert_eq!(con.llen("my_list"), Ok(7i));

    assert_eq!(con.lrange("my_list", 0, 2), Ok((2i, 3i, 4i)));
}
