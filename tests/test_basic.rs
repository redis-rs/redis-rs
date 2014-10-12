#![feature(slicing_syntax)]

extern crate redis;
extern crate libc;
extern crate serialize;

use redis::{Commands, PipelineCommands};

use std::io::process;
use std::io::{IoError, ConnectionRefused};
use std::time::Duration;
use std::sync::Future;
use std::io::timer::sleep;
use std::collections::{HashMap, HashSet};

pub static SERVER_PORT: int = 38991;

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
        let url = format!("redis://127.0.0.1:{port}/0", port=SERVER_PORT);
        let server = RedisServer::new();

        let client = redis::Client::open(url.as_slice()).unwrap();
        let con;

        loop {
            match client.get_connection() {
                Err(redis::Error {
                    kind: redis::InternalIoError(IoError {
                        kind: ConnectionRefused, .. }), ..}) => {
                    sleep(Duration::milliseconds(1));
                },
                Err(err) => { fail!("Could not connect: {}", err); }
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
    assert_eq!(info.find(&"role"), Some(&redis::Status("master".to_string())));
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
    assert_eq!(h.find_equiv(&"key_1"), Some(&1i32));
    assert_eq!(h.find_equiv(&"key_2"), Some(&2i32));
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
    use serialize::json::{Json, List, U64};

    let ctx = TestContext::new();
    let con = ctx.connection();

    redis::cmd("SET").arg("foo").arg("[1, 2, 3]").execute(&con);

    let json : Json = redis::cmd("GET").arg("foo").query(&con).unwrap();
    assert_eq!(json, List(vec![
        U64(1),
        U64(2),
        U64(3),
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

    let mut cmd = redis::cmd("SSCAN");
    let mut iter = cmd.arg("foo").cursor_arg(0).iter(&con).unwrap();

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

    let response : (int,) = con.transaction([key][], |pipe| {
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

    let mut rv = Future::spawn(proc() {
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
