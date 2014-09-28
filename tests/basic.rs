extern crate redis;
extern crate libc;

use std::io::process;
use std::io::{IoError, ConnectionRefused};
use std::time::Duration;
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
                Err(IoError { kind: ConnectionRefused, .. }) => {
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
