extern crate redis;
extern crate libc;

use std::io::process;
use libc::SIGTERM;

pub static SERVER_PORT: int = 38991;

struct RedisServer {
    process: process::Process,
}

impl RedisServer {

    fn new() -> RedisServer {
        let mut process = process::Command::new("redis-server")
                                           .arg("-")
                                           .stdout(process::Ignored)
                                           .stderr(process::Ignored)
                                           .spawn().unwrap();
        let input = format!("
            bind 127.0.0.1
            port {port}
        ", port=SERVER_PORT);
        let _ = process.stdin.get_mut_ref().write(input.as_bytes());
        process.stdin = None;
        RedisServer { process: process }
    }
}

impl Drop for RedisServer {

    fn drop(&mut self) {
        let _ = self.process.signal(SIGTERM as int);
        let rv = self.process.wait();
        assert!(rv.is_ok());
    }
}

struct TestContext {
    server: RedisServer,
    client: redis::Client,
}

impl TestContext {

    fn new() -> TestContext {
        let url = format!("redis://127.0.0.1:{port}/0", port=SERVER_PORT);
        let server = RedisServer::new();

        let mut client;
        loop {
            match redis::Client::open(url.as_slice()) {
                Ok(x) => { client = x; break; }
                Err(redis::ConnectionRefused) => { std::io::timer::sleep(1); }
                _ => { fail!("Error on connect"); }
            }
        }

        client.get_connection().unwrap().flushdb();
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
fn test_ping() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    assert!(con.ping() == true);
}

#[test]
fn test_info() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let info = con.info();
    assert!(*info.find(&String::from_str("tcp_port")).unwrap() == SERVER_PORT.to_string());
}

#[test]
fn test_basics() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    con.set("foo", "bar");
    assert!(con.get("foo") == Some(String::from_str("bar")));
    con.rename("foo", "bar");
    assert!(con.get("foo") == None);
    assert!(con.get("bar") == Some(String::from_str("bar")));
    con.del("bar");
    assert!(con.get("bar") == None);
}

#[test]
fn test_types() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    con.set("foo", 42u32);
    con.set("bar", "test");
    assert!(con.get_type("foo") == redis::StringType);
    assert!(con.get_type("bar") == redis::StringType);
    assert!(con.get("foo") == Some(String::from_str("42")));
    assert!(con.get_as::<int>("foo") == Some(42));
    assert!(con.get("bar") == Some(String::from_str("test")));
    assert!(con.exists("foo") == true);
    assert!(con.exists("invalid") == false);
}

#[test]
fn test_script() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let script = redis::Script::new("
        return tonumber(ARGV[1]) + 1;
    ");
    assert!(con.call_script(&script, [], [redis::StrArg("42")]) == redis::Int(43));

    con.flush_script_cache();
    assert!(con.call_script(&script, [], [redis::StrArg("42")]) == redis::Int(43));
}

#[test]
fn test_blpop() {
    let ctx = TestContext::new();
    let client = ctx.client;
    let (chan, port) = channel();

    spawn(proc() {
        let mut con = client.get_connection().unwrap();
        let rv = con.blpop(["q"], 5.0);
        assert!(rv == Some((String::from_str("q"), String::from_str("awesome"))));
        chan.send(());
    });

    spawn(proc() {
        let mut con = client.get_connection().unwrap();
        con.rpush("q", "awesome");
    });

    port.recv();
}

#[test]
fn test_scan() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    for x in range(0u, 1000) {
        con.set(format!("key:{}", x).as_slice(), x);
    }

    let mut found = [false, .. 1000];
    for item in con.scan("key:*") {
        let num = item.as_slice().split(':').nth(1).unwrap();
        found[from_str::<uint>(num).unwrap()] = true;
    }

    for x in range(0u, 100) {
        assert!(found[x] == true);
    }
}
