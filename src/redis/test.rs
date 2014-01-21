extern mod redis;

use std::io::process;
use std::io::io_error;
use std::libc::SIGTERM;

pub static SERVER_PORT: int = 38991;

struct RedisServer {
    priv process: process::Process,
}

impl RedisServer {

    fn new() -> RedisServer {
        let mut process = process::Process::new(process::ProcessConfig {
            program: "redis-server",
            args: [~"-"],
            env: None,
            cwd: None,
            io: [process::CreatePipe(true, false),
                 process::Ignored,
                 process::Ignored],
        }).unwrap();
        let input = format!("
            port {port}
        ", port=SERVER_PORT);
        process.io[0].get_mut_ref().write(input.as_bytes());
        process.io[0] = None;
        RedisServer { process: process }
    }
}

impl Drop for RedisServer {

    fn drop(&mut self) {
        io_error::cond.trap(|_e| {}).inside(|| {
            self.process.signal(SIGTERM as int);
        });
        let rv = self.process.wait();
        assert!(rv.success());
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
            match redis::Client::open(url) {
                Ok(x) => { client = x; break; }
                Err(redis::ConnectionRefused) => { std::io::timer::sleep(10); }
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
    assert!(*info.find(&~"tcp_port").unwrap() == SERVER_PORT.to_str());
}

#[test]
fn test_basics() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    con.set("foo", "bar");
    assert!(con.get("foo") == Some(~"bar"));
    con.del("foo");
    assert!(con.get("foo") == None);
}

#[test]
fn test_types() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    con.set("foo", 42);
    con.set("bar", "test");
    assert!(con.get_type("foo") == redis::StringType);
    assert!(con.get_type("bar") == redis::StringType);
    assert!(con.get("foo") == Some(~"42"));
    assert!(con.get_as::<int>("foo") == Some(42));
    assert!(con.get("bar") == Some(~"test"));
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
}

#[test]
fn test_blpop() {
    let ctx = TestContext::new();
    let client = ctx.client;
    let (port, chan) = Chan::new();

    do spawn {
        let mut con = client.get_connection().unwrap();
        let rv = con.blpop(["q"], 5.0);
        assert!(rv == Some((~"q", ~"awesome")));
        chan.send(());
    }

    do spawn {
        let mut con = client.get_connection().unwrap();
        con.rpush("q", "awesome");
    }

    port.recv();
}

#[test]
fn test_scan() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    for x in range(0, 100) {
        con.set(format!("key:{}", x), x);
    }

    let mut found = [false, .. 100];
    for item in con.scan("key:*") {
        let mut iter = item.split(':');
        if iter.next().is_none() {
            continue;
        }
        match iter.next() {
            Some(value) => {
                found[from_str::<uint>(value).unwrap()] = true;
            }
            None => {},
        }
    }

    for x in range(0, 100) {
        assert!(found[x] == true);
    }
}
