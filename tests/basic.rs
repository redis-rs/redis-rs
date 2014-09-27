extern crate redis;
extern crate libc;

use std::io::process;
use std::io::{IoError, ConnectionRefused};
use std::time::Duration;
use std::io::timer::sleep;

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
        let mut con;

        loop {
            match client.get_connection() {
                Err(IoError { kind: ConnectionRefused, .. }) => {
                    sleep(Duration::milliseconds(1));
                },
                Err(err) => { fail!("Could not connect: {}", err); }
                Ok(x) => { con = x; break; },
            }
        }
        con.execute_raw("FLUSHDB", []).unwrap();

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
    con.execute_raw("PING", []).unwrap();
}

#[test]
fn test_getset() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    con.execute_raw("SET", [redis::StrArg("foo"), redis::StrArg("42")]).unwrap();
    assert_eq!(con.execute_raw("GET", [redis::StrArg("foo")]), Ok(redis::Data(b"42".to_vec())));
}

#[test]
fn test_incr() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    con.execute_raw("SET", [redis::StrArg("foo"), redis::StrArg("42")]).unwrap();

    assert_eq!(con.execute_raw("INCR", [redis::StrArg("foo")]), Ok(redis::Int(43)));

    let v: i32 = con.execute("INCR", [redis::StrArg("foo")]).unwrap();
    assert_eq!(v, 44);
}

#[test]
fn test_new_api() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let _: () = redis::cmd("SET").arg("foo").arg(42i).execute(&mut con).unwrap();
    let v: i32 = redis::cmd("INCR").arg("foo").execute(&mut con).unwrap();
    assert_eq!(v, 43);
}
