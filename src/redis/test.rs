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
        TestContext {
            server: server,
            client: client,
        }
    }
}


#[test]
fn test_ping() {
    let ctx = TestContext::new();
    let mut con = ctx.client.get_connection().unwrap();
    assert!(con.ping() == true);
}

#[test]
fn test_basics() {
    let ctx = TestContext::new();
    let mut con = ctx.client.get_connection().unwrap();
    con.set("foo", "bar");
    assert!(con.get("foo") == Some(~"bar"));
    con.del("foo");
    assert!(con.get("foo") == None);
}
