#![allow(dead_code)]

use std::{
    env, fs,
    io::{self, Write},
    path::PathBuf,
    process,
    thread::sleep,
    time::Duration,
};

use futures::Future;
use redis::Value;

pub fn current_thread_runtime() -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new();

    #[cfg(feature = "aio")]
    builder.enable_io();

    builder.basic_scheduler().build().unwrap()
}

pub fn block_on_all<F>(f: F) -> F::Output
where
    F: Future,
{
    current_thread_runtime().block_on(f)
}
#[cfg(feature = "async-std-comp")]
pub fn block_on_all_using_async_std<F>(f: F) -> F::Output
where
    F: Future,
{
    async_std::task::block_on(f)
}

#[cfg(feature = "cluster")]
mod cluster;

#[cfg(feature = "cluster")]
pub use self::cluster::*;

#[derive(PartialEq)]
enum ServerType {
    Tcp { tls: bool },
    Unix,
}

pub struct RedisServer {
    pub process: process::Child,
    stunnel_process: Option<process::Child>,
    tempdir: Option<tempdir::TempDir>,
    addr: redis::ConnectionAddr,
}

impl ServerType {
    fn get_intended() -> ServerType {
        match env::var("REDISRS_SERVER_TYPE")
            .ok()
            .as_ref()
            .map(|x| &x[..])
        {
            Some("tcp") => ServerType::Tcp { tls: false },
            Some("tcp+tls") => ServerType::Tcp { tls: true },
            Some("unix") => ServerType::Unix,
            val => {
                panic!("Unknown server type {:?}", val);
            }
        }
    }
}

impl RedisServer {
    pub fn new() -> RedisServer {
        let server_type = ServerType::get_intended();
        let addr = match server_type {
            ServerType::Tcp { tls } => {
                // this is technically a race but we can't do better with
                // the tools that redis gives us :(
                let listener = net2::TcpBuilder::new_v4()
                    .unwrap()
                    .reuse_address(true)
                    .unwrap()
                    .bind("127.0.0.1:0")
                    .unwrap()
                    .listen(1)
                    .unwrap();
                let redis_port = listener.local_addr().unwrap().port();
                if tls {
                    redis::ConnectionAddr::TcpTls {
                        host: "127.0.0.1".to_string(),
                        port: redis_port,
                        insecure: true,
                    }
                } else {
                    redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), redis_port)
                }
            }
            ServerType::Unix => {
                let (a, b) = rand::random::<(u64, u64)>();
                let path = format!("/tmp/redis-rs-test-{}-{}.sock", a, b);
                redis::ConnectionAddr::Unix(PathBuf::from(&path))
            }
        };
        RedisServer::new_with_addr(addr, |cmd| cmd.spawn().unwrap())
    }

    pub fn new_with_addr<F: FnOnce(&mut process::Command) -> process::Child>(
        addr: redis::ConnectionAddr,
        spawner: F,
    ) -> RedisServer {
        let mut redis_cmd = process::Command::new("redis-server");
        redis_cmd
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null());
        let tempdir = tempdir::TempDir::new("redis").expect("failed to create tempdir");
        match addr {
            redis::ConnectionAddr::Tcp(ref bind, server_port) => {
                redis_cmd
                    .arg("--port")
                    .arg(server_port.to_string())
                    .arg("--bind")
                    .arg(bind);

                RedisServer {
                    process: spawner(&mut redis_cmd),
                    stunnel_process: None,
                    tempdir: None,
                    addr,
                }
            }
            redis::ConnectionAddr::TcpTls { ref host, port, .. } => {
                // prepare redis with unix socket
                redis_cmd
                    .arg("--port")
                    .arg("0")
                    .arg("--unixsocket")
                    .arg(tempdir.path().join("redis.sock"));

                // create a self-signed TLS server cert
                let tls_key_path = tempdir.path().join("key.pem");
                let tls_cert_path = tempdir.path().join("cert.crt");
                process::Command::new("openssl")
                    .arg("req")
                    .arg("-nodes")
                    .arg("-new")
                    .arg("-x509")
                    .arg("-keyout")
                    .arg(&tls_key_path)
                    .arg("-out")
                    .arg(&tls_cert_path)
                    .arg("-subj")
                    .arg("/C=XX/ST=crates/L=redis-rs/O=testing/CN=localhost")
                    .stdout(process::Stdio::null())
                    .stderr(process::Stdio::null())
                    .spawn()
                    .expect("failed to spawn openssl")
                    .wait()
                    .expect("failed to create self-signed TLS certificate");

                let stunnel_config_path = tempdir.path().join("stunnel.conf");
                let mut stunnel_config_file = fs::File::create(&stunnel_config_path).unwrap();
                stunnel_config_file
                    .write_all(
                        format!(
                            r#"
                            pid = {tempdir}/stunnel.pid
                            cert = {tempdir}/cert.crt
                            key = {tempdir}/key.pem
                            verify = 0
                            foreground = yes
                            [redis]
                            accept = {host}:{stunnel_port}
                            connect = {tempdir}/redis.sock
                            "#,
                            tempdir = tempdir.path().display(),
                            host = host,
                            stunnel_port = port,
                        )
                        .as_bytes(),
                    )
                    .expect("could not write stunnel config file");

                let addr = redis::ConnectionAddr::TcpTls {
                    host: "127.0.0.1".to_string(),
                    port,
                    insecure: true,
                };
                let mut stunnel_cmd = process::Command::new("stunnel");
                stunnel_cmd
                    .stdout(process::Stdio::null())
                    .stderr(process::Stdio::null())
                    .arg(&stunnel_config_path);

                RedisServer {
                    process: spawner(&mut redis_cmd),
                    stunnel_process: Some(stunnel_cmd.spawn().expect("could not start stunnel")),
                    tempdir: Some(tempdir),
                    addr,
                }
            }
            redis::ConnectionAddr::Unix(ref path) => {
                redis_cmd
                    .arg("--port")
                    .arg("0")
                    .arg("--unixsocket")
                    .arg(&path);
                RedisServer {
                    process: spawner(&mut redis_cmd),
                    stunnel_process: None,
                    tempdir: Some(tempdir),
                    addr,
                }
            }
        }
    }

    pub fn wait(&mut self) {
        self.process.wait().unwrap();
        if let Some(p) = self.stunnel_process.as_mut() {
            p.wait().unwrap();
        };
    }

    pub fn get_client_addr(&self) -> &redis::ConnectionAddr {
        &self.addr
    }

    pub fn stop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let Some(p) = self.stunnel_process.as_mut() {
            let _ = p.kill();
            let _ = p.wait();
        }
        if let redis::ConnectionAddr::Unix(ref path) = *self.get_client_addr() {
            fs::remove_file(&path).ok();
        }
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        self.stop()
    }
}

pub struct TestContext {
    pub server: RedisServer,
    pub client: redis::Client,
}

impl TestContext {
    pub fn new() -> TestContext {
        let server = RedisServer::new();

        let client = redis::Client::open(redis::ConnectionInfo {
            addr: Box::new(server.get_client_addr().clone()),
            db: 0,
            username: None,
            passwd: None,
        })
        .unwrap();
        let mut con;

        let millisecond = Duration::from_millis(1);
        let mut retries = 0;
        loop {
            match client.get_connection() {
                Err(err) => {
                    if err.is_connection_refusal() {
                        sleep(millisecond);
                        retries += 1;
                        if retries > 100000 {
                            panic!("Tried to connect too many times, last error: {}", err);
                        }
                    } else {
                        panic!("Could not connect: {}", err);
                    }
                }
                Ok(x) => {
                    con = x;
                    break;
                }
            }
        }
        redis::cmd("FLUSHDB").execute(&mut con);

        TestContext { server, client }
    }

    pub fn connection(&self) -> redis::Connection {
        self.client.get_connection().unwrap()
    }

    #[cfg(feature = "aio")]
    pub async fn async_connection(&self) -> redis::RedisResult<redis::aio::Connection> {
        self.client.get_async_connection().await
    }

    #[cfg(feature = "async-std-comp")]
    pub async fn async_connection_async_std(&self) -> redis::RedisResult<redis::aio::Connection> {
        self.client.get_async_std_connection().await
    }

    pub fn stop_server(&mut self) {
        self.server.stop();
    }

    #[cfg(feature = "tokio-rt-core")]
    pub fn multiplexed_async_connection(
        &self,
    ) -> impl Future<Output = redis::RedisResult<redis::aio::MultiplexedConnection>> {
        self.multiplexed_async_connection_tokio()
    }

    #[cfg(feature = "tokio-rt-core")]
    pub fn multiplexed_async_connection_tokio(
        &self,
    ) -> impl Future<Output = redis::RedisResult<redis::aio::MultiplexedConnection>> {
        let client = self.client.clone();
        async move { client.get_multiplexed_tokio_connection().await }
    }
    #[cfg(feature = "async-std-comp")]
    pub fn multiplexed_async_connection_async_std(
        &self,
    ) -> impl Future<Output = redis::RedisResult<redis::aio::MultiplexedConnection>> {
        let client = self.client.clone();
        async move { client.get_multiplexed_async_std_connection().await }
    }
}

pub fn encode_value<W>(value: &Value, writer: &mut W) -> io::Result<()>
where
    W: io::Write,
{
    #![allow(clippy::write_with_newline)]
    match *value {
        Value::Nil => write!(writer, "$-1\r\n"),
        Value::Int(val) => write!(writer, ":{}\r\n", val),
        Value::Data(ref val) => {
            write!(writer, "${}\r\n", val.len())?;
            writer.write_all(val)?;
            writer.write_all(b"\r\n")
        }
        Value::Bulk(ref values) => {
            write!(writer, "*{}\r\n", values.len())?;
            for val in values.iter() {
                encode_value(val, writer)?;
            }
            Ok(())
        }
        Value::Okay => write!(writer, "+OK\r\n"),
        Value::Status(ref s) => write!(writer, "+{}\r\n", s),
    }
}
