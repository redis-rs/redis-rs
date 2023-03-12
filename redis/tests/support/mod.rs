#![allow(dead_code)]

#[cfg(feature = "tls")]
use std::fs::read;

use std::{
    env, fs, io, net::SocketAddr, net::TcpListener, path::PathBuf, process, thread::sleep,
    time::Duration,
};

use futures::Future;
#[cfg(feature = "tls")]
use redis::ConnectionAddr;

use redis::Value;
use socket2::{Domain, Socket, Type};
use tempfile::TempDir;

#[cfg(feature = "tls")]
use redis::tls::{Certificate, RedisIdentity};

pub fn current_thread_runtime() -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new_current_thread();

    #[cfg(feature = "aio")]
    builder.enable_io();

    builder.enable_time();

    builder.build().unwrap()
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

#[cfg(any(feature = "cluster", feature = "cluster-async"))]
mod cluster;

#[cfg(any(feature = "cluster", feature = "cluster-async"))]
pub use self::cluster::*;

#[derive(PartialEq)]
enum ServerType {
    Tcp { tls: bool, sec: bool },
    Unix,
}

pub enum Module {
    Json,
}

pub struct RedisServer {
    pub process: process::Child,
    tempdir: Option<tempfile::TempDir>,
    addr: redis::ConnectionAddr,
    tls_paths: Option<TlsFilePaths>,
}

impl ServerType {
    fn get_intended() -> ServerType {
        match env::var("REDISRS_SERVER_TYPE")
            .ok()
            .as_ref()
            .map(|x| &x[..])
        {
            Some("tcp") => ServerType::Tcp {
                tls: false,
                sec: false,
            },
            Some("tcp+tls") => ServerType::Tcp {
                tls: true,
                sec: false,
            },
            Some("tcp+tls+sec") => ServerType::Tcp {
                tls: true,
                sec: true,
            },
            Some("unix") => ServerType::Unix,
            val => {
                panic!("Unknown server type {val:?}");
            }
        }
    }
}

impl RedisServer {
    pub fn new() -> RedisServer {
        RedisServer::with_modules(&[])
    }

    pub fn with_modules(modules: &[Module]) -> RedisServer {
        let server_type = ServerType::get_intended();
        let addr = match server_type {
            ServerType::Tcp { tls, sec } => {
                // this is technically a race but we can't do better with
                // the tools that redis gives us :(
                let addr = &"127.0.0.1:0".parse::<SocketAddr>().unwrap().into();
                let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
                socket.set_reuse_address(true).unwrap();
                socket.bind(addr).unwrap();
                socket.listen(1).unwrap();
                let listener = TcpListener::from(socket);
                let redis_port = listener.local_addr().unwrap().port();
                if tls {
                    redis::ConnectionAddr::TcpTls {
                        host: "localhost".to_string(),
                        port: redis_port,
                        insecure: !sec,
                        #[cfg(feature = "tls")]
                        ca_cert: None,
                        #[cfg(feature = "tls")]
                        identity: None,
                    }
                } else {
                    redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), redis_port)
                }
            }
            ServerType::Unix => {
                let (a, b) = rand::random::<(u64, u64)>();
                let path = format!("/tmp/redis-rs-test-{a}-{b}.sock");
                redis::ConnectionAddr::Unix(PathBuf::from(&path))
            }
        };
        RedisServer::new_with_addr(addr, None, modules, |cmd| {
            cmd.spawn()
                .unwrap_or_else(|err| panic!("Failed to run {cmd:?}: {err}"))
        })
    }

    pub fn new_with_addr<F: FnOnce(&mut process::Command) -> process::Child>(
        addr: redis::ConnectionAddr,
        tls_paths: Option<TlsFilePaths>,
        modules: &[Module],
        spawner: F,
    ) -> RedisServer {
        let mut redis_cmd = process::Command::new("redis-server");

        // Load Redis Modules
        for module in modules {
            match module {
                Module::Json => {
                    redis_cmd
                        .arg("--loadmodule")
                        .arg(env::var("REDIS_RS_REDIS_JSON_PATH").expect(
                        "Unable to find path to RedisJSON at REDIS_RS_REDIS_JSON_PATH, is it set?",
                    ));
                }
            };
        }

        redis_cmd
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null());
        let tempdir = tempfile::Builder::new()
            .prefix("redis")
            .tempdir()
            .expect("failed to create tempdir");
        match addr {
            redis::ConnectionAddr::Tcp(ref bind, server_port) => {
                redis_cmd
                    .arg("--port")
                    .arg(server_port.to_string())
                    .arg("--bind")
                    .arg(bind);

                RedisServer {
                    process: spawner(&mut redis_cmd),
                    tempdir: None,
                    addr,
                    tls_paths,
                }
            }
            redis::ConnectionAddr::TcpTls {
                ref host,
                port,
                insecure,
                ..
            } => {
                let tls_paths = tls_paths.unwrap_or_else(|| build_keys_and_certs_for_tls(&tempdir));
                let auth_client_str = if insecure { "no" } else { "yes" };

                // prepare redis with TLS
                redis_cmd
                    .arg("--tls-port")
                    .arg(&port.to_string())
                    .arg("--port")
                    .arg("0")
                    .arg("--tls-cert-file")
                    .arg(&tls_paths.redis_crt)
                    .arg("--tls-key-file")
                    .arg(&tls_paths.redis_key)
                    .arg("--tls-ca-cert-file")
                    .arg(&tls_paths.ca_crt)
                    .arg("--tls-auth-clients")
                    .arg(auth_client_str)
                    .arg("--bind")
                    .arg(host);

                RedisServer {
                    process: spawner(&mut redis_cmd),
                    tempdir: Some(tempdir),
                    addr,
                    tls_paths: Some(tls_paths),
                }
            }
            redis::ConnectionAddr::Unix(ref path) => {
                redis_cmd
                    .arg("--port")
                    .arg("0")
                    .arg("--unixsocket")
                    .arg(path);
                RedisServer {
                    process: spawner(&mut redis_cmd),
                    tempdir: Some(tempdir),
                    addr,
                    tls_paths: None,
                }
            }
        }
    }

    pub fn client_addr(&self) -> redis::ConnectionAddr {
        let client_address = self.addr.clone();
        match client_address {
            #[cfg(feature = "tls")]
            ConnectionAddr::TcpTls {
                ref host,
                ref port,
                insecure,
                ..
            } => {
                if !insecure {
                    let tls_path = self
                        .tls_paths
                        .as_ref()
                        .expect("failed ot get TLS paths on TLS mode");

                    let (ca_cert, identity) = build_sec_entities(
                        &tls_path.ca_crt,
                        &tls_path.client_crt,
                        &tls_path.client_key,
                    );
                    let sec_client_address = ConnectionAddr::TcpTls {
                        host: host.clone(),
                        port: *port,
                        insecure: false,
                        ca_cert: Some(ca_cert),
                        identity: Some(identity),
                    };
                    return sec_client_address;
                }
                client_address
            }
            _ => client_address,
        }
    }

    pub fn connection_info(&self) -> redis::ConnectionInfo {
        redis::ConnectionInfo {
            addr: self.client_addr(),
            redis: Default::default(),
        }
    }

    pub fn stop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let redis::ConnectionAddr::Unix(ref path) = self.client_addr() {
            fs::remove_file(path).ok();
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
        TestContext::with_modules(&[])
    }

    pub fn with_modules(modules: &[Module]) -> TestContext {
        let server = RedisServer::with_modules(modules);

        let client = redis::Client::open(server.connection_info()).unwrap();
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
                            panic!("Tried to connect too many times, last error: {err}");
                        }
                    } else {
                        panic!("Could not connect: {err}");
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

    #[cfg(feature = "tokio-comp")]
    pub fn multiplexed_async_connection(
        &self,
    ) -> impl Future<Output = redis::RedisResult<redis::aio::MultiplexedConnection>> {
        self.multiplexed_async_connection_tokio()
    }

    #[cfg(feature = "tokio-comp")]
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
        Value::Int(val) => write!(writer, ":{val}\r\n"),
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
        Value::Status(ref s) => write!(writer, "+{s}\r\n"),
    }
}

#[derive(Clone)]
pub struct TlsFilePaths {
    redis_crt: PathBuf,
    redis_key: PathBuf,
    pub ca_crt: PathBuf,
    pub client_crt: PathBuf,
    pub client_key: PathBuf,
}

pub fn build_keys_and_certs_for_tls(tempdir: &TempDir) -> TlsFilePaths {
    // Based on shell script in redis's server tests
    // https://github.com/redis/redis/blob/8c291b97b95f2e011977b522acf77ead23e26f55/utils/gen-test-certs.sh
    let ca_crt = tempdir.path().join("ca.crt");
    let ca_key = tempdir.path().join("ca.key");
    let ca_serial = tempdir.path().join("ca.txt");
    let redis_crt = tempdir.path().join("redis.crt");
    let redis_key = tempdir.path().join("redis.key");
    let client_crt = tempdir.path().join("redis_client.crt");
    let client_key = tempdir.path().join("redis_client.key");

    let ca_conf = tempdir.path().join("ca.conf");
    let csr_conf = tempdir.path().join("csr.conf");
    let cert_conf = tempdir.path().join("cert.conf");

    #[cfg(feature = "tls")]
    write_ca_conf_file(&ca_conf).expect("failed to write ca conf file");

    #[cfg(feature = "tls")]
    write_csr_conf_file(&csr_conf).expect("failed to write csr conf file");

    #[cfg(feature = "tls")]
    write_cert_conf_file(&cert_conf).expect("failed to write cert ext file");

    fn make_key<S: AsRef<std::ffi::OsStr>>(name: S, size: usize) {
        process::Command::new("openssl")
            .arg("genrsa")
            .arg("-out")
            .arg(name)
            .arg(&format!("{size}"))
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null())
            .spawn()
            .expect("failed to spawn openssl")
            .wait()
            .expect("failed to create key");
    }

    fn make_cert(
        key: &PathBuf,
        crt: &PathBuf,
        ca_key: &PathBuf,
        ca_serial: &PathBuf,
        ca_crt: &PathBuf,
        csr_conf: &PathBuf,
        cert_conf: &PathBuf,
    ) {
        // create csr
        let mut key_cmd = process::Command::new("openssl")
            .arg("req")
            .arg("-new")
            .arg("-sha256")
            .arg("-subj")
            .arg("/O=Redis Test/CN=localhost")
            .arg("-key")
            .arg(key)
            .arg("-config")
            .arg(csr_conf)
            .stdout(process::Stdio::piped())
            .stderr(process::Stdio::null())
            .spawn()
            .expect("failed to spawn openssl");

        // build  cert
        process::Command::new("openssl")
            .arg("x509")
            .arg("-req")
            .arg("-sha256")
            .arg("-CA")
            .arg(ca_crt)
            .arg("-CAkey")
            .arg(ca_key)
            .arg("-CAserial")
            .arg(ca_serial)
            .arg("-CAcreateserial")
            .arg("-days")
            .arg("365")
            .arg("-extfile")
            .arg(cert_conf)
            .arg("-out")
            .arg(crt)
            .stdin(key_cmd.stdout.take().expect("should have stdout"))
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null())
            .spawn()
            .expect("failed to spawn openssl")
            .wait()
            .expect("failed to create cert");

        key_cmd.wait().expect("failed to create key");
    }

    fn convert_key(key: &PathBuf, key8: &PathBuf) {
        //convert to pkcs8
        process::Command::new("openssl")
            .arg("pkcs8")
            .arg("-nocrypt")
            .arg("-topk8")
            .arg("-in")
            .arg(key)
            .arg("-out")
            .arg(key8)
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null())
            .spawn()
            .expect("failed to spawn openssl")
            .wait()
            .expect("failed to convert key");
    }

    // Build CA Key
    make_key(&ca_key, 4096);

    // Build redis key
    make_key(&redis_key, 2048);

    //Build client key
    make_key(&client_key, 2048);

    // Build CA Cert
    process::Command::new("openssl")
        .arg("req")
        .arg("-x509")
        .arg("-new")
        .arg("-nodes")
        .arg("-sha256")
        .arg("-key")
        .arg(&ca_key)
        .arg("-days")
        .arg("3650")
        .arg("-config")
        .arg(ca_conf)
        .arg("-subj")
        .arg("/O=Redis Test/CN=Certificate Authority")
        .arg("-out")
        .arg(&ca_crt)
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .spawn()
        .expect("failed to spawn openssl")
        .wait()
        .expect("failed to create CA cert");

    make_cert(
        &redis_key, &redis_crt, &ca_key, &ca_serial, &ca_crt, &csr_conf, &cert_conf,
    );

    make_cert(
        &client_key,
        &client_crt,
        &ca_key,
        &ca_serial,
        &ca_crt,
        &csr_conf,
        &cert_conf,
    );

    let mut ks = client_key.clone().into_os_string();
    ks.push("8");
    let client_key8 = PathBuf::from(ks);
    ks = redis_key.clone().into_os_string();
    ks.push("8");
    let redis_key8 = PathBuf::from(ks);

    convert_key(&client_key, &client_key8);
    convert_key(&redis_key, &redis_key8);

    TlsFilePaths {
        redis_crt,
        redis_key: redis_key8,
        ca_crt,
        client_crt,
        client_key: client_key8,
    }
}

#[cfg(feature = "tls")]
pub fn build_sec_entities(
    ca_crt: &PathBuf,
    my_crt: &PathBuf,
    my_key: &PathBuf,
) -> (Certificate, RedisIdentity) {
    let ca_cert_ = read(ca_crt).unwrap();
    let ca_cert = Certificate::from_pem(ca_cert_.as_slice()).unwrap();

    let tls_key = read(my_key).unwrap();
    let tls_crt = read(my_crt).unwrap();
    let ident = RedisIdentity::build(tls_crt, tls_key);

    (ca_cert, ident)
}

#[cfg(feature = "tls")]
fn write_ca_conf_file(path: &PathBuf) -> io::Result<()> {
    let text = r#"
[req]
x509_extensions=v3_ca
distinguished_name = req_distinguished_name
[ req_distinguished_name ]
[v3_ca]
basicConstraints=critical,CA:TRUE
keyUsage=critical, keyCertSign
extendedKeyUsage=serverAuth,clientAuth

"#;
    fs::write(path, text.as_bytes())
}

#[cfg(feature = "tls")]
fn write_csr_conf_file(path: &PathBuf) -> io::Result<()> {
    let text = r#"
basicConstraints=CA:FALSE
distinguished_name = dn
req_extensions = req_ext

[ req_ext ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
IP.1 = 127.0.0.1

[ dn ]
C = US
ST = California
L = San Fransisco
CN = localhost

"#;
    fs::write(path, text.as_bytes())
}

fn write_cert_conf_file(path: &PathBuf) -> io::Result<()> {
    let text = r#"
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
IP.1 = 127.0.0.1

"#;
    fs::write(path, text.as_bytes())
}
