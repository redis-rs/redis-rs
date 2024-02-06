#![allow(dead_code)]

use std::path::Path;
use std::{
    env, fs, io, net::SocketAddr, net::TcpListener, path::PathBuf, process, thread::sleep,
    time::Duration,
};
#[cfg(feature = "tls-rustls")]
use std::{
    fs::File,
    io::{BufReader, Read},
};

#[cfg(feature = "aio")]
use futures::Future;
use redis::{ConnectionAddr, InfoDict, Value};

#[cfg(feature = "tls-rustls")]
use redis::{ClientTlsConfig, TlsCertificates};

use socket2::{Domain, Socket, Type};
use tempfile::TempDir;

pub fn current_thread_runtime() -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new_current_thread();

    #[cfg(feature = "aio")]
    builder.enable_io();

    builder.enable_time();

    builder.build().unwrap()
}

#[cfg(feature = "aio")]
pub fn block_on_all<F, V>(f: F) -> F::Output
where
    F: Future<Output = redis::RedisResult<V>>,
{
    use std::panic;
    use std::sync::atomic::{AtomicBool, Ordering};

    static CHECK: AtomicBool = AtomicBool::new(false);

    // TODO - this solution is purely single threaded, and won't work on multiple threads at the same time.
    // This is needed because Tokio's Runtime silently ignores panics - https://users.rust-lang.org/t/tokio-runtime-what-happens-when-a-thread-panics/95819
    // Once Tokio stabilizes the `unhandled_panic` field on the runtime builder, it should be used instead.
    panic::set_hook(Box::new(|panic| {
        println!("Panic: {panic}");
        CHECK.store(true, Ordering::Relaxed);
    }));

    // This continuously query the flag, in order to abort ASAP after a panic.
    let check_future = futures_util::FutureExt::fuse(async {
        loop {
            if CHECK.load(Ordering::Relaxed) {
                return Err((redis::ErrorKind::IoError, "panic was caught").into());
            }
            futures_time::task::sleep(futures_time::time::Duration::from_millis(1)).await;
        }
    });
    let f = futures_util::FutureExt::fuse(f);
    futures::pin_mut!(f, check_future);

    let res = current_thread_runtime().block_on(async {
        futures::select! {res = f => res, err = check_future => err}
    });

    let _ = panic::take_hook();
    if CHECK.swap(false, Ordering::Relaxed) {
        panic!("Internal thread panicked");
    }

    res
}

#[cfg(feature = "aio")]
#[test]
fn test_block_on_all_panics_from_spawns() {
    let result = std::panic::catch_unwind(|| {
        block_on_all(async {
            tokio::task::spawn(async {
                futures_time::task::sleep(futures_time::time::Duration::from_millis(1)).await;
                panic!("As it should");
            });
            futures_time::task::sleep(futures_time::time::Duration::from_millis(10)).await;
            Ok(())
        })
    });
    assert!(result.is_err());
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
mod mock_cluster;

mod util;

#[cfg(any(feature = "cluster", feature = "cluster-async"))]
#[allow(unused_imports)]
pub use self::cluster::*;

#[cfg(any(feature = "cluster", feature = "cluster-async"))]
#[allow(unused_imports)]
pub use self::mock_cluster::*;

#[cfg(feature = "sentinel")]
mod sentinel;

#[cfg(feature = "sentinel")]
#[allow(unused_imports)]
pub use self::sentinel::*;

#[derive(PartialEq)]
enum ServerType {
    Tcp { tls: bool },
    Unix,
}

pub enum Module {
    Json,
}

pub struct RedisServer {
    pub process: process::Child,
    tempdir: tempfile::TempDir,
    addr: redis::ConnectionAddr,
    pub(crate) tls_paths: Option<TlsFilePaths>,
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
            Some(val) => {
                panic!("Unknown server type {val:?}");
            }
            None => ServerType::Tcp { tls: false },
        }
    }
}

impl RedisServer {
    pub fn new() -> RedisServer {
        RedisServer::with_modules(&[], false)
    }

    #[cfg(feature = "tls-rustls")]
    pub fn new_with_mtls() -> RedisServer {
        RedisServer::with_modules(&[], true)
    }

    pub fn get_addr(port: u16) -> ConnectionAddr {
        let server_type = ServerType::get_intended();
        match server_type {
            ServerType::Tcp { tls } => {
                if tls {
                    redis::ConnectionAddr::TcpTls {
                        host: "127.0.0.1".to_string(),
                        port,
                        insecure: true,
                        tls_params: None,
                    }
                } else {
                    redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), port)
                }
            }
            ServerType::Unix => {
                let (a, b) = rand::random::<(u64, u64)>();
                let path = format!("/tmp/redis-rs-test-{a}-{b}.sock");
                redis::ConnectionAddr::Unix(PathBuf::from(&path))
            }
        }
    }

    pub fn with_modules(modules: &[Module], mtls_enabled: bool) -> RedisServer {
        // this is technically a race but we can't do better with
        // the tools that redis gives us :(
        let redis_port = get_random_available_port();
        let addr = RedisServer::get_addr(redis_port);

        RedisServer::new_with_addr_tls_modules_and_spawner(
            addr,
            None,
            None,
            mtls_enabled,
            modules,
            |cmd| {
                cmd.spawn()
                    .unwrap_or_else(|err| panic!("Failed to run {cmd:?}: {err}"))
            },
        )
    }

    pub fn new_with_addr_and_modules(
        addr: redis::ConnectionAddr,
        modules: &[Module],
        mtls_enabled: bool,
    ) -> RedisServer {
        RedisServer::new_with_addr_tls_modules_and_spawner(
            addr,
            None,
            None,
            mtls_enabled,
            modules,
            |cmd| {
                cmd.spawn()
                    .unwrap_or_else(|err| panic!("Failed to run {cmd:?}: {err}"))
            },
        )
    }

    pub fn new_with_addr_tls_modules_and_spawner<
        F: FnOnce(&mut process::Command) -> process::Child,
    >(
        addr: redis::ConnectionAddr,
        config_file: Option<&Path>,
        tls_paths: Option<TlsFilePaths>,
        mtls_enabled: bool,
        modules: &[Module],
        spawner: F,
    ) -> RedisServer {
        let mut redis_cmd = process::Command::new("redis-server");

        if let Some(config_path) = config_file {
            redis_cmd.arg(config_path);
        }

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
        redis_cmd.arg("--logfile").arg(Self::log_file(&tempdir));
        match addr {
            redis::ConnectionAddr::Tcp(ref bind, server_port) => {
                redis_cmd
                    .arg("--port")
                    .arg(server_port.to_string())
                    .arg("--bind")
                    .arg(bind);

                RedisServer {
                    process: spawner(&mut redis_cmd),
                    tempdir,
                    addr,
                    tls_paths: None,
                }
            }
            redis::ConnectionAddr::TcpTls { ref host, port, .. } => {
                let tls_paths = tls_paths.unwrap_or_else(|| build_keys_and_certs_for_tls(&tempdir));

                let auth_client = if mtls_enabled { "yes" } else { "no" };

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
                    .arg(auth_client)
                    .arg("--bind")
                    .arg(host);

                // Insecure only disabled if `mtls` is enabled
                let insecure = !mtls_enabled;

                let addr = redis::ConnectionAddr::TcpTls {
                    host: host.clone(),
                    port,
                    insecure,
                    tls_params: None,
                };

                RedisServer {
                    process: spawner(&mut redis_cmd),
                    tempdir,
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
                    tempdir,
                    addr,
                    tls_paths: None,
                }
            }
        }
    }

    pub fn client_addr(&self) -> &redis::ConnectionAddr {
        &self.addr
    }

    pub fn connection_info(&self) -> redis::ConnectionInfo {
        redis::ConnectionInfo {
            addr: self.client_addr().clone(),
            redis: Default::default(),
        }
    }

    pub fn stop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let redis::ConnectionAddr::Unix(ref path) = *self.client_addr() {
            fs::remove_file(path).ok();
        }
    }

    pub fn log_file(tempdir: &TempDir) -> PathBuf {
        tempdir.path().join("redis.log")
    }
}

/// Finds a random open port available for listening at, by spawning a TCP server with
/// port "zero" (which prompts the OS to just use any available port). Between calling
/// this function and trying to bind to this port, the port may be given to another
/// process, so this must be used with care (since here we only use it for tests, it's
/// mostly okay).
pub fn get_random_available_port() -> u16 {
    let addr = &"127.0.0.1:0".parse::<SocketAddr>().unwrap().into();
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
    socket.set_reuse_address(true).unwrap();
    socket.bind(addr).unwrap();
    socket.listen(1).unwrap();
    let listener = TcpListener::from(socket);
    listener.local_addr().unwrap().port()
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

pub(crate) fn is_tls_enabled() -> bool {
    cfg!(all(feature = "tls-rustls", not(feature = "tls-native-tls")))
}

impl TestContext {
    pub fn new() -> TestContext {
        TestContext::with_modules(&[], false)
    }

    #[cfg(feature = "tls-rustls")]
    pub fn new_with_mtls() -> TestContext {
        Self::with_modules(&[], true)
    }

    pub fn with_tls(tls_files: TlsFilePaths, mtls_enabled: bool) -> TestContext {
        let redis_port = get_random_available_port();
        let addr = RedisServer::get_addr(redis_port);

        let server = RedisServer::new_with_addr_tls_modules_and_spawner(
            addr,
            None,
            Some(tls_files),
            mtls_enabled,
            &[],
            |cmd| {
                cmd.spawn()
                    .unwrap_or_else(|err| panic!("Failed to run {cmd:?}: {err}"))
            },
        );

        #[cfg(feature = "tls-rustls")]
        let client =
            build_single_client(server.connection_info(), &server.tls_paths, mtls_enabled).unwrap();
        #[cfg(not(feature = "tls-rustls"))]
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

    pub fn with_modules(modules: &[Module], mtls_enabled: bool) -> TestContext {
        let server = RedisServer::with_modules(modules, mtls_enabled);

        #[cfg(feature = "tls-rustls")]
        let client =
            build_single_client(server.connection_info(), &server.tls_paths, mtls_enabled).unwrap();
        #[cfg(not(feature = "tls-rustls"))]
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
    pub async fn async_connection(&self) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
    }

    #[cfg(feature = "aio")]
    pub async fn async_pubsub(&self) -> redis::RedisResult<redis::aio::PubSub> {
        self.client.get_async_pubsub().await
    }

    #[cfg(feature = "async-std-comp")]
    pub async fn async_connection_async_std(
        &self,
    ) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_async_std_connection().await
    }

    pub fn stop_server(&mut self) {
        self.server.stop();
    }

    #[cfg(feature = "tokio-comp")]
    pub async fn multiplexed_async_connection(
        &self,
    ) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        self.multiplexed_async_connection_tokio().await
    }

    #[cfg(feature = "tokio-comp")]
    pub async fn multiplexed_async_connection_tokio(
        &self,
    ) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_tokio_connection().await
    }

    #[cfg(feature = "async-std-comp")]
    pub async fn multiplexed_async_connection_async_std(
        &self,
    ) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_async_std_connection().await
    }

    pub fn get_version(&self) -> Version {
        let mut conn = self.connection();
        get_version(&mut conn)
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

#[derive(Clone, Debug)]
pub struct TlsFilePaths {
    pub(crate) redis_crt: PathBuf,
    pub(crate) redis_key: PathBuf,
    pub(crate) ca_crt: PathBuf,
}

pub fn build_keys_and_certs_for_tls(tempdir: &TempDir) -> TlsFilePaths {
    // Based on shell script in redis's server tests
    // https://github.com/redis/redis/blob/8c291b97b95f2e011977b522acf77ead23e26f55/utils/gen-test-certs.sh
    let ca_crt = tempdir.path().join("ca.crt");
    let ca_key = tempdir.path().join("ca.key");
    let ca_serial = tempdir.path().join("ca.txt");
    let redis_crt = tempdir.path().join("redis.crt");
    let redis_key = tempdir.path().join("redis.key");
    let ext_file = tempdir.path().join("openssl.cnf");

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

    // Build CA Key
    make_key(&ca_key, 4096);

    // Build redis key
    make_key(&redis_key, 2048);

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

    // Build x509v3 extensions file
    fs::write(
        &ext_file,
        b"keyUsage = digitalSignature, keyEncipherment\n\
    subjectAltName = @alt_names\n\
    [alt_names]\n\
    IP.1 = 127.0.0.1\n",
    )
    .expect("failed to create x509v3 extensions file");

    // Read redis key
    let mut key_cmd = process::Command::new("openssl")
        .arg("req")
        .arg("-new")
        .arg("-sha256")
        .arg("-subj")
        .arg("/O=Redis Test/CN=Generic-cert")
        .arg("-key")
        .arg(&redis_key)
        .stdout(process::Stdio::piped())
        .stderr(process::Stdio::null())
        .spawn()
        .expect("failed to spawn openssl");

    // build redis cert
    process::Command::new("openssl")
        .arg("x509")
        .arg("-req")
        .arg("-sha256")
        .arg("-CA")
        .arg(&ca_crt)
        .arg("-CAkey")
        .arg(&ca_key)
        .arg("-CAserial")
        .arg(&ca_serial)
        .arg("-CAcreateserial")
        .arg("-days")
        .arg("365")
        .arg("-extfile")
        .arg(&ext_file)
        .arg("-out")
        .arg(&redis_crt)
        .stdin(key_cmd.stdout.take().expect("should have stdout"))
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .spawn()
        .expect("failed to spawn openssl")
        .wait()
        .expect("failed to create redis cert");

    key_cmd.wait().expect("failed to create redis key");

    TlsFilePaths {
        redis_crt,
        redis_key,
        ca_crt,
    }
}

pub type Version = (u16, u16, u16);

fn get_version(conn: &mut impl redis::ConnectionLike) -> Version {
    let info: InfoDict = redis::Cmd::new().arg("INFO").query(conn).unwrap();
    let version: String = info.get("redis_version").unwrap();
    let versions: Vec<u16> = version
        .split('.')
        .map(|version| version.parse::<u16>().unwrap())
        .collect();
    assert_eq!(versions.len(), 3);
    (versions[0], versions[1], versions[2])
}

pub fn is_major_version(expected_version: u16, version: Version) -> bool {
    expected_version <= version.0
}

pub fn is_version(expected_major_minor: (u16, u16), version: Version) -> bool {
    expected_major_minor.0 < version.0
        || (expected_major_minor.0 == version.0 && expected_major_minor.1 <= version.1)
}

#[cfg(feature = "tls-rustls")]
fn load_certs_from_file(tls_file_paths: &TlsFilePaths) -> TlsCertificates {
    let ca_file = File::open(&tls_file_paths.ca_crt).expect("Cannot open CA cert file");
    let mut root_cert_vec = Vec::new();
    BufReader::new(ca_file)
        .read_to_end(&mut root_cert_vec)
        .expect("Unable to read CA cert file");

    let cert_file = File::open(&tls_file_paths.redis_crt).expect("cannot open private cert file");
    let mut client_cert_vec = Vec::new();
    BufReader::new(cert_file)
        .read_to_end(&mut client_cert_vec)
        .expect("Unable to read client cert file");

    let key_file = File::open(&tls_file_paths.redis_key).expect("Cannot open private key file");
    let mut client_key_vec = Vec::new();
    BufReader::new(key_file)
        .read_to_end(&mut client_key_vec)
        .expect("Unable to read client key file");

    TlsCertificates {
        client_tls: Some(ClientTlsConfig {
            client_cert: client_cert_vec,
            client_key: client_key_vec,
        }),
        root_cert: Some(root_cert_vec),
    }
}

#[cfg(feature = "tls-rustls")]
pub(crate) fn build_single_client<T: redis::IntoConnectionInfo>(
    connection_info: T,
    tls_file_params: &Option<TlsFilePaths>,
    mtls_enabled: bool,
) -> redis::RedisResult<redis::Client> {
    if mtls_enabled && tls_file_params.is_some() {
        redis::Client::build_with_tls(
            connection_info,
            load_certs_from_file(
                tls_file_params
                    .as_ref()
                    .expect("Expected certificates when `tls-rustls` feature is enabled"),
            ),
        )
    } else {
        redis::Client::open(connection_info)
    }
}

#[cfg(feature = "tls-rustls")]
pub(crate) mod mtls_test {
    use super::*;
    use redis::{cluster::ClusterClient, ConnectionInfo, RedisError};

    fn clean_node_info(nodes: &[ConnectionInfo]) -> Vec<ConnectionInfo> {
        let nodes = nodes
            .iter()
            .map(|node| match node {
                ConnectionInfo {
                    addr: redis::ConnectionAddr::TcpTls { host, port, .. },
                    redis,
                } => ConnectionInfo {
                    addr: redis::ConnectionAddr::TcpTls {
                        host: host.to_owned(),
                        port: *port,
                        insecure: false,
                        tls_params: None,
                    },
                    redis: redis.clone(),
                },
                _ => node.clone(),
            })
            .collect();
        nodes
    }

    pub(crate) fn create_cluster_client_from_cluster(
        cluster: &TestClusterContext,
        mtls_enabled: bool,
    ) -> Result<ClusterClient, RedisError> {
        let server = cluster
            .cluster
            .servers
            .first()
            .expect("Expected at least 1 server");
        let tls_paths = server.tls_paths.as_ref();
        let nodes = clean_node_info(&cluster.nodes);
        let builder = redis::cluster::ClusterClientBuilder::new(nodes);
        if let Some(tls_paths) = tls_paths {
            // server-side TLS available
            if mtls_enabled {
                builder.certs(load_certs_from_file(tls_paths))
            } else {
                builder
            }
        } else {
            // server-side TLS NOT available
            builder
        }
        .build()
    }
}
