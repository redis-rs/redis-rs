#![allow(dead_code)]

#[cfg(feature = "aio")]
use futures::Future;
#[cfg(feature = "cache-aio")]
use redis::caching::CacheConfig;
#[cfg(feature = "tls-rustls")]
use redis::{ClientTlsConfig, TlsCertificates};
use redis::{
    Commands, ConnectionAddr, ErrorKind, Pipeline, ProtocolVersion, RedisResult, ServerErrorKind,
    Value,
};
#[cfg(feature = "aio")]
use redis::{aio, cmd};
use redis_test::server::{
    Module, RedisServer, RedisServerBuilder, RedisServerCommand, use_protocol,
};
use redis_test::utils::{TlsFilePaths, get_random_available_port};
use std::path::PathBuf;
#[cfg(feature = "tls-rustls")]
use std::{
    fs::File,
    io::{BufReader, Read},
};
use std::{io, thread::sleep, time::Duration};

#[macro_use]
mod version;
pub use version::*;

pub fn current_thread_runtime() -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new_current_thread();

    #[cfg(feature = "tokio-comp")]
    builder.enable_io();

    builder.enable_time();

    builder.build().unwrap()
}

#[cfg(feature = "aio")]
#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum RuntimeType {
    #[cfg(feature = "tokio-comp")]
    Tokio,
    #[cfg(feature = "smol-comp")]
    Smol,
}

#[cfg(feature = "aio")]
pub fn block_on_all<F, V>(f: F, runtime: RuntimeType) -> F::Output
where
    F: Future<Output = V>,
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
                return;
            }
            futures_time::task::sleep(futures_time::time::Duration::from_millis(1)).await;
        }
    });
    let f = futures_util::FutureExt::fuse(f);
    futures::pin_mut!(f, check_future);

    let f = async move {
        futures::select! {res = f => Ok(res), err = check_future => Err(err)}
    };

    let res = match runtime {
        #[cfg(feature = "tokio-comp")]
        RuntimeType::Tokio => block_on_all_using_tokio(f),
        #[cfg(feature = "smol-comp")]
        RuntimeType::Smol => block_on_all_using_smol(f),
    };

    let _ = panic::take_hook();
    if CHECK.swap(false, Ordering::Relaxed) {
        panic!("Internal thread panicked");
    }

    res.unwrap()
}

#[cfg(feature = "tokio-comp")]
fn block_on_all_using_tokio<F>(f: F) -> F::Output
where
    F: Future,
{
    #[cfg(feature = "smol-comp")]
    redis::aio::prefer_tokio().unwrap();
    current_thread_runtime().block_on(f)
}

#[cfg(feature = "smol-comp")]
fn block_on_all_using_smol<F>(f: F) -> F::Output
where
    F: Future,
{
    #[cfg(feature = "tokio-comp")]
    redis::aio::prefer_smol().unwrap();
    smol::block_on(f)
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

/// A builder for [`TestContext`]
///
/// # Example
///
/// ```rust,no_run
/// use tests::support::TestContextBuilder;
///
/// let ctx = TestContextBuilder::new().module(Module::Json).build();
/// let connection = ctx.connection();
/// // Use `connection` to run commands
/// ```
// Note that this builder is an owned-builder as we want to build in a single chain anyway and do
// not have to build multiple instances from the same builder. Also, this spares us cloning
// considerations.
#[derive(Default)]
pub struct TestContextBuilder {
    server_builder: RedisServerBuilder,
}

impl TestContextBuilder {
    /// Starts a fresh builder
    pub fn new() -> Self {
        Default::default()
    }

    pub fn address(mut self, address: ConnectionAddr) -> Self {
        self.server_builder = self.server_builder.address(address);
        self
    }

    pub fn config(mut self, config_file: PathBuf) -> Self {
        self.server_builder = self.server_builder.config(config_file);
        self
    }

    pub fn cert_auth_field(mut self, cert_auth_field: impl Into<String>) -> Self {
        self.server_builder = self.server_builder.cert_auth_field(cert_auth_field);
        self
    }

    pub fn cert_auth_field_opt(mut self, opt_cert_auth_field: Option<impl Into<String>>) -> Self {
        self.server_builder = self.server_builder.cert_auth_field_opt(opt_cert_auth_field);
        self
    }

    pub fn module(mut self, module: Module) -> Self {
        self.server_builder = self.server_builder.module(module);
        self
    }

    pub fn modules(mut self, modules: &[Module]) -> Self {
        self.server_builder = self.server_builder.modules(modules);
        self
    }

    pub fn mtls(mut self, enable_mtls: bool) -> Self {
        self.server_builder = self.server_builder.mtls(enable_mtls);
        self
    }

    pub fn tls_paths(mut self, tls_paths: TlsFilePaths) -> Self {
        self.server_builder = self.server_builder.tls_paths(tls_paths);
        self
    }

    pub fn tls_paths_opt(mut self, opt_tls_paths: Option<TlsFilePaths>) -> Self {
        self.server_builder = self.server_builder.tls_paths_opt(opt_tls_paths);
        self
    }

    /// Builds the [`TestContext`] for this instance
    pub fn build(self) -> TestContext {
        self.refine_and_build(|_| {})
    }

    /// Builds the [`TestContext`] for this instance after refining the arguments for the server
    ///
    /// # Arguments
    ///
    /// * `refiner` - See [`RedisServerBuilder::refine_and_build`]
    pub fn refine_and_build(self, refiner: impl FnOnce(&mut RedisServerCommand)) -> TestContext {
        let server = self.server_builder.refine_and_build(refiner);
        TestContext::from_server(server)
    }
}

/// Utility wrapper for a standalone Redis server instance for testing.
///
/// # Example
///
/// Use `default()` to build a [`TestContext`] with default settings:
///
/// ```rust,no_run
/// use tests::support::TestContext;
///
/// let ctx = TestContext::default();
/// let connection = ctx.connection();
/// // Use `connection` to run commands
/// ```
///
/// If you need a custom setup, use [`TestContextBuilder`]:
///
/// ```rust,no_run
/// use tests::support::TestContextBuilder;
///
/// let ctx = TestContextBuilder::new().module(Module::Json).build();
/// let connection = ctx.connection();
/// // Use `connection` to run commands
/// ```
pub struct TestContext {
    pub server: RedisServer,
    pub client: redis::Client,
    pub protocol: ProtocolVersion,
}

pub(crate) fn start_tls_crypto_provider() {
    #[cfg(feature = "tls-rustls")]
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        // we don't care about success, because failure means that the provider was set from another thread.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

impl Default for TestContext {
    fn default() -> Self {
        TestContextBuilder::new().build()
    }
}

impl TestContext {
    pub fn new() -> TestContext {
        TestContext::with_modules(&[])
    }

    #[cfg(feature = "tls-rustls")]
    pub fn new_with_mtls() -> TestContext {
        Self::with_modules_and_tls(&[], true, None)
    }

    #[cfg(feature = "tls-rustls")]
    pub fn new_with_cert_auth(tls_files: TlsFilePaths) -> TestContext {
        Self::new_with_cert_auth_field(tls_files, "CN")
    }

    #[cfg(feature = "tls-rustls")]
    pub fn new_with_cert_auth_field(tls_files: TlsFilePaths, cert_field: &str) -> TestContext {
        start_tls_crypto_provider();
        let redis_port = get_random_available_port();
        let addr = RedisServer::get_addr(redis_port);

        // TLS certificate-based authentication requires TLS connection.
        let addr = match addr {
            ConnectionAddr::Tcp(host, port) => ConnectionAddr::TcpTls {
                host,
                port,
                insecure: true,
                tls_params: None,
            },
            ConnectionAddr::TcpTls { .. } => addr, // Already TLS
            ConnectionAddr::Unix(_) => {
                // Unix sockets don't support TLS - fall back to TCP+TLS
                // Use the same default host that get_addr() would use for TCP
                ConnectionAddr::TcpTls {
                    host: redis_test::server::get_default_host(),
                    port: redis_port,
                    insecure: true,
                    tls_params: None,
                }
            }
            _ => {
                panic!("Unsupported ConnectionAddr variant for cert-based authentication: {addr:?}")
            }
        };

        Self::with_modules_addr_tls_and_cert_auth(
            &[],
            true,
            addr,
            Some(tls_files),
            Some(cert_field),
        )
    }

    pub fn with_modules(modules: &[Module]) -> TestContext {
        Self::with_modules_and_tls(modules, false, None)
    }

    pub fn new_with_addr(addr: ConnectionAddr) -> Self {
        Self::with_modules_addr_and_tls(&[], false, addr, None)
    }

    fn with_modules_and_tls(
        modules: &[Module],
        mtls_enabled: bool,
        tls_files: Option<TlsFilePaths>,
    ) -> Self {
        start_tls_crypto_provider();
        let redis_port = get_random_available_port();
        let addr = RedisServer::get_addr(redis_port);
        Self::with_modules_addr_and_tls(modules, mtls_enabled, addr, tls_files)
    }

    fn with_modules_addr_and_tls(
        modules: &[Module],
        mtls_enabled: bool,
        addr: ConnectionAddr,
        tls_files: Option<TlsFilePaths>,
    ) -> Self {
        Self::with_modules_addr_tls_and_cert_auth(modules, mtls_enabled, addr, tls_files, None)
    }

    fn with_modules_addr_tls_and_cert_auth(
        modules: &[Module],
        mtls_enabled: bool,
        addr: ConnectionAddr,
        tls_files: Option<TlsFilePaths>,
        cert_auth_field: Option<&str>,
    ) -> Self {
        TestContextBuilder::new()
            .modules(modules)
            .mtls(mtls_enabled)
            .address(addr)
            .tls_paths_opt(tls_files)
            .cert_auth_field_opt(cert_auth_field)
            .build()
    }

    /// Builds a new instance from a [`RedisServer`]
    // We intentionally do _not_ implement `From<RedisServer>` as that would be public.
    //
    // Instead, users should to go through `TestContextBuilder` to limit the points of entry and
    // hence help us with maintenance.
    fn from_server(server: RedisServer) -> Self {
        let client =
            build_single_client(server.connection_info(), &server.tls_paths, server.mtls).unwrap();

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
                            panic!(
                                "Tried to connect too many times, last error: {err}, logfile: {:?}",
                                server.log_file_contents()
                            );
                        }
                    } else {
                        panic!(
                            "Could not connect: {err}, logfile: {:?}",
                            server.log_file_contents()
                        );
                    }
                }
                Ok(x) => {
                    con = x;
                    break;
                }
            }
        }

        // Redis may still be loading its dataset after accepting connections,
        // especially with TLS where the handshake completes before Redis is fully ready.
        // Retry flushdb if the BusyLoading error is returned to allow time for initialization.
        let mut flush_retries = 0;
        loop {
            match con.flushdb::<()>() {
                Ok(_) => break,
                Err(err)
                    if matches!(err.kind(), ErrorKind::Server(ServerErrorKind::BusyLoading)) =>
                {
                    sleep(millisecond);
                    flush_retries += 1;
                    if flush_retries > 10000 {
                        panic!(
                            "Redis is still loading after too many retries, last error: {err}, logfile: {:?}",
                            server.log_file_contents()
                        );
                    }
                }
                Err(err) => {
                    panic!(
                        "Failed to flush database: {err}, logfile: {:?}",
                        server.log_file_contents()
                    );
                }
            }
        }

        TestContext {
            server,
            client,
            protocol: use_protocol(),
        }
    }

    pub fn connection(&self) -> redis::Connection {
        self.client.get_connection().unwrap()
    }

    #[cfg(feature = "aio")]
    pub async fn async_connection(&self) -> RedisResult<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
    }

    #[cfg(feature = "aio")]
    pub async fn async_pubsub(&self) -> RedisResult<redis::aio::PubSub> {
        self.client.get_async_pubsub().await
    }

    pub fn stop_server(&mut self) {
        self.server.stop();
    }

    #[cfg(feature = "tokio-comp")]
    pub async fn multiplexed_async_connection_tokio(
        &self,
    ) -> RedisResult<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
    }

    #[cfg(all(feature = "aio", feature = "cache-aio"))]
    pub fn async_connection_with_cache(
        &self,
    ) -> impl Future<Output = redis::RedisResult<redis::aio::MultiplexedConnection>> {
        self.async_connection_with_cache_config(CacheConfig::default())
    }

    #[cfg(all(feature = "aio", feature = "cache-aio"))]
    pub fn async_connection_with_cache_config(
        &self,
        cache_config: CacheConfig,
    ) -> impl Future<Output = redis::RedisResult<redis::aio::MultiplexedConnection>> {
        use redis::AsyncConnectionConfig;

        let client = self.client.clone();
        async move {
            client
                .get_multiplexed_async_connection_with_config(
                    &AsyncConnectionConfig::new().set_cache_config(cache_config),
                )
                .await
        }
    }
}

impl TestContextVersioning for TestContext {
    fn get_available_components(&self) -> AvailableComponents {
        let mut conn = self.connection();
        AvailableComponents::from(&mut conn)
    }
}

fn encode_iter<W>(values: &[Value], writer: &mut W, prefix: &str) -> io::Result<()>
where
    W: io::Write,
{
    write!(writer, "{}{}\r\n", prefix, values.len()).unwrap();
    for val in values.iter() {
        encode_value(val, writer).unwrap();
    }
    Ok(())
}
fn encode_map<W>(values: &[(Value, Value)], writer: &mut W, prefix: &str) -> io::Result<()>
where
    W: io::Write,
{
    write!(writer, "{}{}\r\n", prefix, values.len()).unwrap();
    for (k, v) in values.iter() {
        encode_value(k, writer).unwrap();
        encode_value(v, writer).unwrap();
    }
    Ok(())
}
pub fn encode_value<W>(value: &Value, writer: &mut W) -> io::Result<()>
where
    W: io::Write,
{
    #![allow(clippy::write_with_newline)]
    match *value {
        Value::Nil => write!(writer, "$-1\r\n"),
        Value::Int(val) => write!(writer, ":{val}\r\n"),
        Value::BulkString(ref val) => {
            write!(writer, "${}\r\n", val.len()).unwrap();
            writer.write_all(val).unwrap();
            writer.write_all(b"\r\n")
        }
        Value::Array(ref values) => encode_iter(values, writer, "*"),
        Value::Okay => write!(writer, "+OK\r\n"),
        Value::SimpleString(ref s) => write!(writer, "+{s}\r\n"),
        Value::Map(ref values) => encode_map(values, writer, "%"),
        Value::Attribute {
            ref data,
            ref attributes,
        } => {
            encode_map(attributes, writer, "|").unwrap();
            encode_value(data, writer).unwrap();
            Ok(())
        }
        Value::Set(ref values) => encode_iter(values, writer, "~"),
        Value::Double(val) => write!(writer, ",{val}\r\n"),
        Value::Boolean(v) => {
            if v {
                write!(writer, "#t\r\n")
            } else {
                write!(writer, "#f\r\n")
            }
        }
        Value::VerbatimString {
            ref format,
            ref text,
        } => {
            // format is always 3 bytes
            write!(writer, "={}\r\n{}:{}\r\n", 3 + text.len(), format, text)
        }
        Value::BigNumber(ref val) => {
            #[cfg(feature = "num-bigint")]
            return write!(writer, "({val}\r\n");
            #[cfg(not(feature = "num-bigint"))]
            {
                write!(writer, "(").unwrap();
                for byte in val {
                    write!(writer, "{byte}").unwrap();
                }
                write!(writer, "\r\n")
            }
        }
        Value::Push { ref kind, ref data } => {
            write!(writer, ">{}\r\n+{kind}\r\n", data.len() + 1).unwrap();
            for val in data.iter() {
                encode_value(val, writer).unwrap();
            }
            Ok(())
        }
        Value::ServerError(ref err) => match err.details() {
            Some(details) => write!(writer, "-{} {details}\r\n", err.code()),
            None => write!(writer, "-{}\r\n", err.code()),
        },
        _ => panic!("unknown value {value:?}"),
    }
}

#[cfg(feature = "tls-rustls")]
fn load_certs_from_file(tls_file_paths: &TlsFilePaths) -> TlsCertificates {
    let ca_file = File::open(&tls_file_paths.ca_crt).expect("Cannot open CA cert file");
    let mut root_cert_vec = Vec::new();
    BufReader::new(ca_file)
        .read_to_end(&mut root_cert_vec)
        .expect("Unable to read CA cert file");

    let cert_file = File::open(&tls_file_paths.redis_crt).expect("Cannot open cert file");
    let mut client_cert_vec = Vec::new();
    BufReader::new(cert_file)
        .read_to_end(&mut client_cert_vec)
        .expect("Unable to read cert file");

    let key_file = File::open(&tls_file_paths.redis_key).expect("Cannot open key file");
    let mut client_key_vec = Vec::new();
    BufReader::new(key_file)
        .read_to_end(&mut client_key_vec)
        .expect("Unable to read key file");

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
) -> RedisResult<redis::Client> {
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
pub(crate) fn build_single_client_with_separate_client_cert<T: redis::IntoConnectionInfo>(
    connection_info: T,
    tls_file_params: &TlsFilePaths,
    client_cert_paths: &redis_test::utils::ClientCertPaths,
) -> RedisResult<redis::Client> {
    // Load CA cert for server verification
    let ca_file = File::open(&tls_file_params.ca_crt).expect("Cannot open CA cert file");
    let mut root_cert_vec = Vec::new();
    BufReader::new(ca_file)
        .read_to_end(&mut root_cert_vec)
        .expect("Unable to read CA cert file");

    // Load client cert and key for mTLS authentication
    let cert_file =
        File::open(&client_cert_paths.client_crt).expect("Cannot open client cert file");
    let mut client_cert_vec = Vec::new();
    BufReader::new(cert_file)
        .read_to_end(&mut client_cert_vec)
        .expect("Unable to read client cert file");

    let key_file = File::open(&client_cert_paths.client_key).expect("Cannot open client key file");
    let mut client_key_vec = Vec::new();
    BufReader::new(key_file)
        .read_to_end(&mut client_key_vec)
        .expect("Unable to read client key file");

    redis::Client::build_with_tls(
        connection_info,
        TlsCertificates {
            client_tls: Some(ClientTlsConfig {
                client_cert: client_cert_vec,
                client_key: client_key_vec,
            }),
            root_cert: Some(root_cert_vec),
        },
    )
}

#[cfg(not(feature = "tls-rustls"))]
pub(crate) fn build_single_client<T: redis::IntoConnectionInfo>(
    connection_info: T,
    _tls_file_params: &Option<TlsFilePaths>,
    _mtls_enabled: bool,
) -> RedisResult<redis::Client> {
    redis::Client::open(connection_info)
}

#[cfg(feature = "tls-rustls")]
pub(crate) mod mtls_test {
    use super::*;
    use redis::{ConnectionInfo, IntoConnectionInfo, RedisError, cluster::ClusterClient};

    fn clean_node_info(nodes: &[ConnectionInfo]) -> Vec<ConnectionInfo> {
        nodes
            .iter()
            .map(|node| match node.addr() {
                redis::ConnectionAddr::TcpTls { host, port, .. } => redis::ConnectionAddr::TcpTls {
                    host: host.to_owned(),
                    port: *port,
                    insecure: false,
                    tls_params: None,
                }
                .into_connection_info()
                .unwrap(),
                _ => node.clone(),
            })
            .collect()
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

pub fn build_simple_pipeline_for_invalidation() -> Pipeline {
    let mut pipe = redis::pipe();
    pipe.cmd("GET")
        .arg("key_1")
        .ignore()
        .cmd("SET")
        .arg("key_1")
        .arg(42)
        .ignore();
    pipe
}

#[cfg(feature = "aio")]
pub async fn kill_client_async(
    conn_to_kill: &mut impl aio::ConnectionLike,
    client: &redis::Client,
) -> RedisResult<()> {
    let info: String = cmd("CLIENT")
        .arg("INFO")
        .query_async(conn_to_kill)
        .await
        .unwrap();
    let id = info.split_once(' ').unwrap().0;
    assert!(id.contains("id="));
    let client_to_kill_id = id.split_once("id=").unwrap().1;

    let mut killer_conn = client.get_multiplexed_async_connection().await.unwrap();
    let () = cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(client_to_kill_id)
        .query_async(&mut killer_conn)
        .await
        .unwrap();

    Ok(())
}

pub fn spawn<T>(fut: impl std::future::Future<Output = T> + Send + Sync + 'static)
where
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(tokio_runtime) => {
            tokio_runtime.spawn(fut);
        }
        Err(_) => {
            #[cfg(feature = "smol-comp")]
            smol::spawn(fut).detach();
            #[cfg(not(feature = "smol-comp"))]
            unreachable!()
        }
    }
}
