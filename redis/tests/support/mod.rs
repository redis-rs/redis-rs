#![allow(dead_code)]

#[cfg(feature = "aio")]
use futures::Future;
#[allow(
    unused_imports,
    reason = "`RedisResult` is used in at least 4 unrelated conditions. So instead of modelling all of them in an unreadable condition, we accept if the import is unused"
)]
use redis::RedisResult;
#[cfg(feature = "cache-aio")]
use redis::caching::CacheConfig;
#[cfg(feature = "tls-rustls")]
use redis::{ClientTlsConfig, TlsCertificates};
use redis::{Pipeline, Value};
#[cfg(feature = "aio")]
use redis::{aio, cmd};
#[allow(unused_imports)]
pub use redis_test::run_test_if_version_supported;
#[allow(unused_imports)]
pub use redis_test::skip_if_context_does_not_support;
#[cfg(feature = "tls-rustls")]
use redis_test::utils::TlsFilePaths;
#[allow(unused_imports)]
pub use redis_test::utils::build_single_client;
#[cfg(feature = "tls-rustls")]
pub use redis_test::utils::load_certs_from_file;
#[allow(unused_imports)]
pub use redis_test::utils::start_tls_crypto_provider;
#[allow(unused_imports)]
pub use redis_test::version::TestContextVersioning;
#[allow(unused_imports)]
pub use redis_test::version::{
    REDIS_BLOOM_ANY, REDIS_CE_6_0, REDIS_CE_7_0, REDIS_CE_7_2, REDIS_CE_7_4, REDIS_CE_8_0,
    REDIS_CE_8_2, REDIS_CE_8_4, REDIS_CE_8_6, REDIS_CE_8_8, REDIS_JSON_8_8, VALKEY_8_1, VALKEY_9_0,
    VALKEY_9_1,
};
#[allow(unused_imports)]
pub use redis_test::{TestContext, TestContextBuilder};

use std::io;
#[cfg(feature = "tls-rustls")]
use std::{
    fs::File,
    io::{BufReader, Read},
};

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

// With both `tokio-comp` and `smol-comp` enabled, the tests of both runtimes run
// in the same process. Setting a preferred runtime here would poison the process
// for the other runtime, so we rely on `Runtime::locate`'s auto-detection instead.
#[cfg(feature = "tokio-comp")]
fn block_on_all_using_tokio<F>(f: F) -> F::Output
where
    F: Future,
{
    current_thread_runtime().block_on(f)
}

#[cfg(feature = "smol-comp")]
fn block_on_all_using_smol<F>(f: F) -> F::Output
where
    F: Future,
{
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

/// Extension of [`TestContext`] taylored to the flags available directly on `redis-rs`.
pub trait TestContextExt {
    #[cfg(feature = "tokio-comp")]
    async fn multiplexed_async_connection_tokio(
        &self,
    ) -> RedisResult<redis::aio::MultiplexedConnection>;

    #[cfg(all(feature = "aio", feature = "cache-aio"))]
    fn async_connection_with_cache(
        &self,
    ) -> impl Future<Output = redis::RedisResult<redis::aio::MultiplexedConnection>>;

    #[cfg(all(feature = "aio", feature = "cache-aio"))]
    fn async_connection_with_cache_config(
        &self,
        cache_config: CacheConfig,
    ) -> impl Future<Output = redis::RedisResult<redis::aio::MultiplexedConnection>>;
}

impl TestContextExt for TestContext {
    #[cfg(feature = "tokio-comp")]
    async fn multiplexed_async_connection_tokio(
        &self,
    ) -> RedisResult<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
    }

    #[cfg(all(feature = "aio", feature = "cache-aio"))]
    fn async_connection_with_cache(
        &self,
    ) -> impl Future<Output = redis::RedisResult<redis::aio::MultiplexedConnection>> {
        self.async_connection_with_cache_config(CacheConfig::default())
    }

    #[cfg(all(feature = "aio", feature = "cache-aio"))]
    fn async_connection_with_cache_config(
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

    let client_tls_config = ClientTlsConfig::new(client_cert_vec, client_key_vec);
    redis::Client::build_with_tls(
        connection_info,
        TlsCertificates::new()
            .client_tls_config(client_tls_config)
            .root_cert(root_cert_vec),
    )
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
