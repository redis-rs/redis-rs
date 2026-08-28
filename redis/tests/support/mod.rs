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
use redis_test::TestContext;
#[cfg(feature = "tls-rustls")]
use redis_test::utils::TlsFilePaths;
#[cfg(feature = "tls-rustls")]
use redis_test::utils::load_certs_from_file;

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
pub mod shared;

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
