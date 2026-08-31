#![cfg(feature = "cluster")]
#![allow(dead_code)]

use std::convert::identity;
use std::thread::sleep;
use std::time::Duration;

use redis::Connection;
use redis::ConnectionInfo;
use redis::ProtocolVersion;
use redis::RedisResult;
#[cfg(feature = "cluster-async")]
use redis::aio::ConnectionLike;
#[cfg(feature = "cluster-async")]
use redis::cluster_async::Connect;
use redis_test::cluster::{RedisCluster, RedisClusterConfiguration};
use redis_test::server::{RedisServer, use_protocol};
use redis_test::utils::{build_single_client, start_tls_crypto_provider};
use redis_test::{AvailableComponents, TestContextVersioning};

#[cfg(feature = "tls-rustls")]
use redis_test::utils::load_certs_from_file;

pub struct TestClusterContext {
    pub cluster: RedisCluster,
    pub client: redis::cluster::ClusterClient,
    pub mtls_enabled: bool,
    pub nodes: Vec<ConnectionInfo>,
    pub protocol: ProtocolVersion,
}

impl TestClusterContext {
    pub fn new() -> Self {
        Self::new_with_config(RedisClusterConfiguration::default().insecure_tls())
    }

    pub fn new_with_mtls() -> Self {
        let cfg = RedisClusterConfiguration::default()
            .mtls_enabled()
            .insecure_tls();
        #[cfg(feature = "tls-rustls")]
        let cfg = cfg.cluster_type(redis_test::cluster::ClusterType::TcpTls);

        Self::new_with_config_and_builder(cfg, identity)
    }

    pub fn new_without_ip_alts() -> Self {
        Self::new_with_config_and_builder(
            RedisClusterConfiguration::default()
                .insecure_tls()
                .certs_without_ip_alts(),
            identity,
        )
    }

    pub fn new_with_config(cluster_config: RedisClusterConfiguration) -> Self {
        Self::new_with_config_and_builder(cluster_config, identity)
    }

    pub fn new_with_cluster_client_builder<F>(initializer: F) -> Self
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        Self::new_with_config_and_builder(
            RedisClusterConfiguration::default().insecure_tls(),
            initializer,
        )
    }

    pub fn new_insecure_with_cluster_client_builder<F>(initializer: F) -> Self
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        Self::new_with_config_and_builder(RedisClusterConfiguration::default(), initializer)
    }

    pub fn new_with_config_and_protocol(
        cluster_config: RedisClusterConfiguration,
        protocol: ProtocolVersion,
    ) -> Self {
        Self::new_with_config_and_builder_and_protocol(cluster_config, identity, protocol)
    }

    pub fn new_with_config_and_builder<F>(
        cluster_config: RedisClusterConfiguration,
        initializer: F,
    ) -> Self
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        Self::new_with_config_and_builder_and_protocol(
            cluster_config,
            initializer,
            use_protocol().unwrap_or(ProtocolVersion::RESP2),
        )
    }

    pub fn new_with_config_and_builder_and_protocol<F>(
        cluster_config: RedisClusterConfiguration,
        initializer: F,
        protocol: ProtocolVersion,
    ) -> Self
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        start_tls_crypto_provider();
        #[cfg(feature = "tls-rustls")]
        let _secure_tls = cluster_config.get_require_secure_tls();
        let mtls_enabled = cluster_config.get_mtls_enabled();
        let _cluster_type = cluster_config.get_cluster_type();
        let cluster = RedisCluster::new(cluster_config);
        let initial_nodes: Vec<ConnectionInfo> = cluster
            .iter_servers()
            .map(RedisServer::connection_info)
            .collect();
        let mut builder =
            redis::cluster::ClusterClientBuilder::new(initial_nodes.clone()).use_protocol(protocol);

        #[cfg(feature = "tls-rustls")]
        if (mtls_enabled || cluster.tls_paths.is_some())
            && let Some(tls_file_paths) = &cluster.tls_paths
        {
            builder = builder.certs(load_certs_from_file(tls_file_paths));
        }

        builder = initializer(builder);

        let client = builder.build().unwrap();

        Self {
            cluster,
            client,
            mtls_enabled,
            nodes: initial_nodes,
            protocol,
        }
    }

    /// Builds an additional cluster client against the same cluster.
    pub fn new_client_with_builder<F>(&self, initializer: F) -> redis::cluster::ClusterClient
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        #[allow(unused_mut)]
        let mut builder = redis::cluster::ClusterClientBuilder::new(self.nodes.clone())
            .use_protocol(self.protocol);

        #[cfg(feature = "tls-rustls")]
        if (self.mtls_enabled || self.cluster.tls_paths.is_some())
            && let Some(tls_file_paths) = &self.cluster.tls_paths
        {
            builder = builder.certs(load_certs_from_file(tls_file_paths));
        }

        initializer(builder).build().unwrap()
    }

    /// Returns a new context against the same (already-running) servers, but with
    /// the cluster client rebuilt through the given builder initializer.
    pub fn with_cluster_client_builder<F>(self, initializer: F) -> Self
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        let client = self.new_client_with_builder(initializer);
        Self {
            cluster: self.cluster,
            client,
            mtls_enabled: self.mtls_enabled,
            nodes: self.nodes,
            protocol: self.protocol,
        }
    }

    pub fn connection(&self) -> redis::cluster::ClusterConnection {
        self.client.get_connection().unwrap()
    }

    #[cfg(feature = "cluster-async")]
    pub async fn async_connection(&self) -> redis::cluster_async::ClusterConnection {
        self.client.get_async_connection().await.unwrap()
    }
    #[cfg(feature = "cluster-async")]
    pub async fn async_connection_with_config(
        &self,
        config: redis::cluster::ClusterConfig,
    ) -> redis::cluster_async::ClusterConnection {
        self.client
            .get_async_connection_with_config(config)
            .await
            .unwrap()
    }
    #[cfg(feature = "cluster-async")]
    pub async fn async_generic_connection<
        C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
    >(
        &self,
    ) -> redis::cluster_async::ClusterConnection<C> {
        self.client
            .get_async_generic_connection::<C>()
            .await
            .unwrap()
    }

    pub fn wait_for_cluster_up(&self) {
        let mut con = self.connection();
        let mut c = redis::cmd("CLUSTER");
        c.arg("INFO");

        for _ in 0..100 {
            let r: String = c.query::<String>(&mut con).unwrap();
            if r.starts_with("cluster_state:ok") {
                return;
            }

            sleep(Duration::from_millis(25));
        }

        panic!("failed waiting for cluster to be ready");
    }

    /// Gets a single direct connection to the given server
    ///
    /// # Arguments
    ///
    /// * `server` - The server to connect to
    pub fn build_single_client_connection(&self, server: &RedisServer) -> RedisResult<Connection> {
        let client = build_single_client(
            server.connection_info(),
            &self.cluster.tls_paths,
            self.mtls_enabled,
        )?;

        client.get_connection()
    }

    pub fn disable_default_user(&self) {
        for server in &self.cluster.servers {
            let mut con = self.build_single_client_connection(server).unwrap();
            redis::cmd("ACL")
                .arg("SETUSER")
                .arg("default")
                .arg("off")
                .exec(&mut con)
                .unwrap();

            // subsequent unauthenticated command should fail:
            if let Ok(mut con) = self.build_single_client_connection(server) {
                redis::cmd("PING").exec(&mut con).unwrap_err();
            }
        }
    }

    pub fn get_ports(&self) -> Vec<u16> {
        self.nodes
            .iter()
            .map(|info| match info.addr() {
                redis::ConnectionAddr::Tcp(_, port)
                | redis::ConnectionAddr::TcpTls { port, .. } => *port,
                _ => {
                    panic!("Unsupported address type for cluster tests")
                }
            })
            .collect()
    }
}

impl TestContextVersioning for TestClusterContext {
    fn get_available_components(&self) -> AvailableComponents {
        let server = self.cluster.servers.first().unwrap();
        let mut conn = self.build_single_client_connection(server).unwrap();

        AvailableComponents::from(&mut conn)
    }
}
