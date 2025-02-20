#![cfg(feature = "cluster")]
#![allow(dead_code)]

use std::convert::identity;
use std::thread::sleep;
use std::time::Duration;

use crate::support::{build_single_client, start_tls_crypto_provider};
#[cfg(feature = "cluster-async")]
use redis::aio::ConnectionLike;
#[cfg(feature = "cluster-async")]
use redis::cluster_async::Connect;
use redis::ConnectionInfo;
use redis::ProtocolVersion;
#[cfg(feature = "tls-rustls")]
use redis_test::cluster::ClusterType;
use redis_test::cluster::{RedisCluster, RedisClusterConfiguration};
use redis_test::server::{use_protocol, RedisServer};

#[cfg(feature = "tls-rustls")]
use super::load_certs_from_file;

pub struct TestClusterContext {
    pub cluster: RedisCluster,
    pub client: redis::cluster::ClusterClient,
    pub mtls_enabled: bool,
    pub nodes: Vec<ConnectionInfo>,
    pub protocol: ProtocolVersion,
}

impl TestClusterContext {
    pub fn new() -> TestClusterContext {
        Self::new_with_config(RedisClusterConfiguration {
            tls_insecure: false,
            ..Default::default()
        })
    }

    pub fn new_with_mtls() -> TestClusterContext {
        Self::new_with_config_and_builder(
            RedisClusterConfiguration {
                mtls_enabled: true,
                tls_insecure: false,
                ..Default::default()
            },
            identity,
        )
    }

    pub fn new_without_ip_alts() -> TestClusterContext {
        Self::new_with_config_and_builder(
            RedisClusterConfiguration {
                tls_insecure: false,
                certs_with_ip_alts: false,
                ..Default::default()
            },
            identity,
        )
    }

    pub fn new_with_config(cluster_config: RedisClusterConfiguration) -> TestClusterContext {
        Self::new_with_config_and_builder(cluster_config, identity)
    }

    pub fn new_with_cluster_client_builder<F>(initializer: F) -> TestClusterContext
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        Self::new_with_config_and_builder(
            RedisClusterConfiguration {
                tls_insecure: false,
                ..Default::default()
            },
            initializer,
        )
    }

    pub fn new_insecure_with_cluster_client_builder<F>(initializer: F) -> TestClusterContext
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        Self::new_with_config_and_builder(RedisClusterConfiguration::default(), initializer)
    }

    pub fn new_with_config_and_builder<F>(
        cluster_config: RedisClusterConfiguration,
        initializer: F,
    ) -> TestClusterContext
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        start_tls_crypto_provider();
        #[cfg(feature = "tls-rustls")]
        let tls_insecure = cluster_config.tls_insecure;
        let mtls_enabled = cluster_config.mtls_enabled;
        let cluster = RedisCluster::new(cluster_config);
        let initial_nodes: Vec<ConnectionInfo> = cluster
            .iter_servers()
            .map(RedisServer::connection_info)
            .collect();
        let mut builder = redis::cluster::ClusterClientBuilder::new(initial_nodes.clone())
            .use_protocol(use_protocol());

        #[cfg(feature = "tls-rustls")]
        if mtls_enabled || (ClusterType::get_intended() == ClusterType::TcpTls && !tls_insecure) {
            if let Some(tls_file_paths) = &cluster.tls_paths {
                builder = builder.certs(load_certs_from_file(tls_file_paths));
            }
        }

        builder = initializer(builder);

        let client = builder.build().unwrap();

        TestClusterContext {
            cluster,
            client,
            mtls_enabled,
            nodes: initial_nodes,
            protocol: use_protocol(),
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

    pub fn disable_default_user(&self) {
        for server in &self.cluster.servers {
            let client = build_single_client(
                server.connection_info(),
                &self.cluster.tls_paths,
                self.mtls_enabled,
            )
            .unwrap();

            let mut con = client.get_connection().unwrap();
            redis::cmd("ACL")
                .arg("SETUSER")
                .arg("default")
                .arg("off")
                .exec(&mut con)
                .unwrap();

            // subsequent unauthenticated command should fail:
            if let Ok(mut con) = client.get_connection() {
                assert!(redis::cmd("PING").exec(&mut con).is_err());
            }
        }
    }

    pub fn get_version(&self) -> super::Version {
        let mut conn = self.connection();
        super::get_version(&mut conn)
    }
}
