#![cfg(feature = "cluster")]
#![allow(dead_code)]

use std::convert::identity;
use std::thread::sleep;
use std::time::Duration;

use crate::support::{AvailableComponents, build_single_client, start_tls_crypto_provider};
use redis::Connection;
use redis::ConnectionInfo;
use redis::ErrorKind;
use redis::ProtocolVersion;
use redis::RedisResult;
use redis::Value;
#[cfg(feature = "cluster-async")]
use redis::aio::ConnectionLike;
#[cfg(feature = "cluster-async")]
use redis::cluster_async::Connect;
#[cfg(feature = "tls-rustls")]
use redis_test::cluster::ClusterType;
use redis_test::cluster::{RedisCluster, RedisClusterConfiguration};
use redis_test::server::{RedisServer, use_protocol};

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
    pub fn new() -> Self {
        Self::new_with_config(RedisClusterConfiguration {
            tls_insecure: false,
            ..Default::default()
        })
    }

    pub fn new_with_mtls() -> Self {
        Self::new_with_config_and_builder(
            RedisClusterConfiguration {
                mtls_enabled: true,
                tls_insecure: false,
                ..Default::default()
            },
            identity,
        )
    }

    pub fn new_without_ip_alts() -> Self {
        Self::new_with_config_and_builder(
            RedisClusterConfiguration {
                tls_insecure: false,
                certs_with_ip_alts: false,
                ..Default::default()
            },
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
            RedisClusterConfiguration {
                tls_insecure: false,
                ..Default::default()
            },
            initializer,
        )
    }

    pub fn new_insecure_with_cluster_client_builder<F>(initializer: F) -> Self
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        Self::new_with_config_and_builder(RedisClusterConfiguration::default(), initializer)
    }

    pub fn new_with_config_and_builder<F>(
        cluster_config: RedisClusterConfiguration,
        initializer: F,
    ) -> Self
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
        if (mtls_enabled || (ClusterType::get_intended() == ClusterType::TcpTls && !tls_insecure))
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
            protocol: use_protocol(),
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
        if (self.mtls_enabled || ClusterType::get_intended() == ClusterType::TcpTls)
            && let Some(tls_file_paths) = &self.cluster.tls_paths
        {
            builder = builder.certs(load_certs_from_file(tls_file_paths));
        }

        initializer(builder).build().unwrap()
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
                let err = redis::cmd("PING").exec(&mut con).unwrap_err();
                assert_eq!(err.kind(), ErrorKind::Extension);
                assert_eq!(err.code(), Some("NOAUTH"));
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

    /// Returns connections to the primary that owns `key`'s slot and to another primary, and the
    /// slot.
    fn key_slot_owner_and_other_primary(&self, key: &str) -> (Connection, Connection, u16) {
        let mut primaries: Vec<Connection> = self
            .cluster
            .iter_servers()
            .map(|server| self.build_single_client_connection(server).unwrap())
            .filter_map(|mut con| {
                let role: Vec<Value> = redis::cmd("ROLE").query(&mut con).unwrap();
                (role.first() == Some(&Value::BulkString(b"master".to_vec()))).then_some(con)
            })
            .collect();
        let owner = primaries
            .iter_mut()
            .position(|con| redis::cmd("EXISTS").arg(key).query::<i64>(con).is_ok())
            .unwrap();
        let mut owner = primaries.swap_remove(owner);
        let other = primaries.pop().unwrap();
        let slot: u16 = redis::cmd("CLUSTER")
            .arg("KEYSLOT")
            .arg(key)
            .query(&mut owner)
            .unwrap();
        (owner, other, slot)
    }

    fn node_id(con: &mut Connection) -> String {
        redis::cmd("CLUSTER").arg("MYID").query(con).unwrap()
    }

    /// Moves `key`'s (empty) slot to another primary, without the client noticing.
    /// Requests for the slot sent to the old owner are answered with `MOVED`.
    pub fn move_key_slot(&self, key: &str) {
        let (mut old_owner, mut new_owner, slot) = self.key_slot_owner_and_other_primary(key);
        let new_owner_id = Self::node_id(&mut new_owner);
        for con in [&mut new_owner, &mut old_owner] {
            redis::cmd("CLUSTER")
                .arg("SETSLOT")
                .arg(slot)
                .arg("NODE")
                .arg(&new_owner_id)
                .exec(con)
                .unwrap();
        }
    }

    /// Starts migrating `key`'s (empty) slot to another primary. Requests for missing keys in the
    /// slot sent to the owner are answered with `ASK`.
    pub fn start_key_slot_migration(&self, key: &str) {
        let (mut owner, mut target, slot) = self.key_slot_owner_and_other_primary(key);
        let owner_id = Self::node_id(&mut owner);
        let target_id = Self::node_id(&mut target);
        redis::cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(slot)
            .arg("IMPORTING")
            .arg(&owner_id)
            .exec(&mut target)
            .unwrap();
        redis::cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(slot)
            .arg("MIGRATING")
            .arg(&target_id)
            .exec(&mut owner)
            .unwrap();
    }
}

impl super::TestContextVersioning for TestClusterContext {
    fn get_available_components(&self) -> AvailableComponents {
        let server = self.cluster.servers.first().unwrap();
        let mut conn = self.build_single_client_connection(server).unwrap();

        AvailableComponents::from(&mut conn)
    }
}
