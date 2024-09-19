use redis::sentinel::SentinelNodeConnectionInfo;
use redis::ConnectionAddr;
use redis::ConnectionInfo;
use redis::TlsMode;
use redis_test::sentinel::{wait_for_master_server, wait_for_replica, RedisSentinelCluster};
use redis_test::server::RedisServer;

use crate::support::start_tls_crypto_provider;

const MTLS_NOT_ENABLED: bool = false;

pub struct TestSentinelContext {
    pub cluster: RedisSentinelCluster,
    pub sentinel: redis::sentinel::Sentinel,
    pub sentinels_connection_info: Vec<ConnectionInfo>,
    mtls_enabled: bool, // for future tests
}

impl TestSentinelContext {
    pub fn new(nodes: u16, replicas: u16, sentinels: u16) -> TestSentinelContext {
        Self::new_with_cluster_client_builder(nodes, replicas, sentinels)
    }

    pub fn new_with_cluster_client_builder(
        nodes: u16,
        replicas: u16,
        sentinels: u16,
    ) -> TestSentinelContext {
        start_tls_crypto_provider();
        let cluster = RedisSentinelCluster::new(nodes, replicas, sentinels);
        let initial_nodes: Vec<ConnectionInfo> = cluster
            .iter_sentinel_servers()
            .map(RedisServer::connection_info)
            .collect();
        let sentinel = redis::sentinel::Sentinel::build(initial_nodes.clone());
        let sentinel = sentinel.unwrap();

        let mut context = TestSentinelContext {
            cluster,
            sentinel,
            sentinels_connection_info: initial_nodes,
            mtls_enabled: MTLS_NOT_ENABLED,
        };
        context.wait_for_cluster_up();
        context
    }

    pub fn sentinel(&self) -> &redis::sentinel::Sentinel {
        &self.sentinel
    }

    pub fn sentinel_mut(&mut self) -> &mut redis::sentinel::Sentinel {
        &mut self.sentinel
    }

    pub fn sentinels_connection_info(&self) -> &Vec<ConnectionInfo> {
        &self.sentinels_connection_info
    }

    pub fn sentinel_node_connection_info(&self) -> SentinelNodeConnectionInfo {
        SentinelNodeConnectionInfo {
            tls_mode: if let ConnectionAddr::TcpTls { insecure, .. } =
                self.cluster.servers[0].client_addr()
            {
                if *insecure {
                    Some(TlsMode::Insecure)
                } else {
                    Some(TlsMode::Secure)
                }
            } else {
                None
            },
            redis_connection_info: None,
            certs: None,
        }
    }

    pub fn wait_for_cluster_up(&mut self) {
        let node_conn_info = self.sentinel_node_connection_info();
        let con = self.sentinel_mut();

        let r = wait_for_master_server(|| con.master_for("master1", Some(&node_conn_info)));
        if r.is_err() {
            panic!("failed waiting for sentinel master1 to be ready");
        }

        let r = wait_for_replica(|| con.replica_for("master1", Some(&node_conn_info)));
        if r.is_err() {
            panic!("failed waiting for sentinel master1 replica to be ready");
        }
    }
}
