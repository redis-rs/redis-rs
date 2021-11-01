#![cfg(feature = "cluster")]
#![allow(dead_code)]

use std::convert::identity;
use std::env;
use std::process;
use std::thread::sleep;
use std::time::Duration;

use tempfile::TempDir;

use crate::support::build_keys_and_certs_for_tls;

use super::RedisServer;

const LOCALHOST: &str = "127.0.0.1";

enum ClusterType {
    Tcp,
    TcpTls,
}

impl ClusterType {
    fn get_intended() -> ClusterType {
        match env::var("REDISRS_SERVER_TYPE")
            .ok()
            .as_ref()
            .map(|x| &x[..])
        {
            Some("tcp") => ClusterType::Tcp,
            Some("tcp+tls") => ClusterType::TcpTls,
            val => {
                panic!("Unknown server type {:?}", val);
            }
        }
    }

    fn build_addr(port: u16) -> redis::ConnectionAddr {
        match ClusterType::get_intended() {
            ClusterType::Tcp => redis::ConnectionAddr::Tcp("127.0.0.1".into(), port),
            ClusterType::TcpTls => redis::ConnectionAddr::TcpTls {
                host: "127.0.0.1".into(),
                port,
                insecure: true,
            },
        }
    }
}

pub struct RedisCluster {
    pub servers: Vec<RedisServer>,
    pub folders: Vec<TempDir>,
}

impl RedisCluster {
    pub fn new(nodes: u16, replicas: u16) -> RedisCluster {
        let mut servers = vec![];
        let mut folders = vec![];
        let mut addrs = vec![];
        let start_port = 7000;
        let mut tls_paths = None;
        let mut is_tls = false;

        if let ClusterType::TcpTls = ClusterType::get_intended() {
            // Create a shared set of keys in cluster mode
            let tempdir = tempfile::Builder::new()
                .prefix("redis")
                .tempdir()
                .expect("failed to create tempdir");
            let files = build_keys_and_certs_for_tls(&tempdir);
            folders.push(tempdir);
            tls_paths = Some(files);
            is_tls = true;
        }

        for node in 0..nodes {
            let port = start_port + node;

            servers.push(RedisServer::new_with_addr(
                ClusterType::build_addr(port),
                tls_paths.clone(),
                |cmd| {
                    let tempdir = tempfile::Builder::new()
                        .prefix("redis")
                        .tempdir()
                        .expect("failed to create tempdir");
                    cmd.arg("--cluster-enabled")
                        .arg("yes")
                        .arg("--cluster-config-file")
                        .arg(&tempdir.path().join("nodes.conf"))
                        .arg("--cluster-node-timeout")
                        .arg("5000")
                        .arg("--appendonly")
                        .arg("yes");
                    if is_tls {
                        cmd.arg("--tls-cluster").arg("yes");
                        if replicas > 0 {
                            cmd.arg("--tls-replication").arg("yes");
                        }
                    }
                    cmd.current_dir(&tempdir.path());
                    folders.push(tempdir);
                    addrs.push(format!("127.0.0.1:{}", port));
                    dbg!(&cmd);
                    cmd.spawn().unwrap()
                },
            ));
        }

        sleep(Duration::from_millis(100));

        let mut cmd = process::Command::new("redis-cli");
        cmd.stdout(process::Stdio::null())
            .arg("--cluster")
            .arg("create")
            .args(&addrs);
        if replicas > 0 {
            cmd.arg("--cluster-replicas").arg(replicas.to_string());
        }
        cmd.arg("--cluster-yes");
        if is_tls {
            cmd.arg("--tls").arg("--insecure");
        }
        let status = dbg!(cmd).status().unwrap();
        assert!(status.success());

        let cluster = RedisCluster { servers, folders };
        if replicas > 0 {
            cluster.wait_for_replicas(replicas);
        }
        cluster
    }

    fn wait_for_replicas(&self, replicas: u16) {
        'server: for server in &self.servers {
            let conn_info = redis::ConnectionInfo {
                addr: server.get_client_addr().clone(),
                redis: Default::default(),
            };
            eprintln!(
                "waiting until {:?} knows required number of replicas",
                conn_info.addr
            );
            let client = redis::Client::open(conn_info).unwrap();
            let mut con = client.get_connection().unwrap();

            // retry 100 times
            for _ in 1..100 {
                let value = redis::cmd("CLUSTER").arg("SLOTS").query(&mut con).unwrap();
                let slots: Vec<Vec<redis::Value>> = redis::from_redis_value(&value).unwrap();

                // all slots should have following items:
                // [start slot range, end slot range, master's IP, replica1's IP, replica2's IP,... ]
                if slots.iter().all(|slot| slot.len() >= 3 + replicas as usize) {
                    continue 'server;
                }

                sleep(Duration::from_millis(100));
            }

            panic!("failed to create enough replicas");
        }
    }

    pub fn stop(&mut self) {
        for server in &mut self.servers {
            server.stop();
        }
    }

    pub fn iter_servers(&self) -> impl Iterator<Item = &RedisServer> {
        self.servers.iter()
    }
}

impl Drop for RedisCluster {
    fn drop(&mut self) {
        self.stop()
    }
}

pub struct TestClusterContext {
    pub cluster: RedisCluster,
    pub client: redis::cluster::ClusterClient,
}

impl TestClusterContext {
    pub fn new(nodes: u16, replicas: u16) -> TestClusterContext {
        Self::new_with_cluster_client_builder(nodes, replicas, identity)
    }

    pub fn new_with_cluster_client_builder<F>(
        nodes: u16,
        replicas: u16,
        initializer: F,
    ) -> TestClusterContext
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        let cluster = RedisCluster::new(nodes, replicas);
        let mut builder = redis::cluster::ClusterClientBuilder::new(
            cluster
                .iter_servers()
                .map(|server| redis::ConnectionInfo {
                    addr: server.get_client_addr().clone(),
                    redis: Default::default(),
                })
                .collect(),
        );
        builder = initializer(builder);
        let client = builder.open().unwrap();
        TestClusterContext { cluster, client }
    }

    pub fn connection(&self) -> redis::cluster::ClusterConnection {
        self.client.get_connection().unwrap()
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
}
