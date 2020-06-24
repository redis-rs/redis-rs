#![cfg(feature = "cluster")]
#![allow(dead_code)]

use std::fs;
use std::process;
use std::thread::sleep;
use std::time::Duration;

use std::path::PathBuf;

use super::RedisServer;

pub struct RedisCluster {
    pub servers: Vec<RedisServer>,
    pub folders: Vec<PathBuf>,
}

impl RedisCluster {
    pub fn new(nodes: u16, replicas: u16) -> RedisCluster {
        let mut servers = vec![];
        let mut folders = vec![];
        let mut addrs = vec![];
        let start_port = 7000;
        for node in 0..nodes {
            let port = start_port + node;

            servers.push(RedisServer::new_with_addr(
                redis::ConnectionAddr::Tcp("127.0.0.1".into(), port),
                |cmd| {
                    let (a, b) = rand::random::<(u64, u64)>();
                    let path = PathBuf::from(format!("/tmp/redis-rs-cluster-test-{}-{}-dir", a, b));
                    fs::create_dir_all(&path).unwrap();
                    cmd.arg("--cluster-enabled")
                        .arg("yes")
                        .arg("--cluster-config-file")
                        .arg(&path.join("nodes.conf"))
                        .arg("--cluster-node-timeout")
                        .arg("5000")
                        .arg("--appendonly")
                        .arg("yes");
                    cmd.current_dir(&path);
                    folders.push(path);
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
        let status = dbg!(cmd).status().unwrap();
        assert!(status.success());

        RedisCluster { servers, folders }
    }

    pub fn stop(&mut self) {
        for server in &mut self.servers {
            server.stop();
        }
        for folder in &self.folders {
            fs::remove_dir_all(&folder).unwrap();
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
        let cluster = RedisCluster::new(nodes, replicas);
        let client = redis::cluster::ClusterClient::open(
            cluster
                .iter_servers()
                .map(|x| format!("redis://{}/", x.get_client_addr()))
                .collect(),
        )
        .unwrap();
        TestClusterContext { cluster, client }
    }

    pub fn connection(&self) -> redis::cluster::ClusterConnection {
        self.client.get_connection().unwrap()
    }
}
