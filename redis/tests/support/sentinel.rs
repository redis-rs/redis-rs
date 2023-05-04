use std::fs::File;
use std::io::Write;
use std::thread::sleep;
use std::time::Duration;

use redis::ConnectionInfo;
use tempfile::TempDir;

use super::Module;
use super::RedisServer;

const LOCALHOST: &str = "127.0.0.1";

pub struct RedisSentinelCluster {
    pub servers: Vec<RedisServer>,
    pub sentinel_servers: Vec<RedisServer>,
    pub folders: Vec<TempDir>,
}

fn spawn_master_server(port: u16, dir: &TempDir, modules: &[Module]) -> RedisServer {
    RedisServer::new_with_addr(
        redis::ConnectionAddr::Tcp("127.0.0.1".into(), port),
        None,
        None,
        modules,
        |cmd| {
            // Minimize startup delay
            cmd.arg("--repl-diskless-sync-delay").arg("0");
            // cmd.arg("--repl-ping-replica-period").arg("1");
            cmd.arg("--appendonly").arg("yes");
            cmd.current_dir(dir.path());
            cmd.spawn().unwrap()
        },
    )
}

fn spawn_replica_server(
    port: u16,
    master_port: u16,
    dir: &TempDir,
    modules: &[Module],
) -> RedisServer {
    let config_file_path = dir.path().join("redis_config.conf");
    File::create(&config_file_path).unwrap();

    RedisServer::new_with_addr(
        redis::ConnectionAddr::Tcp("127.0.0.1".into(), port),
        Some(&config_file_path),
        None,
        modules,
        |cmd| {
            cmd.arg("--replicaof")
                .arg("127.0.0.1")
                .arg(master_port.to_string());
            // Minimize startup delay
            // cmd.arg("--repl-diskless-sync-delay").arg("0");
            // cmd.arg("--repl-ping-replica-period").arg("1");
            cmd.arg("--appendonly").arg("yes");
            // cmd.stdout(std::process::Stdio::inherit())
            //     .stderr(std::process::Stdio::inherit());
            // cmd.arg("--loglevel").arg("debug");
            cmd.current_dir(dir.path());
            cmd.spawn().unwrap()
        },
    )
}

fn spawn_sentinel_server(
    port: u16,
    master_ports: &[u16],
    dir: &TempDir,
    modules: &[Module],
) -> RedisServer {
    let config_file_path = dir.path().join("redis_config.conf");
    let mut file = File::create(&config_file_path).unwrap();
    for (i, master_port) in master_ports.iter().enumerate() {
        file.write_all(
            format!("sentinel monitor master{} 127.0.0.1 {} 1\n", i, master_port).as_bytes(),
        )
        .unwrap();
        // file.write_all(format!("sentinel down-after-milliseconds master{} 200\n", i).as_bytes())
        //     .unwrap();
        // file.write_all(format!("sentinel parallel-syncs master{} 1\n", i).as_bytes())
        //     .unwrap();
        // file.write_all(format!("sentinel failover-timeout master{} 5000\n", i).as_bytes())
        //     .unwrap();
    }
    file.flush().unwrap();

    RedisServer::new_with_addr(
        redis::ConnectionAddr::Tcp("127.0.0.1".into(), port),
        Some(&config_file_path),
        None,
        modules,
        |cmd| {
            // cmd.stdout(std::process::Stdio::inherit())
            //     .stderr(std::process::Stdio::inherit());
            // cmd.arg("--loglevel").arg("debug");
            cmd.arg("--sentinel");
            cmd.arg("--appendonly").arg("yes");
            cmd.current_dir(dir.path());
            cmd.spawn().unwrap()
        },
    )
}

impl RedisSentinelCluster {
    pub fn new(masters: u16, replicas_per_master: u16, sentinels: u16) -> RedisSentinelCluster {
        RedisSentinelCluster::with_modules(masters, replicas_per_master, sentinels, &[])
    }

    pub fn with_modules(
        masters: u16,
        replicas_per_master: u16,
        sentinels: u16,
        modules: &[Module],
    ) -> RedisSentinelCluster {
        let mut servers = vec![];
        let mut folders = vec![];
        let mut master_ports = vec![];
        let start_port = 7000;

        for node in 0..masters {
            let port = start_port + (node * (replicas_per_master + 1));
            let tempdir = tempfile::Builder::new()
                .prefix("redis")
                .tempdir()
                .expect("failed to create tempdir");
            servers.push(spawn_master_server(port, &tempdir, modules));
            folders.push(tempdir);
            master_ports.push(port);

            for replica in 0..replicas_per_master {
                let replica_port = port + 1 + replica;
                let tempdir = tempfile::Builder::new()
                    .prefix("redis")
                    .tempdir()
                    .expect("failed to create tempdir");
                servers.push(spawn_replica_server(replica_port, port, &tempdir, modules));
                folders.push(tempdir);
            }
        }

        // Wait for replicas to sync so that the sentinels discover them on the first try
        sleep(Duration::from_millis(50));

        let mut sentinel_servers = vec![];
        let start_port = 27000;
        for node in 0..sentinels {
            let port = start_port + node;
            let tempdir = tempfile::Builder::new()
                .prefix("redis")
                .tempdir()
                .expect("failed to create tempdir");

            sentinel_servers.push(spawn_sentinel_server(
                port,
                &master_ports,
                &tempdir,
                modules,
            ));
            folders.push(tempdir);
        }

        RedisSentinelCluster {
            servers,
            sentinel_servers,
            folders,
        }
    }

    pub fn stop(&mut self) {
        for server in &mut self.servers {
            server.stop();
        }
        for server in &mut self.sentinel_servers {
            server.stop();
        }
    }

    pub fn iter_sentinel_servers(&self) -> impl Iterator<Item = &RedisServer> {
        self.sentinel_servers.iter()
    }
}

impl Drop for RedisSentinelCluster {
    fn drop(&mut self) {
        self.stop()
    }
}

pub struct TestSentinelContext {
    pub cluster: RedisSentinelCluster,
    pub sentinel: redis::sentinel::Sentinel,
    pub sentinels_connection_info: Vec<ConnectionInfo>,
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
        let cluster = RedisSentinelCluster::new(nodes, replicas, sentinels);
        let initial_nodes: Vec<ConnectionInfo> = cluster
            .iter_sentinel_servers()
            .map(RedisServer::connection_info)
            .collect();
        let sentinel = redis::sentinel::Sentinel::build(initial_nodes.clone());
        let sentinel = sentinel.unwrap();

        let context = TestSentinelContext {
            cluster,
            sentinel,
            sentinels_connection_info: initial_nodes,
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

    pub fn wait_for_cluster_up(&self) {
        let con = self.sentinel();
        let rolecmd = redis::cmd("ROLE");

        for _ in 0..100 {
            let master_client = con.master_for("master1");
            if let Ok(master_client) = master_client {
                let r: (String, i32, redis::Value) = rolecmd
                    .query(&mut master_client.get_connection().unwrap())
                    .unwrap();
                if r.0.starts_with("master") {
                    break;
                }
            }

            sleep(Duration::from_millis(25));
        }

        for _ in 0..200 {
            let replica_client = con.replica_for("master1");
            if let Ok(client) = replica_client {
                let r: (String, String, i32, String, i32) = rolecmd
                    .query(&mut client.get_connection().unwrap())
                    .unwrap();
                if r.0.starts_with("slave") {
                    return;
                }
            }

            sleep(Duration::from_millis(25));
        }

        panic!("failed waiting for sentinel cluster to be ready");
    }
}
