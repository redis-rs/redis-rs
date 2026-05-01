use std::{fs::File, io::Write, thread::sleep, time::Duration};

use redis::{Client, ConnectionAddr, FromRedisValue, RedisResult};
use tempfile::TempDir;

use crate::{
    server::{Module, RedisServer},
    utils::{TlsFilePaths, build_keys_and_certs_for_tls, get_random_available_port},
};

pub struct RedisSentinelCluster {
    pub servers: Vec<RedisServer>,
    pub sentinel_servers: Vec<RedisServer>,
    pub folders: Vec<TempDir>,
}

impl RedisSentinelCluster {
    pub fn log_sentinel_state_via_cli(&self, master_name: &str) {
        use std::process::Command;

        if let Some(sentinel) = self.sentinel_servers.first() {
            if let Some((_, port)) = sentinel.host_and_port() {
                println!("\n=== Querying sentinel state via redis-cli ===");

                let output = Command::new("redis-cli")
                    .args(["-p", &port.to_string(), "SENTINEL", "MASTERS"])
                    .output();

                match output {
                    Ok(result) => {
                        println!("SENTINEL MASTERS output:");
                        println!("{}", String::from_utf8_lossy(&result.stdout));
                        if !result.stderr.is_empty() {
                            println!("stderr: {}", String::from_utf8_lossy(&result.stderr));
                        }
                    }
                    Err(e) => println!("Failed to execute redis-cli SENTINEL MASTERS: {}", e),
                }

                let output = Command::new("redis-cli")
                    .args(["-p", &port.to_string(), "SENTINEL", "SLAVES", master_name])
                    .output();

                match output {
                    Ok(result) => {
                        println!("\nSENTINEL SLAVES {} output:", master_name);
                        println!("{}", String::from_utf8_lossy(&result.stdout));
                        if !result.stderr.is_empty() {
                            println!("stderr: {}", String::from_utf8_lossy(&result.stderr));
                        }
                    }
                    Err(e) => println!("Failed to execute redis-cli SENTINEL SLAVES: {}", e),
                }

                let output = Command::new("redis-cli")
                    .args([
                        "-p",
                        &port.to_string(),
                        "SENTINEL",
                        "GET-MASTER-ADDR-BY-NAME",
                        master_name,
                    ])
                    .output();

                match output {
                    Ok(result) => {
                        println!("\nSENTINEL GET-MASTER-ADDR-BY-NAME {} output:", master_name);
                        println!("{}", String::from_utf8_lossy(&result.stdout));
                        if !result.stderr.is_empty() {
                            println!("stderr: {}", String::from_utf8_lossy(&result.stderr));
                        }
                    }
                    Err(e) => println!(
                        "Failed to execute redis-cli SENTINEL GET-MASTER-ADDR-BY-NAME: {}",
                        e
                    ),
                }

                println!("=== End sentinel state ===\n");
            }
        }
    }

    pub fn log_redis_state_via_cli(&self, port: u16) {
        use std::process::Command;

        let output = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "ROLE"])
            .output();

        match output {
            Ok(result) => {
                println!(
                    "Redis 127.0.0.1:{port} ROLE output: {}",
                    String::from_utf8_lossy(&result.stdout)
                );
                if !result.stderr.is_empty() {
                    println!("stderr: {}", String::from_utf8_lossy(&result.stderr));
                }
            }
            Err(e) => println!("Failed to execute redis-cli ROLE on port {}: {}", port, e),
        }
    }
}

const MTLS_NOT_ENABLED: bool = false;

fn get_addr(port: u16) -> ConnectionAddr {
    let addr = RedisServer::get_addr(port);
    if let ConnectionAddr::Unix(_) = addr {
        ConnectionAddr::Tcp(String::from("127.0.0.1"), port)
    } else {
        addr
    }
}

fn spawn_master_server(
    port: u16,
    dir: &TempDir,
    tlspaths: &TlsFilePaths,
    modules: &[Module],
) -> RedisServer {
    RedisServer::new_with_addr_tls_modules_and_spawner(
        get_addr(port),
        None,
        Some(tlspaths.clone()),
        MTLS_NOT_ENABLED,
        modules,
        |cmd| {
            // Minimize startup delay
            cmd.arg("--repl-diskless-sync-delay").arg("0");
            if let ConnectionAddr::TcpTls { .. } = get_addr(port) {
                cmd.arg("--tls-replication").arg("yes");
            }
            cmd.current_dir(dir.path());
            cmd.spawn().unwrap()
        },
    )
}

fn spawn_replica_server(
    port: u16,
    master_port: u16,
    dir: &TempDir,
    tlspaths: &TlsFilePaths,
    modules: &[Module],
) -> RedisServer {
    let config_file_path = dir.path().join("redis_config.conf");
    File::create(&config_file_path).unwrap();

    RedisServer::new_with_addr_tls_modules_and_spawner(
        get_addr(port),
        Some(&config_file_path),
        Some(tlspaths.clone()),
        MTLS_NOT_ENABLED,
        modules,
        |cmd| {
            cmd.arg("--replicaof")
                .arg("127.0.0.1")
                .arg(master_port.to_string());
            if let ConnectionAddr::TcpTls { .. } = get_addr(port) {
                cmd.arg("--tls-replication").arg("yes");
            }
            cmd.current_dir(dir.path());
            cmd.spawn().unwrap()
        },
    )
}

fn spawn_sentinel_server(
    port: u16,
    master_ports: &[u16],
    dir: &TempDir,
    tlspaths: &TlsFilePaths,
    modules: &[Module],
) -> RedisServer {
    let config_file_path = dir.path().join("redis_config.conf");
    let mut file = File::create(&config_file_path).unwrap();
    for (i, master_port) in master_ports.iter().enumerate() {
        file.write_all(
            format!("sentinel monitor master{i} 127.0.0.1 {master_port} 1\n",).as_bytes(),
        )
        .unwrap();
    }
    file.flush().unwrap();

    RedisServer::new_with_addr_tls_modules_and_spawner(
        get_addr(port),
        Some(&config_file_path),
        Some(tlspaths.clone()),
        MTLS_NOT_ENABLED,
        modules,
        |cmd| {
            cmd.arg("--sentinel");
            if let ConnectionAddr::TcpTls { .. } = get_addr(port) {
                cmd.arg("--tls-replication").arg("yes");
            }
            cmd.current_dir(dir.path());
            cmd.spawn().unwrap()
        },
    )
}

pub struct SentinelError;

pub fn wait_for_master_server(
    mut get_client_fn: impl FnMut() -> RedisResult<Client>,
    cluster: Option<&RedisSentinelCluster>,
) -> Result<(), SentinelError> {
    let rolecmd = redis::cmd("ROLE");
    for _ in 0..100 {
        let master_client = get_client_fn();
        match &master_client {
            Ok(client) => match client.get_connection() {
                Ok(mut conn) => {
                    let r: Vec<redis::Value> = rolecmd.query(&mut conn).unwrap();
                    let role = String::from_redis_value_ref(r.first().unwrap()).unwrap();
                    if role.starts_with("master") {
                        println!("found master");
                        return Ok(());
                    } else {
                        println!("failed check for master role - current role: {r:?}")
                    }
                }
                Err(err) => {
                    println!("failed to get master connection: {err:?}");
                    if let Some(cluster) = cluster {
                        if let ConnectionAddr::Tcp(_, port) = client.get_connection_info().addr() {
                            cluster.log_redis_state_via_cli(*port);
                        }
                    }
                }
            },
            Err(err) => {
                println!("failed to get master client: {err:?}",)
            }
        }

        sleep(Duration::from_millis(25));
    }

    Err(SentinelError)
}

pub fn wait_for_replica(
    mut get_client_fn: impl FnMut() -> RedisResult<Client>,
    cluster: Option<&RedisSentinelCluster>,
) -> Result<(), SentinelError> {
    let rolecmd = redis::cmd("ROLE");
    for i in 0..300 {
        let replica_client = get_client_fn();
        match &replica_client {
            Ok(client) => match client.get_connection() {
                Ok(mut conn) => {
                    let r: Vec<redis::Value> = rolecmd.query(&mut conn).unwrap();
                    let role = String::from_redis_value_ref(r.first().unwrap()).unwrap();
                    let state = String::from_redis_value_ref(r.get(3).unwrap()).unwrap();
                    if role.starts_with("slave") && state == "connected" {
                        println!("found replica");
                        return Ok(());
                    } else {
                        println!("failed check for replica role - current role: {r:?}")
                    }
                }
                Err(err) => {
                    println!("failed to get replica connection: {err:?}");
                    if let Some(cluster) = cluster {
                        if let ConnectionAddr::Tcp(_, port) = client.get_connection_info().addr() {
                            cluster.log_redis_state_via_cli(*port);
                        }
                    }
                }
            },
            Err(err) => {
                println!("failed to get replica client: {err:?}")
            }
        }

        let delay = if i < 100 { 25 } else { 50 };
        sleep(Duration::from_millis(delay));
    }

    Err(SentinelError)
}

fn wait_for_replicas_to_sync(cluster: &RedisSentinelCluster, masters: u16) {
    let servers = &cluster.servers;
    let cluster_size = servers.len() / (masters as usize);
    let clusters = servers.len() / cluster_size;
    let replicas = cluster_size - 1;

    for cluster_index in 0..clusters {
        let master_addr = servers[cluster_index * cluster_size].connection_info();
        let r = wait_for_master_server(|| redis::Client::open(master_addr.clone()), Some(cluster));
        if r.is_err() {
            cluster.log_sentinel_state_via_cli(&format!("master{}", cluster_index));
            panic!("failed waiting for master to be ready");
        }

        for replica_index in 0..replicas {
            let replica_addr =
                servers[(cluster_index * cluster_size) + 1 + replica_index].connection_info();
            let r = wait_for_replica(|| redis::Client::open(replica_addr.clone()), Some(cluster));
            if r.is_err() {
                cluster.log_sentinel_state_via_cli(&format!("master{}", cluster_index));
                panic!("failed waiting for replica to be ready and in sync");
            }
        }
    }
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

        let tempdir = tempfile::Builder::new()
            .prefix("redistls")
            .tempdir()
            .expect("failed to create tempdir");
        let tlspaths = build_keys_and_certs_for_tls(&tempdir);
        folders.push(tempdir);

        let required_number_of_sockets = masters * (replicas_per_master + 1) + sentinels;
        let mut available_ports = std::collections::HashSet::new();
        while available_ports.len() < required_number_of_sockets as usize {
            available_ports.insert(get_random_available_port());
        }
        let mut available_ports: Vec<_> = available_ports.into_iter().collect();

        for _ in 0..masters {
            let port = available_ports.pop().unwrap();
            let tempdir = tempfile::Builder::new()
                .prefix("redis")
                .tempdir()
                .expect("failed to create tempdir");
            servers.push(spawn_master_server(port, &tempdir, &tlspaths, modules));
            folders.push(tempdir);
            master_ports.push(port);

            for _ in 0..replicas_per_master {
                let replica_port = available_ports.pop().unwrap();
                let tempdir = tempfile::Builder::new()
                    .prefix("redis")
                    .tempdir()
                    .expect("failed to create tempdir");
                servers.push(spawn_replica_server(
                    replica_port,
                    port,
                    &tempdir,
                    &tlspaths,
                    modules,
                ));
                folders.push(tempdir);
            }
        }

        let mut sentinel_servers = vec![];
        for _ in 0..sentinels {
            let port = available_ports.pop().unwrap();
            let tempdir = tempfile::Builder::new()
                .prefix("redis")
                .tempdir()
                .expect("failed to create tempdir");

            sentinel_servers.push(spawn_sentinel_server(
                port,
                &master_ports,
                &tempdir,
                &tlspaths,
                modules,
            ));
            folders.push(tempdir);
        }

        let cluster = RedisSentinelCluster {
            servers,
            sentinel_servers,
            folders,
        };

        // Wait for replicas to sync so that the sentinels discover them on the first try
        wait_for_replicas_to_sync(&cluster, masters);

        cluster
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
