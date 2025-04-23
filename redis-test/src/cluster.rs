use std::{env, process, thread::sleep, time::Duration};

use tempfile::TempDir;

use crate::{
    server::{Module, RedisServer},
    utils::{build_keys_and_certs_for_tls_ext, get_random_available_port, TlsFilePaths},
};

pub struct RedisClusterConfiguration {
    pub num_nodes: u16,
    pub num_replicas: u16,
    pub modules: Vec<Module>,
    pub tls_insecure: bool,
    pub mtls_enabled: bool,
    pub ports: Vec<u16>,
    pub certs_with_ip_alts: bool,
}

impl RedisClusterConfiguration {
    pub fn single_replica_config() -> Self {
        Self {
            num_nodes: 6,
            num_replicas: 1,
            ..Default::default()
        }
    }
}

impl Default for RedisClusterConfiguration {
    fn default() -> Self {
        Self {
            num_nodes: 3,
            num_replicas: 0,
            modules: vec![],
            tls_insecure: true,
            mtls_enabled: false,
            ports: vec![],
            certs_with_ip_alts: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClusterType {
    Tcp,
    TcpTls,
}

impl ClusterType {
    pub fn get_intended() -> ClusterType {
        match env::var("REDISRS_SERVER_TYPE")
            .ok()
            .as_ref()
            .map(|x| &x[..])
        {
            Some("tcp") => ClusterType::Tcp,
            Some("tcp+tls") => ClusterType::TcpTls,
            Some(val) => {
                panic!("Unknown server type {val:?}");
            }
            None => ClusterType::Tcp,
        }
    }

    fn build_addr(port: u16) -> redis::ConnectionAddr {
        match ClusterType::get_intended() {
            ClusterType::Tcp => redis::ConnectionAddr::Tcp("127.0.0.1".into(), port),
            ClusterType::TcpTls => redis::ConnectionAddr::TcpTls {
                host: "127.0.0.1".into(),
                port,
                insecure: true,
                tls_params: None,
            },
        }
    }
}

fn port_in_use(addr: &str) -> bool {
    let socket_addr: std::net::SocketAddr = addr.parse().expect("Invalid address");
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(socket_addr),
        socket2::Type::STREAM,
        None,
    )
    .expect("Failed to create socket");

    socket.connect(&socket_addr.into()).is_ok()
}

pub struct RedisCluster {
    pub servers: Vec<RedisServer>,
    pub folders: Vec<TempDir>,
    pub tls_paths: Option<TlsFilePaths>,
}

impl RedisCluster {
    pub fn username() -> &'static str {
        "hello"
    }

    pub fn password() -> &'static str {
        "world"
    }

    pub fn new(configuration: RedisClusterConfiguration) -> RedisCluster {
        let RedisClusterConfiguration {
            num_nodes: nodes,
            num_replicas: replicas,
            modules,
            tls_insecure,
            mtls_enabled,
            ports,
            certs_with_ip_alts,
        } = configuration;

        let optional_ports = if ports.is_empty() {
            vec![None; nodes as usize]
        } else {
            assert!(ports.len() == nodes as usize);
            ports.into_iter().map(Some).collect()
        };
        let mut chosen_ports = std::collections::HashSet::new();

        let mut folders = vec![];
        let mut addrs = vec![];
        let mut tls_paths = None;

        let mut is_tls = false;

        if let ClusterType::TcpTls = ClusterType::get_intended() {
            // Create a shared set of keys in cluster mode
            let tempdir = tempfile::Builder::new()
                .prefix("redis")
                .tempdir()
                .expect("failed to create tempdir");
            let files = build_keys_and_certs_for_tls_ext(&tempdir, certs_with_ip_alts);
            folders.push(tempdir);
            tls_paths = Some(files);
            is_tls = true;
        }

        let max_attempts = 5;

        let mut make_server = |port| {
            RedisServer::new_with_addr_tls_modules_and_spawner(
                ClusterType::build_addr(port),
                None,
                tls_paths.clone(),
                mtls_enabled,
                &modules,
                |cmd| {
                    let tempdir = tempfile::Builder::new()
                        .prefix("redis")
                        .tempdir()
                        .expect("failed to create tempdir");
                    let acl_path = tempdir.path().join("users.acl");
                    let acl_content = format!(
                        "user {} on allcommands allkeys >{}",
                        Self::username(),
                        Self::password()
                    );
                    std::fs::write(&acl_path, acl_content).expect("failed to write acl file");
                    cmd.arg("--cluster-enabled")
                        .arg("yes")
                        .arg("--cluster-config-file")
                        .arg(tempdir.path().join("nodes.conf"))
                        .arg("--cluster-node-timeout")
                        .arg("5000")
                        .arg("--appendonly")
                        .arg("yes")
                        .arg("--aclfile")
                        .arg(&acl_path);
                    if is_tls {
                        cmd.arg("--tls-cluster").arg("yes");
                        if replicas > 0 {
                            cmd.arg("--tls-replication").arg("yes");
                        }
                    }
                    cmd.current_dir(tempdir.path());
                    folders.push(tempdir);
                    cmd.spawn().unwrap()
                },
            )
        };

        let verify_server = |server: &mut RedisServer| {
            let process = &mut server.process;
            match process.try_wait() {
                Ok(Some(status)) => {
                    let log_file_contents = server.log_file_contents();
                    let err =
                                    format!("redis server creation failed with status {status:?}.\nlog file: {log_file_contents:?}");
                    Err(err)
                }
                Ok(None) => {
                    // wait for 10 seconds for the server to be available.
                    let max_attempts = 200;
                    let mut cur_attempts = 0;
                    loop {
                        if cur_attempts == max_attempts {
                            let log_file_contents = server.log_file_contents();
                            break Err(format!("redis server creation failed: Address {} closed. {log_file_contents:?}", server.addr));
                        } else if port_in_use(&server.addr.to_string()) {
                            break Ok(());
                        }
                        eprintln!("Waiting for redis process to initialize");
                        sleep(Duration::from_millis(50));
                        cur_attempts += 1;
                    }
                }
                Err(e) => {
                    panic!("Unexpected error in redis server creation {e}");
                }
            }
        };

        let servers = optional_ports
            .into_iter()
            .map(|port_option| {
                for _ in 0..5 {
                    let port = match port_option {
                        Some(port) => port,
                        None => loop {
                            let port = get_random_available_port();
                            if chosen_ports.contains(&port) {
                                continue;
                            }
                            chosen_ports.insert(port);
                            break port;
                        },
                    };
                    let mut server = make_server(port);
                    sleep(Duration::from_millis(50));

                    match verify_server(&mut server) {
                        Ok(_) => {
                            let addr = format!("127.0.0.1:{port}");
                            addrs.push(addr.clone());
                            return server;
                        }
                        Err(err) => eprintln!("{err}"),
                    }
                }
                panic!("Exhausted retries");
            })
            .collect();

        let mut cmd = process::Command::new("redis-cli");
        cmd.stdout(process::Stdio::piped())
            .arg("--cluster")
            .arg("create")
            .args(&addrs);
        if replicas > 0 {
            cmd.arg("--cluster-replicas").arg(replicas.to_string());
        }
        cmd.arg("--cluster-yes");

        if is_tls {
            if mtls_enabled {
                if let Some(TlsFilePaths {
                    redis_crt,
                    redis_key,
                    ca_crt,
                }) = &tls_paths
                {
                    cmd.arg("--cert");
                    cmd.arg(redis_crt);
                    cmd.arg("--key");
                    cmd.arg(redis_key);
                    cmd.arg("--cacert");
                    cmd.arg(ca_crt);
                    cmd.arg("--tls");
                }
            } else if !tls_insecure && tls_paths.is_some() {
                let ca_crt = &tls_paths.as_ref().unwrap().ca_crt;
                cmd.arg("--tls").arg("--cacert").arg(ca_crt);
            } else {
                cmd.arg("--tls").arg("--insecure");
            }
        }

        let mut cur_attempts = 0;
        loop {
            let output = cmd.output().unwrap();
            if output.status.success() {
                break;
            } else {
                let err = format!("Cluster creation failed: {output:?}");
                if cur_attempts == max_attempts {
                    panic!("{err}");
                }
                eprintln!("Retrying: {err}");
                sleep(Duration::from_millis(50));
                cur_attempts += 1;
            }
        }

        let cluster = RedisCluster {
            servers,
            folders,
            tls_paths,
        };
        if replicas > 0 {
            cluster.wait_for_replicas(replicas);
        }

        wait_for_status_ok(&cluster);
        cluster
    }

    fn wait_for_replicas(&self, replicas: u16) {
        'server: for server in &self.servers {
            let conn_info = server.connection_info();
            eprintln!(
                "waiting until {:?} knows required number of replicas",
                conn_info.addr
            );

            let client = redis::Client::open(server.connection_info()).unwrap();

            let mut con = client.get_connection().unwrap();

            // retry 500 times
            for _ in 1..500 {
                let value = redis::cmd("CLUSTER").arg("SLOTS").query(&mut con).unwrap();
                let slots: Vec<Vec<redis::Value>> = redis::from_owned_redis_value(value).unwrap();

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

fn wait_for_status_ok(cluster: &RedisCluster) {
    'server: for server in &cluster.servers {
        let log_file = RedisServer::log_file(&server.tempdir);

        for _ in 1..500 {
            let contents =
                std::fs::read_to_string(&log_file).expect("Should have been able to read the file");

            if contents.contains("Cluster state changed: ok") {
                continue 'server;
            }
            sleep(Duration::from_millis(20));
        }
        panic!("failed to reach state change: OK");
    }
}

impl Drop for RedisCluster {
    fn drop(&mut self) {
        self.stop()
    }
}
