#![cfg(feature = "cluster")]
#![allow(dead_code)]

use std::convert::identity;
use std::env;
use std::io::Read;
use std::process;
use std::thread::sleep;
use std::time::Duration;

#[cfg(feature = "cluster-async")]
use redis::aio::ConnectionLike;
#[cfg(feature = "cluster-async")]
use redis::cluster_async::Connect;
use redis::ConnectionInfo;
use redis::ProtocolVersion;
use tempfile::TempDir;

use crate::support::{build_keys_and_certs_for_tls, Module};

use super::get_random_available_port;
#[cfg(feature = "tls-rustls")]
use super::{build_single_client, load_certs_from_file};

use super::use_protocol;
use super::RedisServer;
use super::TlsFilePaths;

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

pub struct RedisClusterConfiguration {
    pub num_nodes: u16,
    pub num_replicas: u16,
    pub modules: Vec<Module>,
    pub mtls_enabled: bool,
    pub ports: Vec<u16>,
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
            mtls_enabled: false,
            ports: vec![],
        }
    }
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
            mtls_enabled,
            mut ports,
        } = configuration;

        if ports.is_empty() {
            // We use a hashset in order to be sure that we have the right number
            // of unique ports.
            let mut hash = std::collections::HashSet::new();
            while hash.len() < nodes as usize {
                hash.insert(get_random_available_port());
            }
            ports = hash.into_iter().collect();
        }

        let mut servers = vec![];
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
            let files = build_keys_and_certs_for_tls(&tempdir);
            folders.push(tempdir);
            tls_paths = Some(files);
            is_tls = true;
        }

        let max_attempts = 5;

        for port in ports {
            servers.push(RedisServer::new_with_addr_tls_modules_and_spawner(
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
                        .arg(&tempdir.path().join("nodes.conf"))
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
                    let addr = format!("127.0.0.1:{port}");
                    cmd.current_dir(tempdir.path());
                    folders.push(tempdir);
                    addrs.push(addr.clone());

                    let mut cur_attempts = 0;
                    loop {
                        let mut process = cmd.spawn().unwrap();
                        sleep(Duration::from_millis(50));

                        match process.try_wait() {
                            Ok(Some(status)) => {
                                let stdout = process.stdout.map_or(String::new(), |mut out|{
                                    let mut str = String::new();
                                    out.read_to_string(&mut str).unwrap();
                                    str
                                });
                                let stderr = process.stderr.map_or(String::new(), |mut out|{
                                    let mut str = String::new();
                                    out.read_to_string(&mut str).unwrap();
                                    str
                                });
                                let err =
                                    format!("redis server creation failed with status {status:?}.\nstdout: `{stdout}`.\nstderr: `{stderr}`");
                                if cur_attempts == max_attempts {
                                    panic!("{err}");
                                }
                                eprintln!("Retrying: {err}");
                                cur_attempts += 1;
                            }
                            Ok(None) => {
                                // wait for 10 seconds for the server to be available.
                                let max_attempts = 200;
                                let mut cur_attempts = 0;
                                loop {
                                    if cur_attempts == max_attempts {
                                        panic!("redis server creation failed: Port {port} closed")
                                    }
                                    if port_in_use(&addr) {
                                        return process;
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
                    }
                },
            ));
        }

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
            cluster.wait_for_replicas(replicas, mtls_enabled);
        }

        wait_for_status_ok(&cluster);
        cluster
    }

    // parameter `_mtls_enabled` can only be used if `feature = tls-rustls` is active
    #[allow(dead_code)]
    fn wait_for_replicas(&self, replicas: u16, _mtls_enabled: bool) {
        'server: for server in &self.servers {
            let conn_info = server.connection_info();
            eprintln!(
                "waiting until {:?} knows required number of replicas",
                conn_info.addr
            );

            #[cfg(feature = "tls-rustls")]
            let client =
                build_single_client(server.connection_info(), &self.tls_paths, _mtls_enabled)
                    .unwrap();
            #[cfg(not(feature = "tls-rustls"))]
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

pub struct TestClusterContext {
    pub cluster: RedisCluster,
    pub client: redis::cluster::ClusterClient,
    pub mtls_enabled: bool,
    pub nodes: Vec<ConnectionInfo>,
    pub protocol: ProtocolVersion,
}

impl TestClusterContext {
    pub fn new() -> TestClusterContext {
        Self::new_with_config(RedisClusterConfiguration::default())
    }

    pub fn new_with_mtls() -> TestClusterContext {
        Self::new_with_config_and_builder(
            RedisClusterConfiguration {
                mtls_enabled: true,
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
        Self::new_with_config_and_builder(RedisClusterConfiguration::default(), initializer)
    }

    pub fn new_with_config_and_builder<F>(
        cluster_config: RedisClusterConfiguration,
        initializer: F,
    ) -> TestClusterContext
    where
        F: FnOnce(redis::cluster::ClusterClientBuilder) -> redis::cluster::ClusterClientBuilder,
    {
        let mtls_enabled = cluster_config.mtls_enabled;
        let cluster = RedisCluster::new(cluster_config);
        let initial_nodes: Vec<ConnectionInfo> = cluster
            .iter_servers()
            .map(RedisServer::connection_info)
            .collect();
        let mut builder = redis::cluster::ClusterClientBuilder::new(initial_nodes.clone())
            .use_protocol(use_protocol());

        #[cfg(feature = "tls-rustls")]
        if mtls_enabled {
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
            #[cfg(feature = "tls-rustls")]
            let client = build_single_client(
                server.connection_info(),
                &self.cluster.tls_paths,
                self.mtls_enabled,
            )
            .unwrap();
            #[cfg(not(feature = "tls-rustls"))]
            let client = redis::Client::open(server.connection_info()).unwrap();

            let mut con = client.get_connection().unwrap();
            let _: () = redis::cmd("ACL")
                .arg("SETUSER")
                .arg("default")
                .arg("off")
                .query(&mut con)
                .unwrap();

            // subsequent unauthenticated command should fail:
            if let Ok(mut con) = client.get_connection() {
                assert!(redis::cmd("PING").query::<()>(&mut con).is_err());
            }
        }
    }

    pub fn get_version(&self) -> super::Version {
        let mut conn = self.connection();
        super::get_version(&mut conn)
    }
}
