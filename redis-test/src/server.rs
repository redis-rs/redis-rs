use redis::{ConnectionAddr, ProtocolVersion, RedisConnectionInfo};
use std::path::Path;
use std::{env, fs, path::PathBuf, process};

use tempfile::TempDir;

use crate::utils::{build_keys_and_certs_for_tls, get_random_available_port, TlsFilePaths};

pub fn use_protocol() -> ProtocolVersion {
    if env::var("PROTOCOL").unwrap_or_default() == "RESP3" {
        ProtocolVersion::RESP3
    } else {
        ProtocolVersion::RESP2
    }
}

#[derive(PartialEq)]
enum ServerType {
    Tcp { tls: bool },
    Unix,
}

pub enum Module {
    Json,
}

pub struct RedisServer {
    pub process: process::Child,
    pub tempdir: tempfile::TempDir,
    pub log_file: PathBuf,
    pub addr: redis::ConnectionAddr,
    pub tls_paths: Option<TlsFilePaths>,
}

impl ServerType {
    fn get_intended() -> ServerType {
        match env::var("REDISRS_SERVER_TYPE")
            .ok()
            .as_ref()
            .map(|x| &x[..])
        {
            Some("tcp") => ServerType::Tcp { tls: false },
            Some("tcp+tls") => ServerType::Tcp { tls: true },
            Some("unix") => ServerType::Unix,
            Some(val) => {
                panic!("Unknown server type {val:?}");
            }
            None => ServerType::Tcp { tls: false },
        }
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        self.stop()
    }
}

impl Default for RedisServer {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisServer {
    pub fn new() -> RedisServer {
        RedisServer::with_modules(&[], false)
    }

    pub fn new_with_mtls() -> RedisServer {
        RedisServer::with_modules(&[], true)
    }

    pub fn log_file_contents(&self) -> Option<String> {
        std::fs::read_to_string(self.log_file.clone()).ok()
    }

    pub fn get_addr(port: u16) -> ConnectionAddr {
        let server_type = ServerType::get_intended();
        match server_type {
            ServerType::Tcp { tls } => {
                if tls {
                    redis::ConnectionAddr::TcpTls {
                        host: "127.0.0.1".to_string(),
                        port,
                        insecure: true,
                        tls_params: None,
                    }
                } else {
                    redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), port)
                }
            }
            ServerType::Unix => {
                let (a, b) = rand::random::<(u64, u64)>();
                let path = format!("/tmp/redis-rs-test-{a}-{b}.sock");
                redis::ConnectionAddr::Unix(PathBuf::from(&path))
            }
        }
    }

    pub fn with_modules(modules: &[Module], mtls_enabled: bool) -> RedisServer {
        // this is technically a race but we can't do better with
        // the tools that redis gives us :(
        let redis_port = get_random_available_port();
        let addr = RedisServer::get_addr(redis_port);

        RedisServer::new_with_addr_tls_modules_and_spawner(
            addr,
            None,
            None,
            mtls_enabled,
            modules,
            |cmd| {
                cmd.spawn()
                    .unwrap_or_else(|err| panic!("Failed to run {cmd:?}: {err}"))
            },
        )
    }

    pub fn new_with_addr_and_modules(
        addr: redis::ConnectionAddr,
        modules: &[Module],
        mtls_enabled: bool,
    ) -> RedisServer {
        RedisServer::new_with_addr_tls_modules_and_spawner(
            addr,
            None,
            None,
            mtls_enabled,
            modules,
            |cmd| {
                cmd.spawn()
                    .unwrap_or_else(|err| panic!("Failed to run {cmd:?}: {err}"))
            },
        )
    }

    pub fn new_with_addr_tls_modules_and_spawner<
        F: FnOnce(&mut process::Command) -> process::Child,
    >(
        addr: redis::ConnectionAddr,
        config_file: Option<&Path>,
        tls_paths: Option<TlsFilePaths>,
        mtls_enabled: bool,
        modules: &[Module],
        spawner: F,
    ) -> RedisServer {
        let mut redis_cmd = process::Command::new("redis-server");

        if let Some(config_path) = config_file {
            redis_cmd.arg(config_path);
        }

        // Load Redis Modules
        for module in modules {
            match module {
                Module::Json => {
                    redis_cmd
                        .arg("--loadmodule")
                        .arg(env::var("REDIS_RS_REDIS_JSON_PATH").expect(
                        "Unable to find path to RedisJSON at REDIS_RS_REDIS_JSON_PATH, is it set?",
                    ));
                }
            };
        }

        redis_cmd
            .stdout(process::Stdio::piped())
            .stderr(process::Stdio::piped());
        let tempdir = tempfile::Builder::new()
            .prefix("redis")
            .tempdir()
            .expect("failed to create tempdir");
        let log_file = Self::log_file(&tempdir);
        redis_cmd.arg("--logfile").arg(log_file.clone());
        if get_major_version() > 6 {
            redis_cmd.arg("--enable-debug-command").arg("yes");
        }
        match addr {
            redis::ConnectionAddr::Tcp(ref bind, server_port) => {
                redis_cmd
                    .arg("--port")
                    .arg(server_port.to_string())
                    .arg("--bind")
                    .arg(bind);

                RedisServer {
                    process: spawner(&mut redis_cmd),
                    log_file,
                    tempdir,
                    addr,
                    tls_paths: None,
                }
            }
            redis::ConnectionAddr::TcpTls { ref host, port, .. } => {
                let tls_paths = tls_paths.unwrap_or_else(|| build_keys_and_certs_for_tls(&tempdir));

                let auth_client = if mtls_enabled { "yes" } else { "no" };

                // prepare redis with TLS
                redis_cmd
                    .arg("--tls-port")
                    .arg(port.to_string())
                    .arg("--port")
                    .arg("0")
                    .arg("--tls-cert-file")
                    .arg(&tls_paths.redis_crt)
                    .arg("--tls-key-file")
                    .arg(&tls_paths.redis_key)
                    .arg("--tls-ca-cert-file")
                    .arg(&tls_paths.ca_crt)
                    .arg("--tls-auth-clients")
                    .arg(auth_client)
                    .arg("--bind")
                    .arg(host);

                // Insecure only disabled if `mtls` is enabled
                let insecure = !mtls_enabled;

                let addr = redis::ConnectionAddr::TcpTls {
                    host: host.clone(),
                    port,
                    insecure,
                    tls_params: None,
                };

                RedisServer {
                    process: spawner(&mut redis_cmd),
                    log_file,
                    tempdir,
                    addr,
                    tls_paths: Some(tls_paths),
                }
            }
            redis::ConnectionAddr::Unix(ref path) => {
                redis_cmd
                    .arg("--port")
                    .arg("0")
                    .arg("--unixsocket")
                    .arg(path);
                RedisServer {
                    process: spawner(&mut redis_cmd),
                    log_file,
                    tempdir,
                    addr,
                    tls_paths: None,
                }
            }
        }
    }

    pub fn client_addr(&self) -> &redis::ConnectionAddr {
        &self.addr
    }

    pub fn connection_info(&self) -> redis::ConnectionInfo {
        redis::ConnectionInfo {
            addr: self.client_addr().clone(),
            redis: RedisConnectionInfo {
                protocol: use_protocol(),
                ..Default::default()
            },
        }
    }

    pub fn stop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let redis::ConnectionAddr::Unix(ref path) = *self.client_addr() {
            fs::remove_file(path).ok();
        }
    }

    pub fn log_file(tempdir: &TempDir) -> PathBuf {
        tempdir.path().join("redis.log")
    }
}

fn get_major_version() -> u8 {
    let full_string = String::from_utf8(
        process::Command::new("redis-server")
            .arg("-v")
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    let (_, res) = full_string.split_once(" v=").unwrap();
    let (res, _) = res.split_once(".").unwrap();
    res.parse().unwrap()
}
