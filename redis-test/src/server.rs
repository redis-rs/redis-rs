use redis::{ConnectionAddr, IntoConnectionInfo, ProtocolVersion, RedisConnectionInfo};
use std::ffi::OsStr;
use std::path::Path;
use std::{env, fs, path::PathBuf, process};
use tempfile::TempDir;

use crate::utils::{TlsFilePaths, build_keys_and_certs_for_tls, get_random_available_port};

pub fn use_protocol() -> ProtocolVersion {
    if env::var("PROTOCOL").unwrap_or_default() == "RESP3" {
        ProtocolVersion::RESP3
    } else {
        ProtocolVersion::RESP2
    }
}

pub fn redis_settings() -> RedisConnectionInfo {
    RedisConnectionInfo::default().set_protocol(use_protocol())
}

/// Get the default host to use for TCP connections.
pub fn get_default_host() -> String {
    "127.0.0.1".to_string()
}

#[derive(PartialEq)]
enum ServerType {
    Tcp { tls: bool },
    Unix,
}

/// Represents a module that can be loaded into the Redis server.
#[non_exhaustive]
pub enum Module {
    Bloom,
    Json,
}

/// A standalone Redis server instance for testing.
///
/// `RedisServer` manages the lifecycle of a Redis process, including startup,
/// configuration, and shutdown.
///
/// # Example
/// ```rust,no_run
/// use redis_test::server::RedisServer;
///
/// let server = RedisServer::new();
/// let info = server.connection_info();
/// // Connect to the server using `info`...
/// ```
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
                        host: get_default_host(),
                        port,
                        insecure: true,
                        tls_params: None,
                    }
                } else {
                    redis::ConnectionAddr::Tcp(get_default_host(), port)
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

        RedisServer::new_with_addr_tls_modules_and_cmd_refiner(
            addr,
            None,
            None,
            mtls_enabled,
            None,
            modules,
            |_cmd| {},
        )
    }

    pub fn new_with_addr_and_modules(
        addr: redis::ConnectionAddr,
        modules: &[Module],
        mtls_enabled: bool,
    ) -> RedisServer {
        RedisServer::new_with_addr_tls_modules_and_cmd_refiner(
            addr,
            None,
            None,
            mtls_enabled,
            None,
            modules,
            |_cmd| {},
        )
    }

    pub fn new_with_addr_tls_modules_and_cmd_refiner(
        mut addr: redis::ConnectionAddr,
        config_file: Option<&Path>,
        mut tls_paths: Option<TlsFilePaths>,
        mtls_enabled: bool,
        cert_auth_field: Option<&str>,
        modules: &[Module],
        cmd_refiner: impl FnOnce(&mut RedisServerCommand),
    ) -> RedisServer {
        let mut redis_cmd = RedisServerCommand::new();

        if let Some(config_path) = config_file {
            redis_cmd.arg(config_path);
        }

        // Disable snapshotting
        // This stops littering `dump.rdb` files during testing/development.
        redis_cmd.arg2("--save", "");

        redis_cmd.load_modules(modules);

        let tempdir = tempfile::Builder::new()
            .prefix("redis")
            .tempdir()
            .expect("failed to create tempdir");
        let log_file = Self::log_file(&tempdir);
        redis_cmd.arg2("--logfile", log_file.clone());
        if get_major_version() > 6 {
            redis_cmd.arg2("--enable-debug-command", "yes");
        }

        match addr {
            redis::ConnectionAddr::Tcp(ref bind, server_port) => {
                redis_cmd
                    .arg2("--port", server_port.to_string())
                    .arg2("--bind", bind);
            }
            redis::ConnectionAddr::TcpTls { ref host, port, .. } => {
                let tls_paths =
                    tls_paths.get_or_insert_with(|| build_keys_and_certs_for_tls(&tempdir));

                let auth_client = if mtls_enabled { "yes" } else { "no" };

                // prepare redis with TLS
                redis_cmd
                    .arg2("--tls-port", port.to_string())
                    .arg2("--port", "0")
                    .arg2("--tls-cert-file", &tls_paths.redis_crt)
                    .arg2("--tls-key-file", &tls_paths.redis_key)
                    .arg2("--tls-ca-cert-file", &tls_paths.ca_crt)
                    .arg2("--tls-auth-clients", auth_client)
                    .arg2("--bind", host);

                // Enable certificate-based authentication (Redis 8.6+)
                // The cert_auth_field specifies which certificate field to use for username mapping
                // (e.g., "CN" for Common Name)
                if let Some(field) = cert_auth_field {
                    redis_cmd.arg2("--tls-auth-clients-user", field);
                }

                // Insecure only disabled if `mtls` is enabled
                let insecure = !mtls_enabled;

                addr = redis::ConnectionAddr::TcpTls {
                    host: host.clone(),
                    port,
                    insecure,
                    tls_params: None,
                };
            }
            redis::ConnectionAddr::Unix(ref path) => {
                redis_cmd.arg2("--port", "0").arg2("--unixsocket", path);
            }
            _ => panic!("Unknown address format: {addr:?}"),
        };

        cmd_refiner(&mut redis_cmd);

        RedisServer {
            process: redis_cmd.spawn(),
            log_file,
            tempdir,
            addr,
            tls_paths,
        }
    }

    pub fn client_addr(&self) -> &redis::ConnectionAddr {
        &self.addr
    }

    pub fn host_and_port(&self) -> Option<(&str, u16)> {
        match &self.addr {
            ConnectionAddr::Tcp(host, port) => Some((host, *port)),
            ConnectionAddr::TcpTls { host, port, .. } => Some((host, *port)),
            _ => None,
        }
    }

    pub fn connection_info(&self) -> redis::ConnectionInfo {
        self.client_addr()
            .clone()
            .into_connection_info()
            .unwrap()
            .set_redis_settings(redis_settings())
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

pub struct RedisServerCommand {
    // The actual command to run
    cmd: process::Command,
}

impl Default for RedisServerCommand {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisServerCommand {
    pub fn new() -> Self {
        let bin = env::var("REDISRS_SERVER_BIN").unwrap_or_else(|_| "redis-server".to_string());

        // Build the main command
        let mut cmd = process::Command::new(&bin);

        // Capture the command's stdout and stderr
        cmd.stdout(process::Stdio::piped());
        cmd.stderr(process::Stdio::piped());

        // Build the instance
        Self { cmd }
    }

    /// Appends a new argument to the command
    pub fn arg<S: AsRef<OsStr>>(&mut self, arg: S) -> &mut Self {
        self.cmd.arg(arg);
        self
    }

    /// Appends two new arguments to the command
    ///
    /// This method is purely convenience to get more readable argument setting as it allows to
    /// re-write
    ///
    /// ```rust,no_run
    /// # use redis_test::server::RedisServerCommand;
    /// # let mut redis_cmd = RedisServerCommand::new();
    /// redis_cmd
    ///     .arg("--foo")
    ///     .arg("some-value-for-foo")
    ///     .arg("--bar")
    ///     .arg("some-value-for-bar")
    ///     .arg("--baz")
    ///     .arg("some-value-for-baz");
    /// ```
    ///
    /// in a more readable fashion:
    ///
    /// ```rust,no_run
    /// # use redis_test::server::RedisServerCommand;
    /// # let mut redis_cmd = RedisServerCommand::new();
    /// redis_cmd
    ///     .arg2("--foo", "some-value-for-foo")
    ///     .arg2("--bar", "some-value-for-bar")
    ///     .arg2("--baz", "some-value-for-baz");
    /// ```
    pub fn arg2<S1: AsRef<OsStr>, S2: AsRef<OsStr>>(&mut self, arg1: S1, arg2: S2) -> &mut Self {
        self.cmd.arg(arg1).arg(arg2);
        self
    }

    /// Appends three new arguments to the command
    ///
    /// This method is purely convenience to get more readable argument setting (cf. [`arg2`](Self::arg2)).
    pub fn arg3<S1: AsRef<OsStr>, S2: AsRef<OsStr>, S3: AsRef<OsStr>>(
        &mut self,
        arg1: S1,
        arg2: S2,
        arg3: S3,
    ) -> &mut Self {
        self.cmd.arg(arg1).arg(arg2).arg(arg3);
        self
    }

    /// Set the directory to run the command in
    pub fn current_dir<P: AsRef<Path>>(&mut self, dir: P) -> &mut Self {
        self.cmd.current_dir(dir);
        self
    }

    /// Runs the command
    ///
    /// # Panics
    ///
    /// This method panics if spawning fails.
    ///
    /// If the command itself exits (immediately or not, regardless of the exit code) this function
    /// does _not_ panic but returns the `Child` instance.
    pub fn spawn(&mut self) -> process::Child {
        self.cmd
            .spawn()
            .unwrap_or_else(|err| panic!("Failed to run {:?}: {err}", self.cmd))
    }

    /// Loads a module from the given path
    fn load_module(&mut self, path: String) {
        if !Path::new(&path).is_file() {
            panic!("Module doesn't exist or is not a file: {path}");
        }
        self.arg2("--loadmodule", path);
    }

    /// Loads the given modules
    ///
    /// The paths to the modules are inferred from environment variables.
    pub(crate) fn load_modules(&mut self, modules: &[Module]) {
        for module in modules {
            match module {
                Module::Json => {
                    // Try to pick up json module path from REDISRS_REDIS_JSON_PATH environment variable
                    let path = match env::var("REDISRS_REDIS_JSON_PATH") {
                        Ok(path) => path,
                        // Falling back to legacy REDIS_RS_REDIS_JSON_PATH environment variable
                        Err(_) => match env::var("REDIS_RS_REDIS_JSON_PATH") {
                            Ok(path) => {
                                eprintln!(
                                    "Warning: Use of REDIS_RS_REDIS_JSON_PATH is deprecated. Use REDISRS_REDIS_JSON_PATH (no '_' before 'RS') instead"
                                );
                                path
                            }
                            Err(_) => {
                                panic!(
                                    "Unable to find path to RedisJSON at REDISRS_REDIS_JSON_PATH, is it set?"
                                );
                            }
                        },
                    };

                    self.load_module(path);
                }
                Module::Bloom => {
                    let path = env::var("REDISRS_REDIS_BLOOM_PATH").expect(
                        "Unable to find path to RedisBloom at REDISRS_REDIS_BLOOM_PATH, is it set?",
                    );

                    self.load_module(path);
                }
            };
        }
    }
}

fn get_major_version() -> u8 {
    let full_string = String::from_utf8(
        RedisServerCommand::new()
            .arg("-v")
            .spawn()
            .wait_with_output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    let (_, res) = full_string.split_once(" v=").unwrap();
    let (res, _) = res.split_once(".").unwrap();
    res.parse().unwrap()
}
