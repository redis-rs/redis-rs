use redis::{ConnectionAddr, IntoConnectionInfo, ProtocolVersion, RedisConnectionInfo};
use std::ffi::OsStr;
use std::path::Path;
use std::{env, fs, path::PathBuf, process};
use tempfile::TempDir;

use crate::utils::{
    CommandMultiArgs, TlsFilePaths, build_keys_and_certs_for_tls, get_random_available_port,
};

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
#[derive(Clone)]
#[non_exhaustive]
pub enum Module {
    Bloom,
    Json,
}

/// A builder for [`RedisServer`]
///
/// # Example
///
/// ```rust,no_run
/// use redis_test::server::{RedisServerBuilder, Module};
///
/// let server = RedisServerBuilder::new().module(Module::Json).build();
/// let info = server.connection_info();
/// // Connect to the server using `info`...
/// ```
// Note that this builder is an owned-builder as we want to build in a single chain anyway and do
// not have to build multiple instances from the same builder. Also, this spares us cloning
// considerations.
#[derive(Default)]
pub struct RedisServerBuilder {
    address: Option<ConnectionAddr>,
    config_file: Option<PathBuf>,
    cert_auth_field: Option<String>,
    modules: Vec<Module>,
    mtls: bool,
    tls_paths: Option<TlsFilePaths>,
}

impl RedisServerBuilder {
    /// Starts a fresh builder
    pub fn new() -> Self {
        Default::default()
    }

    pub fn address(mut self, address: ConnectionAddr) -> Self {
        self.address = Some(address);
        self
    }

    pub fn config(mut self, config_file: PathBuf) -> Self {
        self.config_file = Some(config_file);
        self
    }

    pub fn cert_auth_field(mut self, cert_auth_field: impl Into<String>) -> Self {
        self.cert_auth_field = Some(cert_auth_field.into());
        self
    }

    pub fn cert_auth_field_opt(mut self, opt_cert_auth_field: Option<impl Into<String>>) -> Self {
        self.cert_auth_field = opt_cert_auth_field.map(|value| value.into());
        self
    }

    pub fn module(self, module: Module) -> Self {
        self.modules(&[module])
    }

    pub fn modules(mut self, modules: &[Module]) -> Self {
        self.modules = modules.to_vec();
        self
    }

    pub fn mtls(mut self, enable_mtls: bool) -> Self {
        self.mtls = enable_mtls;
        self
    }

    pub fn tls_paths(mut self, tls_paths: TlsFilePaths) -> Self {
        self.tls_paths = Some(tls_paths);
        self
    }

    pub fn tls_paths_opt(mut self, opt_tls_paths: Option<TlsFilePaths>) -> Self {
        self.tls_paths = opt_tls_paths;
        self
    }

    /// Builds the [`RedisServer`] for this instance
    pub fn build(self) -> RedisServer {
        self.refine_and_build(|_| {})
    }

    /// Builds the [`RedisServer`] for this instance after refining the arguments for the server
    ///
    /// # Arguments
    ///
    /// * `refiner` - This method is called just before starting the server. It takes one argument,
    ///   which is the command to start the server with. This allows to add additional config to
    ///   the server command.
    pub fn refine_and_build(self, refiner: impl FnOnce(&mut RedisServerCommand)) -> RedisServer {
        let addr = self.address.unwrap_or_else(|| {
            // This is technically a race, but we can't do better with
            // the tools that redis gives us :(
            let redis_port = get_random_available_port();
            RedisServer::get_addr(redis_port)
        });

        RedisServer::new(
            addr,
            self.config_file,
            self.tls_paths,
            self.mtls,
            self.cert_auth_field,
            self.modules.as_slice(),
            refiner,
        )
    }
}

/// A standalone Redis server instance for testing.
///
/// `RedisServer` manages the lifecycle of a Redis process, including startup,
/// configuration, and shutdown.
///
/// # Example
///
/// Use `default()` to build a [`RedisServer`] with default settings:
///
/// ```rust,no_run
/// use redis_test::server::RedisServer;
///
/// let server = RedisServer::default();
/// let info = server.connection_info();
/// // Connect to the server using `info`...
/// ```
///
/// If you need a custom setup, use [`RedisServerBuilder`]:
///
/// ```rust,no_run
/// use redis_test::server::{RedisServerBuilder, Module};
///
/// let server = RedisServerBuilder::new().module(Module::Json).build();
/// let info = server.connection_info();
/// // Connect to the server using `info`...
/// ```
#[non_exhaustive]
pub struct RedisServer {
    pub process: process::Child,
    pub tempdir: tempfile::TempDir,
    pub log_file: PathBuf,
    pub addr: redis::ConnectionAddr,
    pub tls_paths: Option<TlsFilePaths>,
    pub mtls: bool,
}

impl ServerType {
    fn get_intended() -> Self {
        match env::var("REDISRS_SERVER_TYPE")
            .ok()
            .as_ref()
            .map(|x| &x[..])
        {
            Some("tcp+tls") => Self::Tcp { tls: true },
            Some("unix") => Self::Unix,
            Some("tcp") | None => Self::Tcp { tls: false },
            Some(val) => {
                panic!("Unknown server type {val:?}");
            }
        }
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Default for RedisServer {
    fn default() -> Self {
        RedisServerBuilder::new().build()
    }
}

impl RedisServer {
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

    fn new(
        mut addr: redis::ConnectionAddr,
        config_file: Option<PathBuf>,
        mut tls_paths: Option<TlsFilePaths>,
        mtls: bool,
        cert_auth_field: Option<String>,
        modules: &[Module],
        cmd_refiner: impl FnOnce(&mut RedisServerCommand),
    ) -> Self {
        // Guard against unsupported settings
        if tls_paths.is_some() && !matches!(addr, ConnectionAddr::TcpTls { .. }) {
            panic!("'tls_paths' is only supported for TCP with TLS");
        }

        if mtls && !matches!(addr, ConnectionAddr::TcpTls { .. }) {
            panic!("'mtls' is only supported for TCP with TLS");
        }

        if cert_auth_field.is_some() && !matches!(addr, ConnectionAddr::TcpTls { .. }) {
            panic!("'cert_auth_field' is only supported for TCP with TLS");
        }

        if cert_auth_field.is_some() && !mtls {
            panic!("'cert_auth_field' is only supported for mTLS");
        }

        // From here on, settings are good and supported
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

        // Disable all default listening
        redis_cmd.arg2("--port", "0");

        // Enable one kind of listening
        match addr {
            redis::ConnectionAddr::Tcp(ref host, port) => {
                redis_cmd
                    .arg2("--port", port.to_string())
                    .arg2("--bind", host);
            }
            redis::ConnectionAddr::TcpTls { ref host, port, .. } => {
                let tls_paths =
                    tls_paths.get_or_insert_with(|| build_keys_and_certs_for_tls(&tempdir));

                let auth_client = if mtls { "yes" } else { "no" };

                // prepare redis with TLS
                redis_cmd
                    .arg2("--tls-port", port.to_string())
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
                let insecure = !mtls;

                addr = redis::ConnectionAddr::TcpTls {
                    host: host.clone(),
                    port,
                    insecure,
                    tls_params: None,
                };
            }
            redis::ConnectionAddr::Unix(ref path) => {
                redis_cmd.arg2("--unixsocket", path);
            }
            _ => panic!("Unknown address format: {addr:?}"),
        }

        cmd_refiner(&mut redis_cmd);

        Self {
            process: redis_cmd.spawn(),
            log_file,
            tempdir,
            addr,
            tls_paths,
            mtls,
        }
    }

    pub fn client_addr(&self) -> &redis::ConnectionAddr {
        &self.addr
    }

    pub fn host_and_port(&self) -> Option<(&str, u16)> {
        match &self.addr {
            ConnectionAddr::Tcp(host, port) | ConnectionAddr::TcpTls { host, port, .. } => {
                Some((host, *port))
            }
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

    /// Loads a single module
    // Although the passed `path_env_var_name`s will have some common parts, these are on purpose
    // _not_ abstracted away. This forces callers to pass the full environment variable name, which
    // makes sure that grepping for environment variable names leads to the relevant code.
    fn load_module(&mut self, path_env_var_name: &str, description: &str) {
        let path = env::var_os(path_env_var_name).unwrap_or_else(|| {
            panic!(
                "Environment variable {path_env_var_name} is empty, but should hold the path to a {description} module"
            )
        });

        if !Path::new(&path).is_file() {
            panic!("Path for the {description} module doesn't exist or is not a file: {path:?}");
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
                    self.load_module("REDISRS_REDIS_JSON_PATH", "JSON");
                }
                Module::Bloom => {
                    self.load_module("REDISRS_REDIS_BLOOM_PATH", "Bloom");
                }
            }
        }
    }
}

impl CommandMultiArgs for RedisServerCommand {
    fn arg<S: AsRef<OsStr>>(&mut self, arg: S) -> &mut Self {
        self.cmd.arg(arg);
        self
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
