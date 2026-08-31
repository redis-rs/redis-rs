use crate::server::{Module, RedisServer, RedisServerBuilder, RedisServerCommand, use_protocol};
use crate::utils::{TlsFilePaths, build_single_client};
use crate::version::{AvailableComponents, TestContextVersioning};
#[cfg(feature = "aio")]
use redis::RedisResult;
use redis::{ConnectionAddr, ErrorKind, ProtocolVersion, ServerErrorKind, TypedCommands};
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

/// A builder for [`TestContext`]
///
/// # Example
///
/// ```rust,no_run
/// use redis_test::TestContextBuilder;
/// use redis_test::server::Module;
///
/// let ctx = TestContextBuilder::new().module(Module::Json).build();
/// let connection = ctx.connection();
/// // Use `connection` to run commands
/// ```
// Note that this builder is an owned-builder as we want to build in a single chain anyway and do
// not have to build multiple instances from the same builder. Also, this spares us cloning
// considerations.
#[derive(Default)]
pub struct TestContextBuilder {
    server_builder: RedisServerBuilder,
}

impl TestContextBuilder {
    /// Starts a fresh builder
    pub fn new() -> Self {
        Default::default()
    }

    pub fn address(mut self, address: ConnectionAddr) -> Self {
        self.server_builder = self.server_builder.address(address);
        self
    }

    pub fn config(mut self, config_file: PathBuf) -> Self {
        self.server_builder = self.server_builder.config(config_file);
        self
    }

    pub fn cert_auth_field(mut self, cert_auth_field: impl Into<String>) -> Self {
        self.server_builder = self.server_builder.cert_auth_field(cert_auth_field);
        self
    }

    pub fn cert_auth_field_opt(mut self, opt_cert_auth_field: Option<impl Into<String>>) -> Self {
        self.server_builder = self.server_builder.cert_auth_field_opt(opt_cert_auth_field);
        self
    }

    pub fn module(mut self, module: Module) -> Self {
        self.server_builder = self.server_builder.module(module);
        self
    }

    pub fn modules(mut self, modules: &[Module]) -> Self {
        self.server_builder = self.server_builder.modules(modules);
        self
    }

    pub fn mtls(mut self, enable_mtls: bool) -> Self {
        self.server_builder = self.server_builder.mtls(enable_mtls);
        self
    }

    pub fn tls_paths(mut self, tls_paths: TlsFilePaths) -> Self {
        self.server_builder = self.server_builder.tls_paths(tls_paths);
        self
    }

    pub fn tls_paths_opt(mut self, opt_tls_paths: Option<TlsFilePaths>) -> Self {
        self.server_builder = self.server_builder.tls_paths_opt(opt_tls_paths);
        self
    }

    /// Builds the [`TestContext`] for this instance
    pub fn build(self) -> TestContext {
        self.refine_and_build(|_| {})
    }

    /// Builds the [`TestContext`] for this instance after refining the arguments for the server
    ///
    /// # Arguments
    ///
    /// * `refiner` - See [`RedisServerBuilder::refine_and_build`]
    pub fn refine_and_build(self, refiner: impl FnOnce(&mut RedisServerCommand)) -> TestContext {
        let server = self.server_builder.refine_and_build(refiner);
        TestContext::from_server(server)
    }
}

/// `panic`ks and dumps the server log file
macro_rules! panic_w_server_log_dump {
    ($server:ident, $msg:literal $(, $arg:tt)*) => {
        let msg = format!($msg, $(, $arg)*);
        let process_info = $server.stop_with_info();
        panic!("{msg}\n{process_info}")
    }
}

/// Utility wrapper for a standalone Redis server instance for testing.
///
/// # Example
///
/// Use `default()` to build a [`TestContext`] with default settings:
///
/// ```rust,no_run
/// use redis_test::TestContext;
///
/// let ctx = TestContext::default();
/// let connection = ctx.connection();
/// // Use `connection` to run commands
/// ```
///
/// If you need a custom setup, use [`TestContextBuilder`]:
///
/// ```rust,no_run
/// use redis_test::TestContextBuilder;
/// use redis_test::server::Module;
///
/// let ctx = TestContextBuilder::new().module(Module::Json).build();
/// let connection = ctx.connection();
/// // Use `connection` to run commands
/// ```
pub struct TestContext {
    pub server: RedisServer,
    pub client: redis::Client,
    pub protocol: ProtocolVersion,
}

impl Default for TestContext {
    fn default() -> Self {
        TestContextBuilder::new().build()
    }
}

impl TestContext {
    /// Builds a new instance from a [`RedisServer`]
    // We intentionally do _not_ implement `From<RedisServer>` as that would be public.
    //
    // Instead, users should to go through `TestContextBuilder` to limit the points of entry and
    // hence help us with maintenance.
    fn from_server(mut server: RedisServer) -> Self {
        let client =
            build_single_client(server.connection_info(), &server.tls_paths, server.mtls).unwrap();

        if server.tls_paths.is_some() {
            crate::utils::start_tls_crypto_provider();
        }

        let mut con;

        let millisecond = Duration::from_millis(1);
        let mut retries = 0;
        loop {
            match client.get_connection() {
                Err(err) => {
                    if err.is_connection_refusal() {
                        // Check if the server is still alive
                        if !server.is_alive() {
                            panic_w_server_log_dump!(
                                server,
                                "Server exited before we could connect"
                            );
                        }

                        // Wait and retry
                        sleep(millisecond);
                        retries += 1;
                        if retries > 100000 {
                            panic_w_server_log_dump!(
                                server,
                                "Tried to connect too many times, last error: {err}"
                            );
                        }
                    } else {
                        panic_w_server_log_dump!(server, "Could not connect: {err}");
                    }
                }
                Ok(x) => {
                    con = x;
                    break;
                }
            }
        }

        // Redis may still be loading its dataset after accepting connections,
        // especially with TLS where the handshake completes before Redis is fully ready.
        // Retry flushdb if the BusyLoading error is returned to allow time for initialization.
        let mut flush_retries = 0;
        loop {
            match con.flushdb() {
                Ok(_) => break,
                Err(err)
                    if matches!(err.kind(), ErrorKind::Server(ServerErrorKind::BusyLoading)) =>
                {
                    sleep(millisecond);
                    flush_retries += 1;
                    if flush_retries > 10000 {
                        panic_w_server_log_dump!(
                            server,
                            "Redis is still loading after too many retries, last error: {err}"
                        );
                    }
                }
                Err(err) => {
                    panic_w_server_log_dump!(server, "Failed to flush database: {err}");
                }
            }
        }

        Self {
            server,
            client,
            protocol: use_protocol(),
        }
    }

    pub fn connection(&self) -> redis::Connection {
        self.client.get_connection().unwrap()
    }

    #[cfg(feature = "aio")]
    pub async fn async_connection(&self) -> RedisResult<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
    }

    #[cfg(feature = "aio")]
    pub async fn async_pubsub(&self) -> RedisResult<redis::aio::PubSub> {
        self.client.get_async_pubsub().await
    }

    pub fn stop_server(&mut self) {
        self.server.stop();
    }
}

impl TestContextVersioning for TestContext {
    fn get_available_components(&self) -> AvailableComponents {
        let mut conn = self.connection();
        AvailableComponents::from(&mut conn)
    }
}
