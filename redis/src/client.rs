use std::time::Duration;

use crate::{
    connection::{connect, Connection, ConnectionInfo, ConnectionLike, IntoConnectionInfo},
    types::{RedisResult, Value},
};
#[cfg(feature = "aio")]
use std::pin::Pin;

#[cfg(feature = "tls-rustls")]
use crate::tls::{inner_build_with_tls, TlsCertificates};

/// The client type.
#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) connection_info: ConnectionInfo,
}

/// The client acts as connector to the redis server.  By itself it does not
/// do much other than providing a convenient way to fetch a connection from
/// it.  In the future the plan is to provide a connection pool in the client.
///
/// When opening a client a URL in the following format should be used:
///
/// ```plain
/// redis://host:port/db
/// ```
///
/// Example usage::
///
/// ```rust,no_run
/// let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// let con = client.get_connection().unwrap();
/// ```
impl Client {
    /// Connects to a redis server and returns a client.  This does not
    /// actually open a connection yet but it does perform some basic
    /// checks on the URL that might make the operation fail.
    pub fn open<T: IntoConnectionInfo>(params: T) -> RedisResult<Client> {
        Ok(Client {
            connection_info: params.into_connection_info()?,
        })
    }

    /// Instructs the client to actually connect to redis and returns a
    /// connection object.  The connection object can be used to send
    /// commands to the server.  This can fail with a variety of errors
    /// (like unreachable host) so it's important that you handle those
    /// errors.
    pub fn get_connection(&self) -> RedisResult<Connection> {
        connect(&self.connection_info, None)
    }

    /// Instructs the client to actually connect to redis with specified
    /// timeout and returns a connection object.  The connection object
    /// can be used to send commands to the server.  This can fail with
    /// a variety of errors (like unreachable host) so it's important
    /// that you handle those errors.
    pub fn get_connection_with_timeout(&self, timeout: Duration) -> RedisResult<Connection> {
        connect(&self.connection_info, Some(timeout))
    }

    /// Returns a reference of client connection info object.
    pub fn get_connection_info(&self) -> &ConnectionInfo {
        &self.connection_info
    }
}

/// To enable async support you need to chose one of the supported runtimes and active its
/// corresponding feature: `tokio-comp` or `async-std-comp`
#[cfg(feature = "aio")]
#[cfg_attr(docsrs, doc(cfg(feature = "aio")))]
impl Client {
    /// Returns an async connection from the client.
    #[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
    #[deprecated(
        note = "aio::Connection is deprecated. Use client::get_multiplexed_async_connection instead."
    )]
    #[allow(deprecated)]
    pub async fn get_async_connection(&self) -> RedisResult<crate::aio::Connection> {
        let con = match Runtime::locate() {
            #[cfg(feature = "tokio-comp")]
            Runtime::Tokio => {
                self.get_simple_async_connection::<crate::aio::tokio::Tokio>()
                    .await?
            }
            #[cfg(feature = "async-std-comp")]
            Runtime::AsyncStd => {
                self.get_simple_async_connection::<crate::aio::async_std::AsyncStd>()
                    .await?
            }
        };

        crate::aio::Connection::new(&self.connection_info.redis, con).await
    }

    /// Returns an async connection from the client.
    #[cfg(feature = "tokio-comp")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio-comp")))]
    #[deprecated(
        note = "aio::Connection is deprecated. Use client::get_multiplexed_tokio_connection instead."
    )]
    #[allow(deprecated)]
    pub async fn get_tokio_connection(&self) -> RedisResult<crate::aio::Connection> {
        use crate::aio::RedisRuntime;
        Ok(
            crate::aio::connect::<crate::aio::tokio::Tokio>(&self.connection_info)
                .await?
                .map(RedisRuntime::boxed),
        )
    }

    /// Returns an async connection from the client.
    #[cfg(feature = "async-std-comp")]
    #[cfg_attr(docsrs, doc(cfg(feature = "async-std-comp")))]
    #[deprecated(
        note = "aio::Connection is deprecated. Use client::get_multiplexed_async_std_connection instead."
    )]
    #[allow(deprecated)]
    pub async fn get_async_std_connection(&self) -> RedisResult<crate::aio::Connection> {
        use crate::aio::RedisRuntime;
        Ok(
            crate::aio::connect::<crate::aio::async_std::AsyncStd>(&self.connection_info)
                .await?
                .map(RedisRuntime::boxed),
        )
    }

    /// Returns an async connection from the client.
    #[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(feature = "tokio-comp", feature = "async-std-comp")))
    )]
    pub async fn get_multiplexed_async_connection(
        &self,
    ) -> RedisResult<crate::aio::MultiplexedConnection> {
        self.get_multiplexed_async_connection_with_timeouts(
            std::time::Duration::MAX,
            std::time::Duration::MAX,
        )
        .await
    }

    /// Returns an async connection from the client.
    #[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(feature = "tokio-comp", feature = "async-std-comp")))
    )]
    pub async fn get_multiplexed_async_connection_with_timeouts(
        &self,
        response_timeout: std::time::Duration,
        connection_timeout: std::time::Duration,
    ) -> RedisResult<crate::aio::MultiplexedConnection> {
        let result = match Runtime::locate() {
            #[cfg(feature = "tokio-comp")]
            rt @ Runtime::Tokio => {
                rt.timeout(
                    connection_timeout,
                    self.get_multiplexed_async_connection_inner::<crate::aio::tokio::Tokio>(
                        response_timeout,
                    ),
                )
                .await
            }
            #[cfg(feature = "async-std-comp")]
            rt @ Runtime::AsyncStd => {
                rt.timeout(
                    connection_timeout,
                    self.get_multiplexed_async_connection_inner::<crate::aio::async_std::AsyncStd>(
                        response_timeout,
                    ),
                )
                .await
            }
        };

        match result {
            Ok(Ok(connection)) => Ok(connection),
            Ok(Err(e)) => Err(e),
            Err(elapsed) => Err(elapsed.into()),
        }
    }

    /// Returns an async multiplexed connection from the client.
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    #[cfg(feature = "tokio-comp")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio-comp")))]
    pub async fn get_multiplexed_tokio_connection_with_response_timeouts(
        &self,
        response_timeout: std::time::Duration,
        connection_timeout: std::time::Duration,
    ) -> RedisResult<crate::aio::MultiplexedConnection> {
        let result = Runtime::locate()
            .timeout(
                connection_timeout,
                self.get_multiplexed_async_connection_inner::<crate::aio::tokio::Tokio>(
                    response_timeout,
                ),
            )
            .await;

        match result {
            Ok(Ok(connection)) => Ok(connection),
            Ok(Err(e)) => Err(e),
            Err(elapsed) => Err(elapsed.into()),
        }
    }

    /// Returns an async multiplexed connection from the client.
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    #[cfg(feature = "tokio-comp")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio-comp")))]
    pub async fn get_multiplexed_tokio_connection(
        &self,
    ) -> RedisResult<crate::aio::MultiplexedConnection> {
        self.get_multiplexed_tokio_connection_with_response_timeouts(
            std::time::Duration::MAX,
            std::time::Duration::MAX,
        )
        .await
    }

    /// Returns an async multiplexed connection from the client.
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    #[cfg(feature = "async-std-comp")]
    #[cfg_attr(docsrs, doc(cfg(feature = "async-std-comp")))]
    pub async fn get_multiplexed_async_std_connection_with_timeouts(
        &self,
        response_timeout: std::time::Duration,
        connection_timeout: std::time::Duration,
    ) -> RedisResult<crate::aio::MultiplexedConnection> {
        let result = Runtime::locate()
            .timeout(
                connection_timeout,
                self.get_multiplexed_async_connection_inner::<crate::aio::async_std::AsyncStd>(
                    response_timeout,
                ),
            )
            .await;

        match result {
            Ok(Ok(connection)) => Ok(connection),
            Ok(Err(e)) => Err(e),
            Err(elapsed) => Err(elapsed.into()),
        }
    }

    /// Returns an async multiplexed connection from the client.
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    #[cfg(feature = "async-std-comp")]
    #[cfg_attr(docsrs, doc(cfg(feature = "async-std-comp")))]
    pub async fn get_multiplexed_async_std_connection(
        &self,
    ) -> RedisResult<crate::aio::MultiplexedConnection> {
        self.get_multiplexed_async_std_connection_with_timeouts(
            std::time::Duration::MAX,
            std::time::Duration::MAX,
        )
        .await
    }

    /// Returns an async multiplexed connection from the client and a future which must be polled
    /// to drive any requests submitted to it (see `get_multiplexed_tokio_connection`).
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    /// The multiplexer will return a timeout error on any request that takes longer then `response_timeout`.
    #[cfg(feature = "tokio-comp")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio-comp")))]
    pub async fn create_multiplexed_tokio_connection_with_response_timeout(
        &self,
        response_timeout: std::time::Duration,
    ) -> RedisResult<(
        crate::aio::MultiplexedConnection,
        impl std::future::Future<Output = ()>,
    )> {
        self.create_multiplexed_async_connection_inner::<crate::aio::tokio::Tokio>(response_timeout)
            .await
    }

    /// Returns an async multiplexed connection from the client and a future which must be polled
    /// to drive any requests submitted to it (see `get_multiplexed_tokio_connection`).
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    #[cfg(feature = "tokio-comp")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio-comp")))]
    pub async fn create_multiplexed_tokio_connection(
        &self,
    ) -> RedisResult<(
        crate::aio::MultiplexedConnection,
        impl std::future::Future<Output = ()>,
    )> {
        self.create_multiplexed_tokio_connection_with_response_timeout(std::time::Duration::MAX)
            .await
    }

    /// Returns an async multiplexed connection from the client and a future which must be polled
    /// to drive any requests submitted to it (see `get_multiplexed_tokio_connection`).
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    /// The multiplexer will return a timeout error on any request that takes longer then `response_timeout`.
    #[cfg(feature = "async-std-comp")]
    #[cfg_attr(docsrs, doc(cfg(feature = "async-std-comp")))]
    pub async fn create_multiplexed_async_std_connection_with_response_timeout(
        &self,
        response_timeout: std::time::Duration,
    ) -> RedisResult<(
        crate::aio::MultiplexedConnection,
        impl std::future::Future<Output = ()>,
    )> {
        self.create_multiplexed_async_connection_inner::<crate::aio::async_std::AsyncStd>(
            response_timeout,
        )
        .await
    }

    /// Returns an async multiplexed connection from the client and a future which must be polled
    /// to drive any requests submitted to it (see `get_multiplexed_tokio_connection`).
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    #[cfg(feature = "async-std-comp")]
    #[cfg_attr(docsrs, doc(cfg(feature = "async-std-comp")))]
    pub async fn create_multiplexed_async_std_connection(
        &self,
    ) -> RedisResult<(
        crate::aio::MultiplexedConnection,
        impl std::future::Future<Output = ()>,
    )> {
        self.create_multiplexed_async_std_connection_with_response_timeout(std::time::Duration::MAX)
            .await
    }

    /// Returns an async [`ConnectionManager`][connection-manager] from the client.
    ///
    /// The connection manager wraps a
    /// [`MultiplexedConnection`][multiplexed-connection]. If a command to that
    /// connection fails with a connection error, then a new connection is
    /// established in the background and the error is returned to the caller.
    ///
    /// This means that on connection loss at least one command will fail, but
    /// the connection will be re-established automatically if possible. Please
    /// refer to the [`ConnectionManager`][connection-manager] docs for
    /// detailed reconnecting behavior.
    ///
    /// A connection manager can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    ///
    /// [connection-manager]: aio/struct.ConnectionManager.html
    /// [multiplexed-connection]: aio/struct.MultiplexedConnection.html
    #[cfg(feature = "connection-manager")]
    #[cfg_attr(docsrs, doc(cfg(feature = "connection-manager")))]
    #[deprecated(note = "use get_connection_manager instead")]
    pub async fn get_tokio_connection_manager(&self) -> RedisResult<crate::aio::ConnectionManager> {
        crate::aio::ConnectionManager::new(self.clone()).await
    }

    /// Returns an async [`ConnectionManager`][connection-manager] from the client.
    ///
    /// The connection manager wraps a
    /// [`MultiplexedConnection`][multiplexed-connection]. If a command to that
    /// connection fails with a connection error, then a new connection is
    /// established in the background and the error is returned to the caller.
    ///
    /// This means that on connection loss at least one command will fail, but
    /// the connection will be re-established automatically if possible. Please
    /// refer to the [`ConnectionManager`][connection-manager] docs for
    /// detailed reconnecting behavior.
    ///
    /// A connection manager can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    ///
    /// [connection-manager]: aio/struct.ConnectionManager.html
    /// [multiplexed-connection]: aio/struct.MultiplexedConnection.html
    #[cfg(feature = "connection-manager")]
    #[cfg_attr(docsrs, doc(cfg(feature = "connection-manager")))]
    pub async fn get_connection_manager(&self) -> RedisResult<crate::aio::ConnectionManager> {
        crate::aio::ConnectionManager::new(self.clone()).await
    }

    /// Returns an async [`ConnectionManager`][connection-manager] from the client.
    ///
    /// The connection manager wraps a
    /// [`MultiplexedConnection`][multiplexed-connection]. If a command to that
    /// connection fails with a connection error, then a new connection is
    /// established in the background and the error is returned to the caller.
    ///
    /// This means that on connection loss at least one command will fail, but
    /// the connection will be re-established automatically if possible. Please
    /// refer to the [`ConnectionManager`][connection-manager] docs for
    /// detailed reconnecting behavior.
    ///
    /// A connection manager can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    ///
    /// [connection-manager]: aio/struct.ConnectionManager.html
    /// [multiplexed-connection]: aio/struct.MultiplexedConnection.html
    #[cfg(feature = "connection-manager")]
    #[cfg_attr(docsrs, doc(cfg(feature = "connection-manager")))]
    #[deprecated(note = "use get_connection_manager_with_backoff instead")]
    pub async fn get_tokio_connection_manager_with_backoff(
        &self,
        exponent_base: u64,
        factor: u64,
        number_of_retries: usize,
    ) -> RedisResult<crate::aio::ConnectionManager> {
        self.get_tokio_connection_manager_with_backoff_and_timeouts(
            exponent_base,
            factor,
            number_of_retries,
            std::time::Duration::MAX,
            std::time::Duration::MAX,
        )
        .await
    }

    /// Returns an async [`ConnectionManager`][connection-manager] from the client.
    ///
    /// The connection manager wraps a
    /// [`MultiplexedConnection`][multiplexed-connection]. If a command to that
    /// connection fails with a connection error, then a new connection is
    /// established in the background and the error is returned to the caller.
    ///
    /// This means that on connection loss at least one command will fail, but
    /// the connection will be re-established automatically if possible. Please
    /// refer to the [`ConnectionManager`][connection-manager] docs for
    /// detailed reconnecting behavior.
    ///
    /// A connection manager can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    ///
    /// [connection-manager]: aio/struct.ConnectionManager.html
    /// [multiplexed-connection]: aio/struct.MultiplexedConnection.html
    #[cfg(feature = "connection-manager")]
    #[cfg_attr(docsrs, doc(cfg(feature = "connection-manager")))]
    pub async fn get_tokio_connection_manager_with_backoff_and_timeouts(
        &self,
        exponent_base: u64,
        factor: u64,
        number_of_retries: usize,
        response_timeout: std::time::Duration,
        connection_timeout: std::time::Duration,
    ) -> RedisResult<crate::aio::ConnectionManager> {
        crate::aio::ConnectionManager::new_with_backoff_and_timeouts(
            self.clone(),
            exponent_base,
            factor,
            number_of_retries,
            response_timeout,
            connection_timeout,
        )
        .await
    }

    /// Returns an async [`ConnectionManager`][connection-manager] from the client.
    ///
    /// The connection manager wraps a
    /// [`MultiplexedConnection`][multiplexed-connection]. If a command to that
    /// connection fails with a connection error, then a new connection is
    /// established in the background and the error is returned to the caller.
    ///
    /// This means that on connection loss at least one command will fail, but
    /// the connection will be re-established automatically if possible. Please
    /// refer to the [`ConnectionManager`][connection-manager] docs for
    /// detailed reconnecting behavior.
    ///
    /// A connection manager can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    ///
    /// [connection-manager]: aio/struct.ConnectionManager.html
    /// [multiplexed-connection]: aio/struct.MultiplexedConnection.html
    #[cfg(feature = "connection-manager")]
    #[cfg_attr(docsrs, doc(cfg(feature = "connection-manager")))]
    pub async fn get_connection_manager_with_backoff(
        &self,
        exponent_base: u64,
        factor: u64,
        number_of_retries: usize,
    ) -> RedisResult<crate::aio::ConnectionManager> {
        crate::aio::ConnectionManager::new_with_backoff(
            self.clone(),
            exponent_base,
            factor,
            number_of_retries,
        )
        .await
    }

    async fn get_multiplexed_async_connection_inner<T>(
        &self,
        response_timeout: std::time::Duration,
    ) -> RedisResult<crate::aio::MultiplexedConnection>
    where
        T: crate::aio::RedisRuntime,
    {
        let (connection, driver) = self
            .create_multiplexed_async_connection_inner::<T>(response_timeout)
            .await?;
        T::spawn(driver);
        Ok(connection)
    }

    async fn create_multiplexed_async_connection_inner<T>(
        &self,
        response_timeout: std::time::Duration,
    ) -> RedisResult<(
        crate::aio::MultiplexedConnection,
        impl std::future::Future<Output = ()>,
    )>
    where
        T: crate::aio::RedisRuntime,
    {
        let con = self.get_simple_async_connection::<T>().await?;
        crate::aio::MultiplexedConnection::new_with_response_timeout(
            &self.connection_info.redis,
            con,
            response_timeout,
        )
        .await
    }

    async fn get_simple_async_connection<T>(
        &self,
    ) -> RedisResult<Pin<Box<dyn crate::aio::AsyncStream + Send + Sync>>>
    where
        T: crate::aio::RedisRuntime,
    {
        Ok(crate::aio::connect_simple::<T>(&self.connection_info)
            .await?
            .boxed())
    }

    #[cfg(feature = "connection-manager")]
    pub(crate) fn connection_info(&self) -> &ConnectionInfo {
        &self.connection_info
    }

    /// Constructs a new `Client` with parameters necessary to create a TLS connection.
    ///
    /// - `conn_info` - URL using the `rediss://` scheme.
    /// - `tls_certs` - `TlsCertificates` structure containing:
    /// -- `client_tls` - Optional `ClientTlsConfig` containing byte streams for
    /// --- `client_cert` - client's byte stream containing client certificate in PEM format
    /// --- `client_key` - client's byte stream containing private key in PEM format
    /// -- `root_cert` - Optional byte stream yielding PEM formatted file for root certificates.
    ///
    /// If `ClientTlsConfig` ( cert+key pair ) is not provided, then client-side authentication is not enabled.
    /// If `root_cert` is not provided, then system root certificates are used instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{fs::File, io::{BufReader, Read}};
    ///
    /// use redis::{Client, AsyncCommands as _, TlsCertificates, ClientTlsConfig};
    ///
    /// async fn do_redis_code(
    ///     url: &str,
    ///     root_cert_file: &str,
    ///     cert_file: &str,
    ///     key_file: &str
    /// ) -> redis::RedisResult<()> {
    ///     let root_cert_file = File::open(root_cert_file).expect("cannot open private cert file");
    ///     let mut root_cert_vec = Vec::new();
    ///     BufReader::new(root_cert_file)
    ///         .read_to_end(&mut root_cert_vec)
    ///         .expect("Unable to read ROOT cert file");
    ///
    ///     let cert_file = File::open(cert_file).expect("cannot open private cert file");
    ///     let mut client_cert_vec = Vec::new();
    ///     BufReader::new(cert_file)
    ///         .read_to_end(&mut client_cert_vec)
    ///         .expect("Unable to read client cert file");
    ///
    ///     let key_file = File::open(key_file).expect("cannot open private key file");
    ///     let mut client_key_vec = Vec::new();
    ///     BufReader::new(key_file)
    ///         .read_to_end(&mut client_key_vec)
    ///         .expect("Unable to read client key file");
    ///
    ///     let client = Client::build_with_tls(
    ///         url,
    ///         TlsCertificates {
    ///             client_tls: Some(ClientTlsConfig{
    ///                 client_cert: client_cert_vec,
    ///                 client_key: client_key_vec,
    ///             }),
    ///             root_cert: Some(root_cert_vec),
    ///         }
    ///     )
    ///     .expect("Unable to build client");
    ///
    ///     let connection_info = client.get_connection_info();
    ///
    ///     println!(">>> connection info: {connection_info:?}");
    ///
    ///     let mut con = client.get_async_connection().await?;
    ///
    ///     con.set("key1", b"foo").await?;
    ///
    ///     redis::cmd("SET")
    ///         .arg(&["key2", "bar"])
    ///         .query_async(&mut con)
    ///         .await?;
    ///
    ///     let result = redis::cmd("MGET")
    ///         .arg(&["key1", "key2"])
    ///         .query_async(&mut con)
    ///         .await;
    ///     assert_eq!(result, Ok(("foo".to_string(), b"bar".to_vec())));
    ///     println!("Result from MGET: {result:?}");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[cfg(feature = "tls-rustls")]
    pub fn build_with_tls<C: IntoConnectionInfo>(
        conn_info: C,
        tls_certs: TlsCertificates,
    ) -> RedisResult<Client> {
        let connection_info = conn_info.into_connection_info()?;

        inner_build_with_tls(connection_info, tls_certs)
    }

    /// Returns an async receiver for pub-sub messages.
    #[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
    // TODO - do we want to type-erase pubsub using a trait, to allow us to replace it with a different implementation later?
    pub async fn get_async_pubsub(&self) -> RedisResult<crate::aio::PubSub> {
        #[allow(deprecated)]
        self.get_async_connection()
            .await
            .map(|connection| connection.into_pubsub())
    }

    /// Returns an async receiver for monitor messages.
    #[cfg(any(feature = "tokio-comp", feature = "async-std-comp"))]
    // TODO - do we want to type-erase monitor using a trait, to allow us to replace it with a different implementation later?
    pub async fn get_async_monitor(&self) -> RedisResult<crate::aio::Monitor> {
        #[allow(deprecated)]
        self.get_async_connection()
            .await
            .map(|connection| connection.into_monitor())
    }
}

#[cfg(feature = "aio")]
use crate::aio::Runtime;

impl ConnectionLike for Client {
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        self.get_connection()?.req_packed_command(cmd)
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        self.get_connection()?
            .req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.connection_info.redis.db
    }

    fn check_connection(&mut self) -> bool {
        if let Ok(mut conn) = self.get_connection() {
            conn.check_connection()
        } else {
            false
        }
    }

    fn is_open(&self) -> bool {
        if let Ok(conn) = self.get_connection() {
            conn.is_open()
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn regression_293_parse_ipv6_with_interface() {
        assert!(Client::open(("fe80::cafe:beef%eno1", 6379)).is_ok());
    }
}
