use std::time::Duration;

#[cfg(feature = "aio")]
use std::pin::Pin;

use crate::{
    connection::{connect, Connection, ConnectionInfo, ConnectionLike, IntoConnectionInfo},
    types::{RedisResult, Value},
};

/// The client type.
#[derive(Debug, Clone)]
pub struct Client {
    connection_info: ConnectionInfo,
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
        match Runtime::locate() {
            #[cfg(feature = "tokio-comp")]
            Runtime::Tokio => self.get_multiplexed_tokio_connection().await,
            #[cfg(feature = "async-std-comp")]
            Runtime::AsyncStd => self.get_multiplexed_async_std_connection().await,
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
        self.get_multiplexed_async_connection_inner::<crate::aio::tokio::Tokio>()
            .await
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
        self.get_multiplexed_async_connection_inner::<crate::aio::async_std::AsyncStd>()
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
        self.create_multiplexed_async_connection_inner::<crate::aio::tokio::Tokio>()
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
        self.create_multiplexed_async_connection_inner::<crate::aio::async_std::AsyncStd>()
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
    pub async fn get_tokio_connection_manager_with_backoff(
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
    ) -> RedisResult<crate::aio::MultiplexedConnection>
    where
        T: crate::aio::RedisRuntime,
    {
        let (connection, driver) = self
            .create_multiplexed_async_connection_inner::<T>()
            .await?;
        T::spawn(driver);
        Ok(connection)
    }

    async fn create_multiplexed_async_connection_inner<T>(
        &self,
    ) -> RedisResult<(
        crate::aio::MultiplexedConnection,
        impl std::future::Future<Output = ()>,
    )>
    where
        T: crate::aio::RedisRuntime,
    {
        let con = self.get_simple_async_connection::<T>().await?;
        crate::aio::MultiplexedConnection::new(&self.connection_info.redis, con).await
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
