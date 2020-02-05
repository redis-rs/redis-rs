use std::time::Duration;
use crate::connection::{connect, Connection, ConnectionInfo, ConnectionLike, IntoConnectionInfo};
use crate::types::{RedisResult, Value};

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
        Ok(connect(&self.connection_info, None)?)
    }

    /// Instructs the client to actually connect to redis with specified
    /// timeout and returns a connection object.  The connection object
    /// can be used to send commands to the server.  This can fail with
    /// a variety of errors (like unreachable host) so it's important
    /// that you handle those errors.
    pub fn get_connection_with_timeout(&self, timeout: Duration) -> RedisResult<Connection> {
        Ok(connect(&self.connection_info, Some(timeout))?)
    }

    /// Returns an async connection from the client.
    #[cfg(feature = "aio")]
    pub async fn get_async_connection(&self) -> RedisResult<crate::aio::Connection> {
        #[cfg(not(feature = "async-std"))]
        {
            self.get_tokio_connection_tokio().await
        }

        #[cfg(feature = "async-std")]
        {
            if tokio::runtime::Handle::try_current().is_ok() {
                self.get_tokio_connection_tokio().await
            } else {
                self.get_async_std_connection().await
            }
        }
    }

    /// Returns an async connection from the client.
    #[cfg(feature = "aio")]
    pub async fn get_tokio_connection_tokio(&self) -> RedisResult<crate::aio::Connection> {
        crate::aio::connect_tokio(&self.connection_info).await
    }

    /// Returns an async connection from the client.
    #[cfg(feature = "async-std-aio")]
    pub async fn get_async_std_connection(&self) -> RedisResult<crate::aio::Connection> {
        crate::aio::connect_async_std(&self.connection_info).await
    }

    /// Returns an async multiplexed connection from the client.
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    ///
    /// This requires the `tokio-rt-core` feature as it uses the default tokio executor.
    #[cfg(feature = "tokio-rt-core")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio-rt-core")))]
    pub async fn get_multiplexed_tokio_connection(
        &self,
    ) -> RedisResult<crate::aio::MultiplexedConnection> {
        let (connection, driver) = self.create_multiplexed_tokio_connection().await?;
        tokio::spawn(driver);
        Ok(connection)
    }


    /// Returns an async multiplexed connection from the client.
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    ///
    /// This requires the `tokio-rt-core` feature as it uses the default tokio executor.
    #[cfg(feature = "async-std-aio")]
    pub async fn get_multiplexed_async_std_connection(
        &self,
    ) -> RedisResult<crate::aio::MultiplexedConnection> {
        let (connection, driver) = self.create_multiplexed_async_std_connection().await?;
        async_std::task::spawn(driver);
        Ok(connection)
    }

    /// Returns an async multiplexed connection from the client and a future which must be polled
    /// to drive any requests submitted to it (see `get_multiplexed_tokio_connection`).
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    #[cfg(feature = "aio")]
    pub async fn create_multiplexed_tokio_connection(
        &self,
    ) -> RedisResult<(
        crate::aio::MultiplexedConnection,
        impl std::future::Future<Output = ()>,
    )> {
        crate::aio::MultiplexedConnection::new_tokio(&self.connection_info).await
    }

    /// Returns an async multiplexed connection from the client and a future which must be polled
    /// to drive any requests submitted to it (see `get_multiplexed_tokio_connection`).
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    #[cfg(feature = "async-std-aio")]
    pub async fn create_multiplexed_async_std_connection(
        &self,
    ) -> RedisResult<(
        crate::aio::MultiplexedConnection,
        impl std::future::Future<Output = ()>,
    )> {
        crate::aio::MultiplexedConnection::new_async_std(&self.connection_info).await
    }
}

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
        self.connection_info.db
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
