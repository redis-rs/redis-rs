use futures::{prelude::*, task};

use crate::connection::{connect, Connection, ConnectionInfo, ConnectionLike, IntoConnectionInfo};
use crate::types::{RedisError, RedisResult, Value};

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
    pub fn get_async_connection<'a>(
        &'a self,
    ) -> impl Future<Output = RedisResult<crate::aio::Connection>> + 'a {
        crate::aio::connect(&self.connection_info)
    }

    /// Returns an async multiplexed connection from the client.
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    ///
    /// This uses the default tokio executor.
    #[cfg(feature = "executor")]
    pub async fn get_multiplexed_async_connection(
        &self,
    ) -> RedisResult<crate::aio::MultiplexedConnection> {
        // Creates a `task::Spawn` which spawns task on the default tokio executor
        // (for some reason tokio do not include this)
        struct TokioExecutor;
        impl task::Spawn for TokioExecutor {
            fn spawn_obj(
                &mut self,
                future: future::FutureObj<'static, ()>,
            ) -> Result<(), task::SpawnError> {
                use tokio_executor::Executor;
                tokio_executor::DefaultExecutor::current()
                    .spawn(future.boxed())
                    .map_err(|_| task::SpawnError::shutdown())
            }
        }

        let con = self.get_async_connection().await?;
        crate::aio::MultiplexedConnection::new(con, TokioExecutor)
    }

    /// Returns an async multiplexed connection from the client.
    ///
    /// A multiplexed connection can be cloned, allowing requests to be be sent concurrently
    /// on the same underlying connection (tcp/unix socket).
    ///
    /// Requires an executor to spawn a task that drives the underlying connection.
    pub async fn get_multiplexed_async_connection_with_executor<E>(
        &self,
        executor: E,
    ) -> RedisResult<crate::aio::MultiplexedConnection>
    where
        E: task::Spawn,
    {
        let con = self.get_async_connection().await?;
        crate::aio::MultiplexedConnection::new(con, executor)
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
}
