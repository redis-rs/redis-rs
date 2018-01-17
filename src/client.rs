use futures::Future;

use connection::{connect, Connection, ConnectionInfo, ConnectionLike, IntoConnectionInfo};
use types::{RedisError, RedisResult, Value};

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
        Ok(connect(&self.connection_info)?)
    }

    pub fn get_async_connection(
        &self,
    ) -> impl Future<Item = ::aio::Connection, Error = RedisError> {
        ::aio::connect(self.connection_info.clone())
    }

    pub fn get_shared_async_connection(
        &self,
    ) -> impl Future<Item = ::aio::SharedConnection, Error = RedisError> {
        self.get_async_connection()
            .and_then(move |con| ::aio::SharedConnection::new(con))
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
