use connection::{ConnectionInfo, IntoConnectionInfo, Connection, connect,
                 PubSub, connect_pubsub, ConnectionLike};
use types::{RedisResult, Value};


/// The client type.
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
            connection_info: try!(params.into_connection_info())
        })
    }

    /// Instructs the client to actually connect to redis and returns a
    /// connection object.  The connection object can be used to send
    /// commands to the server.  This can fail with a variety of errors
    /// (like unreachable host) so it's important that you handle those
    /// errors.
    pub fn get_connection(&self) -> RedisResult<Connection> {
        Ok(try!(connect(&self.connection_info)))
    }

    /// Returns a PubSub connection.  A pubsub connection can be used to
    /// listen to messages coming in through the redis publish/subscribe
    /// system.
    ///
    /// Note that redis' pubsub operates across all databases.
    pub fn get_pubsub(&self) -> RedisResult<PubSub> {
        Ok(try!(connect_pubsub(&self.connection_info)))
    }
}

impl ConnectionLike for Client {

    fn req_packed_command(&self, cmd: &[u8]) -> RedisResult<Value> {
        try!(self.get_connection()).req_packed_command(cmd)
    }

    fn req_packed_commands(&self, cmd: &[u8],
        offset: uint, count: uint) -> RedisResult<Vec<Value>> {
        try!(self.get_connection()).req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.connection_info.db
    }
}
