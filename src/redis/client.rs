use url::Url;
use std::io::{IoResult, IoError, InvalidInput};

use connection::{Connection, connect, PubSub, connect_pubsub};


/// The client type.
pub struct Client {
    host: String,
    port: u16,
    db: i64,
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
    pub fn open(uri: &str) -> IoResult<Client> {
        let u = unwrap_or!(from_str::<Url>(uri), return Err(IoError {
            kind: InvalidInput,
            desc: "Redis URL did not parse",
            detail: None,
        }));
        ensure!(u.scheme.as_slice() == "redis", Err(IoError {
            kind: InvalidInput,
            desc: "URL provided is not a redis URL",
            detail: None,
        }));

        Ok(Client {
            host: u.host,
            port: u.port.unwrap_or(6379),
            db: match u.path.to_string().as_slice().trim_chars('/') {
                "" => 0,
                path => unwrap_or!(from_str::<i64>(path), return Err(IoError {
                    kind: InvalidInput,
                    desc: "Path is not a valid redis database number",
                    detail: None,
                }))
            },
        })
    }

    /// Instructs the client to actually connect to redis and returns a
    /// connection object.  The connection object can be used to send
    /// commands to the server.  This can fail with a variety of errors
    /// (like unreachable host) so it's important that you handle those
    /// errors.
    pub fn get_connection(&self) -> IoResult<Connection> {
        connect(self.host.as_slice(), self.port, self.db)
    }

    /// Returns a PubSub connection.  A pubsub connection can be used to
    /// listen to messages coming in through the redis publish/subscribe
    /// system.
    ///
    /// Note that redis' pubsub operates across all databases.
    pub fn get_pubsub(&self) -> IoResult<PubSub> {
        connect_pubsub(self.host.as_slice(), self.port)
    }
}
