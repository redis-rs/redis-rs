use std::fmt;
use std::io::{self, Write};
use std::net::{self, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::str::from_utf8;
use std::time::Duration;

use crate::cmd::{cmd, pipe, Pipeline};
use crate::parser::Parser;
use crate::types::{
    from_redis_value, ErrorKind, FromRedisValue, RedisError, RedisResult, ToRedisArgs, Value,
};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

static DEFAULT_PORT: u16 = 6379;

/// This function takes a redis URL string and parses it into a URL
/// as used by rust-url.  This is necessary as the default parser does
/// not understand how redis URLs function.
pub fn parse_redis_url(input: &str) -> Result<url::Url, ()> {
    match url::Url::parse(input) {
        Ok(result) => match result.scheme() {
            "redis" | "redis+unix" | "unix" => Ok(result),
            _ => Err(()),
        },
        Err(_) => Err(()),
    }
}

/// Defines the connection address.
///
/// Not all connection addresses are supported on all platforms.  For instance
/// to connect to a unix socket you need to run this on an operating system
/// that supports them.
#[derive(Clone, Debug, PartialEq)]
pub enum ConnectionAddr {
    /// Format for this is `(host, port)`.
    Tcp(String, u16),
    /// Format for this is the path to the unix socket.
    Unix(PathBuf),
}

impl ConnectionAddr {
    /// Checks if this address is supported.
    ///
    /// Because not all platforms support all connection addresses this is a
    /// quick way to figure out if a connection method is supported.  Currently
    /// this only affects unix connections which are only supported on unix
    /// platforms and on older versions of rust also require an explicit feature
    /// to be enabled.
    pub fn is_supported(&self) -> bool {
        match *self {
            ConnectionAddr::Tcp(_, _) => true,
            ConnectionAddr::Unix(_) => cfg!(unix),
        }
    }
}

impl fmt::Display for ConnectionAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConnectionAddr::Tcp(ref host, port) => write!(f, "{}:{}", host, port),
            ConnectionAddr::Unix(ref path) => write!(f, "{}", path.display()),
        }
    }
}

/// Holds the connection information that redis should use for connecting.
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    /// A boxed connection address for where to connect to.
    pub addr: Box<ConnectionAddr>,
    /// The database number to use.  This is usually `0`.
    pub db: i64,
    /// Optionally a username that should be used for connection.
    pub username: Option<String>,
    /// Optionally a password that should be used for connection.
    pub passwd: Option<String>,
}

/// Converts an object into a connection info struct.  This allows the
/// constructor of the client to accept connection information in a
/// range of different formats.
pub trait IntoConnectionInfo {
    /// Converts the object into a connection info object.
    fn into_connection_info(self) -> RedisResult<ConnectionInfo>;
}

impl IntoConnectionInfo for ConnectionInfo {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        Ok(self)
    }
}

impl<'a> IntoConnectionInfo for &'a str {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        match parse_redis_url(self) {
            Ok(u) => u.into_connection_info(),
            Err(_) => fail!((ErrorKind::InvalidClientConfig, "Redis URL did not parse")),
        }
    }
}

impl IntoConnectionInfo for String {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        match parse_redis_url(&self) {
            Ok(u) => u.into_connection_info(),
            Err(_) => fail!((ErrorKind::InvalidClientConfig, "Redis URL did not parse")),
        }
    }
}

fn url_to_tcp_connection_info(url: url::Url) -> RedisResult<ConnectionInfo> {
    Ok(ConnectionInfo {
        addr: Box::new(ConnectionAddr::Tcp(
            match url.host() {
                Some(host) => host.to_string(),
                None => fail!((ErrorKind::InvalidClientConfig, "Missing hostname")),
            },
            url.port().unwrap_or(DEFAULT_PORT),
        )),
        db: match url.path().trim_matches('/') {
            "" => 0,
            path => unwrap_or!(
                path.parse::<i64>().ok(),
                fail!((ErrorKind::InvalidClientConfig, "Invalid database number"))
            ),
        },
        username: if url.username().is_empty() {
            None
        } else {
            match percent_encoding::percent_decode(url.username().as_bytes()).decode_utf8() {
                Ok(decoded) => Some(decoded.into_owned()),
                Err(_) => fail!((
                    ErrorKind::InvalidClientConfig,
                    "Username is not valid UTF-8 string"
                )),
            }
        },
        passwd: match url.password() {
            Some(pw) => match percent_encoding::percent_decode(pw.as_bytes()).decode_utf8() {
                Ok(decoded) => Some(decoded.into_owned()),
                Err(_) => fail!((
                    ErrorKind::InvalidClientConfig,
                    "Password is not valid UTF-8 string"
                )),
            },
            None => None,
        },
    })
}

#[cfg(unix)]
fn url_to_unix_connection_info(url: url::Url) -> RedisResult<ConnectionInfo> {
    Ok(ConnectionInfo {
        addr: Box::new(ConnectionAddr::Unix(unwrap_or!(
            url.to_file_path().ok(),
            fail!((ErrorKind::InvalidClientConfig, "Missing path"))
        ))),
        db: match url.query_pairs().find(|&(ref key, _)| key == "db") {
            Some((_, db)) => unwrap_or!(
                db.parse::<i64>().ok(),
                fail!((ErrorKind::InvalidClientConfig, "Invalid database number"))
            ),
            None => 0,
        },
        username: Some(url.username().to_string()).filter(|user| !user.is_empty()),
        passwd: url.password().map(|pw| pw.to_string()),
    })
}

#[cfg(not(unix))]
fn url_to_unix_connection_info(_: url::Url) -> RedisResult<ConnectionInfo> {
    fail!((
        ErrorKind::InvalidClientConfig,
        "Unix sockets are not available on this platform."
    ));
}

impl IntoConnectionInfo for url::Url {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        if self.scheme() == "redis" {
            url_to_tcp_connection_info(self)
        } else if self.scheme() == "unix" || self.scheme() == "redis+unix" {
            url_to_unix_connection_info(self)
        } else {
            fail!((
                ErrorKind::InvalidClientConfig,
                "URL provided is not a redis URL"
            ));
        }
    }
}

struct TcpConnection {
    reader: TcpStream,
    open: bool,
}

#[cfg(unix)]
struct UnixConnection {
    sock: UnixStream,
    open: bool,
}

enum ActualConnection {
    Tcp(TcpConnection),
    #[cfg(unix)]
    Unix(UnixConnection),
}

/// Represents a stateful redis TCP connection.
pub struct Connection {
    con: ActualConnection,
    parser: Parser,
    db: i64,

    /// Flag indicating whether the connection was left in the PubSub state after dropping `PubSub`.
    ///
    /// This flag is checked when attempting to send a command, and if it's raised, we attempt to
    /// exit the pubsub state before executing the new request.
    pubsub: bool,
}

/// Represents a pubsub connection.
pub struct PubSub<'a> {
    con: &'a mut Connection,
}

/// Represents a pubsub message.
#[derive(Debug)]
pub struct Msg {
    payload: Value,
    channel: Value,
    pattern: Option<Value>,
}

impl ActualConnection {
    pub fn new(addr: &ConnectionAddr, timeout: Option<Duration>) -> RedisResult<ActualConnection> {
        Ok(match *addr {
            ConnectionAddr::Tcp(ref host, ref port) => {
                let host: &str = &*host;
                let tcp = match timeout {
                    None => TcpStream::connect((host, *port))?,
                    Some(timeout) => {
                        let mut tcp = None;
                        let mut last_error = None;
                        for addr in (host, *port).to_socket_addrs()? {
                            match TcpStream::connect_timeout(&addr, timeout) {
                                Ok(l) => {
                                    tcp = Some(l);
                                    break;
                                }
                                Err(e) => {
                                    last_error = Some(e);
                                }
                            };
                        }
                        match (tcp, last_error) {
                            (Some(tcp), _) => tcp,
                            (None, Some(e)) => {
                                fail!(e);
                            }
                            (None, None) => {
                                fail!((
                                    ErrorKind::InvalidClientConfig,
                                    "could not resolve to any addresses"
                                ));
                            }
                        }
                    }
                };
                ActualConnection::Tcp(TcpConnection {
                    reader: tcp,
                    open: true,
                })
            }
            #[cfg(unix)]
            ConnectionAddr::Unix(ref path) => ActualConnection::Unix(UnixConnection {
                sock: UnixStream::connect(path)?,
                open: true,
            }),
            #[cfg(not(unix))]
            ConnectionAddr::Unix(ref _path) => {
                fail!((
                    ErrorKind::InvalidClientConfig,
                    "Cannot connect to unix sockets \
                     on this platform"
                ));
            }
        })
    }

    pub fn send_bytes(&mut self, bytes: &[u8]) -> RedisResult<Value> {
        match *self {
            ActualConnection::Tcp(ref mut connection) => {
                let res = connection.reader.write_all(bytes).map_err(RedisError::from);
                match res {
                    Err(e) => {
                        if e.is_connection_dropped() {
                            connection.open = false;
                        }
                        Err(e)
                    }
                    Ok(_) => Ok(Value::Okay),
                }
            }
            #[cfg(unix)]
            ActualConnection::Unix(ref mut connection) => {
                let result = connection.sock.write_all(bytes).map_err(RedisError::from);
                match result {
                    Err(e) => {
                        if e.is_connection_dropped() {
                            connection.open = false;
                        }
                        Err(e)
                    }
                    Ok(_) => Ok(Value::Okay),
                }
            }
        }
    }

    pub fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        match *self {
            ActualConnection::Tcp(TcpConnection { ref reader, .. }) => {
                reader.set_write_timeout(dur)?;
            }
            #[cfg(unix)]
            ActualConnection::Unix(UnixConnection { ref sock, .. }) => {
                sock.set_write_timeout(dur)?;
            }
        }
        Ok(())
    }

    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        match *self {
            ActualConnection::Tcp(TcpConnection { ref reader, .. }) => {
                reader.set_read_timeout(dur)?;
            }
            #[cfg(unix)]
            ActualConnection::Unix(UnixConnection { ref sock, .. }) => {
                sock.set_read_timeout(dur)?;
            }
        }
        Ok(())
    }

    pub fn is_open(&self) -> bool {
        match *self {
            ActualConnection::Tcp(TcpConnection { open, .. }) => open,
            #[cfg(unix)]
            ActualConnection::Unix(UnixConnection { open, .. }) => open,
        }
    }
}

pub fn connect(
    connection_info: &ConnectionInfo,
    timeout: Option<Duration>,
) -> RedisResult<Connection> {
    let con = ActualConnection::new(&connection_info.addr, timeout)?;
    let mut rv = Connection {
        con,
        parser: Parser::new(),
        db: connection_info.db,
        pubsub: false,
    };

    if let Some(passwd) = &connection_info.passwd {
        let mut command = cmd("AUTH");
        if let Some(username) = &connection_info.username {
            command.arg(username);
        }
        match command.arg(passwd).query::<Value>(&mut rv) {
            Ok(Value::Okay) => {}
            _ => {
                fail!((
                    ErrorKind::AuthenticationFailed,
                    "Password authentication failed"
                ));
            }
        }
    }

    if connection_info.db != 0 {
        match cmd("SELECT")
            .arg(connection_info.db)
            .query::<Value>(&mut rv)
        {
            Ok(Value::Okay) => {}
            _ => fail!((
                ErrorKind::ResponseError,
                "Redis server refused to switch database"
            )),
        }
    }

    Ok(rv)
}

/// Implements the "stateless" part of the connection interface that is used by the
/// different objects in redis-rs.  Primarily it obviously applies to `Connection`
/// object but also some other objects implement the interface (for instance
/// whole clients or certain redis results).
///
/// Generally clients and connections (as well as redis results of those) implement
/// this trait.  Actual connections provide more functionality which can be used
/// to implement things like `PubSub` but they also can modify the intrinsic
/// state of the TCP connection.  This is not possible with `ConnectionLike`
/// implementors because that functionality is not exposed.
pub trait ConnectionLike {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value>;

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>>;

    /// Returns the database this connection is bound to.  Note that this
    /// information might be unreliable because it's initially cached and
    /// also might be incorrect if the connection like object is not
    /// actually connected.
    fn get_db(&self) -> i64;

    /// Does this connection support pipelining?
    #[doc(hidden)]
    fn supports_pipelining(&self) -> bool {
        true
    }

    /// Check that all connections it has are available (`PING` internally).
    fn check_connection(&mut self) -> bool;

    /// Returns the connection status.
    ///
    /// The connection is open until any `read_response` call recieved an
    /// invalid response from the server (most likely a closed or dropped
    /// connection, otherwise a Redis protocol error). When using unix
    /// sockets the connection is open until writing a command failed with a
    /// `BrokenPipe` error.
    fn is_open(&self) -> bool;
}

/// A connection is an object that represents a single redis connection.  It
/// provides basic support for sending encoded commands into a redis connection
/// and to read a response from it.  It's bound to a single database and can
/// only be created from the client.
///
/// You generally do not much with this object other than passing it to
/// `Cmd` objects.
impl Connection {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// does not read a response.  This is useful for commands like
    /// `MONITOR` which yield multiple items.  This needs to be used with
    /// care because it changes the state of the connection.
    pub fn send_packed_command(&mut self, cmd: &[u8]) -> RedisResult<()> {
        self.con.send_bytes(cmd)?;
        Ok(())
    }

    /// Fetches a single response from the connection.  This is useful
    /// if used in combination with `send_packed_command`.
    pub fn recv_response(&mut self) -> RedisResult<Value> {
        self.read_response()
    }

    /// Sets the write timeout for the connection.
    ///
    /// If the provided value is `None`, then `send_packed_command` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.con.set_write_timeout(dur)
    }

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `recv_response` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.con.set_read_timeout(dur)
    }

    /// Creates a [`PubSub`] instance for this connection.
    pub fn as_pubsub(&mut self) -> PubSub<'_> {
        // NOTE: The pubsub flag is intentionally not raised at this time since
        // running commands within the pubsub state should not try and exit from
        // the pubsub state.
        PubSub::new(self)
    }

    fn exit_pubsub(&mut self) -> RedisResult<()> {
        let res = self.clear_active_subscriptions();
        if res.is_ok() {
            self.pubsub = false;
        } else {
            // Raise the pubsub flag to indicate the connection is "stuck" in that state.
            self.pubsub = true;
        }

        res
    }

    /// Get the inner connection out of a PubSub
    ///
    /// Any active subscriptions are unsubscribed. In the event of an error, the connection is
    /// dropped.
    fn clear_active_subscriptions(&mut self) -> RedisResult<()> {
        // Responses to unsubscribe commands return in a 3-tuple with values
        // ("unsubscribe" or "punsubscribe", name of subscription removed, count of remaining subs).
        // The "count of remaining subs" includes both pattern subscriptions and non pattern
        // subscriptions. Thus, to accurately drain all unsubscribe messages received from the
        // server, both commands need to be executed at once.
        {
            // Prepare both unsubscribe commands
            let unsubscribe = cmd("UNSUBSCRIBE").get_packed_command();
            let punsubscribe = cmd("PUNSUBSCRIBE").get_packed_command();

            // Grab a reference to the underlying connection so that we may send
            // the commands without immediately blocking for a response.
            let con = &mut self.con;

            // Execute commands
            con.send_bytes(&unsubscribe)?;
            con.send_bytes(&punsubscribe)?;
        }

        // Receive responses
        //
        // There will be at minimum two responses - 1 for each of punsubscribe and unsubscribe
        // commands. There may be more responses if there are active subscriptions. In this case,
        // messages are received until the _subscription count_ in the responses reach zero.
        let mut received_unsub = false;
        let mut received_punsub = false;
        loop {
            let res: (Vec<u8>, (), isize) = from_redis_value(&self.recv_response()?)?;

            match res.0.first() {
                Some(&b'u') => received_unsub = true,
                Some(&b'p') => received_punsub = true,
                _ => (),
            }

            if received_unsub && received_punsub && res.2 == 0 {
                break;
            }
        }

        // Finally, the connection is back in its normal state since all subscriptions were
        // cancelled *and* all unsubscribe messages were received.
        Ok(())
    }

    /// Fetches a single response from the connection.
    fn read_response(&mut self) -> RedisResult<Value> {
        let result = match self.con {
            ActualConnection::Tcp(TcpConnection { ref mut reader, .. }) => {
                self.parser.parse_value(reader)
            }
            #[cfg(unix)]
            ActualConnection::Unix(UnixConnection { ref mut sock, .. }) => {
                self.parser.parse_value(sock)
            }
        };
        // shutdown connection on protocol error
        if let Err(e) = &result {
            let shutdown = e.kind() == ErrorKind::ResponseError
                || match e.as_io_error() {
                    Some(e) => e.kind() == io::ErrorKind::UnexpectedEof,
                    None => false,
                };
            if shutdown {
                match self.con {
                    ActualConnection::Tcp(ref mut connection) => {
                        let _ = connection.reader.shutdown(net::Shutdown::Both);
                        connection.open = false;
                    }
                    #[cfg(unix)]
                    ActualConnection::Unix(ref mut connection) => {
                        let _ = connection.sock.shutdown(net::Shutdown::Both);
                        connection.open = false;
                    }
                }
            }
        }
        result
    }
}

impl ConnectionLike for Connection {
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        if self.pubsub {
            self.exit_pubsub()?;
        }

        self.con.send_bytes(cmd)?;
        self.read_response()
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        if self.pubsub {
            self.exit_pubsub()?;
        }
        self.con.send_bytes(cmd)?;
        let mut rv = vec![];
        for idx in 0..(offset + count) {
            let item = self.read_response()?;
            if idx >= offset {
                rv.push(item);
            }
        }
        Ok(rv)
    }

    fn get_db(&self) -> i64 {
        self.db
    }

    fn is_open(&self) -> bool {
        self.con.is_open()
    }

    fn check_connection(&mut self) -> bool {
        cmd("PING").query::<String>(self).is_ok()
    }
}

/// The pubsub object provides convenient access to the redis pubsub
/// system.  Once created you can subscribe and unsubscribe from channels
/// and listen in on messages.
///
/// Example:
///
/// ```rust,no_run
/// # fn do_something() -> redis::RedisResult<()> {
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let mut con = client.get_connection()?;
/// let mut pubsub = con.as_pubsub();
/// pubsub.subscribe("channel_1")?;
/// pubsub.subscribe("channel_2")?;
///
/// loop {
///     let msg = pubsub.get_message()?;
///     let payload : String = msg.get_payload()?;
///     println!("channel '{}': {}", msg.get_channel_name(), payload);
/// }
/// # }
/// ```
impl<'a> PubSub<'a> {
    fn new(con: &'a mut Connection) -> Self {
        Self { con }
    }

    /// Subscribes to a new channel.
    pub fn subscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        Ok(cmd("SUBSCRIBE").arg(channel).query(self.con)?)
    }

    /// Subscribes to a new channel with a pattern.
    pub fn psubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        Ok(cmd("PSUBSCRIBE").arg(pchannel).query(self.con)?)
    }

    /// Unsubscribes from a channel.
    pub fn unsubscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        Ok(cmd("UNSUBSCRIBE").arg(channel).query(self.con)?)
    }

    /// Unsubscribes from a channel with a pattern.
    pub fn punsubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        Ok(cmd("PUNSUBSCRIBE").arg(pchannel).query(self.con)?)
    }

    /// Fetches the next message from the pubsub connection.  Blocks until
    /// a message becomes available.  This currently does not provide a
    /// wait not to block :(
    ///
    /// The message itself is still generic and can be converted into an
    /// appropriate type through the helper methods on it.
    pub fn get_message(&mut self) -> RedisResult<Msg> {
        loop {
            if let Some(msg) = Msg::from_value(&self.con.recv_response()?) {
                return Ok(msg);
            } else {
                continue;
            }
        }
    }

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `get_message` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.con.set_read_timeout(dur)
    }
}

impl<'a> Drop for PubSub<'a> {
    fn drop(&mut self) {
        let _ = self.con.exit_pubsub();
    }
}

/// This holds the data that comes from listening to a pubsub
/// connection.  It only contains actual message data.
impl Msg {
    /// Tries to convert provided [`Value`] into [`Msg`].
    pub fn from_value(value: &Value) -> Option<Self> {
        let raw_msg: Vec<Value> = from_redis_value(value).ok()?;
        let mut iter = raw_msg.into_iter();
        let msg_type: String = from_redis_value(&iter.next()?).ok()?;
        let mut pattern = None;
        let payload;
        let channel;

        if msg_type == "message" {
            channel = iter.next()?;
            payload = iter.next()?;
        } else if msg_type == "pmessage" {
            pattern = Some(iter.next()?);
            channel = iter.next()?;
            payload = iter.next()?;
        } else {
            return None;
        }

        Some(Msg {
            payload,
            channel,
            pattern,
        })
    }

    /// Returns the channel this message came on.
    pub fn get_channel<T: FromRedisValue>(&self) -> RedisResult<T> {
        from_redis_value(&self.channel)
    }

    /// Convenience method to get a string version of the channel.  Unless
    /// your channel contains non utf-8 bytes you can always use this
    /// method.  If the channel is not a valid string (which really should
    /// not happen) then the return value is `"?"`.
    pub fn get_channel_name(&self) -> &str {
        match self.channel {
            Value::Data(ref bytes) => from_utf8(bytes).unwrap_or("?"),
            _ => "?",
        }
    }

    /// Returns the message's payload in a specific format.
    pub fn get_payload<T: FromRedisValue>(&self) -> RedisResult<T> {
        from_redis_value(&self.payload)
    }

    /// Returns the bytes that are the message's payload.  This can be used
    /// as an alternative to the `get_payload` function if you are interested
    /// in the raw bytes in it.
    pub fn get_payload_bytes(&self) -> &[u8] {
        match self.payload {
            Value::Data(ref bytes) => bytes,
            _ => b"",
        }
    }

    /// Returns true if the message was constructed from a pattern
    /// subscription.
    #[allow(clippy::wrong_self_convention)]
    pub fn from_pattern(&self) -> bool {
        self.pattern.is_some()
    }

    /// If the message was constructed from a message pattern this can be
    /// used to find out which one.  It's recommended to match against
    /// an `Option<String>` so that you do not need to use `from_pattern`
    /// to figure out if a pattern was set.
    pub fn get_pattern<T: FromRedisValue>(&self) -> RedisResult<T> {
        match self.pattern {
            None => from_redis_value(&Value::Nil),
            Some(ref x) => from_redis_value(x),
        }
    }
}

/// This function simplifies transaction management slightly.  What it
/// does is automatically watching keys and then going into a transaction
/// loop util it succeeds.  Once it goes through the results are
/// returned.
///
/// To use the transaction two pieces of information are needed: a list
/// of all the keys that need to be watched for modifications and a
/// closure with the code that should be execute in the context of the
/// transaction.  The closure is invoked with a fresh pipeline in atomic
/// mode.  To use the transaction the function needs to return the result
/// from querying the pipeline with the connection.
///
/// The end result of the transaction is then available as the return
/// value from the function call.
///
/// Example:
///
/// ```rust,no_run
/// use redis::Commands;
/// # fn do_something() -> redis::RedisResult<()> {
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let mut con = client.get_connection().unwrap();
/// let key = "the_key";
/// let (new_val,) : (isize,) = redis::transaction(&mut con, &[key], |con, pipe| {
///     let old_val : isize = con.get(key)?;
///     pipe
///         .set(key, old_val + 1).ignore()
///         .get(key).query(con)
/// })?;
/// println!("The incremented number is: {}", new_val);
/// # Ok(()) }
/// ```
pub fn transaction<
    C: ConnectionLike,
    K: ToRedisArgs,
    T,
    F: FnMut(&mut C, &mut Pipeline) -> RedisResult<Option<T>>,
>(
    con: &mut C,
    keys: &[K],
    func: F,
) -> RedisResult<T> {
    let mut func = func;
    loop {
        cmd("WATCH").arg(keys).query::<()>(con)?;
        let mut p = pipe();
        let response: Option<T> = func(con, p.atomic())?;
        match response {
            None => {
                continue;
            }
            Some(response) => {
                // make sure no watch is left in the connection, even if
                // someone forgot to use the pipeline.
                cmd("UNWATCH").query::<()>(con)?;
                return Ok(response);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_redis_url() {
        let cases = vec![
            ("redis://127.0.0.1", true),
            ("redis+unix:///run/redis.sock", true),
            ("unix:///run/redis.sock", true),
            ("http://127.0.0.1", false),
            ("tcp://127.0.0.1", false),
        ];
        for (url, expected) in cases.into_iter() {
            let res = parse_redis_url(&url);
            assert_eq!(
                res.is_ok(),
                expected,
                "Parsed result of `{}` is not expected",
                url,
            );
        }
    }

    #[test]
    fn test_url_to_tcp_connection_info() {
        let cases = vec![
            (
                url::Url::parse("redis://127.0.0.1").unwrap(),
                ConnectionInfo {
                    addr: Box::new(ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379)),
                    db: 0,
                    username: None,
                    passwd: None,
                },
            ),
            (
                url::Url::parse("redis://%25johndoe%25:%23%40%3C%3E%24@example.com/2").unwrap(),
                ConnectionInfo {
                    addr: Box::new(ConnectionAddr::Tcp("example.com".to_string(), 6379)),
                    db: 2,
                    username: Some("%johndoe%".to_string()),
                    passwd: Some("#@<>$".to_string()),
                },
            ),
        ];
        for (url, expected) in cases.into_iter() {
            let res = url_to_tcp_connection_info(url.clone()).unwrap();
            assert_eq!(res.addr, expected.addr, "addr of {} is not expected", url);
            assert_eq!(res.db, expected.db, "db of {} is not expected", url);
            assert_eq!(
                res.username, expected.username,
                "username of {} is not expected",
                url
            );
            assert_eq!(
                res.passwd, expected.passwd,
                "passwd of {} is not expected",
                url
            );
        }
    }

    #[test]
    fn test_url_to_tcp_connection_info_failed() {
        let cases = vec![
            (url::Url::parse("redis://").unwrap(), "Missing hostname"),
            (
                url::Url::parse("redis://127.0.0.1/db").unwrap(),
                "Invalid database number",
            ),
            (
                url::Url::parse("redis://C3%B0@127.0.0.1").unwrap(),
                "Username is not valid UTF-8 string",
            ),
            (
                url::Url::parse("redis://:C3%B0@127.0.0.1").unwrap(),
                "Password is not valid UTF-8 string",
            ),
        ];
        for (url, expected) in cases.into_iter() {
            let res = url_to_tcp_connection_info(url);
            assert_eq!(
                res.as_ref().unwrap_err().kind(),
                crate::ErrorKind::InvalidClientConfig,
                "{}",
                res.as_ref().unwrap_err(),
            );
            assert_eq!(
                res.as_ref().unwrap_err().to_string(),
                expected,
                "{}",
                res.as_ref().unwrap_err(),
            );
        }
    }

    #[ignore]
    #[test]
    #[cfg(unix)]
    fn test_url_to_unix_connection_info() {}
}
