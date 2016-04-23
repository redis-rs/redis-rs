#[cfg(feature="unix_socket")]
use std::path::PathBuf;
use std::io::{Read, BufReader, Write};
use std::net::{self, TcpStream};
use std::str::from_utf8;
use std::cell::RefCell;
use std::collections::HashSet;
use std::time::Duration;

use url;

use cmd::{cmd, pipe, Pipeline};
use types::{RedisResult, Value, ToRedisArgs, FromRedisValue, from_redis_value,
            ErrorKind};
use parser::Parser;

#[cfg(feature="unix_socket")]
use unix_socket::UnixStream;


static DEFAULT_PORT: u16 = 6379;

/// This function takes a redis URL string and parses it into a URL
/// as used by rust-url.  This is necessary as the default parser does
/// not understand how redis URLs function.
pub fn parse_redis_url(input: &str) -> RedisResult<url::Url> {
    match url::Url::parse(input) {
        Ok(result) => {
            if result.scheme() == "redis" || result.scheme() == "unix" {
                Ok(result)
            } else {
                fail!((ErrorKind::InvalidClientConfig, "Redis URL did not parse"));
            }
        },
        Err(_) => fail!((ErrorKind::InvalidClientConfig, "Redis URL did not parse")),
    }
}

/// Defines the connection address.
///
/// The fields available on this struct depend on if the crate has been
/// compiled with the `unix_socket` feature or not.  The `Tcp` field
/// is always available, but the `Unix` one is not.
#[derive(Clone, Debug)]
pub enum ConnectionAddr {
    /// Format for this is `(host, port)`.
    Tcp(String, u16),
    /// Format for this is the path to the unix socket.
    #[cfg(feature="unix_socket")]
    Unix(PathBuf),
}


/// Holds the connection information that redis should use for connecting.
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    /// A boxed connection address for where to connect to.
    pub addr: Box<ConnectionAddr>,
    /// The database number to use.  This is usually `0`.
    pub db: i64,
    /// Optionally a password that should be used for connection.
    pub passwd: Option<String>,
}

/// Converts an object into a connection info struct.  This allows the
/// constructor of the client to accept connection information in a
/// range of different formats.
pub trait IntoConnectionInfo {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo>;
}

impl IntoConnectionInfo for ConnectionInfo {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        Ok(self)
    }
}

impl<'a> IntoConnectionInfo for &'a str {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        parse_redis_url(self).and_then(|u| u.into_connection_info())
    }
}

fn url_to_tcp_connection_info(url: url::Url) -> RedisResult<ConnectionInfo> {
    Ok(ConnectionInfo {
        addr: Box::new(ConnectionAddr::Tcp(
            unwrap_or!(url.host_str().into(),
                fail!((ErrorKind::InvalidClientConfig, "Missing hostname"))).into(),
            url.port().unwrap_or(DEFAULT_PORT)
        )),
        db: match url.path()
                .trim_matches('/') {
            "" => 0,
            path => unwrap_or!(path.parse::<i64>().ok(),
                fail!((ErrorKind::InvalidClientConfig, "Invalid database number"))),
        },
        passwd: url.password().and_then(|pw| Some(pw.to_string())),
    })
}

#[cfg(feature="unix_socket")]
fn url_to_unix_connection_info(url: url::Url) -> RedisResult<ConnectionInfo> {
    Ok(ConnectionInfo {
        addr: Box::new(ConnectionAddr::Unix(
            unwrap_or!(url.to_file_path().ok(),
                fail!((ErrorKind::InvalidClientConfig, "Missing path"))),
        )),
        db: match url.query_pairs().filter(|&(ref key, _)| key == "db").next() {
            Some((_, db)) => unwrap_or!(db.parse::<i64>().ok(),
                fail!((ErrorKind::InvalidClientConfig, "Invalid database number"))),
            None => 0,
        },
        passwd: url.password().and_then(|pw| Some(pw.to_string())),
    })
}

#[cfg(not(feature="unix_socket"))]
fn url_to_unix_connection_info(_: url::Url) -> RedisResult<ConnectionInfo> {
    fail!((ErrorKind::InvalidClientConfig,
           "This version of redis-rs is not compiled with Unix socket support."));
}

impl IntoConnectionInfo for url::Url {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        if self.scheme() == "redis" {
            url_to_tcp_connection_info(self)
        } else if self.scheme() == "unix" {
            url_to_unix_connection_info(self)
        } else {
            fail!((ErrorKind::InvalidClientConfig, "URL provided is not a redis URL"));
        }
    }
}


enum ActualConnection {
    Tcp(BufReader<TcpStream>),
    #[cfg(feature="unix_socket")]
    Unix(UnixStream),
}

/// Represents a stateful redis TCP connection.
pub struct Connection {
    con: RefCell<ActualConnection>,
    db: i64,
}

/// Represents a pubsub connection.
pub struct PubSub {
    con: Connection,
    channels: HashSet<Vec<u8>>,
    pchannels: HashSet<Vec<u8>>,
}

/// Represents a pubsub message.
pub struct Msg {
    payload: Value,
    channel: Value,
    pattern: Option<Value>,
}

impl ActualConnection {

    pub fn new(addr: &ConnectionAddr) -> RedisResult<ActualConnection> {
        Ok(match *addr {
            ConnectionAddr::Tcp(ref host, ref port) => {
                let host : &str = &*host;
                let tcp = try!(TcpStream::connect((host, *port)));
                let buffered = BufReader::new(tcp);
                ActualConnection::Tcp(buffered)
            }
            #[cfg(feature="unix_socket")]
            ConnectionAddr::Unix(ref path) => {
                ActualConnection::Unix(try!(UnixStream::connect(path)))
            }
        })
    }

    pub fn send_bytes(&mut self, bytes: &[u8]) -> RedisResult<Value> {
        let w = match *self {
            ActualConnection::Tcp(ref mut reader) => {
                reader.get_mut() as &mut Write
            }
            #[cfg(feature="unix_socket")]
            ActualConnection::Unix(ref mut sock) => {
                &mut *sock as &mut Write
            }
        };
        try!(w.write(bytes));
        Ok(Value::Okay)
    }

    pub fn read_response(&mut self) -> RedisResult<Value> {
        let result = Parser::new(match *self {
            ActualConnection::Tcp(ref mut reader) => {
                reader as &mut Read
            }
            #[cfg(feature="unix_socket")]
            ActualConnection::Unix(ref mut sock) => {
                &mut *sock as &mut Read
            }
        }).parse_value();
        // shutdown connection on protocol error
        match result {
            Err(ref e) if e.kind() == ErrorKind::ResponseError => {
                match *self {
                    ActualConnection::Tcp(ref mut reader) => {
                        let _ = reader.get_mut().shutdown(net::Shutdown::Both);
                    }
                    #[cfg(feature="unix_socket")]
                    ActualConnection::Unix(ref mut sock) => {
                        let _ = sock.shutdown(net::Shutdown::Both);
                    }
                }
            }
            _ => ()
        }
        result
    }

    pub fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        match *self {
            ActualConnection::Tcp(ref reader) => {
                try!(reader.get_ref().set_write_timeout(dur));
            }
            #[cfg(feature="unix_socket")]
            ActualConnection::Unix(ref sock) => {
                try!(sock.set_write_timeout(dur));
            }
        }
        Ok(())
    }

    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        match *self {
            ActualConnection::Tcp(ref reader) => {
                try!(reader.get_ref().set_read_timeout(dur));
            }
            #[cfg(feature="unix_socket")]
            ActualConnection::Unix(ref sock) => {
                try!(sock.set_read_timeout(dur));
            }
        }
        Ok(())
    }
}


pub fn connect(connection_info: &ConnectionInfo) -> RedisResult<Connection> {
    let con = try!(ActualConnection::new(&connection_info.addr));
    let rv = Connection { con: RefCell::new(con), db: connection_info.db };

    match connection_info.passwd {
        Some(ref passwd) => {
            match cmd("AUTH").arg(&**passwd).query::<Value>(&rv) {
                Ok(Value::Okay) => {}
                _ => { fail!((ErrorKind::AuthenticationFailed,
                               "Password authentication failed")); }
            }
        },
        None => {},
    }

    if connection_info.db != 0 {
        match cmd("SELECT").arg(connection_info.db).query::<Value>(&rv) {
            Ok(Value::Okay) => {}
            _ => fail!((ErrorKind::ResponseError, "Redis server refused to switch database"))
        }
    }

    Ok(rv)
}

pub fn connect_pubsub(connection_info: &ConnectionInfo) -> RedisResult<PubSub> {
    Ok(PubSub {
        con: try!(connect(connection_info)),
        channels: HashSet::new(),
        pchannels: HashSet::new(),
    })
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
    fn req_packed_command(&self, cmd: &[u8]) -> RedisResult<Value>;

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    fn req_packed_commands(&self, cmd: &[u8],
        offset: usize, count: usize) -> RedisResult<Vec<Value>>;

    /// Returns the database this connection is bound to.  Note that this
    /// information might be unreliable because it's initially cached and
    /// also might be incorrect if the connection like object is not
    /// actually connected.
    fn get_db(&self) -> i64;
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
    pub fn send_packed_command(&self, cmd: &[u8]) -> RedisResult<()> {
        try!(self.con.borrow_mut().send_bytes(cmd));
        Ok(())
    }

    /// Fetches a single response from the connection.  This is useful
    /// if used in combination with `send_packed_command`.
    pub fn recv_response(&self) -> RedisResult<Value> {
        self.con.borrow_mut().read_response()
    }

    /// Sets the write timeout for the connection.
    ///
    /// If the provided value is `None`, then `send_packed_command` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.con.borrow().set_write_timeout(dur)
    }

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `recv_response` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.con.borrow().set_read_timeout(dur)
    }
}

impl ConnectionLike for Connection {

    fn req_packed_command(&self, cmd: &[u8]) -> RedisResult<Value> {
        let mut con = self.con.borrow_mut();
        try!(con.send_bytes(cmd));
        con.read_response()
    }

    fn req_packed_commands(&self, cmd: &[u8],
        offset: usize, count: usize) -> RedisResult<Vec<Value>> {
        let mut con = self.con.borrow_mut();
        try!(con.send_bytes(cmd));
        let mut rv = vec![];
        for idx in 0..(offset + count) {
            let item = try!(con.read_response());
            if idx >= offset {
                rv.push(item);
            }
        }
        Ok(rv)
    }

    fn get_db(&self) -> i64 {
        self.db
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
/// let client = try!(redis::Client::open("redis://127.0.0.1/"));
/// let mut pubsub = try!(client.get_pubsub());
/// try!(pubsub.subscribe("channel_1"));
/// try!(pubsub.subscribe("channel_2"));
///
/// loop {
///     let msg = try!(pubsub.get_message());
///     let payload : String = try!(msg.get_payload());
///     println!("channel '{}': {}", msg.get_channel_name(), payload);
/// }
/// # }
/// ```
impl PubSub {

    fn get_channel<T: ToRedisArgs>(&mut self, channel: &T) -> Vec<u8> {
        let mut chan = vec![];
        for item in channel.to_redis_args().iter() {
            chan.extend(item.iter().cloned());
        }
        chan
    }

    /// Subscribes to a new channel.
    pub fn subscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        let chan = self.get_channel(&channel);
        let _ : () = try!(cmd("SUBSCRIBE").arg(&*chan).query(&self.con));
        self.channels.insert(chan);
        Ok(())
    }

    /// Subscribes to a new channel with a pattern.
    pub fn psubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        let chan = self.get_channel(&pchannel);
        let _ : () = try!(cmd("PSUBSCRIBE").arg(&*chan).query(&self.con));
        self.pchannels.insert(chan);
        Ok(())
    }

    /// Unsubscribes from a channel.
    pub fn unsubscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        let chan = self.get_channel(&channel);
        let _ : () = try!(cmd("UNSUBSCRIBE").arg(&*chan).query(&self.con));
        self.channels.remove(&chan);
        Ok(())
    }

    /// Unsubscribes from a channel with a pattern.
    pub fn punsubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        let chan = self.get_channel(&pchannel);
        let _ : () = try!(cmd("PUNSUBSCRIBE").arg(&*chan).query(&self.con));
        self.pchannels.remove(&chan);
        Ok(())
    }

    /// Fetches the next message from the pubsub connection.  Blocks until
    /// a message becomes available.  This currently does not provide a
    /// wait not to block :(
    ///
    /// The message itself is still generic and can be converted into an
    /// appropriate type through the helper methods on it.
    pub fn get_message(&self) -> RedisResult<Msg> {
        loop {
            let raw_msg : Vec<Value> = try!(from_redis_value(
                &try!(self.con.recv_response())));
            let mut iter = raw_msg.into_iter();
            let msg_type : String = try!(from_redis_value(
                &unwrap_or!(iter.next(), continue)));
            let mut pattern = None;
            let payload;
            let channel;

            if msg_type == "message" {
                channel = unwrap_or!(iter.next(), continue);
                payload = unwrap_or!(iter.next(), continue);
            } else if msg_type == "pmessage" {
                pattern = Some(unwrap_or!(iter.next(), continue));
                channel = unwrap_or!(iter.next(), continue);
                payload = unwrap_or!(iter.next(), continue);
            } else {
                continue;
            }

            return Ok(Msg {
                payload: payload,
                channel: channel,
                pattern: pattern,
            })
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


/// This holds the data that comes from listening to a pubsub
/// connection.  It only contains actual message data.
impl Msg {

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
            _ => "?"
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
        match self.channel {
            Value::Data(ref bytes) => bytes,
            _ => b""
        }
    }

    /// Returns true if the message was constructed from a pattern
    /// subscription.
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
/// use redis::{Commands, PipelineCommands};
/// # fn do_something() -> redis::RedisResult<()> {
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let con = client.get_connection().unwrap();
/// let key = "the_key";
/// let (new_val,) : (isize,) = try!(redis::transaction(&con, &[key], |pipe| {
///     let old_val : isize = try!(con.get(key));
///     pipe
///         .set(key, old_val + 1).ignore()
///         .get(key).query(&con)
/// }));
/// println!("The incremented number is: {}", new_val);
/// # Ok(()) }
/// ```
pub fn transaction<K: ToRedisArgs, T: FromRedisValue, F: FnMut(&mut Pipeline) -> RedisResult<Option<T>>>
    (con: &ConnectionLike, keys: &[K], func: F) -> RedisResult<T> {
    let mut func = func;
    loop {
        let _ : () = try!(cmd("WATCH").arg(keys).query(con));
        let mut p = pipe();
        let response : Option<T> = try!(func(p.atomic()));
        match response {
            None => { continue; }
            Some(response) => {
                // make sure no watch is left in the connection, even if
                // someone forgot to use the pipeline.
                let _ : () = try!(cmd("UNWATCH").query(con));
                return Ok(response);
            }
        }
    }
}
