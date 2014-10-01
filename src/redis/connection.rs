use std::io::{Reader, Writer, IoResult, IoError, ConnectionFailed};
use std::io::net::tcp::TcpStream;
use std::cell::RefCell;
use std::collections::HashSet;
use std::str;

use cmd::cmd;
use types::{RedisResult, Okay, Error, Value, Data, Nil, InternalIoError,
            ToRedisArgs, FromRedisValue, from_redis_value};
use parser::Parser;


struct ActualConnection {
    sock: TcpStream,
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

    pub fn new(host: &str, port: u16) -> IoResult<ActualConnection> {
        let sock = try!(TcpStream::connect(host, port));
        Ok(ActualConnection { sock: sock })
    }

    pub fn send_bytes(&mut self, bytes: &[u8]) -> RedisResult<Value> {
        let w = &mut self.sock as &mut Writer;
        match w.write(bytes) {
            Err(err) => {
                Err(Error::simple(
                    InternalIoError(err),
                    "Could not send command because of an IO error"))
            },
            Ok(_) => Ok(Okay)
        }
    }

    pub fn read_response(&mut self) -> RedisResult<Value> {
        let mut parser = Parser::new(&mut self.sock as &mut Reader);
        parser.parse_value()
    }
}


pub fn connect(host: &str, port: u16, db: i64) -> IoResult<Connection> {
    let con = try!(ActualConnection::new(host, port));
    let rv = Connection { con: RefCell::new(con), db: db };
    if db != 0 {
        match cmd("SELECT").arg(db).query::<Value>(&rv) {
            Ok(Okay) => {}
            _ => { return Err(IoError {
                kind: ConnectionFailed,
                desc: "Redis server refused to switch database",
                detail: None,
            }); }
        }
    }
    Ok(rv)
}

pub fn connect_pubsub(host: &str, port: u16) -> IoResult<PubSub> {
    Ok(PubSub {
        con: try!(connect(host, port, 0)),
        channels: HashSet::new(),
        pchannels: HashSet::new(),
    })
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
    /// reads the single response from it.
    pub fn req_packed_command(&self, cmd: &[u8]) -> RedisResult<Value> {
        let mut con = self.con.borrow_mut();
        try!(con.send_bytes(cmd));
        con.read_response()
    }

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

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    pub fn req_packed_commands(&self, cmd: &[u8],
            offset: uint, count: uint) -> RedisResult<Vec<Value>> {
        let mut con = self.con.borrow_mut();
        try!(con.send_bytes(cmd));
        let mut rv = vec![];
        for idx in range(0, offset + count) {
            let item = try!(con.read_response());
            if idx >= offset {
                rv.push(item);
            }
        }
        Ok(rv)
    }

    /// Returns the database this connection is bound to.
    pub fn get_db(&self) -> i64 {
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
/// let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// let mut pubsub = client.get_pubsub().unwrap();
/// pubsub.subscribe("channel_1").unwrap();
/// pubsub.subscribe("channel_2").unwrap();
///
/// loop {
///     let msg = pubsub.get_message().unwrap();
///     let payload : String = msg.get_payload().unwrap();
///     println!("channel '{}': {}", msg.get_channel_name(), payload);
/// }
/// ```
impl PubSub {

    fn get_channel<T: ToRedisArgs>(&mut self, channel: &T) -> Vec<u8> {
        let mut chan = vec![];
        for item in channel.to_redis_args().iter() {
            chan.push_all(item.as_slice());
        }
        chan
    }

    /// Subscribes to a new channel.
    pub fn subscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        let chan = self.get_channel(&channel);
        let _ : () = try!(cmd("SUBSCRIBE").arg(chan.as_slice()).query(&self.con));
        self.channels.insert(chan);
        Ok(())
    }

    /// Subscribes to a new channel with a pattern.
    pub fn psubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        let chan = self.get_channel(&pchannel);
        let _ : () = try!(cmd("PSUBSCRIBE").arg(chan.as_slice()).query(&self.con));
        self.pchannels.insert(chan);
        Ok(())
    }

    /// Unsubscribes from a channel.
    pub fn unsubscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        let chan = self.get_channel(&channel);
        let _ : () = try!(cmd("UNSUBSCRIBE").arg(chan.as_slice()).query(&self.con));
        self.channels.remove(&chan);
        Ok(())
    }

    /// Unsubscribes from a channel with a pattern.
    pub fn punsubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        let chan = self.get_channel(&pchannel);
        let _ : () = try!(cmd("PUNSUBSCRIBE").arg(chan.as_slice()).query(&self.con));
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
            let mut payload;
            let mut channel;

            if msg_type.as_slice() == "message" {
                channel = unwrap_or!(iter.next(), continue);
                payload = unwrap_or!(iter.next(), continue);
            } else if msg_type.as_slice() == "pmessage" {
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
    pub fn get_channel_name<'a>(&'a self) -> &'a str {
        match self.channel {
            Data(ref bytes) => str::from_utf8(bytes.as_slice()).unwrap_or("?"),
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
    pub fn get_payload_bytes<'a>(&'a self) -> &'a [u8] {
        match self.channel {
            Data(ref bytes) => bytes.as_slice(),
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
            None => from_redis_value(&Nil),
            Some(ref x) => from_redis_value(x),
        }
    }
}
