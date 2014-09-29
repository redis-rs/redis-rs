use std::io::{Reader, Writer, IoResult, IoError, ConnectionFailed};
use std::io::net::tcp::TcpStream;
use std::cell::RefCell;
use std::collections::HashSet;

use cmd::cmd;
use types::{RedisResult, Okay, Error, Value, InternalIoError,
            ToRedisArgs, FromRedisValue};
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
    pub unsafe fn send_packed_command(&self, cmd: &[u8]) -> RedisResult<()> {
        try!(self.con.borrow_mut().send_bytes(cmd));
        Ok(())
    }

    /// Fetches a single response from the connection.  This is useful
    /// if used in combination with `send_packed_command`.
    pub unsafe fn recv_response(&self) -> RedisResult<Value> {
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
/// let mut pubsub = client.pubsub();
/// pubsub.subscribe("channel_1").unwrap();
/// pubsub.subscribe("channel_2").unwrap();
///
/// loop {
///     let (channel, message) : (String, String) = pubsub.get_message();
///     println!("channel '{}': {}", channel, message);
/// }
/// ```
impl PubSub {

    fn get_channel<T: ToRedisArgs>(&mut self, channel: &T) -> Vec<u8> {
        let mut chan = vec![];
        for item in channel.to_redis_args().iter() {
            chan.push_all(item[]);
        }
        chan
    }

    /// Subscribes to a new channel.
    pub fn subscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        let chan = self.get_channel(&channel);
        let _ : () = try!(cmd("SUBSCRIBE").arg(chan[]).query(&self.con));
        self.channels.insert(chan);
        Ok(())
    }

    /// Unsubscribes from a channel.
    pub fn unsubscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        let chan = self.get_channel(&channel);
        let _ : () = try!(cmd("UNSUBSCRIBE").arg(chan[]).query(&self.con));
        self.channels.remove(&chan);
        Ok(())
    }

    /// Fetches the next message from the pubsub connection.  Blocks until
    /// a message becomes available.  This currently does not provide a
    /// wait not to block :(
    pub fn get_message<C: FromRedisValue, M: FromRedisValue>(&self) -> RedisResult<(C, M)> {
        loop {
            unsafe {
                let (msg_type, channel, msg) : (String, C, M) =
                    try!(FromRedisValue::from_redis_value(&try!(self.con.recv_response())));
                if msg_type.as_slice() == "message" {
                    return Ok((channel, msg))
                }
            }
        }
    }
}
