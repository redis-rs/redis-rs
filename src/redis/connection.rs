use std::io::{Reader, Writer, IoResult, IoError, ConnectionFailed};
use std::io::net::tcp::TcpStream;
use std::cell::RefCell;

use cmd::cmd;
use types::{RedisResult, Okay, Error, Value, InternalIoError};
use parser::Parser;


struct ActualConnection {
    sock: TcpStream,
}

/// Represents a stateful redis TCP connection.
pub struct Connection {
    con: RefCell<ActualConnection>,
    db: i64,
}

impl ActualConnection {

    pub fn new(host: &str, port: u16) -> IoResult<ActualConnection> {
        let sock = try!(TcpStream::connect(host, port));
        Ok(ActualConnection { sock: sock })
    }

    fn send_bytes(&mut self, bytes: &[u8]) -> RedisResult<Value> {
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

    fn read_response(&mut self) -> RedisResult<Value> {
        let mut parser = Parser::new(&mut self.sock as &mut Reader);
        parser.parse_value()
    }

    pub fn send_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        try!(self.send_bytes(cmd));
        self.read_response()
    }

    pub fn send_packed_commands(&mut self, cmd: &[u8],
            offset: uint, count: uint) -> RedisResult<Vec<Value>> {
        try!(self.send_bytes(cmd));
        let mut rv = vec![];
        for idx in range(0, offset + count) {
            let item = try!(self.read_response());
            if idx >= offset {
                rv.push(item);
            }
        }
        Ok(rv)
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


/// A connection is an object that represents a single redis connection.  It
/// provides basic support for sending encoded commands into a redis connection
/// and to read a response from it.  It's bound to a single database and can
/// only be created from the client.
///
/// You generally do not much with this object other than passing it to
/// `Cmd` objects.
impl Connection {

    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the response from it.
    pub fn send_packed_command(&self, cmd: &[u8]) -> RedisResult<Value> {
        self.con.borrow_mut().send_packed_command(cmd)
    }

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    pub fn send_packed_commands(&self, cmd: &[u8],
            offset: uint, count: uint) -> RedisResult<Vec<Value>> {
        self.con.borrow_mut().send_packed_commands(cmd, offset, count)
    }

    /// Returns the database this connection is bound to.
    pub fn get_db(&self) -> i64 {
        self.db
    }
}
