use std::io::{Reader, Writer, IoResult, IoError, ConnectionFailed};
use std::io::net::tcp::TcpStream;

use cmd::cmd;
use types::{RedisResult, Okay, Error, Value, InternalIoError};
use parser::Parser;


pub struct Connection {
    sock: TcpStream,
    db: i64,
}

impl Connection {

    pub fn new(host: &str, port: u16, db: i64) -> IoResult<Connection> {
        let sock = try!(TcpStream::connect(host, port));

        let mut rv = Connection {
            sock: sock,
            db: db,
        };

        if db != 0 {
            match cmd("SELECT").arg(db).execute::<Value>(&mut rv) {
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

    pub fn send_request(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        try!(self.send_bytes(cmd));
        self.read_response()
    }

    pub fn get_db(&self) -> i64 {
        self.db
    }
}
