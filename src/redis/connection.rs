use std::io::{Reader, Writer, IoResult, IoError, ConnectionFailed};
use std::io::net::tcp::TcpStream;

use types::{CmdArg, StrArg, IntArg, FloatArg, BytesArg,
            RedisResult, Okay, Error, Value, InternalIoError,
            FromRedisValue};
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
            match rv.execute::<Value>("SELECT", [IntArg(db)]) {
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

    fn pack_command(&self, cmd: &str, args: &[CmdArg]) -> Vec<u8> {
        let mut rv = vec![];
        rv.push_all(format!("*{}\r\n", args.len() + 1).as_bytes());
        rv.push_all(format!("${}\r\n", cmd.len()).as_bytes());
        rv.push_all(cmd.as_bytes());
        rv.push_all(b"\r\n");

        for arg in args.iter() {
            let mut buf;
            let encoded_arg = match arg {
                &StrArg(s) => s.as_bytes(),
                &IntArg(i) => {
                    let i_str = i.to_string();
                    buf = i_str.as_bytes().to_vec();
                    buf.as_slice()
                },
                &FloatArg(f) => {
                    let f_str = f.to_string();
                    buf = f_str.as_bytes().to_vec();
                    buf.as_slice()
                },
                &BytesArg(b) => b,
            };
            rv.push_all(format!("${}\r\n", encoded_arg.len()).as_bytes());
            rv.push_all(encoded_arg);
            rv.push_all(b"\r\n");
        }

        rv
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

    fn send_command(&mut self, cmd: &str, args: &[CmdArg]) -> RedisResult<Value> {
        let c = self.pack_command(cmd, args);
        self.send_bytes(c.as_slice())
    }

    pub fn execute_raw(&mut self, cmd: &str, args: &[CmdArg]) -> RedisResult<Value> {
        try!(self.send_command(cmd, args));
        self.read_response()
    }

    pub fn execute<T: FromRedisValue>(&mut self, cmd: &str, args: &[CmdArg]) -> RedisResult<T> {
        match self.execute_raw(cmd, args) {
            Ok(val) => FromRedisValue::from_redis_value(&val),
            Err(e) => Err(e),
        }
    }

    pub fn get_db(&self) -> i64 {
        self.db
    }
}
