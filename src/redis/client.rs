extern mod extra;

use std::vec::bytes::push_bytes;
use std::io::Reader;
use std::io::Writer;
use std::io::net::ip::SocketAddr;
use std::io::net::get_host_addresses;
use std::io::net::tcp::TcpStream;
use std::from_str::from_str;
use std::str::{from_utf8, from_utf8_owned};
use extra::url::Url;

use parser::Parser;
use parser::ByteIterator;

use enums::*;

pub enum ConnectFailure {
    InvalidURI,
    HostNotFound,
    ConnectionRefused,
}

macro_rules! try_unwrap {
    ($expr:expr, $exc:expr) => (
        match ($expr) {
            None => { return Err($exc) },
            Some(x) => x,
        }
    )
}

macro_rules! push_byte_format {
    ($container:expr, $($arg:tt)*) => ({
        let encoded = format!($($arg)*);
        push_bytes($container, encoded.as_bytes());
    })
}


fn value_to_string_list(val: &Value) -> ~[~str] {
    match *val {
        Bulk(ref items) => {
            let mut rv = ~[];
            for item in items.iter() {
                match item {
                    &Data(ref payload) => {
                        rv.push(from_utf8(*payload).to_owned());
                    },
                    _ => {}
                }
            }
            rv
        },
        _ => ~[],
    }
}


pub enum CmdArg<'a> {
    StrArg(&'a str),
    IntArg(int),
    BytesArg(&'a [u8]),
}

pub struct Client {
    priv addr: SocketAddr,
    priv sock: TcpStream,
    priv db: uint,
}

impl Client {

    /// opens a connection to redis by URI
    pub fn open(uri: &str) -> Result<Client, ConnectFailure> {
        let parsed_uri = try_unwrap!(from_str::<Url>(uri), InvalidURI);
        let ip_addrs = try_unwrap!(get_host_addresses(parsed_uri.host), InvalidURI);
        let ip_addr = try_unwrap!(ip_addrs.iter().next(), HostNotFound);
        let port = try_unwrap!(from_str::<u16>(parsed_uri.port.clone()
            .unwrap_or(~"6379")), InvalidURI);
        let db = from_str::<uint>(parsed_uri.path.trim_chars(&'/')).unwrap_or(0);

        let addr = SocketAddr {
            ip: *ip_addr,
            port: port
        };
        let sock = try_unwrap!(TcpStream::connect(addr), ConnectionRefused);

        let mut rv = Client {
            addr: addr,
            sock: sock,
            db: 0,
        };

        if (db != 0) {
            rv.select_db(db);
        }

        Ok(rv)
    }

    /// executes a low-level redis command
    pub fn execute(&mut self, args: &[CmdArg]) -> Value {
        self.send_command(args);
        self.read_response()
    }

    fn pack_command(&self, args: &[CmdArg]) -> ~[u8] {
        let mut rv = ~[];
        push_byte_format!(&mut rv, "*{}\r\n", args.len());

        for arg in args.iter() {
            let mut buf;
            let encoded_arg = match arg {
                &StrArg(s) => s.as_bytes(),
                &IntArg(i) => {
                    let i_str = i.to_str();
                    buf = i_str.as_bytes().to_owned();
                    buf.as_slice()
                },
                &BytesArg(b) => b,
            };
            push_byte_format!(&mut rv, "${}\r\n", encoded_arg.len());
            push_bytes(&mut rv, encoded_arg);
            push_bytes(&mut rv, bytes!("\r\n"));
        }

        rv
    }

    fn send_command(&mut self, args: &[CmdArg]) {
        let cmd = self.pack_command(args);
        let w = &mut self.sock as &mut Writer;
        w.write(cmd);
    }

    fn read_response(&mut self) -> Value {
        let mut parser = Parser::new(ByteIterator {
            reader: &mut self.sock as &mut Reader,
        });
        parser.parse_value()
    }

    // commands

    pub fn select_db(&mut self, db: uint) {
        self.send_command([StrArg("SELECT"), IntArg(db as int)]);
    }

    pub fn ping(&mut self) -> bool {
        match self.execute([StrArg("PING")]) {
            Status(~"PONG") => true,
            _ => false,
        }
    }

    pub fn keys(&mut self, pattern: &str) -> ~[~str] {
        let resp = self.execute([StrArg("KEYS"), StrArg(pattern)]);
        value_to_string_list(&resp)
    }

    pub fn get_bytes(&mut self, key: &str) -> Option<~[u8]> {
        match self.execute([StrArg("GET"), StrArg(key)]) {
            Data(value) => Some(value),
            _ => None,
        }
    }

    pub fn get(&mut self, key: &str) -> Option<~str> {
        match self.get_bytes(key) {
            None => None,
            Some(x) => Some(from_utf8_owned(x)),
        }
    }

    pub fn get_as<T: FromStr>(&mut self, key: &str) -> Option<T> {
        match self.get(key) {
            None => None,
            Some(x) => from_str(x),
        }
    }
}
