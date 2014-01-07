extern mod extra;

use std::vec::bytes::push_bytes;
use std::io::Reader;
use std::io::Writer;
use std::io::net::ip::SocketAddr;
use std::io::net::get_host_addresses;
use std::io::net::tcp::TcpStream;
use std::from_str::from_str;
use std::str::{from_utf8, from_utf8_owned};
use std::container::Map;
use std::hashmap::HashMap;

use extra::url::Url;
use extra::time;

use parser::Parser;
use parser::ByteIterator;

use enums::*;
use script::Script;

mod macros;

pub enum KeyType {
    StringType,
    ListType,
    SetType,
    ZSetType,
    HashType,
    UnknownType,
    NilType,
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


pub struct Client {
    priv addr: SocketAddr,
    priv sock: TcpStream,
    priv db: uint,
}

impl Client {

    /// opens a connection to redis by URI
    pub fn open(uri: &str) -> Result<Client, ConnectFailure> {
        let parsed_uri = try_unwrap!(from_str::<Url>(uri), Err(InvalidURI));
        if parsed_uri.scheme != ~"redis" {
            return Err(InvalidURI);
        }

        let ip_addrs = try_unwrap!(get_host_addresses(parsed_uri.host), Err(InvalidURI));
        let ip_addr = try_unwrap!(ip_addrs.iter().next(), Err(HostNotFound));
        let port = try_unwrap!(from_str::<u16>(parsed_uri.port.clone()
            .unwrap_or(~"6379")), Err(InvalidURI));
        let db = from_str::<uint>(parsed_uri.path.trim_chars(&'/')).unwrap_or(0);

        let addr = SocketAddr {
            ip: *ip_addr,
            port: port
        };
        let sock = try_unwrap!(TcpStream::connect(addr), Err(ConnectionRefused));

        let mut rv = Client {
            addr: addr,
            sock: sock,
            db: 0,
        };

        if db != 0 {
            rv.select_db(db);
        }

        Ok(rv)
    }

    /// executes a low-level redis command
    pub fn execute(&mut self, cmd: &str, args: &[CmdArg]) -> Value {
        self.send_command(cmd, args);
        match self.read_response() {
            Error(ResponseError, msg) => {
                fail!(format!("Redis command failed: {}", msg));
            },
            other => other,
        }
    }

    fn pack_command(&self, cmd: &str, args: &[CmdArg]) -> ~[u8] {
        let mut rv = ~[];
        push_byte_format!(&mut rv, "*{}\r\n", args.len() + 1);

        push_byte_format!(&mut rv, "${}\r\n", cmd.len());
        push_bytes(&mut rv, cmd.as_bytes());
        push_bytes(&mut rv, bytes!("\r\n"));

        for arg in args.iter() {
            let mut buf;
            let encoded_arg = match arg {
                &StrArg(s) => s.as_bytes(),
                &IntArg(i) => {
                    let i_str = i.to_str();
                    buf = i_str.as_bytes().to_owned();
                    buf.as_slice()
                },
                &FloatArg(f) => {
                    let f_str = f.to_str();
                    buf = f_str.as_bytes().to_owned();
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

    fn send_command(&mut self, cmd: &str, args: &[CmdArg]) {
        let cmd = self.pack_command(cmd, args);
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

    pub fn auth(&mut self, password: &str) -> bool {
        self.send_command("AUTH", [StrArg(password)]);
        match self.read_response() {
            Success => true,
            _ => false,
        }
    }

    pub fn select_db(&mut self, db: uint) -> bool {
        match self.execute("SELECT", [IntArg(db as int)]) {
            Success => { self.db = db; true },
            _ => false,
        }
    }

    pub fn ping(&mut self) -> bool {
        match self.execute("PING", []) {
            Status(~"PONG") => true,
            _ => false,
        }
    }

    pub fn keys(&mut self, pattern: &str) -> ~[~str] {
        let resp = self.execute("KEYS", [StrArg(pattern)]);
        value_to_string_list(&resp)
    }

    pub fn get_bytes(&mut self, key: &str) -> Option<~[u8]> {
        match self.execute("GET", [StrArg(key)]) {
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

    pub fn set_bytes(&mut self, key: &str, value: &[u8]) -> bool {
        match self.execute("SET", [StrArg(key), BytesArg(value)]) {
            Success => true,
            _ => false,
        }
    }

    pub fn set<T: ToStr>(&mut self, key: &str, value: T) -> bool {
        let v = value.to_str();
        match self.execute("SET", [StrArg(key), StrArg(v)]) {
            Success => true,
            _ => false,
        }
    }

    pub fn setex_bytes(&mut self, key: &str, value: &[u8], timeout: f32) -> bool {
        let mut cmd;
        let mut t;
        let i_timeout = timeout as int;
        if i_timeout as f32 == timeout {
            cmd = "SETEX";
            t = i_timeout;
        } else {
            cmd = "PSETEX";
            t = (timeout * 1000.0) as int;
        }
        match self.execute(cmd, [StrArg(key), IntArg(t), BytesArg(value)]) {
            Success => true,
            _ => false,
        }
    }

    pub fn setex<T: ToStr>(&mut self, key: &str, value: T, timeout: f32) -> bool {
        let v = value.to_str();
        self.setex_bytes(key, v.as_bytes(), timeout)
    }

    pub fn setnx_bytes(&mut self, key: &str, value: &[u8]) -> bool {
        match self.execute("SETNX", [StrArg(key), BytesArg(value)]) {
            Int(1) => true,
            _ => false,
        }
    }

    pub fn setnx<T: ToStr>(&mut self, key: &str, value: T) -> bool {
        let v = value.to_str();
        match self.execute("SETNX", [StrArg(key), StrArg(v)]) {
            Int(1) => true,
            _ => false,
        }
    }

    pub fn del(&mut self, key: &str) -> bool {
        match self.execute("DEL", [StrArg(key)]) {
            Int(0) | Nil => false,
            Int(_) => true,
            _ => false,
        }
    }

    pub fn del_many(&mut self, keys: &[&str]) -> uint {
        let args = keys.iter().map(|&x| StrArg(x)).to_owned_vec();
        match self.execute("DEL", args) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    pub fn getset_bytes(&mut self, key: &str, value: &[u8]) -> Option<~[u8]> {
        match self.execute("GETSET", [StrArg(key), BytesArg(value)]) {
            Data(value) => Some(value),
            _ => None,
        }
    }

    pub fn getset(&mut self, key: &str, value: ~str) -> Option<~str> {
        match self.execute("GETSET", [StrArg(key), StrArg(value)]) {
            Data(x) => Some(from_utf8_owned(x)),
            _ => None,
        }
    }

    pub fn getset_as<T: ToStr+FromStr>(&mut self, key: &str, value: T) -> Option<T> {
        let v = value.to_str();
        match self.getset(key, v) {
            Some(x) => from_str(x),
            None => None,
        }
    }

    pub fn append_bytes(&mut self, key: &str, value: &[u8]) -> uint {
        match self.execute("APPEND", [StrArg(key), BytesArg(value)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    pub fn append<T: ToStr>(&mut self, key: &str, value: T) -> uint {
        let v = value.to_str();
        self.append_bytes(key, v.as_bytes())
    }

    pub fn setbit(&mut self, key: &str, bit: uint, value: bool) -> bool {
        let v = if value { 1 } else { 0 };
        match self.execute("SETBIT", [StrArg(key), IntArg(bit as int), IntArg(v)]) {
            Int(1) => true,
            _ => false,
        }
    }

    pub fn strlen(&mut self, key: &str) -> uint {
        match self.execute("STRLEN", [StrArg(key)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    pub fn getrange_bytes(&mut self, key: &str, start: int, end: int) -> ~[u8] {
        match self.execute("GETRANGE", [StrArg(key), IntArg(start), IntArg(end)]) {
            Data(value) => value,
            _ => ~[],
        }
    }

    pub fn getrange(&mut self, key: &str, start: int, end: int) -> ~str {
        from_utf8_owned(self.getrange_bytes(key, start, end))
    }

    pub fn setrange_bytes(&mut self, key: &str, offset: int, value: &[u8]) -> uint {
        match self.execute("SETRANGE", [StrArg(key), IntArg(offset), BytesArg(value)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    pub fn setrange(&mut self, key: &str, offset: int, value: &str) -> uint {
        self.setrange_bytes(key, offset, value.as_bytes())
    }

    pub fn popcount(&mut self, key: &str) -> uint {
        match self.execute("POPCOUNT", [StrArg(key)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    pub fn popcount_range(&mut self, key: &str, start: int, end: int) -> uint {
        match self.execute("POPCOUNT", [StrArg(key), IntArg(start), IntArg(end)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    pub fn incr(&mut self, key: &str) -> int {
        match self.execute("INCR", [StrArg(key)]) {
            Int(x) => x as int,
            _ => 0,
        }
    }

    pub fn incrby(&mut self, key: &str, step: int) -> int {
        match self.execute("INCRBY", [StrArg(key), IntArg(step)]) {
            Int(x) => x as int,
            _ => 0,
        }
    }

    pub fn incrby_float(&mut self, key: &str, step: f32) -> f32 {
        match self.execute("INCRBYFLOAT", [StrArg(key), FloatArg(step)]) {
            Data(x) => {
                match from_str(from_utf8(x)) {
                    Some(x) => x,
                    None => 0.0,
                }
            },
            _ => 0.0,
        }
    }

    pub fn decr(&mut self, key: &str) -> int {
        match self.execute("DECR", [StrArg(key)]) {
            Int(x) => x as int,
            _ => 0,
        }
    }

    pub fn decrby(&mut self, key: &str, step: int) -> int {
        match self.execute("DECRBY", [StrArg(key), IntArg(step)]) {
            Int(x) => x as int,
            _ => 0,
        }
    }

    pub fn decrby_float(&mut self, key: &str, step: f32) -> f32 {
        self.incrby_float(key, -step)
    }

    pub fn exists(&mut self, key: &str) -> bool {
        match self.execute("EXISTS", [StrArg(key)]) {
            Int(1) => true,
            _ => false,
        }
    }

    pub fn expire(&mut self, key: &str, timeout: f32) -> bool {
        let mut cmd;
        let mut t;
        let i_timeout = timeout as int;
        if i_timeout as f32 == timeout {
            cmd = "EXPIRE";
            t = i_timeout;
        } else {
            cmd = "PEXPIRE";
            t = (timeout * 1000.0) as int;
        }
        match self.execute(cmd, [StrArg(key), IntArg(t)]) {
            Int(1) => true,
            _ => false,
        }
    }

    pub fn persist(&mut self, key: &str) -> bool {
        match self.execute("PERSIST", [StrArg(key)]) {
            Int(1) => true,
            _ => false,
        }
    }

    pub fn ttl(&mut self, key: &str) -> Option<f32> {
        match self.execute("PTTL", [StrArg(key)]) {
            Int(x) => {
                if x < 0 {
                    None
                } else {
                    Some(x as f32 / 1000.0)
                }
            },
            _ => None
        }
    }

    pub fn rename(&mut self, key: &str, newkey: &str) -> bool {
        match self.execute("RENAME", [StrArg(key), StrArg(newkey)]) {
            Success => true,
            _ => false,
        }
    }

    pub fn renamenx(&mut self, key: &str, newkey: &str) -> bool {
        match self.execute("RENAMENX", [StrArg(key), StrArg(newkey)]) {
            Int(1) => true,
            _ => false,
        }
    }

    pub fn get_type(&mut self, key: &str) -> KeyType {
        match self.execute("TYPE", [StrArg(key)]) {
            Status(key) => {
                match key.as_slice() {
                    "none" => NilType,
                    "string" => StringType,
                    "list" => ListType,
                    "set" => SetType,
                    "zset" => ZSetType,
                    "hash" => HashType,
                    _ => UnknownType,
                }
            },
            _ => UnknownType,
        }
    }

    pub fn info(&mut self) -> ~Map<~str, ~str> {
        let mut rv = ~HashMap::new();
        match self.execute("INFO", []) {
            Data(bytes) => {
                for line in from_utf8(bytes).lines_any() {
                    if line.len() == 0 || line[0] == '#' as u8 {
                        continue;
                    }
                    let mut p = line.splitn(':', 1);
                    let key = p.next();
                    let value = p.next();
                    if value.is_some() {
                        rv.insert(key.unwrap().to_owned(),
                                  value.unwrap().to_owned());
                    }
                }
            },
            _ => {}
        };
        rv as ~Map<~str, ~str>
    }

    pub fn load_script(&mut self, script: &Script) -> bool {
        match self.execute("SCRIPT", [StrArg("LOAD"), BytesArg(script.code)]) {
            Data(_) => true,
            _ => false,
        }
    }

    pub fn call_script<'a>(&mut self, script: &'a Script,
                           keys: &[&'a str], args: &[CmdArg<'a>]) -> Value {
        let mut all_args = ~[StrArg(script.sha), IntArg(keys.len() as int)];
        all_args.extend(&mut keys.iter().map(|&x| StrArg(x)));
        all_args.push_all(args);

        loop {
            match self.execute("EVALSHA", all_args) {
                Error(code, msg) => {
                    match code {
                        NoScriptError => {
                            if !self.load_script(script) {
                                fail!("Failed to load script");
                            }
                        }
                        _ => { return Error(code, msg); }
                    }
                },
                x => { return x; }
            }
        }
    }

    pub fn flush_script_cache(&mut self) -> bool {
        match self.execute("SCRIPT", [StrArg("FLUSH")]) {
            Success => true,
            _ => false,
        }
    }

    pub fn bgsave(&mut self) -> bool {
        match self.execute("BGSAVE", []) {
            Status(_) => true,
            _ => false,
        }
    }

    pub fn save(&mut self) -> bool {
        match self.execute("SAVE", []) {
            Success => true,
            _ => false,
        }
    }

    pub fn bgrewriteaof(&mut self) -> bool {
        match self.execute("BGREWRITEAOF", []) {
            Status(_) => true,
            _ => false,
        }
    }

    pub fn dbsize(&mut self) -> uint {
        match self.execute("DBSIZE", []) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    pub fn lastsave(&mut self) -> time::Tm {
        match self.execute("LASTSAVE", []) {
            Int(x) => {
                time::at_utc(time::Timespec::new(x, 0))
            },
            _ => {
                time::empty_tm()
            }
        }
    }

    pub fn time(&mut self) -> time::Tm {
        match self.execute("TIME", []) {
            Bulk(ref items) => {
                let mut i = items.iter();
                let s = match i.next() {
                    Some(&Data(ref bytes)) => {
                        let s = from_utf8(*bytes);
                        from_str::<i64>(s).unwrap_or(0)
                    },
                    _ => 0,
                };
                let ms = match i.next() {
                    Some(&Data(ref bytes)) => {
                        let s = from_utf8(*bytes);
                        from_str::<i32>(s).unwrap_or(0)
                    },
                    _ => 0,
                };
                time::at_utc(time::Timespec::new(s, ms))
            },
            _ => time::empty_tm()
        }
    }

    pub fn shutdown(&mut self, mode: ShutdownMode) {
        let args = match mode {
            ShutdownNormal => ~[],
            ShutdownSave => ~[StrArg("SAVE")],
            ShutdownNoSave => ~[StrArg("NOSAVE")],
        };
        self.send_command("SHUTDOWN", args);
        // try to read a response but expect this to fail.
        self.read_response();
    }

    pub fn flushdb(&mut self) {
        self.execute("FLUSHDB", []);
    }
}
