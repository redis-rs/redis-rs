extern mod extra;

use std::io::Reader;
use std::io::Writer;
use std::vec::bytes::push_bytes;
use std::str::{from_utf8, from_utf8_owned};
use std::io::net::ip::SocketAddr;
use std::container::Map;
use std::hashmap::HashMap;
use std::io::net::tcp::TcpStream;

use extra::time;

use parser::Parser;
use enums::*;
use script::Script;
use parser::ByteIterator;

mod macros;


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

fn value_to_byte_list(val: &Value) -> ~[~[u8]] {
    match *val {
        Bulk(ref items) => {
            let mut rv = ~[];
            for item in items.iter() {
                match item {
                    &Data(ref payload) => {
                        rv.push(payload.to_owned());
                    },
                    _ => {}
                }
            }
            rv
        },
        _ => ~[],
    }
}

fn value_to_key_value_tuple(val: &Value) -> Option<(~str, ~[u8])> {
    match *val {
        Bulk(ref items) => {
            let mut iter = items.iter();
            let key = match try_unwrap!(iter.next(), None) {
                &Data(ref payload) => {
                    from_utf8(*payload).to_owned()
                },
                _ => { return None; }
            };
            let value = match try_unwrap!(iter.next(), None) {
                &Data(ref payload) => {
                    payload.to_owned()
                },
                _ => { return None; }
            };
            return Some((key, value));
        },
        _ => None
    }
}

pub struct Connection {
    priv addr: SocketAddr,
    priv sock: TcpStream,
    priv db: uint,
}

impl Connection {

    pub fn new(addr: SocketAddr, db: uint) -> Result<Connection, ConnectFailure> {
        let sock = try_unwrap!(TcpStream::connect(addr), Err(ConnectionRefused));

        let mut rv = Connection {
            addr: addr,
            sock: sock,
            db: 0,
        };

        if (db != 0) {
            rv.select_db(db);
        }

        Ok(rv)
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

    fn execute(&mut self, cmd: &str, args: &[CmdArg]) -> Value {
        self.send_command(cmd, args);
        match self.read_response() {
            Error(ResponseError, msg) => {
                fail!(format!("Redis command failed: {}", msg));
            },
            other => other,
        }
    }


    // -- server commands
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

    // -- key commands

    pub fn keys(&mut self, pattern: &str) -> ~[~str] {
        let resp = self.execute("KEYS", [StrArg(pattern)]);
        value_to_string_list(&resp)
    }

    pub fn persist(&mut self, key: &str) -> bool {
        match self.execute("PERSIST", [StrArg(key)]) {
            Int(1) => true,
            _ => false,
        }
    }

    pub fn expire(&mut self, key: &str, timeout: f32) -> bool {
        let mut cmd;
        let mut t;
        let i_timeout = timeout as int;
        if (i_timeout as f32 == timeout) {
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

    // -- script commands

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

    // -- key / value commands

    #[inline]
    pub fn get_bytes(&mut self, key: &str) -> Option<~[u8]> {
        match self.execute("GET", [StrArg(key)]) {
            Data(value) => Some(value),
            _ => None,
        }
    }

    #[inline]
    pub fn get(&mut self, key: &str) -> Option<~str> {
        match self.get_bytes(key) {
            None => None,
            Some(x) => Some(from_utf8_owned(x)),
        }
    }

    #[inline]
    pub fn get_as<T: FromStr>(&mut self, key: &str) -> Option<T> {
        match self.get(key) {
            None => None,
            Some(x) => from_str(x),
        }
    }

    #[inline]
    pub fn set_bytes(&mut self, key: &str, value: &[u8]) -> bool {
        match self.execute("SET", [StrArg(key), BytesArg(value)]) {
            Success => true,
            _ => false,
        }
    }

    #[inline]
    pub fn set<T: ToStr>(&mut self, key: &str, value: T) -> bool {
        let v = value.to_str();
        match self.execute("SET", [StrArg(key), StrArg(v)]) {
            Success => true,
            _ => false,
        }
    }

    #[inline]
    pub fn setex_bytes(&mut self, key: &str, value: &[u8], timeout: f32) -> bool {
        let mut cmd;
        let mut t;
        let i_timeout = timeout as int;
        if (i_timeout as f32 == timeout) {
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

    #[inline]
    pub fn setex<T: ToStr>(&mut self, key: &str, value: T, timeout: f32) -> bool {
        let v = value.to_str();
        self.setex_bytes(key, v.as_bytes(), timeout)
    }

    #[inline]
    pub fn setnx_bytes(&mut self, key: &str, value: &[u8]) -> bool {
        match self.execute("SETNX", [StrArg(key), BytesArg(value)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn setnx<T: ToStr>(&mut self, key: &str, value: T) -> bool {
        let v = value.to_str();
        match self.execute("SETNX", [StrArg(key), StrArg(v)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn del(&mut self, key: &str) -> bool {
        match self.execute("DEL", [StrArg(key)]) {
            Int(0) | Nil => false,
            Int(_) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn del_many(&mut self, keys: &[&str]) -> uint {
        let args = keys.iter().map(|&x| StrArg(x)).to_owned_vec();
        match self.execute("DEL", args) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn getset_bytes(&mut self, key: &str, value: &[u8]) -> Option<~[u8]> {
        match self.execute("GETSET", [StrArg(key), BytesArg(value)]) {
            Data(value) => Some(value),
            _ => None,
        }
    }

    #[inline]
    pub fn getset<T: ToStr+FromStr>(&mut self, key: &str, value: T) -> Option<T> {
        let v = value.to_str();
        match self.getset_bytes(key, v.as_bytes()) {
            Some(x) => from_str(from_utf8_owned(x)),
            None => None,
        }
    }

    #[inline]
    pub fn append_bytes(&mut self, key: &str, value: &[u8]) -> uint {
        match self.execute("APPEND", [StrArg(key), BytesArg(value)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn append<T: ToStr>(&mut self, key: &str, value: T) -> uint {
        let v = value.to_str();
        self.append_bytes(key, v.as_bytes())
    }

    #[inline]
    pub fn setbit(&mut self, key: &str, bit: uint, value: bool) -> bool {
        let v = if value { 1 } else { 0 };
        match self.execute("SETBIT", [StrArg(key), IntArg(bit as int), IntArg(v)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn strlen(&mut self, key: &str) -> uint {
        match self.execute("STRLEN", [StrArg(key)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn getrange_bytes(&mut self, key: &str, start: int, end: int) -> ~[u8] {
        match self.execute("GETRANGE", [StrArg(key), IntArg(start), IntArg(end)]) {
            Data(value) => value,
            _ => ~[],
        }
    }

    #[inline]
    pub fn getrange(&mut self, key: &str, start: int, end: int) -> ~str {
        from_utf8_owned(self.getrange_bytes(key, start, end))
    }

    #[inline]
    pub fn setrange_bytes(&mut self, key: &str, offset: int, value: &[u8]) -> uint {
        match self.execute("SETRANGE", [StrArg(key), IntArg(offset), BytesArg(value)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn setrange(&mut self, key: &str, offset: int, value: &str) -> uint {
        self.setrange_bytes(key, offset, value.as_bytes())
    }

    #[inline]
    pub fn popcount(&mut self, key: &str) -> uint {
        match self.execute("POPCOUNT", [StrArg(key)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn popcount_range(&mut self, key: &str, start: int, end: int) -> uint {
        match self.execute("POPCOUNT", [StrArg(key), IntArg(start), IntArg(end)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn incr(&mut self, key: &str) -> int {
        match self.execute("INCR", [StrArg(key)]) {
            Int(x) => x as int,
            _ => 0,
        }
    }

    #[inline]
    pub fn incrby(&mut self, key: &str, step: int) -> int {
        match self.execute("INCRBY", [StrArg(key), IntArg(step)]) {
            Int(x) => x as int,
            _ => 0,
        }
    }

    #[inline]
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

    #[inline]
    pub fn decr(&mut self, key: &str) -> int {
        match self.execute("DECR", [StrArg(key)]) {
            Int(x) => x as int,
            _ => 0,
        }
    }

    #[inline]
    pub fn decrby(&mut self, key: &str, step: int) -> int {
        match self.execute("DECRBY", [StrArg(key), IntArg(step)]) {
            Int(x) => x as int,
            _ => 0,
        }
    }

    #[inline]
    pub fn decrby_float(&mut self, key: &str, step: f32) -> f32 {
        self.incrby_float(key, -step)
    }

    #[inline]
    pub fn exists(&mut self, key: &str) -> bool {
        match self.execute("EXISTS", [StrArg(key)]) {
            Int(1) => true,
            _ => false,
        }
    }

    // -- list commands

    #[inline]
    fn blocking_pop_bytes(&mut self, cmd: &str, keys: &[&str],
                          timeout: f32) -> Option<(~str, ~[u8])> {
        let mut timeout_s = timeout as int;
        if (timeout_s <= 0) {
            timeout_s = 0;
        }
        let mut args = keys.iter().map(|&x| StrArg(x)).to_owned_vec();
        args.push(IntArg(timeout_s));
        value_to_key_value_tuple(&self.execute(cmd, args))
    }

    #[inline]
    pub fn blpop_bytes(&mut self, keys: &[&str], timeout: f32) -> Option<(~str, ~[u8])> {
        self.blocking_pop_bytes("BLPOP", keys, timeout)
    }

    #[inline]
    pub fn blpop(&mut self, keys: &[&str], timeout: f32) -> Option<(~str, ~str)> {
        match self.blpop_bytes(keys, timeout) {
            Some((key, value)) => {
                Some((key, from_utf8_owned(value)))
            },
            None => None
        }
    }

    #[inline]
    pub fn blpop_as<T: FromStr>(&mut self, keys: &[&str], timeout: f32) -> Option<(~str, T)> {
        match self.blpop(keys, timeout) {
            Some((key, value)) => {
                Some((key, try_unwrap!(from_str(value), None)))
            },
            None => None
        }
    }

    #[inline]
    pub fn brpop_bytes(&mut self, keys: &[&str], timeout: f32) -> Option<(~str, ~[u8])> {
        self.blocking_pop_bytes("BRPOP", keys, timeout)
    }

    #[inline]
    pub fn brpop(&mut self, keys: &[&str], timeout: f32) -> Option<(~str, ~str)> {
        match self.brpop_bytes(keys, timeout) {
            Some((key, value)) => {
                Some((key, from_utf8_owned(value)))
            },
            None => None
        }
    }

    #[inline]
    pub fn brpop_as<T: FromStr>(&mut self, keys: &[&str], timeout: f32) -> Option<(~str, T)> {
        match self.brpop(keys, timeout) {
            Some((key, value)) => {
                Some((key, try_unwrap!(from_str(value), None)))
            },
            None => None
        }
    }

    #[inline]
    pub fn brpoplpush_bytes(&mut self, src: &str, dst: &str, timeout: f32) -> Option<~[u8]> {
        let mut timeout_s = timeout as int;
        if (timeout_s <= 0) {
            timeout_s = 0;
        }
        match self.execute("BRPOPLPUSH", [StrArg(src), StrArg(dst),
                                          IntArg(timeout_s)]) {
            Data(ref payload) => Some(payload.to_owned()),
            _ => None,
        }
    }

    #[inline]
    pub fn brpoplpush(&mut self, src: &str, dst: &str, timeout: f32) -> Option<~str> {
        match self.brpoplpush_bytes(src, dst, timeout) {
            Some(x) => Some(from_utf8_owned(x)),
            None => None,
        }
    }

    #[inline]
    pub fn lindex_bytes(&mut self, key: &str, index: i64) -> Option<~[u8]> {
        match self.execute("LINDEX", [StrArg(key), IntArg(index as int)]) {
            Data(value) => Some(value),
            _ => None,
        }
    }

    #[inline]
    pub fn lindex(&mut self, key: &str, index: i64) -> Option<~str> {
        match self.lindex_bytes(key, index) {
            Some(x) => Some(from_utf8_owned(x)),
            None => None,
        }
    }

    #[inline]
    pub fn lindex_as<T: FromStr>(&mut self, key: &str, index: i64) -> Option<T> {
        match self.lindex(key, index) {
            Some(x) => from_str(x),
            None => None,
        }
    }

    #[inline]
    fn list_insert_operation(&mut self, cmd: &str, key: &str, pivot: &[u8], value: &[u8]) -> int {
        match self.execute("LINSERT", [StrArg(cmd), StrArg(key), StrArg("BEFORE"),
                                       BytesArg(pivot), BytesArg(value)]) {
            Int(value) => value as int,
            _ => 0,
        }
    }

    #[inline]
    pub fn linsert_before_bytes(&mut self, key: &str, pivot: &[u8], value: &[u8]) -> int {
        self.list_insert_operation("BEFORE", key, pivot, value)
    }

    #[inline]
    pub fn linsert_before<T: ToStr>(&mut self, key: &str, pivot: &str, value: T) -> int {
        let v = value.to_str();
        self.linsert_before_bytes(key, pivot.as_bytes(), v.as_bytes())
    }

    #[inline]
    pub fn linsert_after_bytes(&mut self, key: &str, pivot: &[u8], value: &[u8]) -> int {
        self.list_insert_operation("AFTER", key, pivot, value)
    }

    #[inline]
    pub fn linsert_after<T: ToStr>(&mut self, key: &str, pivot: &str, value: T) -> int {
        let v = value.to_str();
        self.linsert_after_bytes(key, pivot.as_bytes(), v.as_bytes())
    }

    #[inline]
    pub fn llen(&mut self, key: &str) -> uint {
        match self.execute("LLEN", [StrArg(key)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn lpop_bytes(&mut self, key: &str) -> Option<~[u8]> {
        match self.execute("LPOP", [StrArg(key)]) {
            Data(payload) => Some(payload),
            _ => None,
        }
    }

    #[inline]
    pub fn lpop(&mut self, key: &str) -> Option<~str> {
        match self.lpop_bytes(key) {
            Some(x) => Some(from_utf8_owned(x)),
            None => None,
        }
    }

    #[inline]
    pub fn lpop_as<T: FromStr>(&mut self, key: &str) -> Option<T> {
        match self.lpop(key) {
            Some(x) => from_str(x),
            None => None,
        }
    }

    #[inline]
    pub fn lpush_bytes(&mut self, key: &str, value: &[u8]) -> uint {
        match self.execute("LPUSH", [StrArg(key), BytesArg(value)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn lpush<T: ToStr>(&mut self, key: &str, value: T) -> uint {
        let v = value.to_str();
        self.lpush_bytes(key, v.as_bytes())
    }

    #[inline]
    pub fn lpushx_bytes(&mut self, key: &str, value: &[u8]) -> uint {
        match self.execute("LPUSHX", [StrArg(key), BytesArg(value)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn lpushx<T: ToStr>(&mut self, key: &str, value: T) -> uint {
        let v = value.to_str();
        self.lpushx_bytes(key, v.as_bytes())
    }

    #[inline]
    pub fn lrange_bytes(&mut self, key: &str, start: int, end: int) -> ~[~[u8]] {
        value_to_byte_list(&self.execute("LRANGE", [StrArg(key), IntArg(start), IntArg(end)]))
    }

    #[inline]
    pub fn lrange(&mut self, key: &str, start: int, end: int) -> ~[~str] {
        let items = self.lrange_bytes(key, start, end);
        items.move_iter().map(|x| from_utf8_owned(x)).to_owned_vec()
    }

    #[inline]
    pub fn lrange_as<T: FromStr>(&mut self, key: &str, start: int, end: int) -> ~[T] {
        let items = self.lrange(key, start, end);
        let mut rv = ~[];
        for item in items.move_iter() {
            match from_str(item) {
                Some(x) => { rv.push(x); }
                None => {}
            };
        }
        rv
    }

    #[inline]
    pub fn lrem_bytes(&mut self, key: &str, count: int, value: &[u8]) -> uint {
        match self.execute("LREM", [StrArg(key), IntArg(count), BytesArg(value)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn lrem<T: ToStr>(&mut self, key: &str, count: int, value: T) -> uint {
        let v = value.to_str();
        self.lrem_bytes(key, count, v.as_bytes())
    }

    #[inline]
    pub fn lset_bytes(&mut self, key: &str, index: int, value: &[u8]) -> bool {
        match self.execute("LSET", [StrArg(key), IntArg(index), BytesArg(value)]) {
            Success => true,
            _ => true,
        }
    }

    #[inline]
    pub fn lset<T: ToStr>(&mut self, key: &str, index: int, value: T) -> bool {
        let v = value.to_str();
        self.lset_bytes(key, index, v.as_bytes())
    }

    #[inline]
    pub fn ltrim(&mut self, key: &str, start: int, stop: int) -> bool {
        match self.execute("LTRIM", [StrArg(key), IntArg(start), IntArg(stop)]) {
            Success => true,
            _ => false,
        }
    }

    #[inline]
    pub fn rpop_bytes(&mut self, key: &str) -> Option<~[u8]> {
        match self.execute("RPOP", [StrArg(key)]) {
            Data(payload) => Some(payload),
            _ => None,
        }
    }

    #[inline]
    pub fn rpop(&mut self, key: &str) -> Option<~str> {
        match self.rpop_bytes(key) {
            Some(x) => Some(from_utf8_owned(x)),
            None => None,
        }
    }

    #[inline]
    pub fn rpop_as<T: FromStr>(&mut self, key: &str) -> Option<T> {
        match self.rpop(key) {
            Some(x) => from_str(x),
            None => None,
        }
    }

    #[inline]
    pub fn rpush_bytes(&mut self, key: &str, value: &[u8]) -> uint {
        match self.execute("RPUSH", [StrArg(key), BytesArg(value)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn rpush<T: ToStr>(&mut self, key: &str, value: T) -> uint {
        let v = value.to_str();
        self.rpush_bytes(key, v.as_bytes())
    }

    #[inline]
    pub fn rpushx_bytes(&mut self, key: &str, value: &[u8]) -> uint {
        match self.execute("RPUSHX", [StrArg(key), BytesArg(value)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn rpushx<T: ToStr>(&mut self, key: &str, value: T) -> uint {
        let v = value.to_str();
        self.rpushx_bytes(key, v.as_bytes())
    }
}
