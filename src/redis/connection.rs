use std::io::Reader;
use std::io::Writer;
use std::vec::bytes::push_bytes;
use std::str::{from_utf8, from_utf8_owned};
use std::io::net::ip::SocketAddr;
use std::container::Map;
use collections::HashMap;
use std::io::net::tcp::TcpStream;

use time;
use parser::Parser;
use enums::*;
use script::Script;
use parser::ByteIterator;
use scan::ScanIterator;

mod macros;


fn value_to_string_list(val: Value) -> ~[~str] {
    match val {
        Bulk(items) => {
            let mut rv = ~[];
            for item in items.iter() {
                match item {
                    &Data(ref payload) => {
                        match from_utf8(*payload) {
                            Some(x) => { rv.push(x.to_owned()); }
                            None => {}
                        }
                    },
                    _ => {}
                }
            }
            rv
        },
        _ => ~[],
    }
}

fn value_to_byte_list(val: Value) -> ~[~[u8]] {
    match val {
        Bulk(items) => {
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

fn value_to_key_value_tuple(val: Value) -> Option<(~str, ~[u8])> {
    match val {
        Bulk(items) => {
            let mut iter = items.iter();
            let key = match try_unwrap!(iter.next(), None) {
                &Data(ref payload) => {
                    (try_unwrap!(from_utf8(*payload), None)).to_owned()
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

fn string_value_convert<T: FromStr>(val: Value, default: T) -> T {
    match val {
        Data(ref x) => {
            match from_str(try_unwrap!(from_utf8(*x), default)) {
                Some(x) => x,
                None => default,
            }
        },
        _ => default,
    }
}

fn value_to_bytes(val: Value) -> Option<~[u8]> {
    match val {
        Data(ref x) => Some(x.to_owned()),
        _ => None,
    }
}

fn value_to_byte_float_tuples(val: Value) -> ~[(~[u8], f32)] {
    match val {
        Bulk(items) => {
            let mut rv = ~[];
            let mut iter = items.move_iter();
            loop {
                let member = match iter.next().unwrap_or(Nil).get_bytes() {
                    Some(x) => x,
                    None => { break; }
                };
                let score = iter.next().unwrap_or(Nil).get_as::<f32>().unwrap_or(0.0);
                rv.push((member, score));
            }
            rv
        },
        _ => ~[],
    }
}

pub struct Connection {
    addr: SocketAddr,
    sock: TcpStream,
    db: i64,
}

impl Connection {

    pub fn new(addr: SocketAddr, db: i64) -> Result<Connection, ConnectFailure> {
        let sock = match TcpStream::connect(addr) {
            Ok(x) => x,
            Err(_) => { return Err(ConnectionRefused) },
        };

        let mut rv = Connection {
            addr: addr,
            sock: sock,
            db: 0,
        };

        if db != 0 {
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

    pub fn send_command(&mut self, cmd: &str, args: &[CmdArg]) {
        let cmd = self.pack_command(cmd, args);
        let w = &mut self.sock as &mut Writer;
        // XXX: error checking
        let _ = w.write(cmd);
    }

    pub fn read_response(&mut self) -> Value {
        let mut parser = Parser::new(ByteIterator {
            reader: &mut self.sock as &mut Reader,
        });
        parser.parse_value()
    }

    pub fn execute(&mut self, cmd: &str, args: &[CmdArg]) -> Value {
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

    pub fn select_db(&mut self, db: i64) -> bool {
        match self.execute("SELECT", [IntArg(db)]) {
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
                for line in from_utf8(bytes).unwrap_or("").lines_any() {
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
            Bulk(items) => {
                let mut i = items.move_iter();
                let s = match i.next() {
                    Some(x) => x.get_as::<i64>().unwrap_or(0),
                    _ => 0,
                };
                let ms = match i.next() {
                    Some(x) => x.get_as::<i32>().unwrap_or(0),
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

    #[inline]
    pub fn keys(&mut self, pattern: &str) -> ~[~str] {
        let resp = self.execute("KEYS", [StrArg(pattern)]);
        value_to_string_list(resp)
    }

    #[inline]
    pub fn scan<'a>(&'a mut self, pattern: &'a str) -> ScanIterator<'a, ~str> {
        ScanIterator {
            con: self,
            cmd: "SCAN",
            pre_args: ~[],
            post_args: ~[StrArg("MATCH"), StrArg(pattern)],
            cursor: 0,
            conv_func: |value| Some(string_value_convert(value, ~"")),
            buffer: ~[],
            end: false,
        }
    }

    #[inline]
    pub fn persist(&mut self, key: &str) -> bool {
        match self.execute("PERSIST", [StrArg(key)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn expire(&mut self, key: &str, timeout: f32) -> bool {
        let mut cmd;
        let mut t;
        let i_timeout = timeout as i64;
        if i_timeout as f32 == timeout {
            cmd = "EXPIRE";
            t = i_timeout;
        } else {
            cmd = "PEXPIRE";
            t = (timeout * 1000.0) as i64;
        }
        match self.execute(cmd, [StrArg(key), IntArg(t)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
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

    #[inline]
    pub fn rename(&mut self, key: &str, newkey: &str) -> bool {
        match self.execute("RENAME", [StrArg(key), StrArg(newkey)]) {
            Success => true,
            _ => false,
        }
    }

    #[inline]
    pub fn renamenx(&mut self, key: &str, newkey: &str) -> bool {
        match self.execute("RENAMENX", [StrArg(key), StrArg(newkey)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
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

    #[inline]
    pub fn load_script(&mut self, script: &Script) -> bool {
        match self.execute("SCRIPT", [StrArg("LOAD"), BytesArg(script.code)]) {
            Data(_) => true,
            _ => false,
        }
    }

    pub fn call_script<'a>(&mut self, script: &'a Script,
                           keys: &[&'a str], args: &[CmdArg<'a>]) -> Value {
        let mut all_args = ~[StrArg(script.sha), IntArg(keys.len() as i64)];
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

    #[inline]
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
            Some(x) => from_utf8_owned(x),
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
        let i_timeout = timeout as i64;
        if i_timeout as f32 == timeout {
            cmd = "SETEX";
            t = i_timeout;
        } else {
            cmd = "PSETEX";
            t = (timeout * 1000.0) as i64;
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
            Some(x) => from_str(from_utf8_owned(x).unwrap_or(~"")),
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
    pub fn setbit(&mut self, key: &str, bit: i64, value: bool) -> bool {
        let v = if value { 1 } else { 0 };
        match self.execute("SETBIT", [StrArg(key), IntArg(bit), IntArg(v)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn strlen(&mut self, key: &str) -> i64 {
        match self.execute("STRLEN", [StrArg(key)]) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn getrange_bytes(&mut self, key: &str, start: i64, end: i64) -> ~[u8] {
        match self.execute("GETRANGE", [StrArg(key), IntArg(start), IntArg(end)]) {
            Data(value) => value,
            _ => ~[],
        }
    }

    #[inline]
    pub fn getrange(&mut self, key: &str, start: i64, end: i64) -> ~str {
        from_utf8_owned(self.getrange_bytes(key, start, end)).unwrap_or(~"")
    }

    #[inline]
    pub fn setrange_bytes(&mut self, key: &str, offset: i64, value: &[u8]) -> i64 {
        match self.execute("SETRANGE", [StrArg(key), IntArg(offset), BytesArg(value)]) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn setrange(&mut self, key: &str, offset: i64, value: &str) -> i64 {
        self.setrange_bytes(key, offset, value.as_bytes())
    }

    #[inline]
    pub fn popcount(&mut self, key: &str) -> i64 {
        match self.execute("POPCOUNT", [StrArg(key)]) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn popcount_range(&mut self, key: &str, start: i64, end: i64) -> i64 {
        match self.execute("POPCOUNT", [StrArg(key), IntArg(start), IntArg(end)]) {
            Int(x) => x,
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
    pub fn incrby(&mut self, key: &str, step: i64) -> i64 {
        match self.execute("INCRBY", [StrArg(key), IntArg(step)]) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn incrby_float(&mut self, key: &str, step: f32) -> f32 {
        string_value_convert(self.execute("INCRBYFLOAT",
            [StrArg(key), FloatArg(step)]), 0.0f32)
    }

    #[inline]
    pub fn decr(&mut self, key: &str) -> int {
        match self.execute("DECR", [StrArg(key)]) {
            Int(x) => x as int,
            _ => 0,
        }
    }

    #[inline]
    pub fn decrby(&mut self, key: &str, step: i64) -> i64 {
        match self.execute("DECRBY", [StrArg(key), IntArg(step)]) {
            Int(x) => x,
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
        let mut timeout_s = timeout as i64;
        if timeout_s <= 0 {
            timeout_s = 0;
        }
        let mut args = keys.iter().map(|&x| StrArg(x)).to_owned_vec();
        args.push(IntArg(timeout_s));
        value_to_key_value_tuple(self.execute(cmd, args))
    }

    #[inline]
    pub fn blpop_bytes(&mut self, keys: &[&str], timeout: f32) -> Option<(~str, ~[u8])> {
        self.blocking_pop_bytes("BLPOP", keys, timeout)
    }

    #[inline]
    pub fn blpop(&mut self, keys: &[&str], timeout: f32) -> Option<(~str, ~str)> {
        match self.blpop_bytes(keys, timeout) {
            Some((key, value)) => {
                Some((key, from_utf8_owned(value).unwrap_or(~"")))
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
                Some((key, from_utf8_owned(value).unwrap_or(~"")))
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
        let mut timeout_s = timeout as i64;
        if timeout_s <= 0 {
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
            Some(x) => from_utf8_owned(x),
            None => None,
        }
    }

    #[inline]
    pub fn lindex_bytes(&mut self, key: &str, index: i64) -> Option<~[u8]> {
        match self.execute("LINDEX", [StrArg(key), IntArg(index)]) {
            Data(value) => Some(value),
            _ => None,
        }
    }

    #[inline]
    pub fn lindex(&mut self, key: &str, index: i64) -> Option<~str> {
        match self.lindex_bytes(key, index) {
            Some(x) => from_utf8_owned(x),
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
            Some(x) => from_utf8_owned(x),
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
    pub fn lrange_bytes(&mut self, key: &str, start: i64, end: i64) -> ~[~[u8]] {
        value_to_byte_list(self.execute("LRANGE", [StrArg(key), IntArg(start), IntArg(end)]))
    }

    #[inline]
    pub fn lrange(&mut self, key: &str, start: i64, end: i64) -> ~[~str] {
        let items = self.lrange_bytes(key, start, end);
        items.move_iter().map(|x| from_utf8_owned(x).unwrap_or(~"")).to_owned_vec()
    }

    #[inline]
    pub fn lrange_as<T: FromStr>(&mut self, key: &str, start: i64, end: i64) -> ~[T] {
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
    pub fn lrem_bytes(&mut self, key: &str, count: i64, value: &[u8]) -> i64 {
        match self.execute("LREM", [StrArg(key), IntArg(count), BytesArg(value)]) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn lrem<T: ToStr>(&mut self, key: &str, count: i64, value: T) -> i64 {
        let v = value.to_str();
        self.lrem_bytes(key, count, v.as_bytes())
    }

    #[inline]
    pub fn lset_bytes(&mut self, key: &str, index: i64, value: &[u8]) -> bool {
        match self.execute("LSET", [StrArg(key), IntArg(index), BytesArg(value)]) {
            Success => true,
            _ => true,
        }
    }

    #[inline]
    pub fn lset<T: ToStr>(&mut self, key: &str, index: i64, value: T) -> bool {
        let v = value.to_str();
        self.lset_bytes(key, index, v.as_bytes())
    }

    #[inline]
    pub fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> bool {
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
            Some(x) => from_utf8_owned(x),
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
    pub fn rpoplpush_bytes(&mut self, src: &str, dst: &str, timeout: f32) -> Option<~[u8]> {
        let mut timeout_s = timeout as i64;
        if timeout_s <= 0 {
            timeout_s = 0;
        }
        match self.execute("RPOPLPUSH", [StrArg(src), StrArg(dst),
                                         IntArg(timeout_s)]) {
            Data(ref payload) => Some(payload.to_owned()),
            _ => None,
        }
    }

    #[inline]
    pub fn rpoplpush(&mut self, src: &str, dst: &str, timeout: f32) -> Option<~str> {
        match self.rpoplpush_bytes(src, dst, timeout) {
            Some(x) => from_utf8_owned(x),
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

    // -- has commands

    #[inline]
    pub fn hdel(&mut self, key: &str) -> bool {
        match self.execute("HDEL", [StrArg(key)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn hdel_many(&mut self, keys: &[&str]) -> uint {
        let args = keys.iter().map(|&x| StrArg(x)).to_owned_vec();
        match self.execute("HDEL", args) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn hexists(&mut self, key: &str, field: &str) -> bool {
        match self.execute("HEXISTS", [StrArg(key), StrArg(field)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn hget_bytes(&mut self, key: &str, field: &str) -> Option<~[u8]> {
        match self.execute("HGET", [StrArg(key), StrArg(field)]) {
            Data(x) => Some(x),
            _ => None,
        }
    }

    #[inline]
    pub fn hget(&mut self, key: &str, field: &str) -> Option<~str> {
        match self.hget_bytes(key, field) {
            Some(x) => from_utf8_owned(x),
            None => None,
        }
    }

    #[inline]
    pub fn hget_as<T: FromStr>(&mut self, key: &str, field: &str) -> Option<T> {
        match self.hget(key, field) {
            Some(x) => from_str(x),
            None => None,
        }
    }

    #[inline]
    pub fn hgetall_bytes(&mut self, key: &str, field: &str) -> ~[~[u8]] {
        match self.execute("HGETALL", [StrArg(key), StrArg(field)]) {
            Bulk(items) => {
                let mut rv = ~[];
                for item in items.move_iter() {
                    match item {
                        Data(x) => { rv.push(x); }
                        _ => {}
                    }
                }
                rv
            }
            _ => ~[],
        }
    }

    #[inline]
    pub fn hgetall(&mut self, key: &str, field: &str) -> ~[~str] {
        self.hgetall_bytes(key, field).move_iter()
            .map(|x| from_utf8_owned(x).unwrap_or(~"")).to_owned_vec()
    }

    #[inline]
    pub fn hgetall_as<T: FromStr>(&mut self, key: &str, field: &str) -> ~[T] {
        let mut rv = ~[];
        for item in self.hgetall(key, field).move_iter() {
            match from_str(item) {
                Some(x) => { rv.push(x); }
                None => {}
            }
        }
        rv
    }

    #[inline]
    pub fn hincrby(&mut self, key: &str, field: &str, step: i64) -> i64 {
        match self.execute("HINCRBY", [StrArg(key), StrArg(field), IntArg(step)]) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn hincr(&mut self, key: &str, field: &str) -> i64 {
        self.hincrby(key, field, 1)
    }

    #[inline]
    pub fn hincrby_float(&mut self, key: &str, field: &str, step: f32) -> f32 {
        string_value_convert(self.execute("HINCRBYFLOAT",
            [StrArg(key), StrArg(field), FloatArg(step)]), 0.0f32)
    }

    #[inline]
    pub fn hkeys(&mut self, key: &str) -> ~[~str] {
        let resp = self.execute("HKEYS", [StrArg(key)]);
        value_to_string_list(resp)
    }

    #[inline]
    pub fn hlen(&mut self, key: &str) -> uint {
        match self.execute("HLEN", [StrArg(key)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    // XXX: mget mset

    #[inline]
    pub fn hset_bytes(&mut self, key: &str, field: &str, value: &[u8]) -> bool {
        match self.execute("HSET", [StrArg(key), StrArg(field), BytesArg(value)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn hset<T: ToStr>(&mut self, key: &str, field: &str, value: T) -> bool {
        let v = value.to_str();
        self.hset_bytes(key, field, v.as_bytes())
    }

    #[inline]
    pub fn hsetnx_bytes(&mut self, key: &str, field: &str, value: &[u8]) -> bool {
        match self.execute("HSETNX", [StrArg(key), StrArg(field), BytesArg(value)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn hsetnx<T: ToStr>(&mut self, key: &str, field: &str, value: T) -> bool {
        let v = value.to_str();
        self.hsetnx_bytes(key, field, v.as_bytes())
    }

    #[inline]
    pub fn hscan_bytes<'a>(&'a mut self, key: &'a str, pattern: &'a str) -> ScanIterator<'a, ~[u8]> {
        ScanIterator {
            con: self,
            cmd: "HSCAN",
            pre_args: ~[StrArg(key)],
            post_args: ~[StrArg("MATCH"), StrArg(pattern)],
            cursor: 0,
            conv_func: |value| value_to_bytes(value),
            buffer: ~[],
            end: false,
        }
    }

    #[inline]
    pub fn hscan<'a>(&'a mut self, key: &'a str, pattern: &'a str) -> ScanIterator<'a, ~str> {
        ScanIterator {
            con: self,
            cmd: "HSCAN",
            pre_args: ~[StrArg(key)],
            post_args: ~[StrArg("MATCH"), StrArg(pattern)],
            cursor: 0,
            conv_func: |value| Some(string_value_convert(value, ~"")),
            buffer: ~[],
            end: false,
        }
    }

    // -- sets

    #[inline]
    pub fn sadd_bytes(&mut self, key: &str, member: &[u8]) -> bool {
        match self.execute("SADD", [StrArg(key), BytesArg(member)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn sadd<T: ToStr>(&mut self, key: &str, member: T) -> bool {
        let v = member.to_str();
        self.sadd_bytes(key, v.as_bytes())
    }

    #[inline]
    pub fn scard(&mut self, key: &str) -> uint {
        match self.execute("SCARD", [StrArg(key)]) {
            Int(x) => x as uint,
            _ => 0,
        }
    }

    #[inline]
    pub fn sdiff_bytes(&mut self, keys: &[&str]) -> ~[~[u8]] {
        let args = keys.iter().map(|&x| StrArg(x)).to_owned_vec();
        match self.execute("SDIFF", args) {
            Bulk(items) => {
                let mut rv = ~[];
                for item in items.move_iter() {
                    match item {
                        Data(x) => { rv.push(x); }
                        _ => {}
                    }
                }
                rv
            }
            _ => ~[],
        }
    }

    #[inline]
    pub fn sdiff(&mut self, keys: &[&str]) -> ~[~str] {
        self.sdiff_bytes(keys).move_iter()
            .map(|x| from_utf8_owned(x).unwrap_or(~"")).to_owned_vec()
    }

    #[inline]
    pub fn sdiff_as<T: FromStr>(&mut self, keys: &[&str]) -> ~[T] {
        let mut rv = ~[];
        for item in self.sdiff(keys).move_iter() {
            match from_str(item) {
                Some(x) => { rv.push(x); }
                None => {}
            }
        }
        rv
    }

    #[inline]
    pub fn sdiffstore(&mut self, dst: &str, keys: &[&str]) -> i64 {
        let mut args = ~[StrArg(dst)];
        args.extend(&mut keys.iter().map(|&x| StrArg(x)));
        match self.execute("SDIFFSTORE", args) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn sinter_bytes(&mut self, keys: &[&str]) -> ~[~[u8]] {
        let args = keys.iter().map(|&x| StrArg(x)).to_owned_vec();
        match self.execute("SINTER", args) {
            Bulk(items) => {
                let mut rv = ~[];
                for item in items.move_iter() {
                    match item {
                        Data(x) => { rv.push(x); }
                        _ => {}
                    }
                }
                rv
            }
            _ => ~[],
        }
    }

    #[inline]
    pub fn sinter(&mut self, keys: &[&str]) -> ~[~str] {
        self.sinter_bytes(keys).move_iter()
            .map(|x| from_utf8_owned(x).unwrap_or(~"")).to_owned_vec()
    }

    #[inline]
    pub fn sinter_as<T: FromStr>(&mut self, keys: &[&str]) -> ~[T] {
        let mut rv = ~[];
        for item in self.sinter(keys).move_iter() {
            match from_str(item) {
                Some(x) => { rv.push(x); }
                None => {}
            }
        }
        rv
    }

    #[inline]
    pub fn sinterstore(&mut self, dst: &str, keys: &[&str]) -> i64 {
        let mut args = ~[StrArg(dst)];
        args.extend(&mut keys.iter().map(|&x| StrArg(x)));
        match self.execute("SINTERSTORE", args) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn sismember_bytes(&mut self, key: &str, member: &[u8]) -> bool {
        match self.execute("SISMEMBER", [StrArg(key), BytesArg(member)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn isimember<T: ToStr>(&mut self, key: &str, member: T) -> bool {
        let v = member.to_str();
        self.sismember_bytes(key, v.as_bytes())
    }

    #[inline]
    pub fn smembers_bytes(&mut self, key: &str) -> ~[~[u8]] {
        let resp = self.execute("SMEMBERS", [StrArg(key)]);
        value_to_byte_list(resp)
    }

    #[inline]
    pub fn smembers(&mut self, key: &str) -> ~[~str] {
        let resp = self.execute("SMEMBERS", [StrArg(key)]);
        value_to_string_list(resp)
    }

    #[inline]
    pub fn sscan_bytes<'a>(&'a mut self, key: &'a str, pattern: &'a str) -> ScanIterator<'a, ~[u8]> {
        ScanIterator {
            con: self,
            cmd: "SSCAN",
            pre_args: ~[StrArg(key)],
            post_args: ~[StrArg("MATCH"), StrArg(pattern)],
            cursor: 0,
            conv_func: |value| value_to_bytes(value),
            buffer: ~[],
            end: false,
        }
    }

    #[inline]
    pub fn sscan<'a>(&'a mut self, key: &'a str, pattern: &'a str) -> ScanIterator<'a, ~str> {
        ScanIterator {
            con: self,
            cmd: "SSCAN",
            pre_args: ~[StrArg(key)],
            post_args: ~[StrArg("MATCH"), StrArg(pattern)],
            cursor: 0,
            conv_func: |value| Some(string_value_convert(value, ~"")),
            buffer: ~[],
            end: false,
        }
    }

    #[inline]
    pub fn sunion_bytes(&mut self, keys: &[&str]) -> ~[~[u8]] {
        let args = keys.iter().map(|&x| StrArg(x)).to_owned_vec();
        match self.execute("SUNION", args) {
            Bulk(items) => {
                let mut rv = ~[];
                for item in items.move_iter() {
                    match item {
                        Data(x) => { rv.push(x); }
                        _ => {}
                    }
                }
                rv
            }
            _ => ~[],
        }
    }

    #[inline]
    pub fn sunion(&mut self, keys: &[&str]) -> ~[~str] {
        self.sunion_bytes(keys).move_iter()
            .map(|x| from_utf8_owned(x).unwrap_or(~"")).to_owned_vec()
    }

    #[inline]
    pub fn sunion_as<T: FromStr>(&mut self, keys: &[&str]) -> ~[T] {
        let mut rv = ~[];
        for item in self.sunion(keys).move_iter() {
            match from_str(item) {
                Some(x) => { rv.push(x); }
                None => {}
            }
        }
        rv
    }

    #[inline]
    pub fn sunionstore(&mut self, dst: &str, keys: &[&str]) -> i64 {
        let mut args = ~[StrArg(dst)];
        args.extend(&mut keys.iter().map(|&x| StrArg(x)));
        match self.execute("SUNIONSTORE", args) {
            Int(x) => x,
            _ => 0,
        }
    }

    // -- sorted sets

    #[inline]
    pub fn zadd_bytes(&mut self, key: &str, score: f32, member: &[u8]) -> bool {
        match self.execute("ZADD", [StrArg(key), FloatArg(score), BytesArg(member)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn zadd<T: ToStr>(&mut self, key: &str, score: f32, member: T) -> bool {
        let v = member.to_str();
        self.zadd_bytes(key, score, v.as_bytes())
    }

    // XXX: many add

    #[inline]
    pub fn zcard(&mut self, key: &str) -> i64 {
        match self.execute("ZCARD", [StrArg(key)]) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn zcount(&mut self, key: &str, min: RangeBoundary, max: RangeBoundary) -> i64 {
        let min_s = min.to_str();
        let max_s = max.to_str();
        match self.execute("ZCOUNT", [StrArg(key), StrArg(min_s), StrArg(max_s)]) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn zincr_bytes(&mut self, key: &str, member: &[u8]) -> i64 {
        self.zincrby_bytes(key, 1.0, member)
    }

    #[inline]
    pub fn zincrby_bytes(&mut self, key: &str, increment: f32, member: &[u8]) -> i64 {
        match self.execute("ZINCRBY", [StrArg(key), FloatArg(increment),
                                       BytesArg(member)]) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn zincr<T: ToStr>(&mut self, key: &str, member: T) -> i64 {
        let v = member.to_str();
        self.zincr_bytes(key, v.as_bytes())
    }

    #[inline]
    pub fn zincrby<T: ToStr>(&mut self, key: &str, increment: f32, member: T) -> i64 {
        let v = member.to_str();
        self.zincrby_bytes(key, increment, v.as_bytes())
    }

    // XXX: interstore

    #[inline]
    pub fn zrange_bytes(&mut self, key: &str, start: i64, stop: i64) -> ~[~[u8]] {
        value_to_byte_list(self.execute("ZRANGE",
            [StrArg(key), IntArg(start), IntArg(stop)]))
    }

    #[inline]
    pub fn zrange(&mut self, key: &str, start: i64, stop: i64) -> ~[~str] {
        value_to_string_list(self.execute("ZRANGE",
            [StrArg(key), IntArg(start), IntArg(stop)]))
    }

    pub fn zrange_bytes_withscores(&mut self, key: &str, start: i64, stop: i64) -> ~[(~[u8], f32)] {
        value_to_byte_float_tuples(self.execute("ZRANGE",
            [StrArg(key), IntArg(start), IntArg(stop), StrArg("WITHSCORES")]))
    }

    #[inline]
    pub fn zrange_withscores(&mut self, key: &str, start: i64, stop: i64) -> ~[(~str, f32)] {
        self.zrange_bytes_withscores(key, start, stop).move_iter()
            .map(|(member, score)| (from_utf8_owned(member).unwrap_or(~""), score)).to_owned_vec()
    }

    fn zrangebyscore_operation(&mut self, cmd: &str,
                               key: &str, min: RangeBoundary,
                               max: RangeBoundary,
                               withscores: bool,
                               limit: Option<(i64, i64)>) -> Value {
        let min_s = min.to_str();
        let max_s = max.to_str();
        let mut args = ~[StrArg(key), StrArg(min_s), StrArg(max_s)];
        if withscores {
            args.push(StrArg("WITHSCORES"));
        }
        match limit {
            Some((offset, count)) => {
                args.push(IntArg(offset));
                args.push(IntArg(count));
            },
            None => {}
        }
        self.execute(cmd, args)
    }

    #[inline]
    pub fn zrangebyscore_bytes(&mut self, key: &str, min: RangeBoundary,
                               max: RangeBoundary,
                               limit: Option<(i64, i64)>) -> ~[~[u8]] {
        value_to_byte_list(self.zrangebyscore_operation(
            "ZRANGEBYSCORE", key, min, max, false, limit))
    }

    #[inline]
    pub fn zrangebyscore(&mut self, key: &str, min: RangeBoundary,
                         max: RangeBoundary,
                         limit: Option<(i64, i64)>) -> ~[~str] {
        value_to_string_list(self.zrangebyscore_operation(
            "ZRANGEBYSCORE", key, min, max, false, limit))
    }

    #[inline]
    pub fn zrangebyscore_bytes_withscores(&mut self, key: &str, min: RangeBoundary,
                                          max: RangeBoundary,
                                          limit: Option<(i64, i64)>) -> ~[(~[u8], f32)] {
        value_to_byte_float_tuples(self.zrangebyscore_operation(
            "ZRANGEBYSCORE", key, min, max, true, limit))
    }

    #[inline]
    pub fn zrangebyscore_withscores(&mut self, key: &str, min: RangeBoundary,
                                    max: RangeBoundary,
                                    limit: Option<(i64, i64)>) -> ~[(~str, f32)] {
        self.zrangebyscore_bytes_withscores(key, min, max, limit).move_iter()
            .map(|(member, score)| (from_utf8_owned(member).unwrap_or(~""), score)).to_owned_vec()
    }

    #[inline]
    pub fn zrank_bytes(&mut self, key: &str, member: &[u8]) -> Option<i64> {
        match self.execute("ZRANK", [StrArg(key), BytesArg(member)]) {
            Int(x) => Some(x),
            _ => None,
        }
    }

    #[inline]
    pub fn zrank(&mut self, key: &str, member: &str) -> Option<i64> {
        self.zrank_bytes(key, member.as_bytes())
    }

    #[inline]
    pub fn zrem_bytes(&mut self, key: &str, member: &[u8]) -> bool {
        match self.execute("ZREM", [StrArg(key), BytesArg(member)]) {
            Int(1) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn zrem(&mut self, key: &str, member: &str) -> bool {
        self.zrem_bytes(key, member.as_bytes())
    }

    #[inline]
    pub fn zrem_bytes_many(&mut self, key: &str, members: &[&[u8]]) -> i64 {
        let mut args = ~[StrArg(key)];
        args.extend(&mut members.iter().map(|&x| BytesArg(x)));
        match self.execute("ZREM", args) {
            Int(x) => x,
            _ => 0,
        }
    }

    #[inline]
    pub fn zrem_many(&mut self, key: &str, members: &[&str]) -> i64 {
        self.zrem_bytes_many(key, members.iter().map(|&x| x.as_bytes()).to_owned_vec())
    }

    // XXX: ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE ZREVRANK 

    #[inline]
    pub fn zscan_bytes<'a>(&'a mut self, key: &'a str, pattern: &'a str) -> ScanIterator<'a, ~[u8]> {
        ScanIterator {
            con: self,
            cmd: "ZSCAN",
            pre_args: ~[StrArg(key)],
            post_args: ~[StrArg("MATCH"), StrArg(pattern)],
            cursor: 0,
            conv_func: |value| value_to_bytes(value),
            buffer: ~[],
            end: false,
        }
    }

    #[inline]
    pub fn zscan<'a>(&'a mut self, key: &'a str, pattern: &'a str) -> ScanIterator<'a, ~str> {
        ScanIterator {
            con: self,
            cmd: "ZSCAN",
            pre_args: ~[StrArg(key)],
            post_args: ~[StrArg("MATCH"), StrArg(pattern)],
            cursor: 0,
            conv_func: |value| Some(string_value_convert(value, ~"")),
            buffer: ~[],
            end: false,
        }
    }

    #[inline]
    pub fn zscore_bytes(&mut self, key: &str, member: &[u8]) -> Option<i64> {
        match self.execute("ZSCORE", [StrArg(key), BytesArg(member)]) {
            Int(x) => Some(x),
            _ => None,
        }
    }

    #[inline]
    pub fn zscore(&mut self, key: &str, member: &str) -> Option<i64> {
        self.zscore_bytes(key, member.as_bytes())
    }

    // XXX: ZUNIONSTORE
}
