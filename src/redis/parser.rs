use std::str;

use std::io::Reader;
use std::str::from_utf8;

use enums::*;


pub struct Parser<T> {
    priv iter: T,
}

pub struct ByteIterator<'a> {
    reader: &'a mut Reader,
}

impl<T: Iterator<u8>> Parser<T> {

    /// Creates a new parser from a character source iterator.
    pub fn new(iter: T) -> Parser<T> {
        Parser { iter: iter }
    }

    /// parses a value from the iterator
    pub fn parse_value(&mut self) -> Value {
        match self.next_byte() as char {
            '+' => self.parse_status(),
            ':' => self.parse_int(),
            '$' => self.parse_data(),
            '*' => self.parse_bulk(),
            '-' => self.parse_error(),
            _ => Invalid,
        }
    }

    fn next_byte(&mut self) -> u8 {
        match self.iter.next() {
            Some(b) => b,
            None => 0xffu8,
        }
    }

    fn eat_terminator(&mut self, current: u8) -> bool {
        if (current as char == '\n') {
            return true;
        }
        if (current as char != '\r') {
            return false;
        }
        let next = self.next_byte();
        if (next as char != '\n') {
            return false;
        }
        return true;
    }

    fn read_line(&mut self) -> ~[u8] {
        let mut rv = ~[];

        loop {
            let b = self.next_byte();
            if self.eat_terminator(b) {
                break;
            }
            rv.push(b);
        }

        rv
    }

    fn read(&mut self, bytes: uint) -> ~[u8] {
        let mut rv = ~[];
        rv.reserve_at_least(bytes);

        for _ in range(0, bytes) {
            rv.push(self.next_byte());
        }

        rv
    }

    fn read_int_line(&mut self) -> i64 {
        let line = self.read_line();
        from_str(from_utf8(line).trim()).unwrap_or(-1)
    }

    fn parse_status(&mut self) -> Value {
        let line = self.read_line();
        Status(str::from_utf8_owned(line))
    }

    fn parse_int(&mut self) -> Value {
        Int(self.read_int_line() as int)
    }

    fn parse_data(&mut self) -> Value {
        let length = self.read_int_line();
        if length < 0 {
            Nil
        } else {
            let rv = self.read(length as uint);
            let b = self.next_byte();
            self.eat_terminator(b);
            Data(rv)
        }
    }

    fn parse_bulk(&mut self) -> Value {
        let length = self.read_int_line();
        if length < 0 {
            Nil
        } else {
            let mut rv = ~[];
            rv.reserve_at_least(length as uint);
            for _ in range(0, length) {
                rv.push(self.parse_value());
            }
            Bulk(rv)
        }
    }

    fn parse_error(&mut self) -> Value {
        let byte_line = self.read_line();
        let line = str::from_utf8(byte_line);
        let mut pieces = line.splitn(' ', 1);
        let code = match pieces.next().unwrap() {
            "ERR" => ResponseError,
            "EXECABORT" => ExecAbortError,
            "LOADING" => BusyLoadingError,
            "NOSCRIPT" => NoScriptError,
            "" => UnknownError,
            other => ExtensionError(other.to_owned()),
        };
        let message = pieces.next().unwrap_or("An unknown error ocurred.").to_owned();
        return Error(code, message);
    }
}


/// Parses bytes into a redis value
pub fn parse_redis_value(bytes: &[u8]) -> Value {
    let mut parser = Parser::new(bytes.iter().map(|x| *x));
    parser.parse_value()
}


/// A simple iterator helper that reads bytes from a reader
impl<'a> Iterator<u8> for ByteIterator<'a> {
    #[inline]
    fn next(&mut self) -> Option<u8> {
        self.reader.read_byte()
    }
}
