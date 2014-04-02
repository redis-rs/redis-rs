use std::str;

use std::io::Reader;
use std::str::from_utf8;

use enums::*;
mod macros;


pub struct Parser<T> {
    iter: T,
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
        let b = try_unwrap!(self.iter.next(), Invalid);
        match b as char {
            '+' => self.parse_status(),
            ':' => self.parse_int(),
            '$' => self.parse_data(),
            '*' => self.parse_bulk(),
            '-' => self.parse_error(),
            _ => Invalid,
        }
    }

    #[inline]
    fn expect_char(&mut self, refchar: char) -> bool {
        match self.iter.next() {
            Some(c) => {
                if c as char == refchar {
                    return true;
                }
            },
            _ => {}
        }
        return false;
    }

    #[inline]
    fn expect_newline(&mut self) -> bool {
        let c = try_unwrap!(self.iter.next(), false) as char;
        if c == '\n' {
            return true;
        }
        if c != '\r' {
            return false;
        }
        return self.expect_char('\n');
    }

    fn read_line(&mut self) -> Option<~[u8]> {
        let mut rv = ~[];

        loop {
            let b = try_unwrap!(self.iter.next(), None);
            match b as char {
                '\n' => { break; }
                '\r' => {
                    if self.expect_char('\n') {
                        break;
                    } else {
                        return None;
                    }
                },
                _ => { rv.push(b) }
            };
        }

        Some(rv)
    }

    fn read(&mut self, bytes: uint) -> Option<~[u8]> {
        let mut rv = ~[];
        rv.reserve(bytes);

        for _ in range(0, bytes) {
            rv.push(try_unwrap!(self.iter.next(), None));
        }

        Some(rv)
    }

    fn read_int_line(&mut self) -> Option<i64> {
        let line = try_unwrap!(self.read_line(), None);
        from_str(try_unwrap!(from_utf8(line), None).trim())
    }

    fn parse_status(&mut self) -> Value {
        let line = try_unwrap!(self.read_line(), Invalid);
        let s = try_unwrap!(str::from_utf8_owned(line), Invalid);
        if s == ~"OK" {
            Success
        } else {
            Status(s)
        }
    }

    fn parse_int(&mut self) -> Value {
        Int(try_unwrap!(self.read_int_line(), Invalid))
    }

    fn parse_data(&mut self) -> Value {
        let length = try_unwrap!(self.read_int_line(), Invalid);
        if length < 0 {
            Nil
        } else {
            let data = try_unwrap!(self.read(length as uint), Invalid);
            if !self.expect_newline() {
                Invalid
            } else {
                Data(data)
            }
        }
    }

    fn parse_bulk(&mut self) -> Value {
        let length = try_unwrap!(self.read_int_line(), Invalid);
        if length < 0 {
            Nil
        } else {
            let mut rv = ~[];
            rv.reserve(length as uint);
            for _ in range(0, length) {
                match self.parse_value() {
                    Invalid => { return Invalid; }
                    value => rv.push(value)
                };
            }
            Bulk(rv)
        }
    }

    fn parse_error(&mut self) -> Value {
        let byte_line = try_unwrap!(self.read_line(), Invalid);
        let line = try_unwrap!(str::from_utf8(byte_line), Invalid);
        let mut pieces = line.splitn(' ', 1);
        let code = match pieces.next().unwrap() {
            "ERR" => ResponseError,
            "EXECABORT" => ExecAbortError,
            "LOADING" => BusyLoadingError,
            "NOSCRIPT" => NoScriptError,
            "" => UnknownError,
            other => ExtensionError(other.to_owned()),
        };
        let message = pieces.next().unwrap_or("An unknown error ocurred.");
        return Error(code, message.to_owned());
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
        self.reader.read_byte().ok()
    }
}
