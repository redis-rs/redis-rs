use std::io::{Reader, BufReader};
use std::str::from_utf8;

use enums::{RedisResult, Nil, Int, Data, Bulk, Okay, Status, RedisError,
            ResponseError, ExecAbortError, BusyLoadingError,
            NoScriptError, ExtensionError, InternalIoError};


pub struct Parser<T> {
    reader: T,
}

macro_rules! try_io {
    ($expr:expr) => (
        match $expr {
            Err(err) => {
                return Err(RedisError::simple(
                    InternalIoError(err),
                    "Operation failed because of an IO error"));
            },
            Ok(x) => x,
        }
    )
}

impl<'a, T: Reader> Parser<T> {

    pub fn new(reader: T) -> Parser<T> {
        Parser { reader: reader }
    }

    /* internal helpers */

    #[inline]
    fn expect_char(&mut self, refchar: char) -> Result<(), RedisError> {
        if try_io!(self.reader.read_byte()) as char == refchar {
            Ok(())
        } else {
            Err(RedisError::simple(ResponseError, "Invalid byte in response"))
        }
    }

    #[inline]
    fn expect_newline(&mut self) -> Result<(), RedisError> {
        match try_io!(self.reader.read_byte()) as char {
            '\n' => { Ok(()) }
            '\r' => { self.expect_char('\n') }
            _ => { Err(RedisError::simple(
                ResponseError, "Invalid byte in response")) }
        }
    }

    fn read_line(&mut self) -> Result<Vec<u8>, RedisError> {
        let mut rv = vec![];

        loop {
            let b = try_io!(self.reader.read_byte());
            match b as char {
                '\n' => { break; }
                '\r' => {
                    try!(self.expect_char('\n'));
                    break;
                },
                _ => { rv.push(b) }
            };
        }

        Ok(rv)
    }

    fn read_string_line(&mut self) -> Result<String, RedisError> {
        match String::from_utf8(try!(self.read_line())) {
            Err(_) => {
                Err(RedisError::simple(
                    ResponseError,
                        "Expected valid string, got garbage"))
            }
            Ok(value) => Ok(value),
        }
    }

    fn read(&mut self, bytes: uint) -> Result<Vec<u8>, RedisError> {
        let mut rv = vec![];
        rv.reserve(bytes);

        for _ in range(0, bytes) {
            rv.push(try_io!(self.reader.read_byte()));
        }

        Ok(rv)
    }

    fn read_int_line(&mut self) -> Result<i64, RedisError> {
        let line = try!(self.read_string_line());
        match from_str::<i64>(line.as_slice().trim()) {
            None => Err(RedisError::simple(ResponseError, "Expected integer, got garbage")),
            Some(value) => Ok(value)
        }
    }

    /* public api */

    pub fn parse_value(&mut self) -> RedisResult {
        let b = try_io!(self.reader.read_byte());
        match b as char {
            '+' => self.parse_status(),
            ':' => self.parse_int(),
            '$' => self.parse_data(),
            '*' => self.parse_bulk(),
            '-' => self.parse_error(),
            _ => Err(RedisError::simple(ResponseError,
                                        "Invalid response when parsing value")),
        }
    }

    fn parse_status(&mut self) -> RedisResult {
        let line = try!(self.read_string_line());
        if line.as_slice() == "OK" {
            Ok(Okay)
        } else {
            Ok(Status(line))
        }
    }

    fn parse_int(&mut self) -> RedisResult {
        Ok(Int(try!(self.read_int_line())))
    }

    fn parse_data(&mut self) -> RedisResult {
        let length = try!(self.read_int_line());
        if length < 0 {
            Ok(Nil)
        } else {
            let data = try!(self.read(length as uint));
            try!(self.expect_newline());
            Ok(Data(data))
        }
    }

    fn parse_bulk(&mut self) -> RedisResult {
        let length = try!(self.read_int_line());
        if length < 0 {
            Ok(Nil)
        } else {
            let mut rv = vec![];
            rv.reserve(length as uint);
            for _ in range(0, length) {
                rv.push(try!(self.parse_value()));
            }
            Ok(Bulk(rv))
        }
    }

    fn parse_error(&mut self) -> RedisResult {
        let line = try!(self.read_string_line());
        let mut pieces = line.as_slice().splitn(1, ' ');
        let kind = match pieces.next().unwrap() {
            "ERR" => ResponseError,
            "EXECABORT" => ExecAbortError,
            "LOADING" => BusyLoadingError,
            "NOSCRIPT" => NoScriptError,
            other => ExtensionError(other.to_string()),
        };
        let message = pieces.next().unwrap_or("An unknown error ocurred.");
        Err(RedisError {
            kind: kind,
            desc: "A error was signalled by the server",
            detail: Some(message.to_string()),
        })
    }
}


/// Parses bytes into a redis value
pub fn parse_redis_value(bytes: &[u8]) -> RedisResult {
    let mut parser = Parser::new(BufReader::new(bytes));
    parser.parse_value()
}
