use std::io::{Reader, BufReader};
use std::str::from_utf8;

use types::{RedisResult, RedisError, Value,
            ResponseError, ExecAbortError, BusyLoadingError,
            NoScriptError, ExtensionError};


/// The internal redis response parser.
pub struct Parser<T> {
    reader: T,
}

/// The parser can be used to parse redis responses into values.  Generally
/// you normally do not use this directly as it's already done for you by
/// the client but in some more complex situations it might be useful to be
/// able to parse the redis responses.
impl<'a, T: Reader> Parser<T> {

    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub fn new(reader: T) -> Parser<T> {
        Parser { reader: reader }
    }

    /* public api */

    /// parses a single value out of the stream.  If there are multiple
    /// values you can call this multiple times.  If the reader is not yet
    /// ready this will block.
    pub fn parse_value(&mut self) -> RedisResult<Value> {
        let b = try!(self.reader.read_byte());
        match b as char {
            '+' => self.parse_status(),
            ':' => self.parse_int(),
            '$' => self.parse_data(),
            '*' => self.parse_bulk(),
            '-' => self.parse_error(),
            _ => fail!((ResponseError, "Invalid response when parsing value")),
        }
    }

    /* internal helpers */

    #[inline]
    fn expect_char(&mut self, refchar: char) -> RedisResult<()> {
        if try!(self.reader.read_byte()) as char == refchar {
            Ok(())
        } else {
            fail!((ResponseError, "Invalid byte in response"));
        }
    }

    #[inline]
    fn expect_newline(&mut self) -> RedisResult<()> {
        match try!(self.reader.read_byte()) as char {
            '\n' => Ok(()),
            '\r' => self.expect_char('\n'),
            _ => fail!((ResponseError, "Invalid byte in response")),
        }
    }

    fn read_line(&mut self) -> RedisResult<Vec<u8>> {
        let mut rv = vec![];

        loop {
            let b = try!(self.reader.read_byte());
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

    fn read_string_line(&mut self) -> RedisResult<String> {
        match String::from_utf8(try!(self.read_line())) {
            Err(_) => {
                fail!((ResponseError, "Expected valid string, got garbage"))
            }
            Ok(value) => Ok(value),
        }
    }

    fn read(&mut self, bytes: uint) -> RedisResult<Vec<u8>> {
        let mut rv = vec![];
        rv.reserve(bytes);

        for _ in range(0, bytes) {
            rv.push(try!(self.reader.read_byte()));
        }

        Ok(rv)
    }

    fn read_int_line(&mut self) -> RedisResult<i64> {
        let line = try!(self.read_string_line());
        match line.as_slice().trim().parse::<i64>() {
            None => fail!((ResponseError, "Expected integer, got garbage")),
            Some(value) => Ok(value)
        }
    }

    fn parse_status(&mut self) -> RedisResult<Value> {
        let line = try!(self.read_string_line());
        if line.as_slice() == "OK" {
            Ok(Value::Okay)
        } else {
            Ok(Value::Status(line))
        }
    }

    fn parse_int(&mut self) -> RedisResult<Value> {
        Ok(Value::Int(try!(self.read_int_line())))
    }

    fn parse_data(&mut self) -> RedisResult<Value> {
        let length = try!(self.read_int_line());
        if length < 0 {
            Ok(Value::Nil)
        } else {
            let data = try!(self.read(length as uint));
            try!(self.expect_newline());
            Ok(Value::Data(data))
        }
    }

    fn parse_bulk(&mut self) -> RedisResult<Value> {
        let length = try!(self.read_int_line());
        if length < 0 {
            Ok(Value::Nil)
        } else {
            let mut rv = vec![];
            rv.reserve(length as uint);
            for _ in range(0, length) {
                rv.push(try!(self.parse_value()));
            }
            Ok(Value::Bulk(rv))
        }
    }

    fn parse_error(&mut self) -> RedisResult<Value> {
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
        fail!(RedisError {
            kind: kind,
            desc: "A error was signalled by the server",
            detail: Some(message.to_string()),
        })
    }
}


/// Parses bytes into a redis value.
///
/// This is the most straightforward way to parse something into a low
/// level redis value instead of having to use a whole parser.
pub fn parse_redis_value(bytes: &[u8]) -> RedisResult<Value> {
    let mut parser = Parser::new(BufReader::new(bytes));
    parser.parse_value()
}
