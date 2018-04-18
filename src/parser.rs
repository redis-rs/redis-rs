use std::io::{Read, BufReader};

use types::{RedisResult, Value, ErrorKind, make_extension_error};


/// The internal redis response parser.
pub struct Parser<T> {
    reader: T,
}

/// The parser can be used to parse redis responses into values.  Generally
/// you normally do not use this directly as it's already done for you by
/// the client but in some more complex situations it might be useful to be
/// able to parse the redis responses.
impl<'a, T: Read> Parser<T> {
    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub fn new(reader: T) -> Parser<T> {
        Parser { reader: reader }
    }

    // public api

    /// parses a single value out of the stream.  If there are multiple
    /// values you can call this multiple times.  If the reader is not yet
    /// ready this will block.
    pub fn parse_value(&mut self) -> RedisResult<Value> {
        let b = try!(self.read_byte());
        match b as char {
            '+' => self.parse_status(),
            ':' => self.parse_int(),
            '$' => self.parse_data(),
            '*' => self.parse_bulk(),
            '-' => self.parse_error(),
            _ => fail!((ErrorKind::ResponseError, "Invalid response when parsing value")),
        }
    }

    // internal helpers

    #[inline]
    fn expect_char(&mut self, refchar: char) -> RedisResult<()> {
        if try!(self.read_byte()) as char == refchar {
            Ok(())
        } else {
            fail!((ErrorKind::ResponseError, "Invalid byte in response"));
        }
    }

    #[inline]
    fn expect_newline(&mut self) -> RedisResult<()> {
        match try!(self.read_byte()) as char {
            '\n' => Ok(()),
            '\r' => self.expect_char('\n'),
            _ => fail!((ErrorKind::ResponseError, "Invalid byte in response")),
        }
    }

    fn read_line(&mut self) -> RedisResult<Vec<u8>> {
        let mut rv = vec![];

        loop {
            let b = try!(self.read_byte());
            match b as char {
                '\n' => {
                    break;
                }
                '\r' => {
                    try!(self.expect_char('\n'));
                    break;
                }
                _ => rv.push(b),
            };
        }

        Ok(rv)
    }

    fn read_string_line(&mut self) -> RedisResult<String> {
        match String::from_utf8(try!(self.read_line())) {
            Err(_) => fail!((ErrorKind::ResponseError, "Expected valid string, got garbage")),
            Ok(value) => Ok(value),
        }
    }

    fn read_byte(&mut self) -> RedisResult<u8> {
        let buf: &mut [u8; 1] = &mut [0];
        let nread = try!(self.reader.read(buf));

        if nread < 1 {
            fail!((ErrorKind::ResponseError, "Could not read enough bytes"))
        } else {
            Ok(buf[0])
        }
    }

    fn read(&mut self, bytes: usize) -> RedisResult<Vec<u8>> {
        let mut rv = Vec::with_capacity(bytes);
        unsafe { rv.set_len(bytes); }
        try!(self.reader.read_exact(&mut rv));
        Ok(rv)
    }

    fn read_int_line(&mut self) -> RedisResult<i64> {
        let mut int = 0i64;
        let mut neg = 1;

        let mut b = try!(self.read_byte());
        if b as char == '-' {
            neg = -1;
            b = try!(self.read_byte());
        }

        loop {
            match b as char {
                '0' ... '9' => {
                    int = (int * 10) + (b - '0' as u8) as i64;
                }
                '\n' => {
                    break;
                }
                '\r' => {
                    try!(self.expect_char('\n'));
                    break;
                }
                _ => fail!((ErrorKind::ResponseError, "Expected integer, got garbage"))
            };
            b = try!(self.read_byte());
        }

        Ok(neg * int)
    }

    fn parse_status(&mut self) -> RedisResult<Value> {
        let line = try!(self.read_string_line());
        if line == "OK" {
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
            let data = try!(self.read(length as usize));
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
            rv.reserve(length as usize);
            for _ in 0..length {
                rv.push(try!(self.parse_value()));
            }
            Ok(Value::Bulk(rv))
        }
    }

    fn parse_error(&mut self) -> RedisResult<Value> {
        let desc = "An error was signalled by the server";
        let line = try!(self.read_string_line());
        let mut pieces = line.splitn(2, ' ');
        let kind = match pieces.next().unwrap() {
            "ERR" => ErrorKind::ResponseError,
            "EXECABORT" => ErrorKind::ExecAbortError,
            "LOADING" => ErrorKind::BusyLoadingError,
            "NOSCRIPT" => ErrorKind::NoScriptError,
            code => {
                fail!(make_extension_error(code, pieces.next()));
            }
        };
        match pieces.next() {
            Some(detail) => fail!((kind, desc, detail.to_string())),
            None => fail!((kind, desc)),
        }
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
