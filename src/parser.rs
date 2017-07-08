use std::io::Read;
use std::str::{self, FromStr};

use types::{RedisResult, Value, ErrorKind, make_extension_error};

use nom::{IResult, line_ending};

/// The internal redis response parser.
pub struct Parser<T> {
    reader: T
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

    fn read_byte(&mut self) -> RedisResult<u8> {
        let buf: &mut [u8; 1] = &mut [0];
        let nread = self.reader.read(buf)?;

        if nread < 1 {
            fail!((ErrorKind::ResponseError, "failed to read a byte"))
        } else {
            Ok(buf[0])
        }
    }

    fn read_line(&mut self) -> RedisResult<Vec<u8>> {
        let mut resp = vec![];

        loop {
            let b = self.read_byte()?;
            resp.push(b);

            if b == b'\n' {
                break;
            }
        }

        Ok(resp)
    }

    // public api

    /// parses a single value out of the stream.  If there are multiple
    /// values you can call this multiple times.  If the reader is not yet
    /// ready this will block.
    pub fn parse_value(&mut self) -> RedisResult<Value> {
        let mut response = vec![];

        let buffer = self.read_line()?;
        response.extend(&buffer);

        if buffer[0] == b'$' && buffer[1] != b'-' {
            let len = find_len(&buffer[1..]);
            let buf = self.handle_bulk_string(len)?;
            response.extend(&buf);
        } else if buffer[0] == b'*' && buffer[1] != b'-' {
            let len = find_len(&buffer[1..]);
            let buf = self.handle_array(len)?;
            response.extend(&buf);
        }

        match parse_resp(&response) {
            IResult::Done(_, output) => {
                return output;
            }
            IResult::Incomplete(_) => {
                fail!((ErrorKind::ResponseError, "incomplete"))
            }
            IResult::Error(_) => {
                fail!((ErrorKind::ResponseError, "error parsing value"))
            }
        }
    }

    fn handle_bulk_string(&mut self, len: u64) -> RedisResult<Vec<u8>> {
        let mut resp = vec![];

        // We add an additional two reads for the \r\n.
        for _ in 0..len + 2 {
            resp.push(self.read_byte()?);
        }

        Ok(resp)
    }

    fn handle_array(&mut self, len: u64) -> RedisResult<Vec<u8>> {
        let mut response = vec![];
        let mut count = 0;

        while count < len {
            let buf = self.read_line()?;
            response.extend(&buf);

            if buf[0] == b'*' && buf[1] != b'-' {
                let inner_len = find_len(&buf[1..]);
                let resp = self.handle_array(inner_len)?;
                response.extend(&resp);
            } else if buf[0] == b'$' && buf[1] != b'-' {
                let inner_len = find_len(&buf[1..]);
                let resp = self.handle_bulk_string(inner_len)?;
                response.extend(&resp);
            }

            count += 1;
        }

        Ok(response)
    }
}

fn find_len(buffer: &[u8]) -> u64 {
    let mut size = vec![];
    for b in buffer {
        if *b != b'\r' {
            size.push(*b);
        } else {
            break;
        }
    }

    let s;
    unsafe {
        s = str::from_utf8_unchecked(&size[..]);
    }

    match s.parse::<u64>() {
        Ok(l) => l,
        Err(_) => 0,
    }
}

fn check_okay(response: &str) -> Value {
    match response {
        "OK" => Value::Okay,
        _ => Value::Status(String::from(response)),
    }
}

fn check_array_errors(values: Vec<RedisResult<Value>>) -> RedisResult<Value> {
    let mut bulk = vec![];
    for v in values {
        if v.is_err() {
            return v;
        } else {
            bulk.push(v.unwrap());
        }
    }
    Ok(Value::Bulk(bulk))
}

fn parse_error(line: &str) -> RedisResult<Value> {
    let desc = "An error was signaled by the server";
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

named!(up_to_eol, take_until!("\r\n"));

named!(simple_string<RedisResult<Value> >,
    do_parse!(
        char!('+') >>
        res: map!(map_res!(up_to_eol, str::from_utf8), check_okay) >>
        line_ending >>
        (Ok(res))
    )
);

named!(error<RedisResult<Value> >,
    do_parse!(
        char!('-') >>
        res: map!(map_res!(up_to_eol, str::from_utf8), parse_error) >>
        line_ending >>
        (res)
    )
);

named!(integer<RedisResult<Value> >,
    do_parse!(
        char!(':') >>
        res: map_res!(map_res!(up_to_eol, str::from_utf8), i64::from_str) >>
        line_ending >>
        (Ok(Value::Int(res)))
    )
);

named!(bulk_string<RedisResult<Value> >,
    do_parse!(
        char!('$') >>
        len: map_res!(map_res!(up_to_eol, str::from_utf8), i64::from_str) >>
        line_ending >>
        res: take!(len) >>
        line_ending >>
        (Ok(Value::Data(res.to_vec())))
    )
);

named!(null_bulk_string<RedisResult<Value> >,
    do_parse!(
        tag!("$-") >>
        char!('1') >>
        line_ending >>
        (Ok(Value::Nil))
    )
);

named!(null_array<RedisResult<Value> >,
    do_parse!(
        tag!("*-") >>
        char!('1') >>
        line_ending >>
        (Ok(Value::Nil))
    )
);

named!(empty_array<RedisResult<Value> >,
    do_parse!(
        tag!("*") >>
        char!('0') >>
        line_ending >>
        (Ok(Value::Bulk(vec![])))
    )
);

named!(parse_resp<RedisResult<Value> >,
    alt!(null_bulk_string | null_array | empty_array | integer | simple_string |
        error | bulk_string | array));

named!(array<RedisResult<Value> >,
    do_parse!(
        tag!("*") >>
        len: map_res!(map_res!(up_to_eol, str::from_utf8), i64::from_str) >>
        line_ending >>
        res: map!(many0!(parse_resp), check_array_errors) >>
        (res)
    )
);

/// Parses bytes into a redis value.
///
/// This is the most straightforward way to parse something into a low
/// level redis value instead of having to use a whole parser.
pub fn parse_redis_value(bytes: &[u8]) -> RedisResult<Value> {
    match parse_resp(bytes) {
        IResult::Done(_, output) => output,
        _ => fail!((ErrorKind::ResponseError, "error parsing response")),
    }
}
