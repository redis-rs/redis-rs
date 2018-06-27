use std::io::{self, BufRead, BufReader};
use std::str;

use types::{make_extension_error, ErrorKind, RedisError, RedisResult, Value};

use futures::{Async, Future, Poll};
use tokio_io::AsyncRead;

use combine;
use combine::byte::{byte, crlf, newline};
use combine::combinator::{any_send_partial_state, AnySendPartialState};
#[allow(unused_imports)] // See https://github.com/rust-lang/rust/issues/43970
use combine::error::StreamError;
use combine::parser::choice::choice;
use combine::range::{recognize, take, take_until_range};
use combine::stream::{RangeStream, StreamErrorFor};

struct ResultExtend<T, E>(Result<T, E>);

impl<T, E> Default for ResultExtend<T, E>
where
    T: Default,
{
    fn default() -> Self {
        ResultExtend(Ok(T::default()))
    }
}

impl<T, U, E> Extend<Result<U, E>> for ResultExtend<T, E>
where
    T: Extend<U>,
{
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = Result<U, E>>,
    {
        let mut returned_err = None;
        match self.0 {
            Ok(ref mut elems) => elems.extend(iter.into_iter().scan((), |_, item| match item {
                Ok(item) => Some(item),
                Err(err) => {
                    returned_err = Some(err);
                    None
                }
            })),
            Err(_) => (),
        }
        if let Some(err) = returned_err {
            self.0 = Err(err);
        }
    }
}

parser!{
    type PartialState = AnySendPartialState;
    fn value['a, I]()(I) -> RedisResult<Value>
        where [I: RangeStream<Item = u8, Range = &'a [u8]> ]
    {
        let end_of_line: fn () -> _ = || crlf().or(newline());
        let line = || recognize(take_until_range(&b"\r\n"[..]).with(end_of_line()))
            .and_then(|line: &[u8]| {
                str::from_utf8(line)
                    .map(|line| line.trim_right_matches(|c: char| c == '\r' || c == '\n'))
                    .map_err(StreamErrorFor::<I>::other)
            });

        let status = || line().map(|line| {
            if line == "OK" {
                Value::Okay
            } else {
                Value::Status(line.into())
            }
        });

        let int = || line().and_then(|line| {
            match line.trim().parse::<i64>() {
                Err(_) => Err(StreamErrorFor::<I>::message_static_message("Expected integer, got garbage")),
                Ok(value) => Ok(value),
            }
        });

        let data = || int().then_partial(move |size| {
            if *size < 0 {
                combine::value(Value::Nil).left()
            } else {
                take(*size as usize)
                    .map(|bs: &[u8]| Value::Data(bs.to_vec()))
                    .skip(end_of_line())
                    .right()
            }
        });

        let bulk = || {
            int().then_partial(|&mut length| {
                if length < 0 {
                    combine::value(Value::Nil).map(Ok).left()
                } else {
                    let length = length as usize;
                    combine::count_min_max(length, length, value()).map(|result: ResultExtend<_, _>| {
                        result.0.map(Value::Bulk)
                    }).right()
                }
            })
        };

        let error = || {
            line()
                .map(|line: &str| {
                    let desc = "An error was signalled by the server";
                    let mut pieces = line.splitn(2, ' ');
                    let kind = match pieces.next().unwrap() {
                        "ERR" => ErrorKind::ResponseError,
                        "EXECABORT" => ErrorKind::ExecAbortError,
                        "LOADING" => ErrorKind::BusyLoadingError,
                        "NOSCRIPT" => ErrorKind::NoScriptError,
                        code => {
                            return make_extension_error(code, pieces.next())
                        }
                    };
                    match pieces.next() {
                        Some(detail) => RedisError::from((kind, desc, detail.to_string())),
                        None => RedisError::from((kind, desc)),
                    }
                })
        };

        any_send_partial_state(choice((
           byte(b'+').with(status().map(Ok)),
           byte(b':').with(int().map(Value::Int).map(Ok)),
           byte(b'$').with(data().map(Ok)),
           byte(b'*').with(bulk()),
           byte(b'-').with(error().map(Err))
        )))
    }
}

pub struct ValueFuture<R> {
    reader: Option<R>,
    state: AnySendPartialState,
    // Intermediate storage for data we know that we need to parse a value but we haven't been able
    // to parse completely yet
    remaining: Vec<u8>,
}

impl<R> Future for ValueFuture<R>
where
    R: BufRead,
{
    type Item = (R, Value);
    type Error = RedisError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            assert!(
                self.reader.is_some(),
                "ValueFuture: poll called on completed future"
            );
            let remaining_data = self.remaining.len();

            let (opt, mut removed) = {
                let buffer = try_nb!(self.reader.as_mut().unwrap().fill_buf());
                if buffer.len() == 0 {
                    fail!((ErrorKind::ResponseError, "Could not read enough bytes"))
                }
                let buffer = if !self.remaining.is_empty() {
                    self.remaining.extend(buffer);
                    &self.remaining[..]
                } else {
                    buffer
                };
                let stream = combine::easy::Stream(combine::stream::PartialStream(buffer));
                match combine::stream::decode(value(), stream, &mut self.state) {
                    Ok(x) => x,
                    Err(err) => {
                        let err = err.map_position(|pos| pos.translate_position(buffer))
                            .map_range(|range| format!("{:?}", range))
                            .to_string();
                        return Err(RedisError::from((
                            ErrorKind::ResponseError,
                            "parse error",
                            err,
                        )));
                    }
                }
            };

            if !self.remaining.is_empty() {
                // Remove the data we have parsed and adjust `removed` to be the amount of data we
                // consumed from `self.reader`
                self.remaining.drain(..removed);
                if removed >= remaining_data {
                    removed = removed - remaining_data;
                } else {
                    removed = 0;
                }
            }

            match opt {
                Some(value) => {
                    self.reader.as_mut().unwrap().consume(removed);
                    let reader = self.reader.take().unwrap();
                    return Ok(Async::Ready((reader, value?)));
                }
                None => {
                    // We have not enough data to produce a Value but we know that all the data of
                    // the current buffer are necessary. Consume all the buffered data to ensure
                    // that the next iteration actually reads more data.
                    let buffer_len = {
                        let buffer = try!(self.reader.as_mut().unwrap().fill_buf());
                        if remaining_data == 0 {
                            self.remaining.extend(&buffer[removed..]);
                        }
                        buffer.len()
                    };
                    self.reader.as_mut().unwrap().consume(buffer_len);
                }
            }
        }
    }
}

pub fn parse_async<R>(reader: R) -> ValueFuture<R>
where
    R: AsyncRead + BufRead,
{
    ValueFuture {
        reader: Some(reader),
        state: Default::default(),
        remaining: Vec::new(),
    }
}

/// The internal redis response parser.
pub struct Parser<T> {
    reader: T,
}

/// The parser can be used to parse redis responses into values.  Generally
/// you normally do not use this directly as it's already done for you by
/// the client but in some more complex situations it might be useful to be
/// able to parse the redis responses.
impl<'a, T: BufRead> Parser<T> {
    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub fn new(reader: T) -> Parser<T> {
        Parser { reader: reader }
    }

    // public api

    pub fn parse_value(&mut self) -> RedisResult<Value> {
        let mut parser = ValueFuture {
            reader: Some(&mut self.reader),
            state: Default::default(),
            remaining: Vec::new(),
        };
        match parser.poll()? {
            Async::NotReady => Err(io::Error::from(io::ErrorKind::WouldBlock).into()),
            Async::Ready((_, value)) => Ok(value),
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
