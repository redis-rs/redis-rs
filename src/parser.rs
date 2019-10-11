use std::io::{self, BufRead, Read};
use std::marker::Unpin;
use std::pin::Pin;
use std::str;

use crate::types::{make_extension_error, ErrorKind, RedisError, RedisResult, Value};

use pin_project::pin_project;

use bytes::BytesMut;
use combine::{combine_parse_partial, combine_parser_impl, parse_mode, parser};
use futures::{prelude::*, ready, task, Poll};
use tokio::codec::{Decoder, Encoder};
use tokio::io::{AsyncBufRead, AsyncRead};

use combine;
use combine::byte::{byte, crlf, take_until_bytes};
use combine::combinator::{any_send_partial_state, AnySendPartialState};
#[allow(unused_imports)] // See https://github.com/rust-lang/rust/issues/43970
use combine::error::StreamError;
use combine::parser::choice::choice;
use combine::range::{recognize, take};
use combine::stream::{FullRangeStream, StreamErrorFor};

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
        if let Ok(ref mut elems) = self.0 {
            elems.extend(iter.into_iter().scan((), |_, item| match item {
                Ok(item) => Some(item),
                Err(err) => {
                    returned_err = Some(err);
                    None
                }
            }));
        }
        if let Some(err) = returned_err {
            self.0 = Err(err);
        }
    }
}

parser! {
    type PartialState = AnySendPartialState;
    fn value['a, I]()(I) -> RedisResult<Value>
        where [I: FullRangeStream<Item = u8, Range = &'a [u8]> ]
    {
        let line = || recognize(take_until_bytes(&b"\r\n"[..]).with(take(2).map(|_| ())))
            .and_then(|line: &[u8]| {
                str::from_utf8(&line[..line.len() - 2])
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
                    .skip(crlf())
                    .right()
            }
        });

        let bulk = || {
            int().then_partial(|&mut length| {
                if length < 0 {
                    combine::value(Value::Nil).map(Ok).left()
                } else {
                    let length = length as usize;
                    combine::count_min_max(length, length, value())
                        .map(|result: ResultExtend<_, _>| {
                            result.0.map(Value::Bulk)
                        })
                        .right()
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

#[derive(Default)]
pub struct ValueCodec {
    state: AnySendPartialState,
}

impl Encoder for ValueCodec {
    type Item = Vec<u8>;
    type Error = RedisError;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(item.as_ref());
        Ok(())
    }
}

impl Decoder for ValueCodec {
    type Item = Value;
    type Error = RedisError;
    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let (opt, removed_len) = {
            let buffer = &bytes[..];
            let stream = combine::easy::Stream(combine::stream::PartialStream(buffer));
            match combine::stream::decode(value(), stream, &mut self.state) {
                Ok(x) => x,
                Err(err) => {
                    let err = err
                        .map_position(|pos| pos.translate_position(buffer))
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

        bytes.split_to(removed_len);
        match opt {
            Some(result) => Ok(Some(result?)),
            None => Ok(None),
        }
    }
}

#[pin_project]
pub struct ValueFuture<R> {
    #[pin]
    reader: Option<R>,
    state: AnySendPartialState,
    // Intermediate storage for data we know that we need to parse a value but we haven't been able
    // to parse completely yet
    remaining: Vec<u8>,
}

impl<R> ValueFuture<R> {
    fn reader(&mut self) -> Pin<&mut R>
    where
        R: Unpin,
    {
        Pin::new(self.reader.as_mut().unwrap())
    }
}

impl<R> Future for ValueFuture<R>
where
    R: AsyncBufRead + Unpin,
{
    type Output = RedisResult<Value>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
        loop {
            assert!(
                self.reader.is_some(),
                "ValueFuture: poll called on completed future"
            );
            let remaining_data = self.remaining.len();

            let (opt, mut removed) = {
                let self_ = &mut *self;
                let buffer = match ready!(Pin::new(self_.reader.as_mut().unwrap()).poll_fill_buf(cx))
                {
                    Ok(buffer) => buffer,
                    Err(err) => return Err(err.into()).into(),
                };
                if buffer.len() == 0 {
                    return Err((ErrorKind::ResponseError, "Could not read enough bytes").into())
                        .into();
                }
                let buffer = if !self_.remaining.is_empty() {
                    self_.remaining.extend(buffer);
                    &self_.remaining[..]
                } else {
                    buffer
                };
                let stream = combine::easy::Stream(combine::stream::PartialStream(buffer));
                match combine::stream::decode(value(), stream, &mut self_.state) {
                    Ok(x) => x,
                    Err(err) => {
                        let err = err
                            .map_position(|pos| pos.translate_position(buffer))
                            .map_range(|range| format!("{:?}", range))
                            .to_string();
                        return Err(RedisError::from((
                            ErrorKind::ResponseError,
                            "parse error",
                            err,
                        )))
                        .into();
                    }
                }
            };

            if !self.remaining.is_empty() {
                // Remove the data we have parsed and adjust `removed` to be the amount of data we
                // consumed from `self.reader`
                self.remaining.drain(..removed);
                if removed >= remaining_data {
                    removed -= remaining_data;
                } else {
                    removed = 0;
                }
            }

            match opt {
                Some(value) => {
                    self.reader().consume(removed);
                    self.reader.take().unwrap();
                    return Ok(value?).into();
                }
                None => {
                    // We have not enough data to produce a Value but we know that all the data of
                    // the current buffer are necessary. Consume all the buffered data to ensure
                    // that the next iteration actually reads more data.
                    let buffer_len = {
                        let self_ = &mut *self;
                        let buffer =
                            ready!(Pin::new(self_.reader.as_mut().unwrap()).poll_fill_buf(cx))?;
                        if remaining_data == 0 {
                            self_.remaining.extend(&buffer[removed..]);
                        }
                        buffer.len()
                    };
                    self.reader().consume(buffer_len);
                }
            }
        }
    }
}

/// Parses a redis value asynchronously.
pub fn parse_redis_value_async<R>(reader: R) -> impl Future<Output = RedisResult<Value>>
where
    R: AsyncRead + AsyncBufRead + Unpin,
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

struct BlockingWrapper<R>(R);

impl<T> AsyncRead for BlockingWrapper<T>
where
    T: Read + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut task::Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.0.read(buf).into()
    }
}

impl<T> AsyncBufRead for BlockingWrapper<T>
where
    T: BufRead + Unpin,
{
    fn poll_fill_buf(self: Pin<&mut Self>, _cx: &mut task::Context) -> Poll<io::Result<&[u8]>> {
        self.get_mut().0.fill_buf().into()
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.0.consume(amt)
    }
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
        Parser { reader }
    }

    // public api

    /// Parses synchronously into a single value from the reader.
    pub fn parse_value(&mut self) -> RedisResult<Value> {
        let parser = ValueFuture {
            reader: Some(BlockingWrapper(&mut self.reader)),
            state: Default::default(),
            remaining: Vec::new(),
        };
        futures::executor::block_on(parser)
    }
}

/// Parses bytes into a redis value.
///
/// This is the most straightforward way to parse something into a low
/// level redis value instead of having to use a whole parser.
pub fn parse_redis_value(bytes: &[u8]) -> RedisResult<Value> {
    let mut parser = Parser::new(bytes);
    parser.parse_value()
}
