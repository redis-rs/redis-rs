use std::io::{self, BufRead, Read};
use std::marker::Unpin;
use std::pin::Pin;
use std::str;

use crate::types::{make_extension_error, ErrorKind, RedisError, RedisResult, Value};

use bytes::BytesMut;
use combine::{combine_parse_partial, combine_parser_impl, parse_mode, parser};
use futures::{future, task, Poll};
use tokio_codec::{Decoder, Encoder};
use tokio_io::{AsyncBufRead, AsyncRead};

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

// https://github.com/tokio-rs/tokio/pull/1687
async fn fill_buf<R>(reader: &mut R) -> io::Result<&[u8]>
where
    R: AsyncBufRead + Unpin,
{
    let mut reader = Some(reader);
    future::poll_fn(move |cx| match reader.take() {
        Some(r) => match Pin::new(&mut *r).poll_fill_buf(cx) {
            // SAFETY We either drop `self.reader` and return a slice with the lifetime of the
            // reader or we return Pending/Err (neither which contains `'a`).
            // In either case `poll_fill_buf` can not be called while it's contents are exposed
            Poll::Ready(Ok(x)) => unsafe { return Ok(&*(x as *const _)).into() },
            Poll::Ready(Err(err)) => Err(err).into(),
            Poll::Pending => {
                reader = Some(r);
                Poll::Pending
            }
        },
        None => panic!("fill_buf polled after completion"),
    })
    .await
}

/// Parses a redis value asynchronously.
pub async fn parse_redis_value_async<R>(mut reader: R) -> RedisResult<Value>
where
    R: AsyncBufRead + Unpin,
{
    let mut state = Default::default();
    let mut remaining = Vec::new();
    loop {
        let remaining_data = remaining.len();

        let (opt, mut removed) = {
            let buffer = fill_buf(&mut reader).await?;
            if buffer.len() == 0 {
                return Err((ErrorKind::ResponseError, "Could not read enough bytes").into());
            }
            let buffer = if !remaining.is_empty() {
                remaining.extend(buffer);
                &remaining[..]
            } else {
                buffer
            };

            let stream = combine::easy::Stream(combine::stream::PartialStream(&buffer[..]));
            match combine::stream::decode(value(), stream, &mut state) {
                Ok(x) => x,
                Err(err) => {
                    let err = err
                        .map_position(|pos| pos.translate_position(&buffer[..]))
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

        if !remaining.is_empty() {
            // Remove the data we have parsed and adjust `removed` to be the amount of data we
            // consumed from `self.reader`
            remaining.drain(..removed);
            if removed >= remaining_data {
                removed -= remaining_data;
            } else {
                removed = 0;
            }
        }

        match opt {
            Some(value) => {
                Pin::new(&mut reader).consume(removed);
                return Ok(value?);
            }
            None => {
                // We have not enough data to produce a Value but we know that all the data of
                // the current buffer are necessary. Consume all the buffered data to ensure
                // that the next iteration actually reads more data.
                let buffer_len = {
                    let buffer = fill_buf(&mut reader).await?;
                    if remaining_data == 0 {
                        remaining.extend(&buffer[removed..]);
                    }
                    buffer.len()
                };
                Pin::new(&mut reader).consume(buffer_len);
            }
        }
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
        let parser = parse_redis_value_async(BlockingWrapper(&mut self.reader));
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
