use std::{
    io::{self, Read},
    str,
};

use crate::types::{make_extension_error, ErrorKind, RedisError, RedisResult, Value};

use combine::{
    any,
    error::StreamError,
    opaque,
    parser::{
        byte::{crlf, take_until_bytes},
        combinator::{any_send_sync_partial_state, AnySendSyncPartialState},
        range::{recognize, take},
    },
    stream::{PointerOffset, RangeStream, StreamErrorFor},
    ParseError, Parser as _,
};

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

fn value<'a, I>(
) -> impl combine::Parser<I, Output = RedisResult<Value>, PartialState = AnySendSyncPartialState>
where
    I: RangeStream<Token = u8, Range = &'a [u8]>,
    I::Error: combine::ParseError<u8, &'a [u8], I::Position>,
{
    opaque!(any_send_sync_partial_state(any().then_partial(
        move |&mut b| {
            let line = || {
                recognize(take_until_bytes(&b"\r\n"[..]).with(take(2).map(|_| ()))).and_then(
                    |line: &[u8]| {
                        str::from_utf8(&line[..line.len() - 2]).map_err(StreamErrorFor::<I>::other)
                    },
                )
            };

            let status = || {
                line().map(|line| {
                    if line == "OK" {
                        Value::Okay
                    } else {
                        Value::Status(line.into())
                    }
                })
            };

            let int = || {
                line().and_then(|line| match line.trim().parse::<i64>() {
                    Err(_) => Err(StreamErrorFor::<I>::message_static_message(
                        "Expected integer, got garbage",
                    )),
                    Ok(value) => Ok(value),
                })
            };

            let data = || {
                int().then_partial(move |size| {
                    if *size < 0 {
                        combine::value(Value::Nil).left()
                    } else {
                        take(*size as usize)
                            .map(|bs: &[u8]| Value::Data(bs.to_vec()))
                            .skip(crlf())
                            .right()
                    }
                })
            };

            let bulk = || {
                int().then_partial(|&mut length| {
                    if length < 0 {
                        combine::value(Value::Nil).map(Ok).left()
                    } else {
                        let length = length as usize;
                        combine::count_min_max(length, length, value())
                            .map(|result: ResultExtend<_, _>| result.0.map(Value::Bulk))
                            .right()
                    }
                })
            };

            let error = || {
                line().map(|line: &str| {
                    let desc = "An error was signalled by the server";
                    let mut pieces = line.splitn(2, ' ');
                    let kind = match pieces.next().unwrap() {
                        "ERR" => ErrorKind::ResponseError,
                        "EXECABORT" => ErrorKind::ExecAbortError,
                        "LOADING" => ErrorKind::BusyLoadingError,
                        "NOSCRIPT" => ErrorKind::NoScriptError,
                        "MOVED" => ErrorKind::Moved,
                        "ASK" => ErrorKind::Ask,
                        "TRYAGAIN" => ErrorKind::TryAgain,
                        "CLUSTERDOWN" => ErrorKind::ClusterDown,
                        "CROSSSLOT" => ErrorKind::CrossSlot,
                        "MASTERDOWN" => ErrorKind::MasterDown,
                        "READONLY" => ErrorKind::ReadOnly,
                        code => return make_extension_error(code, pieces.next()),
                    };
                    match pieces.next() {
                        Some(detail) => RedisError::from((kind, desc, detail.to_string())),
                        None => RedisError::from((kind, desc)),
                    }
                })
            };

            combine::dispatch!(b;
                b'+' => status().map(Ok),
                b':' => int().map(|i| Ok(Value::Int(i))),
                b'$' => data().map(Ok),
                b'*' => bulk(),
                b'-' => error().map(Err),
                b => combine::unexpected_any(combine::error::Token(b))
            )
        }
    )))
}

#[cfg(feature = "aio")]
mod aio_support {
    use super::*;

    use bytes::{Buf, BytesMut};
    use tokio::io::AsyncRead;
    use tokio_util::codec::{Decoder, Encoder};

    #[derive(Default)]
    pub struct ValueCodec {
        state: AnySendSyncPartialState,
    }

    impl ValueCodec {
        fn decode_stream(
            &mut self,
            bytes: &mut BytesMut,
            eof: bool,
        ) -> RedisResult<Option<RedisResult<Value>>> {
            let (opt, removed_len) = {
                let buffer = &bytes[..];
                let mut stream =
                    combine::easy::Stream(combine::stream::MaybePartialStream(buffer, !eof));
                match combine::stream::decode_tokio(value(), &mut stream, &mut self.state) {
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

            bytes.advance(removed_len);
            match opt {
                Some(result) => Ok(Some(result)),
                None => Ok(None),
            }
        }
    }

    impl Encoder<Vec<u8>> for ValueCodec {
        type Error = RedisError;
        fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Self::Error> {
            dst.extend_from_slice(item.as_ref());
            Ok(())
        }
    }

    impl Decoder for ValueCodec {
        type Item = RedisResult<Value>;
        type Error = RedisError;

        fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            self.decode_stream(bytes, false)
        }

        fn decode_eof(&mut self, bytes: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            self.decode_stream(bytes, true)
        }
    }

    /// Parses a redis value asynchronously.
    pub async fn parse_redis_value_async<R>(
        decoder: &mut combine::stream::Decoder<AnySendSyncPartialState, PointerOffset<[u8]>>,
        read: &mut R,
    ) -> RedisResult<Value>
    where
        R: AsyncRead + std::marker::Unpin,
    {
        let result = combine::decode_tokio!(*decoder, *read, value(), |input, _| {
            combine::stream::easy::Stream::from(input)
        });
        match result {
            Err(err) => Err(match err {
                combine::stream::decoder::Error::Io { error, .. } => error.into(),
                combine::stream::decoder::Error::Parse(err) => {
                    if err.is_unexpected_end_of_input() {
                        RedisError::from(io::Error::from(io::ErrorKind::UnexpectedEof))
                    } else {
                        let err = err
                            .map_range(|range| format!("{:?}", range))
                            .map_position(|pos| pos.translate_position(decoder.buffer()))
                            .to_string();
                        RedisError::from((ErrorKind::ResponseError, "parse error", err))
                    }
                }
            }),
            Ok(result) => result,
        }
    }
}

#[cfg(feature = "aio")]
#[cfg_attr(docsrs, doc(cfg(feature = "aio")))]
pub use self::aio_support::*;

/// The internal redis response parser.
pub struct Parser {
    decoder: combine::stream::decoder::Decoder<AnySendSyncPartialState, PointerOffset<[u8]>>,
}

impl Default for Parser {
    fn default() -> Self {
        Parser::new()
    }
}

/// The parser can be used to parse redis responses into values.  Generally
/// you normally do not use this directly as it's already done for you by
/// the client but in some more complex situations it might be useful to be
/// able to parse the redis responses.
impl Parser {
    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub fn new() -> Parser {
        Parser {
            decoder: combine::stream::decoder::Decoder::new(),
        }
    }

    // public api

    /// Parses synchronously into a single value from the reader.
    pub fn parse_value<T: Read>(&mut self, mut reader: T) -> RedisResult<Value> {
        let mut decoder = &mut self.decoder;
        let result = combine::decode!(decoder, reader, value(), |input, _| {
            combine::stream::easy::Stream::from(input)
        });
        match result {
            Err(err) => Err(match err {
                combine::stream::decoder::Error::Io { error, .. } => error.into(),
                combine::stream::decoder::Error::Parse(err) => {
                    if err.is_unexpected_end_of_input() {
                        RedisError::from(io::Error::from(io::ErrorKind::UnexpectedEof))
                    } else {
                        let err = err
                            .map_range(|range| format!("{:?}", range))
                            .map_position(|pos| pos.translate_position(decoder.buffer()))
                            .to_string();
                        RedisError::from((ErrorKind::ResponseError, "parse error", err))
                    }
                }
            }),
            Ok(result) => result,
        }
    }
}

/// Parses bytes into a redis value.
///
/// This is the most straightforward way to parse something into a low
/// level redis value instead of having to use a whole parser.
pub fn parse_redis_value(bytes: &[u8]) -> RedisResult<Value> {
    let mut parser = Parser::new();
    parser.parse_value(bytes)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "aio")]
    use super::*;

    #[cfg(feature = "aio")]
    #[test]
    fn decode_eof_returns_none_at_eof() {
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec::default();

        let mut bytes = bytes::BytesMut::from(&b"+GET 123\r\n"[..]);
        assert_eq!(
            codec.decode_eof(&mut bytes),
            Ok(Some(Ok(parse_redis_value(b"+GET 123\r\n").unwrap())))
        );
        assert_eq!(codec.decode_eof(&mut bytes), Ok(None));
        assert_eq!(codec.decode_eof(&mut bytes), Ok(None));
    }
}
