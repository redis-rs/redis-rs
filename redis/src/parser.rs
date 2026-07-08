use std::{
    io::{self, Read},
    ops::Range,
    str,
};

use crate::errors::{ParsingError, RedisError, Repr, ServerError, ServerErrorKind};
use crate::types::{PushKind, RedisResult, Str, Value, VerbatimFormat};

use bytes::{Bytes, BytesMut};

use combine::{
    Parser as _, any,
    error::StreamError,
    opaque,
    parser::{
        byte::{crlf, take_until_bytes},
        combinator::{AnySendSyncPartialState, any_send_sync_partial_state},
        range::{recognize, take},
    },
    stream::{RangeStream, StreamErrorFor},
    unexpected_any,
};

const MAX_RECURSE_DEPTH: usize = 100;

/// An intermediate, zero-copy representation of a parsed [`Value`].
///
/// The parser produces a `ValueRange` whose textual/binary leaves store
/// `Range<usize>` offsets into the response buffer rather than copies of the
/// data. Once a complete value has been parsed, [`materialize`] converts the
/// ranges into [`Value`] by cheaply slicing the (frozen) buffer `Bytes`.
enum ValueRange {
    Nil,
    Int(i64),
    BulkString(Range<usize>),
    Array(Vec<ValueRange>),
    SimpleString(Range<usize>),
    Okay,
    Map(Vec<(ValueRange, ValueRange)>),
    Attribute {
        data: Box<ValueRange>,
        attributes: Vec<(ValueRange, ValueRange)>,
    },
    Set(Vec<ValueRange>),
    Double(f64),
    Boolean(bool),
    VerbatimString {
        format: VerbatimFormatRange,
        text: Range<usize>,
    },
    #[cfg(feature = "num-bigint")]
    BigNumber(num_bigint::BigInt),
    #[cfg(not(feature = "num-bigint"))]
    BigNumber(Range<usize>),
    /// The parsed elements of a push message. The first element, if any, is the
    /// push kind; the rest is the data.
    Push(Vec<ValueRange>),
    ServerError(ServerErrorRange),
}

enum VerbatimFormatRange {
    Text,
    Markdown,
    Unknown(Range<usize>),
}

struct ServerErrorRange {
    /// `Some` for a known error kind, `None` for an extension error (in which
    /// case `code` holds the error code).
    kind: Option<ServerErrorKind>,
    code: Range<usize>,
    detail: Option<Range<usize>>,
}

/// Computes the offset range of `slice` within the buffer that starts at `base`.
///
/// `slice` must be a sub-slice of the buffer whose start address is `base`.
#[inline]
fn range_of(slice: &[u8], base: usize) -> Range<usize> {
    let start = slice.as_ptr() as usize - base;
    start..start + slice.len()
}

fn err_parser(line: &str, base: usize) -> ServerErrorRange {
    let mut pieces = line.splitn(2, ' ');
    let code = pieces.next().unwrap();
    let kind = match code {
        "ERR" => Some(ServerErrorKind::ResponseError),
        "EXECABORT" => Some(ServerErrorKind::ExecAbort),
        "LOADING" => Some(ServerErrorKind::BusyLoading),
        "NOSCRIPT" => Some(ServerErrorKind::NoScript),
        "MOVED" => Some(ServerErrorKind::Moved),
        "ASK" => Some(ServerErrorKind::Ask),
        "TRYAGAIN" => Some(ServerErrorKind::TryAgain),
        "CLUSTERDOWN" => Some(ServerErrorKind::ClusterDown),
        "CROSSSLOT" => Some(ServerErrorKind::CrossSlot),
        "MASTERDOWN" => Some(ServerErrorKind::MasterDown),
        "READONLY" => Some(ServerErrorKind::ReadOnly),
        "NOTBUSY" => Some(ServerErrorKind::NotBusy),
        "NOSUB" => Some(ServerErrorKind::NoSub),
        "NOPERM" => Some(ServerErrorKind::NoPerm),
        _ => None,
    };
    ServerErrorRange {
        kind,
        code: range_of(code.as_bytes(), base),
        detail: pieces
            .next()
            .map(|detail| range_of(detail.as_bytes(), base)),
    }
}

// ------------------------------------------------------------------
// Materialization: `ValueRange` + the response buffer -> `Value`.
// ------------------------------------------------------------------

/// Slices `frame` at `range`, wrapping it as a [`Str`] without re-validating.
///
/// All ranges that reach this helper were validated as UTF-8 during parsing.
#[inline]
fn slice_str(frame: &Bytes, range: Range<usize>) -> Str {
    // SAFETY: the parser only produces `Str` ranges from input that has already
    // been validated as UTF-8.
    unsafe { Str::from_utf8_unchecked(frame.slice(range)) }
}

fn materialize(range: ValueRange, frame: &Bytes) -> Result<Value, ParsingError> {
    Ok(match range {
        ValueRange::Nil => Value::Nil,
        ValueRange::Int(val) => Value::Int(val),
        ValueRange::BulkString(range) => Value::BulkString(frame.slice(range)),
        ValueRange::Array(items) => Value::Array(materialize_vec(items, frame)?),
        ValueRange::SimpleString(range) => Value::SimpleString(slice_str(frame, range)),
        ValueRange::Okay => Value::Okay,
        ValueRange::Map(pairs) => Value::Map(materialize_pairs(pairs, frame)?),
        ValueRange::Attribute { data, attributes } => Value::Attribute {
            data: Box::new(materialize(*data, frame)?),
            attributes: materialize_pairs(attributes, frame)?,
        },
        ValueRange::Set(items) => Value::Set(materialize_vec(items, frame)?),
        ValueRange::Double(val) => Value::Double(val),
        ValueRange::Boolean(val) => Value::Boolean(val),
        ValueRange::VerbatimString { format, text } => Value::VerbatimString {
            format: materialize_format(format, frame),
            text: slice_str(frame, text),
        },
        #[cfg(feature = "num-bigint")]
        ValueRange::BigNumber(num) => Value::BigNumber(num),
        #[cfg(not(feature = "num-bigint"))]
        ValueRange::BigNumber(range) => Value::BigNumber(frame.slice(range)),
        ValueRange::Push(items) => materialize_push(items, frame)?,
        ValueRange::ServerError(err) => Value::ServerError(materialize_error(err, frame)),
    })
}

fn materialize_vec(items: Vec<ValueRange>, frame: &Bytes) -> Result<Vec<Value>, ParsingError> {
    items
        .into_iter()
        .map(|item| materialize(item, frame))
        .collect()
}

fn materialize_pairs(
    pairs: Vec<(ValueRange, ValueRange)>,
    frame: &Bytes,
) -> Result<Vec<(Value, Value)>, ParsingError> {
    pairs
        .into_iter()
        .map(|(k, v)| Ok((materialize(k, frame)?, materialize(v, frame)?)))
        .collect()
}

fn materialize_format(format: VerbatimFormatRange, frame: &Bytes) -> VerbatimFormat {
    match format {
        VerbatimFormatRange::Text => VerbatimFormat::Text,
        VerbatimFormatRange::Markdown => VerbatimFormat::Markdown,
        VerbatimFormatRange::Unknown(range) => VerbatimFormat::Unknown(slice_str(frame, range)),
    }
}

/// Copies `frame[range]` out into a detached [`Str`].
///
/// Used for error payloads instead of [`slice_str`]: errors are rare, small,
/// and often outlive the response they arrived in (users store them, retry
/// loops hold them, cluster redirects carry them around). Slicing would pin
/// the whole response frame for as long as the error lives — e.g. one error
/// among thousands of pipelined results would retain the entire reply buffer.
/// Copying a few bytes here keeps the hot data path zero-copy while giving
/// errors an independent lifetime.
#[inline]
fn detached_str(frame: &Bytes, range: Range<usize>) -> Str {
    Str::from(slice_str(frame, range).as_str())
}

fn materialize_error(err: ServerErrorRange, frame: &Bytes) -> ServerError {
    let detail = err.detail.map(|range| detached_str(frame, range));
    match err.kind {
        Some(kind) => ServerError(Repr::Known { kind, detail }),
        None => ServerError(Repr::Extension {
            code: detached_str(frame, err.code),
            detail,
        }),
    }
}

fn materialize_push(items: Vec<ValueRange>, frame: &Bytes) -> Result<Value, ParsingError> {
    let mut it = items.into_iter();
    let Some(first) = it.next() else {
        return Ok(Value::Push {
            kind: PushKind::Other(Str::from_static("")),
            data: vec![],
        });
    };
    let kind = match first {
        ValueRange::BulkString(range) | ValueRange::SimpleString(range) => {
            get_push_kind(frame.slice(range))?
        }
        _ => return Err(ParsingError::from("parse error when decoding push")),
    };
    let data = it
        .map(|item| materialize(item, frame))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Push { kind, data })
}

fn get_push_kind(name: Bytes) -> Result<PushKind, ParsingError> {
    let name =
        Str::from_utf8(name).map_err(|_| ParsingError::from("parse error when decoding push"))?;
    let known = match name.as_str() {
        "invalidate" => Some(PushKind::Invalidate),
        "message" => Some(PushKind::Message),
        "pmessage" => Some(PushKind::PMessage),
        "smessage" => Some(PushKind::SMessage),
        "unsubscribe" => Some(PushKind::Unsubscribe),
        "punsubscribe" => Some(PushKind::PUnsubscribe),
        "sunsubscribe" => Some(PushKind::SUnsubscribe),
        "subscribe" => Some(PushKind::Subscribe),
        "psubscribe" => Some(PushKind::PSubscribe),
        "ssubscribe" => Some(PushKind::SSubscribe),
        _ => None,
    };
    Ok(known.unwrap_or_else(|| PushKind::Other(name)))
}

// ------------------------------------------------------------------
// The combine-based parser. Produces `ValueRange` (offsets), not `Value`.
// ------------------------------------------------------------------

fn value<'a, I>(
    base: usize,
    count: Option<usize>,
) -> impl combine::Parser<I, Output = ValueRange, PartialState = AnySendSyncPartialState>
where
    I: RangeStream<Token = u8, Range = &'a [u8]>,
    I::Error: combine::ParseError<u8, &'a [u8], I::Position>,
{
    let count = count.unwrap_or(1);

    opaque!(any_send_sync_partial_state(
        any()
            .then_partial(move |&mut b| {
                if count > MAX_RECURSE_DEPTH {
                    combine::unexpected_any("Maximum recursion depth exceeded").left()
                } else {
                    combine::value(b).right()
                }
            })
            .then_partial(move |&mut b| {
                let line = || {
                    recognize(take_until_bytes(&b"\r\n"[..]).with(take(2).map(|_| ()))).and_then(
                        |line: &[u8]| {
                            str::from_utf8(&line[..line.len() - 2])
                                .map_err(StreamErrorFor::<I>::other)
                        },
                    )
                };

                let simple_string = || {
                    line().map(move |line: &str| {
                        if line == "OK" {
                            ValueRange::Okay
                        } else {
                            ValueRange::SimpleString(range_of(line.as_bytes(), base))
                        }
                    })
                };

                let int = || {
                    line().and_then(|line| {
                        line.trim().parse::<i64>().map_err(|_| {
                            StreamErrorFor::<I>::message_static_message(
                                "Expected integer, got garbage",
                            )
                        })
                    })
                };

                let bulk_string = || {
                    int().then_partial(move |size| {
                        if *size < 0 {
                            combine::produce(|| ValueRange::Nil).left()
                        } else {
                            take(*size as usize)
                                .map(move |bs: &[u8]| ValueRange::BulkString(range_of(bs, base)))
                                .skip(crlf())
                                .right()
                        }
                    })
                };
                let array = || {
                    int().then_partial(move |&mut length| {
                        if length < 0 {
                            combine::produce(|| ValueRange::Nil).left()
                        } else {
                            let length = length as usize;
                            combine::count_min_max(length, length, value(base, Some(count + 1)))
                                .map(ValueRange::Array)
                                .right()
                        }
                    })
                };

                let error = || line().map(move |line: &str| err_parser(line, base));
                let map = || {
                    int().then_partial(move |&mut kv_length| {
                        match (kv_length as usize).checked_mul(2) {
                            Some(length) => {
                                combine::count_min_max(length, length, value(base, Some(count + 1)))
                                    .map(move |result: Vec<ValueRange>| {
                                        let mut it = result.into_iter();
                                        let mut x = Vec::with_capacity(kv_length as usize);
                                        for _ in 0..kv_length {
                                            if let (Some(k), Some(v)) = (it.next(), it.next()) {
                                                x.push((k, v))
                                            }
                                        }
                                        ValueRange::Map(x)
                                    })
                                    .left()
                            }
                            None => {
                                unexpected_any("Attribute key-value length is too large").right()
                            }
                        }
                    })
                };
                let attribute = || {
                    int().then_partial(move |&mut kv_length| {
                        match (kv_length as usize).checked_mul(2) {
                            Some(length) => {
                                // + 1 is for data!
                                let length = length + 1;
                                combine::count_min_max(length, length, value(base, Some(count + 1)))
                                    .map(move |result: Vec<ValueRange>| {
                                        let mut it = result.into_iter();
                                        let mut attributes = Vec::with_capacity(kv_length as usize);
                                        for _ in 0..kv_length {
                                            if let (Some(k), Some(v)) = (it.next(), it.next()) {
                                                attributes.push((k, v))
                                            }
                                        }
                                        ValueRange::Attribute {
                                            data: Box::new(it.next().unwrap()),
                                            attributes,
                                        }
                                    })
                                    .left()
                            }
                            None => {
                                unexpected_any("Attribute key-value length is too large").right()
                            }
                        }
                    })
                };
                let set = || {
                    int().then_partial(move |&mut length| {
                        if length < 0 {
                            combine::produce(|| ValueRange::Nil).left()
                        } else {
                            let length = length as usize;
                            combine::count_min_max(length, length, value(base, Some(count + 1)))
                                .map(ValueRange::Set)
                                .right()
                        }
                    })
                };
                let push = || {
                    int().then_partial(move |&mut length| {
                        if length <= 0 {
                            combine::produce(|| ValueRange::Push(vec![])).left()
                        } else {
                            let length = length as usize;
                            combine::count_min_max(length, length, value(base, Some(count + 1)))
                                .map(ValueRange::Push)
                                .right()
                        }
                    })
                };
                let null = || line().map(|_| ValueRange::Nil);
                let double = || {
                    line().and_then(|line| {
                        line.trim()
                            .parse::<f64>()
                            .map_err(StreamErrorFor::<I>::other)
                    })
                };
                let boolean = || {
                    line().and_then(|line: &str| match line {
                        "t" => Ok(true),
                        "f" => Ok(false),
                        _ => Err(StreamErrorFor::<I>::message_static_message(
                            "Expected boolean, got garbage",
                        )),
                    })
                };
                let blob_error = || {
                    int().then_partial(move |&mut size| {
                        take(size as usize)
                            .and_then(move |bs: &[u8]| {
                                let line =
                                    str::from_utf8(bs).map_err(StreamErrorFor::<I>::other)?;
                                Ok::<_, StreamErrorFor<I>>(err_parser(line, base))
                            })
                            .skip(crlf())
                    })
                };
                let verbatim = || {
                    int().then_partial(move |&mut size| {
                        take(size as usize)
                            .and_then(move |bs: &[u8]| {
                                let line =
                                    str::from_utf8(bs).map_err(StreamErrorFor::<I>::other)?;
                                if let Some((format, text)) = line.split_once(':') {
                                    let format = match format {
                                        "txt" => VerbatimFormatRange::Text,
                                        "mkd" => VerbatimFormatRange::Markdown,
                                        x => VerbatimFormatRange::Unknown(range_of(
                                            x.as_bytes(),
                                            base,
                                        )),
                                    };
                                    Ok(ValueRange::VerbatimString {
                                        format,
                                        text: range_of(text.as_bytes(), base),
                                    })
                                } else {
                                    Err(StreamErrorFor::<I>::message_static_message(
                                        "parse error when decoding verbatim string",
                                    ))
                                }
                            })
                            .skip(crlf())
                    })
                };
                let big_number = || {
                    line().and_then(move |line| {
                        #[cfg(not(feature = "num-bigint"))]
                        return Ok::<_, StreamErrorFor<I>>(ValueRange::BigNumber(range_of(
                            line.as_bytes(),
                            base,
                        )));
                        #[cfg(feature = "num-bigint")]
                        num_bigint::BigInt::parse_bytes(line.as_bytes(), 10)
                            .ok_or_else(|| {
                                StreamErrorFor::<I>::message_static_message(
                                    "Expected bigint, got garbage",
                                )
                            })
                            .map(ValueRange::BigNumber)
                    })
                };
                combine::dispatch!(b;
                    b'+' => simple_string(),
                    b':' => int().map(ValueRange::Int),
                    b'$' => bulk_string(),
                    b'*' => array(),
                    b'%' => map(),
                    b'|' => attribute(),
                    b'~' => set(),
                    b'-' => error().map(ValueRange::ServerError),
                    b'_' => null(),
                    b',' => double().map(ValueRange::Double),
                    b'#' => boolean().map(ValueRange::Boolean),
                    b'!' => blob_error().map(ValueRange::ServerError),
                    b'=' => verbatim(),
                    b'(' => big_number(),
                    b'>' => push(),
                    b => combine::unexpected_any(combine::error::Token(b))
                )
            })
    ))
}

/// Attempts to parse a single value from `buffer`.
///
/// Parsing always starts from the beginning of `buffer` with fresh state, so
/// the offsets recorded in the returned [`ValueRange`] are valid relative to
/// the start of `buffer`. Returns `Ok(None)` if `buffer` does not yet contain a
/// complete value (and `complete` is `false`, i.e. more data may still arrive).
fn decode_value_range(buffer: &[u8], complete: bool) -> RedisResult<Option<(ValueRange, usize)>> {
    let base = buffer.as_ptr() as usize;
    let mut state = AnySendSyncPartialState::default();
    let mut stream = combine::easy::Stream(combine::stream::MaybePartialStream(buffer, !complete));
    match combine::stream::decode_tokio(value(base, None), &mut stream, &mut state) {
        Ok((opt, removed_len)) => Ok(opt.map(|value| (value, removed_len))),
        Err(err) => {
            let err = err
                .map_position(|pos| pos.translate_position(buffer))
                .map_range(|range| format!("{range:?}"))
                .to_string();
            Err(RedisError::from(ParsingError::from(err)))
        }
    }
}

/// Parses a single value from `buffer`, consuming the bytes that make it up.
///
/// On success the consumed prefix is frozen into a [`Bytes`] and the value's
/// leaves are produced as cheap, reference-counted slices into it; the
/// remaining bytes are left in `buffer` for the next value.
fn parse_buffer(buffer: &mut BytesMut, complete: bool) -> RedisResult<Option<Value>> {
    let Some((range, consumed)) = decode_value_range(&buffer[..], complete)? else {
        return Ok(None);
    };
    let frame = buffer.split_to(consumed).freeze();
    Ok(Some(materialize(range, &frame)?))
}

#[cfg(feature = "aio")]
mod aio_support {
    use super::*;

    use tokio::io::{AsyncRead, AsyncReadExt};
    use tokio_util::codec::{Decoder, Encoder};

    #[derive(Default)]
    pub struct ValueCodec;

    impl Encoder<Vec<u8>> for ValueCodec {
        type Error = RedisError;
        fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<(), Self::Error> {
            dst.extend_from_slice(item.as_ref());
            Ok(())
        }
    }

    impl Decoder for ValueCodec {
        type Item = Value;
        type Error = RedisError;

        fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            parse_buffer(bytes, false)
        }

        fn decode_eof(&mut self, bytes: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            parse_buffer(bytes, true)
        }
    }

    /// Parses a redis value asynchronously.
    ///
    /// `buffer` accumulates data read from `read` and retains any bytes that
    /// follow a parsed value, so the same buffer should be reused across calls
    /// on the same stream.
    pub async fn parse_redis_value_async<R>(
        buffer: &mut BytesMut,
        read: &mut R,
    ) -> RedisResult<Value>
    where
        R: AsyncRead + std::marker::Unpin,
    {
        loop {
            if let Some(value) = parse_buffer(buffer, false)? {
                return Ok(value);
            }
            // Keep a sane minimum read granularity: `read_buf` only fills the
            // buffer's spare capacity, which after a `split_to`/`freeze` can be
            // arbitrarily small (the frozen prefix may still share the backing
            // allocation). Without this, reads can degrade to tiny chunks.
            buffer.reserve(8 * 1024);
            if read.read_buf(buffer).await? == 0 {
                return match parse_buffer(buffer, true)? {
                    Some(value) => Ok(value),
                    None => Err(RedisError::from(io::Error::from(
                        io::ErrorKind::UnexpectedEof,
                    ))),
                };
            }
        }
    }
}

#[cfg(feature = "aio")]
#[cfg_attr(docsrs, doc(cfg(feature = "aio")))]
pub use self::aio_support::*;

/// The internal redis response parser.
pub struct Parser {
    buffer: BytesMut,
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
            buffer: BytesMut::new(),
        }
    }

    // public api

    /// Parses synchronously into a single value from the reader.
    pub fn parse_value<T: Read>(&mut self, mut reader: T) -> RedisResult<Value> {
        loop {
            if let Some(value) = parse_buffer(&mut self.buffer, false)? {
                return Ok(value);
            }
            let mut chunk = [0u8; 8 * 1024];
            let read = reader.read(&mut chunk)?;
            if read == 0 {
                return match parse_buffer(&mut self.buffer, true)? {
                    Some(value) => Ok(value),
                    None => Err(RedisError::from(io::Error::from(
                        io::ErrorKind::UnexpectedEof,
                    ))),
                };
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }
}

/// Parses bytes into a redis value.
///
/// This is the most straightforward way to parse something into a low
/// level redis value instead of having to use a whole parser.
pub fn parse_redis_value(bytes: &[u8]) -> RedisResult<Value> {
    let mut buffer = BytesMut::from(bytes);
    match parse_buffer(&mut buffer, true)? {
        Some(value) => Ok(value),
        None => Err(RedisError::from(io::Error::from(
            io::ErrorKind::UnexpectedEof,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::ErrorKind;
    use assert_matches::assert_matches;

    #[cfg(feature = "aio")]
    #[test]
    fn decode_eof_returns_none_at_eof() {
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec;

        let mut bytes = bytes::BytesMut::from(&b"+GET 123\r\n"[..]);
        assert_eq!(
            codec.decode_eof(&mut bytes),
            Ok(Some(parse_redis_value(b"+GET 123\r\n").unwrap()))
        );
        assert_eq!(codec.decode_eof(&mut bytes), Ok(None));
        assert_eq!(codec.decode_eof(&mut bytes), Ok(None));
    }

    #[cfg(feature = "aio")]
    #[test]
    fn decode_eof_returns_error_inside_array_and_can_parse_more_inputs() {
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec;

        let mut bytes =
            bytes::BytesMut::from(b"*3\r\n+OK\r\n-LOADING server is loading\r\n+OK\r\n".as_slice());
        let result = codec.decode_eof(&mut bytes).unwrap().unwrap();

        assert_eq!(
            result,
            Value::Array(vec![
                Value::Okay,
                Value::ServerError(ServerError(Repr::Known {
                    kind: ServerErrorKind::BusyLoading,
                    detail: Some(Str::from_static("server is loading"))
                })),
                Value::Okay
            ])
        );

        let mut bytes = bytes::BytesMut::from(b"+OK\r\n".as_slice());
        let result = codec.decode_eof(&mut bytes).unwrap().unwrap();

        assert_eq!(result, Value::Okay);
    }

    #[test]
    fn parse_nested_error_and_handle_more_inputs() {
        // from https://redis.io/docs/interact/transactions/ -
        // "EXEC returned two-element bulk string reply where one is an OK code and the other an error reply. It's up to the client library to find a sensible way to provide the error to the user."

        let bytes = b"*3\r\n+OK\r\n-LOADING server is loading\r\n+OK\r\n";
        let result = parse_redis_value(bytes);

        assert_eq!(
            result.unwrap(),
            Value::Array(vec![
                Value::Okay,
                Value::ServerError(ServerError(Repr::Known {
                    kind: ServerErrorKind::BusyLoading,
                    detail: Some(Str::from_static("server is loading"))
                })),
                Value::Okay
            ])
        );

        let result = parse_redis_value(b"+OK\r\n").unwrap();

        assert_eq!(result, Value::Okay);
    }

    #[test]
    fn decode_resp3_double() {
        let val = parse_redis_value(b",1.23\r\n").unwrap();
        assert_eq!(val, Value::Double(1.23));
        let val = parse_redis_value(b",nan\r\n").unwrap();
        if let Value::Double(val) = val {
            assert!(val.is_sign_positive());
            assert!(val.is_nan());
        } else {
            panic!("expected double");
        }
        // -nan is supported prior to redis 7.2
        let val = parse_redis_value(b",-nan\r\n").unwrap();
        if let Value::Double(val) = val {
            assert!(val.is_sign_negative());
            assert!(val.is_nan());
        } else {
            panic!("expected double");
        }
        //Allow doubles in scientific E notation
        let val = parse_redis_value(b",2.67923e+8\r\n").unwrap();
        assert_eq!(val, Value::Double(267923000.0));
        let val = parse_redis_value(b",2.67923E+8\r\n").unwrap();
        assert_eq!(val, Value::Double(267923000.0));
        let val = parse_redis_value(b",-2.67923E+8\r\n").unwrap();
        assert_eq!(val, Value::Double(-267923000.0));
        let val = parse_redis_value(b",2.1E-2\r\n").unwrap();
        assert_eq!(val, Value::Double(0.021));

        let val = parse_redis_value(b",-inf\r\n").unwrap();
        assert_eq!(val, Value::Double(-f64::INFINITY));
        let val = parse_redis_value(b",inf\r\n").unwrap();
        assert_eq!(val, Value::Double(f64::INFINITY));
    }

    #[test]
    fn decode_resp3_map() {
        let val = parse_redis_value(b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n").unwrap();
        let mut v = val.as_map_iter().unwrap();
        assert_eq!(
            (&Value::SimpleString("first".into()), &Value::Int(1)),
            v.next().unwrap()
        );
        assert_eq!(
            (&Value::SimpleString("second".into()), &Value::Int(2)),
            v.next().unwrap()
        );
    }

    #[test]
    fn decode_resp3_boolean() {
        let val = parse_redis_value(b"#t\r\n").unwrap();
        assert_eq!(val, Value::Boolean(true));
        let val = parse_redis_value(b"#f\r\n").unwrap();
        assert_eq!(val, Value::Boolean(false));
        let val = parse_redis_value(b"#x\r\n");
        assert_matches!(val, Err(_));
        let val = parse_redis_value(b"#\r\n");
        assert_matches!(val, Err(_));
    }

    #[test]
    fn decode_resp3_blob_error() {
        let val = parse_redis_value(b"!21\r\nSYNTAX invalid syntax\r\n");
        assert_eq!(
            val.unwrap(),
            Value::ServerError(ServerError(Repr::Extension {
                code: Str::from_static("SYNTAX"),
                detail: Some(Str::from_static("invalid syntax"))
            }))
        )
    }

    #[test]
    fn invalid_utf8_in_verbatim_and_blob_error_is_rejected() {
        // Both verbatim strings and blob errors are now decoded with strict
        // `str::from_utf8` (previously `from_utf8_lossy`), so a non-UTF-8 body
        // must surface as a parse error rather than silently gaining U+FFFD
        // replacement characters.
        assert_matches!(parse_redis_value(b"=5\r\ntxt:\xff\r\n"), Err(_));
        assert_matches!(parse_redis_value(b"!4\r\nER\xff\xff\r\n"), Err(_));
    }

    #[cfg(feature = "aio")]
    #[test]
    fn parsed_error_is_detached_from_reply_buffer() {
        // A parsed error must copy its code/detail out of the reply frame
        // rather than slice into it, so that holding on to a small error does
        // not pin the whole response buffer alive. We check this by asserting
        // the detail bytes live outside the original buffer's allocation.
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec;
        let mut buf = bytes::BytesMut::from(b"!21\r\nSYNTAX invalid syntax\r\n".as_slice());
        let base = buf.as_ptr() as usize;
        let cap = buf.capacity();
        let val = codec.decode(&mut buf).unwrap().unwrap();
        let Value::ServerError(err) = val else {
            panic!("expected ServerError, got {val:?}");
        };
        let detail = err.details().expect("detail present");
        let ptr = detail.as_ptr() as usize;
        assert!(
            ptr < base || ptr >= base + cap,
            "error detail is a slice into the reply buffer (frame-pinning regression)"
        );
    }

    #[test]
    fn decode_resp3_big_number() {
        let val = parse_redis_value(b"(3492890328409238509324850943850943825024385\r\n").unwrap();
        #[cfg(feature = "num-bigint")]
        let expected = Value::BigNumber(
            num_bigint::BigInt::parse_bytes(b"3492890328409238509324850943850943825024385", 10)
                .unwrap(),
        );
        #[cfg(not(feature = "num-bigint"))]
        let expected = Value::BigNumber(Bytes::from_static(
            b"3492890328409238509324850943850943825024385",
        ));
        assert_eq!(val, expected);
    }

    #[test]
    fn decode_resp3_set() {
        let val = parse_redis_value(b"~5\r\n+orange\r\n+apple\r\n#t\r\n:100\r\n:999\r\n").unwrap();
        let v = val.as_sequence().unwrap();
        assert_eq!(Value::SimpleString("orange".into()), v[0]);
        assert_eq!(Value::SimpleString("apple".into()), v[1]);
        assert_eq!(Value::Boolean(true), v[2]);
        assert_eq!(Value::Int(100), v[3]);
        assert_eq!(Value::Int(999), v[4]);
    }

    #[test]
    fn decode_resp3_push() {
        let val = parse_redis_value(b">3\r\n+message\r\n+somechannel\r\n+this is the message\r\n")
            .unwrap();
        if let Value::Push { ref kind, ref data } = val {
            assert_eq!(&PushKind::Message, kind);
            assert_eq!(Value::SimpleString("somechannel".into()), data[0]);
            assert_eq!(Value::SimpleString("this is the message".into()), data[1]);
        } else {
            panic!("Expected Value::Push")
        }
    }

    #[test]
    fn test_max_recursion_depth_set_and_array() {
        for test_byte in ["*", "~"] {
            let initial = format!("{test_byte}1\r\n").as_bytes().to_vec();
            let end = format!("{test_byte}0\r\n").as_bytes().to_vec();

            let mut ba = initial.repeat(MAX_RECURSE_DEPTH - 1).to_vec();
            ba.extend(end.clone());
            match parse_redis_value(&ba) {
                Ok(Value::Array(a)) => assert_eq!(a.len(), 1),
                Ok(Value::Set(s)) => assert_eq!(s.len(), 1),
                _ => panic!("Expected valid array or set"),
            }

            let mut ba = initial.repeat(MAX_RECURSE_DEPTH).to_vec();
            ba.extend(end);
            match parse_redis_value(&ba) {
                Ok(_) => panic!("Expected ParseError"),
                Err(e) => assert_matches!(e.kind(), ErrorKind::Parse),
            }
        }
    }

    #[test]
    fn test_max_recursion_depth_map() {
        let initial = b"%1\r\n+a\r\n";
        let end = b"%0\r\n";

        let mut ba = initial.repeat(MAX_RECURSE_DEPTH - 1).to_vec();
        ba.extend(*end);
        match parse_redis_value(&ba) {
            Ok(Value::Map(m)) => assert_eq!(m.len(), 1),
            Ok(Value::Set(s)) => assert_eq!(s.len(), 1),
            _ => panic!("Expected valid array or set"),
        }

        let mut ba = initial.repeat(MAX_RECURSE_DEPTH).to_vec();
        ba.extend(end);
        match parse_redis_value(&ba) {
            Ok(_) => panic!("Expected ParseError"),
            Err(e) => assert_matches!(e.kind(), ErrorKind::Parse),
        }
    }
}
