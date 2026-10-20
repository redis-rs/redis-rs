use std::{
    io::{self, Read},
    str,
};

use crate::errors::{ParsingError, RedisError, Repr, ServerError, ServerErrorKind};
use crate::types::{PushKind, RedisResult, Value, VerbatimFormat};

use combine::{
    ParseError, Parser as _, any,
    error::StreamError,
    opaque,
    parser::{
        byte::{crlf, take_until_bytes},
        combinator::{AnySendSyncPartialState, any_send_sync_partial_state},
        range::{recognize, take},
    },
    stream::{
        PointerOffset, RangeStream, StreamErrorFor,
        decoder::{self, Decoder},
    },
    unexpected_any,
};

const MAX_RECURSE_DEPTH: usize = 100;

fn err_parser(line: &str) -> ServerError {
    let mut pieces = line.splitn(2, ' ');
    let kind = match pieces.next().unwrap() {
        "ERR" => ServerErrorKind::ResponseError,
        "EXECABORT" => ServerErrorKind::ExecAbort,
        "LOADING" => ServerErrorKind::BusyLoading,
        "NOSCRIPT" => ServerErrorKind::NoScript,
        "MOVED" => ServerErrorKind::Moved,
        "ASK" => ServerErrorKind::Ask,
        "TRYAGAIN" => ServerErrorKind::TryAgain,
        "CLUSTERDOWN" => ServerErrorKind::ClusterDown,
        "CROSSSLOT" => ServerErrorKind::CrossSlot,
        "MASTERDOWN" => ServerErrorKind::MasterDown,
        "READONLY" => ServerErrorKind::ReadOnly,
        "NOTBUSY" => ServerErrorKind::NotBusy,
        "NOSUB" => ServerErrorKind::NoSub,
        "NOPERM" => ServerErrorKind::NoPerm,
        code => {
            return ServerError(Repr::Extension {
                code: code.into(),
                detail: pieces.next().map(|str| str.into()),
            });
        }
    };
    let detail = pieces.next().map(|str| str.into());
    ServerError(Repr::Known { kind, detail })
}

pub fn get_push_kind(kind: String) -> PushKind {
    match kind.as_str() {
        "invalidate" => PushKind::Invalidate,
        "message" => PushKind::Message,
        "pmessage" => PushKind::PMessage,
        "smessage" => PushKind::SMessage,
        "unsubscribe" => PushKind::Unsubscribe,
        "punsubscribe" => PushKind::PUnsubscribe,
        "sunsubscribe" => PushKind::SUnsubscribe,
        "subscribe" => PushKind::Subscribe,
        "psubscribe" => PushKind::PSubscribe,
        "ssubscribe" => PushKind::SSubscribe,
        _ => PushKind::Other(kind),
    }
}

fn value<'a, I>(
    count: Option<usize>,
) -> impl combine::Parser<I, Output = Value, PartialState = AnySendSyncPartialState>
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
                    line().map(|line| {
                        if line == "OK" {
                            Value::Okay
                        } else {
                            Value::SimpleString(line.into())
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
                            combine::produce(|| Value::Nil).left()
                        } else {
                            take(*size as usize)
                                .map(|bs: &[u8]| Value::BulkString(bs.to_vec()))
                                .skip(crlf())
                                .right()
                        }
                    })
                };
                let blob = || {
                    int().then_partial(move |size| {
                        take(*size as usize)
                            .map(|bs: &[u8]| String::from_utf8_lossy(bs).to_string())
                            .skip(crlf())
                    })
                };

                let array = || {
                    int().then_partial(move |&mut length| {
                        if length < 0 {
                            combine::produce(|| Value::Nil).left()
                        } else {
                            let length = length as usize;
                            combine::count_min_max(length, length, value(Some(count + 1)))
                                .map(Value::Array)
                                .right()
                        }
                    })
                };

                let error = || line().map(err_parser);
                let map = || {
                    int().then_partial(move |&mut kv_length| {
                        match (kv_length as usize).checked_mul(2) {
                            Some(length) => {
                                combine::count_min_max(length, length, value(Some(count + 1)))
                                    .map(move |result: Vec<Value>| {
                                        let mut it = result.into_iter();
                                        let mut x = vec![];
                                        for _ in 0..kv_length {
                                            if let (Some(k), Some(v)) = (it.next(), it.next()) {
                                                x.push((k, v));
                                            }
                                        }
                                        Value::Map(x)
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
                                combine::count_min_max(length, length, value(Some(count + 1)))
                                    .map(move |result: Vec<Value>| {
                                        let mut it = result.into_iter();
                                        let mut attributes = vec![];
                                        for _ in 0..kv_length {
                                            if let (Some(k), Some(v)) = (it.next(), it.next()) {
                                                attributes.push((k, v));
                                            }
                                        }
                                        Value::Attribute {
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
                            combine::produce(|| Value::Nil).left()
                        } else {
                            let length = length as usize;
                            combine::count_min_max(length, length, value(Some(count + 1)))
                                .map(Value::Set)
                                .right()
                        }
                    })
                };
                let push = || {
                    int().then_partial(move |&mut length| {
                        if length <= 0 {
                            combine::produce(|| Value::Push {
                                kind: PushKind::Other("".to_string()),
                                data: vec![],
                            })
                            .left()
                        } else {
                            let length = length as usize;
                            combine::count_min_max(length, length, value(Some(count + 1)))
                                .and_then(|result: Vec<Value>| {
                                    let mut it = result.into_iter();
                                    let first = it.next().unwrap_or(Value::Nil);
                                    if let Value::BulkString(kind) = first {
                                        let push_kind = String::from_utf8(kind)
                                            .map_err(StreamErrorFor::<I>::other)?;
                                        Ok(Value::Push {
                                            kind: get_push_kind(push_kind),
                                            data: it.collect(),
                                        })
                                    } else if let Value::SimpleString(kind) = first {
                                        Ok(Value::Push {
                                            kind: get_push_kind(kind),
                                            data: it.collect(),
                                        })
                                    } else {
                                        Err(StreamErrorFor::<I>::message_static_message(
                                            "parse error when decoding push",
                                        ))
                                    }
                                })
                                .right()
                        }
                    })
                };
                let null = || line().map(|_| Value::Nil);
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
                let blob_error = || blob().map(|line| err_parser(&line));
                let verbatim = || {
                    blob().and_then(|line| {
                        if let Some((format, text)) = line.split_once(':') {
                            let format = match format {
                                "txt" => VerbatimFormat::Text,
                                "mkd" => VerbatimFormat::Markdown,
                                x => VerbatimFormat::Unknown(x.to_string()),
                            };
                            Ok(Value::VerbatimString {
                                format,
                                text: text.to_string(),
                            })
                        } else {
                            Err(StreamErrorFor::<I>::message_static_message(
                                "parse error when decoding verbatim string",
                            ))
                        }
                    })
                };
                let big_number = || {
                    line().and_then(|line| {
                        #[cfg(not(feature = "num-bigint"))]
                        return Ok::<_, StreamErrorFor<I>>(Value::BigNumber(
                            line.as_bytes().to_vec(),
                        ));
                        #[cfg(feature = "num-bigint")]
                        num_bigint::BigInt::parse_bytes(line.as_bytes(), 10)
                            .ok_or_else(|| {
                                StreamErrorFor::<I>::message_static_message(
                                    "Expected bigint, got garbage",
                                )
                            })
                            .map(Value::BigNumber)
                    })
                };
                combine::dispatch!(b;
                    b'+' => simple_string(),
                    b':' => int().map(Value::Int),
                    b'$' => bulk_string(),
                    b'*' => array(),
                    b'%' => map(),
                    b'|' => attribute(),
                    b'~' => set(),
                    b'-' => error().map(Value::ServerError),
                    b'_' => null(),
                    b',' => double().map(Value::Double),
                    b'#' => boolean().map(Value::Boolean),
                    b'!' => blob_error().map(Value::ServerError),
                    b'=' => verbatim(),
                    b'(' => big_number(),
                    b'>' => push(),
                    b => combine::unexpected_any(combine::error::Token(b))
                )
            })
    ))
}

// a macro is needed because of lifetime shenanigans with `decoder`.
macro_rules! to_redis_err {
    ($err: expr, $decoder: expr) => {
        match $err {
            decoder::Error::Io { error, .. } => error.into(),
            decoder::Error::Parse(err) => {
                if err.is_unexpected_end_of_input() {
                    RedisError::from(io::Error::from(io::ErrorKind::UnexpectedEof))
                } else {
                    let err = err
                        .map_range(|range| format!("{range:?}"))
                        .map_position(|pos| pos.translate_position($decoder.buffer()))
                        .to_string();
                    RedisError::from(ParsingError::from(err))
                }
            }
        }
    };
}

#[cfg(feature = "aio")]
mod aio_support {
    use super::*;

    use bytes::{Buf, BytesMut};
    use tokio::io::AsyncRead;
    use tokio_util::codec::{Decoder, Encoder};

    /// Decodes Redis replies from an async byte stream.
    ///
    /// `doc(hidden)`-re-exported in `lib.rs` so the codec microbenchmarks can
    /// drive it directly.
    #[derive(Default)]
    pub struct ValueCodec {
        state: AnySendSyncPartialState,
        /// Whether `combine` is partway through a reply. If it is, the front of
        /// the buffer is mid-reply and must not be read as a new kind byte, so
        /// the fast path in [`Self::decode_stream`] stays out of the way.
        in_progress: bool,
    }

    impl ValueCodec {
        fn decode_stream(&mut self, bytes: &mut BytesMut, eof: bool) -> RedisResult<Option<Value>> {
            // Settle replies the direct paths can materialize before `combine`
            // can start walking them. Only at a reply boundary: once it has
            // consumed part of a reply, the front of the buffer is mid-reply,
            // not a new kind byte.
            let direct = if self.in_progress {
                None
            } else {
                fast_value(bytes)
            };
            if let Some((value, consumed)) = direct {
                bytes.advance(consumed);
                return Ok(Some(value));
            }
            let (opt, removed_len) = {
                let buffer = &bytes[..];
                let mut stream =
                    combine::easy::Stream(combine::stream::MaybePartialStream(buffer, !eof));
                match combine::stream::decode_tokio(value(None), &mut stream, &mut self.state) {
                    Ok(x) => x,
                    Err(err) => {
                        // The reply was rejected; start the next input from a
                        // general parse.
                        self.in_progress = false;
                        let err = err
                            .map_position(|pos| pos.translate_position(buffer))
                            .map_range(|range| format!("{range:?}"))
                            .to_string();
                        return Err(RedisError::from(ParsingError::from(err)));
                    }
                }
            };

            bytes.advance(removed_len);
            match opt {
                Some(result) => {
                    self.in_progress = false;
                    Ok(Some(result))
                }
                None => {
                    self.in_progress = true;
                    Ok(None)
                }
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
        type Item = Value;
        type Error = RedisError;

        fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            self.decode_stream(src, false)
        }

        fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            self.decode_stream(buf, true)
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
        let result = combine::decode_tokio!(*decoder, *read, value(None), |input, _| {
            combine::stream::easy::Stream::from(input)
        });
        match result {
            Err(err) => Err(to_redis_err!(err, decoder)),
            Ok(result) => Ok(result),
        }
    }
}

#[cfg(feature = "aio")]
#[cfg_attr(docsrs, doc(cfg(feature = "aio")))]
pub use self::aio_support::*;

/// Finds the end of the line that starts at `start`, returning `(end, after)`
/// where `buffer[start..end]` is the line without its CRLF and `after` is the
/// index just past the CRLF.
#[inline]
fn line_bounds(buffer: &[u8], start: usize) -> Option<(usize, usize)> {
    let rest = buffer.get(start..)?;
    let at = rest.windows(2).position(|window| window == b"\r\n")?;
    Some((start + at, start + at + 2))
}

/// The UTF-8 text of the line starting at `start`, plus the index just past it.
#[inline]
fn line_str(buffer: &[u8], start: usize) -> Option<(&str, usize)> {
    let (end, after) = line_bounds(buffer, start)?;
    Some((str::from_utf8(&buffer[start..end]).ok()?, after))
}

/// Parses the canonical decimal form `-?[0-9]+`.
///
/// Returns `None` for anything else -- whitespace, a leading `+`, a non-digit,
/// or `i64::MIN`'s magnitude -- so the caller can fall back to the more
/// permissive general parser (which trims and uses `str::parse`).
#[inline(always)]
fn fast_int_line(line: &[u8]) -> Option<i64> {
    let (negative, digits) = match line.split_first()? {
        (b'-', rest) => (true, rest),
        (_, _) => (false, line),
    };
    if digits.is_empty() {
        return None;
    }
    let mut value: i64 = 0;
    for &byte in digits {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(digit as i64)?;
    }
    Some(if negative { -value } else { value })
}

/// Parses a top-level `+<utf8>\r\n` or `:<int>\r\n` reply, returning its value
/// and the number of bytes it occupies.
///
/// Returns `None` for anything else -- another kind, an incomplete line, or
/// content `combine` must judge (invalid UTF-8, an integer it would reject) --
/// so the caller can fall back to it, errors included. Nested values never
/// reach this; `combine` parses those.
#[inline]
fn fast_scalar(buffer: &[u8]) -> Option<(Value, usize)> {
    let (&kind, rest) = buffer.split_first()?;
    if !matches!(kind, b'+' | b':') {
        return None;
    }
    let line_end = rest.windows(2).position(|window| window == b"\r\n")?;
    let line = &rest[..line_end];
    // Kind byte, line, CRLF.
    let consumed = line_end + 3;
    let value = match kind {
        b'+' => {
            if line == b"OK" {
                // Comparing the bytes first avoids a UTF-8 check for the most
                // common status reply.
                Value::Okay
            } else {
                Value::SimpleString(str::from_utf8(line).ok()?.into())
            }
        }
        // `int` trims the line before parsing it, so anything but the canonical
        // decimal form falls back to it (and its error) rather than being
        // judged differently here.
        _ => match fast_int_line(line) {
            Some(value) => Value::Int(value),
            None => Value::Int(str::from_utf8(line).ok()?.trim().parse().ok()?),
        },
    };
    Some((value, consumed))
}

/// Parses `+`, `:`, `_`, `,`, `#` and aggregates of those without invoking
/// `combine`.
///
/// Only values that are owned outright are accepted. A non-`OK` simple string
/// and a bulk string both need an allocation on this branch, so they are left
/// to the general parser; in return, this can build an array of integers,
/// booleans, nils, doubles, and `OK`s directly. Anything invalid or incomplete
/// is also declined, so `combine` still owns error reporting.
fn fast_frameless_value(buffer: &[u8], pos: usize, depth: usize) -> Option<(Value, usize)> {
    if depth > MAX_RECURSE_DEPTH {
        return None;
    }
    match *buffer.get(pos)? {
        b':' => {
            let (end, after) = line_bounds(buffer, pos + 1)?;
            Some((Value::Int(fast_int_line(&buffer[pos + 1..end])?), after))
        }
        b'+' => {
            let (line, after) = line_str(buffer, pos + 1)?;
            (line == "OK").then_some((Value::Okay, after))
        }
        b'_' => {
            // `line` validates UTF-8 even though the payload is ignored, and
            // the fast path has to reject exactly what `combine` rejects.
            let (_, after) = line_str(buffer, pos + 1)?;
            Some((Value::Nil, after))
        }
        b',' => {
            let (line, after) = line_str(buffer, pos + 1)?;
            Some((Value::Double(line.trim().parse().ok()?), after))
        }
        b'#' => {
            let (line, after) = line_str(buffer, pos + 1)?;
            match line {
                "t" => Some((Value::Boolean(true), after)),
                "f" => Some((Value::Boolean(false), after)),
                _ => None,
            }
        }
        b'*' => {
            let (end, after) = line_bounds(buffer, pos + 1)?;
            let length = fast_int_line(&buffer[pos + 1..end])?;
            if length < 0 {
                return Some((Value::Nil, after));
            }
            let length = usize::try_from(length).ok()?;
            // Every accepted child takes at least three bytes (`_\r\n`), so a
            // length that cannot fit in the rest of the buffer is incomplete.
            // This also keeps a malicious header from reserving a huge vector
            // before the input is known to contain that many elements.
            if length > buffer.len().saturating_sub(after) / 3 {
                return None;
            }
            let mut items = Vec::with_capacity(length);
            let mut at = after;
            for _ in 0..length {
                let (item, next) = fast_frameless_value(buffer, at, depth + 1)?;
                items.push(item);
                at = next;
            }
            Some((Value::Array(items), at))
        }
        _ => None,
    }
}

/// Parses a complete `$<length>\r\n<payload>\r\n` reply.
///
/// Unlike the fast scalar paths, this has to allocate the payload: on this
/// branch `Value::BulkString` owns a `Vec<u8>`. It still avoids walking
/// `combine` for the common bulk reply, but it cannot avoid the copy.
fn fast_bulk_value(buffer: &[u8]) -> Option<(Value, usize)> {
    let (line, payload_start) = line_str(buffer, 1)?;
    let length: i64 = line.trim().parse().ok()?;
    if length < 0 {
        return Some((Value::Nil, payload_start));
    }
    let length = usize::try_from(length).ok()?;
    let payload_end = payload_start.checked_add(length)?;
    let after = payload_end.checked_add(2)?;
    if buffer.len() < after || &buffer[payload_end..after] != b"\r\n" {
        return None;
    }
    Some((
        Value::BulkString(buffer[payload_start..payload_end].to_vec()),
        after,
    ))
}

/// Tries the direct paths for the reply at the start of `buffer`.
///
/// Line scalars keep their own small function so they stay inlined; the
/// frameless aggregate and bulk paths are deliberately separate so their code
/// does not bloat the common `+OK`/integer case.
#[inline]
fn fast_value(buffer: &[u8]) -> Option<(Value, usize)> {
    match buffer.first()? {
        b'+' | b':' => fast_scalar(buffer),
        b'$' => fast_bulk_value(buffer),
        b'*' | b'_' | b',' | b'#' => fast_frameless_value(buffer, 0, 1),
        _ => None,
    }
}

/// The internal redis response parser.
pub struct Parser {
    decoder: Decoder<AnySendSyncPartialState, PointerOffset<[u8]>>,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
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
    pub fn new() -> Self {
        Self {
            decoder: Decoder::new(),
        }
    }

    // public api

    /// Parses synchronously into a single value from the reader.
    pub fn parse_value<T: Read>(&mut self, mut reader: T) -> RedisResult<Value> {
        let mut decoder = &mut self.decoder;
        let result = combine::decode!(decoder, reader, value(None), |input, _| {
            combine::stream::easy::Stream::from(input)
        });
        match result {
            Err(err) => Err(to_redis_err!(err, decoder)),
            Ok(result) => Ok(result),
        }
    }
}

/// Parses bytes into a redis value.
///
/// This is the most straightforward way to parse something into a low
/// level redis value instead of having to use a whole parser.
pub fn parse_redis_value(bytes: &[u8]) -> RedisResult<Value> {
    // Settle replies the direct paths can materialize before `combine`;
    // anything they decline goes through the general parser, errors included.
    if let Some((value, _consumed)) = fast_value(bytes) {
        return Ok(value);
    }
    let mut parser = Parser::new();
    parser.parse_value(bytes)
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
        let mut codec = ValueCodec::default();

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
        let mut codec = ValueCodec::default();

        let mut bytes =
            bytes::BytesMut::from(b"*3\r\n+OK\r\n-LOADING server is loading\r\n+OK\r\n".as_slice());
        let result = codec.decode_eof(&mut bytes).unwrap().unwrap();

        assert_eq!(
            result,
            Value::Array(vec![
                Value::Okay,
                Value::ServerError(ServerError(Repr::Known {
                    kind: ServerErrorKind::BusyLoading,
                    detail: Some(arcstr::literal!("server is loading"))
                })),
                Value::Okay
            ])
        );

        let mut bytes = bytes::BytesMut::from(b"+OK\r\n".as_slice());
        let result = codec.decode_eof(&mut bytes).unwrap().unwrap();

        assert_eq!(result, Value::Okay);
    }

    #[cfg(feature = "aio")]
    #[test]
    fn decode_fast_path_line_scalars() {
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec::default();

        let mut bytes = bytes::BytesMut::from(&b"+OK\r\n:42\r\n"[..]);
        assert_eq!(codec.decode(&mut bytes), Ok(Some(Value::Okay)));
        assert_eq!(codec.decode(&mut bytes), Ok(Some(Value::Int(42))));
        assert!(bytes.is_empty());
        assert_eq!(codec.decode(&mut bytes), Ok(None));
    }

    #[cfg(feature = "aio")]
    #[test]
    fn decode_fast_path_bulk_reply() {
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec::default();

        let mut bytes = bytes::BytesMut::from(&b"$3\r\nfoo\r\n$0\r\n\r\n$-1\r\n"[..]);
        assert_eq!(
            codec.decode(&mut bytes),
            Ok(Some(Value::BulkString(b"foo".to_vec())))
        );
        assert_eq!(
            codec.decode(&mut bytes),
            Ok(Some(Value::BulkString(Vec::new())))
        );
        assert_eq!(codec.decode(&mut bytes), Ok(Some(Value::Nil)));
        assert!(bytes.is_empty());
    }

    #[cfg(feature = "aio")]
    #[test]
    fn decode_fast_path_declines_payloads_it_cannot_vouch_for() {
        use tokio_util::codec::Decoder;

        // An integer the general parser rejects must be reported as such, not
        // silently accepted by the fast path.
        let mut codec = ValueCodec::default();
        let mut bytes = bytes::BytesMut::from(&b":garbage\r\n"[..]);
        let err = codec.decode(&mut bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Parse);
        assert_eq!(
            err.to_string(),
            "Incompatible type - Parse error at 1\nExpected integer, got garbage\n"
        );

        // Likewise invalid UTF-8 in a simple string.
        let mut codec = ValueCodec::default();
        let mut bytes = bytes::BytesMut::from(&b"+\xff\r\n"[..]);
        let err = codec.decode(&mut bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Parse);
        assert_eq!(
            err.to_string(),
            "Incompatible type - Parse error at 1\ninvalid utf-8 sequence of 1 bytes from index 0\n"
        );
    }

    #[cfg(feature = "aio")]
    #[test]
    fn decode_split_line_scalar_is_not_misread_as_the_next_kind() {
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec::default();

        // `combine` may consume the `+` before the rest of the line arrives, so
        // the buffer stands at `:42` in the middle of the reply. Reading that
        // as a new integer reply would return 42 instead of the simple string
        // ":42", so the fast path must stay out of an unfinished reply.
        let mut bytes = bytes::BytesMut::from(&b"+:42"[..]);
        assert_eq!(codec.decode(&mut bytes), Ok(None));
        bytes.extend_from_slice(b"\r\n");
        assert_eq!(
            codec.decode(&mut bytes),
            Ok(Some(Value::SimpleString(":42".to_string())))
        );
        assert!(bytes.is_empty());
    }

    #[cfg(feature = "aio")]
    #[test]
    fn decode_split_line_scalar_resumes() {
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec::default();

        let mut bytes = bytes::BytesMut::from(&b"+OK"[..]);
        assert_eq!(codec.decode(&mut bytes), Ok(None));
        bytes.extend_from_slice(b"\r\n");
        assert_eq!(codec.decode(&mut bytes), Ok(Some(Value::Okay)));
    }

    #[cfg(feature = "aio")]
    #[test]
    fn decode_split_bulk_payload_is_not_misread_as_a_scalar() {
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec::default();

        // A bulk payload may itself start with a kind byte. The header is
        // consumed before the payload completes, so the buffer stands at `+OK`
        // in the middle of the reply; it has to be reported as payload, not as
        // an `OK` status reply.
        let mut bytes = bytes::BytesMut::from(&b"$3\r\n+OK"[..]);
        assert_eq!(codec.decode(&mut bytes), Ok(None));
        bytes.extend_from_slice(b"\r\n");
        assert_eq!(
            codec.decode(&mut bytes),
            Ok(Some(Value::BulkString(b"+OK".to_vec())))
        );
        assert!(bytes.is_empty());
    }

    #[test]
    fn parse_redis_value_fast_path_agrees_with_the_general_parse() {
        assert_eq!(parse_redis_value(b"+OK\r\n").unwrap(), Value::Okay);
        assert_eq!(
            parse_redis_value(b"+hello\r\n").unwrap(),
            Value::SimpleString("hello".to_string())
        );
        assert_eq!(parse_redis_value(b":42\r\n").unwrap(), Value::Int(42));
        // The general parser trims integer lines, and so must the fast path.
        assert_eq!(parse_redis_value(b": 42 \r\n").unwrap(), Value::Int(42));
        // A `:` after `+` is line content, not a nested kind.
        assert_eq!(
            parse_redis_value(b"+:42\r\n").unwrap(),
            Value::SimpleString(":42".to_string())
        );
        // Trailing input is left alone, as the general parser leaves it.
        assert_eq!(parse_redis_value(b"+OK\r\n:1\r\n").unwrap(), Value::Okay);
        // Rejected payloads still go through the general parser's error.
        let err = parse_redis_value(b":garbage\r\n").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Parse);
        assert_eq!(
            err.to_string(),
            "Incompatible type - Parse error at 1\nExpected integer, got garbage\n"
        );
        let err = parse_redis_value(b"+\xff\r\n").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Parse);
        assert_eq!(
            err.to_string(),
            "Incompatible type - Parse error at 1\ninvalid utf-8 sequence of 1 bytes from index 0\n"
        );
    }

    #[test]
    fn direct_values_match_the_general_parser() {
        // Every payload the direct paths claim has to produce exactly the value
        // `combine` produces, and every payload they decline has to keep
        // reaching it (errors included).
        let payloads: &[&[u8]] = &[
            // Line scalars, including forms `fast_int_line` declines.
            b"+OK\r\n",
            b"+\r\n",
            b"+GET 123\r\n",
            b"+\xff\xfe\r\n",
            b":1\r\n",
            b":-5\r\n",
            b":+5\r\n",
            b": 7\r\n",
            b":001\r\n",
            b":-0\r\n",
            b":-9223372036854775808\r\n",
            b":9223372036854775808\r\n",
            b":abc\r\n",
            // Bulk strings, including empty, null, binary, and malformed.
            b"$0\r\n\r\n",
            b"$3\r\nfoo\r\n",
            b"$3\r\nfoo\r\n:1\r\n",
            b"$3\r\nf\xffo\r\n",
            b"$-1\r\n",
            b"$-2\r\n",
            b"$ 3 \r\nfoo\r\n",
            b"$+3\r\nfoo\r\n",
            b"$03\r\nfoo\r\n",
            b"$3\r\nfooXX",
            b"$3\r\nfoo\rX",
            b"$3\r\nfoo",
            b"$9999999999999\r\n",
            // Frameless aggregates and their fallbacks.
            b"*0\r\n",
            b"*-1\r\n",
            b"*3\r\n:100\r\n:101\r\n:102\r\n",
            b"*3\r\n+OK\r\n#t\r\n_\r\n",
            b"*2\r\n,1.5\r\n:2\r\n",
            b"*2\r\n,inf\r\n:nan\r\n",
            b"*1\r\n*1\r\n:1\r\n",
            b"*2\r\n_ignored\r\n:1\r\n",
            b"* 2 \r\n:1\r\n:2\r\n",
            b"*+2\r\n:1\r\n:2\r\n",
            b"*3\r\n+GET\r\n:1\r\n$3\r\nfoo\r\n",
            b"*1\r\n:abc\r\n",
            b"*1\r\n#x\r\n",
            b"*2\r\n_\xff\r\n:1\r\n",
            b"*1\r\n",
            b"*9223372036854775807\r\n",
            // Top-level frameless scalars.
            b"_\r\n",
            b"_\xff\r\n",
            b",1.5\r\n",
            b",garbage\r\n",
            b"#t\r\n",
            b"#x\r\n",
            // Unknown kinds still go through the general parser.
            b"|1\r\n+key\r\n:1\r\n+data\r\n",
        ];
        for payload in payloads {
            let expected = Parser::new().parse_value(*payload);
            let actual = parse_redis_value(payload);
            match (expected, actual) {
                (Ok(expected), Ok(actual)) => assert_eq!(actual, expected, "{payload:?}"),
                (Err(expected), Err(actual)) => {
                    assert_eq!(actual.kind(), expected.kind(), "{payload:?}")
                }
                (expected, actual) => panic!("{payload:?}: {expected:?} but got {actual:?}"),
            }
        }
    }

    #[cfg(feature = "aio")]
    #[test]
    fn decode_fast_path_frameless_aggregates() {
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec::default();

        let mut bytes = bytes::BytesMut::from(b"*3\r\n:100\r\n:101\r\n:102\r\n+OK\r\n".as_slice());
        assert_eq!(
            codec.decode(&mut bytes),
            Ok(Some(Value::Array(vec![
                Value::Int(100),
                Value::Int(101),
                Value::Int(102)
            ])))
        );
        assert_eq!(codec.decode(&mut bytes), Ok(Some(Value::Okay)));
        assert!(bytes.is_empty());
    }

    #[cfg(feature = "aio")]
    #[test]
    fn decode_split_frameless_aggregate_resumes() {
        use tokio_util::codec::Decoder;
        let mut codec = ValueCodec::default();

        // The first read lets the direct path see a partial aggregate; combine
        // takes over and resumes it on later reads.
        let payload = b"*3\r\n:10\r\n:20\r\n:30\r\n+OK\r\n";
        let split = payload.len() - b"+OK\r\n".len();
        let mut bytes = bytes::BytesMut::new();

        for &byte in &payload[..split - 1] {
            bytes.extend_from_slice(&[byte]);
            assert_eq!(codec.decode(&mut bytes), Ok(None));
        }
        bytes.extend_from_slice(&payload[split - 1..split]);
        assert_eq!(
            codec.decode(&mut bytes),
            Ok(Some(Value::Array(vec![
                Value::Int(10),
                Value::Int(20),
                Value::Int(30)
            ])))
        );
        assert!(bytes.is_empty());

        bytes.extend_from_slice(&payload[split..]);
        assert_eq!(codec.decode(&mut bytes), Ok(Some(Value::Okay)));
        assert!(bytes.is_empty());
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
                    detail: Some(arcstr::literal!("server is loading"))
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
            (&Value::SimpleString("first".to_string()), &Value::Int(1)),
            v.next().unwrap()
        );
        assert_eq!(
            (&Value::SimpleString("second".to_string()), &Value::Int(2)),
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
                code: arcstr::literal!("SYNTAX"),
                detail: Some(arcstr::literal!("invalid syntax"))
            }))
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
        let expected = Value::BigNumber(b"3492890328409238509324850943850943825024385".to_vec());
        assert_eq!(val, expected);
    }

    #[test]
    fn decode_resp3_set() {
        let val = parse_redis_value(b"~5\r\n+orange\r\n+apple\r\n#t\r\n:100\r\n:999\r\n").unwrap();
        let v = val.as_sequence().unwrap();
        assert!(v.len() >= 5);
        assert_eq!(Value::SimpleString("orange".to_string()), v[0]);
        assert_eq!(Value::SimpleString("apple".to_string()), v[1]);
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
            assert_eq!(Value::SimpleString("somechannel".to_string()), data[0]);
            assert_eq!(
                Value::SimpleString("this is the message".to_string()),
                data[1]
            );
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
