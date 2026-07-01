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

// ---------------------------------------------------------------------------
// Hand-written RESP fast-path decoder.
//
// Profiling the client read path showed most of the CPU spent inside `combine`'s
// partial-state parser-combinator machinery. That machinery exists to resume a
// parse across a buffer boundary,
// but for the overwhelmingly common case — a *complete* reply already sitting
// in the buffer — it is pure overhead.
//
// `fast_parse_value` decodes one complete RESP value directly from a byte
// slice for the high-frequency types (`+ - : $ * _`). It returns
// `Some((value, consumed))` ONLY when it fully parses a supported, well-formed
// value with semantics identical to the `combine` grammar below. For anything
// else — an incomplete buffer, an unsupported RESP3 type (`% ~ | , # ! = ( >`),
// or malformed input — it returns `None`, and the caller falls back to the
// `combine` parser, which remains the single source of truth for correctness
// and error reporting. This keeps the fast path a pure accelerator that can
// only ever agree with `combine`, never diverge from it.

/// Index of the `\r` of the first `\r\n` at or after `from`, or `None` if the
/// buffer does not yet contain a line terminator.
#[inline]
fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while let Some(off) = buf[i..].iter().position(|&b| b == b'\r') {
        let cr = i + off;
        match buf.get(cr + 1) {
            Some(b'\n') => return Some(cr),
            Some(_) => i = cr + 1,
            None => return None,
        }
    }
    None
}

/// Read a `\r\n`-terminated line starting at `pos`. Returns the content (without
/// the terminator) and the position just past the `\r\n`.
#[inline]
fn read_line(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    let cr = find_crlf(buf, pos)?;
    Some((&buf[pos..cr], cr + 2))
}

/// Parse one RESP value from `buf` starting at `pos`. Returns `(value, new_pos)`
/// on success. `depth` guards against unbounded recursion, matching the
/// `combine` grammar's `MAX_RECURSE_DEPTH` behavior (fall back on excess).
fn fast_parse_at(buf: &[u8], pos: usize, depth: usize) -> Option<(Value, usize)> {
    if depth > MAX_RECURSE_DEPTH {
        return None;
    }
    let marker = *buf.get(pos)?;
    let body = pos + 1;
    match marker {
        b'+' => {
            let (line, np) = read_line(buf, body)?;
            let s = str::from_utf8(line).ok()?;
            let v = if s == "OK" {
                Value::Okay
            } else {
                Value::SimpleString(s.into())
            };
            Some((v, np))
        }
        b'-' => {
            let (line, np) = read_line(buf, body)?;
            let s = str::from_utf8(line).ok()?;
            Some((Value::ServerError(err_parser(s)), np))
        }
        b':' => {
            let (line, np) = read_line(buf, body)?;
            let s = str::from_utf8(line).ok()?;
            let n = s.trim().parse::<i64>().ok()?;
            Some((Value::Int(n), np))
        }
        b'_' => {
            // RESP3 null. combine's `null()` runs through the same UTF-8-validating
            // `line()` as every other type, so a non-UTF-8 body is rejected there;
            // validate here too (else the fast path would accept `_\xff\r\n` as Nil
            // while combine errors). The content is otherwise ignored.
            let (line, np) = read_line(buf, body)?;
            str::from_utf8(line).ok()?;
            Some((Value::Nil, np))
        }
        b'$' => {
            let (line, np) = read_line(buf, body)?;
            let s = str::from_utf8(line).ok()?;
            let size = s.trim().parse::<i64>().ok()?;
            if size < 0 {
                return Some((Value::Nil, np));
            }
            let size = size as usize;
            let end = np.checked_add(size)?;
            // Need the payload plus its trailing CRLF.
            if end + 2 > buf.len() {
                return None;
            }
            if &buf[end..end + 2] != b"\r\n" {
                return None; // malformed → let combine report it
            }
            Some((Value::BulkString(buf[np..end].to_vec()), end + 2))
        }
        b'*' => {
            let (line, np) = read_line(buf, body)?;
            let s = str::from_utf8(line).ok()?;
            let len = s.trim().parse::<i64>().ok()?;
            if len < 0 {
                return Some((Value::Nil, np));
            }
            let len = len as usize;
            // Cap the pre-allocation so a bogus length can't trigger a huge
            // up-front allocation; the real bound is the buffer contents.
            let mut out = Vec::with_capacity(len.min(1024));
            let mut cur = np;
            for _ in 0..len {
                let (v, next) = fast_parse_at(buf, cur, depth + 1)?;
                out.push(v);
                cur = next;
            }
            Some((Value::Array(out), cur))
        }
        _ => None, // unsupported RESP3 type → fall back to combine
    }
}

/// Fast-path entry point: decode one complete supported RESP value from the
/// start of `buf`. See the module note above for the contract.
#[inline]
fn fast_parse_value(buf: &[u8]) -> Option<(Value, usize)> {
    // Start at depth 1 to match the `combine` grammar, whose recursion `count`
    // starts at 1 and errors when it exceeds MAX_RECURSE_DEPTH.
    fast_parse_at(buf, 0, 1)
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
                                                x.push((k, v))
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
                                                attributes.push((k, v))
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

    #[derive(Default)]
    pub struct ValueCodec {
        state: AnySendSyncPartialState,
        // true while `combine` holds partial state from an earlier
        // incomplete decode. The fast path is only tried when this is false, so
        // a fast-path success can never be interleaved into a `combine`
        // resume-across-reads sequence (which would strand `state`).
        partial: bool,
    }

    impl ValueCodec {
        fn decode_stream(&mut self, bytes: &mut BytesMut, eof: bool) -> RedisResult<Option<Value>> {
            // Fast path: only safe when combine is not mid-parse. It consumes
            // exactly one complete value; Framed will call us again for the next.
            if !self.partial
                && let Some((value, consumed)) = fast_parse_value(&bytes[..])
            {
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
                        // A parse error tears down the connection today, but reset
                        // the flag anyway so the fast-path invariant (partial ⇔
                        // combine holds live state) is self-enforcing rather than
                        // relying on the caller not reusing the codec.
                        self.partial = false;
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
                    // combine finished a value; its partial state is reset, so
                    // the fast path is eligible again next call.
                    self.partial = false;
                    Ok(Some(result))
                }
                None => {
                    // combine consumed the buffer without completing a value and
                    // retained partial state; defer to combine until it finishes.
                    self.partial = true;
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

/// The internal redis response parser.
pub struct Parser {
    decoder: Decoder<AnySendSyncPartialState, PointerOffset<[u8]>>,
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
            decoder: Decoder::new(),
        }
    }

    // public api

    /// Parses synchronously into a single value from the reader.
    pub fn parse_value<T: Read>(&mut self, mut reader: T) -> RedisResult<Value> {
        // Fast path: if the decoder already holds a complete supported value,
        // decode it straight from the buffer and advance, skipping combine. This
        // is the common case for pipelined reads, where a single `read` buffers
        // many replies and only the first `parse_value` call actually blocks on
        // I/O. Anything else — empty/incomplete buffer, or an unsupported RESP3
        // type — falls through to combine, which handles reading more, partial
        // state, EOF, and error reporting exactly as before.
        if let Some((value, consumed)) = fast_parse_value(self.decoder.buffer()) {
            self.decoder.advance(&mut reader, consumed);
            return Ok(value);
        }
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
    // Fast path: a complete slice is the ideal case for the hand-written
    // decoder. Fall back to `combine` for unsupported types / malformed input.
    if let Some((value, _consumed)) = fast_parse_value(bytes) {
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

    /// Differential fuzz. `parse_redis_value` (fast path + combine
    /// fallback) must be observationally identical to pure `combine` on ANY
    /// input — same value on success, and error ⇔ error. This catches the fast
    /// path accepting something combine rejects (e.g. a non-UTF-8 `_` null line),
    /// not just value mismatches.
    ///
    /// Inputs are biased toward RESP shapes (leading marker + short, possibly
    /// non-UTF-8 body + optional CRLF) so the fast path's type arms and their
    /// edge cases are actually exercised — plain random bytes almost never form
    /// a parseable frame.
    #[derive(Clone, Debug)]
    struct RespishBytes(Vec<u8>);

    impl quickcheck::Arbitrary for RespishBytes {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            const MARKERS: &[u8] = b"+-:$*_%~,#!=(>";
            let mut out = Vec::new();
            let lines = (usize::arbitrary(g) % 4) + 1;
            for _ in 0..lines {
                out.push(*g.choose(MARKERS).unwrap());
                for _ in 0..(usize::arbitrary(g) % 6) {
                    out.push(u8::arbitrary(g));
                }
                if bool::arbitrary(g) {
                    out.extend_from_slice(b"\r\n");
                }
            }
            RespishBytes(out)
        }
    }

    #[test]
    fn fast_path_never_diverges_from_combine_fuzz() {
        fn prop(input: RespishBytes) -> bool {
            let data = &input.0;
            let via_fast = parse_redis_value(data); // fast path + combine fallback
            let via_combine = Parser::new().parse_value(&data[..]); // pure combine
            match (via_fast, via_combine) {
                (Ok(a), Ok(b)) => a == b,
                (Err(_), Err(_)) => true,
                _ => false, // one accepted, one rejected → divergence
            }
        }
        quickcheck::QuickCheck::new()
            .tests(100_000)
            .quickcheck(prop as fn(RespishBytes) -> bool);
    }

    /// The fast path must agree with the `combine` grammar on every
    /// input it claims (`Some`), and must decline (`None`) unsupported RESP3
    /// types so `combine` handles them. Guards against the fast path silently
    /// diverging from the canonical parser.
    #[test]
    fn fast_path_matches_combine() {
        // Supported types + edge cases: the fast path must fully parse these
        // and agree with the combine reference.
        let supported: &[&[u8]] = &[
            b"+OK\r\n",
            b"+hello world\r\n",
            b"-ERR bad\r\n",
            b"-MOVED 1234 127.0.0.1:6379\r\n",
            b":12345\r\n",
            b":-9\r\n",
            b"$5\r\nhello\r\n",
            b"$0\r\n\r\n",
            b"$-1\r\n",
            b"_\r\n",
            b"*3\r\n$3\r\nfoo\r\n:1\r\n+OK\r\n",
            b"*-1\r\n",
            b"*0\r\n",
            b"*2\r\n*1\r\n:1\r\n$1\r\na\r\n",
        ];
        for input in supported {
            let fast = fast_parse_value(input).map(|(v, _)| v);
            let reference = Parser::new().parse_value(*input).ok();
            assert_eq!(
                fast, reference,
                "fast path disagrees with combine on {input:?}"
            );
            assert!(fast.is_some(), "fast path should handle {input:?}");
        }

        // Unsupported RESP3 types: the fast path must decline so combine parses
        // them (and still produce the same value via parse_redis_value).
        let deferred: &[&[u8]] = &[
            b"%1\r\n+a\r\n:1\r\n",
            b"~2\r\n:1\r\n:2\r\n",
            b",3.14\r\n",
            b"#t\r\n",
            b"(12345\r\n",
            b">2\r\n$7\r\nmessage\r\n$1\r\nx\r\n",
        ];
        for input in deferred {
            assert!(
                fast_parse_value(input).is_none(),
                "fast path should decline unsupported type {input:?}"
            );
            // parse_redis_value must still succeed (via the combine fallback).
            assert!(
                parse_redis_value(input).is_ok(),
                "combine fallback for {input:?}"
            );
        }

        // Incomplete buffers: the fast path must decline (return None), leaving
        // the caller to wait for / fall back for more bytes.
        let incomplete: &[&[u8]] = &[b"$5\r\nhel", b"*2\r\n:1\r\n", b"+OK\r", b":12"];
        for input in incomplete {
            assert!(
                fast_parse_value(input).is_none(),
                "fast path should decline incomplete {input:?}"
            );
        }

        // Malformed line bodies: combine validates UTF-8 in `line()` for every
        // line-based type, so a non-UTF-8 body must be *rejected*. The fast path
        // must decline (→ combine errors), never accept it as a value. Regression
        // for the `_` null arm, which originally skipped this check.
        let non_utf8: &[&[u8]] = &[
            b"_\xff\r\n",
            b"+\xff\r\n",
            b":\xff\r\n",
            b"-\xff\r\n",
            b"$\xff\r\n",
            b"*\xff\r\n",
        ];
        for input in non_utf8 {
            assert!(
                fast_parse_value(input).is_none(),
                "fast path must decline non-utf8 line {input:?}"
            );
            assert!(
                Parser::new().parse_value(&input[..]).is_err(),
                "combine rejects non-utf8 line {input:?} (so the fast path must too)"
            );
        }
    }

    /// Sync `Parser::parse_value` over a buffer holding several concatenated
    /// replies: the first call reads + buffers them all, later calls hit the
    /// fast path over the decoder's buffer, and an unsupported type mid-stream
    /// hands back to combine — all sharing one decoder buffer. The decoded
    /// sequence must match exactly.
    #[test]
    fn sync_parser_buffered_fast_path() {
        let wire: &[u8] = b"$5\r\nhello\r\n+OK\r\n:42\r\n%1\r\n+k\r\n:1\r\n*2\r\n:1\r\n:2\r\n_\r\n";
        let expected = vec![
            Value::BulkString(b"hello".to_vec()),
            Value::Okay,
            Value::Int(42),
            Value::Map(vec![(Value::SimpleString("k".into()), Value::Int(1))]),
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            Value::Nil,
        ];
        let mut parser = Parser::new();
        let mut reader = wire;
        let mut got = Vec::new();
        for _ in 0..expected.len() {
            got.push(parser.parse_value(&mut reader).unwrap());
        }
        assert_eq!(got, expected);
    }

    /// After the fast path has advanced the shared decoder buffer, a malformed
    /// value must still make combine return an error (not panic) despite the
    /// bypassed `position`/`state`.
    #[test]
    fn sync_parser_error_after_fast_path() {
        let wire: &[u8] = b":1\r\n+OK\r\n$abc\r\n";
        let mut parser = Parser::new();
        let mut reader = wire;
        assert_eq!(parser.parse_value(&mut reader).unwrap(), Value::Int(1));
        assert_eq!(parser.parse_value(&mut reader).unwrap(), Value::Okay);
        assert!(parser.parse_value(&mut reader).is_err());
    }

    /// Exercise the async `ValueCodec` fast-path ↔ combine-partial
    /// interleaving across EVERY chunk boundary. At chunk size == wire length
    /// every frame is fully buffered (fast path handles all); at chunk size 1
    /// every frame arrives a byte at a time (combine partial-resume handles all);
    /// sizes in between mix the two. In all cases the decoded sequence must match
    /// exactly and no bytes may be stranded — i.e. no drop, duplicate, or
    /// stranded `partial`/`state`.
    #[cfg(feature = "aio")]
    #[test]
    fn codec_fast_path_and_partial_interleave() {
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let wire: &[u8] =
            b"$5\r\nhello\r\n+OK\r\n:42\r\n*2\r\n:1\r\n:2\r\n_\r\n$-1\r\n-ERR boom\r\n";
        let expected = vec![
            Value::BulkString(b"hello".to_vec()),
            Value::Okay,
            Value::Int(42),
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            Value::Nil,
            Value::Nil,
            Value::ServerError(err_parser("ERR boom")),
        ];

        for chunk in 1..=wire.len() {
            let mut codec = ValueCodec::default();
            let mut buf = BytesMut::new();
            let mut got = Vec::new();
            for piece in wire.chunks(chunk) {
                buf.extend_from_slice(piece);
                while let Some(v) = codec.decode(&mut buf).unwrap() {
                    got.push(v);
                }
            }
            while let Some(v) = codec.decode_eof(&mut buf).unwrap() {
                got.push(v);
            }
            assert_eq!(
                got, expected,
                "decoded sequence mismatch at chunk size {chunk}"
            );
            assert!(buf.is_empty(), "stranded bytes at chunk size {chunk}");
        }
    }

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
        )
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
