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
// Fast path for complete replies.
//
// The `combine` grammar below can suspend in the middle of a value and resume
// once more bytes arrive. That machinery has a large per-reply cost, which is
// wasted whenever the whole reply is already buffered - the common case.
//
// `fast_parse_value` decodes one complete RESP value straight from a byte
// slice. It returns `Some((value, consumed))` only when it produced exactly
// the value `combine` would have produced. For anything else - incomplete
// input, malformed input, or input `combine` would reject - it returns `None`
// and the caller falls back to `combine`, which stays the only producer of
// parse errors.

/// Finds the first `\r\n` at or after `pos`, returning the index of its `\r`.
#[inline]
fn find_crlf(buf: &[u8], pos: usize) -> Option<usize> {
    let mut from = pos;
    loop {
        let cr = from + buf.get(from..)?.iter().position(|&b| b == b'\r')?;
        if *buf.get(cr + 1)? == b'\n' {
            return Some(cr);
        }
        from = cr + 1;
    }
}

/// Reads a `\r\n`-terminated UTF-8 line starting at `pos`. Returns the line
/// without its terminator, and the position just after the terminator.
#[inline]
fn fast_line(buf: &[u8], pos: usize) -> Option<(&str, usize)> {
    let cr = find_crlf(buf, pos)?;
    let line = str::from_utf8(&buf[pos..cr]).ok()?;
    Some((line, cr + 2))
}

/// Reads an integer line, with the same leniency as `combine`'s `int()`.
#[inline]
fn fast_int(buf: &[u8], pos: usize) -> Option<(i64, usize)> {
    let (line, pos) = fast_line(buf, pos)?;
    Some((line.trim().parse().ok()?, pos))
}

/// Reads a `len`-byte payload starting at `pos`, followed by `\r\n`.
#[inline]
fn fast_payload(buf: &[u8], pos: usize, len: i64) -> Option<(&[u8], usize)> {
    let end = pos.checked_add(usize::try_from(len).ok()?)?;
    let payload = buf.get(pos..end)?;
    if buf.get(end..end.checked_add(2)?)? != b"\r\n" {
        return None;
    }
    Some((payload, end + 2))
}

/// Reads `len` consecutive values starting at `pos`.
fn fast_values(buf: &[u8], pos: usize, len: usize, depth: usize) -> Option<(Vec<Value>, usize)> {
    // The shortest encoded value is 3 bytes (e.g. `_\r\n`), so if fewer than
    // `3 * len` bytes are buffered the aggregate can't be complete. Checking
    // this first avoids decoding a prefix only to throw it away, and means the
    // allocation below is bounded by the buffer's size, not by a length read
    // from the wire.
    if len > buf.len().saturating_sub(pos) / 3 {
        return None;
    }
    let mut values = Vec::with_capacity(len);
    let mut pos = pos;
    for _ in 0..len {
        let (value, next) = fast_parse_at(buf, pos, depth)?;
        values.push(value);
        pos = next;
    }
    Some((values, pos))
}

fn into_pairs(values: Vec<Value>) -> Vec<(Value, Value)> {
    let mut it = values.into_iter();
    let mut pairs = Vec::with_capacity(it.len() / 2);
    while let (Some(k), Some(v)) = (it.next(), it.next()) {
        pairs.push((k, v));
    }
    pairs
}

fn fast_verbatim(payload: &[u8]) -> Option<Value> {
    let text = String::from_utf8_lossy(payload);
    let (format, text) = text.split_once(':')?;
    let format = match format {
        "txt" => VerbatimFormat::Text,
        "mkd" => VerbatimFormat::Markdown,
        x => VerbatimFormat::Unknown(x.to_string()),
    };
    Some(Value::VerbatimString {
        format,
        text: text.to_string(),
    })
}

/// Parses one value starting at `pos`, returning it and the position after
/// it. `depth` follows the `combine` grammar's recursion count, so values
/// nested too deeply are left to `combine` to reject.
fn fast_parse_at(buf: &[u8], pos: usize, depth: usize) -> Option<(Value, usize)> {
    if depth > MAX_RECURSE_DEPTH {
        return None;
    }
    let marker = *buf.get(pos)?;
    let pos = pos + 1;
    // Arms are ordered roughly by how often each type shows up in replies.
    // Negative lengths are mapped to whatever `combine` maps them to. The
    // spec only uses `-1` for null, but the two parsers must agree on every
    // input.
    let parsed = match marker {
        b'$' => {
            let (len, pos) = fast_int(buf, pos)?;
            if len < 0 {
                return Some((Value::Nil, pos));
            }
            let (payload, pos) = fast_payload(buf, pos, len)?;
            (Value::BulkString(payload.to_vec()), pos)
        }
        b'*' => {
            let (len, pos) = fast_int(buf, pos)?;
            if len < 0 {
                return Some((Value::Nil, pos));
            }
            let len = usize::try_from(len).ok()?;
            let (values, pos) = fast_values(buf, pos, len, depth + 1)?;
            (Value::Array(values), pos)
        }
        b':' => {
            let (n, pos) = fast_int(buf, pos)?;
            (Value::Int(n), pos)
        }
        b'+' => {
            let (line, pos) = fast_line(buf, pos)?;
            let value = if line == "OK" {
                Value::Okay
            } else {
                Value::SimpleString(line.into())
            };
            (value, pos)
        }
        b'-' => {
            let (line, pos) = fast_line(buf, pos)?;
            (Value::ServerError(err_parser(line)), pos)
        }
        b'_' => {
            let (_, pos) = fast_line(buf, pos)?;
            (Value::Nil, pos)
        }
        _ => return fast_parse_resp3(buf, marker, pos, depth),
    };
    Some(parsed)
}

/// The less common types. Kept out of `fast_parse_at`, since a smaller hot
/// function measurably speeds up the common replies.
#[inline(never)]
fn fast_parse_resp3(buf: &[u8], marker: u8, pos: usize, depth: usize) -> Option<(Value, usize)> {
    let parsed = match marker {
        b'%' => {
            let (len, pos) = fast_int(buf, pos)?;
            let len = usize::try_from(len).ok()?.checked_mul(2)?;
            let (values, pos) = fast_values(buf, pos, len, depth + 1)?;
            (Value::Map(into_pairs(values)), pos)
        }
        b'~' => {
            let (len, pos) = fast_int(buf, pos)?;
            if len < 0 {
                return Some((Value::Nil, pos));
            }
            let len = usize::try_from(len).ok()?;
            let (values, pos) = fast_values(buf, pos, len, depth + 1)?;
            (Value::Set(values), pos)
        }
        b'>' => {
            let (len, pos) = fast_int(buf, pos)?;
            if len <= 0 {
                let empty = Value::Push {
                    kind: PushKind::Other(String::new()),
                    data: vec![],
                };
                return Some((empty, pos));
            }
            let len = usize::try_from(len).ok()?;
            let (values, pos) = fast_values(buf, pos, len, depth + 1)?;
            let mut values = values.into_iter();
            let kind = match values.next()? {
                Value::BulkString(kind) => String::from_utf8(kind).ok()?,
                Value::SimpleString(kind) => kind,
                _ => return None,
            };
            let push = Value::Push {
                kind: get_push_kind(kind),
                data: values.collect(),
            };
            (push, pos)
        }
        b',' => {
            let (line, pos) = fast_line(buf, pos)?;
            (Value::Double(line.trim().parse().ok()?), pos)
        }
        b'#' => {
            let (line, pos) = fast_line(buf, pos)?;
            let value = match line {
                "t" => true,
                "f" => false,
                _ => return None,
            };
            (Value::Boolean(value), pos)
        }
        b'(' => {
            let (line, pos) = fast_line(buf, pos)?;
            #[cfg(feature = "num-bigint")]
            let number = num_bigint::BigInt::parse_bytes(line.as_bytes(), 10)?;
            #[cfg(not(feature = "num-bigint"))]
            let number = line.as_bytes().to_vec();
            (Value::BigNumber(number), pos)
        }
        b'=' => {
            let (len, pos) = fast_int(buf, pos)?;
            let (payload, pos) = fast_payload(buf, pos, len)?;
            (fast_verbatim(payload)?, pos)
        }
        b'!' => {
            let (len, pos) = fast_int(buf, pos)?;
            let (payload, pos) = fast_payload(buf, pos, len)?;
            let error = err_parser(&String::from_utf8_lossy(payload));
            (Value::ServerError(error), pos)
        }
        b'|' => {
            let (len, pos) = fast_int(buf, pos)?;
            let len = usize::try_from(len).ok()?.checked_mul(2)?.checked_add(1)?;
            let (mut values, pos) = fast_values(buf, pos, len, depth + 1)?;
            let data = Box::new(values.pop()?);
            let attribute = Value::Attribute {
                data,
                attributes: into_pairs(values),
            };
            (attribute, pos)
        }
        _ => return None,
    };
    Some(parsed)
}

/// Decodes one complete value from the start of `buf`. See the note above.
#[inline]
fn fast_parse_value(buf: &[u8]) -> Option<(Value, usize)> {
    // `combine`'s recursion count starts at 1.
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

    /// Decodes Redis replies out of an async byte stream.
    ///
    /// Harness-visible through the `doc(hidden)` re-export in `lib.rs` so the
    /// codec-level microbenchmarks can drive it directly.
    #[derive(Default)]
    pub struct ValueCodec {
        state: AnySendSyncPartialState,
        // Set while `combine` may hold state from an incomplete value. The
        // fast path is skipped until `combine` finishes that value, since the
        // buffer then starts in the middle of it.
        pub(super) partial: bool,
        // Progress through an incomplete reply that the fast path will decode
        // once it has fully arrived.
        pub(super) scan: Scan,
    }

    /// The outcome of scanning a buffer for the end of a reply.
    #[derive(Debug, PartialEq)]
    pub(super) enum ScanResult {
        /// The buffer holds the whole reply.
        Complete,
        /// The reply so far is well formed, but more bytes are needed.
        Incomplete,
        /// The reply is malformed, or uses something the scan doesn't
        /// handle. It's left to `combine`.
        Unsure,
    }

    /// A long line is left to `combine`, rather than rescanning it from its
    /// start on every read.
    const MAX_SCAN_LINE: usize = 4096;

    /// Finds where an incomplete reply ends without decoding it, so it can be
    /// decoded once, by the fast path, after all of it has arrived.
    ///
    /// Without this, an incomplete reply is decoded by the fast path, which
    /// fails and throws that work away, and then by `combine`. For large
    /// aggregates this made fragmented replies slower than with `combine`
    /// alone. The scan doesn't allocate, and resumes from where the previous
    /// read left off, so each byte is scanned once.
    ///
    /// The scan only decides *when* to try the fast path. If it gets
    /// anything wrong, the fast path declines and `combine` decodes the
    /// reply from its start, since no bytes are consumed while scanning.
    #[derive(Default)]
    pub(super) struct Scan {
        pub(super) active: bool,
        // Start of the first element not yet scanned.
        pos: usize,
        // How many more elements each open aggregate needs, innermost last.
        pending: Vec<usize>,
    }

    impl Scan {
        fn reset(&mut self) {
            self.active = false;
            self.pos = 0;
            self.pending.clear();
        }

        pub(super) fn resume(&mut self, buf: &[u8]) -> ScanResult {
            loop {
                let pos = self.pos;
                let Some(&marker) = buf.get(pos) else {
                    return ScanResult::Incomplete;
                };
                // The depth `fast_parse_at` would see for this element.
                let depth = self.pending.len() + 1;
                if depth > MAX_RECURSE_DEPTH {
                    return ScanResult::Unsure;
                }
                if find_crlf(buf, pos + 1).is_none() {
                    return if buf.len() - pos > MAX_SCAN_LINE {
                        ScanResult::Unsure
                    } else {
                        ScanResult::Incomplete
                    };
                }
                let children = match marker {
                    b'$' | b'=' | b'!' => {
                        let Some((len, start)) = fast_int(buf, pos + 1) else {
                            return ScanResult::Unsure;
                        };
                        if len < 0 && marker == b'$' {
                            self.pos = start;
                        } else {
                            let Some(end) = usize::try_from(len)
                                .ok()
                                .and_then(|len| start.checked_add(len)?.checked_add(2))
                            else {
                                return ScanResult::Unsure;
                            };
                            if buf.len() < end {
                                return ScanResult::Incomplete;
                            }
                            if &buf[end - 2..end] != b"\r\n" {
                                return ScanResult::Unsure;
                            }
                            self.pos = end;
                        }
                        0
                    }
                    b'*' | b'~' | b'>' | b'%' | b'|' => {
                        let Some((len, start)) = fast_int(buf, pos + 1) else {
                            return ScanResult::Unsure;
                        };
                        // Negative lengths: `Nil` (or an empty push) for the
                        // first three, declined by the fast path for the rest.
                        let children = match (marker, usize::try_from(len)) {
                            (b'*' | b'~' | b'>', Err(_)) => Some(0),
                            (b'%', Ok(len)) => len.checked_mul(2),
                            (b'|', Ok(len)) => len.checked_mul(2).and_then(|n| n.checked_add(1)),
                            (_, Ok(len)) => Some(len),
                            _ => None,
                        };
                        let Some(children) = children else {
                            return ScanResult::Unsure;
                        };
                        self.pos = start;
                        children
                    }
                    // Single-line values: `fast_parse_at` checks the line
                    // the same way it will when decoding.
                    _ => match fast_parse_at(buf, pos, depth) {
                        Some((_, next)) => {
                            self.pos = next;
                            0
                        }
                        None => return ScanResult::Unsure,
                    },
                };
                if children > 0 {
                    self.pending.push(children);
                    continue;
                }
                // An element is complete, which may complete its parents.
                loop {
                    let Some(left) = self.pending.last_mut() else {
                        return ScanResult::Complete;
                    };
                    *left -= 1;
                    if *left > 0 {
                        break;
                    }
                    self.pending.pop();
                }
            }
        }
    }

    impl ValueCodec {
        fn decode_stream(&mut self, bytes: &mut BytesMut, eof: bool) -> RedisResult<Option<Value>> {
            if !self.partial {
                // `Framed` calls `decode` again after draining the buffer. Don't
                // hand an empty buffer to `combine`, which would set `partial`
                // and push the next reply off the fast path.
                if bytes.is_empty() {
                    return Ok(None);
                }
                if !self.scan.active {
                    if let Some((value, consumed)) = fast_parse_value(&bytes[..]) {
                        bytes.advance(consumed);
                        return Ok(Some(value));
                    }
                    if !eof {
                        self.scan.active = true;
                        if self.scan.resume(bytes) == ScanResult::Incomplete {
                            return Ok(None);
                        }
                        self.scan.reset();
                    }
                } else if eof {
                    // `combine` alone would already have consumed the buffered
                    // bytes, before seeing EOF. Do the same, so that EOF in
                    // the middle of a reply is handled the same way.
                    self.scan.reset();
                    if let Some(value) = self.decode_combine(bytes, false)? {
                        return Ok(Some(value));
                    }
                } else {
                    match self.scan.resume(bytes) {
                        ScanResult::Incomplete => return Ok(None),
                        ScanResult::Complete => {
                            self.scan.reset();
                            if let Some((value, consumed)) = fast_parse_value(&bytes[..]) {
                                bytes.advance(consumed);
                                return Ok(Some(value));
                            }
                        }
                        ScanResult::Unsure => self.scan.reset(),
                    }
                }
            }
            self.decode_combine(bytes, eof)
        }

        fn decode_combine(
            &mut self,
            bytes: &mut BytesMut,
            eof: bool,
        ) -> RedisResult<Option<Value>> {
            let (opt, removed_len) = {
                let buffer = &bytes[..];
                let mut stream =
                    combine::easy::Stream(combine::stream::MaybePartialStream(buffer, !eof));
                match combine::stream::decode_tokio(value(None), &mut stream, &mut self.state) {
                    Ok(x) => x,
                    Err(err) => {
                        // Parse errors close the connection, so nothing will be
                        // resumed after this.
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
                    self.partial = false;
                    Ok(Some(result))
                }
                None => {
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
    use std::assert_matches;

    /// RESP-shaped random input for differential testing. Half of the inputs
    /// are encodings of valid value trees of every type, possibly truncated or
    /// with one corrupted byte. The other half are short sequences of type
    /// markers followed by plausible or random line contents.
    #[derive(Clone, Debug)]
    struct RespishBytes(Vec<u8>);

    fn arbitrary_frame(g: &mut quickcheck::Gen, out: &mut Vec<u8>, depth: usize) {
        use quickcheck::Arbitrary;
        let blob = |out: &mut Vec<u8>, marker: u8, data: &[u8]| {
            out.push(marker);
            out.extend_from_slice(format!("{}\r\n", data.len()).as_bytes());
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
        };
        let word = |g: &mut quickcheck::Gen| -> &'static [u8] {
            g.choose(&[
                &b"OK"[..],
                b"x",
                b"",
                b"invalidate",
                b"message",
                b"txt:a",
                b"ERR e",
            ])
            .unwrap()
        };
        let kind = u8::arbitrary(g) % if depth == 0 { 10 } else { 15 };
        match kind {
            0 => blob(out, b'$', word(g)),
            1 => blob(out, b'$', &Vec::<u8>::arbitrary(g)),
            2 => out.extend_from_slice(format!(":{}\r\n", i64::arbitrary(g)).as_bytes()),
            3 => {
                out.push(b'+');
                out.extend_from_slice(word(g));
                out.extend_from_slice(b"\r\n");
            }
            4 => out.extend_from_slice(b"-ERR boom\r\n"),
            5 => out.extend_from_slice(g.choose(&[&b"_\r\n"[..], b"$-1\r\n", b"*-1\r\n"]).unwrap()),
            6 => out.extend_from_slice(format!(",{}\r\n", f64::arbitrary(g)).as_bytes()),
            7 => out.extend_from_slice(g.choose(&[&b"#t\r\n"[..], b"#f\r\n"]).unwrap()),
            8 => out.extend_from_slice(format!("({}\r\n", i128::arbitrary(g)).as_bytes()),
            9 => blob(out, *g.choose(b"=!").unwrap(), word(g)),
            _ => {
                let marker = *g.choose(b"*%~>|").unwrap();
                let len = usize::arbitrary(g) % 4;
                let children = match marker {
                    b'%' => len * 2,
                    b'|' => len * 2 + 1,
                    _ => len,
                };
                out.extend_from_slice(format!("{}{len}\r\n", marker as char).as_bytes());
                for _ in 0..children {
                    arbitrary_frame(g, out, depth - 1);
                }
            }
        }
    }

    impl quickcheck::Arbitrary for RespishBytes {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            const MARKERS: &[u8] = b"+-:$*_%~,#!=(>|";
            const BODIES: &[&[u8]] = &[
                b"-1",
                b"0",
                b"1",
                b"2",
                b"3",
                b"5",
                b"t",
                b"f",
                b"OK",
                b"txt:a",
                b"1.5",
                b"invalidate",
                b"\xff",
            ];
            let mut out = Vec::new();
            if bool::arbitrary(g) {
                arbitrary_frame(g, &mut out, 3);
                match u8::arbitrary(g) % 4 {
                    0 => out.truncate(usize::arbitrary(g) % (out.len() + 1)),
                    1 => {
                        let i = usize::arbitrary(g) % out.len();
                        out[i] = u8::arbitrary(g);
                    }
                    _ => {}
                }
                return Self(out);
            }
            for _ in 0..(usize::arbitrary(g) % 8) + 1 {
                out.push(*g.choose(MARKERS).unwrap());
                if bool::arbitrary(g) {
                    out.extend_from_slice(g.choose(BODIES).unwrap());
                } else {
                    for _ in 0..(usize::arbitrary(g) % 6) {
                        out.push(u8::arbitrary(g));
                    }
                }
                if u8::arbitrary(g) % 8 != 0 {
                    out.extend_from_slice(b"\r\n");
                }
            }
            Self(out)
        }
    }

    fn assert_same_as_combine(input: &[u8]) {
        let fast = parse_redis_value(input);
        let combine = Parser::new().parse_value(input);
        match (&fast, &combine) {
            // Compare `Debug` output too, since `NaN != NaN`.
            (Ok(a), Ok(b)) => assert!(
                a == b || format!("{a:?}") == format!("{b:?}"),
                "values differ for {input:?}: {a:?} vs {b:?}"
            ),
            (Err(_), Err(_)) => {}
            _ => panic!("{input:?}: parse_redis_value {fast:?}, combine {combine:?}"),
        }
    }

    #[test]
    fn fast_path_never_diverges_from_combine_fuzz() {
        fn prop(input: RespishBytes) -> bool {
            assert_same_as_combine(&input.0);
            true
        }
        quickcheck::QuickCheck::new()
            .tests(100_000)
            .max_tests(100_000)
            .quickcheck(prop as fn(RespishBytes) -> bool);
    }

    #[test]
    fn fast_path_matches_combine() {
        let handled: &[&[u8]] = &[
            b"+OK\r\n",
            b"+hello world\r\n",
            b"+\r\n",
            b"-ERR bad\r\n",
            b"-MOVED 1234 127.0.0.1:6379\r\n",
            b":12345\r\n",
            b":-9\r\n",
            b": 7 \r\n",
            b"$5\r\nhello\r\n",
            b"$5\r\nhe\r\no\r\n",
            b"$0\r\n\r\n",
            b"$-1\r\n",
            b"$-2\r\n",
            b"_\r\n",
            b"*3\r\n$3\r\nfoo\r\n:1\r\n+OK\r\n",
            b"*-1\r\n",
            b"*0\r\n",
            b"*2\r\n*1\r\n:1\r\n$1\r\na\r\n",
            b"%1\r\n+a\r\n:1\r\n",
            b"%0\r\n",
            b"~2\r\n:1\r\n:2\r\n",
            b"~-1\r\n",
            b",3.14\r\n",
            b",inf\r\n",
            b"#t\r\n",
            b"#f\r\n",
            b"(12345\r\n",
            b">2\r\n$7\r\nmessage\r\n$1\r\nx\r\n",
            b">2\r\n+invalidate\r\n*1\r\n$1\r\nk\r\n",
            b">0\r\n",
            b">-1\r\n",
            b"=9\r\ntxt:hello\r\n",
            b"=9\r\nmkd:hello\r\n",
            b"=9\r\nxyz:hello\r\n",
            b"!9\r\nERR hello\r\n",
            b"|1\r\n+key\r\n+value\r\n:5\r\n",
            b"+OK\r\n+trailing\r\n",
        ];
        for input in handled {
            assert!(
                fast_parse_value(input).is_some(),
                "fast path should handle {input:?}"
            );
            assert_same_as_combine(input);
        }

        let declined: &[&[u8]] = &[
            // Incomplete.
            b"",
            b"$5\r\nhel",
            b"$5\r\nhello\r",
            b"*2\r\n:1\r\n",
            b"*1000000\r\n:1\r\n",
            b"+OK\r",
            b":12",
            // Not valid UTF-8 where `combine` requires it.
            b"_\xff\r\n",
            b"+\xff\r\n",
            b":\xff\r\n",
            b"-\xff\r\n",
            b"$\xff\r\n",
            b"*\xff\r\n",
            b",\xff\r\n",
            b"(\xff\r\n",
            // Malformed.
            b"$3\r\nhello\r\n",
            b":abc\r\n",
            b"#x\r\n",
            b",abc\r\n",
            b"=5\r\nhello\r\n",
            b"=-1\r\n",
            b"!-1\r\n",
            b"%-1\r\n",
            b"|-1\r\n",
            b">1\r\n:1\r\n",
            b">1\r\n+OK\r\n",
            b">1\r\n$1\r\n\xff\r\n",
            b"?\r\n",
        ];
        for input in declined {
            assert!(
                fast_parse_value(input).is_none(),
                "fast path should decline {input:?}"
            );
            assert_same_as_combine(input);
        }
    }

    #[test]
    fn fast_path_recursion_limit_matches_combine() {
        for depth in [
            MAX_RECURSE_DEPTH - 1,
            MAX_RECURSE_DEPTH,
            MAX_RECURSE_DEPTH + 1,
        ] {
            let mut input = b"*1\r\n".repeat(depth);
            input.extend_from_slice(b":1\r\n");
            assert_eq!(
                fast_parse_value(&input).is_some(),
                depth < MAX_RECURSE_DEPTH,
                "depth {depth}"
            );
            assert_same_as_combine(&input);
        }
    }

    /// Decodes a stream split at every possible chunk size, so that each frame
    /// is decoded by the fast path, by `combine` resuming across reads, or by a
    /// mix of both.
    #[cfg(feature = "aio")]
    #[test]
    fn codec_fast_path_and_partial_interleave() {
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let wire: &[u8] =
            b"$5\r\nhello\r\n+OK\r\n:42\r\n*2\r\n:1\r\n:2\r\n_\r\n$-1\r\n-ERR boom\r\n\
            %1\r\n+k\r\n,1.5\r\n>2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n";
        let expected = vec![
            Value::BulkString(b"hello".to_vec()),
            Value::Okay,
            Value::Int(42),
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            Value::Nil,
            Value::Nil,
            Value::ServerError(err_parser("ERR boom")),
            Value::Map(vec![(Value::SimpleString("k".into()), Value::Double(1.5))]),
            Value::Push {
                kind: PushKind::Invalidate,
                data: vec![Value::Array(vec![Value::BulkString(b"k".to_vec())])],
            },
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

    /// `Framed` calls `decode` once more after the buffer is drained. That
    /// call mustn't take the codec off the fast path for the next reply.
    #[cfg(feature = "aio")]
    #[test]
    fn codec_stays_on_fast_path_after_draining() {
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let mut codec = ValueCodec::default();
        let mut buf = BytesMut::from(&b"+OK\r\n"[..]);
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(Value::Okay));
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(!codec.partial);

        // An incomplete reply is scanned until it completes, then decoded by
        // the fast path.
        buf.extend_from_slice(b"$5\r\nhel");
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(codec.scan.active && !codec.partial);
        buf.extend_from_slice(b"lo\r\n");
        assert_eq!(
            codec.decode(&mut buf).unwrap(),
            Some(Value::BulkString(b"hello".to_vec()))
        );
        assert!(!codec.scan.active && !codec.partial);

        // One the scan can't vouch for goes to `combine`, and the codec
        // returns to the fast path once it completes.
        let long = vec![b'a'; 5000];
        buf.extend_from_slice(b"+");
        buf.extend_from_slice(&long);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(codec.partial && !codec.scan.active);
        buf.extend_from_slice(b"\r\n");
        assert_eq!(
            codec.decode(&mut buf).unwrap(),
            Some(Value::SimpleString(String::from_utf8(long).unwrap()))
        );
        assert!(!codec.partial);
    }

    /// Decodes `wire` split into reads of the given sizes, returning the
    /// values decoded and whether decoding failed. With `combine_only`, the
    /// fast path is never used.
    #[cfg(feature = "aio")]
    fn decode_in_reads(wire: &[u8], reads: &[usize], combine_only: bool) -> (Vec<Value>, bool) {
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let mut codec = ValueCodec::default();
        let mut buf = BytesMut::new();
        let mut got = Vec::new();
        let mut rest = wire;
        let mut reads = reads.iter().cycle();
        loop {
            let eof = rest.is_empty();
            if !eof {
                let n = (*reads.next().unwrap()).clamp(1, rest.len());
                buf.extend_from_slice(&rest[..n]);
                rest = &rest[n..];
            }
            loop {
                if combine_only {
                    codec.partial = true;
                }
                let result = if eof {
                    codec.decode_eof(&mut buf)
                } else {
                    codec.decode(&mut buf)
                };
                match result {
                    Ok(Some(value)) => got.push(value),
                    Ok(None) => break,
                    Err(_) => return (got, true),
                }
            }
            if eof {
                return (got, false);
            }
        }
    }

    /// Feeds random replies, some malformed, through the codec in random
    /// read sizes, and checks it decodes what `combine` alone does.
    ///
    /// `combine` alone isn't consistent on malformed input: an invalid type
    /// byte at the very end of a read is skipped, while one in the middle of
    /// a read is an error. So the codec must match `combine` given either the
    /// same reads or the whole input in one read.
    #[cfg(feature = "aio")]
    #[test]
    fn codec_never_diverges_from_combine_fuzz() {
        fn prop(frames: Vec<RespishBytes>, reads: Vec<u8>) -> bool {
            let wire: Vec<u8> = frames.iter().take(4).flat_map(|f| f.0.clone()).collect();
            let reads: Vec<usize> = reads.iter().map(|&n| usize::from(n % 32) + 1).collect();
            let reads = if reads.is_empty() {
                vec![wire.len()]
            } else {
                reads
            };
            let got = format!("{:?}", decode_in_reads(&wire, &reads, false));
            let same_reads = format!("{:?}", decode_in_reads(&wire, &reads, true));
            let one_read = format!("{:?}", decode_in_reads(&wire, &[usize::MAX], true));
            assert!(
                got == same_reads || got == one_read,
                "{wire:?} in reads of {reads:?}: codec {got}, combine {same_reads}, \
                combine in one read {one_read}"
            );
            true
        }
        quickcheck::QuickCheck::new()
            .tests(30_000)
            .max_tests(30_000)
            .quickcheck(prop as fn(Vec<RespishBytes>, Vec<u8>) -> bool);
    }

    /// A large reply arriving over many reads is scanned, never decoded by
    /// `combine`.
    #[cfg(feature = "aio")]
    #[test]
    fn codec_scans_fragmented_aggregate() {
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let mut wire = b"*3\r\n%1\r\n+k\r\n*2\r\n:1\r\n$3\r\nabc\r\n_\r\n".to_vec();
        wire.extend_from_slice(b">2\r\n$7\r\nmessage\r\n,1.5\r\n");
        let expected = parse_redis_value(&wire).unwrap();
        let mut codec = ValueCodec::default();
        let mut buf = BytesMut::new();
        let (last, init) = wire.split_last().unwrap();
        for &byte in init {
            buf.extend_from_slice(&[byte]);
            assert_eq!(codec.decode(&mut buf).unwrap(), None);
            assert!(codec.scan.active && !codec.partial);
        }
        buf.extend_from_slice(&[*last]);
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(expected));
        assert!(buf.is_empty() && !codec.scan.active && !codec.partial);
    }

    /// A malformed element inside an incomplete aggregate is still reported,
    /// as soon as it's buffered.
    #[cfg(feature = "aio")]
    #[test]
    fn codec_scan_reports_malformed_element() {
        use bytes::BytesMut;
        use tokio_util::codec::Decoder;

        let mut codec = ValueCodec::default();
        let mut buf = BytesMut::from(&b"*3\r\n:1\r\n"[..]);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(codec.scan.active);
        buf.extend_from_slice(b":nope\r\n");
        codec.decode(&mut buf).unwrap_err();
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
