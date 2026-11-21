//! Differential checks shared by the fuzz targets and `reproduce`. Each check
//! panics if the fast path decodes anything differently from `combine`.

use bytes::BytesMut;
use redis::{Parser, RedisResult, Value, ValueCodec, parse_redis_value};
use tokio_util::codec::Decoder;

/// `Value` compared through `Debug`, since `Double(NaN) != Double(NaN)`.
fn same(a: &Value, b: &Value) -> bool {
    a == b || format!("{a:?}") == format!("{b:?}")
}

/// `parse_redis_value` (fast path, then `combine`) must agree with `Parser`
/// (`combine` only): the same value, or an error from both.
pub fn check_parse(data: &[u8]) -> RedisResult<Value> {
    let fast = parse_redis_value(data);
    let combine = Parser::new().parse_value(data);
    match (&fast, &combine) {
        (Ok(a), Ok(b)) => assert!(same(a, b), "parse_redis_value {a:?}, combine {b:?}"),
        (Err(_), Err(_)) => {}
        _ => panic!("parse_redis_value {fast:?}, combine {combine:?}"),
    }
    fast
}

/// The read sizes encoded by the first byte of a codec input: `0` is one
/// read of everything, anything else seeds a sequence of sizes from 1 to 32.
fn read_sizes(seed: u8, len: usize) -> Vec<usize> {
    if seed == 0 {
        return vec![len.max(1)];
    }
    let mut state = u32::from(seed).wrapping_mul(0x9E37_79B9) | 1;
    std::iter::repeat_with(|| {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        (state % 32) as usize + 1
    })
    .take(len.max(1))
    .collect()
}

/// What one `decode` or `decode_eof` call returned.
#[derive(Debug, PartialEq)]
enum Call {
    Value,
    Pending,
    Failed,
}

/// The result of `decode_in_reads`.
#[derive(Debug)]
pub struct Decoded {
    pub values: Vec<Value>,
    pub failed: bool,
    /// The outcome of every call, in order. Parity here means a value was
    /// returned on exactly the same read.
    calls: Vec<Call>,
}

/// Decodes `wire` in reads of the given sizes, the way `Framed` does,
/// finishing with `decode_eof`.
fn decode_in_reads(mut codec: ValueCodec, wire: &[u8], reads: &[usize]) -> Decoded {
    let mut buf = BytesMut::new();
    let mut out = Decoded {
        values: Vec::new(),
        failed: false,
        calls: Vec::new(),
    };
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
            let result = if eof {
                codec.decode_eof(&mut buf)
            } else {
                codec.decode(&mut buf)
            };
            match result {
                Ok(Some(value)) => {
                    out.calls.push(Call::Value);
                    out.values.push(value);
                }
                Ok(None) => {
                    out.calls.push(Call::Pending);
                    break;
                }
                Err(_) => {
                    out.calls.push(Call::Failed);
                    out.failed = true;
                    return out;
                }
            }
        }
        if eof {
            return out;
        }
    }
}

/// `ValueCodec` must decode a stream, split into reads as encoded by the
/// first byte of `data`, the way a `combine`-only codec does.
///
/// `combine` alone isn't consistent on malformed input: an invalid type byte
/// that ends a read is skipped, while one in the middle of a read is an
/// error. So the codec must match `combine` given either the same reads or
/// the whole stream in one read. When the stream decodes without error, the
/// codec must also return each value on the same call as `combine` does, so
/// that no value, server errors included, is delayed.
pub fn check_codec(data: &[u8]) -> Decoded {
    let Some((&seed, wire)) = data.split_first() else {
        return Decoded {
            values: Vec::new(),
            failed: false,
            calls: Vec::new(),
        };
    };
    let reads = read_sizes(seed, wire.len());
    let got = decode_in_reads(ValueCodec::default(), wire, &reads);
    let same_reads = decode_in_reads(ValueCodec::combine_only(), wire, &reads);
    let one_read = decode_in_reads(ValueCodec::combine_only(), wire, &[usize::MAX]);
    let matches = |want: &Decoded| {
        got.failed == want.failed
            && got.values.len() == want.values.len()
            && got.values.iter().zip(&want.values).all(|(a, b)| same(a, b))
    };
    assert!(
        matches(&same_reads) || matches(&one_read),
        "reads {reads:?}: codec {got:?}, combine {same_reads:?}, combine in one read {one_read:?}"
    );
    if !one_read.failed {
        assert_eq!(
            got.calls, same_reads.calls,
            "reads {reads:?}: a value was delayed or early"
        );
    }
    got
}
