# Zero-copy response parsing: `Value` backed by `Bytes`/`Str`

## Summary

Reworks `Value` to store its textual and binary payloads in cheaply-cloneable, reference-counted buffers (`bytes::Bytes` and a new UTF-8 `Str` type) instead of owned `Vec<u8>`/`String`, and rewrites the RESP parser to be **zero-copy**. Parsing a response no longer allocates once per element — leaves are produced as refcount-bumped slices into the response buffer.

Result: **1.4×–4.1× faster parsing and 10×–470× fewer heap allocations**, with the biggest wins on multi-element responses (large arrays/maps). Cloning a `Value` is now a refcount bump rather than a deep copy.

## Motivation

The old `Value` owned its data (`BulkString(Vec<u8>)`, `SimpleString(String)`, …), so the parser allocated and memcpy'd a fresh buffer for **every leaf** of a response. For replies with many elements (`MGET`, `HGETALL`, `SCAN`, stream reads), that's thousands of small allocations per response, and every `Value::clone()` is a deep copy. Backing the payloads with `Bytes` lets the parser slice the read buffer instead of copying it, and makes clones O(1).

## What changed

### `Value` payloads → `Bytes` / `Str`
- `BulkString(Vec<u8>)` → `BulkString(bytes::Bytes)`
- `SimpleString(String)` → `SimpleString(Str)`
- `VerbatimString { text: String, .. }` → `{ text: Str, .. }`
- `BigNumber(Vec<u8>)` → `BigNumber(bytes::Bytes)` (the `num-bigint` variant is unchanged)
- `PushKind::Other(String)` / `VerbatimFormat::Unknown(String)` → `Str`
- `ServerError` code/detail: `ArcStr` → `Str`

### New `Str` type
A UTF-8-guaranteed string backed by `bytes::Bytes`:
- `Deref<Target = str>` (+ `AsRef<str>`/`AsRef<[u8]>`/`Borrow<str>`), `Display`, `Debug`, `PartialEq`/`Eq`/`Hash`
- `From<&str>`/`From<String>`, `Into<String>`/`Into<Bytes>`, `const fn from_static`
- so most code that treated the inner value as `&str` keeps compiling unchanged.

### Zero-copy parser rewrite (`parser.rs`)
- The `combine`-based parser now produces an intermediate `ValueRange` whose leaves hold `Range<usize>` offsets into the buffer rather than copied data.
- After a complete value is parsed, the consumed prefix is frozen into a single `Bytes` and `materialize()` turns each range into a cheap `Bytes::slice`. The async `ValueCodec` path performs **no copy at all**; the borrowed-`&[u8]` entry points (`parse_redis_value`) copy the input once and then slice it.
- All entry points (`ValueCodec`, `Parser::parse_value`, `parse_redis_value`, `parse_redis_value_async`) were unified onto this path. Parsing is single-pass with fresh state, so offsets are always valid for the buffer being materialized.

### Dependency / features
- `bytes` is now a mandatory dependency (it was optional behind `aio`). A no-op `bytes` feature is kept so existing `#[cfg(feature = "bytes")]` gates and downstream `features = ["bytes"]` still work.
- `aio` now enables `tokio/io-util` (the async reader uses `AsyncReadExt::read_buf`).

### Consumer migration
Updated `FromRedisValue` impls, accessors, `Debug`, cluster handling, commands (`acl`, `geo`, `streams`, `hotkeys`), pubsub, `redis-test`, and the test suite for the new payload types. Cluster topology/`NodeAddress` deliberately **stay `ArcStr`** — they're long-lived and deduplicated, so a compact interned string is better than pinning the response buffer.

## Benchmarks

New bench: `cargo bench -p redis --bench bench_decode` (no server required). It reports wall-clock time (criterion) and deterministic heap-allocation counts (counting global allocator), comparing the new parser against the previous `Vec`/`String`-based one (pre-refactor baseline):

| Response | Allocations (before → after) | Time (before → after) |
|---|---|---|
| Single 1 MiB bulk string | 154 → **2** (77×) | 50.8 µs → 12.3 µs (**4.1×**) |
| Array of 5000 small bulks | 7509 → **16** (469×) | 548 µs → 367 µs (1.5×) |
| Array of 500 × 1 KiB bulks | 2022 → **11** (184×) | 160.7 µs → 45.6 µs (**3.5×**) |
| Array of 5000 simple strings | 7152 → **16** (447×) | 411 µs → 263 µs (1.6×) |
| Map of 1000 key/value pairs | 2933 → **13** (226×) | 206 µs → 149 µs (1.4×) |

The old parser allocated ~one heap block per leaf; the new one allocates a small constant regardless of element count.

## Breaking changes & migration

Most code going through `FromRedisValue`/`from_redis_value` is unaffected. Code that matches on `Value` directly needs minor updates:

```rust
// Before:
if let Value::BulkString(bytes) = v {
    let s = String::from_utf8(bytes)?;        // bytes: Vec<u8>
}
// After:
if let Value::BulkString(bytes) = v {
    let s = String::from_utf8(bytes.into())?; // bytes: Bytes  (or use &bytes as &[u8])
}
```

`Str` derefs to `&str`, so reads keep working; construction takes a `Str` (e.g. `Value::SimpleString("OK".into())`).

Other notes:
- `parse_redis_value_async` now takes `&mut BytesMut` (the reusable read buffer) instead of a `combine::stream::Decoder`.
- Verbatim strings / blob errors are now validated as UTF-8 (previously lossy) — they are UTF-8 in practice.
- `Value::BulkString` `Debug` output for binary data uses `Bytes`'s formatting.

## Testing

- `cargo build --workspace --all-features --tests` and `--no-default-features` — clean
- `cargo clippy --workspace --all-features --all-targets` — clean (`-D warnings`)
- 200 lib unit tests + 38 `test_types` conversion tests pass
- The `partial_io_parse` quickcheck test (arbitrary values through a partial/would-block async reader) passes, validating offset correctness across fragmented reads
- New `bench_decode` benchmark added

## Files

~25 files, +944/−373. Core: `redis/src/types.rs` (`Str` + `Value`), `redis/src/parser.rs` (rewrite), `redis/src/errors/server_error.rs`; plus consumer updates across cluster/commands/aio, `redis-test`, tests, and `redis/benches/bench_decode.rs`.
