# Announcing redis-rs & redis-test 2.0.0

With version 1.0.0 behind us, version 2.0.0 begins a new major series, with redis-test tracking it with the same version.

This document highlights the breaking changes in version 2.0.0. For a complete list of changes, see CHANGELOG.md. We appreciate feedback and bug reports — please open an issue for anything you encounter during migration. In order to get the newest version, please specify in your Cargo.toml file

```toml
redis = "2"
```

## Breaking Changes

### `RedisServer::new...` got removed; use `RedisServerBuilder` instead (Breaking Change)

Over time `RedisServer::new...` methods grew in parameters and made them hard to use.
So they got removed in favor of `RedisServerBuilder`, which is now the recommended way to build `RedisServer` instances.

**Migration:** Switch from `RedisServer::new...` to `RedisServerBuilder`

`RedisServerBuilder::new()` starts a new builder.

It's fluent interface allows to set the needed parameters in a chaining manner (`.address(...).mtls(...).modules(...)`).

Call `.build()` to finally build the `RedisServer` with the set properties.

To refine the command before actually starting the server, use `refine_and_build(...)` instead.
This allows to fine tune the start command.
The passed `RedisServerCommand` comes with syntactic sugar to make things more readable (e.g. `arg2`).

```rust
// Before:

let server = RedisServer::new_with_addr_tls_modules_and_spawner(
    addr,
    None,
    None,
    true,
    None,
    &[], |cmd| {
        cmd.arg("--foo")
            .arg("value-foo")
            .arg("--bar")
            .arg("value-baz");
        cmd.spawn().unwrap()
    }
);

// After:
let server = RedisServerBuilder::new()
    .address(addr)
    .mtls(true)
    .refine_and_build(|cmd| {
        cmd.arg2("--foo", "value-foo")
            .arg2("--bar", "value-baz");
    });
```

### `Generic` typed commands have their `RV` moved from first to last parameter (Breaking Change)

Untyped commands (`Commands`, `AsyncCommands`) have the return value's type (`RV`) as last type parameter, while for typed commands (`TypedCommands`, `AsyncTypedCommands`) it was the first.

Now both typed and untyped commands have their return value's type as last type parameter.

**Migration:** Move the return type parameter to the last position, if you explicitly gave it.

```rust
// Before:
con.rpop::<Vec<String>, _>("foo", NonZeroUsize::new(1));
//         ^^^ RV as first type parameter

// After:
con.rpop::<_, Vec<String>>("foo", NonZeroUsize::new(1));
//            ^^^ RV as last type parameter
```

### TCP_NODELAY is now enabled by default (Breaking Change)

By default, Nagle's algorithm is now disabled on every TCP connection the crate creates (sync and async, plaintext and TLS). Previously it was left enabled, which serialized writes on a multiplexed connection to one per ACK round-trip under concurrency — measured at 39–68% lower throughput and roughly double the p50 latency on a real network (see [#2195](https://github.com/redis-rs/redis-rs/issues/2195) for the full evidence). Sequential request-response traffic is unaffected, and Redis clients in other ecosystems already ship with TCP_NODELAY enabled.

No API changed, but the wire behavior did: the client now emits more, smaller packets at moderate concurrency. Deployments close to packets-per-second limits (small cloud instances) or on metered/WAN links may prefer the old behavior.

**Migration:** nothing to do for most users — expect lower latency and higher multiplexed throughput. To keep Nagle's algorithm:

```rust
use redis::{IntoConnectionInfo, io::tcp::TcpSettings};

let info = "redis://127.0.0.1/".into_connection_info()?
    .set_tcp_settings(TcpSettings::default().set_nodelay(false));
```

### Zero-copy response parsing (Breaking Change)

`Value` now stores its textual and binary payloads in cheaply-cloneable,
reference-counted buffers instead of owned `Vec<u8>`/`String`:

- `Value::BulkString(Vec<u8>)` → `Value::BulkString(bytes::Bytes)`
- `Value::SimpleString(String)` → `Value::SimpleString(Str)`
- `Value::VerbatimString { text: String, .. }` → `{ text: Str, .. }`
- `Value::BigNumber(Vec<u8>)` → `Value::BigNumber(bytes::Bytes)` (unchanged under the `num-bigint` feature)
- `PushKind::Other(String)` / `VerbatimFormat::Unknown(String)` → `Str`
- Server error code/detail are now `Str` as well

`Str` is a new UTF-8-guaranteed string backed by `bytes::Bytes`. It derefs to
`&str`, so most code keeps working unchanged. Constructing one from `&str`/`String`
is cheap, and `Into<Bytes>` is free (it just moves the inner buffer). Converting
to an owned `String` (`Into<String>`) copies when the `Str` is a shared slice into
a response — the common parser case — so only reach for it when you need ownership.

The parser was rewritten to be **zero-copy**: instead of allocating a fresh
`Vec`/`String` for every element of a response, it parses into byte-range
offsets and then produces each leaf as a cheap reference-counted slice into the
response buffer. A response with many elements no longer performs a heap
allocation per element.

**Migration:** Most code that goes through `FromRedisValue`/`from_redis_value`
is unaffected. Code that matches on `Value` directly should:

```rust
// Before:
if let Value::BulkString(bytes) = v {
    let s = String::from_utf8(bytes)?;       // bytes: Vec<u8>
}
// After:
if let Value::BulkString(bytes) = v {
    let s = String::from_utf8(bytes.into())?; // bytes: Bytes  (or use &bytes as &[u8])
}
```

`Str` derefs to `&str`, so `match` arms that previously used the inner `String`
of a `Value::SimpleString` as a `&str` continue to work; constructing one now
takes a `Str` (e.g. `Value::SimpleString("OK".into())`).

The (rarely used) re-exported `parse_redis_value_async` also changed shape as
part of the rewrite: its first argument is now a `&mut bytes::BytesMut` read
buffer instead of a `combine::stream::Decoder`. Call it with a `BytesMut` you
own and reuse across calls.

### Why it's faster

The new parser allocates a small, constant number of times per response rather
than once per element, and avoids copying bulk-string payloads out of the read
buffer entirely on the async codec path. Parsing benchmarks
(`cargo bench -p redis --bench bench_decode`) comparing the new parser against
the previous `Vec`/`String`-based one:

| Response                     | Allocations (before → after) | Time (before → after)         |
| ---------------------------- | ---------------------------- | ----------------------------- |
| Single 1 MiB bulk string     | 154 → **2** (77×)            | 50.8 µs → 12.3 µs (**4.1×**)  |
| Array of 5000 small bulks    | 7509 → **16** (469×)         | 548 µs → 367 µs (1.5×)        |
| Array of 500 × 1 KiB bulks   | 2022 → **11** (184×)         | 160.7 µs → 45.6 µs (**3.5×**) |
| Array of 5000 simple strings | 7152 → **16** (447×)         | 411 µs → 263 µs (1.6×)        |
| Map of 1000 key/value pairs  | 2933 → **13** (226×)         | 206 µs → 149 µs (1.4×)        |

In short: **1.4×–4.1× faster parsing and 10×–470× fewer heap allocations**, with
the largest wins on responses that contain many elements. Cloning a `Value` (or
any `Str`/`BulkString` inside it) is now a reference-count bump rather than a
deep copy.

### Trade-offs to be aware of

- **Memory retention:** every `Bytes`/`Str` leaf is a reference-counted slice of
  the response it arrived in, so holding on to one small field keeps that whole
  response's buffer alive (one buffer per reply, not per connection). If you
  extract a small piece of a large response and store it long-term, copy it out
  (e.g. `Vec::from(&bytes[..])` or `str.to_string()`). Server errors are already
  copied out by the parser for exactly this reason — storing an error never pins
  a response buffer.
- **Extracting owned `Vec<u8>`/`String`:** conversions like
  `from_redis_value::<Vec<u8>>` now perform their copy at conversion time rather
  than at parse time (the total number of copies is unchanged — one). Code that
  reads payloads by reference performs no copy at all.
- **Fragmented arrival of huge multi-element responses:** the parser re-parses
  the buffered prefix when a response arrives across many reads (parse state is
  not kept between reads, which is what makes the zero-copy offsets sound).
  For typical responses over typical networks this is a handful of cheap
  attempts; a response with a very large number of elements arriving in many
  small fragments does more repeated work than before. Bulk-string payloads are
  skipped in O(1) regardless of size.

### Removed `zinterstore_*` and `zunionstore_*` commands in favor of `zinterstore`, `zinterstore_with_weights`, `zunionstore`, and `zunionstore_with_weights`

The following commands have been removed:

- `zinterstore_min`, `zinterstore_max`
- `zinterstore_weights`, `zinterstore_min_weights`, `zinterstore_max_weights`
- `zunionstore_min`, `zunionstore_max`
- `zunionstore_weights`, `zunionstore_min_weights`, `zunionstore_max_weights`

Adding more options to these commands would have caused an exponential explosion of variants. Instead, there are now two variants for each command:

- `zinterstore(dstkey, keys, options)` / `zunionstore(dstkey, keys, options)` — keys without weights
- `zinterstore_with_weights(dstkey, keys_and_weights, options)` / `zunionstore_with_weights(dstkey, keys_and_weights, options)` — keys paired with weights as `&[(key, weight)]`

The `SortedSetOperationOptions` struct carries only the optional `AGGREGATE` modifier and defaults to `SUM`.

**Migration:**

```rust
use redis::{Commands, SortedSetOperationOptions, Aggregate};

// Before:
con.zinterstore("out", &["zset1", "zset2"])?;
con.zinterstore_min("out", &["zset1", "zset2"])?;
con.zinterstore_weights("out", &[("zset1", 2), ("zset2", 3)])?;
con.zinterstore_min_weights("out", &[("zset1", 2), ("zset2", 3)])?;

// After:
con.zinterstore("out", &["zset1", "zset2"], SortedSetOperationOptions::default())?;
con.zinterstore("out", &["zset1", "zset2"], SortedSetOperationOptions::default().aggregate(Aggregate::Min))?;
con.zinterstore_with_weights("out", &[("zset1", 2), ("zset2", 3)], SortedSetOperationOptions::default())?;
con.zinterstore_with_weights("out", &[("zset1", 2), ("zset2", 3)], SortedSetOperationOptions::default().aggregate(Aggregate::Min))?;
```

The same pattern applies to `zunionstore` and `zunionstore_with_weights`.

### `cmd_iter` yields `CmdRef` instead of `&Cmd` (Breaking Change)

**Most users can upgrade to 2.0.0 with no code changes.** The flattening is an internal representation change; the pipeline builder API (`cmd`, `arg`, `add_command`, `ignore`, `query`, `query_async`, `exec`, …) is unchanged. The only adjustments are needed if you iterate a pipeline's commands or call `with_capacity` directly.

Because a pipeline no longer owns a `Vec<Cmd>`, there is no `&Cmd` to hand out. [`Pipeline::cmd_iter`] and [`ClusterPipeline::cmd_iter`] now yield `CmdRef<'_>`, a lightweight, `Copy` view that borrows directly into the pipeline's shared buffers — iterating a pipeline's commands performs no per-command allocation.

`CmdRef` is intentionally opaque so that the underlying storage can keep evolving. It exposes the read-only accessors you previously reached for on `&Cmd`, including `args_iter()`, `arg_idx()`, `data()`, `cursor()`, `is_no_response()`, and `get_packed_command()`. If you genuinely need an owned `Cmd`, call `to_cmd()`.

**Migration:** Update code that iterates a pipeline's commands. Most call sites only need to drop a borrow or call an accessor:

```rust
// Before:
for cmd in pipe.cmd_iter() {
    let name = cmd.arg_idx(0);
    // cmd: &Cmd
}

// After:
for cmd in pipe.cmd_iter() {
    let name = cmd.arg_idx(0);
    // cmd: CmdRef<'_> — same read accessors, Copy
}
```

If you stored or passed the `&Cmd` onward and need an owned value:

```rust
// Before:
let owned: Vec<Cmd> = pipe.cmd_iter().cloned().collect();

// After:
let owned: Vec<Cmd> = pipe.cmd_iter().map(|cmd| cmd.to_cmd()).collect();
```

### `Pipeline::with_capacity` is replaced by `reserve_for_*` methods (Breaking Change)

[`Pipeline::with_capacity`] and [`ClusterPipeline::with_capacity`] have been removed. A flattened pipeline stores its commands across three buffers (commands, arguments, and argument bytes), and a single capacity number no longer maps cleanly onto them. Rather than force you to estimate all three up front, pre-allocation is now opt-in per buffer via chainable methods, so you reserve only the dimensions you actually have a number for:

```rust
pub fn reserve_for_commands(&mut self, additional: usize) -> &mut Self
pub fn reserve_for_args(&mut self, additional: usize) -> &mut Self
pub fn reserve_for_data(&mut self, additional: usize) -> &mut Self // argument bytes
```

**Migration:** Replace `with_capacity` with the reservations you can estimate:

```rust
// Before:
let mut pipe = redis::Pipeline::with_capacity(16); // 16 commands

// After: reserve whichever buffers you have an estimate for
let mut pipe = redis::pipe();
pipe.reserve_for_commands(16).reserve_for_args(48);
```

`Pipeline::new()` and `pipe()` are unchanged.
