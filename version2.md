# Announcing redis-rs & redis-test 2.0.0

With version 1.0.0 behind us, version 2.0.0 begins a new major series, with redis-test tracking it with the same version.

This document highlights the breaking changes in version 2.0.0. For a complete list of changes, see CHANGELOG.md. We appreciate feedback and bug reports — please open an issue for anything you encounter during migration. In order to get the newest version, please specify in your Cargo.toml file

```toml
redis = "2"
```

## Breaking Changes

### Several `struct`s and `enum`s are now marked `#[non_exhaustive]` (Breaking Change)

The `#[non_exhaustive]` will allow us to add fields without having to trigger version bumps, hence help with maintenance.

You can no longer build them directly with a [`StructExpression`](https://doc.rust-lang.org/reference/expressions/struct-expr.html#grammar-StructExpression).

When decomposing them, you need to adapt to ignore fields potentially added in the future by adding `, ..`.

The affected structs are:

* `BloomFilterDumpChunk`
* `CacheStatistics`
* `ClientTlsConfig`
* `Coord`
* `FlushAllOptions`
* `RadiusSearchResult`
* `SendError`
* `SentinelError`

### `ScanOptions::with_type` takes `ValueType` instead of `Into<String>` (Breaking Change)

To increase type safety, `ScanOptions::with_type` now takes a `ValueType`.
This allows to check at compile time that correct type names are used, and hence helps to avoid accidental typos.

It also gives more readable code, as `ValueType::CountMin` is easier to understand than `CMSk-TYPE`.

For types that lack a `ValueType` dedicated variant, any `String`-like value converts `into` `ValueType`.

**Migration:** Switch from `String`, `&str`, or `Into<String>` to `ValueType`

```rust
// Before:
let opts1 = ScanOptions::default().with_type("ReJSON-RL"); // Has a `ValueType`; we switch to it.
let opts2 = ScanOptions::default().with_type("your-custom-type"); // Does not have a `ValueType`; we convert into it.

// After:
let opts1 = ScanOptions::default().with_type(ValueType::JSON); // Use `ValueType`
let opts2 = ScanOptions::default().with_type("your-custom-type".into()); // Convert to `ValueType`
```

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

`Str` is a new UTF-8-guaranteed string backed by `bytes::Bytes`, exported as
`redis::Str`. It derefs to `&str`, so most code keeps working unchanged, and it
implements `FromRedisValue`/`ToRedisArgs`, so `con.get::<_, Str>("key")` reads a
string without copying the payload. `Str::from_static` and `From<String>` are
free; `From<&str>` copies, as does `Into<String>` when the `Str` is a shared slice
into a response — the common parser case. `Into<Bytes>` just moves the buffer.

`Str` otherwise carries the borrowed-string traits of the `String` it replaced:
`Borrow<str>`, `AsRef<str>`/`AsRef<[u8]>`, `Display`/`Debug`, `str`-consistent
`Hash`/`Eq`/`Ord` (so it is a `HashMap`/`BTreeMap` key you can look up by `&str`),
`From<&str>`/`String`/`&String`/`Cow<str>`/`char`, `FromStr`, and
`Into<String>`/`Into<Bytes>`/`Into<Vec<u8>>`. It also compares and orders directly
against `str`, `&str`, `String` and `Cow<str>` in either operand position.

`Str` and `Bytes` are immutable views, so payloads can no longer be edited in
place: convert out (`String::from(s)`, `b.to_vec()`), modify, convert back.
Traits that other crates implement for `Vec<u8>`/`String` — quickcheck's
`Arbitrary`, proptest strategies — no longer apply to a payload either, so
convert at that boundary too.

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
of a `Value::SimpleString` as a `&str` continue to work. Constructing and
byte-wise reading are where the compiler will stop you:

```rust
// Constructing a `Value` (tests, mocks, `redis-test` expectations):
Value::BulkString(b"key".to_vec())      // → Value::BulkString(b"key".to_vec().into())
Value::SimpleString("OK".to_string())   // → Value::SimpleString("OK".into())
// For literals, `Bytes::from_static(b"OK")` / `Str::from_static("OK")` skip the copy.
// `.as_bytes().into()` on a non-'static string will not borrow-check: use
// `Bytes::copy_from_slice(..)`, or move a `String` in with `.into()`.

// Reading a `BulkString` payload:
b.as_slice()                            // → b.as_ref()
b == b"OK"                              // → b.as_ref() == b"OK", or simply b == "OK"
takes_vec(b)                            // → takes_vec(b.into())
```

`.as_slice()` is worth calling out: on `Bytes` it resolves to an unstable method
and reports `error[E0658]: use of unstable library feature 'str_as_str'`, which
mentions neither `Bytes` nor the fix. You do not need nightly — use `.as_ref()`.

The (rarely used) re-exported `parse_redis_value_async` also changed shape as
part of the rewrite: its first argument is now a `&mut bytes::BytesMut` read
buffer instead of a `combine::stream::Decoder`. Call it with a `BytesMut` you
own and reuse across calls.

`redis`'s `bytes` feature is gone: `bytes` is now an unconditional dependency, so
the `FromRedisValue for bytes::Bytes` and `RedisWrite::bufmut_for_next_arg` impls
it used to gate are always available. Remove `"bytes"` from your `redis` features
— Cargo errors on features that no longer exist. (`redis-test` keeps its own
`bytes` feature; leave that one alone.) Code that needs to *name* `Bytes`/
`BytesMut` (rather than rely on `.into()` and `Deref<Target = [u8]>`) should add
`bytes = "1"` to its own `Cargo.toml`.

#### Why it's faster

The new parser allocates a small, constant number of times per response rather
than once per element, and avoids copying bulk-string payloads out of the read
buffer entirely on the async codec path. From `cargo bench -p redis --bench
bench_decode`; the "before" column comes from running that same benchmark file
against 1.x, where it compiles unchanged:

| Response                      | Allocations (before → after) |
| ----------------------------- | ---------------------------- |
| Single 1 MiB bulk string      | 154 → **2** (77×)            |
| Array of 5000 small bulks     | 7509 → **16** (469×)         |
| Array of 500 × 1 KiB bulks    | 2022 → **11** (184×)         |
| Array of 5000 simple strings  | 7152 → **16** (447×)         |
| Array of 1000 key/value pairs | 2933 → **13** (226×)         |

That is **77×–470× fewer heap allocations** on large multi-element responses.
Allocation counts are deterministic, so those numbers reproduce anywhere; the
timings that go with them are hardware-dependent and are in
[#2199](https://github.com/redis-rs/redis-rs/pull/2199) rather than here, where
they would go stale.

Small replies are a different story: the per-reply bookkeeping (one
reference-counted frame per response) is a fixed cost that the saved allocations
no longer pay for, so a single `+OK` or `:1` does not get faster and may be
slightly slower. Cloning a `Value` payload is now a reference-count bump rather
than a deep copy (cloning an aggregate still copies the `Vec` spine).

#### Trade-offs to be aware of

- **Peak memory is lower, but it moves into one contiguous allocation:** a large
  reply is parsed out of a single buffer that cannot be drained until the reply
  is complete, and its payloads are then slices of that buffer rather than fresh
  copies. Total peak is roughly one copy of the reply, where before it was
  roughly two (the old parser also had to buffer the whole reply, then allocated
  owned `Vec`s on top). The buffer itself, however, is now as large as the reply
  and has to be contiguous, where it used to stay near the read size — so a
  fragmented heap can fail an allocation that previously succeeded.
- **Memory retention:** every `Bytes`/`Str` leaf is a reference-counted slice of
  the buffer it arrived in, so holding on to one small field keeps that whole
  buffer alive — and because replies that arrive in the same read share one
  allocation, that can be more than just the reply you kept a field from. If you
  extract a small piece of a large response and store it long-term, copy it out
  (e.g. `Vec::from(&bytes[..])` or `s.to_string()`). Server errors are already
  copied out by the parser for exactly this reason — storing an error never pins
  a response buffer.
- **Extracting owned `Vec<u8>`/`String`:** conversions like
  `from_redis_value::<Vec<u8>>` now perform their copy at conversion time rather
  than at parse time (the total number of copies is unchanged — one). Code that
  reads payloads by reference performs no copy at all.
- **Lossy UTF-8 decoding is now strict:** verbatim strings and blob errors were
  previously decoded with `from_utf8_lossy`, silently substituting U+FFFD for
  invalid bytes. They are now validated, so a non-UTF-8 payload in one of those
  reply types fails the reply with a parse error instead of being corrupted.

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

### `ClientCertificate` holds the raw PKCS12 archive and is built through `ClientCertificate::new` (Breaking Change)

The `entra-id` feature moved from `azure_identity` 0.31 to 1.0, whose `ClientCertificateCredential` expects the decoded PKCS12 (PFX) archive rather than a base64-encoded one. As a result, `ClientCertificate` changed and the `base64_pkcs12: String` field became `pkcs12: Vec<u8>`.
Both fields are private now, so a certificate is built with `ClientCertificate::new`, and a password-protected archive gets its password through `ClientCertificate::set_password`.

`ClientCertificate` continues to implement `Debug`, but it prints neither the certificate data nor the password.

**Migration:** Replace the struct expression with `ClientCertificate::new` and hand it the decoded archive:

```rust
// Before:
let certificate_base64 = fs::read_to_string("path/to/base64_pkcs12_certificate")?;
let certificate = ClientCertificate {
    base64_pkcs12: certificate_base64,
    password: None,
};

// After:
let certificate = ClientCertificate::new(fs::read("path/to/pkcs12_certificate")?);
```

For a password-protected archive:

```rust
// Before:
let certificate = ClientCertificate {
    base64_pkcs12: certificate_base64,
    password: Some("your-password".to_string()),
};

// After:
let certificate =
    ClientCertificate::new(fs::read("path/to/pkcs12_certificate")?).set_password("your-password");
```

### `entra-id` uses `rustls` for the requests to the Entra ID token endpoint (Breaking Change)

The bump to `azure_identity` 1.0 and `azure_core` 1.1 switches their HTTP stack from `native-tls` to `rustls` together with the platform's certificate store. This concerns only the HTTPS requests that fetch the tokens from Entra ID. The TLS backend for the connection to the server is unaffected and is still selected through the `tls-native-tls` and `tls-rustls` features.

That has two consequences:

* Enabling `entra-id` alongside `tls-native-tls` links both TLS implementations into the binary.
* `rustls` validates certificates more strictly than OpenSSL, so fetching a token can start failing in environments that intercept TLS traffic.

**Migration:** No code changes are needed. In an environment that intercepts TLS traffic, make sure that the intercepting certificate authority is installed in the platform's certificate store and that its chain is one that `rustls` accepts.
