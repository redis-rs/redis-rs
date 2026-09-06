# Announcing redis-rs & redis-test 2.0.0

With version 1.0.0 behind us, version 2.0.0 begins a new major series, with redis-test tracking it with the same version.

This document highlights the breaking changes in version 2.0.0. For a complete list of changes, see CHANGELOG.md. We appreciate feedback and bug reports — please open an issue for anything you encounter during migration. In order to get the newest version, please specify in your Cargo.toml file

```toml
redis = "2"
```

## Breaking Changes

### Tests: The path to Redis modules is only picked up from `REDISRS_MODULE_...` environment variables (Breaking Change)

The modules get picked only from the following environment variables:

| EnvVar Name | Description |
| --- | --- |
| `REDISRS_MODULE_BLOOM_PATH` | Path to the `bloom` module |
| `REDISRS_MODULE_JSON_PATH` | Path to the `json` module |

**Migration:**

* Switch from `REDIS_RS_REDIS_JSON_PATH` to `REDISRS_MODULE_JSON_PATH`
* Switch from `REDISRS_REDIS_JSON_PATH` to `REDISRS_MODULE_JSON_PATH`
* Switch from `REDISRS_REDIS_BLOOM_PATH` to `REDISRS_MODULE_BLOOM_PATH`

### `StreamInfoStreamReplyWithIdempotency` got folded into `StreamInfoStreamReply`

`StreamInfoStreamReply` had the idempotency fields added.

This removed the need for `StreamInfoStreamReplyWithIdempotency` and the `xinfo_stream_with_idempotency` command, which therefore got dropped.

**Migration:**

* Switch from `StreamInfoStreamReplyWithIdempotency` to `StreamInfoStreamReply`, and
* Switch from `xinfo_stream_with_idempotency` to `xinfo_stream`.

### Several `struct`s and `enum`s are now marked `#[non_exhaustive]` (Breaking Change)

The `#[non_exhaustive]` will allow us to add fields without having to trigger version bumps, hence help with maintenance.

You can no longer build them directly with a [`StructExpression`](https://doc.rust-lang.org/reference/expressions/struct-expr.html#grammar-StructExpression).

When decomposing them, you need to adapt to ignore fields potentially added in the future by adding `, ..`.

The affected structs are:

* `AclInfo`
* `BloomFilterDumpChunk`
* `CacheStatistics`
* `ClientTlsConfig`
* `Coord`
* `FlushAllOptions`
* `PushInfo`
* `RadiusSearchResult`
* `RandomReplicaStrategy`
* `SendError`
* `SentinelError`
* `StreamAutoClaimReply`
* `StreamClaimReply`
* `StreamId`
* `StreamIdempotencyMode`
* `StreamInfoConsumer`
* `StreamInfoConsumersReply`
* `StreamInfoGroup`
* `StreamInfoGroupsReply`
* `StreamInfoStreamReply`
* `StreamKey`
* `StreamPendingCountReply`
* `StreamPendingData`
* `StreamPendingId`
* `StreamRangeReply`
* `StreamReadReply`
* `TlsCertificates`

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

### `ValueComparison` no longer forces utf8 (Minor breaking Change)

As [this ticket](https://github.com/redis-rs/redis-rs/issues/2315) demonstrates, `ValueComparison` performed unnecessary UTF8 string coercion. Now this behavior was removed. This is a technically breaking change, while for most users this should be an improvement in correctness.

### `(Async)TypedCommands::hmget`, `hget_ex`, `zscore_multiple`, and `geo_hash` return optional values in lists (Breaking Change)

Following [issue #2311](https://github.com/redis-rs/redis-rs/issues/2311), the same issue was addressed across Redis commands `HMGET`, `HGETEX`, `ZMSCORE`, and `GEOHASH`, which return `nil` elements for missing keys or missing fields/members.
Previously, `TypedCommands` and `AsyncTypedCommands` defined these with non-optional inner element types (or an incorrect outer option in `zscore_multiple`), causing any missing field or member to fail with a type conversion error.

Their return types are now updated to:
- `hmget`: `Vec<Option<String>>` (was `Vec<String>`)
- `hget_ex`: `Vec<Option<String>>` (was `Vec<String>`)
- `zscore_multiple`: `Vec<Option<f64>>` (was `Option<Vec<f64>>`)
- `geo_hash`: `Vec<Option<String>>` (was `Vec<String>`)

### Out-of-range integer and double replies are rejected instead of wrapping (Breaking Change)

`FromRedisValue` for the integer types converted `Value::Int` and `Value::Double` with `as`, which wraps around for integers and saturates for floats, turning `NaN` into zero. Both did so silently, so a reply outside the target type's range produced a wrong number rather than an error.

The most visible case is `TTL`, which replies `-2` for a missing key and `-1` for a key without an expiry:

```rust
let ttl: u64 = con.ttl("key")?; // 18446744073709551615, no error
```

The same value arriving as a string already errored, so the outcome depended on how the reply happened to be encoded. RESP3 makes the same split reachable for scores, since `ZSCORE` replies with a double under RESP3 and a bulk string under RESP2.

Every arm now rejects a value the requested type cannot represent, so a reply converts to the same result whether the server sent it as a number or as a string. `Value::Int` goes through `try_from`, and for the integer types `Value::Double` accepts only whole numbers in range, which also rejects `NaN`, the infinities, and fractional values such as `1.9` that previously truncated to `1`. Whole numbers in range, including the exact type boundaries, convert exactly as before, and `f32`/`f64` are untouched.

**Migration:** Handle the error, or parse into a type that covers the full range of the reply.

```rust
// Before: silently wrapped to 18446744073709551615
let ttl: u64 = con.ttl("key")?;

// After: use a signed type that can represent the -1 and -2 sentinels
let ttl: i64 = con.ttl("key")?;
match ttl {
    -2 => { /* key does not exist */ }
    -1 => { /* key exists but has no expiry */ }
    secs => { /* seconds remaining */ }
}
```

### Map types accept the RESP3 array-of-pairs shape (Minor breaking Change)

RESP3 returns `ZRANGE ... WITHSCORES` and its relatives as an array of `[member, score]` pairs where RESP2 returned one flat array. `Vec<(K, V)>` accepted both, because `from_redis_values` checks `Value::is_collection_of_len`, but the map types only accepted the flat one, so the same command on the same data parsed under RESP2 and errored under RESP3. `as_map_iter` and `into_map_iter` now recognise the nested shape using that same check.

For the affected commands this turns an error into a map, which needs no migration. The breaking part is narrower: a key or value type that can itself absorb a two element array now sees the pairs split rather than combined.

**Migration:** Expect one entry per pair for such a type.

```rust
// value: [[a, 1], [b, 2]]

// Before:
// {["a", "1"]: ["b", "2"]}

// After:
// {["a"]: ["1"], ["b"]: ["2"]}
let map: HashMap<Vec<String>, Vec<String>> = con.zrange_withscores("z", 0, -1)?;
```
