# Announcing redis-rs & redis-test 1.0.0

The first commit to redis-rs was uploaded almost 12 years ago, and now we're pleased to announce that the crate ~will soon~ has hit version 1, with redis-test tracking it with the same version.

The basic design has held up well over the years. We've improved significant parts of the underlying code while preserving backward compatibility for most common use cases. After 3 months of alpha releases and 2 more months of release-candidate releases, the version is now officially ready for production use.

This document highlights the breaking changes and new features in version 1.0.0. For a complete list of changes, see CHANGELOG.md. We appreciate feedback and bug reports. Please open an issue for anything you encounter during migration. in order to get the newest version, please specify in your Cargo.toml file 
```toml
redis = "1.0.0"
```

I'd like to thank everyone who contributed to the release of V1, with code contributions, testing and feedback! 
@kudlatyamroth, @LazyMechanic, @Totodore, @peterwalkley, @StefanPalashev, @Marwes, @Nekidev, @FalkWoldmann, @QuarticCat, @tottoto, @kamulos, thanks :)

**Most users can upgrade to 1.0.0 with minimal code changes.** The primary adjustments needed are:
- Handling `Result` types when iterating
- Adding `ToSingleRedisArg` trait bounds for custom types
- Updating `FromRedisValue` implementations to accept owned values

## Safe iteration (Breaking Change)

Previously, iterators silently ignored deserialization failures, which could lead to data loss without any indication of errors. In version 1.0.0, iterators now return `RedisResult<T>` instead of `T`, making errors explicit and recoverable.

The `safe_iterators` feature flag (introduced in [#1641](https://github.com/redis-rs/redis-rs/pull/1641)) is now the default and only behavior ([#1660](https://github.com/redis-rs/redis-rs/pull/1660)).

**Migration:** Update iterator usage to handle `Result` types:
```rust
// Before:
for value in iter {
    process(value);
}

// After:
for value in iter {
    let value = value?; // or handle the error explicitly
    process(value);
}
```

## ToSingleRedisArg trait (Breaking Change)

Previously, `ToRedisArgs` didn't distinguish between single and multiple values, allowing users to accidentally pass collections (arrays, slices, etc.) to commands expecting a single argument. This caused runtime errors from Redis rather than compile-time errors.

Version 1.0.0 introduces the `ToSingleRedisArg` marker trait ([#1792](https://github.com/redis-rs/redis-rs/pull/1792)) to catch these errors at compile time. This trait should only be implemented for types that serialize to exactly one Redis value.

**Migration:** If you have custom `ToRedisArgs` implementations that produce a single value, also implement `ToSingleRedisArg`:
```rust
impl ToSingleRedisArg for MyType {}
```

Most built-in types already implement this trait.

## Default connection and response timeouts (Breaking Change)

Async connections now have default timeouts set to prevent indefinite hangs. Connection timeout defaults to 1 second and response timeout defaults to 500 milliseconds ([#1686](https://github.com/redis-rs/redis-rs/pull/1686)).

**Migration:** If you need to disable timeouts or adjust them, configure via `AsyncConnectionConfig` or `ConnectionManagerConfig`:
```rust
let config = redis::AsyncConnectionConfig::new()
    .set_connection_timeout(None)
    .set_response_timeout(None);

let mut con = client.get_multiplexed_async_connection_with_config(&config).await?;
```

## Better visibility of typed traits (Recommended)

Recent Rust compiler changes caused confusing error messages like `the trait FromRedisValue is not implemented for !` when using commands with `()` return types ([#1730](https://github.com/redis-rs/redis-rs/issues/1730)).

To improve the developer experience, we introduced the `TypedCommands` and `AsyncTypedCommands` traits ([#1480](https://github.com/redis-rs/redis-rs/pull/1480)) with concrete return types for most commands. In version 1.0.0, the documentation now prominently features these traits.

**Recommendation:** Use `TypedCommands` instead of `Commands` for a better experience:
```rust
// Recommended: TypedCommands with inferred types
use redis::TypedCommands;
con.set("key", 42)?;  // Returns () automatically
let value = con.get_int("key")?;  // Returns Option<isize>

// Still supported: Commands with explicit type annotations
use redis::Commands;
let _: () = con.set("key", 42)?;
let value: isize = con.get("key")?;
```

This is not a breaking change—both approaches remain valid.

## More efficient deserialization (Breaking Change)

`FromRedisValue` now takes an owned value instead of a reference ([#1784](https://github.com/redis-rs/redis-rs/pull/1784)), enabling zero-copy deserialization in many cases and improving performance by avoiding unnecessary allocations.

**Migration:** Update `FromRedisValue` implementations:
```rust
// Before:
impl FromRedisValue for MyType {
    fn from_redis_value(v: &Value) -> RedisResult<Self> { ... }
}

// After:
impl FromRedisValue for MyType {
    fn from_redis_value(v: Value) -> Result<Self, redis::ParsingError> { ... }
}
```

Additionally, deserialization errors now use a new error type ([#1661](https://github.com/redis-rs/redis-rs/pull/1661)) that can be easily created from strings, simplifying error handling in custom implementations.

## Better error messages (Potentially Breaking)

Multiple improvements ([#1661](https://github.com/redis-rs/redis-rs/pull/1661), [#1706](https://github.com/redis-rs/redis-rs/pull/1706), [#1718](https://github.com/redis-rs/redis-rs/pull/1718), [#1755](https://github.com/redis-rs/redis-rs/pull/1755)) make error messages more complete and precise.

**Migration:** If your code parses error messages or matches on specific `ErrorKind` text, you may need to update it to handle the new error structure and phrasing. Most users who only propagate errors with `?` are unaffected.

## Deprecated features removal (Breaking Change)

Version 1.0.0 removes previously deprecated features ([#1662](https://github.com/redis-rs/redis-rs/pull/1662)), most notably support for the `async-std` runtime.

**Migration:** If you're using `async-std`, switch to the `smol` runtime:
```toml
# Before:
redis = { version = "0.x", features = ["async-std-comp"] }

# After:
redis = { version = "1.0", features = ["smol-comp"] }
```

## Async fire-and-forget (New Feature)

Version 1.0.0 adds support for sending commands without waiting for responses ([#1756](https://github.com/redis-rs/redis-rs/pull/1756)), improving throughput in scenarios where responses aren't needed, such as publishing pub/sub messages.

```rust
redis::cmd("SET")
    .arg("key")
    .arg(b"foo")
    .set_no_response(true)
    .exec_async(&mut con)
    .await?;
```

This is a new feature with no migration required.

## Partial failures in pipeline (New Feature)

Version 1.0.0 adds support for allowing pipelined requests to interleaved failures and successes ([#1865](https://github.com/redis-rs/redis-rs/pull/1865)), allowing users to use values from successful calls and retrying the failed calls.

```rust
let results: Vec<redis::RedisResult<i32>> = redis::pipe()
    .set("key_a", 1)
    .hset("key_a", "field", "val") // This will fail (WRONGTYPE)
    .get("key_a")
    .ignore_errors()
    .query_async(&mut con)
    .await?; // the `?` operator will only trigger on connection errors, not on error responses returned from the server
```

This is a new feature with no migration required.
