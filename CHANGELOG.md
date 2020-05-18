# Changelog

## [0.16.0](https://github.com/mitsuhiko/redis-rs/compare/0.15.1...0.16.0) - 2020-05-10

**Fixes and improvements**

* Reduce dependencies without async IO ([#266](https://github.com/mitsuhiko/redis-rs/pull/266))
* Add an afl fuzz target ([#274](https://github.com/mitsuhiko/redis-rs/pull/274))
* Updated to combine 4 and avoid async dependencies for sync-only ([#272](https://github.com/mitsuhiko/redis-rs/pull/272))
    * **BREAKING CHANGE**: The parser type now only persists the buffer and takes the Read instance in `parse_value`
* Implement a connection manager for automatic reconnection ([#278](https://github.com/mitsuhiko/redis-rs/pull/278))
* Add async-std support ([#281](https://github.com/mitsuhiko/redis-rs/pull/281))
* Fix key extraction for some stream commands ([#283](https://github.com/mitsuhiko/redis-rs/pull/283))
* Add asynchronous PubSub support ([#287](https://github.com/mitsuhiko/redis-rs/pull/287))
* Add Redis Streams commands ([#162](https://github.com/mitsuhiko/redis-rs/pull/319))

### Breaking changes

#### Changes to the `Parser` type ([#272](https://github.com/mitsuhiko/redis-rs/pull/272))

The parser type now only persists the buffer and takes the Read instance in `parse_value`.
`redis::parse_redis_value` is unchanged and continues to work.


Old:

```rust
let mut parser = Parser::new(bytes);
let result = parser.parse_value();
```

New:

```rust
let mut parser = Parser::new();
let result = parser.pase_value(bytes);
```

## [0.15.1](https://github.com/mitsuhiko/redis-rs/compare/0.15.0...0.15.1) - 2020-01-15

**Fixes and improvements**

* Fixed the `r2d2` feature (re-added it) ([#265](https://github.com/mitsuhiko/redis-rs/pull/265))

## [0.15.0](https://github.com/mitsuhiko/redis-rs/compare/0.14.0...0.15.0) - 2020-01-15

**Fixes and improvements**

* Added support for redis cluster ([#239](https://github.com/mitsuhiko/redis-rs/pull/239))

## [0.14.0](https://github.com/mitsuhiko/redis-rs/compare/0.13.0...0.14.0) - 2020-01-08

**Fixes and improvements**

* Fix the command verb being sent to redis for `zremrangebyrank` ([#240](https://github.com/mitsuhiko/redis-rs/pull/240))
* Add `get_connection_with_timeout` to Client ([#243](https://github.com/mitsuhiko/redis-rs/pull/243))
* **Breaking change:**  Add Cmd::get, Cmd::set and remove PipelineCommands ([#253](https://github.com/mitsuhiko/redis-rs/pull/253))
* Async-ify the API ([#232](https://github.com/mitsuhiko/redis-rs/pull/232))
* Bump minimal required Rust version to 1.39 (required for the async/await API)
* Add async/await examples ([#261](https://github.com/mitsuhiko/redis-rs/pull/261), [#263](https://github.com/mitsuhiko/redis-rs/pull/263))
* Added support for PSETEX and PTTL commands. ([#259](https://github.com/mitsuhiko/redis-rs/pull/259))

### Breaking changes

#### Add Cmd::get, Cmd::set and remove PipelineCommands ([#253](https://github.com/mitsuhiko/redis-rs/pull/253))

If you are using pipelines and were importing the `PipelineCommands` trait you can now remove that import
and only use the `Commands` trait.

Old:

```rust
use redis::{Commands, PipelineCommands};
```

New:

```rust
use redis::Commands;
```

## [0.13.0](https://github.com/mitsuhiko/redis-rs/compare/0.12.0...0.13.0) - 2019-10-14

**Fixes and improvements**

* **Breaking change:** rename `parse_async` to `parse_redis_value_async` for consistency ([ce59cecb](https://github.com/mitsuhiko/redis-rs/commit/ce59cecb830d4217115a4e74e38891e76cf01474)).
* Run clippy over the entire codebase ([#238](https://github.com/mitsuhiko/redis-rs/pull/238))
* **Breaking change:** Make `Script#invoke_async` generic over `aio::ConnectionLike` ([#242](https://github.com/mitsuhiko/redis-rs/pull/242))

### BREAKING CHANGES

#### Rename `parse_async` to `parse_redis_value_async` for consistency ([ce59cecb](https://github.com/mitsuhiko/redis-rs/commit/ce59cecb830d4217115a4e74e38891e76cf01474)).

If you used `redis::parse_async` before, you now need to change this to `redis::parse_redis_value_async`
or import the method under the new name: `use redis::parse_redis_value_async`.

#### Make `Script#invoke_async` generic over `aio::ConnectionLike` ([#242](https://github.com/mitsuhiko/redis-rs/pull/242))

`Script#invoke_async` was changed to be generic over `aio::ConnectionLike` in order to support wrapping a `SharedConnection` in user code.
This required adding a new generic parameter to the method, causing an error when the return type is defined using the turbofish syntax.

Old:

```rust
redis::Script::new("return ...")
  .key("key1")
  .arg("an argument")
  .invoke_async::<String>()
```

New:

```rust
redis::Script::new("return ...")
  .key("key1")
  .arg("an argument")
  .invoke_async::<_, String>()
```

## [0.12.0](https://github.com/mitsuhiko/redis-rs/compare/0.11.0...0.12.0) - 2019-08-26

**Fixes and improvements**

* **Breaking change:** Use `dyn` keyword to avoid deprecation warning ([#223](https://github.com/mitsuhiko/redis-rs/pull/223))
* **Breaking change:** Update url dependency to v2 ([#234](https://github.com/mitsuhiko/redis-rs/pull/234))
* **Breaking change:** (async) Fix `Script::invoke_async` return type error ([#233](https://github.com/mitsuhiko/redis-rs/pull/233))
* Add `GETRANGE` and `SETRANGE` commands ([#202](https://github.com/mitsuhiko/redis-rs/pull/202))
* Fix `SINTERSTORE` wrapper name, it's now correctly `sinterstore` ([#225](https://github.com/mitsuhiko/redis-rs/pull/225))
* Allow running `SharedConnection` with any other runtime ([#229](https://github.com/mitsuhiko/redis-rs/pull/229))
* Reformatted as Edition 2018 code ([#235](https://github.com/mitsuhiko/redis-rs/pull/235))

### BREAKING CHANGES

#### Use `dyn` keyword to avoid deprecation warning ([#223](https://github.com/mitsuhiko/redis-rs/pull/223))

Rust nightly deprecated bare trait objects.
This PR adds the `dyn` keyword to all trait objects in order to get rid of the warning.
This bumps the minimal supported Rust version to [Rust 1.27](https://blog.rust-lang.org/2018/06/21/Rust-1.27.html).

#### Update url dependency to v2 ([#234](https://github.com/mitsuhiko/redis-rs/pull/234))

We updated the `url` dependency to v2. We do expose this on our public API on the `redis::parse_redis_url` function. If you depend on that, make sure to also upgrade your direct dependency.

#### (async) Fix Script::invoke_async return type error ([#233](https://github.com/mitsuhiko/redis-rs/pull/233))

Previously, invoking a script with a complex return type would cause the following error:

```
Response was of incompatible type: "Not a bulk response" (response was string data('"4b98bef92b171357ddc437b395c7c1a5145ca2bd"'))
```

This was because the Future returned when loading the script into the database returns the hash of the script, and thus the return type of `String` would not match the intended return type.

This commit adds an enum to account for the different Future return types.


## [0.11.0](https://github.com/mitsuhiko/redis-rs/compare/0.11.0-beta.2...0.11.0) - 2019-07-19

This release includes all fixes & improvements from the two beta releases listed below.
This release contains breaking changes.

**Fixes and improvements**

* (async) Fix performance problem for SharedConnection ([#222](https://github.com/mitsuhiko/redis-rs/pull/222))

## [0.11.0-beta.2](https://github.com/mitsuhiko/redis-rs/compare/0.11.0-beta.1...0.11.0-beta.2) - 2019-07-14

**Fixes and improvements**

* (async) Don't block the executor from shutting down ([#217](https://github.com/mitsuhiko/redis-rs/pull/217))

## [0.11.0-beta.1](https://github.com/mitsuhiko/redis-rs/compare/0.10.0...0.11.0-beta.1) - 2019-05-30

**Fixes and improvements**

* (async) Simplify implicit pipeline handling ([#182](https://github.com/mitsuhiko/redis-rs/pull/182))
* (async) Use `tokio_sync`'s channels instead of futures ([#195](https://github.com/mitsuhiko/redis-rs/pull/195))
* (async) Only allocate one oneshot per request ([#194](https://github.com/mitsuhiko/redis-rs/pull/194))
* Remove redundant BufReader when parsing ([#197](https://github.com/mitsuhiko/redis-rs/pull/197))
* Hide actual type returned from async parser ([#193](https://github.com/mitsuhiko/redis-rs/pull/193))
* Use more performant operations for line parsing ([#198](https://github.com/mitsuhiko/redis-rs/pull/198))
* Optimize the command encoding, see below for **breaking changes** ([#165](https://github.com/mitsuhiko/redis-rs/pull/165))
* Add support for geospatial commands ([#130](https://github.com/mitsuhiko/redis-rs/pull/130))
* (async) Add support for async Script invocation ([#206](https://github.com/mitsuhiko/redis-rs/pull/206))

### BREAKING CHANGES

#### Renamed the async module to aio ([#189](https://github.com/mitsuhiko/redis-rs/pull/189))

`async` is a reserved keyword in Rust 2018, so this avoids the need to write `r#async` in it.

Old code:

```rust
use redis::async::SharedConnection;
```

New code:

```rust
use redis::aio::SharedConnection;
```

#### The trait `ToRedisArgs` was changed ([#165](https://github.com/mitsuhiko/redis-rs/pull/165))

`ToRedisArgs` has been changed to take take an instance of `RedisWrite` instead of `Vec<Vec<u8>>`. Use the `write_arg` method instead of `Vec::push`.

#### Minimum Rust version is now 1.26 ([#165](https://github.com/mitsuhiko/redis-rs/pull/165))

Upgrade your compiler.
`impl Iterator` is used, requiring a more recent version of the Rust compiler.

#### `iter` now takes `self` by value ([#165](https://github.com/mitsuhiko/redis-rs/pull/165))

`iter` now takes `self` by value instead of cloning `self` inside the method.

Old code:

```rust
let mut iter : redis::Iter<isize> = cmd.arg("my_set").cursor_arg(0).iter(&con).unwrap();
```

New code:

```rust
let mut iter : redis::Iter<isize> = cmd.arg("my_set").cursor_arg(0).clone().iter(&con).unwrap();
```

(The above line calls `clone()`.)

#### A mutable connection object is now required ([#148](https://github.com/mitsuhiko/redis-rs/pull/148))

We removed the internal usage of `RefCell` and `Cell` and instead require a mutable reference, `&mut ConnectionLike`,
on all command calls.

Old code:

```rust
let client = redis::Client::open("redis://127.0.0.1/")?;
let con = client.get_connection()?;
redis::cmd("SET").arg("my_key").arg(42).execute(&con);
```

New code:

```rust
let client = redis::Client::open("redis://127.0.0.1/")?;
let mut con = client.get_connection()?;
redis::cmd("SET").arg("my_key").arg(42).execute(&mut con);
```

Due to this, `transaction` has changed. The callback now also receives a mutable reference to the used connection.

Old code:

```rust
let client = redis::Client::open("redis://127.0.0.1/").unwrap();
let con = client.get_connection().unwrap();
let key = "the_key";
let (new_val,) : (isize,) = redis::transaction(&con, &[key], |pipe| {
    let old_val : isize = con.get(key)?;
    pipe
        .set(key, old_val + 1).ignore()
        .get(key).query(&con)
})?;
```

New code:

```rust
let client = redis::Client::open("redis://127.0.0.1/").unwrap();
let mut con = client.get_connection().unwrap();
let key = "the_key";
let (new_val,) : (isize,) = redis::transaction(&mut con, &[key], |con, pipe| {
    let old_val : isize = con.get(key)?;
    pipe
        .set(key, old_val + 1).ignore()
        .get(key).query(&con)
})?;
```

#### Remove `rustc-serialize` feature ([#200](https://github.com/mitsuhiko/redis-rs/pull/200))

We removed serialization to/from JSON. The underlying library is deprecated for a long time.

Old code in `Cargo.toml`:

```
[dependencies.redis]
version = "0.9.1"
features = ["with-rustc-json"]
```

There's no replacement for the feature.
Use [serde](https://serde.rs/) and handle the serialization/deserialization in your own code.

#### Remove `with-unix-sockets` feature ([#201](https://github.com/mitsuhiko/redis-rs/pull/201))

We removed the Unix socket feature. It is now always enabled.
We also removed auto-detection.

Old code in `Cargo.toml`:

```
[dependencies.redis]
version = "0.9.1"
features = ["with-unix-sockets"]
```

There's no replacement for the feature. Unix sockets will continue to work by default.

## [0.10.0](https://github.com/mitsuhiko/redis-rs/compare/0.9.1...0.10.0) - 2019-02-19

* Fix handling of passwords with special characters (#163)
* Better performance for async code due to less boxing (#167)
    * CAUTION: redis-rs will now require Rust 1.26
* Add `clear` method to the pipeline (#176)
* Better benchmarking (#179)
* Fully formatted source code (#181)

## [0.9.1](https://github.com/mitsuhiko/redis-rs/compare/0.9.0...0.9.1) (2018-09-10)

* Add ttl command

## [0.9.0](https://github.com/mitsuhiko/redis-rs/compare/0.8.0...0.9.0) (2018-08-08)

Some time has passed since the last release.
This new release will bring less bugs, more commands, experimental async support and better performance.

Highlights:

* Implement flexible PubSub API (#136)
* Avoid allocating some redundant Vec's during encoding (#140)
* Add an async interface using futures-rs (#141)
* Allow the async connection to have multiple in flight requests (#143)

The async support is currently experimental.

## [0.8.0](https://github.com/mitsuhiko/redis-rs/compare/0.7.1...0.8.0) (2016-12-26)

* Add publish command

## [0.7.1](https://github.com/mitsuhiko/redis-rs/compare/0.7.0...0.7.1) (2016-12-17)

* Fix unix socket builds
* Relax lifetimes for scripts

## [0.7.0](https://github.com/mitsuhiko/redis-rs/compare/0.6.0...0.7.0) (2016-07-23)

* Add support for built-in unix sockets

## [0.6.0](https://github.com/mitsuhiko/redis-rs/compare/0.5.4...0.6.0) (2016-07-14)

* feat: Make rustc-serialize an optional feature (#96)

## [0.5.4](https://github.com/mitsuhiko/redis-rs/compare/0.5.3...0.5.4) (2016-06-25)

* fix: Improved single arg handling (#95)
* feat: Implement ToRedisArgs for &String (#89)
* feat: Faster command encoding (#94)

## [0.5.3](https://github.com/mitsuhiko/redis-rs/compare/0.5.2...0.5.3) (2016-05-03)

* fix: Use explicit versions for dependencies
* fix: Send `AUTH` command before other commands
* fix: Shutdown connection upon protocol error
* feat: Add `keys` method
* feat: Possibility to set read and write timeouts for the connection
