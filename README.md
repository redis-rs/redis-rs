# redis-rs

[![Rust](https://github.com/redis-rs/redis-rs/actions/workflows/rust.yml/badge.svg)](https://github.com/redis-rs/redis-rs/actions/workflows/rust.yml)
[![crates.io](https://img.shields.io/crates/v/redis.svg)](https://crates.io/crates/redis)
[![Chat](https://img.shields.io/discord/976380008299917365?logo=discord)](https://discord.gg/WHKcJK9AKP)

Redis-rs is a high level redis library for Rust. It provides convenient access
to all Redis functionality through a very flexible but low-level API. It
uses a customizable type conversion trait so that any operation can return
results in just the type you are expecting. This makes for a very pleasant
development experience.

The crate is called `redis` and you can depend on it via cargo:

```ini
[dependencies]
redis = "0.25.4"
```

Documentation on the library can be found at
[docs.rs/redis](https://docs.rs/redis).

**Note: redis-rs requires at least Rust 1.60.**

## Basic Operation

To open a connection you need to create a client and then to fetch a
connection from it. In the future there will be a connection pool for
those, currently each connection is separate and not pooled.

Many commands are implemented through the `Commands` trait but manual
command creation is also possible.

```rust
use redis::Commands;

fn fetch_an_integer() -> redis::RedisResult<isize> {
    // connect to redis
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;
    // throw away the result, just make sure it does not fail
    let _ : () = con.set("my_key", 42)?;
    // read back the key and return it.  Because the return value
    // from the function is a result for integer this will automatically
    // convert into one.
    con.get("my_key")
}
```

Variables are converted to and from the Redis format for a wide variety of types
(`String`, num types, tuples, `Vec<u8>`). If you want to use it with your own types,
you can implement the `FromRedisValue` and `ToRedisArgs` traits, or derive it with the
[redis-macros](https://github.com/daniel7grant/redis-macros/#json-wrapper-with-redisjson) crate.

## Async support

To enable asynchronous clients, enable the relevant feature in your Cargo.toml,
`tokio-comp` for tokio users or `async-std-comp` for async-std users.

```
# if you use tokio
redis = { version = "0.25.4", features = ["tokio-comp"] }

# if you use async-std
redis = { version = "0.25.4", features = ["async-std-comp"] }
```

## TLS Support

To enable TLS support, you need to use the relevant feature entry in your Cargo.toml.
Currently, `native-tls` and `rustls` are supported.

To use `native-tls`:

```
redis = { version = "0.25.4", features = ["tls-native-tls"] }

# if you use tokio
redis = { version = "0.25.4", features = ["tokio-native-tls-comp"] }

# if you use async-std
redis = { version = "0.25.4", features = ["async-std-native-tls-comp"] }
```

To use `rustls`:

```
redis = { version = "0.25.4", features = ["tls-rustls"] }

# if you use tokio
redis = { version = "0.25.4", features = ["tokio-rustls-comp"] }

# if you use async-std
redis = { version = "0.25.4", features = ["async-std-rustls-comp"] }
```

With `rustls`, you can add the following feature flags on top of other feature flags to enable additional features:

-   `tls-rustls-insecure`: Allow insecure TLS connections
-   `tls-rustls-webpki-roots`: Use `webpki-roots` (Mozilla's root certificates) instead of native root certificates

then you should be able to connect to a redis instance using the `rediss://` URL scheme:

```rust
let client = redis::Client::open("rediss://127.0.0.1/")?;
```

To enable insecure mode, append `#insecure` at the end of the URL:

```rust
let client = redis::Client::open("rediss://127.0.0.1/#insecure")?;
```

**Deprecation Notice:** If you were using the `tls` or `async-std-tls-comp` features, please use the `tls-native-tls` or `async-std-native-tls-comp` features respectively.

## Cluster Support

Support for Redis Cluster can be enabled by enabling the `cluster` feature in your Cargo.toml:

`redis = { version = "0.25.4", features = [ "cluster"] }`

Then you can simply use the `ClusterClient`, which accepts a list of available nodes. Note
that only one node in the cluster needs to be specified when instantiating the client, though
you can specify multiple.

```rust
use redis::cluster::ClusterClient;
use redis::Commands;

fn fetch_an_integer() -> String {
    let nodes = vec!["redis://127.0.0.1/"];
    let client = ClusterClient::new(nodes).unwrap();
    let mut connection = client.get_connection().unwrap();
    let _: () = connection.set("test", "test_data").unwrap();
    let rv: String = connection.get("test").unwrap();
    return rv;
}
```

Async Redis Cluster support can be enabled by enabling the `cluster-async` feature, along
with your preferred async runtime, e.g.:

`redis = { version = "0.25.4", features = [ "cluster-async", "tokio-std-comp" ] }`

```rust
use redis::cluster::ClusterClient;
use redis::AsyncCommands;

async fn fetch_an_integer() -> String {
    let nodes = vec!["redis://127.0.0.1/"];
    let client = ClusterClient::new(nodes).unwrap();
    let mut connection = client.get_async_connection().await.unwrap();
    let _: () = connection.set("test", "test_data").await.unwrap();
    let rv: String = connection.get("test").await.unwrap();
    return rv;
}
```

## JSON Support

Support for the RedisJSON Module can be enabled by specifying "json" as a feature in your Cargo.toml.

`redis = { version = "0.25.4", features = ["json"] }`

Then you can simply import the `JsonCommands` trait which will add the `json` commands to all Redis Connections (not to be confused with just `Commands` which only adds the default commands)

```rust
use redis::Client;
use redis::JsonCommands;
use redis::RedisResult;
use redis::ToRedisArgs;

// Result returns Ok(true) if the value was set
// Result returns Err(e) if there was an error with the server itself OR serde_json was unable to serialize the boolean
fn set_json_bool<P: ToRedisArgs>(key: P, path: P, b: bool) -> RedisResult<bool> {
    let client = Client::open("redis://127.0.0.1").unwrap();
    let connection = client.get_connection().unwrap();

    // runs `JSON.SET {key} {path} {b}`
    connection.json_set(key, path, b)?
}

```

To parse the results, you'll need to use `serde_json` (or some other json lib) to deserialize
the results from the bytes. It will always be a `Vec`, if no results were found at the path it'll
be an empty `Vec`. If you want to handle deserialization and `Vec` unwrapping automatically,
you can use the `Json` wrapper from the
[redis-macros](https://github.com/daniel7grant/redis-macros/#json-wrapper-with-redisjson) crate.

## Development

To test `redis` you're going to need to be able to test with the Redis Modules, to do this
you must set the following environment variable before running the test script

-   `REDIS_RS_REDIS_JSON_PATH` = The absolute path to the RedisJSON module (Either `librejson.so` for Linux or `librejson.dylib` for MacOS).

-   Please refer to this [link](https://github.com/RedisJSON/RedisJSON) to access the RedisJSON module:

<!-- As support for modules are added later, it would be wise to update this list -->

If you want to develop on the library there are a few commands provided
by the makefile:

To build:

    $ make

To test:

    $ make test

To run benchmarks:

    $ make bench

To build the docs (require nightly compiler, see [rust-lang/rust#43781](https://github.com/rust-lang/rust/issues/43781)):

    $ make docs

We encourage you to run `clippy` prior to seeking a merge for your work. The lints can be quite strict. Running this on your own workstation can save you time, since Travis CI will fail any build that doesn't satisfy `clippy`:

    $ cargo clippy --all-features --all --tests --examples -- -D clippy::all -D warnings

To run fuzz tests with afl, first install cargo-afl (`cargo install -f afl`),
then run:

    $ make fuzz

If the fuzzer finds a crash, in order to reproduce it, run:

    $ cd afl/<target>/
    $ cargo run --bin reproduce -- out/crashes/<crashfile>
