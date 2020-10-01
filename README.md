# redis-rs

[![Build Status](https://travis-ci.org/mitsuhiko/redis-rs.svg?branch=master)](https://travis-ci.org/mitsuhiko/redis-rs)
[![crates.io](http://meritbadge.herokuapp.com/redis)](https://crates.io/crates/redis)

Redis-rs is a high level redis library for Rust.  It provides convenient access
to all Redis functionality through a very flexible but low-level API.  It
uses a customizable type conversion trait so that any operation can return
results in just the type you are expecting.  This makes for a very pleasant
development experience.

The crate is called `redis` and you can depend on it via cargo:

```ini
[dependencies]
redis = "0.17.0"
```

Documentation on the library can be found at
[docs.rs/redis](https://docs.rs/redis).

**Note: redis-rs requires at least Rust 1.39.**

## Basic Operation

To open a connection you need to create a client and then fetch a
connection from it.

Many commands are implemented through the `Commands` trait but manual
command creation is also possible.

```rust
extern crate redis;
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

## R2D2 Support 

Opening a new database connection every time one 
is needed is both inefficient and can lead to resource exhaustion under 
high traffic conditions. To prevent this, you can use a connection pool, 
which will maintain a set of open connections to a database, handing them 
out for repeated use. You can use the popular `r2d2` connection pool by enabling the 
`r2d2` feature:
```
redis = { version = "0.17.0", features = ["r2d2"] }
```
You can then use `r2d2::Pool::builder()` to create a connection pool:

```rust
let client = redis::Client::open("redis://127.0.0.1/").unwrap()
let pool = r2d2::Pool::builder().build(client)
```

Retrieving a connection from the pool is done through the `get()` method.
The connection can then be used as usual:

```rust
let mut conn = pool.get().unwrap()

let _: () = conn.set("KEY", "VALUE").unwrap();
let val: String = conn.get("KEY").unwrap();
```

[Click here for a complete example](https://github.com/mitsuhiko/redis-rs/blob/master/examples/r2d2.rs).

## Async support

To enable asynchronous clients a feature for the underlying feature need to be activated.

```toml
# if you use tokio
redis = { version = "0.17.0", features = ["tokio-comp"] }
# for the multiplexed client
redis = { version = "0.17.0", features = ["tokio-rt-core"] }

# if you use async-std
redis = { version = "0.17.0", features = ["async-std-comp"] }
```

Note that you cannot use r2d2 with an asynchronous client because r2d2 is a synchronous
connection pool implementation. For an asynchronous connection pool, look into [`deadpool-redis`](https://crates.io/crates/deadpool-redis) or [`bb8-redis`](https://crates.io/crates/bb8-redis/0.3.1)

## TLS Support

To enable TLS support, you need to use the relevant feature entry in your Cargo.toml.

```
redis = { version = "0.17.0", features = ["tls"] }

# if you use tokio
redis = { version = "0.17.0", features = ["tls", "tokio-tls-comp"] }

# if you use async-std
redis = { version = "0.17.0", features = ["tls", "async-std-tls-comp"] }
```

then you should be able to connect to a redis instance using the `redis://` URL scheme:

```rust
let client = redis::Client::open("rediss://127.0.0.1/")?;
```

## Cluster Support

Cluster mode can be used by specifying "cluster" as a features entry in your Cargo.toml.

`redis = { version = "0.17.0", features = [ "cluster"] }`

Then you can simply use the `ClusterClient` which accepts a list of available nodes.

```rust
use redis::cluster::ClusterClient;
use redis::Commands;

fn fetch_an_integer() -> String {
    // connect to redis
    let nodes = vec!["redis://127.0.0.1/"];
    let client = ClusterClient::open(nodes).unwrap();
    let mut connection = client.get_connection().unwrap();
    let _: () = connection.set("test", "test_data").unwrap();
    let rv: String = connection.get("test").unwrap();
    return rv;
}
```

## Development

If you want to develop on the library there are a few commands provided
by the makefile:

To build:

    $ make

To test:

    $ make test

To run benchmarks:

    $ make bench

To build the docs:

    $ make docs

We encourage you to run `clippy` prior to seeking a merge for your work.  The lints can be quite strict.  Running this on your own workstation can save you time, since Travis CI will fail any build that doesn't satisfy `clippy`:

    $ cargo clippy --all-features --all --tests --examples -- -D clippy::all -D warnings

To run fuzz tests with afl, first install cargo-afl (`cargo install -f afl`),
then run:

    $ make fuzz

If the fuzzer finds a crash, in order to reproduce it, run:

    $ cd afl/<target>/
    $ cargo run --bin reproduce -- out/crashes/<crashfile>
