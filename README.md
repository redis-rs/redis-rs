# redis-rs

[![Build Status](https://travis-ci.org/mitsuhiko/redis-rs.svg?branch=master)](https://travis-ci.org/mitsuhiko/redis-rs)

Redis-rs is a high level redis library for rust.  It provides convenient access
to all redis functionality through a very flexible but low-level API.  It
uses a customizable type conversion trait so that any operation can return
results in just the type you are expecting.  This makes for a very pleasant
development experience.

The crate is called `redis` and you can depend on it via cargo:

```ini
[dependencies.redis]
git = "https://github.com/mitsuhiko/redis-rs.git"
```

Documentation on the library can be found at
[mitsuhiko.github.io/redis-rs](http://mitsuhiko.github.io/redis-rs/redis/).

## Basic Operation

To open a connection you need to create a client and then to fetch a
connection from it.  In the future there will be a connection pool for
those, currently each connection is separate and not pooled.

Many commands are implemented through the `Commands` trait but manual
command creation is also possible.

```rust
extern crate redis;
use redis::Commands;

fn main() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection();
    assert_eq!(con.set("my_key", 42i), Ok(()));
    assert_eq!(con.get("my_key"), Ok(42i));
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
