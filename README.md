# redis-rs

Redis-rs isaa high level redis library for rust.  I provides convenient access
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

Queries are then performed through the `execute` and `query` methods.

```rust,no_run
extern crate redis;

fn main() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection().unwrap();
    redis::cmd("SET").arg("my_key").arg(42i).execute(&con);
    assert_eq!(redis::cmd("GET").arg("my_key").query(&con), Ok(42i));
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
