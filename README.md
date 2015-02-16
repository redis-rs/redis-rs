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

Note that this library always tracks Rust nightly until Rust will
release a proper stable version.

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

fn fetch_an_integer() -> redis::RedisResult<int> {
    // connect to redis
    let client = try!(redis::Client::open("redis://127.0.0.1/"));
    let con = try!(client.get_connection());
    // throw away the result, just make sure it does not fail
    let _ : () = try!(con.set("my_key", 42i));
    // read back the key and return it.  Because the return value
    // from the function is a result for integer this will automatically
    // convert into one.
    con.get("my_key")
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

The tests need a redis server.  By default they assume that a redis
server can be started with the `redis-server` command.  If this
command is not available locally, you can run provide a redis instance
on `localhost:38991` and run the tests with `REDISRS_NO_SERVER=1`.

One way to start such a server is with [docker](https://www.docker.com/):

    docker run -d -p 38991:6379 --name redis redis:latest

Another way would be to port forward a remote redis server instance
using `ssh`:

    ssh -L 38991:localhost:6379

On OS X, you might need a combination of `docker` and `ssh`
port-forwarding to use [`boot2docker`](http://boot2docker.io/).
