//! redis-rs is a rust implementation of a Redis client library.  It exposes
//! a general purpose interface to Redis and also provides specific helpers for
//! commonly used functionality.
//!
//! The crate is called `redis` and you can depend on it via cargo:
//!
//! ```ini
//! [dependencies.redis]
//! version = "*"
//! ```
//!
//! If you want to use the git version:
//!
//! ```ini
//! [dependencies.redis]
//! git = "https://github.com/mitsuhiko/redis-rs.git"
//! ```
//!
//! # Basic Operation
//!
//! redis-rs exposes to API levels: a low- and a high-level part.  The high-level
//! part does not expose all the functionality of redis and might take some
//! liberties in how it speaks the protocol.  The low-level part of the API
//! allows you to express any request on the redis level.  You can fluently
//! switch between both API levels at any point.
//!
//! ## Connection Handling
//!
//! For connecting to redis you can use a client object which then can produce
//! actual connections.  Connections and clients as well as results of
//! connections and clients are considered `ConnectionLike` objects and
//! can be used anywhere a request is made.
//!
//! The full canonical way to get a connection is to create a client and
//! to ask for a connection from it:
//!
//! ```rust,no_run
//! extern crate redis;
//!
//! fn do_something() -> redis::RedisResult<()> {
//!     let client = try!(redis::Client::open("redis://127.0.0.1/"));
//!     let con = try!(client.get_connection());
//!
//!     /* do something here */
//!
//!     Ok(())
//! }
//! # fn main() {}
//! ```
//!
//! ## Unix Sockets
//!
//! By default this library does not support unix sockets but starting with
//! redis-rs 0.5.0 you can optionally compile it with unix sockets enabled.
//! For this you just need to enable the `unix_sockets` flag and some of the
//! otherwise unavailable APIs become available:
//!
//! ```ini
//! [dependencies.redis]
//! version = "*"
//! features = ["unix_socket"]
//! ```
//!
//! ## Connection Parameters
//!
//! redis-rs knows different ways to define where a connection should
//! go.  The parameter to `Client::open` needs to implement the
//! `IntoConnectionInfo` trait of which there are three implementations:
//!
//! * string slices in `redis://` URL format.
//! * URL objects from the redis-url crate.
//! * `ConnectionInfo` objects.
//!
//! The URL format is `redis://[:<passwd>@]<hostname>[:port][/<db>]`
//!
//! In case you have compiled the crate with the `unix_sockets` feature
//! then you can also use a unix URL in this format:
//!
//! `unix:///[:<passwd>@]<path>[?db=<db>]`
//!
//! ## Executing Low-Level Commands
//!
//! To execute low-level commands you can use the `cmd` function which allows
//! you to build redis requests.  Once you have configured a command object
//! to your liking you can send a query into any `ConnectionLike` object:
//!
//! ```rust,no_run
//! fn do_something(con: &redis::Connection) -> redis::RedisResult<()> {
//!     let _ : () = try!(redis::cmd("SET").arg("my_key").arg(42).query(con));
//!     Ok(())
//! }
//! ```
//!
//! Upon querying the return value is a result object.  If you do not care
//! about the actual return value (other than that it is not a failure)
//! you can always type annotate it to the unit type `()`.
//!
//! ## Executing High-Level Commands
//!
//! The high-level interface is similar.  For it to become available you
//! need to use the `Commands` trait in which case all `ConnectionLike`
//! objects the library provides will also have high-level methods which
//! make working with the protocol easier:
//!
//! ```rust,no_run
//! extern crate redis;
//! use redis::Commands;
//!
//! fn do_something(con: &redis::Connection) -> redis::RedisResult<()> {
//!     let _ : () = try!(con.set("my_key", 42));
//!     Ok(())
//! }
//! # fn main() {}
//! ```
//!
//! Note that high-level commands are work in progress and many are still
//! missing!
//!
//! ## Type Conversions
//!
//! Because redis inherently is mostly type-less and the protocol is not
//! exactly friendly to developers, this library provides flexible support
//! for casting values to the intended results.  This is driven through the
//! `FromRedisValue` and `ToRedisArgs` traits.
//!
//! The `arg` method of the command will accept a wide range of types through
//! the `ToRedisArgs` trait and the `query` method of a command can convert the
//! value to what you expect the function to return through the `FromRedisValue`
//! trait.  This is quite flexible and allows vectors, tuples, hashsets, hashmaps
//! as well as optional values:
//!
//! ```rust,no_run
//! # use redis::Commands;
//! # use std::collections::{HashMap, HashSet};
//! # fn do_something() -> redis::RedisResult<()> {
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let count : i32 = try!(con.get("my_counter"));
//! let count = con.get("my_counter").unwrap_or(0i32);
//! let k : Option<String> = try!(con.get("missing_key"));
//! let name : String = try!(con.get("my_name"));
//! let bin : Vec<u8> = try!(con.get("my_binary"));
//! let map : HashMap<String, i32> = try!(con.hgetall("my_hash"));
//! let keys : Vec<String> = try!(con.hkeys("my_hash"));
//! let mems : HashSet<i32> = try!(con.smembers("my_set"));
//! let (k1, k2) : (String, String) = try!(con.get(&["k1", "k2"]));
//! # Ok(())
//! # }
//! ```
//!
//! # Iteration Protocol
//!
//! In addition to sending a single query you iterators are also supported.  When
//! used with regular bulk responses they don't give you much over querying and
//! converting into a vector (both use a vector internally) but they can also
//! be used with `SCAN` like commands in which case iteration will send more
//! queries until the cursor is exhausted:
//!
//! ```rust,no_run
//! # fn do_something() -> redis::RedisResult<()> {
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let mut iter : redis::Iter<isize> = try!(redis::cmd("SSCAN").arg("my_set")
//!     .cursor_arg(0).iter(&con));
//! for x in iter {
//!     // do something with the item
//! }
//! # Ok(()) }
//! ```
//!
//! As you can see the cursor argument needs to be defined with `cursor_arg`
//! instead of `arg` so that the library knows which argument needs updating
//! as the query is run for more items.
//!
//! # Pipelining
//!
//! In addition to simple queries you can also send command pipelines.  This
//! is provided through the `pipe` function.  It works very similar to sending
//! individual commands but you can send more than one in one go.  This also
//! allows you to ignore individual results so that matching on the end result
//! is easier:
//!
//! ```rust,no_run
//! # fn do_something() -> redis::RedisResult<()> {
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let (k1, k2) : (i32, i32) = try!(redis::pipe()
//!     .cmd("SET").arg("key_1").arg(42).ignore()
//!     .cmd("SET").arg("key_2").arg(43).ignore()
//!     .cmd("GET").arg("key_1")
//!     .cmd("GET").arg("key_2").query(&con));
//! # Ok(()) }
//! ```
//!
//! If you want the pipeline to be wrapped in a `MULTI`/`EXEC` block you can
//! easily do that by switching the pipeline into `atomic` mode.  From the
//! caller's point of view nothing changes, the pipeline itself will take
//! care of the rest for you:
//!
//! ```rust,no_run
//! # fn do_something() -> redis::RedisResult<()> {
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let (k1, k2) : (i32, i32) = try!(redis::pipe()
//!     .atomic()
//!     .cmd("SET").arg("key_1").arg(42).ignore()
//!     .cmd("SET").arg("key_2").arg(43).ignore()
//!     .cmd("GET").arg("key_1")
//!     .cmd("GET").arg("key_2").query(&con));
//! # Ok(()) }
//! ```
//!
//! You can also use high-level commands on pipelines through the
//! `PipelineCommands` trait:
//!
//! ```rust,no_run
//! # fn do_something() -> redis::RedisResult<()> {
//! use redis::PipelineCommands;
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let (k1, k2) : (i32, i32) = try!(redis::pipe()
//!     .atomic()
//!     .set("key_1", 42).ignore()
//!     .set("key_2", 43).ignore()
//!     .get("key_1")
//!     .get("key_2").query(&con));
//! # Ok(()) }
//! ```
//!
//! # Transactions
//!
//! Transactions are available through atomic pipelines.  In order to use
//! them in a more simple way you can use the `transaction` function of a
//! connection:
//!
//! ```rust,no_run
//! # fn do_something() -> redis::RedisResult<()> {
//! use redis::{Commands, PipelineCommands};
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let key = "the_key";
//! let (new_val,) : (isize,) = try!(redis::transaction(&con, &[key], |pipe| {
//!     let old_val : isize = try!(con.get(key));
//!     pipe
//!         .set(key, old_val + 1).ignore()
//!         .get(key).query(&con)
//! }));
//! println!("The incremented number is: {}", new_val);
//! # Ok(()) }
//! ```
//!
//! For more information see the `transaction` function.
//!
//! # PubSub
//!
//! Pubsub is currently work in progress but provided through the `PubSub`
//! connection object.  Due to the fact that Rust does not have support
//! for async IO in libnative yet, the API does not provide a way to
//! read messages with any form of timeout yet.
//!
//! Example usage:
//!
//! ```rust,no_run
//! # fn do_something() -> redis::RedisResult<()> {
//! let client = try!(redis::Client::open("redis://127.0.0.1/"));
//! let mut pubsub = try!(client.get_pubsub());
//! try!(pubsub.subscribe("channel_1"));
//! try!(pubsub.subscribe("channel_2"));
//!
//! loop {
//!     let msg = try!(pubsub.get_message());
//!     let payload : String = try!(msg.get_payload());
//!     println!("channel '{}': {}", msg.get_channel_name(), payload);
//! }
//! # }
//! ```
//!
//! # Scripts
//!
//! Lua scripts are supported through the `Script` type in a convenient
//! way (it does not support pipelining currently).  It will automatically
//! load the script if it does not exist and invoke it.
//!
//! Example:
//!
//! ```rust,no_run
//! # fn do_something() -> redis::RedisResult<()> {
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let script = redis::Script::new(r"
//!     return tonumber(ARGV[1]) + tonumber(ARGV[2]);
//! ");
//! let result : isize = try!(script.arg(1).arg(2).invoke(&con));
//! assert_eq!(result, 3);
//! # Ok(()) }
//! ```
//!
//! ## Breaking Changes
//!
//! In Rust 0.5.0 the semi-internal `ConnectionInfo` struct had to be
//! changed because of the unix socket support.  You are generally
//! heavily encouraged to use the URL based configuration format which
//! is a lot more stable than the structs.

#![deny(non_camel_case_types)]

extern crate url;
extern crate rustc_serialize as serialize;
extern crate sha1;

#[cfg(feature="unix_socket")]
extern crate unix_socket;

// public api
pub use parser::{parse_redis_value, Parser};
pub use client::Client;
pub use script::{Script, ScriptInvocation};
pub use connection::{Connection, ConnectionLike, ConnectionInfo, ConnectionAddr,
                     IntoConnectionInfo, PubSub, Msg, transaction, parse_redis_url};
pub use cmd::{cmd, Cmd, pipe, Pipeline, Iter, pack_command};
pub use commands::{Commands, PipelineCommands};

#[doc(hidden)]
pub use types::{/* low level values */ Value, /* error and result types */ RedisError,
                RedisResult, /* error kinds */ ErrorKind, /* utility types */ InfoDict,
                NumericBehavior, /* conversion traits */ FromRedisValue, ToRedisArgs,
                /* utility functions */ from_redis_value};

mod macros;

mod parser;
mod client;
mod connection;
mod types;
mod script;
mod cmd;
mod commands;
