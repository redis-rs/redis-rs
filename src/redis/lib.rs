//! redis-rs is a rust implementation of a Redis client library.  It exposes
//! a general purpose interface to Redis and also provides specific helpers for
//! commonly used functionality.
//!
//! # Basic Operation
//!
//! To open a connection you need to create a client and then to fetch a
//! connection from it.  In the future there will be a connection pool for
//! those, currently each connection is separate and not pooled.
//!
//! Queries are then performed through the `execute` and `query` methods.
//!
//! ```rust,no_run
//! extern crate redis;
//!
//! fn main() {
//!     let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//!     let con = client.get_connection().unwrap();
//!     redis::cmd("SET").arg("my_key").arg(42i).execute(&con);
//!     assert_eq!(redis::cmd("GET").arg("my_key").query(&con), Ok(42i));
//! }
//! ```
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
//! # use std::collections::{HashMap, HashSet};
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let count : i32 = redis::cmd("GET").arg("my_counter").query(&con).unwrap();
//! let count = redis::cmd("GET").arg("my_counter").query(&con).unwrap_or(0i32);
//! let k : Option<String> = redis::cmd("GET").arg("missing_key").query(&con).unwrap();
//! let name : String = redis::cmd("GET").arg("my_name").query(&con).unwrap();
//! let bin : Vec<u8> = redis::cmd("GET").arg("my_binary").query(&con).unwrap();
//! let map : HashMap<String, i32> = redis::cmd("HGETALL").arg("my_hash").query(&con).unwrap();
//! let keys : Vec<String> = redis::cmd("KEYS").query(&con).unwrap();
//! let mems : HashSet<i32> = redis::cmd("SMEMBERS").arg("s").query(&con).unwrap();
//! let (k1, k2) : (String, String) = redis::cmd("MGET").arg("k1").arg("k2").query(&con).unwrap();
//! ```
//!
//! ## Iteration Protocol
//!
//! In addition to sending a single query you iterators are also supported.  When
//! used with regular bulk responses they don't give you much over querying and
//! converting into a vector (both use a vector internally) but they can also
//! be used with `SCAN` like commands in which case iteration will send more
//! queries until the cursor is exhausted:
//!
//! ```rust,no_run
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let mut cmd = redis::cmd("SSCAN");
//! let mut iter : redis::Iter<int> = cmd.arg("my_set").cursor_arg(0).iter(&con).unwrap();
//! for x in iter {
//!     // do something with the item
//! }
//! ```
//!
//! As you can see the cursor argument needs to be defined with `cursor_arg` instead
//! of `arg` so that the library knows which argument needs updating as the
//! query is run for more items.
//!
//! ## Pipelining
//!
//! In addition to simple queries you can also send command pipelines.  This
//! is provided through the `pipe` function.  It works very similar to sending
//! individual commands but you can send more than one in one go.  This also
//! allows you to ignore individual results so that matching on the end result
//! is easier:
//!
//! ```rust,no_run
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let (k1, k2) : (i32, i32) = redis::pipe()
//!     .cmd("SET").arg("key_1").arg(42i).ignore()
//!     .cmd("SET").arg("key_2").arg(43i).ignore()
//!     .cmd("GET").arg("key_1")
//!     .cmd("GET").arg("key_2").query(&con).unwrap();
//! ```
//!
//! If you want the pipeline to be wrapped in a `MULTI`/`EXEC` block you can
//! easily do that by switching the pipeline into `atomic` mode.  From the
//! caller's point of view nothing changes, the pipeline itself will take
//! care of the rest for you:
//!
//! ```rust,no_run
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let (k1, k2) : (i32, i32) = redis::pipe()
//!     .atomic()
//!     .cmd("SET").arg("key_1").arg(42i).ignore()
//!     .cmd("SET").arg("key_2").arg(43i).ignore()
//!     .cmd("GET").arg("key_1")
//!     .cmd("GET").arg("key_2").query(&con).unwrap();
//! ```
//!
//! ## Transactions
//!
//! Transactions are available through atomic pipelines.  In order to use
//! them in a more simple way you can use the `transaction` function of a
//! connection:
//!
//! ```rust,no_run
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let key = "the_key";
//! let (new_val,) : (int,) = con.transaction([key][], |pipe| {
//!     let old_val : int = try!(redis::cmd("GET").arg(key).query(&con));
//!     pipe
//!         .cmd("SET").arg(key).arg(old_val + 1).ignore()
//!         .cmd("GET").arg(key).query(&con)
//! }).unwrap();
//! println!("The incremented number is: {}", new_val);
//! ```
//!
//! For more information see the `transaction` function.
//!
//! ## PubSub
//!
//! Pubsub is currently work in progress but provided through the `PubSub`
//! connection object.  Due to the fact that Rust does not have support
//! for async IO in libnative yet, the API does not provide a way to
//! read messages with any form of timeout yet.
//!
//! Example usage:
//!
//! ```rust,no_run
//! let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! let mut pubsub = client.get_pubsub().unwrap();
//! pubsub.subscribe("channel_1").unwrap();
//! pubsub.subscribe("channel_2").unwrap();
//!
//! loop {
//!     let msg = pubsub.get_message().unwrap();
//!     let payload : String = msg.get_payload().unwrap();
//!     println!("channel '{}': {}", msg.get_channel_name(), payload);
//! }
//! ```
//!
//! ## Scripts
//!
//! Lua scripts are supported through the `Script` type in a convenient
//! way (it does not support pipelining currently).  It will automatically
//! load the script if it does not exist and invoke it.
//!
//! Example:
//!
//! ```rust,no_run
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let script = redis::Script::new(r"
//!     return tonumber(ARGV[1]) + tonumber(ARGV[2]);
//! ");
//! let result : int = script.arg(1i).arg(2i).invoke(&con).unwrap();
//! assert_eq!(result, 3);
//! ```

#![crate_name = "redis"]
#![crate_type = "lib"]
#![license = "BSD"]
#![comment = "Bindings and wrapper functions for redis."]

#![deny(non_camel_case_types)]

#![feature(macro_rules)]
#![feature(default_type_params)]
#![feature(slicing_syntax)]

#![experimental]

extern crate url;
extern crate serialize;
extern crate "rust-crypto" as crypto;

/* public api */
pub use parser::{parse_redis_value, Parser};
pub use client::Client;
pub use script::{Script, ScriptInvocation};
pub use connection::{Connection, PubSub, Msg};
pub use cmd::{cmd, Cmd, pipe, Pipeline, Iter, pack_command};

#[doc(hidden)]
pub use types::{
    /* low level values */
    Value,
        Nil,
        Int,
        Data,
        Bulk,
        Okay,
        Status,

    /* error and result types */
    Error,
    RedisResult,

    /* error kinds */
    ErrorKind,
        ResponseError,
        TypeError,
        ExecAbortError,
        BusyLoadingError,
        NoScriptError,
        ExtensionError,
        InternalIoError,

    /* utility types */
    InfoDict,

    /* conversion traits */
    FromRedisValue,
    ToRedisArgs,

    /* utility functions */
    from_redis_value,
};

pub mod macros;

mod parser;
mod client;
mod connection;
mod types;
mod script;
mod cmd;
