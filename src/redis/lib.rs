//! redis-rs is a rust implementation of a Redis client library.  It exposes
//! a general purpose interface to Redis and also provides specific helpers for
//! commonly used functionality.
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
//! Because redis inherently is mostly type-less and the protocol is not
//! exactly friendly to developers, this library provides flexible support
//! for casting values to the intended results.  This is driven through the
//! `FromRedisValue` trait.
//!
//! The `query` method of a `Cmd` can conver the value to what you expect
//! the function to return:
//!
//! ```rust,no_run
//! # use std::collections::{HashMap, HashSet};
//! # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
//! # let con = client.get_connection().unwrap();
//! let count : i32 = redis::cmd("GET").arg("my_counter").query(&con).unwrap();
//! let name : String = redis::cmd("GET").arg("my_name").query(&con).unwrap();
//! let bin : Vec<u8> = redis::cmd("GET").arg("my_binary").query(&con).unwrap();
//! let map : HashMap<String, i32> = redis::cmd("HGETALL").arg("my_hash").query(&con).unwrap();
//! let keys : Vec<String> = redis::cmd("KEYS").query(&con).unwrap();
//! let mems : HashSet<i32> = redis::cmd("SMEMBERS").arg("s").query(&con).unwrap();
//! ```

#![crate_name = "redis"]
#![crate_type = "lib"]
#![license = "BSD"]
#![comment = "Bindings and wrapper functions for redis."]

#![deny(non_camel_case_types)]

#![feature(macro_rules)]
#![feature(default_type_params)]

extern crate url;

/* public api */
pub use parser::{parse_redis_value, Parser};
pub use client::Client;
pub use connection::Connection;
pub use cmd::{cmd, Cmd};

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
    ToRedisArg
};

pub mod macros;

mod parser;
mod client;
mod connection;
mod types;
mod cmd;
