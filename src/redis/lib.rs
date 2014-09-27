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

#![crate_name = "redis"]
#![crate_type = "lib"]
#![license = "BSD"]
#![comment = "Bindings and wrapper functions for redis."]

#![deny(non_camel_case_types)]

#![feature(macro_rules)]

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
