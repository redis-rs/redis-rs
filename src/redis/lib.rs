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

/* public enums */
pub use enums::{
    RedisErrorKind,
        ResponseError,
        ExecAbortError,
        BusyLoadingError,
        NoScriptError,
        ExtensionError,
        InternalIoError,

    RedisValue,
        Nil,
        Int,
        Data,
        Bulk,
        Okay,
        Status,

    RedisError,

    CmdArg,
        StrArg,
        IntArg,
        FloatArg,
        BytesArg,

    RedisResult,
};

pub mod macros;

mod enums;
mod parser;
mod client;
mod connection;
