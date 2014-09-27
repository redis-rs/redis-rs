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

pub use types::{
    /* public types */
    ErrorKind,
        ResponseError,
        ExecAbortError,
        BusyLoadingError,
        NoScriptError,
        ExtensionError,
        InternalIoError,

    Value,
        Nil,
        Int,
        Data,
        Bulk,
        Okay,
        Status,

    Error,

    CmdArg,
        StrArg,
        IntArg,
        FloatArg,
        BytesArg,

    RedisResult,

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
