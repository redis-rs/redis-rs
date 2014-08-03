#![crate_id = "redis#0.1"]
#![crate_type = "lib"]
#![license = "BSD"]
#![comment = "Bindings and wrapper functions for redis."]

#![deny(non_camel_case_types)]
#![feature(macro_rules)]
#![feature(globs)]

extern crate extra;
extern crate time;
extern crate collections;
extern crate serialize;

pub use parser::parse_redis_value;
pub use parser::Parser;
pub use enums::*;

pub use client::Client;
pub use script::Script;
pub use connection::Connection;

mod parser;
mod client;
mod script;
mod enums;
mod utils;
mod connection;
mod scan;
