#![macro_escape]

macro_rules! ensure {
    ($expr:expr, $err_result:expr) => (
        if !($expr) { return $err_result; }
    )
}

macro_rules! unwrap_or {
    ($expr:expr, $or:expr) => (
        match $expr {
            Some(x) => x,
            None => { $or; }
        }
    )
}

macro_rules! try_io {
    ($expr:expr) => (
        match $expr {
            Ok(x) => x,
            Err(err) => {
                use types::{Error, InternalIoError};
                return Err(Error::simple(
                    InternalIoError(err),
                    "Operation failed because of an IO error"));
            }
        }
    )
}
