#![macro_escape]

macro_rules! ensure {
    ($expr:expr, $err_result:expr) => (
        if !($expr) { return $err_result; }
    )
}

macro_rules! throw {
    ($expr:expr) => (
        return Err(::std::error::FromError::from_error($expr));
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
