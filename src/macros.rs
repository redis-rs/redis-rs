#![macro_use]

macro_rules! ensure {
    ($expr:expr, $err_result:expr) => (
        if !($expr) { return $err_result; }
    )
}

macro_rules! fail {
    ($expr:expr) => (
        return Err(::std::error::FromError::from_error($expr));
    )
}

macro_rules! result_unwrap_or {
    ($expr:expr, $or:expr) => (
        match $expr {
            Ok(x) => x,
            Err(_) => { $or; }
        }
    )
}

macro_rules! option_unwrap_or {
    ($expr:expr, $or:expr) => (
        match $expr {
            Some(x) => x,
            None => { $or; }
        }
    )
}
