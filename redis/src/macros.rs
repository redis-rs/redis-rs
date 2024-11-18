#![macro_use]

macro_rules! fail {
    ($expr:expr) => {
        return Err(::std::convert::From::from($expr))
    };
}

#[allow(unused_macros)]
macro_rules! not_convertible_error {
    ($v:expr, $det:expr) => {
        RedisError::from((
            ErrorKind::TypeError,
            "Response type not convertible",
            format!("{:?} (response was {:?})", $det, $v),
        ))
    };
}
