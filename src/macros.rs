#![macro_use]

macro_rules! fail {
    ($expr:expr) => {
        return Err(::std::convert::From::from($expr))
    };
}

macro_rules! unwrap_or {
    ($expr:expr, $or:expr) => {
        match $expr {
            Some(x) => x,
            None => {
                $or;
            }
        }
    };
}
