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

macro_rules! unwrap_or {
    ($expr:expr, $or:expr) => ({
        compile_warning!("unwrap_or! is deprecated; use '.unwrap_or' instead");
        match $expr {
            Some(x) => x,
            None => { $or; }
        }
    })
}
