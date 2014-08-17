#![macro_escape]

macro_rules! ensure {
    ($expr:expr, $err_result:expr) => (
        if !($expr) { return $err_result; }
    )
}

macro_rules! try_unwrap {
    ($expr:expr, $err_result:expr) => (
        match $expr {
            Some(x) => x,
            None => { return $err_result },
        }
    )
}
