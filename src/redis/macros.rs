#[macro_escape];

macro_rules! try_unwrap {
    ($expr:expr, $err_result:expr) => (
        match ($expr) {
            Some(x) => x,
            None => { return $err_result },
        }
    )
}

macro_rules! push_byte_format {
    ($container:expr, $($arg:tt)*) => ({
        let encoded = format!($($arg)*);
        push_bytes($container, encoded.as_bytes());
    })
}
