#![macro_escape]

#[macro_export]
macro_rules! ensure {
    ($expr:expr, $err_result:expr) => (
        if !($expr) { return $err_result; }
    )
}

#[macro_export]
macro_rules! try_unwrap {
    ($expr:expr, $err_result:expr) => (
        match $expr {
            Some(x) => x,
            None => { return $err_result },
        }
    )
}

#[macro_export]
macro_rules! push_byte_format {
    ($container:expr, $($arg:tt)*) => ({
        let encoded = format!($($arg)*);
        $container.push_all(encoded.as_bytes());
    })
}
