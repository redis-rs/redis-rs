#[macro_export]
macro_rules! assert_args {
    ($value:expr, $($args:expr),+) => {
        let args = $value.to_redis_args();
        let strings: Vec<_> = args.iter()
                                .map(|a| std::str::from_utf8(a.as_ref()).unwrap())
                                .collect();
        assert_eq!(strings, vec![$($args),+]);
    }
}
