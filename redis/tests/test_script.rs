#![cfg(feature = "script")]

mod support;

mod script {
    use redis::ErrorKind;

    use crate::support::*;

    #[test]
    fn test_script() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let script = redis::Script::new(r"return {redis.call('GET', KEYS[1]), ARGV[1]}");

        let _: () = redis::cmd("SET")
            .arg("my_key")
            .arg("foo")
            .query(&mut con)
            .unwrap();
        let response = script.key("my_key").arg(42).invoke(&mut con);

        assert_eq!(response, Ok(("foo".to_string(), 42)));
    }

    #[test]
    fn test_script_load() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let script = redis::Script::new("return 'Hello World'");

        let hash = script.prepare_invoke().load(&mut con);

        assert_eq!(hash, Ok(script.get_hash().to_string()));
    }

    #[test]
    fn test_script_that_is_not_loaded_fails_on_pipeline_invocation() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let script = redis::Script::new(r"return tonumber(ARGV[1]) + tonumber(ARGV[2]);");
        let r: Result<(), _> = redis::pipe()
            .invoke_script(script.arg(1).arg(2))
            .query(&mut con);
        assert_eq!(r.unwrap_err().kind(), ErrorKind::NoScriptError);
    }

    #[test]
    fn test_script_pipeline() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let script = redis::Script::new(r"return tonumber(ARGV[1]) + tonumber(ARGV[2]);");
        script.prepare_invoke().load(&mut con).unwrap();

        let (a, b): (isize, isize) = redis::pipe()
            .invoke_script(script.arg(1).arg(2))
            .invoke_script(script.arg(2).arg(3))
            .query(&mut con)
            .unwrap();

        assert_eq!(a, 3);
        assert_eq!(b, 5);
    }
}
