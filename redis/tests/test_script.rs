#![allow(clippy::let_unit_value)]

use redis::ErrorKind;

use crate::support::*;

mod support;

#[test]
#[cfg(feature = "script")]
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
#[cfg(feature = "script")]
fn test_script_load() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let script = redis::Script::new("return 'Hello World'");

    let hash = script.prepare_invoke().load(&mut con);

    assert_eq!(hash, Ok(script.get_hash().to_string()));
}

#[test]
#[cfg(feature = "script")]
fn test_script_pipeline_no_autoload() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let script = redis::Script::new(r"return tonumber(ARGV[1]) + tonumber(ARGV[2]);");
    let r: Result<(), _> = redis::pipe().script(script.arg(1).arg(2)).query(&mut con);
    assert_eq!(r.unwrap_err().kind(), ErrorKind::NoScriptError);
}

#[test]
#[cfg(feature = "script")]
fn test_script_pipeline() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let script = redis::Script::new(r"return tonumber(ARGV[1]) + tonumber(ARGV[2]);");
    script.prepare_invoke().load(&mut con).unwrap();

    let (a, b): (isize, isize) = redis::pipe()
        .script(script.arg(1).arg(2))
        .script(script.arg(2).arg(3))
        .query(&mut con)
        .unwrap();

    assert_eq!(a, 3);
    assert_eq!(b, 5);
}
