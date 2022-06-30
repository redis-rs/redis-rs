#![cfg(feature = "json")]

use std::assert_eq;

use redis::{
    Commands, RedisResult,
    Value::{self, *},
};

use crate::support::*;
mod support;

// adds json! macro for quick json generation on the fly.
use serde_json::{self, json};

const TEST_KEY: &str = "my_json";

#[test]
fn test_json_arrappend() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":[1i64], "nested": {"a": [1i64, 2i64]}, "nested2": {"a": 42i64}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_append: RedisResult<Value> = con.json_arrappend(TEST_KEY, "$..a", 3i64);

    assert_eq!(json_append, Ok(Bulk(vec![Int(2i64), Int(3i64), Nil])));
}

#[test]
fn test_json_arrindex() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":[1i64, 2i64, 3i64, 2i64], "nested": {"a": [3i64, 4i64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrindex: RedisResult<Value> = con.json_arrindex(TEST_KEY, "$..a", 2i64);

    assert_eq!(json_arrindex, Ok(Bulk(vec![Int(1i64), Int(-1i64)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":[1i64, 2i64, 3i64, 2i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrindex_2: RedisResult<Value> = con.json_arrindex(TEST_KEY, "$..a", 2i64);

    assert_eq!(json_arrindex_2, Ok(Bulk(vec![Int(1i64), Nil])));
}

#[test]
fn test_json_arrinsert() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":[3i64], "nested": {"a": [3i64 ,4i64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrinsert: RedisResult<Value> = con.json_arrinsert(TEST_KEY, "$..a", 0, 1i64);

    assert_eq!(json_arrinsert, Ok(Bulk(vec![Int(2), Int(3)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":[1i64 ,2i64 ,3i64 ,2i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrinsert_2: RedisResult<Value> = con.json_arrinsert(TEST_KEY, "$..a", 0, 1i64);

    assert_eq!(json_arrinsert_2, Ok(Bulk(vec![Int(5), Nil])));
}

#[test]
fn test_json_arrlen() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a": [3i64], "nested": {"a": [3i64, 4i64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrlen: RedisResult<Value> = con.json_arrlen(TEST_KEY, "$..a");

    assert_eq!(json_arrlen, Ok(Bulk(vec![Int(1), Int(2)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a": [1i64, 2i64, 3i64, 2i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrlen_2: RedisResult<Value> = con.json_arrlen(TEST_KEY, "$..a");

    assert_eq!(json_arrlen_2, Ok(Bulk(vec![Int(4), Nil])));
}

#[test]
fn test_json_arrpop() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a": [3i64], "nested": {"a": [3i64, 4i64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrpop: RedisResult<Value> = con.json_arrpop(TEST_KEY, "$..a", None);

    assert_eq!(
        json_arrpop,
        Ok(Bulk(vec![
            // convert string 3 to its ascii value as bytes
            Data(Vec::from("3".as_bytes())),
            Data(Vec::from("4".as_bytes()))
        ]))
    );

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":["foo", "bar"], "nested": {"a": false}, "nested2": {"a":[]}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrpop_2: RedisResult<Value> = con.json_arrpop(TEST_KEY, "$..a", None);

    assert_eq!(
        json_arrpop_2,
        Ok(Bulk(vec![Data(Vec::from("\"bar\"".as_bytes())), Nil, Nil]))
    );
}

#[test]
fn test_json_arrtrim() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a": [], "nested": {"a": [1i64, 4u64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrtrim: RedisResult<Value> = con.json_arrtrim(TEST_KEY, "$..a", (1, 1));

    assert_eq!(json_arrtrim, Ok(Bulk(vec![Int(0), Int(1)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a": [1i64, 2i64, 3i64, 4i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrtrim_2: RedisResult<Value> = con.json_arrtrim(TEST_KEY, "$..a", (1, 1));

    assert_eq!(json_arrtrim_2, Ok(Bulk(vec![Int(1), Nil])));
}

#[test]
fn test_json_clear() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"obj": {"a": 1i64, "b": 2i64}, "arr": [1i64, 2i64, 3i64], "str": "foo", "bool": true, "int": 42i64, "float": 3.14f64}));

    assert_eq!(set_initial, Ok(true));

    let json_clear: RedisResult<i64> = con.json_clear(TEST_KEY, "$.*");

    assert_eq!(json_clear, Ok(4));

    let checking_value: RedisResult<String> = con.json_get(TEST_KEY, "$");

    // float is set to 0 and serde_json serialises 0f64 to 0.0, which is a different string
    assert_eq!(
        checking_value,
        Ok("[{\"obj\":{},\"arr\":[],\"str\":\"foo\",\"bool\":true,\"int\":0,\"float\":0}]".into())
    );
}

#[test]
fn test_json_del() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a": 1i64, "nested": {"a": 2i64, "b": 3i64}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_del: RedisResult<i64> = con.json_del(TEST_KEY, "$..a");

    assert_eq!(json_del, Ok(2));
}

#[test]
fn test_json_get() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":2i64, "b": 3i64, "nested": {"a": 4i64, "b": null}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_get: RedisResult<String> = con.json_get(TEST_KEY, "$..b");

    assert_eq!(json_get, Ok("[3,null]".into()));

    let json_get_multi: RedisResult<String> = con.json_get(TEST_KEY, "..a $..b");

    assert_eq!(
        json_get_multi,
        Ok("{\"$..b\":[3,null],\"..a\":[2,4]}".into())
    );
}

#[test]
fn test_json_mget() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial_a: RedisResult<bool> = con.json_set(
        format!("{TEST_KEY}-a"),
        "$",
        json!({"a":1i64, "b": 2i64, "nested": {"a": 3i64, "b": null}}),
    );
    let set_initial_b: RedisResult<bool> = con.json_set(
        format!("{TEST_KEY}-b"),
        "$",
        json!({"a":4i64, "b": 5i64, "nested": {"a": 6i64, "b": null}}),
    );

    assert_eq!(set_initial_a, Ok(true));
    assert_eq!(set_initial_b, Ok(true));

    let json_mget: RedisResult<Value> = con.json_mget(
        vec![format!("{TEST_KEY}-a"), format!("{TEST_KEY}-b")],
        "$..a",
    );

    assert_eq!(
        json_mget,
        Ok(Bulk(vec![
            Data(Vec::from("[1,3]".as_bytes())),
            Data(Vec::from("[4,6]".as_bytes()))
        ]))
    );
}

#[test]
fn test_json_numincrby() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":"b","b":[{"a":2i64}, {"a":5i64}, {"a":"c"}]}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_numincrby_a: RedisResult<String> = con.json_numincrby(TEST_KEY, "$.a", 2);

    // cannot increment a string
    assert_eq!(json_numincrby_a, Ok("[null]".into()));

    let json_numincrby_b: RedisResult<String> = con.json_numincrby(TEST_KEY, "$..a", 2);

    // however numbers can be incremented
    assert_eq!(json_numincrby_b, Ok("[null,4,7,null]".into()));
}

#[test]
fn test_json_objkeys() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":[3i64], "nested": {"a": {"b":2i64, "c": 1i64}}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_objkeys: RedisResult<Value> = con.json_objkeys(TEST_KEY, "$..a");

    assert_eq!(
        json_objkeys,
        Ok(Bulk(vec![
            Nil,
            Bulk(vec![
                Data(Vec::from("b".as_bytes())),
                Data(Vec::from("c".as_bytes()))
            ])
        ]))
    );
}

#[test]
fn test_json_objlen() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":[3i64], "nested": {"a": {"b":2i64, "c": 1i64}}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_objlen: RedisResult<Value> = con.json_objlen(TEST_KEY, "$..a");

    assert_eq!(json_objlen, Ok(Bulk(vec![Nil, Int(2)])));
}

#[test]
fn test_json_set() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"key": "value"}));

    assert_eq!(result, Ok(true));
}

#[test]
fn test_json_strappend() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31i64}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_strappend: RedisResult<Value> = con.json_strappend(TEST_KEY, "$..a", "\"baz\"");

    assert_eq!(json_strappend, Ok(Bulk(vec![Int(6), Int(8), Nil])));

    let json_get_check: RedisResult<String> = con.json_get(TEST_KEY, "$");

    assert_eq!(
        json_get_check,
        Ok("[{\"a\":\"foobaz\",\"nested\":{\"a\":\"hellobaz\"},\"nested2\":{\"a\":31}}]".into())
    );
}

#[test]
fn test_json_strlen() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31i32}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_strlen: RedisResult<Value> = con.json_set(TEST_KEY, "$..a");

    assert_eq!(json_strlen, Ok(Bulk(vec![Int(3), Int(5), Nil])));
}

#[test]
fn test_json_toggle() {
    let ctx = TextContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"bool": true}));

    assert_eq!(set_initial, Ok(true));

    let json_toggle_a: RedisResult<Value> = con.json_toggle(TEST_KET, "$.bool");
    assert_eq!(json_toggle_a, Ok(Bulk(vec![Int(0)])));

    let json_toggle_b: RedisResult<Value> = con.json_toggle(TEST_KET, "$.bool");
    assert_eq!(json_toggle_b, Ok(Bulk(vec![Int(1)])));
}

#[test]
fn test_json_type() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        json!({"a":2i64, "nested": {"a": true}, "foo": "bar"}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_type_a: RedisResult<Value> = con.json_type(TEST_KEY, "$..foo");

    assert_eq!(
        json_type_a,
        Ok(Bulk(vec![Data(Vec::from("string".as_bytes()))]))
    );

    let json_type_b: RedisResult<Value> = con.json_type(TEST_KEY, "$..a");

    assert_eq!(
        json_type_b,
        Ok(Bulk(vec![
            Data(Vec::from("integer".as_bytes())),
            Data(Vec::from("string".as_bytes()))
        ]))
    );

    let json_type_c: RedisResult<Value> = con.json_type(TEST_KEY, "$..dummy");

    assert_eq!(json_type_c, Ok(Bulk(vec![])));
}
