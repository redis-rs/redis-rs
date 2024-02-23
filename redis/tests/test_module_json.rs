#![cfg(feature = "json")]

use std::assert_eq;
use std::collections::HashMap;

use redis::JsonCommands;

use redis::{
    ErrorKind, RedisError, RedisResult,
    Value::{self, *},
};

use crate::support::*;
mod support;

use serde::Serialize;
// adds json! macro for quick json generation on the fly.
use serde_json::json;

const TEST_KEY: &str = "my_json";

const MTLS_NOT_ENABLED: bool = false;

#[test]
fn test_module_json_serialize_error() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    #[derive(Debug, Serialize)]
    struct InvalidSerializedStruct {
        // Maps in serde_json must have string-like keys
        // so numbers and strings, anything else will cause the serialization to fail
        // this is basically the only way to make a serialization fail at runtime
        // since rust doesnt provide the necessary ability to enforce this
        pub invalid_json: HashMap<Option<bool>, i64>,
    }

    let mut test_invalid_value: InvalidSerializedStruct = InvalidSerializedStruct {
        invalid_json: HashMap::new(),
    };

    test_invalid_value.invalid_json.insert(None, 2i64);

    let set_invalid: RedisResult<bool> = con.json_set(TEST_KEY, "$", &test_invalid_value);

    assert_eq!(
        set_invalid,
        Err(RedisError::from((
            ErrorKind::Serialize,
            "Serialization Error",
            String::from("key must be string")
        )))
    );
}

#[test]
fn test_module_json_arr_append() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1i64], "nested": {"a": [1i64, 2i64]}, "nested2": {"a": 42i64}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_append: RedisResult<Value> = con.json_arr_append(TEST_KEY, "$..a", &3i64);

    assert_eq!(json_append, Ok(Bulk(vec![Int(2i64), Int(3i64), Nil])));
}

#[test]
fn test_module_json_arr_index() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1i64, 2i64, 3i64, 2i64], "nested": {"a": [3i64, 4i64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrindex: RedisResult<Value> = con.json_arr_index(TEST_KEY, "$..a", &2i64);

    assert_eq!(json_arrindex, Ok(Bulk(vec![Int(1i64), Int(-1i64)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1i64, 2i64, 3i64, 2i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrindex_2: RedisResult<Value> =
        con.json_arr_index_ss(TEST_KEY, "$..a", &2i64, &0, &0);

    assert_eq!(json_arrindex_2, Ok(Bulk(vec![Int(1i64), Nil])));
}

#[test]
fn test_module_json_arr_insert() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[3i64], "nested": {"a": [3i64 ,4i64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrinsert: RedisResult<Value> = con.json_arr_insert(TEST_KEY, "$..a", 0, &1i64);

    assert_eq!(json_arrinsert, Ok(Bulk(vec![Int(2), Int(3)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1i64 ,2i64 ,3i64 ,2i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrinsert_2: RedisResult<Value> = con.json_arr_insert(TEST_KEY, "$..a", 0, &1i64);

    assert_eq!(json_arrinsert_2, Ok(Bulk(vec![Int(5), Nil])));
}

#[test]
fn test_module_json_arr_len() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": [3i64], "nested": {"a": [3i64, 4i64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrlen: RedisResult<Value> = con.json_arr_len(TEST_KEY, "$..a");

    assert_eq!(json_arrlen, Ok(Bulk(vec![Int(1), Int(2)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": [1i64, 2i64, 3i64, 2i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrlen_2: RedisResult<Value> = con.json_arr_len(TEST_KEY, "$..a");

    assert_eq!(json_arrlen_2, Ok(Bulk(vec![Int(4), Nil])));
}

#[test]
fn test_module_json_arr_pop() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": [3i64], "nested": {"a": [3i64, 4i64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrpop: RedisResult<Value> = con.json_arr_pop(TEST_KEY, "$..a", -1);

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
        &json!({"a":["foo", "bar"], "nested": {"a": false}, "nested2": {"a":[]}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrpop_2: RedisResult<Value> = con.json_arr_pop(TEST_KEY, "$..a", -1);

    assert_eq!(
        json_arrpop_2,
        Ok(Bulk(vec![Data(Vec::from("\"bar\"".as_bytes())), Nil, Nil]))
    );
}

#[test]
fn test_module_json_arr_trim() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": [], "nested": {"a": [1i64, 4u64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrtrim: RedisResult<Value> = con.json_arr_trim(TEST_KEY, "$..a", 1, 1);

    assert_eq!(json_arrtrim, Ok(Bulk(vec![Int(0), Int(1)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": [1i64, 2i64, 3i64, 4i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrtrim_2: RedisResult<Value> = con.json_arr_trim(TEST_KEY, "$..a", 1, 1);

    assert_eq!(json_arrtrim_2, Ok(Bulk(vec![Int(1), Nil])));
}

#[test]
fn test_module_json_clear() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", &json!({"obj": {"a": 1i64, "b": 2i64}, "arr": [1i64, 2i64, 3i64], "str": "foo", "bool": true, "int": 42i64, "float": std::f64::consts::PI}));

    assert_eq!(set_initial, Ok(true));

    let json_clear: RedisResult<i64> = con.json_clear(TEST_KEY, "$.*");

    assert_eq!(json_clear, Ok(4));

    let checking_value: RedisResult<String> = con.json_get(TEST_KEY, "$");

    // float is set to 0 and serde_json serializes 0f64 to 0.0, which is a different string
    assert_eq!(
        checking_value,
        // i found it changes the order?
        // its not reallt a problem if you're just deserializing it anyway but still
        // kinda weird
        Ok("[{\"arr\":[],\"bool\":true,\"float\":0,\"int\":0,\"obj\":{},\"str\":\"foo\"}]".into())
    );
}

#[test]
fn test_module_json_del() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": 1i64, "nested": {"a": 2i64, "b": 3i64}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_del: RedisResult<i64> = con.json_del(TEST_KEY, "$..a");

    assert_eq!(json_del, Ok(2));
}

#[test]
fn test_module_json_get() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":2i64, "b": 3i64, "nested": {"a": 4i64, "b": null}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_get: RedisResult<String> = con.json_get(TEST_KEY, "$..b");

    assert_eq!(json_get, Ok("[3,null]".into()));

    let json_get_multi: RedisResult<String> = con.json_get(TEST_KEY, vec!["..a", "$..b"]);

    if json_get_multi != Ok("{\"$..b\":[3,null],\"..a\":[2,4]}".into())
        && json_get_multi != Ok("{\"..a\":[2,4],\"$..b\":[3,null]}".into())
    {
        panic!("test_error: incorrect response from json_get_multi");
    }
}

#[test]
fn test_module_json_mget() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial_a: RedisResult<bool> = con.json_set(
        format!("{TEST_KEY}-a"),
        "$",
        &json!({"a":1i64, "b": 2i64, "nested": {"a": 3i64, "b": null}}),
    );
    let set_initial_b: RedisResult<bool> = con.json_set(
        format!("{TEST_KEY}-b"),
        "$",
        &json!({"a":4i64, "b": 5i64, "nested": {"a": 6i64, "b": null}}),
    );

    assert_eq!(set_initial_a, Ok(true));
    assert_eq!(set_initial_b, Ok(true));

    let json_mget: RedisResult<Value> = con.json_get(
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
fn test_module_json_num_incr_by() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":"b","b":[{"a":2i64}, {"a":5i64}, {"a":"c"}]}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_numincrby_a: RedisResult<String> = con.json_num_incr_by(TEST_KEY, "$.a", 2);

    // cannot increment a string
    assert_eq!(json_numincrby_a, Ok("[null]".into()));

    let json_numincrby_b: RedisResult<String> = con.json_num_incr_by(TEST_KEY, "$..a", 2);

    // however numbers can be incremented
    assert_eq!(json_numincrby_b, Ok("[null,4,7,null]".into()));
}

#[test]
fn test_module_json_obj_keys() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[3i64], "nested": {"a": {"b":2i64, "c": 1i64}}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_objkeys: RedisResult<Value> = con.json_obj_keys(TEST_KEY, "$..a");

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
fn test_module_json_obj_len() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[3i64], "nested": {"a": {"b":2i64, "c": 1i64}}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_objlen: RedisResult<Value> = con.json_obj_len(TEST_KEY, "$..a");

    assert_eq!(json_objlen, Ok(Bulk(vec![Nil, Int(2)])));
}

#[test]
fn test_module_json_set() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set: RedisResult<bool> = con.json_set(TEST_KEY, "$", &json!({"key": "value"}));

    assert_eq!(set, Ok(true));
}

#[test]
fn test_module_json_str_append() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31i64}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_strappend: RedisResult<Value> = con.json_str_append(TEST_KEY, "$..a", "\"baz\"");

    assert_eq!(json_strappend, Ok(Bulk(vec![Int(6), Int(8), Nil])));

    let json_get_check: RedisResult<String> = con.json_get(TEST_KEY, "$");

    assert_eq!(
        json_get_check,
        Ok("[{\"a\":\"foobaz\",\"nested\":{\"a\":\"hellobaz\"},\"nested2\":{\"a\":31}}]".into())
    );
}

#[test]
fn test_module_json_str_len() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31i32}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_strlen: RedisResult<Value> = con.json_str_len(TEST_KEY, "$..a");

    assert_eq!(json_strlen, Ok(Bulk(vec![Int(3), Int(5), Nil])));
}

#[test]
fn test_module_json_toggle() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", &json!({"bool": true}));

    assert_eq!(set_initial, Ok(true));

    let json_toggle_a: RedisResult<Value> = con.json_toggle(TEST_KEY, "$.bool");
    assert_eq!(json_toggle_a, Ok(Bulk(vec![Int(0)])));

    let json_toggle_b: RedisResult<Value> = con.json_toggle(TEST_KEY, "$.bool");
    assert_eq!(json_toggle_b, Ok(Bulk(vec![Int(1)])));
}

#[test]
fn test_module_json_type() {
    let ctx = TestContext::with_modules(&[Module::Json], MTLS_NOT_ENABLED);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":2i64, "nested": {"a": true}, "foo": "bar"}),
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
            Data(Vec::from("boolean".as_bytes()))
        ]))
    );

    let json_type_c: RedisResult<Value> = con.json_type(TEST_KEY, "$..dummy");

    assert_eq!(json_type_c, Ok(Bulk(vec![])));
}
