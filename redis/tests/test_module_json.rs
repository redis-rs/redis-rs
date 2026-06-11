#![cfg(feature = "json")]

use redis::{
    Commands, ExistenceCheck, FphaInput, FphaType, JsonCommands, JsonSetOptions, ValueType,
};
use std::assert_eq;
use std::collections::HashMap;
use std::f32::consts::PI;

use redis::{
    ErrorKind, RedisResult,
    Value::{self, *},
};
use redis_test::server::Module;

use crate::support::*;
mod support;

use serde::Serialize;
// adds json! macro for quick json generation on the fly.
use serde_json::json;

const TEST_KEY: &str = "my_json";

#[test]
fn test_module_json_serialize_error() {
    let ctx = TestContext::with_modules(&[Module::Json]);
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

    let set_invalid = con
        .json_set::<_, _, _, bool>(TEST_KEY, "$", &test_invalid_value)
        .unwrap_err();

    assert_eq!(set_invalid.kind(), ErrorKind::Serialize);
    assert_eq!(
        set_invalid.to_string(),
        String::from("key must be a string")
    );
}

#[test]
fn test_module_json_arr_append() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1i64], "nested": {"a": [1i64, 2i64]}, "nested2": {"a": 42i64}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_append: RedisResult<Value> = con.json_arr_append(TEST_KEY, "$..a", &3i64);

    assert_eq!(json_append, Ok(Array(vec![Int(2i64), Int(3i64), Nil])));
}

#[test]
fn test_module_json_arr_index() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1i64, 2i64, 3i64, 2i64], "nested": {"a": [3i64, 4i64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrindex: RedisResult<Value> = con.json_arr_index(TEST_KEY, "$..a", &2i64);

    assert_eq!(json_arrindex, Ok(Array(vec![Int(1i64), Int(-1i64)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1i64, 2i64, 3i64, 2i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrindex_2: RedisResult<Value> =
        con.json_arr_index_ss(TEST_KEY, "$..a", &2i64, &0, &0);

    assert_eq!(json_arrindex_2, Ok(Array(vec![Int(1i64), Nil])));
}

#[test]
fn test_module_json_arr_insert() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[3i64], "nested": {"a": [3i64 ,4i64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrinsert: RedisResult<Value> = con.json_arr_insert(TEST_KEY, "$..a", 0, &1i64);

    assert_eq!(json_arrinsert, Ok(Array(vec![Int(2), Int(3)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1i64 ,2i64 ,3i64 ,2i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrinsert_2: RedisResult<Value> = con.json_arr_insert(TEST_KEY, "$..a", 0, &1i64);

    assert_eq!(json_arrinsert_2, Ok(Array(vec![Int(5), Nil])));
}

#[test]
fn test_module_json_arr_len() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": [3i64], "nested": {"a": [3i64, 4i64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrlen: RedisResult<Value> = con.json_arr_len(TEST_KEY, "$..a");

    assert_eq!(json_arrlen, Ok(Array(vec![Int(1), Int(2)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": [1i64, 2i64, 3i64, 2i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrlen_2: RedisResult<Value> = con.json_arr_len(TEST_KEY, "$..a");

    assert_eq!(json_arrlen_2, Ok(Array(vec![Int(4), Nil])));
}

#[test]
fn test_module_json_arr_pop() {
    let ctx = TestContext::with_modules(&[Module::Json]);
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
        Ok(Array(vec![
            // convert string 3 to its ascii value as bytes
            BulkString(Vec::from("3".as_bytes())),
            BulkString(Vec::from("4".as_bytes()))
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
        Ok(Array(vec![
            BulkString(Vec::from("\"bar\"".as_bytes())),
            Nil,
            Nil
        ]))
    );
}

#[test]
fn test_module_json_arr_trim() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": [], "nested": {"a": [1i64, 4u64]}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_arrtrim: RedisResult<Value> = con.json_arr_trim(TEST_KEY, "$..a", 1, 1);

    assert_eq!(json_arrtrim, Ok(Array(vec![Int(0), Int(1)])));

    let update_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": [1i64, 2i64, 3i64, 4i64], "nested": {"a": false}}),
    );

    assert_eq!(update_initial, Ok(true));

    let json_arrtrim_2: RedisResult<Value> = con.json_arr_trim(TEST_KEY, "$..a", 1, 1);

    assert_eq!(json_arrtrim_2, Ok(Array(vec![Int(1), Nil])));
}

#[test]
fn test_module_json_clear() {
    let ctx = TestContext::with_modules(&[Module::Json]);
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
        // its not really a problem if you're just deserializing it anyway but still
        // kinda weird
        Ok("[{\"arr\":[],\"bool\":true,\"float\":0,\"int\":0,\"obj\":{},\"str\":\"foo\"}]".into())
    );
}

#[test]
fn test_module_json_del() {
    let ctx = TestContext::with_modules(&[Module::Json]);
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
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":2i64, "b": 3i64, "nested": {"a": 4i64, "b": null}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_get: RedisResult<String> = con.json_get(TEST_KEY, "$..b");

    assert_eq!(json_get, Ok("[3,null]".into()));

    let json_get_multi: RedisResult<String> = con.json_get(TEST_KEY, &["..a", "$..b"]);

    if json_get_multi != Ok("{\"$..b\":[3,null],\"..a\":[2,4]}".into())
        && json_get_multi != Ok("{\"..a\":[2,4],\"$..b\":[3,null]}".into())
    {
        panic!("test_error: incorrect response from json_get_multi");
    }
}

#[test]
fn test_module_json_mget() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_mset(&[
        (
            format!("{TEST_KEY}-a"),
            "$",
            &json!({"a":1i64, "b": 2i64, "nested": {"a": 3i64, "b": null}}),
        ),
        (
            format!("{TEST_KEY}-b"),
            "$",
            &json!({"a":4i64, "b": 5i64, "nested": {"a": 6i64, "b": null}}),
        ),
    ]);

    assert_eq!(set_initial, Ok(true));

    let json_mget: RedisResult<Value> = con.json_mget(
        vec![format!("{TEST_KEY}-a"), format!("{TEST_KEY}-b")],
        "$..a",
    );

    assert_eq!(
        json_mget,
        Ok(Array(vec![
            BulkString(Vec::from("[1,3]".as_bytes())),
            BulkString(Vec::from("[4,6]".as_bytes()))
        ]))
    );
}

#[test]
fn test_module_json_num_incr_by() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":"b","b":[{"a":2i64}, {"a":5i64}, {"a":"c"}]}),
    );

    assert_eq!(set_initial, Ok(true));

    if ctx.protocol.supports_resp3() && ctx.get_version() >= REDIS_VERSION_CE_7_0 {
        // cannot increment a string
        let json_numincrby_a: RedisResult<Vec<Value>> = con.json_num_incr_by(TEST_KEY, "$.a", 2);
        assert_eq!(json_numincrby_a, Ok(vec![Nil]));

        let json_numincrby_b: RedisResult<Vec<Value>> = con.json_num_incr_by(TEST_KEY, "$..a", 2);

        // however numbers can be incremented
        assert_eq!(json_numincrby_b, Ok(vec![Nil, Int(4), Int(7), Nil]));
    } else {
        // cannot increment a string
        let json_numincrby_a: RedisResult<String> = con.json_num_incr_by(TEST_KEY, "$.a", 2);
        assert_eq!(json_numincrby_a, Ok("[null]".into()));

        let json_numincrby_b: RedisResult<String> = con.json_num_incr_by(TEST_KEY, "$..a", 2);

        // however numbers can be incremented
        assert_eq!(json_numincrby_b, Ok("[null,4,7,null]".into()));
    }
}

#[test]
fn test_module_json_obj_keys() {
    let ctx = TestContext::with_modules(&[Module::Json]);
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
        Ok(Array(vec![
            Nil,
            Array(vec![
                BulkString(Vec::from("b".as_bytes())),
                BulkString(Vec::from("c".as_bytes()))
            ])
        ]))
    );
}

#[test]
fn test_module_json_obj_len() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[3i64], "nested": {"a": {"b":2i64, "c": 1i64}}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_objlen: RedisResult<Value> = con.json_obj_len(TEST_KEY, "$..a");

    assert_eq!(json_objlen, Ok(Array(vec![Nil, Int(2)])));
}

#[test]
fn test_module_json_set() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set: RedisResult<bool> = con.json_set(TEST_KEY, "$", &json!({"key": "value"}));

    assert_eq!(set, Ok(true));
}

#[test]
fn test_module_json_str_append() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31i64}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_strappend: RedisResult<Value> = con.json_str_append(TEST_KEY, "$..a", "\"baz\"");

    assert_eq!(json_strappend, Ok(Array(vec![Int(6), Int(8), Nil])));

    let json_get_check: RedisResult<String> = con.json_get(TEST_KEY, "$");

    assert_eq!(
        json_get_check,
        Ok("[{\"a\":\"foobaz\",\"nested\":{\"a\":\"hellobaz\"},\"nested2\":{\"a\":31}}]".into())
    );
}

#[test]
fn test_module_json_str_len() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":"foo", "nested": {"a": "hello"}, "nested2": {"a": 31i32}}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_strlen: RedisResult<Value> = con.json_str_len(TEST_KEY, "$..a");

    assert_eq!(json_strlen, Ok(Array(vec![Int(3), Int(5), Nil])));
}

#[test]
fn test_module_json_toggle() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", &json!({"bool": true}));

    assert_eq!(set_initial, Ok(true));

    let json_toggle_a: RedisResult<Value> = con.json_toggle(TEST_KEY, "$.bool");
    assert_eq!(json_toggle_a, Ok(Array(vec![Int(0)])));

    let json_toggle_b: RedisResult<Value> = con.json_toggle(TEST_KEY, "$.bool");
    assert_eq!(json_toggle_b, Ok(Array(vec![Int(1)])));
}

#[test]
fn test_module_json_type() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let set_initial: RedisResult<bool> = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":2i64, "nested": {"a": true}, "foo": "bar"}),
    );

    assert_eq!(set_initial, Ok(true));

    let json_type_a: RedisResult<Value> = con.json_type(TEST_KEY, "$..foo");
    let json_type_b: RedisResult<Value> = con.json_type(TEST_KEY, "$..a");
    let json_type_c: RedisResult<Value> = con.json_type(TEST_KEY, "$..dummy");

    if ctx.protocol.supports_resp3() && ctx.get_version() >= REDIS_VERSION_CE_7_0 {
        // In RESP3 current RedisJSON always gives response in an array.
        assert_eq!(
            json_type_a,
            Ok(Array(vec![Array(vec![BulkString(Vec::from(
                "string".as_bytes()
            ))])]))
        );

        assert_eq!(
            json_type_b,
            Ok(Array(vec![Array(vec![
                BulkString(Vec::from("integer".as_bytes())),
                BulkString(Vec::from("boolean".as_bytes()))
            ])]))
        );
        assert_eq!(json_type_c, Ok(Array(vec![Array(vec![])])));
    } else {
        assert_eq!(
            json_type_a,
            Ok(Array(vec![BulkString(Vec::from("string".as_bytes()))]))
        );

        assert_eq!(
            json_type_b,
            Ok(Array(vec![
                BulkString(Vec::from("integer".as_bytes())),
                BulkString(Vec::from("boolean".as_bytes()))
            ]))
        );
        assert_eq!(json_type_c, Ok(Array(vec![])));
    }

    // Checking the type of the key as a whole
    let key_type: RedisResult<ValueType> = con.key_type(TEST_KEY);
    assert_eq!(key_type, Ok(ValueType::JSON));
}

#[test]
fn test_module_json_set_options_json_value() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let opts = JsonSetOptions::json(&json!({"a": 1, "b": [2, 3]})).unwrap();
    let set_result: RedisResult<bool> = con.json_set_options(TEST_KEY, "$", &opts);
    assert_eq!(set_result, Ok(true));

    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok(r#"[{"a":1,"b":[2,3]}]"#.to_string()));
}

#[test]
fn test_module_json_set_options_nx_xx() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    // XX on a missing key should not create the key.
    let opts_xx = JsonSetOptions::json(&json!({"v": 0}))
        .unwrap()
        .conditional_set(ExistenceCheck::XX);
    let _: RedisResult<redis::Value> = con.json_set_options(TEST_KEY, "$", &opts_xx);
    let key_exists: RedisResult<bool> = con.exists(TEST_KEY);
    assert_eq!(key_exists, Ok(false));

    // NX on a fresh key should succeed.
    let opts_nx = JsonSetOptions::json(&json!({"v": 1}))
        .unwrap()
        .conditional_set(ExistenceCheck::NX);
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts_nx),
        Ok(true),
    );
    let key_exists: RedisResult<bool> = con.exists(TEST_KEY);
    assert_eq!(key_exists, Ok(true));

    // NX again must be a no-op because the key exists.
    let opts_nx = JsonSetOptions::json(&json!({"v": 999}))
        .unwrap()
        .conditional_set(ExistenceCheck::NX);
    let _: RedisResult<redis::Value> = con.json_set_options(TEST_KEY, "$", &opts_nx);
    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$.v");
    assert_eq!(get_result, Ok("[1]".to_string()));

    // XX on the existing key should succeed and overwrite the value.
    let opts_xx = JsonSetOptions::json(&json!({"v": 2}))
        .unwrap()
        .conditional_set(ExistenceCheck::XX);
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts_xx),
        Ok(true),
    );
    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$.v");
    assert_eq!(get_result, Ok("[2]".to_string()));
}

// FPHA integration tests.

// The value travels as a JSON array of numbers.
// The `FPHA <TYPE>` token is a storage hint that asks the server to pack the array internally as bf16/fp16/fp32/fp64 lanes.
// Round-trip via `JSON.GET` returns the array as a JSON array of numbers.
#[rstest::rstest]
#[case::fp32(FphaInput::Fp32(&[1.0_f32, 2.0, -3.5]))]
#[case::fp64(FphaInput::Fp64(&[1.0_f64, 2.0, -3.5]))]
#[case::bf16(FphaInput::Bf16(&[1.0_f32, 2.0, -3.5]))]
#[case::fp16(FphaInput::Fp16(&[1.0_f32, 2.0, -3.5]))]
fn test_module_json_set_fpha_roundtrip(#[case] input: FphaInput<'static>) {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let opts = JsonSetOptions::fpha(input).unwrap();
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts),
        Ok(true),
    );

    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok("[[1.0,2.0,-3.5]]".to_string()));
}

#[test]
fn test_module_json_set_fpha_empty_payload() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let opts = JsonSetOptions::fpha(FphaInput::Fp32(&[])).unwrap();
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts),
        Ok(true),
    );

    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok("[[]]".to_string()));
}

#[test]
fn test_module_json_set_fpha_with_existence_check() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    // XX against a missing key must not create it.
    let lanes = [1.0_f32];
    let opts_xx = JsonSetOptions::fpha(FphaInput::Fp32(&lanes))
        .unwrap()
        .conditional_set(ExistenceCheck::XX);
    let _: RedisResult<redis::Value> = con.json_set_options(TEST_KEY, "$", &opts_xx);
    let key_exists: RedisResult<bool> = con.exists(TEST_KEY);
    assert_eq!(key_exists, Ok(false));

    // NX on the same missing key creates it.
    let lanes = [1.0_f32, 2.0, 3.0];
    let opts_nx = JsonSetOptions::fpha(FphaInput::Fp32(&lanes))
        .unwrap()
        .conditional_set(ExistenceCheck::NX);
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts_nx),
        Ok(true),
    );
    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok("[[1.0,2.0,3.0]]".to_string()));
}

// FP16 storage range is ±65504.
// A value outside that range must be rejected by the server with an out-of-range error.
#[test]
fn test_module_json_set_fpha_fp16_overflow() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let lanes = [70000.0_f32];
    let opts = JsonSetOptions::fpha(FphaInput::Fp16(&lanes)).unwrap();
    let set_result: RedisResult<redis::Value> = con.json_set_options(TEST_KEY, "$", &opts);
    assert!(
        set_result.is_err(),
        "expected server error, got {set_result:?}"
    );

    let key_exists: RedisResult<bool> = con.exists(TEST_KEY);
    assert_eq!(key_exists, Ok(false));
}

// Per the FPHA docs:
// "If at least one value in the FP array does not fit the FPHA type, the command errors."

// Verify that a single out-of-range element in an otherwise valid payload rejects the whole command and leaves the key untouched.
#[test]
fn test_module_json_set_fpha_fp16_partial_overflow() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let lanes = [1.0_f32, 2.0, 70000.0, 3.0];
    let opts = JsonSetOptions::fpha(FphaInput::Fp16(&lanes)).unwrap();
    let set_result: RedisResult<redis::Value> = con.json_set_options(TEST_KEY, "$", &opts);
    assert!(
        set_result.is_err(),
        "expected server error, got {set_result:?}"
    );

    let key_exists: RedisResult<bool> = con.exists(TEST_KEY);
    assert_eq!(key_exists, Ok(false));
}

// 65504 is the largest finite value representable in IEEE-754 binary16.
#[test]
fn test_module_json_set_fpha_fp16_max_boundary() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let lanes = [65504.0_f32];
    let opts = JsonSetOptions::fpha(FphaInput::Fp16(&lanes)).unwrap();
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts),
        Ok(true),
    );

    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok("[[65504.0]]".to_string()));
}

// 3.4e38 is near the largest finite value representable in IEEE-754 binary32.
#[test]
fn test_module_json_set_fpha_fp32_max_boundary() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let lanes = [3.4e38_f32];
    let opts = JsonSetOptions::fpha(FphaInput::Fp32(&lanes)).unwrap();
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts),
        Ok(true),
    );

    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok("[[3.4e38]]".to_string()));
}

// 2^20 (= 1048576) is exactly representable in bf16 and well above FP16's ±65504 limit.
// The same value would be rejected under FPHA FP16 but under FPHA BF16 it round-trips losslessly.
#[test]
fn test_module_json_set_fpha_bf16_above_fp16_range() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let lanes = [1048576.0_f32];
    let opts = JsonSetOptions::fpha(FphaInput::Bf16(&lanes)).unwrap();
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts),
        Ok(true),
    );

    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok("[[1048576.0]]".to_string()));
}

// Values that serde_json emits in scientific notation must be accepted by the server and round-tripped back as scientific notation.
// Note: serde emits `6.022e+23` while the server omits the `+`.
#[test]
fn test_module_json_set_fpha_fp32_scientific_notation() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let lanes = [1e-10_f32, 6.022e23_f32];
    let opts = JsonSetOptions::fpha(FphaInput::Fp32(&lanes)).unwrap();
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts),
        Ok(true),
    );

    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok("[[1e-10,6.022e23]]".to_string()));
}

// Demonstrate the lossy nature of FPHA BF16 storage.

// bf16 has a 7-bit mantissa.
// Around 100 its step size is 0.5, so 100.7 snaps to 100.5
// pi (3.1415927) snaps to 3.14.
#[test]
fn test_module_json_set_fpha_bf16_truncation() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let lanes = [100.7_f32, PI];
    let opts = JsonSetOptions::fpha(FphaInput::Bf16(&lanes)).unwrap();
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts),
        Ok(true),
    );

    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok("[[100.5,3.14]]".to_string()));
}

// fp16 has a 10-bit mantissa, so it preserves more precision than bf16.
// Around 1.0 its step size is ~0.001, which snaps 1.0009766 to 1.001, pi still snaps to 3.14.
#[test]
fn test_module_json_set_fpha_fp16_truncation() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let lanes = [1.0009766_f32, PI];
    let opts = JsonSetOptions::fpha(FphaInput::Fp16(&lanes)).unwrap();
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts),
        Ok(true),
    );

    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok("[[1.001,3.14]]".to_string()));
}

// `JsonSetOptions::fpha_serialize` accepts arbitrary serializable values.
// A 2-D matrix exercises the docs' "all FP arrays in value" wording - the hint is applied to every inner array.
// With BF16, 100.7 snaps to 100.5 and pi snaps to 3.14 within their respective inner arrays.
#[test]
fn test_module_json_set_fpha_serialize_matrix() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let matrix: &[&[f32]] = &[&[1.0, 100.7], &[PI, 4.0]];
    let opts = JsonSetOptions::fpha_serialize(matrix, FphaType::Bf16).unwrap();
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts),
        Ok(true),
    );

    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok("[[[1.0,100.5],[3.14,4.0]]]".to_string()));
}

// An object holding multiple FP-array fields gets the storage hint applied to each field independently.
#[test]
fn test_module_json_set_fpha_serialize_object_with_array_fields() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let value = serde_json::json!({"weights": [1.0, 2.0, 3.0], "bias": [0.5, 0.25]});
    let opts = JsonSetOptions::fpha_serialize(&value, FphaType::Fp16).unwrap();
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts),
        Ok(true),
    );

    // `serde_json::Value::Object` is a BTreeMap, so keys serialize in
    // alphabetical order (`bias` before `weights`) regardless of input order.
    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(
        get_result,
        Ok(r#"[{"bias":[0.5,0.25],"weights":[1.0,2.0,3.0]}]"#.to_string()),
    );
}

// Per the FPHA docs:
// "If at least one value in the FP array does not fit the FPHA type, the command errors."

// Verify that a single out-of-range value causes the entire command to fail without modifying the key, even when the offending value appears in a nested array.
#[test]
fn test_module_json_set_fpha_serialize_nested_partial_overflow() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let matrix: &[&[f32]] = &[&[1.0, 2.0], &[70000.0, 3.0]];
    let opts = JsonSetOptions::fpha_serialize(matrix, FphaType::Fp16).unwrap();
    let set_result: RedisResult<redis::Value> = con.json_set_options(TEST_KEY, "$", &opts);
    assert!(
        set_result.is_err(),
        "expected server error, got {set_result:?}"
    );

    let key_exists: RedisResult<bool> = con.exists(TEST_KEY);
    assert_eq!(key_exists, Ok(false));
}

// A scalar (not an array) is also a valid FPHA payload server-side.
#[test]
fn test_module_json_set_fpha_serialize_scalar() {
    let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8, &[Module::Json]);
    let mut con = ctx.connection();

    let opts = JsonSetOptions::fpha_serialize(&1.5_f32, FphaType::Fp32).unwrap();
    assert_eq!(
        con.json_set_options::<_, _, bool>(TEST_KEY, "$", &opts),
        Ok(true),
    );

    let get_result: RedisResult<String> = con.json_get(TEST_KEY, "$");
    assert_eq!(get_result, Ok("[1.5]".to_string()));
}
