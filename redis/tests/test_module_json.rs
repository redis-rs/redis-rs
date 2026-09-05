#![cfg(feature = "json")]

use redis::json::{FphaType, JsonSetOptions, RedisJsonType};
use redis::{ExistenceCheck, TypedCommands};
use redis_test::server::Module;
use redis_test::{REDIS_CE_8_8, REDIS_JSON_8_8, TestContextBuilder, run_test_if_version_supported};
use std::assert_eq;
use std::collections::HashMap;
use std::f32::consts::PI;

use redis::ErrorKind;

mod support;

// adds json! macro for quick json generation on the fly.
use serde_json::json;

const TEST_KEY: &str = "my_json";

#[test]
fn test_module_json_serialize_error() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    // Maps in JSON need to have string keys. So the following will fail to serialize.
    let unserializable: HashMap<Option<bool>, i64> = HashMap::from([(None, 42)]);

    let err = con.json_set(TEST_KEY, "$", &unserializable).unwrap_err();

    assert_eq!(err.kind(), ErrorKind::Serialize);
    assert_eq!(err.to_string(), String::from("key must be a string"));
}

#[test]
fn test_module_json_arr_append() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":[1], "nested": {"a": [1, 2]}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_arr_append(TEST_KEY, ".a", &4711).unwrap();
    assert_eq!(*result, vec![Some(2)]);

    // Testing a $-path
    let result = con.json_arr_append(TEST_KEY, "$..a", &3).unwrap();
    assert_eq!(*result, vec![Some(3), Some(3), None]); // 3 for the first item, as the .-path command run also added an item
}

#[test]
fn test_module_json_arr_index() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":[1, 2, 3, 2], "nested": {"a": [3, 4]}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_arr_index(TEST_KEY, ".a", &2).unwrap();

    assert_eq!(*result, vec![Some(1)]);
    // Testing a $-path
    let result = con.json_arr_index(TEST_KEY, "$..a", &2).unwrap();
    assert_eq!(*result, vec![Some(1), Some(-1), None]);
}

#[test]
fn test_module_json_arr_index_ss() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_arr_index_ss(TEST_KEY, ".a", &2, &2, &4).unwrap();
    assert_eq!(*result, vec![Some(3)]);

    // Testing a $-path
    let result = con.json_arr_index_ss(TEST_KEY, "$..a", &2, &2, &4).unwrap();
    assert_eq!(*result, vec![Some(3), None, None]);
}

#[test]
fn test_module_json_arr_insert() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_arr_insert(TEST_KEY, ".a", 2, &1).unwrap();
    assert_eq!(*result, vec![Some(5)]);

    // Testing a $-path
    let result = con.json_arr_insert(TEST_KEY, "$..a", 0, &1).unwrap();
    assert_eq!(*result, vec![Some(6), None, None]); // 6 for the first item, as the .-path command run also added an item
}

#[test]
fn test_module_json_arr_len() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_arr_len(TEST_KEY, ".a").unwrap();
    assert_eq!(*result, vec![Some(4)]);

    // Testing a $-path
    let result = con.json_arr_len(TEST_KEY, "$..a").unwrap();
    assert_eq!(*result, vec![Some(4), None, None]);
}

#[test]
fn test_module_json_arr_pop() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_arr_pop(TEST_KEY, ".a", -1).unwrap();
    assert_eq!(*result, vec![Some("2".to_string())]);

    // Testing a $-path
    let result = con.json_arr_pop(TEST_KEY, "$..a", -1).unwrap();
    assert_eq!(*result, vec![Some("3".to_string()), None, None]); // "3 for the first item", as the .-path command run also took an item
}

#[test]
fn test_module_json_arr_trim() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_arr_trim(TEST_KEY, ".a", 1, 2).unwrap();
    assert_eq!(*result, vec![Some(2)]);

    // Testing a $-path
    let result = con.json_arr_trim(TEST_KEY, "$..a", 1, 2).unwrap();
    assert_eq!(*result, vec![Some(1), None, None]); // 1 for the first item, as the .-path command run trimmed to 2 elements, and we're trying to take the 2nd (which exists) and 3rd (which does no longer exist)
}

#[test]
fn test_module_json_clear() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_clear(TEST_KEY, ".a").unwrap();
    assert_eq!(result, 1);

    // Testing a $-path
    let result = con.json_clear(TEST_KEY, "$..a").unwrap();
    assert_eq!(result, 1); // 1, as the .-path command run took the main `a`, and `nested.a` is not numeric
}

#[test]
fn test_module_json_del() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_del(TEST_KEY, ".a").unwrap();
    assert_eq!(result, 1);

    // Testing a $-path
    let result = con.json_del(TEST_KEY, "$..a").unwrap();
    assert_eq!(result, 2); // 2, as the .-path command run took the main `a`
}

#[test]
fn test_module_json_get() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_get(TEST_KEY, ".a").unwrap();
    assert_eq!(result, "[1,2,3,2]");

    // Testing a $-path
    let result = con.json_get(TEST_KEY, "$..a").unwrap();
    assert_eq!(result, "[[1,2,3,2],\"foo\",42]");

    // Testing multiple paths
    let paths = [".nested.a", "$..a", ".nested2"];
    let result = con.json_get(TEST_KEY, &paths).unwrap();
    // As the result is a serialized object, the keys don't have a fixed order in the serialization.
    // So we parse it to check reliably.
    let parsed_result: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(
        parsed_result,
        json!({
            ".nested.a": ["foo"],
            "$..a": [[1, 2, 3, 2], "foo", 42],
            ".nested2": [{"a": 42}],
        })
    );
}

#[test]
fn test_module_json_mget() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let keys = [
        format!("{TEST_KEY}-a"),
        format!("{TEST_KEY}-b"),
        format!("{TEST_KEY}-c"),
    ];
    let setup = con
        .json_mset(&[
            (
                &keys[0],
                "$",
                &json!({"a":1, "b": 2, "nested": {"a": 3, "b": null}}),
            ),
            (
                &keys[1],
                "$",
                &json!({"a":4, "b": 5, "nested": {"a": 6, "b": null}}),
            ),
        ])
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_mget(&keys, ".a").unwrap();
    assert_eq!(
        result,
        vec![Some("1".to_string()), Some("4".to_string()), None]
    );

    // Testing a $-path
    let result = con.json_mget(&keys, "$..a").unwrap();
    assert_eq!(
        result,
        vec![Some("[1,3]".to_string()), Some("[4,6]".to_string()), None]
    );
}

#[test]
fn test_module_json_num_incr_by() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a": 4711, "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    if ctx.protocol.supports_resp3() {
        // Testing a .-path
        let result = con.json_num_incr_by(TEST_KEY, ".a", 42).unwrap();
        assert_eq!(*result, vec![Some("4753".to_string())]);

        // Testing a $-path
        let result = con.json_num_incr_by(TEST_KEY, "$..a", 42).unwrap();
        assert_eq!(
            *result,
            vec![Some("4795".to_string()), None, Some("84".to_string())]
        ); // 4795 for the first item, as the .-path command run already increased 4711 to 4753
    } else {
        // Testing a .-path
        let result = con.json_num_incr_by(TEST_KEY, ".a", 42).unwrap();
        assert_eq!(*result, vec![Some("4753".to_string())]);

        // Testing a $-path
        let result = con.json_num_incr_by(TEST_KEY, "$..a", 42).unwrap();
        assert_eq!(*result, vec![Some("[4795,null,84]".to_string())]); // 4795 for the first item, as the .-path command run already increased 4711 to 4753
    }
}

#[test]
fn test_module_json_obj_keys() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":[3], "nested": {"a": {"b":2, "c": 1}}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_obj_keys(TEST_KEY, ".nested.a").unwrap();
    assert_eq!(*result, vec![Some(vec!["b".to_string(), "c".to_string()])]);

    // Testing a $-path
    let result = con.json_obj_keys(TEST_KEY, "$..a").unwrap();
    assert_eq!(
        *result,
        vec![None, Some(vec!["b".to_string(), "c".to_string()])]
    );
}

#[test]
fn test_module_json_obj_len() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a":{ "foo": 42, "bar": 4711, "nested": {"a": 23}}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_obj_len(TEST_KEY, ".a").unwrap();
    assert_eq!(*result, vec![Some(3)]);

    // Testing a $-path
    let result = con.json_obj_len(TEST_KEY, "$..a").unwrap();
    assert_eq!(*result, vec![Some(3), None]);
}

#[test]
fn test_module_json_set() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let result = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a": 4711, "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(result);
}

#[test]
fn test_module_json_str_append() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a": 4711, "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con
        .json_str_append(TEST_KEY, ".nested.a", "\"bar\"")
        .unwrap();
    assert_eq!(*result, vec![Some(6)]);

    // Testing a $-path
    let result = con.json_str_append(TEST_KEY, "$..a", "\"baz\"").unwrap();
    assert_eq!(*result, vec![None, Some(9), None]); // 9 for the 2nd item, as the .-path command run already added "bar"
}

#[test]
fn test_module_json_str_len() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a": 4711, "nested": {"a": "foo"}, "nested2": {"a": 42}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_str_len(TEST_KEY, ".nested.a").unwrap();
    assert_eq!(*result, vec![Some(3)]);

    // Testing a $-path
    let result = con.json_str_len(TEST_KEY, "$..a").unwrap();
    assert_eq!(*result, vec![None, Some(3), None]); // 9 for the 2nd item, as the .-path command run already added "bar"
}

#[test]
fn test_module_json_toggle() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a": true, "nested": {"a": "foo"}, "nested2": {"a": true}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_toggle(TEST_KEY, ".a").unwrap();
    assert_eq!(*result, vec![Some(false)]);

    // Testing a $-path
    let result = con.json_toggle(TEST_KEY, "$..a").unwrap();
    assert_eq!(*result, vec![Some(true), None, Some(false)]); // true for the first item, as the .-path command run already toggled it
}

#[test]
fn test_module_json_type() {
    let ctx = TestContextBuilder::new().module(Module::Json).build();
    let mut con = ctx.connection();

    let setup = con
        .json_set(
            TEST_KEY,
            "$",
            &json!({"a": true, "nested": {"a": "foo"}, "nested2": {"a": 4711}}),
        )
        .unwrap();
    assert!(setup);

    // Testing a .-path
    let result = con.json_type(TEST_KEY, ".a").unwrap();
    assert_eq!(*result, vec![RedisJsonType::Boolean]);

    // Testing a $-path
    let result = con.json_type(TEST_KEY, "$..a").unwrap();
    assert_eq!(
        *result,
        vec![
            RedisJsonType::Boolean,
            RedisJsonType::String,
            RedisJsonType::Integer,
        ]
    );
}

#[test]
fn test_module_json_set_options_json_value() {
    let ctx = TestContextBuilder::new().modules(&[Module::Json]).build();
    let mut con = ctx.connection();

    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &json!({"a": 1, "b": [2, 3]}),
            &JsonSetOptions::default(),
        )
        .unwrap();
    assert!(set_result);

    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, r#"[{"a":1,"b":[2,3]}]"#);
}

#[test]
fn test_module_json_set_options_nx_xx() {
    let ctx = TestContextBuilder::new().modules(&[Module::Json]).build();
    let mut con = ctx.connection();

    let opts_xx = JsonSetOptions::default().conditional_set(ExistenceCheck::XX);
    let opts_nx = JsonSetOptions::default().conditional_set(ExistenceCheck::NX);

    // XX on a missing key should not create the key.
    let set_result = con
        .json_set_options(TEST_KEY, "$", &json!({"v": 0}), &opts_xx)
        .unwrap();
    assert!(!set_result);
    let key_exists = con.exists(TEST_KEY).unwrap();
    assert!(!key_exists);

    // NX on a fresh key should succeed.
    let set_result = con
        .json_set_options(TEST_KEY, "$", &json!({"v": 1}), &opts_nx)
        .unwrap();
    assert!(set_result);
    let key_exists = con.exists(TEST_KEY).unwrap();
    assert!(key_exists);

    // NX again must be a no-op because the key exists.
    let set_result = con
        .json_set_options(TEST_KEY, "$", &json!({"v": 999}), &opts_nx)
        .unwrap();
    assert!(!set_result);
    let get_result = con.json_get(TEST_KEY, "$.v").unwrap();
    assert_eq!(&get_result, "[1]");

    // XX on the existing key should succeed and overwrite the value.
    let set_result = con
        .json_set_options(TEST_KEY, "$", &json!({"v": 2}), &opts_xx)
        .unwrap();
    assert!(set_result);
    let get_result = con.json_get(TEST_KEY, "$.v").unwrap();
    assert_eq!(&get_result, "[2]");
}

// FPHA integration tests.

// The value travels as a JSON array of numbers.
// The `FPHA <TYPE>` token is a storage hint that asks the server to pack the array internally as bf16/fp16/fp32/fp64 lanes.
// Round-trip via `JSON.GET` returns the array as a JSON array of numbers.
#[rstest::rstest]
#[case::fp32(FphaType::Fp32)]
#[case::fp64(FphaType::Fp64)]
#[case::bf16(FphaType::Bf16)]
#[case::fp16(FphaType::Fp16)]
fn test_module_json_set_fpha_roundtrip(#[case] fpha_type: FphaType) {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &[1.0_f32, 2.0, -3.5],
            &JsonSetOptions::default().fpha(fpha_type),
        )
        .unwrap();
    assert!(set_result);

    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, "[[1.0,2.0,-3.5]]");
}

#[test]
fn test_module_json_set_fpha_empty_payload() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &[0_f32; 0],
            &JsonSetOptions::default().fpha(FphaType::Fp32),
        )
        .unwrap();
    assert!(set_result);

    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, "[[]]");
}

#[test]
fn test_module_json_set_fpha_with_existence_check() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    // XX against a missing key must not create it.
    let opts_xx = JsonSetOptions::default()
        .fpha(FphaType::Fp32)
        .conditional_set(ExistenceCheck::XX);
    let set_result = con
        .json_set_options(TEST_KEY, "$", &[1.0_f32], &opts_xx)
        .unwrap();
    assert!(!set_result);
    let key_exists: bool = con.exists(TEST_KEY).unwrap();
    assert!(!key_exists);

    // NX on the same missing key creates it.
    let opts_nx = JsonSetOptions::default()
        .fpha(FphaType::Fp32)
        .conditional_set(ExistenceCheck::NX);
    let set_result = con
        .json_set_options(TEST_KEY, "$", &[1.0_f32, 2.0, 3.0], &opts_nx)
        .unwrap();
    assert!(set_result);
    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, "[[1.0,2.0,3.0]]");
}

// FP16 storage range is ±65504.
// A value outside that range must be rejected by the server with an out-of-range error.
#[test]
fn test_module_json_set_fpha_fp16_overflow() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let error = con
        .json_set_options(
            TEST_KEY,
            "$",
            &[70000.0_f32],
            &JsonSetOptions::default().fpha(FphaType::Fp16),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("out of range for F16"),
        "unexpected error message: {error}",
    );

    let key_exists = con.exists(TEST_KEY).unwrap();
    assert!(!key_exists);
}

// Per the FPHA docs:
// "If at least one value in the FP array does not fit the FPHA type, the command errors."

// Verify that a single out-of-range element in an otherwise valid payload rejects the whole command and leaves the key untouched.
#[test]
fn test_module_json_set_fpha_fp16_partial_overflow() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    con.json_set_options(
        TEST_KEY,
        "$",
        &[1.0_f32, 2.0, 70000.0, 3.0],
        &JsonSetOptions::default().fpha(FphaType::Fp16),
    )
    .unwrap_err();

    let key_exists = con.exists(TEST_KEY).unwrap();
    assert!(!key_exists);
}

// 65504 is the largest finite value representable in IEEE-754 binary16.
#[test]
fn test_module_json_set_fpha_fp16_max_boundary() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &[65504.0_f32],
            &JsonSetOptions::default().fpha(FphaType::Fp16),
        )
        .unwrap();
    assert!(set_result);

    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, "[[65504.0]]");
}

// 3.4e38 is near the largest finite value representable in IEEE-754 binary32.
#[test]
fn test_module_json_set_fpha_fp32_max_boundary() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &[3.4e38_f32],
            &JsonSetOptions::default().fpha(FphaType::Fp32),
        )
        .unwrap();
    assert!(set_result);

    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, "[[3.4e38]]");
}

// 2^20 (= 1048576) is exactly representable in bf16 and well above FP16's ±65504 limit.
// The same value would be rejected under FPHA FP16 but under FPHA BF16 it round-trips losslessly.
#[test]
fn test_module_json_set_fpha_bf16_above_fp16_range() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &[1048576.0_f32],
            &JsonSetOptions::default().fpha(FphaType::Bf16),
        )
        .unwrap();
    assert!(set_result);

    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, "[[1048576.0]]");
}

// Values that serde_json emits in scientific notation must be accepted by the server and round-tripped back as scientific notation.
// Note: serde emits `6.022e+23` while the server omits the `+`.
#[test]
fn test_module_json_set_fpha_fp32_scientific_notation() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &[1e-10_f32, 6.022e23_f32],
            &JsonSetOptions::default().fpha(FphaType::Fp32),
        )
        .unwrap();
    assert!(set_result);

    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, "[[1e-10,6.022e23]]");
}

// Demonstrate the lossy nature of FPHA BF16 storage.

// bf16 has a 7-bit mantissa.
// Around 100 its step size is 0.5, so 100.7 snaps to 100.5
// pi (3.1415927) snaps to 3.14.
#[test]
fn test_module_json_set_fpha_bf16_truncation() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &[100.7_f32, PI],
            &JsonSetOptions::default().fpha(FphaType::Bf16),
        )
        .unwrap();
    assert!(set_result);

    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, "[[100.5,3.14]]");
}

// fp16 has a 10-bit mantissa, so it preserves more precision than bf16.
// Around 1.0 its step size is ~0.001, which snaps 1.0009766 to 1.001, pi still snaps to 3.14.
#[test]
fn test_module_json_set_fpha_fp16_truncation() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &[1.0009766_f32, PI],
            &JsonSetOptions::default().fpha(FphaType::Fp16),
        )
        .unwrap();
    assert!(set_result);

    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, "[[1.001,3.14]]");
}

// The FPHA hint applies to any serializable value, not just flat slices.
// A 2-D matrix exercises the docs' "all FP arrays in value" wording - the hint is applied to every inner array.
// With BF16, 100.7 snaps to 100.5 and pi snaps to 3.14 within their respective inner arrays.
#[test]
fn test_module_json_set_fpha_matrix() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let matrix: &[&[f32]] = &[&[1.0, 100.7], &[PI, 4.0]];
    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &matrix,
            &JsonSetOptions::default().fpha(FphaType::Bf16),
        )
        .unwrap();
    assert!(set_result);
    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, "[[[1.0,100.5],[3.14,4.0]]]");
}

// An object holding multiple FP-array fields gets the storage hint applied to each field independently.
#[test]
fn test_module_json_set_fpha_object_with_array_fields() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &serde_json::json!({"weights": [1.0, 2.0, 3.0], "bias": [0.5, 0.25]}),
            &JsonSetOptions::default().fpha(FphaType::Fp16),
        )
        .unwrap();
    assert!(set_result);

    // `serde_json::Value::Object` is a BTreeMap, so keys serialize in
    // alphabetical order (`bias` before `weights`) regardless of input order.
    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(
        &get_result,
        r#"[{"bias":[0.5,0.25],"weights":[1.0,2.0,3.0]}]"#,
    );
}

// Per the FPHA docs:
// "If at least one value in the FP array does not fit the FPHA type, the command errors."

// Verify that a single out-of-range value causes the entire command to fail without modifying the key, even when the offending value appears in a nested array.
#[test]
fn test_module_json_set_fpha_nested_partial_overflow() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let matrix: &[&[f32]] = &[&[1.0, 2.0], &[70000.0, 3.0]];
    con.json_set_options(
        TEST_KEY,
        "$",
        &matrix,
        &JsonSetOptions::default().fpha(FphaType::Fp16),
    )
    .unwrap_err();

    let key_exists = con.exists(TEST_KEY).unwrap();
    assert!(!key_exists);
}

// A scalar (not an array) is also a valid FPHA payload server-side.
#[test]
fn test_module_json_set_fpha_scalar() {
    let ctx =
        run_test_if_version_supported!([&[REDIS_CE_8_8][..], &[REDIS_JSON_8_8]], &[Module::Json]);
    let mut con = ctx.connection();

    let set_result = con
        .json_set_options(
            TEST_KEY,
            "$",
            &1.5_f32,
            &JsonSetOptions::default().fpha(FphaType::Fp32),
        )
        .unwrap();
    assert!(set_result);

    let get_result = con.json_get(TEST_KEY, "$").unwrap();
    assert_eq!(&get_result, "[1.5]");
}
