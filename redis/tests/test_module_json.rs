#![cfg(feature = "json")]

use redis::{ErrorKind, RedisResult, TypedCommands};
use redis_test::server::Module;
use std::assert_eq;
use std::collections::HashMap;

use crate::support::*;
mod support;

// adds json! macro for quick json generation on the fly.
use serde_json::json;

const TEST_KEY: &str = "my_json";

#[test]
fn test_module_json_serialize_error() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    // Maps in JSON need to have string keys. So the following will fail to serialize.
    let unserializable: HashMap<Option<bool>, i64> = HashMap::from([(None, 42)]);

    let err = con.json_set(TEST_KEY, "$", &unserializable).unwrap_err();

    assert_eq!(err.kind(), ErrorKind::Serialize);
    assert_eq!(err.to_string(), String::from("key must be a string"));
}

#[test]
fn test_module_json_arr_append() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1], "nested": {"a": [1, 2]}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_arr_append(TEST_KEY, ".a", &4711).unwrap();
    assert_eq!(*result, vec![Some(2)]);

    // Testing a $-path
    let result = con.json_arr_append(TEST_KEY, "$..a", &3).unwrap();
    assert_eq!(*result, vec![Some(3), Some(3), None]); // 3 for the first item, as the .-path command run also added an item
}

#[test]
fn test_module_json_arr_index() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1, 2, 3, 2], "nested": {"a": [3, 4]}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_arr_index(TEST_KEY, ".a", &2).unwrap();

    assert_eq!(*result, vec![Some(1)]);
    // Testing a $-path
    let result = con.json_arr_index(TEST_KEY, "$..a", &2).unwrap();
    assert_eq!(*result, vec![Some(1), Some(-1), None]);
}

#[test]
fn test_module_json_arr_index_ss() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_arr_index_ss(TEST_KEY, ".a", &2, &2, &4).unwrap();
    assert_eq!(*result, vec![Some(3)]);

    // Testing a $-path
    let result = con.json_arr_index_ss(TEST_KEY, "$..a", &2, &2, &4).unwrap();
    assert_eq!(*result, vec![Some(3), None, None]);
}

#[test]
fn test_module_json_arr_insert() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_arr_insert(TEST_KEY, ".a", 2, &1).unwrap();
    assert_eq!(*result, vec![Some(5)]);

    // Testing a $-path
    let result = con.json_arr_insert(TEST_KEY, "$..a", 0, &1).unwrap();
    assert_eq!(*result, vec![Some(6), None, None]); // 6 for the first item, as the .-path command run also added an item
}

#[test]
fn test_module_json_arr_len() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_arr_len(TEST_KEY, ".a").unwrap();
    assert_eq!(*result, vec![Some(4)]);

    // Testing a $-path
    let result = con.json_arr_len(TEST_KEY, "$..a").unwrap();
    assert_eq!(*result, vec![Some(4), None, None]);
}

#[test]
fn test_module_json_arr_pop() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_arr_pop(TEST_KEY, ".a", -1).unwrap();
    assert_eq!(*result, vec![Some("2".to_string())]);

    // Testing a $-path
    let result = con.json_arr_pop(TEST_KEY, "$..a", -1).unwrap();
    assert_eq!(*result, vec![Some("3".to_string()), None, None]); // "3 for the first item", as the .-path command run also took an item
}

#[test]
fn test_module_json_arr_trim() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_arr_trim(TEST_KEY, ".a", 1, 2).unwrap();
    assert_eq!(*result, vec![Some(2)]);

    // Testing a $-path
    let result = con.json_arr_trim(TEST_KEY, "$..a", 1, 2).unwrap();
    assert_eq!(*result, vec![Some(1), None, None]); // 1 for the first item, as the .-path command run trimmed to 2 elements, and we're trying to take the 2nd (which exists) and 3rd (which does no longer exist)
}

#[test]
fn test_module_json_clear() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_clear(TEST_KEY, ".a").unwrap();
    assert_eq!(result, 1);

    // Testing a $-path
    let result = con.json_clear(TEST_KEY, "$..a").unwrap();
    assert_eq!(result, 1); // 1, as the .-path command run took the main `a`, and `nested.a` is not numeric
}

#[test]
fn test_module_json_del() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_del(TEST_KEY, ".a").unwrap();
    assert_eq!(result, 1);

    // Testing a $-path
    let result = con.json_del(TEST_KEY, "$..a").unwrap();
    assert_eq!(result, 2); // 2, as the .-path command run took the main `a`
}

#[test]
fn test_module_json_get() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[1, 2, 3, 2], "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

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
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let keys = [
        format!("{TEST_KEY}-a"),
        format!("{TEST_KEY}-b"),
        format!("{TEST_KEY}-c"),
    ];
    let setup: RedisResult<bool> = con.json_mset(&[
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
    ]);
    assert_eq!(setup, Ok(true));

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
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": 4711, "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

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
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":[3], "nested": {"a": {"b":2, "c": 1}}}),
    );
    assert_eq!(setup, Ok(true));

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
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a":{ "foo": 42, "bar": 4711, "nested": {"a": 23}}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_obj_len(TEST_KEY, ".a").unwrap();
    assert_eq!(*result, vec![Some(3)]);

    // Testing a $-path
    let result = con.json_obj_len(TEST_KEY, "$..a").unwrap();
    assert_eq!(*result, vec![Some(3), None]);
}

#[test]
fn test_module_json_set() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let result = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": 4711, "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(result, Ok(true));
}

#[test]
fn test_module_json_str_append() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": 4711, "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

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
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": 4711, "nested": {"a": "foo"}, "nested2": {"a": 42}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_str_len(TEST_KEY, ".nested.a").unwrap();
    assert_eq!(*result, vec![Some(3)]);

    // Testing a $-path
    let result = con.json_str_len(TEST_KEY, "$..a").unwrap();
    assert_eq!(*result, vec![None, Some(3), None]); // 9 for the 2nd item, as the .-path command run already added "bar"
}

#[test]
fn test_module_json_toggle() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": true, "nested": {"a": "foo"}, "nested2": {"a": true}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_toggle(TEST_KEY, ".a").unwrap();
    assert_eq!(*result, vec![Some(false)]);

    // Testing a $-path
    let result = con.json_toggle(TEST_KEY, "$..a").unwrap();
    assert_eq!(*result, vec![Some(true), None, Some(false)]); // true for the first item, as the .-path command run already toggled it
}

#[test]
fn test_module_json_type() {
    let ctx = TestContext::with_modules(&[Module::Json]);
    let mut con = ctx.connection();

    let setup = con.json_set(
        TEST_KEY,
        "$",
        &json!({"a": true, "nested": {"a": "foo"}, "nested2": {"a": 4711}}),
    );
    assert_eq!(setup, Ok(true));

    // Testing a .-path
    let result = con.json_type(TEST_KEY, ".a").unwrap();
    assert_eq!(*result, vec![vec!["boolean".to_string()]]);

    // Testing a $-path
    let result = con.json_type(TEST_KEY, "$..a").unwrap();
    assert_eq!(
        *result,
        vec![vec![
            "boolean".to_string(),
            "string".to_string(),
            "integer".to_string(),
        ]]
    );
}
