#![cfg(feature = "json")]

use std::assert_eq;

use redis::{Commands, RedisResult, Value::{self, *}};

use crate::support::*;
mod support;

// adds json! macro for quick json generation on the fly.
use serde_json::{self, json};

const TEST_KEY: &str = "my_json";

#[test]
fn test_json_arrappend() {
	let ctx = TestContext::new();
	let mut con = ctx.connection();

	let set_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"a":[1i64], "nested": {"a": [1i64, 2i64]}, "nested2": {"a": 42i64}}));

	assert_eq!(set_initial, Ok(true));

	let json_append: RedisResult<Value> = con.json_arrappend(TEST_KEY, "$..a", 3i64);

	assert_eq!(json_append, 
		Ok(
			Bulk(vec![
				Int(2i64), Int(3i64), Nil
			])
		)
	);
}

#[test]
fn test_json_arrindex() {
	let ctx = TestContext::new();
	let mut con = ctx.connection();

	let set_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"a":[1i64, 2i64, 3i64, 2i64], "nested": {"a": [3i64, 4i64]}}));

	assert_eq!(set_initial, Ok(true));

	let json_arrindex: RedisResult<Value> = con.json_arrindex(TEST_KEY, "$..a", 2i64);

	assert_eq!(json_arrindex, Ok(Bulk(vec![Int(1i64), Int(-1i64)])));

	let update_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"a":[1i64, 2i64, 3i64, 2i64], "nested": {"a": false}}));

	assert_eq!(update_initial, Ok(true));

	let json_arrindex_2: RedisResult<Value> = con.json_arrindex(TEST_KEY, "$..a", 2i64);

	assert_eq!(json_arrindex_2, Ok(Bulk(vec![Int(1i64), Nil])));
}

#[test]
fn test_json_arrinsert() {
	let ctx = TestContext::new();
	let mut con = ctx.connection();

	let set_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"a":[3i64], "nested": {"a": [3i64 ,4i64]}}));

	assert_eq!(set_initial, Ok(true));

	let json_arrinsert: RedisResult<Value> = con.json_arrinsert(TEST_KEY, "$..a", 0, 1i64);

	assert_eq!(json_arrinsert, Ok(Bulk(vec![Int(2), Int(3)])));

	let update_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"a":[1i64 ,2i64 ,3i64 ,2i64], "nested": {"a": false}}));

	assert_eq!(update_initial, Ok(true));

	let json_arrinsert_2: RedisResult<Value> = con.json_arrinsert(TEST_KEY, "$..a", 0, 1i64);

	assert_eq!(json_arrinsert_2, Ok(Bulk(vec![Int(5), Nil])));
}

#[test]
fn test_json_arrlen() {
	let ctx = TestContext::new();
	let mut con = ctx.connection();

	let set_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"a": [3i64], "nested": {"a": [3i64, 4i64]}}));

	assert_eq!(set_initial, Ok(true));

	let json_arrlen: RedisResult<Value> = con.json_arrlen(TEST_KEY, "$..a");

	assert_eq!(json_arrlen, Ok(Bulk(vec![Int(1), Int(2)])));

	let update_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"a": [1i64, 2i64, 3i64, 2i64], "nested": {"a": false}}));

	assert_eq!(update_initial, Ok(true));

	let json_arrlen_2: RedisResult<Value> = con.json_arrlen(TEST_KEY, "$..a");

	assert_eq!(json_arrlen_2, Ok(Bulk(vec![Int(4), Nil])));
}

#[test]
fn test_json_arrpop() {
	let ctx = TestContext::new();
	let mut con = ctx.connection();

	let set_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"a": [3i64], "nested": {"a": [3i64, 4i64]}}));

	assert_eq!(set_initial, Ok(true));

	let json_arrpop: RedisResult<Value> = con.json_arrpop(TEST_KEY, "$..a", None);

	assert_eq!(json_arrpop, 
		Ok(
			Bulk(vec![
				// convert string 3 to its ascii value as bytes
				Data(Vec::from("3".as_bytes())), 
				Data(Vec::from("4".as_bytes()))
			])
		)
	);

	let update_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"a":["foo", "bar"], "nested": {"a": false}, "nested2": {"a":[]}}));

	assert_eq!(update_initial, Ok(true));

	let json_arrpop_2: RedisResult<Value> = con.json_arrpop(TEST_KEY, "$..a", None);

	assert_eq!(json_arrpop_2, 
		Ok(
			Bulk(vec![
				Data(Vec::from("\"bar\"".as_bytes())), 
				Nil, 
				Nil
			])
		)
	);
}

#[test]
fn test_json_arrtrim() {
	let ctx = TestContext::new();
	let mut con = ctx.connection();

	let set_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"a": [], "nested": {"a": [1i64, 4u64]}}));

	assert_eq!(set_initial, Ok(true));

	let json_arrtrim: RedisResult<Value> = con.json_arrtrim(TEST_KEY, "$..a", (1, 1));

	assert_eq!(json_arrtrim, 
		Ok(
			Bulk(vec![
				Int(0), Int(1)
			])
		)
	);

	let update_initial: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"a": [1i64, 2i64, 3i64, 4i64], "nested": {"a": false}}));

	assert_eq!(update_initial, Ok(true));

	let json_arrtrim_2: RedisResult<Value> = con.json_arrtrim(TEST_KEY, "$..a", (1, 1));

	assert_eq!(json_arrtrim_2,
		Ok(
			Bulk(vec![
				Int(1), Nil
			])
		)
	);
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
	assert_eq!(checking_value, Ok(json!([{"obj":{},"arr":[],"str":"foo","bool":true,"int":0i64,"float":0i64}]).to_string()));
}

#[test]
fn test_json_del() {
	
}

#[test]
fn test_json_set() {
	let ctx = TestContext::new();
	let mut con = ctx.connection();

	let result: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"key": "value"}));

	assert_eq!(result, Ok(true));
}

