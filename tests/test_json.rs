#![cfg(feature = "json")]

use redis::{Commands, RedisResult};

use crate::support::*;
mod support;

// adds json! macro for quick json generation on the fly.
use serde_json::json;

const TEST_KEY: &str = "my_json";

#[test]
fn test_json_set() {
	let ctx = TestContext::new();
	let mut con = ctx.connection();

	let result: RedisResult<bool> = con.json_set(TEST_KEY, "$", json!({"key": "value"}));

	assert_eq!(result, Ok(true));
}