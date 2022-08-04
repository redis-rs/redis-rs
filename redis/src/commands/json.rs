// can't use rustfmt here because it screws up the file.
#![cfg_attr(rustfmt, rustfmt_skip)]
use crate::cmd::{cmd, Cmd};
use crate::connection::ConnectionLike;
use crate::pipeline::Pipeline;
use crate::types::{FromRedisValue, RedisResult, ToRedisArgs};
use crate::{RedisError, ErrorKind};

#[cfg(feature = "cluster")]
use crate::commands::ClusterPipeline;

use serde::ser::Serialize;

impl From<serde_json::Error> for RedisError {
	fn from(serde_err: serde_json::Error) -> RedisError {
		RedisError::from((
			ErrorKind::Serialize,
			"Serialization Error",
			format!("{}", serde_err)
		))
	}
}

implement_json_commands! {
    'a
	
	/// Append the JSON `value` to the array at `path` after the last element in it.
	fn json_arr_append<K: ToRedisArgs, P: ToRedisArgs, V: Serialize>(key: K, path: P, value: &'a V) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.ARRAPPEND")
				.arg_take(key)
				.arg_take(path)
				.arg_take(serde_json::to_string(value)?)
		)
	}

	/// Index array at `path`, returns first occurance of `value`
	///
	/// Pass `None` to `start_stop` to default it to `0, 0` (the Redis default in this command).
	fn json_arr_index<K: ToRedisArgs, P: ToRedisArgs, V: Serialize>(key: K, path: P, value: &'a V) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.ARRINDEX")
				.arg_take(key)
				.arg_take(path)
				.arg_take(serde_json::to_string(value)?)
		)
	}

	/// Inserts the JSON `value` in the array at `path` before the `index` (shifts to the right)
	/// `index` must be withing the array's range.
	fn json_arr_insert<K: ToRedisArgs, P: ToRedisArgs, V: Serialize>(key: K, path: P, index: i64, value: &'a V) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.ARRINSERT")
				.arg_take(key)
				.arg_take(path)
				.arg_take(index)
				.arg_take(serde_json::to_string(value)?)
		)
	}

	/// Reports the length of the JSON Array at `path` in `key`.
	fn json_arr_len<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.ARRLEN")
				.arg_take(key)
				.arg_take(path)
		)
	}

	/// Removes and returns an element from the `index` in the array.
	/// `index` defaults to `-1` (the end of the array)
	fn json_arr_pop<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P, index: i64) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.ARRPOP")
				.arg_take(key)
				.arg_take(path)
				.arg_take(index)
		)
	}

	/// Trims an array so that it contains only the specified inclusive range of elements.
	///
	/// This command is extremely forgiving and using it with out-of-range indexes will not produce an error.
	/// There are a few differences between how RedisJSON v2.0 and legacy versions handle out-of-range indexes.
	fn json_arr_trim<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P, start: i64, stop: i64) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.ARRTRIM")
				.arg_take(key)
				.arg_take(path)
				.arg_take(start)
				.arg_take(stop)
		)
	}

	/// Clears container values (Arrays/Objects), and sets numeric values to 0.
	fn json_clear<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.CLEAR")
				.arg_take(key)
				.arg_take(path)
		)
	}

	/// Deletes a value at `path`.
	fn json_del<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.DEL")
				.arg_take(key)
				.arg_take(path)
		)
	}

	/// Gets JSON Value(s) at `path`
	/// Runs JSON.GET is key is singular, Runs JSON.MGET if there are multiple keys
	fn json_get<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
		Ok::<Cmd, RedisError>(
			cmd(if key.is_single_arg() { "JSON.GET" } else { "JSON.MGET" })
				.arg_take(key)
				.arg_take(path)
		)
	}

	/// Increments the number value stored at `path` by `number`.
	fn json_num_incr_by<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P, value: i64) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.NUMINCRBY")
				.arg_take(key)
				.arg_take(path)
				.arg_take(value)
		)
	}   

	/// Returns the keys in the object that's referenced by `path`.
	fn json_obj_keys<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.OBJKEYS")
				.arg_take(key)
				.arg_take(path)
		)
	}

	/// Reports the number of keys in the JSON Object at `path` in `key`.
	fn json_obj_len<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.OBJLEN")
				.arg_take(key)
				.arg_take(path)
		)
	}

	/// Sets the JSON Value at `path` in `key`
	fn json_set<K: ToRedisArgs, P: ToRedisArgs, V: Serialize>(key: K, path: P, value: &'a V) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.SET")
				.arg_take(key)
				.arg_take(path)
				.arg_take(serde_json::to_string(value)?)
		)
	}

	/// Appends the `json-string` values to the string at `path`.
	fn json_str_append<K: ToRedisArgs, P: ToRedisArgs, V: ToRedisArgs>(key: K, path: P, value: V) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.STRAPPEND")
				.arg_take(key)
				.arg_take(path)
				.arg_take(value)
		)
	}

	/// Reports the length of the JSON String at `path` in `key`.
	fn json_str_len<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.STRLEN")
				.arg_take(key)
				.arg_take(path)
		)
	}

	/// Toggle a `boolean` value stored at `path`.
	fn json_toggle<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.TOGGLE")
				.arg_take(key)
				.arg_take(path)
		)
	}

	/// Reports the type of JSON value at `path`.
	fn json_type<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
		Ok::<Cmd, RedisError>(
			cmd("JSON.TYPE")
				.arg_take(key)
				.arg_take(path)
		)
	}
}

impl<T> JsonCommands for T where T: ConnectionLike {}

#[cfg(feature = "aio")]
impl<T> JsonAsyncCommands for T where T: crate::aio::ConnectionLike + Send + Sized {}
