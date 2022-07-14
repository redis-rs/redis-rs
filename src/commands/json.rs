// can't use rustfmt here because it screws up the file.
#![cfg_attr(rustfmt, rustfmt_skip)]
use crate::cmd::{cmd, Cmd, Iter};
use crate::connection::{ConnectionLike};
use crate::pipeline::Pipeline;
use crate::types::{FromRedisValue, RedisResult, ToRedisArgs};

use serde_json;
use serde::ser::Serialize;

implement_commands! {
    'a

	/// Append the JSON `value` to the array at `path` after the last element in it.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_arrappend<K: ToRedisArgs, P: ToString, V: Serialize>(key: K, path: P, value: V) {
		cmd("JSON.ARRAPPEND").arg(key).arg(path.to_string()).arg(serde_json::to_string(&value).unwrap())
	}

	/// Index array at `path`, returns first occurance of `value`
	/// Pass `None` to `start_stop` to default it to `0, 0` (the redis default in this command)
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_arrindex<K: ToRedisArgs, P: ToString, V: Serialize>(key: K, path: P, value: V) {
		cmd("JSON.ARRINDEX").arg(key).arg(path.to_string()).arg(serde_json::to_string(&value).unwrap())
	}

	/// Inserts the JSON `value` in the array at `path` before the `index` (shifts to the right)
	/// `index` must be withing the array's range.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_arrinsert<K: ToRedisArgs, P: ToString, V: Serialize>(key: K, path: P, index: i64, value: V) {
		cmd("JSON.ARRINSERT").arg(key).arg(path.to_string()).arg(index).arg(serde_json::to_string(&value).unwrap())
	}

	/// Reports the length of the JSON Array at `path` in `key`.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_arrlen<K: ToRedisArgs, P: ToString>(key: K, path: P) {
		cmd("JSON.ARRLEN").arg(key).arg(path.to_string())
	}

	/// Removes and returns an element from the `index` in the array.
	/// `index` defaults to `-1` (the end of the array)
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_arrpop<K: ToRedisArgs, P: ToString>(key: K, path: P, index: Option<i64>) {
		cmd("JSON.ARRPOP").arg(key).arg(path.to_string()).arg(index.unwrap_or(-1))
	}

	/// Trims an array so that it contains only the specified inclusive range of elements.
	///
	/// This command is extremely forgiving and using it with out-of-range indexes will not produce an error.
	/// There are a few differences between how RedisJSON v2.0 and legacy versions handle out-of-range indexes.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_arrtrim<K: ToRedisArgs, P: ToString>(key: K, path: P, start_stop: (i64, i64)) {
		cmd("JSON.ARRTRIM").arg(key).arg(path.to_string()).arg(start_stop.0).arg(start_stop.1)
	}

	/// Clears container values (Arrays/Objects), and sets numeric values to 0.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_clear<K: ToRedisArgs, P: ToString>(key: K, path: P) {
		cmd("JSON.CLEAR").arg(key).arg(path.to_string())
	}

	/// Deletes a value at `path`.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_del<K: ToRedisArgs, P: ToString>(key: K, path: P) {
		cmd("JSON.DEL").arg(key).arg(path.to_string())
	}

	/// Returns the value at `path` in JSON serialized form.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_get<K: ToRedisArgs, P: ToString>(key: K, path: P) {
		cmd("JSON.GET").arg(key).arg(path.to_string())
	}

	/// Returns the values at `path` from multiple `key` arguments.
	/// Returns `null` for nonexistent keys and nonexistent paths.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_mget<K: ToRedisArgs, P: ToString>(keys: Vec<K>, path: P) {
		cmd("JSON.MGET").arg(keys).arg(path.to_string())
	}

	/// Increments the number value stored at `path` by `number`.
	fn json_numincrby<K: ToRedisArgs, P: ToString>(key: K, path: P, value: i64) {
		cmd("JSON.NUMINCRBY").arg(key).arg(path.to_string()).arg(value)
	}   

	/// Returns the keys in the object that's referenced by `path`.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_objkeys<K: ToRedisArgs, P: ToString>(key: K, path: P) {
		cmd("JSON.OBJKEYS").arg(key).arg(path.to_string())
	}

	/// Reports the number of keys in the JSON Object at `path` in `key`.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_objlen<K: ToRedisArgs, P: ToString>(key: K, path: P) {
		cmd("JSON.OBJLEN").arg(key).arg(path.to_string())
	}

	/// Sets the JSON Value at `path` in `key`
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_set<K: ToRedisArgs, P: ToString, V: Serialize>(key: K, path: P, value: V) {
		cmd("JSON.SET").arg(key).arg(path.to_string()).arg(serde_json::to_string(&value).unwrap())
	}

	/// Appends the `json-string` values to the string at `path`.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_strappend<K: ToRedisArgs, P: ToString>(key: K, path: P, value: P) {
		cmd("JSON.STRAPPEND").arg(key).arg(path.to_string()).arg(value.to_string())
	}

	/// Reports the length of the JSON String at `path` in `key`.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_strlen<K: ToRedisArgs, P: ToString>(key: K, path: P) {
		cmd("JSON.STRLEN").arg(key).arg(path.to_string())
	}

	/// Toggle a `boolean` value stored at `path`.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_toggle<K: ToRedisArgs, P: ToString>(key: K, path: P) {
		cmd("JSON.TOGGLE").arg(key).arg(path.to_string())
	}

	/// Reports the type of JSON value at `path`s.
	#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
	fn json_type<K: ToRedisArgs, P: ToString>(key: K, path: P) {
		cmd("JSON.TYPE").arg(key).arg(path.to_string())
	}
}


impl<T> Commands for T where T: ConnectionLike {}

#[cfg(feature = "aio")]
impl<T> AsyncCommands for T where T: crate::aio::ConnectionLike + Send + Sized {}