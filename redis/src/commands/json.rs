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
        let mut cmd = cmd("JSON.ARRAPPEND");

        cmd.arg(key)
           .arg(path)
           .arg(serde_json::to_string(value)?);

        Ok::<_, RedisError>(cmd)
    }

    /// Index array at `path`, returns first occurance of `value`
    fn json_arr_index<K: ToRedisArgs, P: ToRedisArgs, V: Serialize>(key: K, path: P, value: &'a V) {
        let mut cmd = cmd("JSON.ARRINDEX");

        cmd.arg(key)
           .arg(path)
           .arg(serde_json::to_string(value)?);

        Ok::<_, RedisError>(cmd)
    }

    /// Same as `json_arr_index` except takes a `start` and a `stop` value, setting these to `0` will mean
    /// they make no effect on the query
    ///
    /// The default values for `start` and `stop` are `0`, so pass those in if you want them to take no effect
    fn json_arr_index_ss<K: ToRedisArgs, P: ToRedisArgs, V: Serialize>(key: K, path: P, value: &'a V, start: &'a isize, stop: &'a isize) {
        let mut cmd = cmd("JSON.ARRINDEX");
    
        cmd.arg(key)
           .arg(path)
           .arg(serde_json::to_string(value)?)
           .arg(start)
           .arg(stop);

        Ok::<_, RedisError>(cmd)
    }

    /// Inserts the JSON `value` in the array at `path` before the `index` (shifts to the right).
    ///
    /// `index` must be withing the array's range.
    fn json_arr_insert<K: ToRedisArgs, P: ToRedisArgs, V: Serialize>(key: K, path: P, index: i64, value: &'a V) {
        let mut cmd = cmd("JSON.ARRINSERT");
        
        cmd.arg(key)
           .arg(path)
           .arg(index)
           .arg(serde_json::to_string(value)?);

        Ok::<_, RedisError>(cmd)
        
    }

    /// Reports the length of the JSON Array at `path` in `key`.
    fn json_arr_len<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
        let mut cmd = cmd("JSON.ARRLEN");

        cmd.arg(key)
           .arg(path);

        Ok::<_, RedisError>(cmd)
    }

    /// Removes and returns an element from the `index` in the array.
    ///
    /// `index` defaults to `-1` (the end of the array).
    fn json_arr_pop<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P, index: i64) {
        let mut cmd = cmd("JSON.ARRPOP");

        cmd.arg(key)
           .arg(path)
           .arg(index);

        Ok::<_, RedisError>(cmd)
    }

    /// Trims an array so that it contains only the specified inclusive range of elements.
    ///
    /// This command is extremely forgiving and using it with out-of-range indexes will not produce an error.
    /// There are a few differences between how RedisJSON v2.0 and legacy versions handle out-of-range indexes.
    fn json_arr_trim<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P, start: i64, stop: i64) {
        let mut cmd = cmd("JSON.ARRTRIM");

        cmd.arg(key)
           .arg(path)
           .arg(start)
           .arg(stop);

        Ok::<_, RedisError>(cmd)
    }

    /// Clears container values (Arrays/Objects), and sets numeric values to 0.
    fn json_clear<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
        let mut cmd = cmd("JSON.CLEAR");

        cmd.arg(key)
           .arg(path);

        Ok::<_, RedisError>(cmd)
    }

    /// Deletes a value at `path`.
    fn json_del<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
        let mut cmd = cmd("JSON.DEL");

        cmd.arg(key)
           .arg(path);

        Ok::<_, RedisError>(cmd)
    }

    /// Gets JSON Value(s) at `path`.
    ///
    /// Runs `JSON.GET` is key is singular, `JSON.MGET` if there are multiple keys.
    fn json_get<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
        let mut cmd = cmd(if key.is_single_arg() { "JSON.GET" } else { "JSON.MGET" });

        cmd.arg(key)
           .arg(path);

        Ok::<_, RedisError>(cmd)
    }

    /// Increments the number value stored at `path` by `number`.
    fn json_num_incr_by<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P, value: i64) {
        let mut cmd = cmd("JSON.NUMINCRBY");

        cmd.arg(key)
           .arg(path)
           .arg(value);

        Ok::<_, RedisError>(cmd)
    }   

    /// Returns the keys in the object that's referenced by `path`.
    fn json_obj_keys<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
        let mut cmd = cmd("JSON.OBJKEYS");

        cmd.arg(key)
           .arg(path);

        Ok::<_, RedisError>(cmd)
    }

    /// Reports the number of keys in the JSON Object at `path` in `key`.
    fn json_obj_len<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
        let mut cmd = cmd("JSON.OBJLEN");

        cmd.arg(key)
           .arg(path);

        Ok::<_, RedisError>(cmd)
    }

    /// Sets the JSON Value at `path` in `key`.
    fn json_set<K: ToRedisArgs, P: ToRedisArgs, V: Serialize>(key: K, path: P, value: &'a V) {
        let mut cmd = cmd("JSON.SET");

        cmd.arg(key)
           .arg(path)
           .arg(serde_json::to_string(value)?);

        Ok::<_, RedisError>(cmd)
    }

    /// Appends the `json-string` values to the string at `path`.
    fn json_str_append<K: ToRedisArgs, P: ToRedisArgs, V: ToRedisArgs>(key: K, path: P, value: V) {
        let mut cmd = cmd("JSON.STRAPPEND");

        cmd.arg(key)
           .arg(path)
           .arg(value);

        Ok::<_, RedisError>(cmd)
    }

    /// Reports the length of the JSON String at `path` in `key`.
    fn json_str_len<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
        let mut cmd = cmd("JSON.STRLEN");

        cmd.arg(key)
           .arg(path);

        Ok::<_, RedisError>(cmd)
    }

    /// Toggle a `boolean` value stored at `path`.
    fn json_toggle<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
        let mut cmd = cmd("JSON.TOGGLE");

        cmd.arg(key)
           .arg(path);

        Ok::<_, RedisError>(cmd)
    }

    /// Reports the type of JSON value at `path`.
    fn json_type<K: ToRedisArgs, P: ToRedisArgs>(key: K, path: P) {
        let mut cmd = cmd("JSON.TYPE");

        cmd.arg(key)
           .arg(path);

        Ok::<_, RedisError>(cmd)
    }
}

impl<T> JsonCommands for T where T: ConnectionLike {}

#[cfg(feature = "aio")]
impl<T> JsonAsyncCommands for T where T: crate::aio::ConnectionLike + Send + Sized {}
