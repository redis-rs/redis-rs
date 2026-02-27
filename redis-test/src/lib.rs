//! Testing support
//!
//! This module provides `MockRedisConnection` which implements ConnectionLike and can be
//! used in the same place as any other type that behaves like a Redis connection. This is useful
//! for writing unit tests without needing a Redis server.
//!
//! # Example
//!
//! ```rust
//! use redis::{ConnectionLike, RedisError};
//! use redis_test::{MockCmd, MockRedisConnection};
//!
//! fn my_exists<C: ConnectionLike>(conn: &mut C, key: &str) -> Result<bool, RedisError> {
//!     let exists: bool = redis::cmd("EXISTS").arg(key).query(conn)?;
//!     Ok(exists)
//! }
//!
//! let mut mock_connection = MockRedisConnection::new(vec![
//!     MockCmd::new(redis::cmd("EXISTS").arg("foo"), Ok("1")),
//! ]);
//!
//! let result = my_exists(&mut mock_connection, "foo").unwrap();
//! assert_eq!(result, true);
//! ```

pub mod cluster;
pub mod sentinel;
pub mod server;
pub mod utils;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use redis::{
    Cmd, ConnectionLike, ErrorKind, Pipeline, RedisError, RedisResult, ServerError, Value,
};

#[cfg(feature = "aio")]
use futures::{FutureExt, future};

#[cfg(feature = "aio")]
use redis::{RedisFuture, aio::ConnectionLike as AioConnectionLike};

/// Helper trait for converting test values into a `redis::Value` returned from a
/// `MockRedisConnection`. This is necessary because neither `redis::types::ToRedisArgs`
/// nor `redis::types::FromRedisValue` performs the precise conversion needed.
pub trait IntoRedisValue {
    /// Convert a value into `redis::Value`.
    fn into_redis_value(self) -> Value;
}

macro_rules! into_redis_value_impl_int {
    ($t:ty) => {
        impl IntoRedisValue for $t {
            fn into_redis_value(self) -> Value {
                Value::Int(self as i64)
            }
        }
    };
}

into_redis_value_impl_int!(i8);
into_redis_value_impl_int!(i16);
into_redis_value_impl_int!(i32);
into_redis_value_impl_int!(i64);
into_redis_value_impl_int!(u8);
into_redis_value_impl_int!(u16);
into_redis_value_impl_int!(u32);

macro_rules! into_redis_value_impl_float {
    ($t:ty) => {
        impl IntoRedisValue for $t {
            fn into_redis_value(self) -> Value {
                Value::Double(self as f64)
            }
        }
    };
}

into_redis_value_impl_float!(f32);
into_redis_value_impl_float!(f64);

impl IntoRedisValue for String {
    fn into_redis_value(self) -> Value {
        Value::BulkString(self.as_bytes().to_vec())
    }
}

impl IntoRedisValue for &str {
    fn into_redis_value(self) -> Value {
        Value::BulkString(self.as_bytes().to_vec())
    }
}

impl IntoRedisValue for bool {
    fn into_redis_value(self) -> Value {
        Value::Boolean(self)
    }
}

#[cfg(feature = "bytes")]
impl IntoRedisValue for bytes::Bytes {
    fn into_redis_value(self) -> Value {
        Value::BulkString(self.to_vec())
    }
}

impl IntoRedisValue for Vec<u8> {
    fn into_redis_value(self) -> Value {
        Value::BulkString(self)
    }
}

impl IntoRedisValue for Vec<Value> {
    fn into_redis_value(self) -> Value {
        Value::Array(self)
    }
}

impl IntoRedisValue for Vec<(Value, Value)> {
    fn into_redis_value(self) -> Value {
        Value::Map(self)
    }
}

impl<K, V> IntoRedisValue for HashMap<K, V>
where
    K: IntoRedisValue,
    V: IntoRedisValue,
{
    fn into_redis_value(self) -> Value {
        Value::Map(
            self.into_iter()
                .map(|(k, v)| (k.into_redis_value(), v.into_redis_value()))
                .collect(),
        )
    }
}

impl<V> IntoRedisValue for HashSet<V>
where
    V: IntoRedisValue,
{
    fn into_redis_value(self) -> Value {
        Value::Set(
            self.into_iter()
                .map(IntoRedisValue::into_redis_value)
                .collect(),
        )
    }
}

impl IntoRedisValue for Value {
    fn into_redis_value(self) -> Value {
        self
    }
}

impl IntoRedisValue for ServerError {
    fn into_redis_value(self) -> Value {
        Value::ServerError(self)
    }
}

/// Build [`Value`]s from a JSON-like notation
///
/// This macro handles:
///
/// * `u8`, `u16`, `u32`, `i8`, `i16`, `i32`, `i64`, `String`, `&str` and other types that implement [`IntoRedisValue`]
/// * `true`, `false` - map to their corresponding [`Value::Boolean`]
/// * `nil` - maps to [`Value::Nil`]
/// * `ok`, `okay` - map to [`Value::Okay`]
/// * `[element1, element2, ..., elementN]` - maps to [`Value::Array`]
/// * `{key1: value1, key2: value2, ..., keyN: valueN]` - maps to [`Value::Map`]
/// * `set:[element1, element2, ..., elementN]` - maps to [`Value::Set`] (wrap in `()` within complex structures)
/// * `simple:"SomeString"` - maps to [`Value::SimpleString`] (wrap in `()` within complex structures)
///
/// # Example
///
/// ```rust
/// use redis::Value;
/// use redis_test::redis_value;
///
/// let actual = redis_value!([42, "foo", {true: nil}]);
///
/// let expected = Value::Array(vec![
///   Value::Int(42),
///   Value::BulkString("foo".as_bytes().to_vec()),
///   Value::Map(vec![(Value::Boolean(true), Value::Nil)])
/// ]);
/// assert_eq!(actual, expected)
/// ```
#[macro_export]
macro_rules! redis_value {
    // Map of elements
    ({$($k:tt: $v:tt),* $(,)*}) => {
        redis::Value::Map(vec![$(($crate::redis_value!($k), $crate::redis_value!($v))),*])
    };

    // Array of elements
    ([$($e:tt),* $(,)*]) => {
        redis::Value::Array(vec![$($crate::redis_value!($e)),*])
    };

    // Set of elements
    (set:[$($e:tt),* $(,)*]) => {
        redis::Value::Set(vec![$($crate::redis_value!($e)),*])
    };

    // Simple strings
    (simple:$e:tt) => {
        redis::Value::SimpleString($e.to_string())
    };

    // Nil
    (nil) => {
        redis::Value::Nil
    };

    // Okay
    (ok) => {
        $crate::redis_value!(okay)
    };

    (okay) => {
        redis::Value::Okay
    };

    // Unwrap extra context-isolating parentheses
    (($context:tt:$e:tt)) => {
        $crate::redis_value!($context:$e)
    };

    // Fallback to primitive conversion
    ($e:expr) => {
        $crate::IntoRedisValue::into_redis_value($e)
    };
}

/// Helper trait for converting `redis::Cmd` and `redis::Pipeline` instances into
/// encoded byte vectors.
pub trait IntoRedisCmdBytes {
    /// Convert a command into an encoded byte vector.
    fn into_redis_cmd_bytes(self) -> Vec<u8>;
}

impl IntoRedisCmdBytes for Cmd {
    fn into_redis_cmd_bytes(self) -> Vec<u8> {
        self.get_packed_command()
    }
}

impl IntoRedisCmdBytes for &Cmd {
    fn into_redis_cmd_bytes(self) -> Vec<u8> {
        self.get_packed_command()
    }
}

impl IntoRedisCmdBytes for &mut Cmd {
    fn into_redis_cmd_bytes(self) -> Vec<u8> {
        self.get_packed_command()
    }
}

impl IntoRedisCmdBytes for Pipeline {
    fn into_redis_cmd_bytes(self) -> Vec<u8> {
        self.get_packed_pipeline()
    }
}

impl IntoRedisCmdBytes for &Pipeline {
    fn into_redis_cmd_bytes(self) -> Vec<u8> {
        self.get_packed_pipeline()
    }
}

impl IntoRedisCmdBytes for &mut Pipeline {
    fn into_redis_cmd_bytes(self) -> Vec<u8> {
        self.get_packed_pipeline()
    }
}

/// Represents a command to be executed against a `MockConnection`.
pub struct MockCmd {
    cmd_bytes: Vec<u8>,
    responses: Result<Vec<Value>, RedisError>,
}

impl MockCmd {
    /// Create a new `MockCmd` given a Redis command and either a value convertible to
    /// a `redis::Value` or a `RedisError`.
    pub fn new<C, V>(cmd: C, response: Result<V, RedisError>) -> Self
    where
        C: IntoRedisCmdBytes,
        V: IntoRedisValue,
    {
        MockCmd {
            cmd_bytes: cmd.into_redis_cmd_bytes(),
            responses: response.map(|r| vec![r.into_redis_value()]),
        }
    }

    /// Create a new `MockCommand` given a Redis command/pipeline and a vector of value convertible
    /// to a `redis::Value` or a `RedisError`.
    pub fn with_values<C, V>(cmd: C, responses: Result<Vec<V>, RedisError>) -> Self
    where
        C: IntoRedisCmdBytes,
        V: IntoRedisValue,
    {
        MockCmd {
            cmd_bytes: cmd.into_redis_cmd_bytes(),
            responses: responses.map(|xs| xs.into_iter().map(|x| x.into_redis_value()).collect()),
        }
    }
}

/// A mock Redis client for testing without a server. `MockRedisConnection` checks whether the
/// client submits a specific sequence of commands and generates an error if it does not.
#[derive(Clone)]
pub struct MockRedisConnection {
    commands: Arc<Mutex<VecDeque<MockCmd>>>,
    assert_is_empty_on_drop: bool,
}

impl MockRedisConnection {
    /// Construct a new from the given sequence of commands.
    pub fn new<I>(commands: I) -> Self
    where
        I: IntoIterator<Item = MockCmd>,
    {
        MockRedisConnection {
            commands: Arc::new(Mutex::new(VecDeque::from_iter(commands))),
            assert_is_empty_on_drop: false,
        }
    }

    /// Enable assertion to ensure all commands have been consumed
    pub fn assert_all_commands_consumed(mut self) -> Self {
        self.assert_is_empty_on_drop = true;
        self
    }
}

impl Drop for MockRedisConnection {
    fn drop(&mut self) {
        if self.assert_is_empty_on_drop {
            let commands = self.commands.lock().unwrap();
            if Arc::strong_count(&self.commands) == 1 {
                assert!(commands.back().is_none());
            }
        }
    }
}

impl MockRedisConnection {
    pub fn is_empty(&self) -> bool {
        self.commands.lock().unwrap().is_empty()
    }
}

impl ConnectionLike for MockRedisConnection {
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        let mut commands = self.commands.lock().unwrap();
        let next_cmd = commands.pop_front().ok_or_else(|| {
            self.assert_is_empty_on_drop = false;
            RedisError::from((ErrorKind::Client, "TEST", "unexpected command".to_owned()))
        })?;

        if cmd != next_cmd.cmd_bytes {
            self.assert_is_empty_on_drop = false;
            return Err(RedisError::from((
                ErrorKind::Client,
                "TEST",
                format!(
                    "unexpected command: expected={}, actual={}",
                    String::from_utf8(next_cmd.cmd_bytes)
                        .unwrap_or_else(|_| "decode error".to_owned()),
                    String::from_utf8(Vec::from(cmd)).unwrap_or_else(|_| "decode error".to_owned()),
                ),
            )));
        }

        next_cmd
            .responses
            .and_then(|values| match values.as_slice() {
                [value] => Ok(value.clone()),
                [] => {
                    self.assert_is_empty_on_drop = false;
                    Err(RedisError::from((
                    ErrorKind::Client,
                    "no value configured as response",
                )))},
                _ => {
                    self.assert_is_empty_on_drop = false;
                    Err(RedisError::from((
                    ErrorKind::Client,
                    "multiple values configured as response for command expecting a single value",
                )))},
            })
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        _offset: usize,
        _count: usize,
    ) -> RedisResult<Vec<Value>> {
        let mut commands = self.commands.lock().unwrap();
        let next_cmd = commands.pop_front().ok_or_else(|| {
            RedisError::from((ErrorKind::Client, "TEST", "unexpected command".to_owned()))
        })?;

        if cmd != next_cmd.cmd_bytes {
            return Err(RedisError::from((
                ErrorKind::Client,
                "TEST",
                format!(
                    "unexpected command: expected={}, actual={}",
                    String::from_utf8(next_cmd.cmd_bytes)
                        .unwrap_or_else(|_| "decode error".to_owned()),
                    String::from_utf8(Vec::from(cmd)).unwrap_or_else(|_| "decode error".to_owned()),
                ),
            )));
        }

        next_cmd.responses
    }

    fn get_db(&self) -> i64 {
        0
    }

    fn check_connection(&mut self) -> bool {
        true
    }

    fn is_open(&self) -> bool {
        true
    }
}

#[cfg(feature = "aio")]
impl AioConnectionLike for MockRedisConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        let packed_cmd = cmd.get_packed_command();
        let response = <MockRedisConnection as ConnectionLike>::req_packed_command(
            self,
            packed_cmd.as_slice(),
        );
        future::ready(response).boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        let packed_cmd = cmd.get_packed_pipeline();
        let response = <MockRedisConnection as ConnectionLike>::req_packed_commands(
            self,
            packed_cmd.as_slice(),
            offset,
            count,
        );
        future::ready(response).boxed()
    }

    fn get_db(&self) -> i64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::{IntoRedisValue, MockCmd, MockRedisConnection};
    use redis::{ErrorKind, ServerError, Value, cmd, make_extension_error, pipe};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn into_redis_value_i8() {
        assert_eq!(42_i8.into_redis_value(), Value::Int(42));
    }

    #[test]
    fn into_redis_value_i16() {
        assert_eq!(42_i16.into_redis_value(), Value::Int(42));
    }

    #[test]
    fn into_redis_value_i32() {
        assert_eq!(42_i32.into_redis_value(), Value::Int(42));
    }

    #[test]
    fn into_redis_value_i64() {
        assert_eq!(42_i64.into_redis_value(), Value::Int(42));
    }

    #[test]
    fn into_redis_value_u8() {
        assert_eq!(42_u8.into_redis_value(), Value::Int(42));
    }

    #[test]
    fn into_redis_value_u16() {
        assert_eq!(42_u16.into_redis_value(), Value::Int(42));
    }

    #[test]
    fn into_redis_value_u32() {
        assert_eq!(42_u32.into_redis_value(), Value::Int(42));
    }

    #[test]
    fn into_redis_value_string() {
        let input = "foo".to_string();

        let actual = input.into_redis_value();

        let expected = Value::BulkString(vec![
            0x66, /* f */
            0x6f, /* o */
            0x6f, /* o */
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn into_redis_value_str_ref() {
        let input = "foo";

        let actual = input.into_redis_value();

        let expected = Value::BulkString(vec![
            0x66, /* f */
            0x6f, /* o */
            0x6f, /* o */
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn into_redis_value_bool_true() {
        assert_eq!(true.into_redis_value(), Value::Boolean(true));
    }

    #[test]
    fn into_redis_value_bool_false() {
        assert_eq!(false.into_redis_value(), Value::Boolean(false));
    }

    #[cfg(feature = "bytes")]
    #[test]
    fn into_redis_value_bytes() {
        let input = bytes::Bytes::from("foo");

        let actual = input.into_redis_value();

        let expected = Value::BulkString(vec![
            0x66, /* f */
            0x6f, /* o */
            0x6f, /* o */
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn into_redis_value_vec_u8() {
        let input = vec![0x66 /* f */, 0x6f /* o */, 0x6f /* o */];

        let actual = input.into_redis_value();

        let expected = Value::BulkString(vec![
            0x66, /* f */
            0x6f, /* o */
            0x6f, /* o */
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn into_redis_value_vec_value() {
        let input = vec![Value::Int(42), Value::Boolean(true)];

        let actual = input.into_redis_value();

        let expected = Value::Array(vec![Value::Int(42), Value::Boolean(true)]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn into_redis_value_vec_value_value() {
        let input = vec![
            (Value::Int(42), Value::Boolean(true)),
            (Value::Int(23), Value::Nil),
        ];

        let actual = input.into_redis_value();

        let expected = Value::Map(vec![
            (Value::Int(42), Value::Boolean(true)),
            (Value::Int(23), Value::Nil),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn into_redis_value_hashmap() {
        let input = HashMap::from([(23, true), (42, false)]);

        let actual = input.into_redis_value();

        let mut actual_entries = actual
            .into_map_iter()
            .expect("extracting elements should work")
            .collect::<Vec<(Value, Value)>>();

        // Sorting the entries, to make sure they are in the order that we expect
        actual_entries.sort_by(|a, b| {
            let Value::Int(int_key_a) = a.0 else {
                panic!("left-hand argument has to be a `Value::Int`");
            };
            let Value::Int(int_key_b) = b.0 else {
                panic!("right-hand argument has to be a `Value::Int`");
            };
            int_key_a.cmp(&int_key_b)
        });

        let expected_entries = vec![
            (Value::Int(23), Value::Boolean(true)),
            (Value::Int(42), Value::Boolean(false)),
        ];

        assert_eq!(actual_entries, expected_entries);
    }

    #[test]
    fn into_redis_value_hashset() {
        let input = HashSet::from([23, 42]);

        let actual = input.into_redis_value();

        let mut actual_entries = actual
            .into_sequence()
            .expect("extracting elements should work");

        // Sorting the entries, to make sure they are in the order that we expect
        actual_entries.sort_by(|a, b| {
            let Value::Int(int_a) = a else {
                panic!("left-hand argument has to be a `Value::Int`");
            };
            let Value::Int(int_b) = b else {
                panic!("right-hand argument has to be a `Value::Int`");
            };
            int_a.cmp(int_b)
        });

        let expected_entries = vec![Value::Int(23), Value::Int(42)];

        assert_eq!(actual_entries, expected_entries);
    }

    #[test]
    fn into_redis_value_value() {
        let input = Value::Int(42);

        let actual = input.into_redis_value();

        assert_eq!(actual, Value::Int(42));
    }

    #[test]
    fn into_redis_value_server_error() {
        let server_error = ServerError::try_from(make_extension_error("FOO".to_string(), None))
            .expect("conversion should work");

        let actual = server_error.clone().into_redis_value();

        assert_eq!(actual, Value::ServerError(server_error));
    }

    #[test]
    fn redis_simple_direct() {
        assert_eq!(
            redis_value!(simple:"foo"),
            Value::SimpleString("foo".to_string())
        );
    }

    #[test]
    fn redis_simple_in_complex() {
        let actual = redis_value!([(simple:"foo")]);

        let expected = Value::Array(vec![Value::SimpleString("foo".to_string())]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_nil() {
        assert_eq!(redis_value!(nil), Value::Nil);
    }

    #[test]
    fn redis_ok() {
        assert_eq!(redis_value!(ok), Value::Okay);
    }

    #[test]
    fn redis_okay() {
        assert_eq!(redis_value!(okay), Value::Okay);
    }

    #[test]
    fn redis_i8() {
        assert_eq!(redis_value!(42_i8), Value::Int(42));
    }

    #[test]
    fn redis_i16() {
        assert_eq!(redis_value!(42_i16), Value::Int(42));
    }

    #[test]
    fn redis_i32() {
        assert_eq!(redis_value!(42_i32), Value::Int(42));
    }

    #[test]
    fn redis_i64() {
        assert_eq!(redis_value!(42_i64), Value::Int(42));
    }

    #[test]
    fn redis_u8() {
        assert_eq!(redis_value!(42_u8), Value::Int(42));
    }

    #[test]
    fn redis_u16() {
        assert_eq!(redis_value!(42_u16), Value::Int(42));
    }

    #[test]
    fn redis_u32() {
        assert_eq!(redis_value!(42_u32), Value::Int(42));
    }

    #[test]
    fn redis_string() {
        let actual = redis_value!("foo".to_string());

        let expected = Value::BulkString(vec![
            0x66, /* f */
            0x6f, /* o */
            0x6f, /* o */
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_str_ref() {
        let actual = redis_value!("foo");

        let expected = Value::BulkString(vec![
            0x66, /* f */
            0x6f, /* o */
            0x6f, /* o */
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_true() {
        assert_eq!(redis_value!(true), Value::Boolean(true));
    }

    #[test]
    fn redis_false() {
        assert_eq!(redis_value!(false), Value::Boolean(false));
    }

    #[test]
    fn redis_value() {
        let actual = Value::Int(42);

        assert_eq!(redis_value!(actual), Value::Int(42));
    }

    #[test]
    fn redis_array_empty() {
        assert_eq!(redis_value!([]), Value::Array(vec![]));
    }

    #[test]
    fn redis_array_single_entry() {
        let actual = redis_value!([42]);

        let expected = Value::Array(vec![Value::Int(42)]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_array_single_entry_trailing_comma() {
        let actual = redis_value!([42,]);

        let expected = Value::Array(vec![Value::Int(42)]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_array_multiple_primitive_entries() {
        let last_arg = Value::Boolean(true); // pass the last arg in as variable
        let actual = redis_value!([42, "foo", nil, last_arg]);

        let expected1 = Value::Int(42);
        let expected2 = Value::BulkString(vec![
            0x66, /* f */
            0x6f, /* o */
            0x6f, /* o */
        ]);
        let expected3 = Value::Nil;
        let expected4 = Value::Boolean(true);
        let expected = Value::Array(vec![expected1, expected2, expected3, expected4]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_array_multiple_entries() {
        let last_arg = Value::Boolean(true); // pass the last arg in as variable
        let actual = redis_value!([42, ["foo", nil,], last_arg]);

        let expected1 = Value::Int(42);
        let expected21 = Value::BulkString(vec![
            0x66, /* f */
            0x6f, /* o */
            0x6f, /* o */
        ]);
        let expected22 = Value::Nil;
        let expected2 = Value::Array(vec![expected21, expected22]);
        let expected3 = Value::Boolean(true);
        let expected = Value::Array(vec![expected1, expected2, expected3]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_set_empty() {
        assert_eq!(redis_value!(set:[]), Value::Set(vec![]));
    }

    #[test]
    fn redis_set_single_entry() {
        let actual = redis_value!(set:[42]);

        let expected = Value::Set(vec![Value::Int(42)]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_set_single_entry_trailing_comma() {
        let actual = redis_value!(set:[42,]);

        let expected = Value::Set(vec![Value::Int(42)]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_set_multiple_primitive_entries() {
        let last_arg = Value::Boolean(true); // pass the last arg in as variable
        let actual = redis_value!(set:[42, "foo", nil, last_arg]);

        let expected1 = Value::Int(42);
        let expected2 = Value::BulkString(vec![
            0x66, /* f */
            0x6f, /* o */
            0x6f, /* o */
        ]);
        let expected3 = Value::Nil;
        let expected4 = Value::Boolean(true);
        let expected = Value::Set(vec![expected1, expected2, expected3, expected4]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_set_multiple_entries() {
        let last_arg = Value::Boolean(true); // pass the last arg in as variable
        let actual = redis_value!(set:[42, (set:["foo", nil,]), last_arg]);

        let expected1 = Value::Int(42);
        let expected21 = Value::BulkString(vec![
            0x66, /* f */
            0x6f, /* o */
            0x6f, /* o */
        ]);
        let expected22 = Value::Nil;
        let expected2 = Value::Set(vec![expected21, expected22]);
        let expected3 = Value::Boolean(true);
        let expected = Value::Set(vec![expected1, expected2, expected3]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_map_empty() {
        assert_eq!(redis_value!({}), Value::Map(vec![]));
    }

    #[test]
    fn redis_map_single_entry() {
        let actual = redis_value!({42: true});

        let expected = Value::Map(vec![(Value::Int(42), Value::Boolean(true))]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_map_single_entry_trailing_comma() {
        let actual = redis_value!({42: true,});

        let expected = Value::Map(vec![(Value::Int(42), Value::Boolean(true))]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_map_multiple_primitive_entries() {
        let actual = redis_value!({42: true, nil: "foo"});

        let expected1 = (Value::Int(42), Value::Boolean(true));
        let expected2 = (
            Value::Nil,
            Value::BulkString(vec![
                0x66, /* f */
                0x6f, /* o */
                0x6f, /* o */
            ]),
        );
        let expected = Value::Map(vec![expected1, expected2]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn redis_map_multiple_entries() {
        let actual = redis_value!({[42, false]: {true: [23, 4711],}, nil: "foo"});

        let expected1_key = Value::Array(vec![Value::Int(42), Value::Boolean(false)]);
        let expected1_value_key = Value::Boolean(true);
        let expected1_value_value = Value::Array(vec![Value::Int(23), Value::Int(4711)]);
        let expected1_value = Value::Map(vec![(expected1_value_key, expected1_value_value)]);
        let expected1 = (expected1_key, expected1_value);
        let expected2 = (
            Value::Nil,
            Value::BulkString(vec![
                0x66, /* f */
                0x6f, /* o */
                0x6f, /* o */
            ]),
        );
        let expected = Value::Map(vec![expected1, expected2]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn sync_basic_test() {
        let mut conn = MockRedisConnection::new(vec![
            MockCmd::new(cmd("SET").arg("foo").arg(42), Ok("")),
            MockCmd::new(cmd("GET").arg("foo"), Ok(42)),
            MockCmd::new(cmd("SET").arg("bar").arg("foo"), Ok("")),
            MockCmd::new(cmd("GET").arg("bar"), Ok("foo")),
        ])
        .assert_all_commands_consumed();

        cmd("SET").arg("foo").arg(42).exec(&mut conn).unwrap();
        assert_eq!(cmd("GET").arg("foo").query(&mut conn), Ok(42));

        cmd("SET").arg("bar").arg("foo").exec(&mut conn).unwrap();
        assert_eq!(
            cmd("GET").arg("bar").query(&mut conn),
            Ok(Value::BulkString(b"foo".as_ref().into()))
        );
    }

    #[cfg(feature = "aio")]
    #[tokio::test]
    async fn async_basic_test() {
        let mut conn = MockRedisConnection::new(vec![
            MockCmd::new(cmd("SET").arg("foo").arg(42), Ok("")),
            MockCmd::new(cmd("GET").arg("foo"), Ok(42)),
            MockCmd::new(cmd("SET").arg("bar").arg("foo"), Ok("")),
            MockCmd::new(cmd("GET").arg("bar"), Ok("foo")),
        ])
        .assert_all_commands_consumed();

        cmd("SET")
            .arg("foo")
            .arg("42")
            .exec_async(&mut conn)
            .await
            .unwrap();
        let result: Result<usize, _> = cmd("GET").arg("foo").query_async(&mut conn).await;
        assert_eq!(result, Ok(42));

        cmd("SET")
            .arg("bar")
            .arg("foo")
            .exec_async(&mut conn)
            .await
            .unwrap();
        let result: Result<Vec<u8>, _> = cmd("GET").arg("bar").query_async(&mut conn).await;
        assert_eq!(result.as_deref(), Ok(&b"foo"[..]));
    }

    #[test]
    fn errors_for_unexpected_commands() {
        let mut conn = MockRedisConnection::new(vec![
            MockCmd::new(cmd("SET").arg("foo").arg(42), Ok("")),
            MockCmd::new(cmd("GET").arg("foo"), Ok(42)),
        ])
        .assert_all_commands_consumed();

        cmd("SET").arg("foo").arg(42).exec(&mut conn).unwrap();
        assert_eq!(cmd("GET").arg("foo").query(&mut conn), Ok(42));

        let err = cmd("SET")
            .arg("bar")
            .arg("foo")
            .exec(&mut conn)
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Client);
        assert_eq!(err.detail(), Some("unexpected command"));
    }

    #[test]
    fn errors_for_mismatched_commands() {
        let mut conn = MockRedisConnection::new(vec![
            MockCmd::new(cmd("SET").arg("foo").arg(42), Ok("")),
            MockCmd::new(cmd("GET").arg("foo"), Ok(42)),
            MockCmd::new(cmd("SET").arg("bar").arg("foo"), Ok("")),
        ])
        .assert_all_commands_consumed();

        cmd("SET").arg("foo").arg(42).exec(&mut conn).unwrap();
        let err = cmd("SET")
            .arg("bar")
            .arg("foo")
            .exec(&mut conn)
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Client);
        assert!(err.detail().unwrap().contains("unexpected command"));
    }

    #[test]
    fn pipeline_basic_test() {
        let mut conn = MockRedisConnection::new(vec![MockCmd::with_values(
            pipe().cmd("GET").arg("foo").cmd("GET").arg("bar"),
            Ok(vec!["hello", "world"]),
        )])
        .assert_all_commands_consumed();

        let results: Vec<String> = pipe()
            .cmd("GET")
            .arg("foo")
            .cmd("GET")
            .arg("bar")
            .query(&mut conn)
            .expect("success");
        assert_eq!(results, vec!["hello", "world"]);
    }

    #[test]
    fn pipeline_atomic_test() {
        let mut conn = MockRedisConnection::new(vec![MockCmd::with_values(
            pipe().atomic().cmd("GET").arg("foo").cmd("GET").arg("bar"),
            Ok(vec![Value::Array(
                vec!["hello", "world"]
                    .into_iter()
                    .map(|x| Value::BulkString(x.as_bytes().into()))
                    .collect(),
            )]),
        )])
        .assert_all_commands_consumed();

        let results: Vec<String> = pipe()
            .atomic()
            .cmd("GET")
            .arg("foo")
            .cmd("GET")
            .arg("bar")
            .query(&mut conn)
            .expect("success");
        assert_eq!(results, vec!["hello", "world"]);
    }
}
