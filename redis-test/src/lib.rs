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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use redis::{Cmd, ConnectionLike, ErrorKind, Pipeline, RedisError, RedisResult, Value};

#[cfg(feature = "aio")]
use futures::{future, FutureExt};

#[cfg(feature = "aio")]
use redis::{aio::ConnectionLike as AioConnectionLike, RedisFuture};

/// Helper trait for converting test values into a `redis::Value` returned from a
/// `MockRedisConnection`. This is necessary because neither `redis::types::ToRedisArgs`
/// nor `redis::types::FromRedisValue` performs the precise conversion needed.
pub trait IntoRedisValue {
    /// Convert a value into `redis::Value`.
    fn into_redis_value(self) -> Value;
}

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

impl IntoRedisValue for Value {
    fn into_redis_value(self) -> Value {
        self
    }
}

impl IntoRedisValue for i64 {
    fn into_redis_value(self) -> Value {
        Value::Int(self)
    }
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
}

impl MockRedisConnection {
    /// Construct a new from the given sequence of commands.
    pub fn new<I>(commands: I) -> Self
    where
        I: IntoIterator<Item = MockCmd>,
    {
        MockRedisConnection {
            commands: Arc::new(Mutex::new(VecDeque::from_iter(commands))),
        }
    }
}

impl ConnectionLike for MockRedisConnection {
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        let mut commands = self.commands.lock().unwrap();
        let next_cmd = commands.pop_front().ok_or_else(|| {
            RedisError::from((
                ErrorKind::ClientError,
                "TEST",
                "unexpected command".to_owned(),
            ))
        })?;

        if cmd != next_cmd.cmd_bytes {
            return Err(RedisError::from((
                ErrorKind::ClientError,
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
                [] => Err(RedisError::from((
                    ErrorKind::ClientError,
                    "no value configured as response",
                ))),
                _ => Err(RedisError::from((
                    ErrorKind::ClientError,
                    "multiple values configured as response for command expecting a single value",
                ))),
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
            RedisError::from((
                ErrorKind::ClientError,
                "TEST",
                "unexpected command".to_owned(),
            ))
        })?;

        if cmd != next_cmd.cmd_bytes {
            return Err(RedisError::from((
                ErrorKind::ClientError,
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
    use super::{MockCmd, MockRedisConnection};
    use redis::{cmd, pipe, ErrorKind, Value};

    #[test]
    fn sync_basic_test() {
        let mut conn = MockRedisConnection::new(vec![
            MockCmd::new(cmd("SET").arg("foo").arg(42), Ok("")),
            MockCmd::new(cmd("GET").arg("foo"), Ok(42)),
            MockCmd::new(cmd("SET").arg("bar").arg("foo"), Ok("")),
            MockCmd::new(cmd("GET").arg("bar"), Ok("foo")),
        ]);

        cmd("SET").arg("foo").arg(42).execute(&mut conn);
        assert_eq!(cmd("GET").arg("foo").query(&mut conn), Ok(42));

        cmd("SET").arg("bar").arg("foo").execute(&mut conn);
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
        ]);

        cmd("SET")
            .arg("foo")
            .arg("42")
            .query_async::<_, ()>(&mut conn)
            .await
            .unwrap();
        let result: Result<usize, _> = cmd("GET").arg("foo").query_async(&mut conn).await;
        assert_eq!(result, Ok(42));

        cmd("SET")
            .arg("bar")
            .arg("foo")
            .query_async::<_, ()>(&mut conn)
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
        ]);

        cmd("SET").arg("foo").arg(42).execute(&mut conn);
        assert_eq!(cmd("GET").arg("foo").query(&mut conn), Ok(42));

        let err = cmd("SET")
            .arg("bar")
            .arg("foo")
            .query::<()>(&mut conn)
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ClientError);
        assert_eq!(err.detail(), Some("unexpected command"));
    }

    #[test]
    fn errors_for_mismatched_commands() {
        let mut conn = MockRedisConnection::new(vec![
            MockCmd::new(cmd("SET").arg("foo").arg(42), Ok("")),
            MockCmd::new(cmd("GET").arg("foo"), Ok(42)),
            MockCmd::new(cmd("SET").arg("bar").arg("foo"), Ok("")),
        ]);

        cmd("SET").arg("foo").arg(42).execute(&mut conn);
        let err = cmd("SET")
            .arg("bar")
            .arg("foo")
            .query::<()>(&mut conn)
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ClientError);
        assert!(err.detail().unwrap().contains("unexpected command"));
    }

    #[test]
    fn pipeline_basic_test() {
        let mut conn = MockRedisConnection::new(vec![MockCmd::with_values(
            pipe().cmd("GET").arg("foo").cmd("GET").arg("bar"),
            Ok(vec!["hello", "world"]),
        )]);

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
        )]);

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
