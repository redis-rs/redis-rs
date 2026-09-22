//! String related implementations

use super::{get_inner_value, get_owned_inner_value};
use crate::{FromRedisValue, ParsingError, RedisWrite, ToRedisArgs, ToSingleRedisArg, Value};
use std::borrow::Cow;
use std::ffi::CString;
use std::str::from_utf8;

impl ToRedisArgs for String {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.as_bytes());
    }

    #[inline]
    fn args_size(&self) -> usize {
        self.len()
    }
}
impl ToSingleRedisArg for String {}

impl ToRedisArgs for &str {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.as_bytes());
    }

    #[inline]
    fn args_size(&self) -> usize {
        self.len()
    }
}

impl ToSingleRedisArg for &str {}

impl<'a, T> ToRedisArgs for Cow<'a, T>
where
    T: ToOwned + ?Sized,
    &'a T: ToRedisArgs,
    T::Owned: ToRedisArgs,
{
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Cow::Borrowed(inner) => inner.write_redis_args(out),
            Cow::Owned(inner) => inner.write_redis_args(out),
        }
    }

    #[inline]
    fn args_size(&self) -> usize {
        match self {
            Cow::Borrowed(inner) => inner.args_size(),
            Cow::Owned(inner) => inner.args_size(),
        }
    }
}

impl<'a, T> ToSingleRedisArg for Cow<'a, T>
where
    T: ToOwned + ?Sized,
    &'a T: ToSingleRedisArg,
    T::Owned: ToSingleRedisArg,
{
}

impl FromRedisValue for CString {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        let v = get_inner_value(v);
        match *v {
            Value::BulkString(ref bytes) => Ok(Self::new(bytes.as_slice())?),
            Value::Okay => Ok(Self::new("OK")?),
            Value::SimpleString(ref val) => Ok(Self::new(val.as_bytes())?),
            _ => crate::errors::invalid_type_error!(v, "Response type not CString compatible."),
        }
    }
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let v = get_owned_inner_value(v);
        match v {
            Value::BulkString(bytes) => Ok(Self::new(bytes)?),
            Value::Okay => Ok(Self::new("OK")?),
            Value::SimpleString(val) => Ok(Self::new(val)?),
            _ => crate::errors::invalid_type_error!(v, "Response type not CString compatible."),
        }
    }
}

impl FromRedisValue for String {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        let v = get_inner_value(v);
        match *v {
            Value::BulkString(ref bytes) => Ok(from_utf8(bytes)?.to_string()),
            Value::Okay => Ok("OK".to_string()),
            Value::SimpleString(ref val) => Ok(val.to_string()),
            Value::VerbatimString {
                format: _,
                ref text,
            } => Ok(text.to_string()),
            Value::Double(ref val) => Ok(val.to_string()),
            Value::Int(val) => Ok(val.to_string()),
            _ => crate::errors::invalid_type_error!(v, "Response type not string compatible."),
        }
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let v = get_owned_inner_value(v);
        match v {
            Value::BulkString(bytes) => Ok(Self::from_utf8(bytes)?),
            Value::Okay => Ok("OK".to_string()),
            Value::SimpleString(val) => Ok(val),
            Value::VerbatimString { format: _, text } => Ok(text),
            Value::Double(val) => Ok(val.to_string()),
            Value::Int(val) => Ok(val.to_string()),
            _ => crate::errors::invalid_type_error!(v, "Response type not string compatible."),
        }
    }
}
