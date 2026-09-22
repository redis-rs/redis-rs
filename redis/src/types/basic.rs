//! Implementations related to basic types that are neither numbers, strings, nor collections

use super::{get_inner_value, get_owned_inner_value};
use crate::{
    FromRedisValue, NumericBehavior, ParsingError, RedisWrite, ToRedisArgs, ToSingleRedisArg,
    Value, from_redis_value, from_redis_value_ref,
};

impl ToRedisArgs for bool {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(if *self { b"1" } else { b"0" });
    }

    #[inline]
    fn args_size(&self) -> usize {
        1
    }
}

impl ToSingleRedisArg for bool {}

impl<T: ToRedisArgs> ToRedisArgs for Option<T> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref x) = *self {
            x.write_redis_args(out);
        }
    }

    #[inline]
    fn describe_numeric_behavior(&self) -> NumericBehavior {
        match *self {
            Some(ref x) => x.describe_numeric_behavior(),
            None => NumericBehavior::NonNumeric,
        }
    }

    #[inline]
    fn num_of_args(&self) -> usize {
        match *self {
            Some(ref x) => x.num_of_args(),
            None => 0,
        }
    }

    #[inline]
    fn args_size(&self) -> usize {
        match *self {
            Some(ref x) => x.args_size(),
            None => 0,
        }
    }
}

macro_rules! deref_to_write_redis_args_impl {
    ($type:ty) => {
        impl<'a, T> ToRedisArgs for $type
        where
            T: ToRedisArgs,
        {
            #[inline]
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                (**self).write_redis_args(out)
            }

            #[inline]
            fn num_of_args(&self) -> usize {
                (**self).num_of_args()
            }

            #[inline]
            fn args_size(&self) -> usize {
                (**self).args_size()
            }

            #[inline]
            fn num_of_args_and_size(&self) -> (usize, usize) {
                (**self).num_of_args_and_size()
            }

            #[inline]
            fn describe_numeric_behavior(&self) -> NumericBehavior {
                (**self).describe_numeric_behavior()
            }
        }

        impl<'a, T> ToSingleRedisArg for $type where T: ToSingleRedisArg {}
    };
}

deref_to_write_redis_args_impl! {&'a T}
deref_to_write_redis_args_impl! {&'a mut T}
deref_to_write_redis_args_impl! {Box<T>}
deref_to_write_redis_args_impl! {std::sync::Arc<T>}
deref_to_write_redis_args_impl! {std::rc::Rc<T>}

impl FromRedisValue for bool {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        let v = get_inner_value(v);
        match *v {
            Value::Nil => Ok(false),
            Value::Int(val) => Ok(val != 0),
            Value::SimpleString(ref s) => {
                if &s[..] == "1" {
                    Ok(true)
                } else if &s[..] == "0" {
                    Ok(false)
                } else {
                    crate::errors::invalid_type_error!(v, "Response status not valid boolean");
                }
            }
            Value::BulkString(ref bytes) => {
                if bytes == b"1" {
                    Ok(true)
                } else if bytes == b"0" {
                    Ok(false)
                } else {
                    crate::errors::invalid_type_error!(v, "Response type not bool compatible.");
                }
            }
            Value::Boolean(b) => Ok(b),
            Value::Okay => Ok(true),
            _ => crate::errors::invalid_type_error!(v, "Response type not bool compatible."),
        }
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        Self::from_redis_value_ref(&v)
    }
}

macro_rules! pointer_from_redis_value_impl {
    (
        $(#[$attr:meta])*
        $id:ident, $ty:ty, $func:expr
    ) => {
        $(#[$attr])*
        impl<$id:  FromRedisValue> FromRedisValue for $ty {
            fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError>
            {
                FromRedisValue::from_redis_value_ref(v).map($func)
            }

            fn from_redis_value(v: Value) -> Result<Self, ParsingError>{
                FromRedisValue::from_redis_value(v).map($func)
            }
        }
    }
}

pointer_from_redis_value_impl!(T, Box<T>, Box::new);
pointer_from_redis_value_impl!(T, std::sync::Arc<T>, std::sync::Arc::new);
pointer_from_redis_value_impl!(T, std::rc::Rc<T>, std::rc::Rc::new);

impl FromRedisValue for () {
    fn from_redis_value_ref(v: &Value) -> Result<(), ParsingError> {
        match v {
            Value::ServerError(err) => Err(ParsingError::from(err.to_string())),
            _ => Ok(()),
        }
    }

    fn from_redis_value(v: Value) -> Result<(), ParsingError> {
        Self::from_redis_value_ref(&v)
    }
}

impl<T: FromRedisValue> FromRedisValue for Option<T> {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        let v = get_inner_value(v);
        if *v == Value::Nil {
            return Ok(None);
        }
        Ok(Some(from_redis_value_ref(v)?))
    }
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let v = get_owned_inner_value(v);
        if v == Value::Nil {
            return Ok(None);
        }
        Ok(Some(from_redis_value(v)?))
    }
}

#[cfg(feature = "bytes")]
impl FromRedisValue for bytes::Bytes {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        let v = get_inner_value(v);
        match v {
            Value::BulkString(bytes_vec) => Ok(Self::copy_from_slice(bytes_vec.as_ref())),
            _ => crate::errors::invalid_type_error!(v, "Not a bulk string"),
        }
    }
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let v = get_owned_inner_value(v);
        match v {
            Value::BulkString(bytes_vec) => Ok(bytes_vec.into()),
            _ => crate::errors::invalid_type_error!(v, "Not a bulk string"),
        }
    }
}

#[cfg(feature = "uuid")]
impl FromRedisValue for uuid::Uuid {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        match *v {
            Value::BulkString(ref bytes) => Ok(Self::from_slice(bytes)?),
            _ => crate::errors::invalid_type_error!(v, "Response type not uuid compatible."),
        }
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        Self::from_redis_value_ref(&v)
    }
}

#[cfg(feature = "uuid")]
impl ToRedisArgs for uuid::Uuid {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.as_bytes());
    }

    #[inline]
    fn args_size(&self) -> usize {
        self.as_bytes().len()
    }
}

#[cfg(feature = "uuid")]
impl ToSingleRedisArg for uuid::Uuid {}
