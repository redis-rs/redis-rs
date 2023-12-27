use std::str::from_utf8;

use super::{ErrorKind, RedisError, RedisResult};
use crate::types::{FromRedisValue, RedisWrite, ToRedisArgs, Value};

macro_rules! invalid_type_error {
    ($v:expr, $det:expr) => {{
        fail!(invalid_type_error_inner!($v, $det))
    }};
}

macro_rules! invalid_type_error_inner {
    ($v:expr, $det:expr) => {
        RedisError::from((
            ErrorKind::TypeError,
            "Response was of incompatible type",
            format!("{:?} (response was {:?})", $det, $v),
        ))
    };
}

macro_rules! from_redis_value_for_bignum_internal {
    ($t:ty, $v:expr) => {{
        let v = $v;
        match *v {
            Value::Int(val) => match <$t>::try_from(val) {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!(v, "Could not convert from integer."),
            },
            Value::Status(ref s) => match s.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!(v, "Could not convert from string."),
            },
            Value::Data(ref bytes) => match from_utf8(bytes)?.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!(v, "Could not convert from string."),
            },
            _ => invalid_type_error!(v, "Response type not convertible to numeric."),
        }
    }};
}

macro_rules! from_redis_value_for_bignum {
    ($t:ty) => {
        impl FromRedisValue for $t {
            fn from_redis_value(v: &Value) -> RedisResult<$t> {
                from_redis_value_for_bignum_internal!($t, v)
            }
        }
    };
}

from_redis_value_for_bignum!(rust_decimal::Decimal);
from_redis_value_for_bignum!(bigdecimal::BigDecimal);
from_redis_value_for_bignum!(num_bigint::BigInt);
from_redis_value_for_bignum!(num_bigint::BigUint);

macro_rules! bignum_to_redis_impl {
    ($t:ty) => {
        impl ToRedisArgs for $t {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                out.write_arg(&self.to_string().into_bytes())
            }
        }
    };
}

bignum_to_redis_impl!(rust_decimal::Decimal);
bignum_to_redis_impl!(bigdecimal::BigDecimal);
bignum_to_redis_impl!(num_bigint::BigInt);
bignum_to_redis_impl!(num_bigint::BigUint);
