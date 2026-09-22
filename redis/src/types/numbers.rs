//! Number related implementations

use std::str::from_utf8;

use crate::{
    FromRedisValue, NumericBehavior, ParsingError, RedisWrite, ToRedisArgs, ToSingleRedisArg, Value,
};

macro_rules! itoa_based_to_redis_impl {
    ($t:ty, $numeric:expr) => {
        impl ToRedisArgs for $t {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                let mut buf = ::itoa::Buffer::new();
                let s = buf.format(*self);
                out.write_arg(s.as_bytes())
            }

            #[inline]
            fn args_size(&self) -> usize {
                let mut buf = ::itoa::Buffer::new();
                buf.format(*self).len()
            }

            #[inline]
            fn describe_numeric_behavior(&self) -> NumericBehavior {
                $numeric
            }
        }

        impl ToSingleRedisArg for $t {}
    };
}

macro_rules! non_zero_itoa_based_to_redis_impl {
    ($t:ty, $numeric:expr) => {
        impl ToRedisArgs for $t {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                let mut buf = ::itoa::Buffer::new();
                let s = buf.format(self.get());
                out.write_arg(s.as_bytes())
            }

            #[inline]
            fn args_size(&self) -> usize {
                let mut buf = ::itoa::Buffer::new();
                buf.format(self.get()).len()
            }

            #[inline]
            fn describe_numeric_behavior(&self) -> NumericBehavior {
                $numeric
            }
        }

        impl ToSingleRedisArg for $t {}
    };
}

macro_rules! ryu_based_to_redis_impl {
    ($t:ty, $numeric:expr) => {
        impl ToRedisArgs for $t {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                let mut buf = ::ryu::Buffer::new();
                let s = buf.format(*self);
                out.write_arg(s.as_bytes())
            }

            #[inline]
            fn args_size(&self) -> usize {
                let mut buf = ::ryu::Buffer::new();
                buf.format(*self).len()
            }

            #[inline]
            fn describe_numeric_behavior(&self) -> NumericBehavior {
                $numeric
            }
        }

        impl ToSingleRedisArg for $t {}
    };
}

impl ToRedisArgs for u8 {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let mut buf = ::itoa::Buffer::new();
        let s = buf.format(*self);
        out.write_arg(s.as_bytes());
    }

    #[inline]
    fn args_size(&self) -> usize {
        let mut buf = ::itoa::Buffer::new();
        buf.format(*self).len()
    }

    fn write_args_from_slice<W>(items: &[Self], out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(items);
    }

    #[inline]
    fn num_of_args_and_size_for_slice(items: &[Self]) -> (usize, usize) {
        (1, items.len())
    }

    #[inline]
    fn num_of_args_and_size_for_array<const N: usize>(_items: &[Self; N]) -> (usize, usize) {
        (1, N)
    }

    #[inline]
    fn is_single_vec_arg(_items: &[Self]) -> bool {
        true
    }
}

impl ToSingleRedisArg for u8 {}

itoa_based_to_redis_impl!(i8, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(i16, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(u16, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(i32, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(u32, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(i64, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(u64, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(i128, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(u128, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(isize, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(usize, NumericBehavior::NumberIsInteger);

non_zero_itoa_based_to_redis_impl!(core::num::NonZeroU8, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroI8, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroU16, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroI16, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroU32, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroI32, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroU64, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroI64, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroU128, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroI128, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroUsize, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroIsize, NumericBehavior::NumberIsInteger);

ryu_based_to_redis_impl!(f32, NumericBehavior::NumberIsFloat);
ryu_based_to_redis_impl!(f64, NumericBehavior::NumberIsFloat);

#[cfg(any(
    feature = "rust_decimal",
    feature = "bigdecimal",
    feature = "num-bigint"
))]
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

        impl ToSingleRedisArg for $t {}
    };
}

#[cfg(feature = "rust_decimal")]
bignum_to_redis_impl!(rust_decimal::Decimal);
#[cfg(feature = "bigdecimal")]
bignum_to_redis_impl!(bigdecimal::BigDecimal);
#[cfg(feature = "num-bigint")]
bignum_to_redis_impl!(num_bigint::BigInt);
#[cfg(feature = "num-bigint")]
bignum_to_redis_impl!(num_bigint::BigUint);

macro_rules! from_redis_value_for_float_internal {
    ($t:ty, $v:expr) => {{
        let v = if let Value::Attribute {
            data,
            attributes: _,
        } = $v
        {
            data
        } else {
            $v
        };
        match *v {
            Value::Int(val) => Ok(val as $t),
            Value::SimpleString(ref s) => match s.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => crate::errors::invalid_type_error!(v, "Could not convert from string."),
            },
            Value::BulkString(ref bytes) => match from_utf8(bytes)?.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => crate::errors::invalid_type_error!(v, "Could not convert from string."),
            },
            Value::Double(val) => Ok(val as $t),
            _ => crate::errors::invalid_type_error!(v, "Response type not convertible to numeric."),
        }
    }};
}

/// Same as `from_redis_value_for_float_internal`, but for integer types.
///
/// Every arm rejects a value that the requested type cannot represent, so that
/// a reply converts to the same result whether the server sent it as a number
/// or as a string.
macro_rules! from_redis_value_for_int_internal {
    ($t:ty, $v:expr) => {{
        let v = if let Value::Attribute {
            data,
            attributes: _,
        } = $v
        {
            data
        } else {
            $v
        };
        match *v {
            Value::Int(val) => match <$t>::try_from(val) {
                Ok(rv) => Ok(rv),
                Err(_) => crate::errors::invalid_type_error!(
                    v,
                    "Integer is out of range for the requested type."
                ),
            },
            Value::SimpleString(ref s) => match s.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => crate::errors::invalid_type_error!(v, "Could not convert from string."),
            },
            Value::BulkString(ref bytes) => match from_utf8(bytes)?.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => crate::errors::invalid_type_error!(v, "Could not convert from string."),
            },
            Value::Double(val) => {
                // `<$t>::MAX as f64` rounds *up* to the next power of two for the
                // wider integer types, so the upper bound is a strict `<` against
                // `MAX as f64 + 1.0`. NaN and the infinities fail both
                // comparisons, and a fractional value is not an integer, so all
                // three are rejected the same way `str::parse` rejects them.
                if val.fract() == 0.0 && val >= <$t>::MIN as f64 && val < <$t>::MAX as f64 + 1.0 {
                    Ok(val as $t)
                } else {
                    crate::errors::invalid_type_error!(
                        v,
                        "Double is not an integer in range for the requested type."
                    )
                }
            }
            _ => crate::errors::invalid_type_error!(v, "Response type not convertible to numeric."),
        }
    }};
}

macro_rules! from_redis_value_for_float {
    ($t:ty) => {
        impl FromRedisValue for $t {
            fn from_redis_value_ref(v: &Value) -> Result<$t, ParsingError> {
                from_redis_value_for_float_internal!($t, v)
            }

            fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
                Self::from_redis_value_ref(&v)
            }
        }
    };
}

macro_rules! from_redis_value_for_int {
    ($t:ty) => {
        impl FromRedisValue for $t {
            fn from_redis_value_ref(v: &Value) -> Result<$t, ParsingError> {
                from_redis_value_for_int_internal!($t, v)
            }

            fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
                Self::from_redis_value_ref(&v)
            }
        }
    };
}

impl FromRedisValue for u8 {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        from_redis_value_for_int_internal!(Self, v)
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        Self::from_redis_value_ref(&v)
    }

    // this hack allows us to specialize Vec<u8> to work with binary data.
    fn from_byte_slice(vec: &[u8]) -> Option<Vec<Self>> {
        Some(vec.to_vec())
    }
    fn from_byte_vec(vec: Vec<u8>) -> Result<Vec<Self>, ParsingError> {
        Ok(vec)
    }
}

from_redis_value_for_int!(i8);
from_redis_value_for_int!(i16);
from_redis_value_for_int!(u16);
from_redis_value_for_int!(i32);
from_redis_value_for_int!(u32);
from_redis_value_for_int!(i64);
from_redis_value_for_int!(u64);
from_redis_value_for_int!(i128);
from_redis_value_for_int!(u128);
from_redis_value_for_float!(f32);
from_redis_value_for_float!(f64);
from_redis_value_for_int!(isize);
from_redis_value_for_int!(usize);

#[cfg(any(
    feature = "rust_decimal",
    feature = "bigdecimal",
    feature = "num-bigint"
))]
macro_rules! from_redis_value_for_bignum_internal {
    ($t:ty, $v:expr) => {{
        let v = $v;
        match *v {
            Value::Int(val) => <$t>::try_from(val).map_err(|_| {
                crate::errors::invalid_type_error_inner!(v, "Could not convert from integer.")
            }),
            Value::SimpleString(ref s) => match s.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => crate::errors::invalid_type_error!(v, "Could not convert from string."),
            },
            Value::BulkString(ref bytes) => match from_utf8(bytes)?.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => crate::errors::invalid_type_error!(v, "Could not convert from string."),
            },
            _ => crate::errors::invalid_type_error!(v, "Response type not convertible to numeric."),
        }
    }};
}

#[cfg(any(
    feature = "rust_decimal",
    feature = "bigdecimal",
    feature = "num-bigint"
))]
macro_rules! from_redis_value_for_bignum {
    ($t:ty) => {
        impl FromRedisValue for $t {
            fn from_redis_value_ref(v: &Value) -> Result<$t, ParsingError> {
                from_redis_value_for_bignum_internal!($t, v)
            }

            fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
                Self::from_redis_value_ref(&v)
            }
        }
    };
}

#[cfg(feature = "rust_decimal")]
from_redis_value_for_bignum!(rust_decimal::Decimal);
#[cfg(feature = "bigdecimal")]
from_redis_value_for_bignum!(bigdecimal::BigDecimal);
#[cfg(feature = "num-bigint")]
from_redis_value_for_bignum!(num_bigint::BigInt);
#[cfg(feature = "num-bigint")]
from_redis_value_for_bignum!(num_bigint::BigUint);
