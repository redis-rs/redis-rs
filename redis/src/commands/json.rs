//! Types for the JSON module
//!
//! Some commands in the JSON module return either `T` or `Vec<T>` depending on the path argument.
//! [`VecOrSingleton`] and [`SingletonOrVec`] abstract this difference away, treating `T`
//! as singleton `Vec<T>`:
//!
//! * [`VecOrSingleton`] first tries to parse as `Vec<T>` and falls back to parsing as `T`.
//! * [`SingletonOrVec`] first tries to parse as `T` and falls back to parsing as `Vec<T>`.

use crate::errors::invalid_type_error;
use crate::{FromRedisValue, ParsingError, Value};
use std::ops::Deref;

/// A [`Vec`] that tries to parse to `Vec<T>`, if unsuccessful to `T`, treated as singleton `Vec<T>`
///
/// This struct is useful for typing in Redis' JSON module, where return types are often either `T`
/// or `Vec<T>` depending on the path argument. This struct allows to abstract that difference away.
///
/// It dereferences to a plain [`Vec`].
///
/// This struct is similar to [`SingletonOrVec`], except that it first tries to parse as
/// `Vec<T>` and falls back to parsing as `T`.
#[derive(Debug)]
pub struct VecOrSingleton<T>(Vec<T>);

impl<T> Deref for VecOrSingleton<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: FromRedisValue> FromRedisValue for VecOrSingleton<T> {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        let items = if let Value::Array(arr) = v {
            arr.iter()
                .map(|item| T::from_redis_value_ref(item))
                .collect::<Result<Vec<_>, ParsingError>>()?
        } else {
            vec![T::from_redis_value_ref(v)?]
        };
        Ok(Self(items))
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let items = if let Value::Array(arr) = v {
            arr.into_iter()
                .map(|item| T::from_redis_value(item))
                .collect::<Result<Vec<_>, ParsingError>>()?
        } else {
            vec![T::from_redis_value(v)?]
        };
        Ok(Self(items))
    }
}

/// A [`Vec`] that tries to parse to `T`, treated as singleton `Vec<T>`, if unsuccessful to `Vec<T>`
///
/// This struct is useful for typing in Redis' JSON module, where return types are often either `T`
/// or `Vec<T>` depending on the path argument. This struct allows to abstract that difference away.
///
/// It dereferences to a plain [`Vec`].
///
/// This struct is similar to [`VecOrSingleton`], except that it first tries to parse as `T` and falls
/// back to parsing as `Vec<T>`.
///
/// If `T` allows it, use [`VecOrSingleton`], as it has a cheaper to decide whether to parse as `T`,
/// or `Vec<T>`.
///
/// [`SingletonOrVec`] allows to avoid mis-parsings, if `T` contains a `Vec` of a type
/// that itself parses to a `Vec`, like `Vec<String>`.
///
/// E.g.: On the wire, `JSON.OBJKEYS` for .-paths yields `Array`s of `BulkString`s and `Nil`s. But
/// as `BulkString` and `Nil` both themselves parse to `Vec<String>`,
/// `SingletonFallbackVec<Opt<Vec<String>>` would parse `Array(BulkString(foo), BulkString(bar))` to
/// `[Some([foo]), Some([bar])]` instead of `[Some[foo, bar]]`. [`SingletonOrVec`] parses
/// to the latter.
#[derive(Debug)]
pub struct SingletonOrVec<T>(Vec<T>);

impl<T> Deref for SingletonOrVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: FromRedisValue> FromRedisValue for SingletonOrVec<T> {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        if let Ok(parsed) = T::from_redis_value_ref(v) {
            return Ok(Self(vec![parsed]));
        }

        let Value::Array(arr) = v else {
            invalid_type_error!(v, "Could not convert to T or Vec<T>");
        };

        Ok(Self(
            arr.iter()
                .map(|item| T::from_redis_value_ref(item))
                .collect::<Result<Vec<_>, ParsingError>>()?,
        ))
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        if let Ok(parsed) = T::from_redis_value_ref(&v) {
            return Ok(Self(vec![parsed]));
        }

        let Value::Array(arr) = v else {
            invalid_type_error!(v, "Could not convert to T or Vec<T>");
        };

        Ok(Self(
            arr.into_iter()
                .map(|item| T::from_redis_value(item))
                .collect::<Result<Vec<_>, ParsingError>>()?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::SingletonOrVec;
    use super::VecOrSingleton;
    use crate::Value::*;
    use crate::types::FromRedisValue;

    /// Tries to assure that converting an Array value to an `SingletonFallbackVec` works
    #[test]
    fn singleton_fallback_vec_from_redis_value_array() {
        // The value to test with
        let value = Array(vec![Int(4711), Nil, Int(42)]);

        // The actual conversion
        let converted = VecOrSingleton::<Option<i64>>::from_redis_value(value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711), None, Some(42)]);
    }

    /// Tries to assure that converting a basic value to an `SingletonFallbackVec` works
    #[test]
    fn singleton_fallback_vec_from_redis_value_basic() {
        // The value to test with
        let value = Int(4711);

        // The actual conversion
        let converted = VecOrSingleton::<Option<i64>>::from_redis_value(value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711)]);
    }

    /// Tries to assure that result of parsing a value that parses as both `T` and `Vec<T>`
    #[test]
    fn singleton_fallback_vec_from_redis_value_ambiguous() {
        // The value to test with
        let value = Array(vec![BulkString("foo".into()), BulkString("bar".into())]);

        // The actual conversion
        let converted = VecOrSingleton::<Option<Vec<String>>>::from_redis_value(value).unwrap();

        // Check the resulting value
        assert_eq!(
            *converted,
            vec![Some(vec!["foo".to_string()]), Some(vec!["bar".to_string()])]
        );
    }

    /// Tries to assure that converting fails for unconvertible values in `SingletonFallbackVec`
    #[test]
    fn singleton_fallback_vec_from_redis_value_other_fails() {
        // The value to test with
        let value = BulkString("foo".into());

        // The actual conversion should fail as a `str` should not convert to `i64` or `Vec<i64>`.
        let err = VecOrSingleton::<Option<i64>>::from_redis_value(value).unwrap_err();

        // Check the resulting value
        assert!(err.description.contains("not convert"));
    }

    /// Tries to assure that converting an Array value ref to an `SingletonFallbackVec` works
    #[test]
    fn singleton_fallback_vec_from_redis_value_ref_array() {
        // The value to test with
        let value = Array(vec![Int(4711), Nil, Int(42)]);

        // The actual conversion
        let converted = VecOrSingleton::<Option<i64>>::from_redis_value_ref(&value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711), None, Some(42)]);
    }

    /// Tries to assure that converting a basic value ref to an `SingletonFallbackVec` works
    #[test]
    fn singleton_fallback_vec_from_redis_value_ref_basic() {
        // The value to test with
        let value = Int(4711);

        // The actual conversion
        let converted = VecOrSingleton::<Option<i64>>::from_redis_value_ref(&value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711)]);
    }

    /// Tries to assure that result of parsing a ref to a value that parses as both `T` and `Vec<T>`
    #[test]
    fn singleton_fallback_vec_from_redis_value_ref_ambiguous() {
        // The value to test with
        let value = Array(vec![BulkString("foo".into()), BulkString("bar".into())]);

        // The actual conversion
        let converted =
            VecOrSingleton::<Option<Vec<String>>>::from_redis_value_ref(&value).unwrap();

        // Check the resulting value
        assert_eq!(
            *converted,
            vec![Some(vec!["foo".to_string()]), Some(vec!["bar".to_string()])]
        );
    }

    /// Tries to assure that converting fails for refs to unconvertible values in `SingletonFallbackVec`
    #[test]
    fn singleton_fallback_vec_from_redis_value_ref_other_fails() {
        // The value to test with
        let value = BulkString("foo".into());

        // The actual conversion should fail as a `str` should not convert to `i64` or `Vec<i64>`.
        let err = VecOrSingleton::<Option<i64>>::from_redis_value_ref(&value).unwrap_err();

        // Check the resulting value
        assert!(err.description.contains("not convert"));
    }

    /// Tries to assure that converting an Array value to an `SingletonFirstVec` works
    #[test]
    fn singleton_first_vec_from_redis_value_array() {
        // The value to test with
        let value = Array(vec![Int(4711), Nil, Int(42)]);

        // The actual conversion
        let converted = SingletonOrVec::<Option<i64>>::from_redis_value(value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711), None, Some(42)]);
    }

    /// Tries to assure that converting a basic value to an `SingletonFirstVec` works
    #[test]
    fn singleton_first_vec_from_redis_value_basic() {
        // The value to test with
        let value = Int(4711);

        // The actual conversion
        let converted = SingletonOrVec::<Option<i64>>::from_redis_value(value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711)]);
    }

    /// Tries to assure that result of parsing a value that parses as both `T` and `Vec<T>`
    #[test]
    fn singleton_first_vec_from_redis_value_ambiguous() {
        // The value to test with
        let value = Array(vec![BulkString("foo".into()), BulkString("bar".into())]);

        // The actual conversion
        let converted = SingletonOrVec::<Option<Vec<String>>>::from_redis_value(value).unwrap();

        // Check the resulting value
        assert_eq!(
            *converted,
            vec![Some(vec!["foo".to_string(), "bar".to_string()])]
        );
    }

    /// Tries to assure that converting fails for unconvertible values in `SingletonFirstVec`
    #[test]
    fn singleton_first_vec_from_redis_value_other_fails() {
        // The value to test with
        let value = BulkString("foo".into());

        // The actual conversion should fail as a `str` should not convert to `i64` or `Vec<i64>`.
        let err = SingletonOrVec::<Option<i64>>::from_redis_value(value).unwrap_err();

        // Check the resulting value
        assert!(err.description.contains("not convert"));
    }

    /// Tries to assure that converting an Array value ref to an `SingletonFirstVec` works
    #[test]
    fn singleton_first_vec_from_redis_value_ref_array() {
        // The value to test with
        let value = Array(vec![Int(4711), Nil, Int(42)]);

        // The actual conversion
        let converted = SingletonOrVec::<Option<i64>>::from_redis_value_ref(&value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711), None, Some(42)]);
    }

    /// Tries to assure that converting a basic value ref to an `SingletonFirstVec` works
    #[test]
    fn singleton_first_vec_from_redis_value_ref_basic() {
        // The value to test with
        let value = Int(4711);

        // The actual conversion
        let converted = SingletonOrVec::<Option<i64>>::from_redis_value_ref(&value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711)]);
    }

    /// Tries to assure that result of parsing a ref to a value that parses as both `T` and `Vec<T>`
    #[test]
    fn singleton_first_vec_from_redis_value_ref_ambiguous() {
        // The value to test with
        let value = Array(vec![BulkString("foo".into()), BulkString("bar".into())]);

        // The actual conversion
        let converted =
            SingletonOrVec::<Option<Vec<String>>>::from_redis_value_ref(&value).unwrap();

        // Check the resulting value
        assert_eq!(
            *converted,
            vec![Some(vec!["foo".to_string(), "bar".to_string()])]
        );
    }

    /// Tries to assure that converting fails for refs to unconvertible values in `SingletonFirstVec`
    #[test]
    fn singleton_first_vec_from_redis_value_ref_other_fails() {
        // The value to test with
        let value = BulkString("foo".into());

        // The actual conversion should fail as a `str` should not convert to `i64` or `Vec<i64>`.
        let err = SingletonOrVec::<Option<i64>>::from_redis_value_ref(&value).unwrap_err();

        // Check the resulting value
        assert!(err.description.contains("not convert"));
    }
}
