//! Commands and types for working with the RedisJSON module.

use crate::errors::invalid_type_error;
use crate::types::{ExistenceCheck, RedisWrite, ToRedisArgs};
use crate::{FromRedisValue, ParsingError, Value};
use std::ops::Deref;

/// Storage-precision tag for the `FPHA` form of `JSON.SET`.
///
/// Applied via [`JsonSetOptions::fpha`] instructs the server to pack any floating-point arrays in the payload using the chosen lane precision.
/// Values that fall outside the chosen type's representable range cause the server to reject the command with `ERR value out of range for <TYPE>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FphaType {
    /// Server stores lanes as Google brain-float 16 (`bfloat16`).
    Bf16,
    /// Server stores lanes as IEEE-754 binary16.
    Fp16,
    /// Server stores lanes as IEEE-754 binary32.
    Fp32,
    /// Server stores lanes as IEEE-754 binary64.
    Fp64,
}

impl ToRedisArgs for FphaType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Self::Bf16 => out.write_arg(b"BF16"),
            Self::Fp16 => out.write_arg(b"FP16"),
            Self::Fp32 => out.write_arg(b"FP32"),
            Self::Fp64 => out.write_arg(b"FP64"),
        }
    }
}

/// Options for the [`JSON.SET`](https://redis.io/commands/json.set) command.
///
/// Carries the optional `NX`/`XX` existence check and the optional `FPHA <TYPE>` storage hint.
///
/// # Example
/// ```rust,no_run
/// use redis::json::{FphaType, JsonSetOptions};
/// use redis::{ExistenceCheck, Commands};
/// use serde_json::json;
/// # fn do_something() -> redis::RedisResult<()> {
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let mut con = client.get_connection()?;
/// let opts = JsonSetOptions::default()
///     .conditional_set(ExistenceCheck::NX)
///     .fpha(FphaType::Fp32);
/// let _: () = con.json_set_options("my_key", "$", &[1.0_f32, 2.0], &opts)?;
/// # Ok(()) }
/// ```
#[derive(Clone, Default)]
pub struct JsonSetOptions {
    conditional_set: Option<ExistenceCheck>,
    fpha_type: Option<FphaType>,
}

impl JsonSetOptions {
    /// Apply an `NX` or `XX` existence check to the command.
    pub fn conditional_set(mut self, existence_check: ExistenceCheck) -> Self {
        self.conditional_set = Some(existence_check);
        self
    }

    /// Add an `FPHA <TYPE>` storage hint to the command.
    pub fn fpha(mut self, fpha_type: FphaType) -> Self {
        self.fpha_type = Some(fpha_type);
        self
    }
}

impl ToRedisArgs for JsonSetOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref conditional_set) = self.conditional_set {
            conditional_set.write_redis_args(out);
        }
        if let Some(ref ty) = self.fpha_type {
            out.write_arg(b"FPHA");
            ty.write_redis_args(out);
        }
    }
}

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

/// A [`Vec`] that parses by descending into [`Value::Array`]s, flattening the parsed items
// The implementation is not very efficient, but as it is typically only used small lists, the
// simple implementation is good enough.
#[derive(Debug, PartialEq, Eq)]
pub struct FlattenedVec<T>(Vec<T>);

impl<T> Deref for FlattenedVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: FromRedisValue> FlattenedVec<T> {
    fn flatten_into(v: Value, collector: &mut Vec<T>) -> Result<(), ParsingError> {
        match v {
            Value::Array(elements) => {
                for element in elements {
                    Self::flatten_into(element, collector)?;
                }
            }
            _ => {
                collector.push(T::from_redis_value(v)?);
            }
        }
        Ok(())
    }
}
impl<T: FromRedisValue> FromRedisValue for FlattenedVec<T> {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let mut ret = Vec::new();
        Self::flatten_into(v, &mut ret)?;
        Ok(Self(ret))
    }
}

/// Json-like types returned by [`Cmd::json_type`](crate::Cmd::json_type)
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedisJsonType {
    /// Type `null` values
    Null,
    /// Type for boolean values
    Boolean,
    /// Type for integers
    Integer,
    /// Type for non-integer numbers
    Number,
    /// Type for strings
    String,
    /// Type for arrays
    Array,
    /// Type for objects
    Object,
}

impl TryFrom<&[u8]> for RedisJsonType {
    type Error = ParsingError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match value {
            b"null" => Ok(Self::Null),
            b"boolean" => Ok(Self::Boolean),
            b"integer" => Ok(Self::Integer),
            b"number" => Ok(Self::Number),
            b"string" => Ok(Self::String),
            b"array" => Ok(Self::Array),
            b"object" => Ok(Self::Object),
            _ => invalid_type_error!(value, "Response type not RedisJsonType compatible."),
        }
    }
}

impl FromRedisValue for RedisJsonType {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        match v {
            Value::SimpleString(str) => Self::try_from(str.as_ref()),
            Value::BulkString(str) => Self::try_from(str.as_ref()),
            _ => invalid_type_error!(v, "Response type not RedisJsonType compatible."),
        }
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        Self::from_redis_value_ref(&v)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value::*;
    use crate::cmd::{Arg, Cmd, cmd};
    use crate::types::FromRedisValue;
    use rstest::rstest;
    use serde::ser::Serialize;

    fn simple_args(c: &Cmd) -> Vec<Vec<u8>> {
        c.args_iter()
            .map(|a| match a {
                Arg::Simple(b) => b.to_vec(),
                Arg::Cursor => b"<CURSOR>".to_vec(),
            })
            .collect()
    }

    fn build<V: Serialize + ?Sized>(value: &V, opts: &JsonSetOptions) -> Vec<Vec<u8>> {
        let mut c = cmd("JSON.SET");
        c.arg("k")
            .arg("$")
            .arg(serde_json::to_string(value).unwrap())
            .arg(opts);
        simple_args(&c)
    }

    #[test]
    fn json_value_with_default_options_writes_serialized_document_only() {
        assert_eq!(
            build(&serde_json::json!({"a": 1}), &JsonSetOptions::default()),
            vec![
                b"JSON.SET".to_vec(),
                b"k".to_vec(),
                b"$".to_vec(),
                br#"{"a":1}"#.to_vec(),
            ],
        );
    }

    #[test]
    fn json_set_options_builder_is_order_independent() {
        let a = JsonSetOptions::default()
            .conditional_set(ExistenceCheck::NX)
            .fpha(FphaType::Fp64);
        let b = JsonSetOptions::default()
            .fpha(FphaType::Fp64)
            .conditional_set(ExistenceCheck::NX);
        assert_eq!(build(&[1.0_f64], &a), build(&[1.0_f64], &b));
    }

    #[test]
    fn fpha_type_writes_expected_bytes() {
        for (ty, expected) in [
            (FphaType::Bf16, b"BF16".as_slice()),
            (FphaType::Fp16, b"FP16".as_slice()),
            (FphaType::Fp32, b"FP32".as_slice()),
            (FphaType::Fp64, b"FP64".as_slice()),
        ] {
            let args = build(&[0.0_f32], &JsonSetOptions::default().fpha(ty));
            assert_eq!(args.len(), 6);
            assert_eq!(args[4], b"FPHA");
            assert_eq!(args[5], expected);
        }
    }

    #[test]
    fn conditional_set_nx_appends_existence_check() {
        let args = build(
            &serde_json::json!(1),
            &JsonSetOptions::default().conditional_set(ExistenceCheck::NX),
        );
        assert_eq!(args.len(), 5);
        assert_eq!(args.last().unwrap(), b"NX");
    }

    #[test]
    fn fpha_with_existence_check_orders_value_then_existence_check_then_fpha_type() {
        let args = build(
            &[1.0_f32, -0.5, 1234.5],
            &JsonSetOptions::default()
                .conditional_set(ExistenceCheck::XX)
                .fpha(FphaType::Fp32),
        );
        // [JSON.SET, k, $, <json>, XX, FPHA, FP32]
        assert_eq!(args.len(), 7);
        assert_eq!(args[3], b"[1.0,-0.5,1234.5]");
        assert_eq!(args[4], b"XX");
        assert_eq!(args[5], b"FPHA");
        assert_eq!(args[6], b"FP32");
    }

    #[test]
    fn fpha_empty_payload_still_emits_fpha_type() {
        let args = build(&[0_f32; 0], &JsonSetOptions::default().fpha(FphaType::Fp32));
        assert_eq!(args.len(), 6);
        assert_eq!(args[3], b"[]");
        assert_eq!(args[4], b"FPHA");
        assert_eq!(args[5], b"FP32");
    }

    #[test]
    fn fpha_with_matrix_emits_nested_json_and_fpha_type() {
        let matrix: &[&[f32]] = &[&[1.0, 2.5], &[3.0, 4.0]];
        let args = build(matrix, &JsonSetOptions::default().fpha(FphaType::Bf16));
        assert_eq!(args.len(), 6);
        assert_eq!(args[3], b"[[1.0,2.5],[3.0,4.0]]");
        assert_eq!(args[4], b"FPHA");
        assert_eq!(args[5], b"BF16");
    }

    #[test]
    fn json_object_properly_serialized_with_value_and_fpha_type() {
        let value = serde_json::json!({"weights": [1.0, 2.0], "bias": [0.5]});
        let args = build(&value, &JsonSetOptions::default().fpha(FphaType::Fp16));
        assert_eq!(args[3], br#"{"bias":[0.5],"weights":[1.0,2.0]}"#);
        assert_eq!(args[4], b"FPHA");
        assert_eq!(args[5], b"FP16");
    }

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

    #[rstest]
    #[case::direct_item(Int(42), vec![42])]
    #[case::empty_array(Array(vec![]), vec![])]
    #[case::flat_array(Array(vec![Int(42), Double(4711.)]), vec![42, 4711])]
    #[case::nested_array(Array(vec![Int(23), Array(vec![Array(vec![Int(42)]), Int(151)])]), vec![23, 42, 151])]
    fn flattened_conversion_success(#[case] input: Value, #[case] expected: Vec<u64>) {
        let result = FlattenedVec::<u64>::from_redis_value(input).unwrap();
        assert_eq!(*result, expected);
    }

    #[rstest]
    #[case::direct_item(BulkString("foo".into()))]
    #[case::flat_array(Array(vec![Int(42), BulkString("foo".into()), Int(4711)]))]
    #[case::nested_array(Array(vec![Int(23), Array(vec![Int(42), BulkString("foo".into()), Int(151)]), Int(4711)]))]
    fn flattened_conversion_failure(#[case] input: Value) {
        let err = FlattenedVec::<u64>::from_redis_value(input).unwrap_err();
        assert!(err.description.contains("convert"));
    }

    #[rstest]
    #[case::null("null", RedisJsonType::Null)]
    #[case::bool("boolean", RedisJsonType::Boolean)]
    #[case::int("integer", RedisJsonType::Integer)]
    #[case::number("number", RedisJsonType::Number)]
    #[case::array("array", RedisJsonType::Array)]
    #[case::object("object", RedisJsonType::Object)]
    fn redis_json_type_parsing_success(#[case] input: &str, #[case] expected: RedisJsonType) {
        let value = BulkString(Vec::from(input));

        assert_eq!(
            RedisJsonType::from_redis_value_ref(&value).unwrap(),
            expected
        );
        assert_eq!(RedisJsonType::from_redis_value(value).unwrap(), expected);
    }

    #[rstest]
    #[case::bulkstring_unparsable(BulkString("foo".into()))]
    #[case::simplestring_unparsable(SimpleString("foo".into()))]
    #[case::nil(Nil)]
    fn redis_json_type_parsing_errors(#[case] value: Value) {
        let err = RedisJsonType::from_redis_value_ref(&value).unwrap_err();
        assert!(err.description.contains("compatible"));

        let err = RedisJsonType::from_redis_value(value).unwrap_err();
        assert!(err.description.contains("compatible"));
    }
}
