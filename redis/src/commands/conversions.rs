//! Alternate conversions from [`Value`]

use crate::{FromRedisValue, ParsingError, Value};

/// Tries to parse a [`Value`] as `Vec<T>`, and that failing as `T`, yielding it as one-item `Vec`
pub(super) fn try_vec_then_singleton<T: FromRedisValue>(v: Value) -> Result<Vec<T>, ParsingError> {
    // If it's an array, try parsing as array
    //
    // We cannot use `Vec::<T>::from_redis_value` as that would consume `v`, which is still needed
    // when trying to parse as `T` later.
    //
    // We do not want to use `Vec::<T>::from_redis_value_ref` as that would mean more allocations,
    // if the conversion works.
    let ret = if let Value::Array(arr) = v {
        arr.into_iter()
            .map(|item| T::from_redis_value(item))
            .collect::<Result<Vec<_>, ParsingError>>()?
    } else {
        // Fallback to trying to parse as singleton
        vec![T::from_redis_value(v)?]
    };

    Ok(ret)
}

/// Tries to parse a [`Value`] as `T` (yielding it as one-item `Vec`), and that failing as `Vec<T>`
pub(super) fn try_singleton_then_vec<T: FromRedisValue>(v: Value) -> Result<Vec<T>, ParsingError> {
    // Trying to parse as `T`
    if let Ok(item) = T::from_redis_value_ref(&v) {
        return Ok(vec![item]);
    }

    // Parsing as `T` failed, so we try parsing as `Vec<T>`
    Vec::<T>::from_redis_value(v)
}

/// Recursive descend helper for [`flattened_array`]
fn flattened_array_rec_desc<T: FromRedisValue>(
    v: Value,
    collector: &mut Vec<T>,
) -> Result<(), ParsingError> {
    match v {
        Value::Array(elements) => {
            for element in elements {
                flattened_array_rec_desc(element, collector)?;
            }
        }
        _ => {
            collector.push(T::from_redis_value(v)?);
        }
    }
    Ok(())
}

/// Tries to parse a [`Value`] as `T`s descending recursively into arrays
pub(super) fn flattened_array<T: FromRedisValue>(v: Value) -> Result<Vec<T>, ParsingError> {
    let mut collector = Vec::new();
    flattened_array_rec_desc(v, &mut collector)?;
    Ok(collector)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Value::*;
    use rstest::rstest;

    /// Tries to assure that `try_vec_then_singleton` can convert an Array
    #[test]
    fn try_vec_then_singleton_array() {
        // The value to test with
        let value = Array(vec![Int(4711), Nil, Int(42)]);

        // The actual conversion
        let converted = try_vec_then_singleton::<Option<i64>>(value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711), None, Some(42)]);
    }

    /// Tries to assure that `try_vec_then_singleton` can convert a basic value
    #[test]
    fn try_vec_then_singleton_basic() {
        // The value to test with
        let value = Int(4711);

        // The actual conversion
        let converted = try_vec_then_singleton::<Option<i64>>(value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711)]);
    }

    /// Tries to assure that if both `Vec<T>` and `T` work `try_vec_then_singleton` picks correctly
    #[test]
    fn try_vec_then_singleton_ambiguous() {
        // The value to test with
        let value = Array(vec![BulkString("foo".into()), BulkString("bar".into())]);

        // The actual conversion
        let converted = try_vec_then_singleton::<Option<Vec<String>>>(value).unwrap();

        // Check the resulting value
        assert_eq!(
            *converted,
            vec![Some(vec!["foo".to_string()]), Some(vec!["bar".to_string()])]
        );
    }

    /// Tries to assure that `try_vec_then_singleton` fails for unconvertable values
    #[test]
    fn try_vec_then_singleton_other_fails() {
        // The value to test with
        let value = BulkString("foo".into());

        // The actual conversion should fail as a `str` should not convert to `i64` or `Vec<i64>`.
        let err = try_vec_then_singleton::<Option<i64>>(value).unwrap_err();

        // Check the resulting value
        assert!(err.description.contains("not convert"));
    }

    /// Tries to assure that `try_singleton_then_vec` can convert an Array
    #[test]
    fn try_singleton_then_vec_array() {
        // The value to test with
        let value = Array(vec![Int(4711), Nil, Int(42)]);

        // The actual conversion
        let converted = try_singleton_then_vec::<Option<i64>>(value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711), None, Some(42)]);
    }

    /// Tries to assure that `try_singleton_then_vec` can convert a basic value
    #[test]
    fn try_singleton_then_vec_basic() {
        // The value to test with
        let value = Int(4711);

        // The actual conversion
        let converted = try_singleton_then_vec::<Option<i64>>(value).unwrap();

        // Check the resulting value
        assert_eq!(*converted, vec![Some(4711)]);
    }

    /// Tries to assure that if both `Vec<T>` and `T` work `try_singleton_then_vec` picks correctly
    #[test]
    fn try_singleton_then_vec_ambiguous() {
        // The value to test with
        let value = Array(vec![BulkString("foo".into()), BulkString("bar".into())]);

        // The actual conversion
        let converted = try_singleton_then_vec::<Option<Vec<String>>>(value).unwrap();

        // Check the resulting value
        assert_eq!(
            *converted,
            vec![Some(vec!["foo".to_string(), "bar".to_string()])]
        );
    }

    /// Tries to assure that `try_singleton_then_vec` fails for unconvertable values
    #[test]
    fn try_singleton_then_vec_other_fails() {
        // The value to test with
        let value = BulkString("foo".into());

        // The actual conversion should fail as a `str` should not convert to `i64` or `Vec<i64>`.
        let err = try_singleton_then_vec::<Option<i64>>(value).unwrap_err();

        // Check the resulting value
        assert!(err.description.contains("not convert"));
    }

    #[rstest]
    #[case::direct_item(Int(42), vec![42])]
    #[case::empty_array(Array(vec![]), vec![])]
    #[case::flat_array(Array(vec![Int(42), Double(4711.)]), vec![42, 4711])]
    #[case::nested_array(Array(vec![Int(23), Array(vec![Array(vec![Int(42)]), Int(151)])]), vec![23, 42, 151])]
    fn flattened_conversion_success(#[case] input: Value, #[case] expected: Vec<u64>) {
        let result = flattened_array::<u64>(input).unwrap();
        assert_eq!(*result, expected);
    }

    #[rstest]
    #[case::direct_item(BulkString("foo".into()))]
    #[case::flat_array(Array(vec![Int(42), BulkString("foo".into()), Int(4711)]))]
    #[case::nested_array(Array(vec![Int(23), Array(vec![Int(42), BulkString("foo".into()), Int(151)]), Int(4711)]))]
    fn flattened_conversion_failure(#[case] input: Value) {
        let err = flattened_array::<u64>(input).unwrap_err();
        assert!(err.description.contains("convert"));
    }
}
