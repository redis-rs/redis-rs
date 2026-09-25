//! Utilities for implementing types

use crate::{FromRedisValue, ParsingError, ToRedisArgs, ToSingleRedisArg, Value};

/// A shortcut function to invoke `FromRedisValue::from_redis_value_ref`
/// to make the API slightly nicer.
pub fn from_redis_value_ref<T: FromRedisValue>(v: &Value) -> Result<T, ParsingError> {
    FromRedisValue::from_redis_value_ref(v)
}

/// A shortcut function to invoke `FromRedisValue::from_redis_value`
/// to make the API slightly nicer.
pub fn from_redis_value<T: FromRedisValue>(v: Value) -> Result<T, ParsingError> {
    FromRedisValue::from_redis_value(v)
}

/// Calculates a digest/hash of the given value for use with Redis value comparison operations.
/// This function uses the XXH3 algorithm, which is the same algorithm used by Redis for its DIGEST command.
/// The resulting digest can be used with `ValueComparison::IFDEQ` and `ValueComparison::IFDNE`.
///
/// # Example
/// ```rust
/// use redis::{calculate_value_digest, ValueComparison, SetOptions};
///
/// let value = "my_value";
/// let digest = calculate_value_digest(value);
///
/// // Use the digest in a value comparison
/// let opts = SetOptions::default()
///     .value_comparison(ValueComparison::ifdeq(&digest));
/// ```
pub fn calculate_value_digest<T: ToRedisArgs>(value: T) -> String {
    use xxhash_rust::xxh3::xxh3_64;

    // Convert the value to Redis args format (bytes)
    let args = value.to_redis_args();

    // For consistency with Redis behavior, hash the concatenated bytes
    // of all arguments, similar to how Redis would serialize the value
    let mut combined_bytes = Vec::new();
    for arg in args {
        combined_bytes.extend_from_slice(&arg);
    }

    // Calculate XXH3 hash (64-bit) and format as hexadecimal string
    let hash = xxh3_64(&combined_bytes);
    format!("{hash:016x}")
}

pub(crate) fn get_inner_value(v: &Value) -> &Value {
    if let Value::Attribute {
        data,
        attributes: _,
    } = v
    {
        data.as_ref()
    } else {
        v
    }
}

pub(crate) fn get_owned_inner_value(v: Value) -> Value {
    if let Value::Attribute {
        data,
        attributes: _,
    } = v
    {
        *data
    } else {
        v
    }
}

pub(crate) fn to_single_arg(value: impl ToSingleRedisArg) -> Vec<u8> {
    let mut vec = value.to_redis_args();
    debug_assert_eq!(
        vec.len(),
        1,
        "Value implementing ToSingleRedisArg has to become a single argument"
    );
    vec.pop().unwrap_or_default()
}

/// RESP3 returns key/value replies that RESP2 flattened into a single array as an
/// array of two-element pairs instead (`ZRANGE ... WITHSCORES` and friends). Detect
/// that shape so it converts into a map type just like the RESP2 shape does. This
/// mirrors the check `<(K, V)>::from_redis_values` already makes via
/// `Value::is_collection_of_len`.
pub(crate) fn is_nested_pairs(items: &[Value]) -> bool {
    !items.is_empty() && items.iter().all(|item| item.is_collection_of_len(2))
}

/// Validates that the given string is a valid 16-byte hex digest.
pub fn is_valid_16_bytes_hex_digest(s: &str) -> bool {
    s.len() == 16 && s.chars().all(|c| c.is_ascii_hexdigit())
}

pub(crate) fn vec_to_array<T, const N: usize>(
    items: Vec<T>,
    original_value: &Value,
) -> Result<[T; N], ParsingError> {
    match items.try_into() {
        Ok(array) => Ok(array),
        Err(items) => {
            let msg = format!(
                "Response has wrong dimension, expected {N}, got {}",
                items.len()
            );
            crate::errors::invalid_type_error!(original_value, msg)
        }
    }
}
