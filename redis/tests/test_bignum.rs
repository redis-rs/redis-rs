#![cfg(any(
    feature = "rust_decimal",
    feature = "bigdecimal",
    feature = "num-bigint"
))]
use redis::{ErrorKind, FromRedisValue, RedisResult, ToRedisArgs, Value};
use std::str::FromStr;

fn test<T>(content: &str)
where
    T: FromRedisValue
        + ToRedisArgs
        + std::str::FromStr
        + std::convert::From<u32>
        + std::cmp::PartialEq
        + std::fmt::Debug,
    <T as FromStr>::Err: std::fmt::Debug,
{
    let v: RedisResult<T> = FromRedisValue::from_redis_value(&Value::Data(Vec::from(content)));
    assert_eq!(v, Ok(T::from_str(content).unwrap()));

    let arg = ToRedisArgs::to_redis_args(&v.unwrap());
    assert_eq!(arg[0], Vec::from(content));

    let v: RedisResult<T> = FromRedisValue::from_redis_value(&Value::Int(0));
    assert_eq!(v.unwrap(), T::from(0u32));

    let v: RedisResult<T> = FromRedisValue::from_redis_value(&Value::Int(42));
    assert_eq!(v.unwrap(), T::from(42u32));

    let v: RedisResult<T> = FromRedisValue::from_redis_value(&Value::Okay);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<T> = FromRedisValue::from_redis_value(&Value::Nil);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
}

#[test]
#[cfg(feature = "rust_decimal")]
fn test_rust_decimal() {
    test::<rust_decimal::Decimal>("-79228162514264.337593543950335");
}

#[test]
#[cfg(feature = "bigdecimal")]
fn test_bigdecimal() {
    test::<bigdecimal::BigDecimal>("-14272476927059598810582859.69449495136382746623");
}

#[test]
#[cfg(feature = "num-bigint")]
fn test_bigint() {
    test::<num_bigint::BigInt>("-1427247692705959881058285969449495136382746623");
}

#[test]
#[cfg(feature = "num-bigint")]
fn test_biguint() {
    test::<num_bigint::BigUint>("1427247692705959881058285969449495136382746623");
}
