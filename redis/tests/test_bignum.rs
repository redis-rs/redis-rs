use redis::{ErrorKind, FromRedisValue, RedisResult, ToRedisArgs, Value};
use std::str::FromStr;

#[test]
fn test_rust_decimal() {
    use rust_decimal::Decimal;

    let content = "-79228162514264.337593543950335";

    let v: RedisResult<Decimal> =
        FromRedisValue::from_redis_value(&Value::Data(Vec::from(content)));
    assert_eq!(v, Ok(Decimal::from_str(content).unwrap()));

    let arg = ToRedisArgs::to_redis_args(&v.unwrap());
    assert_eq!(arg[0], Vec::from(content));

    let v: RedisResult<Decimal> =
        FromRedisValue::from_redis_value(&Value::Status("garbage".into()));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<Decimal> = FromRedisValue::from_redis_value(&Value::Int(0));
    assert_eq!(v.unwrap(), Decimal::from(0));

    let v: RedisResult<Decimal> = FromRedisValue::from_redis_value(&Value::Int(42));
    assert_eq!(v.unwrap(), Decimal::from(42));

    let v: RedisResult<Decimal> = FromRedisValue::from_redis_value(&Value::Okay);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<Decimal> = FromRedisValue::from_redis_value(&Value::Nil);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
}

#[test]
fn test_bigdecimal() {
    use bigdecimal::BigDecimal;

    let content = "-14272476927059598810582859.69449495136382746623";

    let v: RedisResult<BigDecimal> =
        FromRedisValue::from_redis_value(&Value::Data(Vec::from(content)));
    assert_eq!(v, Ok(BigDecimal::from_str(content).unwrap()));

    let arg = ToRedisArgs::to_redis_args(&v.unwrap());
    assert_eq!(arg[0], Vec::from(content));

    let v: RedisResult<BigDecimal> =
        FromRedisValue::from_redis_value(&Value::Status("garbage".into()));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<BigDecimal> = FromRedisValue::from_redis_value(&Value::Int(0));
    assert_eq!(v.unwrap(), BigDecimal::from(0));

    let v: RedisResult<BigDecimal> = FromRedisValue::from_redis_value(&Value::Int(42));
    assert_eq!(v.unwrap(), BigDecimal::from(42));

    let v: RedisResult<BigDecimal> = FromRedisValue::from_redis_value(&Value::Okay);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<BigDecimal> = FromRedisValue::from_redis_value(&Value::Nil);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
}

#[test]
fn test_bigint() {
    use num_bigint::BigInt;

    let content = "-1427247692705959881058285969449495136382746623";

    let v: RedisResult<BigInt> = FromRedisValue::from_redis_value(&Value::Data(Vec::from(content)));
    assert_eq!(v, Ok(BigInt::from_str(content).unwrap()));

    let arg = ToRedisArgs::to_redis_args(&v.unwrap());
    assert_eq!(arg[0], Vec::from(content));

    let v: RedisResult<BigInt> = FromRedisValue::from_redis_value(&Value::Status("garbage".into()));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<BigInt> = FromRedisValue::from_redis_value(&Value::Int(0));
    assert_eq!(v.unwrap(), BigInt::from(0));

    let v: RedisResult<BigInt> = FromRedisValue::from_redis_value(&Value::Int(42));
    assert_eq!(v.unwrap(), BigInt::from(42));

    let v: RedisResult<BigInt> = FromRedisValue::from_redis_value(&Value::Okay);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<BigInt> = FromRedisValue::from_redis_value(&Value::Nil);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
}

#[test]
fn test_biguint() {
    use num_bigint::BigUint;

    let content = "1427247692705959881058285969449495136382746623";

    let v: RedisResult<BigUint> =
        FromRedisValue::from_redis_value(&Value::Data(Vec::from(content)));
    assert_eq!(v, Ok(BigUint::from_str(content).unwrap()));

    let arg = ToRedisArgs::to_redis_args(&v.unwrap());
    assert_eq!(arg[0], Vec::from(content));

    let v: RedisResult<BigUint> =
        FromRedisValue::from_redis_value(&Value::Status("garbage".into()));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<BigUint> = FromRedisValue::from_redis_value(&Value::Int(0));
    assert_eq!(v.unwrap(), BigUint::from(0u32));

    let v: RedisResult<BigUint> = FromRedisValue::from_redis_value(&Value::Int(42));
    assert_eq!(v.unwrap(), BigUint::from(42u32));

    let v: RedisResult<BigUint> = FromRedisValue::from_redis_value(&Value::Okay);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<BigUint> = FromRedisValue::from_redis_value(&Value::Nil);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
}
