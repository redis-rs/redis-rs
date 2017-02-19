extern crate redis;


#[test]
fn test_is_single_arg() {
    use redis::ToRedisArgs;

    let sslice: &[_] = &["foo"][..];
    let nestslice: &[_] = &[sslice][..];
    let nestvec = vec![nestslice];
    let bytes = b"Hello World!";
    let twobytesslice: &[_] = &[bytes, bytes][..];
    let twobytesvec = vec![bytes, bytes];

    assert_eq!("foo".is_single_arg(), true);
    assert_eq!(sslice.is_single_arg(), true);
    assert_eq!(nestslice.is_single_arg(), true);
    assert_eq!(nestvec.is_single_arg(), true);
    assert_eq!(bytes.is_single_arg(), true);
    assert_eq!(twobytesslice.is_single_arg(), false);
    assert_eq!(twobytesvec.is_single_arg(), false);
}

#[test]
fn test_info_dict() {
    use redis::{InfoDict, FromRedisValue, Value};

    let d: InfoDict =
        FromRedisValue::from_redis_value(&Value::Status("# this is a comment\nkey1:foo\nkey2:42\n"
                .into()))
            .unwrap();

    assert_eq!(d.get("key1"), Some("foo".to_string()));
    assert_eq!(d.get("key2"), Some(42i64));
    assert_eq!(d.get::<String>("key3"), None);
}

#[test]
fn test_i32() {
    use redis::{FromRedisValue, Value, ErrorKind};

    let i = FromRedisValue::from_redis_value(&Value::Status("42".into()));
    assert_eq!(i, Ok(42i32));

    let i = FromRedisValue::from_redis_value(&Value::Int(42));
    assert_eq!(i, Ok(42i32));

    let i = FromRedisValue::from_redis_value(&Value::Data("42".into()));
    assert_eq!(i, Ok(42i32));

    let bad_i: Result<i32, _> = FromRedisValue::from_redis_value(&Value::Status("42x".into()));
    assert_eq!(bad_i.unwrap_err().kind(), ErrorKind::TypeError);
}

#[test]
fn test_u32() {
    use redis::{FromRedisValue, Value, ErrorKind};

    let i = FromRedisValue::from_redis_value(&Value::Status("42".into()));
    assert_eq!(i, Ok(42u32));

    let bad_i: Result<u32, _> = FromRedisValue::from_redis_value(&Value::Status("-1".into()));
    assert_eq!(bad_i.unwrap_err().kind(), ErrorKind::TypeError);
}

#[test]
fn test_vec() {
    use redis::{FromRedisValue, Value};

    let v = FromRedisValue::from_redis_value(&Value::Bulk(vec![Value::Data("1".into()),
                                                               Value::Data("2".into()),
                                                               Value::Data("3".into())]));

    assert_eq!(v, Ok(vec![1i32, 2, 3]));
}

#[test]
fn test_bool() {
    use redis::{FromRedisValue, Value, ErrorKind};

    let v = FromRedisValue::from_redis_value(&Value::Status("1".into()));
    assert_eq!(v, Ok(true));

    let v = FromRedisValue::from_redis_value(&Value::Status("0".into()));
    assert_eq!(v, Ok(false));

    let v: Result<bool, _> = FromRedisValue::from_redis_value(&Value::Status("garbage".into()));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v = FromRedisValue::from_redis_value(&Value::Okay);
    assert_eq!(v, Ok(true));

    let v = FromRedisValue::from_redis_value(&Value::Nil);
    assert_eq!(v, Ok(false));

    let v = FromRedisValue::from_redis_value(&Value::Int(0));
    assert_eq!(v, Ok(false));

    let v = FromRedisValue::from_redis_value(&Value::Int(42));
    assert_eq!(v, Ok(true));
}

#[test]
fn test_types_to_redis_args() {
    use redis::ToRedisArgs;
    use std::collections::HashSet;
    use std::collections::BTreeSet;
    use std::collections::BTreeMap;

    assert!( 5i32.to_redis_args().len() > 0 );
    assert!( "abc".to_redis_args().len() > 0 );
    assert!( "abc".to_redis_args().len() > 0 );
    assert!( String::from("x").to_redis_args().len() > 0);

    assert!( [5,4].into_iter().cloned().collect::<HashSet<_>>()
        .to_redis_args().len() > 0 );

    assert!( [5,4].into_iter().cloned().collect::<BTreeSet<_>>()
        .to_redis_args().len() > 0 );

    // this can be used on something HMSET
    assert!( [("a", 5), ("b", 6), ("C",7) ].into_iter().cloned()
            .collect::<BTreeMap<_,_>>()
            .to_redis_args().len() > 0);


}