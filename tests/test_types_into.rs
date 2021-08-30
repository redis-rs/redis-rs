#[test]
fn test_info_dict() {
    use redis::{InfoDict, RedisValueInto, Value};

    let d: InfoDict = RedisValueInto::redis_value_into(Value::Status(
        "# this is a comment\nkey1:foo\nkey2:42\n".into(),
    ))
    .unwrap();

    assert_eq!(d.get("key1"), Some("foo".to_string()));
    assert_eq!(d.get("key2"), Some(42i64));
    assert_eq!(d.get::<String>("key3"), None);
}

#[test]
fn test_i32() {
    use redis::{ErrorKind, RedisValueInto, Value};

    let i = RedisValueInto::redis_value_into(Value::Status("42".into()));
    assert_eq!(i, Ok(42i32));

    let i = RedisValueInto::redis_value_into(Value::Int(42));
    assert_eq!(i, Ok(42i32));

    let i = RedisValueInto::redis_value_into(Value::Data("42".into()));
    assert_eq!(i, Ok(42i32));

    let bad_i: Result<i32, _> = RedisValueInto::redis_value_into(Value::Status("42x".into()));
    assert_eq!(bad_i.unwrap_err().kind(), ErrorKind::TypeError);
}

#[test]
fn test_u32() {
    use redis::{ErrorKind, RedisValueInto, Value};

    let i = RedisValueInto::redis_value_into(Value::Status("42".into()));
    assert_eq!(i, Ok(42u32));

    let bad_i: Result<u32, _> = RedisValueInto::redis_value_into(Value::Status("-1".into()));
    assert_eq!(bad_i.unwrap_err().kind(), ErrorKind::TypeError);
}

#[test]
fn test_vec() {
    use redis::{RedisValueInto, Value};

    let v = RedisValueInto::redis_value_into(Value::Bulk(vec![
        Value::Data("1".into()),
        Value::Data("2".into()),
        Value::Data("3".into()),
    ]));

    assert_eq!(v, Ok(vec![1i32, 2, 3]));
}

#[test]
fn test_hashmap() {
    use fnv::FnvHasher;
    use redis::{RedisValueInto, Value};
    use std::collections::HashMap;
    use std::hash::BuildHasherDefault;

    type Hm = HashMap<String, i32>;

    let v: Result<Hm, _> = RedisValueInto::redis_value_into(Value::Bulk(vec![
        Value::Data("a".into()),
        Value::Data("1".into()),
        Value::Data("b".into()),
        Value::Data("2".into()),
        Value::Data("c".into()),
        Value::Data("3".into()),
    ]));
    let mut e: Hm = HashMap::new();
    e.insert("a".into(), 1);
    e.insert("b".into(), 2);
    e.insert("c".into(), 3);
    assert_eq!(v, Ok(e));

    type Hasher = BuildHasherDefault<FnvHasher>;
    type HmHasher = HashMap<String, i32, Hasher>;
    let v: Result<HmHasher, _> = RedisValueInto::redis_value_into(Value::Bulk(vec![
        Value::Data("a".into()),
        Value::Data("1".into()),
        Value::Data("b".into()),
        Value::Data("2".into()),
        Value::Data("c".into()),
        Value::Data("3".into()),
    ]));

    let fnv = Hasher::default();
    let mut e: HmHasher = HashMap::with_hasher(fnv);
    e.insert("a".into(), 1);
    e.insert("b".into(), 2);
    e.insert("c".into(), 3);
    assert_eq!(v, Ok(e));
}

#[test]
fn test_bool() {
    use redis::{ErrorKind, RedisValueInto, Value};

    let v = RedisValueInto::redis_value_into(Value::Data("1".into()));
    assert_eq!(v, Ok(true));

    let v = RedisValueInto::redis_value_into(Value::Data("0".into()));
    assert_eq!(v, Ok(false));

    let v: Result<bool, _> = RedisValueInto::redis_value_into(Value::Data("garbage".into()));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v = RedisValueInto::redis_value_into(Value::Status("1".into()));
    assert_eq!(v, Ok(true));

    let v = RedisValueInto::redis_value_into(Value::Status("0".into()));
    assert_eq!(v, Ok(false));

    let v: Result<bool, _> = RedisValueInto::redis_value_into(Value::Status("garbage".into()));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v = RedisValueInto::redis_value_into(Value::Okay);
    assert_eq!(v, Ok(true));

    let v = RedisValueInto::redis_value_into(Value::Nil);
    assert_eq!(v, Ok(false));

    let v = RedisValueInto::redis_value_into(Value::Int(0));
    assert_eq!(v, Ok(false));

    let v = RedisValueInto::redis_value_into(Value::Int(42));
    assert_eq!(v, Ok(true));
}

#[cfg(feature = "bytes")]
#[test]
fn test_bytes() {
    use bytes::Bytes;
    use redis::{ErrorKind, RedisResult, RedisValueInto, Value};

    let content: &[u8] = b"\x01\x02\x03\x04";
    let content_vec: Vec<u8> = Vec::from(content);
    let content_bytes = Bytes::from_static(content);

    let v: RedisResult<Bytes> = RedisValueInto::redis_value_into(Value::Data(content_vec));
    assert_eq!(v, Ok(content_bytes));

    let v: RedisResult<Bytes> = RedisValueInto::redis_value_into(Value::Status("garbage".into()));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<Bytes> = RedisValueInto::redis_value_into(Value::Okay);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<Bytes> = RedisValueInto::redis_value_into(Value::Nil);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<Bytes> = RedisValueInto::redis_value_into(Value::Int(0));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<Bytes> = RedisValueInto::redis_value_into(Value::Int(42));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
}

#[test]
fn test_types_to_redis_args() {
    use redis::ToRedisArgs;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashSet;

    assert!(!5i32.to_redis_args().is_empty());
    assert!(!"abc".to_redis_args().is_empty());
    assert!(!"abc".to_redis_args().is_empty());
    assert!(!String::from("x").to_redis_args().is_empty());

    assert!(![5, 4]
        .iter()
        .cloned()
        .collect::<HashSet<_>>()
        .to_redis_args()
        .is_empty());

    assert!(![5, 4]
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .to_redis_args()
        .is_empty());

    // this can be used on something HMSET
    assert!(![("a", 5), ("b", 6), ("C", 7)]
        .iter()
        .cloned()
        .collect::<BTreeMap<_, _>>()
        .to_redis_args()
        .is_empty());
}
