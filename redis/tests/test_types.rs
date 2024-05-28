use redis::{ErrorKind, FromRedisValue, RedisError, ToRedisArgs, Value};
mod support;

#[test]
fn test_is_io_error() {
    let err = RedisError::from((
        ErrorKind::IoError,
        "Multiplexed connection driver unexpectedly terminated",
    ));
    assert!(err.is_io_error());
}

#[test]
fn test_is_single_arg() {
    let sslice: &[_] = &["foo"][..];
    let nestslice: &[_] = &[sslice][..];
    let nestvec = vec![nestslice];
    let bytes = b"Hello World!";
    let twobytesslice: &[_] = &[bytes, bytes][..];
    let twobytesvec = vec![bytes, bytes];

    assert!("foo".is_single_arg());
    assert!(sslice.is_single_arg());
    assert!(nestslice.is_single_arg());
    assert!(nestvec.is_single_arg());
    assert!(bytes.is_single_arg());

    assert!(!twobytesslice.is_single_arg());
    assert!(!twobytesvec.is_single_arg());
}

/// The `FromRedisValue` trait provides two methods for parsing:
/// - `fn from_redis_value(&Value) -> Result<T, RedisError>`
/// - `fn from_owned_redis_value(Value) -> Result<T, RedisError>`
/// The `RedisParseMode` below allows choosing between the two
/// so that test logic does not need to be duplicated for each.
enum RedisParseMode {
    Owned,
    Ref,
}

impl RedisParseMode {
    /// Calls either `FromRedisValue::from_owned_redis_value` or
    /// `FromRedisValue::from_redis_value`.
    fn parse_redis_value<T: redis::FromRedisValue>(
        &self,
        value: redis::Value,
    ) -> Result<T, redis::RedisError> {
        match self {
            Self::Owned => redis::FromRedisValue::from_owned_redis_value(value),
            Self::Ref => redis::FromRedisValue::from_redis_value(&value),
        }
    }
}

#[test]
fn test_info_dict() {
    use redis::{InfoDict, Value};

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let d: InfoDict = parse_mode
            .parse_redis_value(Value::Status(
                "# this is a comment\nkey1:foo\nkey2:42\n".into(),
            ))
            .unwrap();

        assert_eq!(d.get("key1"), Some("foo".to_string()));
        assert_eq!(d.get("key2"), Some(42i64));
        assert_eq!(d.get::<String>("key3"), None);
    }
}

#[test]
fn test_i32() {
    use redis::{ErrorKind, Value};

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let i = parse_mode.parse_redis_value(Value::Status("42".into()));
        assert_eq!(i, Ok(42i32));

        let i = parse_mode.parse_redis_value(Value::Int(42));
        assert_eq!(i, Ok(42i32));

        let i = parse_mode.parse_redis_value(Value::Data("42".into()));
        assert_eq!(i, Ok(42i32));

        let bad_i: Result<i32, _> = parse_mode.parse_redis_value(Value::Status("42x".into()));
        assert_eq!(bad_i.unwrap_err().kind(), ErrorKind::TypeError);
    }
}

#[test]
fn test_u32() {
    use redis::{ErrorKind, Value};

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let i = parse_mode.parse_redis_value(Value::Status("42".into()));
        assert_eq!(i, Ok(42u32));

        let bad_i: Result<u32, _> = parse_mode.parse_redis_value(Value::Status("-1".into()));
        assert_eq!(bad_i.unwrap_err().kind(), ErrorKind::TypeError);
    }
}

#[test]
fn test_vec() {
    use redis::Value;

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let v = parse_mode.parse_redis_value(Value::Bulk(vec![
            Value::Data("1".into()),
            Value::Data("2".into()),
            Value::Data("3".into()),
        ]));
        assert_eq!(v, Ok(vec![1i32, 2, 3]));

        let content: &[u8] = b"\x01\x02\x03\x04";
        let content_vec: Vec<u8> = Vec::from(content);
        let v = parse_mode.parse_redis_value(Value::Data(content_vec.clone()));
        assert_eq!(v, Ok(content_vec));

        let content: &[u8] = b"1";
        let content_vec: Vec<u8> = Vec::from(content);
        let v = parse_mode.parse_redis_value(Value::Data(content_vec.clone()));
        assert_eq!(v, Ok(vec![b'1']));
        let v = parse_mode.parse_redis_value(Value::Data(content_vec));
        assert_eq!(v, Ok(vec![1_u16]));
    }
}

#[test]
fn test_box_slice() {
    use redis::{FromRedisValue, Value};
    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let v = parse_mode.parse_redis_value(Value::Bulk(vec![
            Value::Data("1".into()),
            Value::Data("2".into()),
            Value::Data("3".into()),
        ]));
        assert_eq!(v, Ok(vec![1i32, 2, 3].into_boxed_slice()));

        let content: &[u8] = b"\x01\x02\x03\x04";
        let content_vec: Vec<u8> = Vec::from(content);
        let v = parse_mode.parse_redis_value(Value::Data(content_vec.clone()));
        assert_eq!(v, Ok(content_vec.into_boxed_slice()));

        let content: &[u8] = b"1";
        let content_vec: Vec<u8> = Vec::from(content);
        let v = parse_mode.parse_redis_value(Value::Data(content_vec.clone()));
        assert_eq!(v, Ok(vec![b'1'].into_boxed_slice()));
        let v = parse_mode.parse_redis_value(Value::Data(content_vec));
        assert_eq!(v, Ok(vec![1_u16].into_boxed_slice()));

        assert_eq!(
        Box::<[i32]>::from_redis_value(
            &Value::Data("just a string".into())
        ).unwrap_err().to_string(),
        "Response was of incompatible type - TypeError: \"Conversion to alloc::boxed::Box<[i32]> failed.\" (response was string-data('\"just a string\"'))",
    );
    }
}

#[test]
fn test_arc_slice() {
    use redis::{FromRedisValue, Value};
    use std::sync::Arc;

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let v = parse_mode.parse_redis_value(Value::Bulk(vec![
            Value::Data("1".into()),
            Value::Data("2".into()),
            Value::Data("3".into()),
        ]));
        assert_eq!(v, Ok(Arc::from(vec![1i32, 2, 3])));

        let content: &[u8] = b"\x01\x02\x03\x04";
        let content_vec: Vec<u8> = Vec::from(content);
        let v = parse_mode.parse_redis_value(Value::Data(content_vec.clone()));
        assert_eq!(v, Ok(Arc::from(content_vec)));

        let content: &[u8] = b"1";
        let content_vec: Vec<u8> = Vec::from(content);
        let v = parse_mode.parse_redis_value(Value::Data(content_vec.clone()));
        assert_eq!(v, Ok(Arc::from(vec![b'1'])));
        let v = parse_mode.parse_redis_value(Value::Data(content_vec));
        assert_eq!(v, Ok(Arc::from(vec![1_u16])));

        assert_eq!(
        Arc::<[i32]>::from_redis_value(
            &Value::Data("just a string".into())
        ).unwrap_err().to_string(),
        "Response was of incompatible type - TypeError: \"Conversion to alloc::sync::Arc<[i32]> failed.\" (response was string-data('\"just a string\"'))",
    );
    }
}

#[test]
fn test_single_bool_vec() {
    use redis::Value;

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let v = parse_mode.parse_redis_value(Value::Data("1".into()));

        assert_eq!(v, Ok(vec![true]));
    }
}

#[test]
fn test_single_i32_vec() {
    use redis::Value;

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let v = parse_mode.parse_redis_value(Value::Data("1".into()));

        assert_eq!(v, Ok(vec![1i32]));
    }
}

#[test]
fn test_single_u32_vec() {
    use redis::Value;

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let v = parse_mode.parse_redis_value(Value::Data("42".into()));

        assert_eq!(v, Ok(vec![42u32]));
    }
}

#[test]
fn test_single_string_vec() {
    use redis::Value;

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let v = parse_mode.parse_redis_value(Value::Data("1".into()));

        assert_eq!(v, Ok(vec!["1".to_string()]));
    }
}

#[test]
fn test_tuple() {
    use redis::Value;

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let v = parse_mode.parse_redis_value(Value::Bulk(vec![Value::Bulk(vec![
            Value::Data("1".into()),
            Value::Data("2".into()),
            Value::Data("3".into()),
        ])]));

        assert_eq!(v, Ok(((1i32, 2, 3,),)));
    }
}

#[test]
fn test_hashmap() {
    use fnv::FnvHasher;
    use redis::{ErrorKind, Value};
    use std::collections::HashMap;
    use std::hash::BuildHasherDefault;

    type Hm = HashMap<String, i32>;

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let v: Result<Hm, _> = parse_mode.parse_redis_value(Value::Bulk(vec![
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
        let v: Result<HmHasher, _> = parse_mode.parse_redis_value(Value::Bulk(vec![
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

        let v: Result<Hm, _> =
            parse_mode.parse_redis_value(Value::Bulk(vec![Value::Data("a".into())]));
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
    }
}

#[test]
fn test_bool() {
    use redis::{ErrorKind, Value};

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let v = parse_mode.parse_redis_value(Value::Data("1".into()));
        assert_eq!(v, Ok(true));

        let v = parse_mode.parse_redis_value(Value::Data("0".into()));
        assert_eq!(v, Ok(false));

        let v: Result<bool, _> = parse_mode.parse_redis_value(Value::Data("garbage".into()));
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

        let v = parse_mode.parse_redis_value(Value::Status("1".into()));
        assert_eq!(v, Ok(true));

        let v = parse_mode.parse_redis_value(Value::Status("0".into()));
        assert_eq!(v, Ok(false));

        let v: Result<bool, _> = parse_mode.parse_redis_value(Value::Status("garbage".into()));
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

        let v = parse_mode.parse_redis_value(Value::Okay);
        assert_eq!(v, Ok(true));

        let v = parse_mode.parse_redis_value(Value::Nil);
        assert_eq!(v, Ok(false));

        let v = parse_mode.parse_redis_value(Value::Int(0));
        assert_eq!(v, Ok(false));

        let v = parse_mode.parse_redis_value(Value::Int(42));
        assert_eq!(v, Ok(true));
    }
}

#[cfg(feature = "bytes")]
#[test]
fn test_bytes() {
    use bytes::Bytes;
    use redis::{ErrorKind, RedisResult, Value};

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let content: &[u8] = b"\x01\x02\x03\x04";
        let content_vec: Vec<u8> = Vec::from(content);
        let content_bytes = Bytes::from_static(content);

        let v: RedisResult<Bytes> = parse_mode.parse_redis_value(Value::Data(content_vec));
        assert_eq!(v, Ok(content_bytes));

        let v: RedisResult<Bytes> = parse_mode.parse_redis_value(Value::Status("garbage".into()));
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

        let v: RedisResult<Bytes> = parse_mode.parse_redis_value(Value::Okay);
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

        let v: RedisResult<Bytes> = parse_mode.parse_redis_value(Value::Nil);
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

        let v: RedisResult<Bytes> = parse_mode.parse_redis_value(Value::Int(0));
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

        let v: RedisResult<Bytes> = parse_mode.parse_redis_value(Value::Int(42));
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
    }
}

#[cfg(feature = "uuid")]
#[test]
fn test_uuid() {
    use std::str::FromStr;

    use redis::{ErrorKind, FromRedisValue, RedisResult, Value};
    use uuid::Uuid;

    let uuid = Uuid::from_str("abab64b7-e265-4052-a41b-23e1e28674bf").unwrap();
    let bytes = uuid.as_bytes().to_vec();

    let v: RedisResult<Uuid> = FromRedisValue::from_redis_value(&Value::Data(bytes));
    assert_eq!(v, Ok(uuid));

    let v: RedisResult<Uuid> = FromRedisValue::from_redis_value(&Value::Status("garbage".into()));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<Uuid> = FromRedisValue::from_redis_value(&Value::Okay);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<Uuid> = FromRedisValue::from_redis_value(&Value::Nil);
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<Uuid> = FromRedisValue::from_redis_value(&Value::Int(0));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

    let v: RedisResult<Uuid> = FromRedisValue::from_redis_value(&Value::Int(42));
    assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
}

#[test]
fn test_cstring() {
    use redis::{ErrorKind, RedisResult, Value};
    use std::ffi::CString;

    for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
        let content: &[u8] = b"\x01\x02\x03\x04";
        let content_vec: Vec<u8> = Vec::from(content);

        let v: RedisResult<CString> = parse_mode.parse_redis_value(Value::Data(content_vec));
        assert_eq!(v, Ok(CString::new(content).unwrap()));

        let v: RedisResult<CString> = parse_mode.parse_redis_value(Value::Status("garbage".into()));
        assert_eq!(v, Ok(CString::new("garbage").unwrap()));

        let v: RedisResult<CString> = parse_mode.parse_redis_value(Value::Okay);
        assert_eq!(v, Ok(CString::new("OK").unwrap()));

        let v: RedisResult<CString> =
            parse_mode.parse_redis_value(Value::Status("gar\0bage".into()));
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

        let v: RedisResult<CString> = parse_mode.parse_redis_value(Value::Nil);
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

        let v: RedisResult<CString> = parse_mode.parse_redis_value(Value::Int(0));
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

        let v: RedisResult<CString> = parse_mode.parse_redis_value(Value::Int(42));
        assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
    }
}

#[test]
fn test_types_to_redis_args() {
    use redis::ToRedisArgs;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashMap;
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

    // this can also be used on something HMSET
    assert!(![("d", 8), ("e", 9), ("f", 10)]
        .iter()
        .cloned()
        .collect::<HashMap<_, _>>()
        .to_redis_args()
        .is_empty());
}

#[test]
fn test_large_usize_array_to_redis_args_and_back() {
    use crate::support::encode_value;
    use redis::ToRedisArgs;

    let mut array = [0; 1000];
    for (i, item) in array.iter_mut().enumerate() {
        *item = i;
    }

    let vec = (&array).to_redis_args();
    assert_eq!(array.len(), vec.len());

    let value = Value::Bulk(vec.iter().map(|val| Value::Data(val.clone())).collect());
    let mut encoded_input = Vec::new();
    encode_value(&value, &mut encoded_input).unwrap();

    let new_array: [usize; 1000] = FromRedisValue::from_redis_value(&value).unwrap();
    assert_eq!(new_array, array);
}

#[test]
fn test_large_u8_array_to_redis_args_and_back() {
    use crate::support::encode_value;
    use redis::ToRedisArgs;

    let mut array: [u8; 1000] = [0; 1000];
    for (i, item) in array.iter_mut().enumerate() {
        *item = (i % 256) as u8;
    }

    let vec = (&array).to_redis_args();
    assert_eq!(vec.len(), 1);
    assert_eq!(array.len(), vec[0].len());

    let value = Value::Bulk(vec[0].iter().map(|val| Value::Int(*val as i64)).collect());
    let mut encoded_input = Vec::new();
    encode_value(&value, &mut encoded_input).unwrap();

    let new_array: [u8; 1000] = FromRedisValue::from_redis_value(&value).unwrap();
    assert_eq!(new_array, array);
}

#[test]
fn test_large_string_array_to_redis_args_and_back() {
    use crate::support::encode_value;
    use redis::ToRedisArgs;

    let mut array: [String; 1000] = [(); 1000].map(|_| String::new());
    for (i, item) in array.iter_mut().enumerate() {
        *item = format!("{i}");
    }

    let vec = (&array).to_redis_args();
    assert_eq!(array.len(), vec.len());

    let value = Value::Bulk(vec.iter().map(|val| Value::Data(val.clone())).collect());
    let mut encoded_input = Vec::new();
    encode_value(&value, &mut encoded_input).unwrap();

    let new_array: [String; 1000] = FromRedisValue::from_redis_value(&value).unwrap();
    assert_eq!(new_array, array);
}

#[test]
fn test_0_length_usize_array_to_redis_args_and_back() {
    use crate::support::encode_value;
    use redis::ToRedisArgs;

    let array: [usize; 0] = [0; 0];

    let vec = (&array).to_redis_args();
    assert_eq!(array.len(), vec.len());

    let value = Value::Bulk(vec.iter().map(|val| Value::Data(val.clone())).collect());
    let mut encoded_input = Vec::new();
    encode_value(&value, &mut encoded_input).unwrap();

    let new_array: [usize; 0] = FromRedisValue::from_redis_value(&value).unwrap();
    assert_eq!(new_array, array);

    let new_array: [usize; 0] = FromRedisValue::from_redis_value(&Value::Nil).unwrap();
    assert_eq!(new_array, array);
}
