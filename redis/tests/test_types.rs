mod support;

mod types {
    use std::{
        collections::{HashMap, HashSet},
        error::Error,
        rc::Rc,
        sync::Arc,
    };

    use redis::{ErrorKind, FromRedisValue, RedisError, RedisResult, ToRedisArgs, Value};

    #[test]
    fn test_is_io_error() {
        let err = RedisError::from((
            ErrorKind::IoError,
            "Multiplexed connection driver unexpectedly terminated",
        ));
        assert!(err.is_io_error());
    }

    #[test]
    fn test_redis_error_source_presence_for_io_wrapped_errors() {
        let io_error = RedisError::from(std::io::Error::new(
            std::io::ErrorKind::Other,
            "I/O failure",
        ));
        let source = io_error.source();
        assert_eq!(io_error.kind(), redis::ErrorKind::IoError);

        assert!(source.is_some());
        assert_eq!(source.unwrap().to_string(), "I/O failure");
    }

    #[test]
    fn test_redis_error_source_absence_for_non_io_wrapped_errors() {
        // Even though the ErrorKind is IoError, no actual I/O error is wrapped, so the source should be None.
        let simulated_io_error = RedisError::from((ErrorKind::IoError, "Simulated I/O error"));
        // Similarly this error is not an Extension error, even though it's ErrorKind is ExtensionError
        let simulated_extension_error =
            RedisError::from((ErrorKind::ExtensionError, "Simulated extension error"));
        let simulated_type_error_with_details = RedisError::from((
            ErrorKind::TypeError,
            "Simulated type error",
            "Type error details".to_string(),
        ));

        let an_extension_error =
            redis::make_extension_error("A true extension error".to_string(), None);
        assert_eq!(an_extension_error.kind(), redis::ErrorKind::ExtensionError);

        for error in [
            simulated_io_error,
            simulated_extension_error,
            simulated_type_error_with_details,
            an_extension_error,
        ] {
            assert!(error.source().is_none());
        }
    }

    #[test]
    fn test_is_single_arg() {
        let sslice: &[_] = &["foo"][..];
        let nestslice: &[_] = &[sslice][..];
        let nestvec = vec![nestslice];
        let bytes = b"Hello World!";
        let twobytesslice: &[_] = &[bytes, bytes][..];
        let twobytesvec = vec![bytes, bytes];

        assert_eq!("foo".num_of_args(), 1);
        assert_eq!(sslice.num_of_args(), 1);
        assert_eq!(nestslice.num_of_args(), 1);
        assert_eq!(nestvec.num_of_args(), 1);
        assert_eq!(bytes.num_of_args(), 1);
        assert_eq!(Arc::new(sslice).num_of_args(), 1);
        assert_eq!(Rc::new(nestslice).num_of_args(), 1);

        assert_eq!(twobytesslice.num_of_args(), 2);
        assert_eq!(twobytesvec.num_of_args(), 2);
        assert_eq!(Arc::new(twobytesslice).num_of_args(), 2);
        assert_eq!(Rc::new(twobytesslice).num_of_args(), 2);
    }

    /// The `FromRedisValue` trait provides two methods for parsing:
    /// - `fn from_redis_value(&Value) -> Result<T, RedisError>`
    /// - `fn from_owned_redis_value(Value) -> Result<T, RedisError>`
    ///
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
        use redis::InfoDict;

        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let d: InfoDict = parse_mode
                .parse_redis_value(Value::SimpleString(
                    "# this is a comment\nkey1:foo\nkey2:42\n".into(),
                ))
                .unwrap();

            assert_eq!(d.get("key1"), Some("foo".to_string()));
            assert_eq!(d.get("key2"), Some(42i64));
            assert_eq!(d.get::<String>("key3"), None);
        }
    }

    #[test]
    fn test_role_ret() {
        use redis::ReplicaInfo;
        use redis::Role;

        let parse_models = [RedisParseMode::Owned, RedisParseMode::Ref];
        let test_cases = vec![
            (
                Value::Array(vec![
                    Value::BulkString("master".into()),
                    Value::Int(3129659),
                    Value::Array(vec![
                        Value::Array(vec![
                            Value::BulkString("127.0.0.1".into()),
                            Value::BulkString("9001".into()),
                            Value::BulkString("3129242".into()),
                        ]),
                        Value::Array(vec![
                            Value::BulkString("127.0.0.1".into()),
                            Value::BulkString("9002".into()),
                            Value::BulkString("3129543".into()),
                        ]),
                    ]),
                ]),
                Role::Primary {
                    replication_offset: 3129659,
                    replicas: vec![
                        ReplicaInfo {
                            ip: "127.0.0.1".to_string(),
                            port: 9001,
                            replication_offset: 3129242,
                        },
                        ReplicaInfo {
                            ip: "127.0.0.1".to_string(),
                            port: 9002,
                            replication_offset: 3129543,
                        },
                    ],
                },
            ),
            (
                Value::Array(vec![
                    Value::BulkString("slave".into()),
                    Value::BulkString("127.0.0.1".into()),
                    Value::Int(9000),
                    Value::BulkString("connected".into()),
                    Value::Int(3167038),
                ]),
                Role::Replica {
                    primary_ip: "127.0.0.1".to_string(),
                    primary_port: 9000,
                    replication_state: "connected".to_string(),
                    data_received: 3167038,
                },
            ),
            (
                Value::Array(vec![
                    Value::BulkString("sentinel".into()),
                    Value::Array(vec![
                        Value::BulkString("resque-master".into()),
                        Value::BulkString("html-fragments-master".into()),
                        Value::BulkString("stats-master".into()),
                        Value::BulkString("metadata-master".into()),
                    ]),
                ]),
                Role::Sentinel {
                    primary_names: vec![
                        "resque-master".to_string(),
                        "html-fragments-master".to_string(),
                        "stats-master".to_string(),
                        "metadata-master".to_string(),
                    ],
                },
            ),
        ];

        for parse_mode in &parse_models {
            for (value, expected) in test_cases.clone() {
                let parsed: Role = parse_mode.parse_redis_value(value).unwrap();
                assert_eq!(parsed, expected);
            }
        }
    }

    #[test]
    fn test_i32() {
        // from the book hitchhiker's guide to the galaxy
        let everything_num = 42i32;
        let everything_str_x = "42x";

        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let i = parse_mode.parse_redis_value(Value::SimpleString(everything_num.to_string()));
            assert_eq!(i, Ok(everything_num));

            let i = parse_mode.parse_redis_value(Value::Int(everything_num.into()));
            assert_eq!(i, Ok(everything_num));

            let i =
                parse_mode.parse_redis_value(Value::BulkString(everything_num.to_string().into()));
            assert_eq!(i, Ok(everything_num));

            let bad_i: Result<i32, _> =
                parse_mode.parse_redis_value(Value::SimpleString(everything_str_x.into()));
            assert_eq!(bad_i.unwrap_err().kind(), ErrorKind::TypeError);

            let bad_i_deref: Result<Box<i32>, _> =
                parse_mode.parse_redis_value(Value::SimpleString(everything_str_x.into()));
            assert_eq!(bad_i_deref.unwrap_err().kind(), ErrorKind::TypeError);
        }
    }

    #[test]
    fn test_u32() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let i = parse_mode.parse_redis_value(Value::SimpleString("42".into()));
            assert_eq!(i, Ok(42u32));

            let bad_i: Result<u32, _> =
                parse_mode.parse_redis_value(Value::SimpleString("-1".into()));
            assert_eq!(bad_i.unwrap_err().kind(), ErrorKind::TypeError);
        }
    }

    #[test]
    fn test_parse_boxed() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let simple_string_exp = "Simple string".to_string();
            let v = parse_mode.parse_redis_value(Value::SimpleString(simple_string_exp.clone()));
            assert_eq!(v, Ok(Box::new(simple_string_exp.clone())));
        }
    }

    #[test]
    fn test_parse_arc() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let simple_string_exp = "Simple string".to_string();
            let v = parse_mode.parse_redis_value(Value::SimpleString(simple_string_exp.clone()));
            assert_eq!(v, Ok(Arc::new(simple_string_exp.clone())));

            // works with optional
            let v = parse_mode.parse_redis_value(Value::SimpleString(simple_string_exp.clone()));
            assert_eq!(v, Ok(Arc::new(Some(simple_string_exp))));
        }
    }

    #[test]
    fn test_parse_rc() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let simple_string_exp = "Simple string".to_string();
            let v = parse_mode.parse_redis_value(Value::SimpleString(simple_string_exp.clone()));
            assert_eq!(v, Ok(Rc::new(simple_string_exp.clone())));

            // works with optional
            let v = parse_mode.parse_redis_value(Value::SimpleString(simple_string_exp.clone()));
            assert_eq!(v, Ok(Rc::new(Some(simple_string_exp))));
        }
    }

    #[test]
    fn test_vec() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v = parse_mode.parse_redis_value(Value::Array(vec![
                Value::BulkString("1".into()),
                Value::BulkString("2".into()),
                Value::BulkString("3".into()),
            ]));
            assert_eq!(v, Ok(vec![1i32, 2, 3]));

            let content: &[u8] = b"\x01\x02\x03\x04";
            let content_vec: Vec<u8> = Vec::from(content);
            let v = parse_mode.parse_redis_value(Value::BulkString(content_vec.clone()));
            assert_eq!(v, Ok(content_vec));

            let content: &[u8] = b"1";
            let content_vec: Vec<u8> = Vec::from(content);
            let v = parse_mode.parse_redis_value(Value::BulkString(content_vec.clone()));
            assert_eq!(v, Ok(vec![b'1']));
            let v = parse_mode.parse_redis_value(Value::BulkString(content_vec));
            assert_eq!(v, Ok(vec![1_u16]));
        }
    }

    #[test]
    fn test_box_slice() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v = parse_mode.parse_redis_value(Value::Array(vec![
                Value::BulkString("1".into()),
                Value::BulkString("2".into()),
                Value::BulkString("3".into()),
            ]));
            assert_eq!(v, Ok(vec![1i32, 2, 3].into_boxed_slice()));

            let content: &[u8] = b"\x01\x02\x03\x04";
            let content_vec: Vec<u8> = Vec::from(content);
            let v = parse_mode.parse_redis_value(Value::BulkString(content_vec.clone()));
            assert_eq!(v, Ok(content_vec.into_boxed_slice()));

            let content: &[u8] = b"1";
            let content_vec: Vec<u8> = Vec::from(content);
            let v = parse_mode.parse_redis_value(Value::BulkString(content_vec.clone()));
            assert_eq!(v, Ok(vec![b'1'].into_boxed_slice()));
            let v = parse_mode.parse_redis_value(Value::BulkString(content_vec));
            assert_eq!(v, Ok(vec![1_u16].into_boxed_slice()));

            assert_eq!(
        Box::<[i32]>::from_redis_value(
            &Value::BulkString("just a string".into())
        ).unwrap_err().to_string(),
        "Response was of incompatible type - TypeError: \"Conversion to alloc::boxed::Box<[i32]> failed.\" (response was bulk-string('\"just a string\"'))",
    );
        }
    }

    #[test]
    fn test_arc_slice() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v = parse_mode.parse_redis_value::<Arc<[_]>>(Value::Array(vec![
                Value::BulkString("1".into()),
                Value::BulkString("2".into()),
                Value::BulkString("3".into()),
            ]));
            assert_eq!(v, Ok(Arc::from(vec![1i32, 2, 3])));

            let content: &[u8] = b"\x01\x02\x03\x04";
            let content_vec: Vec<u8> = Vec::from(content);
            let v =
                parse_mode.parse_redis_value::<Arc<[_]>>(Value::BulkString(content_vec.clone()));
            assert_eq!(v, Ok(Arc::from(content_vec)));

            let content: &[u8] = b"1";
            let content_vec: Vec<u8> = Vec::from(content);
            let v: Result<Arc<[u8]>, _> =
                parse_mode.parse_redis_value(Value::BulkString(content_vec.clone()));
            assert_eq!(v, Ok(Arc::from(vec![b'1'])));
            let v = parse_mode.parse_redis_value::<Arc<[_]>>(Value::BulkString(content_vec));
            assert_eq!(v, Ok(Arc::from(vec![1_u16])));

            assert_eq!(
            Arc::<[i32]>::from_redis_value(
                &Value::BulkString("just a string".into())
            ).unwrap_err().to_string(),
            "Response was of incompatible type - TypeError: \"Conversion to alloc::sync::Arc<[i32]> failed.\" (response was bulk-string('\"just a string\"'))",
        );
        }
    }

    #[test]
    fn test_single_bool_vec() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v = parse_mode.parse_redis_value(Value::BulkString("1".into()));

            assert_eq!(v, Ok(vec![true]));
        }
    }

    #[test]
    fn test_single_i32_vec() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v = parse_mode.parse_redis_value(Value::BulkString("1".into()));

            assert_eq!(v, Ok(vec![1i32]));
        }
    }

    #[test]
    fn test_single_u32_vec() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v = parse_mode.parse_redis_value(Value::BulkString("42".into()));

            assert_eq!(v, Ok(vec![42u32]));
        }
    }

    #[test]
    fn test_single_string_vec() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v = parse_mode.parse_redis_value(Value::BulkString("1".into()));
            assert_eq!(v, Ok(vec!["1".to_string()]));
        }
    }

    #[test]
    fn test_tuple() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v = parse_mode.parse_redis_value(Value::Array(vec![Value::Array(vec![
                Value::BulkString("1".into()),
                Value::BulkString("2".into()),
                Value::BulkString("3".into()),
            ])]));

            assert_eq!(v, Ok(((1i32, 2, 3,),)));
        }
    }

    #[test]
    fn test_hashmap() {
        use fnv::FnvHasher;
        use std::collections::HashMap;
        use std::hash::BuildHasherDefault;

        type Hm = HashMap<String, i32>;

        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v: Result<Hm, _> = parse_mode.parse_redis_value(Value::Array(vec![
                Value::BulkString("a".into()),
                Value::BulkString("1".into()),
                Value::BulkString("b".into()),
                Value::BulkString("2".into()),
                Value::BulkString("c".into()),
                Value::BulkString("3".into()),
            ]));
            let mut e: Hm = HashMap::new();
            e.insert("a".into(), 1);
            e.insert("b".into(), 2);
            e.insert("c".into(), 3);
            assert_eq!(v, Ok(e));

            type Hasher = BuildHasherDefault<FnvHasher>;
            type HmHasher = HashMap<String, i32, Hasher>;
            let v: Result<HmHasher, _> = parse_mode.parse_redis_value(Value::Array(vec![
                Value::BulkString("a".into()),
                Value::BulkString("1".into()),
                Value::BulkString("b".into()),
                Value::BulkString("2".into()),
                Value::BulkString("c".into()),
                Value::BulkString("3".into()),
            ]));

            let fnv = Hasher::default();
            let mut e: HmHasher = HashMap::with_hasher(fnv);
            e.insert("a".into(), 1);
            e.insert("b".into(), 2);
            e.insert("c".into(), 3);
            assert_eq!(v, Ok(e));

            let v: Result<Hm, _> =
                parse_mode.parse_redis_value(Value::Array(vec![Value::BulkString("a".into())]));
            assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
        }
    }

    #[cfg(feature = "hashbrown")]
    #[test]
    fn test_hashbrown_hashmap() {
        use fnv::FnvHasher;
        use hashbrown::HashMap;
        use std::hash::BuildHasherDefault;

        type Hm = HashMap<String, i32>;

        let test_arr = vec![
            Value::BulkString("a".into()),
            Value::BulkString("1".into()),
            Value::BulkString("b".into()),
            Value::BulkString("2".into()),
            Value::BulkString("c".into()),
            Value::BulkString("3".into()),
        ];
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v: Result<Hm, _> = parse_mode.parse_redis_value(Value::Array(test_arr.clone()));
            let mut e: Hm = HashMap::new();
            e.insert("a".into(), 1);
            e.insert("b".into(), 2);
            e.insert("c".into(), 3);
            assert_eq!(v, Ok(e));

            type Hasher = BuildHasherDefault<FnvHasher>;
            type HmHasher = HashMap<String, i32, Hasher>;
            let v: Result<HmHasher, _> =
                parse_mode.parse_redis_value(Value::Array(test_arr.clone()));

            let fnv = Hasher::default();
            let mut e: HmHasher = HashMap::with_hasher(fnv);
            e.insert("a".into(), 1);
            e.insert("b".into(), 2);
            e.insert("c".into(), 3);
            assert_eq!(v, Ok(e));

            let v: Result<Hm, _> =
                parse_mode.parse_redis_value(Value::Array(vec![Value::BulkString("a".into())]));
            assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);
        }
    }

    #[cfg(feature = "hashbrown")]
    #[test]
    fn test_hashbrown_hashset() {
        use hashbrown::HashSet;

        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v: Result<HashSet<String>, _> = parse_mode.parse_redis_value(Value::Array(vec![
                Value::BulkString("a".into()),
                Value::BulkString("b".into()),
                Value::BulkString("c".into()),
            ]));

            let mut expected = HashSet::new();
            expected.insert("a".to_string());
            expected.insert("b".to_string());
            expected.insert("c".to_string());

            assert_eq!(v, Ok(expected));

            let v: Result<HashSet<i32>, _> = parse_mode.parse_redis_value(Value::Int(42));
            assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

            let mut set = HashSet::new();
            set.insert("x".to_string());
            set.insert("y".to_string());
            set.insert("z".to_string());

            let args = set.to_redis_args();
            assert_eq!(args.len(), 3);

            let args_set: HashSet<Vec<u8>> = args.into_iter().collect();
            assert!(args_set.contains(&b"x"[..].to_vec()));
            assert!(args_set.contains(&b"y"[..].to_vec()));
            assert!(args_set.contains(&b"z"[..].to_vec()));
        }
    }

    #[test]
    fn test_bool() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let v = parse_mode.parse_redis_value(Value::BulkString("1".into()));
            assert_eq!(v, Ok(true));

            let v = parse_mode.parse_redis_value(Value::BulkString("0".into()));
            assert_eq!(v, Ok(false));

            let v: Result<bool, _> =
                parse_mode.parse_redis_value(Value::BulkString("garbage".into()));
            assert_eq!(v.unwrap_err().kind(), ErrorKind::TypeError);

            let v = parse_mode.parse_redis_value(Value::SimpleString("1".into()));
            assert_eq!(v, Ok(true));

            let v = parse_mode.parse_redis_value(Value::SimpleString("0".into()));
            assert_eq!(v, Ok(false));

            let v: Result<bool, _> =
                parse_mode.parse_redis_value(Value::SimpleString("garbage".into()));
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

        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let content: &[u8] = b"\x01\x02\x03\x04";
            let content_vec: Vec<u8> = Vec::from(content);
            let content_bytes = Bytes::from_static(content);

            let v: RedisResult<Bytes> =
                parse_mode.parse_redis_value(Value::BulkString(content_vec));
            assert_eq!(v, Ok(content_bytes));

            let v: RedisResult<Bytes> =
                parse_mode.parse_redis_value(Value::SimpleString("garbage".into()));
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

        use uuid::Uuid;

        let uuid = Uuid::from_str("abab64b7-e265-4052-a41b-23e1e28674bf").unwrap();
        let bytes = uuid.as_bytes().to_vec();

        let v: RedisResult<Uuid> = FromRedisValue::from_redis_value(&Value::BulkString(bytes));
        assert_eq!(v, Ok(uuid));

        let v: RedisResult<Uuid> =
            FromRedisValue::from_redis_value(&Value::SimpleString("garbage".into()));
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
        use std::ffi::CString;

        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let content: &[u8] = b"\x01\x02\x03\x04";
            let content_vec: Vec<u8> = Vec::from(content);

            let v: RedisResult<CString> =
                parse_mode.parse_redis_value(Value::BulkString(content_vec));
            assert_eq!(v, Ok(CString::new(content).unwrap()));

            let v: RedisResult<CString> =
                parse_mode.parse_redis_value(Value::SimpleString("garbage".into()));
            assert_eq!(v, Ok(CString::new("garbage").unwrap()));

            let v: RedisResult<CString> = parse_mode.parse_redis_value(Value::Okay);
            assert_eq!(v, Ok(CString::new("OK").unwrap()));

            let v: RedisResult<CString> =
                parse_mode.parse_redis_value(Value::SimpleString("gar\0bage".into()));
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
    fn test_std_types_to_redis_args() {
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
    #[allow(unused_allocation)]
    fn test_deref_types_to_redis_args() {
        use std::collections::BTreeMap;

        let number = 456i64;
        let expected_result = number.to_redis_args();
        assert_eq!(Arc::new(number).to_redis_args(), expected_result);
        assert_eq!(Arc::new(&number).to_redis_args(), expected_result);
        assert_eq!(Box::new(number).to_redis_args(), expected_result);
        assert_eq!(Rc::new(&number).to_redis_args(), expected_result);

        let array = vec![1, 2, 3];
        let expected_array = array.to_redis_args();
        assert_eq!(Arc::new(array.clone()).to_redis_args(), expected_array);
        assert_eq!(Arc::new(&array).to_redis_args(), expected_array);
        assert_eq!(Box::new(array.clone()).to_redis_args(), expected_array);
        assert_eq!(Rc::new(array.clone()).to_redis_args(), expected_array);

        let map = [("k1", "v1"), ("k2", "v2")]
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let expected_map = map.to_redis_args();
        assert_eq!(Arc::new(map.clone()).to_redis_args(), expected_map);
        assert_eq!(Box::new(map.clone()).to_redis_args(), expected_map);
        assert_eq!(Rc::new(map).to_redis_args(), expected_map);
    }

    #[test]
    fn test_cow_types_to_redis_args() {
        use std::borrow::Cow;

        let s = "key".to_string();
        let expected_string = s.to_redis_args();
        assert_eq!(Cow::Borrowed(s.as_str()).to_redis_args(), expected_string);
        assert_eq!(Cow::<str>::Owned(s).to_redis_args(), expected_string);

        let array = vec![0u8, 4, 2, 3, 1];
        let expected_array = array.to_redis_args();

        assert_eq!(
            Cow::Borrowed(array.as_slice()).to_redis_args(),
            expected_array
        );
        assert_eq!(Cow::<[u8]>::Owned(array).to_redis_args(), expected_array);
    }

    #[test]
    fn test_large_usize_array_to_redis_args_and_back() {
        use crate::support::encode_value;

        let mut array = [0; 1000];
        for (i, item) in array.iter_mut().enumerate() {
            *item = i;
        }

        let vec = (&array).to_redis_args();
        assert_eq!(array.len(), vec.len());

        let value = Value::Array(
            vec.iter()
                .map(|val| Value::BulkString(val.clone()))
                .collect(),
        );
        let mut encoded_input = Vec::new();
        encode_value(&value, &mut encoded_input).unwrap();

        let new_array: [usize; 1000] = FromRedisValue::from_redis_value(&value).unwrap();
        assert_eq!(new_array, array);
    }

    #[test]
    fn test_large_u8_array_to_redis_args_and_back() {
        use crate::support::encode_value;

        let mut array: [u8; 1000] = [0; 1000];
        for (i, item) in array.iter_mut().enumerate() {
            *item = (i % 256) as u8;
        }

        let vec = (&array).to_redis_args();
        assert_eq!(vec.len(), 1);
        assert_eq!(array.len(), vec[0].len());

        let value = Value::Array(vec[0].iter().map(|val| Value::Int(*val as i64)).collect());
        let mut encoded_input = Vec::new();
        encode_value(&value, &mut encoded_input).unwrap();

        let new_array: [u8; 1000] = FromRedisValue::from_redis_value(&value).unwrap();
        assert_eq!(new_array, array);
    }

    #[test]
    fn test_large_string_array_to_redis_args_and_back() {
        use crate::support::encode_value;

        let mut array: [String; 1000] = [(); 1000].map(|_| String::new());
        for (i, item) in array.iter_mut().enumerate() {
            *item = format!("{i}");
        }

        let vec = (&array).to_redis_args();
        assert_eq!(array.len(), vec.len());

        let value = Value::Array(
            vec.iter()
                .map(|val| Value::BulkString(val.clone()))
                .collect(),
        );
        let mut encoded_input = Vec::new();
        encode_value(&value, &mut encoded_input).unwrap();

        let new_array: [String; 1000] = FromRedisValue::from_redis_value(&value).unwrap();
        assert_eq!(new_array, array);
    }

    #[test]
    fn test_0_length_usize_array_to_redis_args_and_back() {
        use crate::support::encode_value;

        let array: [usize; 0] = [0; 0];

        let vec = (&array).to_redis_args();
        assert_eq!(array.len(), vec.len());

        let value = Value::Array(
            vec.iter()
                .map(|val| Value::BulkString(val.clone()))
                .collect(),
        );
        let mut encoded_input = Vec::new();
        encode_value(&value, &mut encoded_input).unwrap();

        let new_array: [usize; 0] = FromRedisValue::from_redis_value(&value).unwrap();
        assert_eq!(new_array, array);

        let new_array: [usize; 0] = FromRedisValue::from_redis_value(&Value::Nil).unwrap();
        assert_eq!(new_array, array);
    }

    #[test]
    fn test_attributes() {
        use redis::parse_redis_value;
        let bytes: &[u8] = b"*3\r\n:1\r\n:2\r\n|1\r\n+ttl\r\n:3600\r\n:3\r\n";
        let val = parse_redis_value(bytes).unwrap();
        {
            // The case user doesn't expect attributes from server
            let x: Vec<i32> = redis::FromRedisValue::from_redis_value(&val).unwrap();
            assert_eq!(x, vec![1, 2, 3]);
        }
        {
            // The case user wants raw value from server
            let x: Value = FromRedisValue::from_redis_value(&val).unwrap();
            assert_eq!(
                x,
                Value::Array(vec![
                    Value::Int(1),
                    Value::Int(2),
                    Value::Attribute {
                        data: Box::new(Value::Int(3)),
                        attributes: vec![(
                            Value::SimpleString("ttl".to_string()),
                            Value::Int(3600)
                        )]
                    }
                ])
            )
        }
    }

    #[test]
    fn test_complex_nested_tuples() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let value = Value::Array(vec![
                Value::Array(vec![
                    Value::BulkString(b"Hi1".to_vec()),
                    Value::BulkString(b"Bye1".to_vec()),
                    Value::BulkString(b"Hi2".to_vec()),
                    Value::BulkString(b"Bye2".to_vec()),
                ]),
                Value::Array(vec![
                    Value::BulkString(b"S1".to_vec()),
                    Value::BulkString(b"S2".to_vec()),
                    Value::BulkString(b"S3".to_vec()),
                ]),
                Value::Array(vec![
                    Value::BulkString(b"Hi3".to_vec()),
                    Value::BulkString(b"Bye3".to_vec()),
                    Value::BulkString(b"Hi4".to_vec()),
                    Value::BulkString(b"Bye4".to_vec()),
                ]),
                Value::Array(vec![
                    Value::BulkString(b"S4".to_vec()),
                    Value::BulkString(b"S5".to_vec()),
                    Value::BulkString(b"S6".to_vec()),
                ]),
            ]);
            let res: Vec<(HashMap<String, String>, Vec<String>)> =
                parse_mode.parse_redis_value(value).unwrap();

            let mut expected_map1 = HashMap::new();
            expected_map1.insert("Hi1".to_string(), "Bye1".to_string());
            expected_map1.insert("Hi2".to_string(), "Bye2".to_string());

            let mut expected_map2 = HashMap::new();
            expected_map2.insert("Hi3".to_string(), "Bye3".to_string());
            expected_map2.insert("Hi4".to_string(), "Bye4".to_string());

            assert_eq!(
                res,
                vec![
                    (
                        expected_map1,
                        vec!["S1".to_string(), "S2".to_string(), "S3".to_string()]
                    ),
                    (
                        expected_map2,
                        vec!["S4".to_string(), "S5".to_string(), "S6".to_string()]
                    )
                ]
            );
        }
    }

    #[test]
    fn test_complex_nested_tuples_resp3() {
        for parse_mode in [RedisParseMode::Owned, RedisParseMode::Ref] {
            let value = Value::Array(vec![
                Value::Map(vec![
                    (
                        Value::BulkString(b"Hi1".to_vec()),
                        Value::BulkString(b"Bye1".to_vec()),
                    ),
                    (
                        Value::BulkString(b"Hi2".to_vec()),
                        Value::BulkString(b"Bye2".to_vec()),
                    ),
                ]),
                Value::Set(vec![
                    Value::BulkString(b"S1".to_vec()),
                    Value::BulkString(b"S2".to_vec()),
                    Value::BulkString(b"S3".to_vec()),
                ]),
                Value::Map(vec![
                    (
                        Value::BulkString(b"Hi3".to_vec()),
                        Value::BulkString(b"Bye3".to_vec()),
                    ),
                    (
                        Value::BulkString(b"Hi4".to_vec()),
                        Value::BulkString(b"Bye4".to_vec()),
                    ),
                ]),
                Value::Set(vec![
                    Value::BulkString(b"S4".to_vec()),
                    Value::BulkString(b"S5".to_vec()),
                    Value::BulkString(b"S6".to_vec()),
                ]),
            ]);
            let res: Vec<(HashMap<String, String>, HashSet<String>)> =
                parse_mode.parse_redis_value(value).unwrap();

            let mut expected_map1 = HashMap::new();
            expected_map1.insert("Hi1".to_string(), "Bye1".to_string());
            expected_map1.insert("Hi2".to_string(), "Bye2".to_string());
            let mut expected_set1 = HashSet::new();
            expected_set1.insert("S1".to_string());
            expected_set1.insert("S2".to_string());
            expected_set1.insert("S3".to_string());

            let mut expected_map2 = HashMap::new();
            expected_map2.insert("Hi3".to_string(), "Bye3".to_string());
            expected_map2.insert("Hi4".to_string(), "Bye4".to_string());
            let mut expected_set2 = HashSet::new();
            expected_set2.insert("S4".to_string());
            expected_set2.insert("S5".to_string());
            expected_set2.insert("S6".to_string());

            assert_eq!(
                res,
                vec![
                    (expected_map1, expected_set1),
                    (expected_map2, expected_set2)
                ]
            );
        }
    }
}
