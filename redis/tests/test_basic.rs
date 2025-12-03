#![allow(clippy::let_unit_value)]

#[macro_use]
mod support;

#[cfg(test)]
mod basic {
    use crate::{assert_args, support::*};
    use assert_approx_eq::assert_approx_eq;
    use rand::distr::Alphanumeric;
    use rand::prelude::IndexedRandom;
    use rand::{rng, Rng};

    use redis::{calculate_value_digest, is_valid_16_bytes_hex_digest};
    use redis::{
        cmd, Client, Connection, ConnectionInfo, ConnectionLike, ControlFlow, CopyOptions,
        ErrorKind, ExistenceCheck, ExpireOption, Expiry, FieldExistenceCheck,
        HashFieldExpirationOptions,
        IntegerReplyOrNoOp::{ExistsButNotRelevant, IntegerReply},
        MSetOptions, ProtocolVersion, PubSubCommands, PushInfo, PushKind, RedisConnectionInfo,
        RedisResult, Role, ScanOptions, SetExpiry, SetOptions, SortedSetAddOptions, ToRedisArgs,
        TypedCommands, UpdateCheck, Value, ValueComparison, ValueType,
    };
    use redis::{RedisError, ServerErrorKind};

    #[cfg(feature = "vector-sets")]
    use redis::vector_sets::{
        EmbeddingInput, VAddOptions, VEmbOptions, VSimOptions, VectorAddInput, VectorQuantization,
        VectorSimilaritySearchInput,
    };
    use redis_test::server::redis_settings;
    use redis_test::utils::get_listener_on_free_port;

    #[cfg(feature = "vector-sets")]
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};
    use std::collections::{HashMap, HashSet};
    use std::io::Read;
    use std::thread::{self, sleep, spawn};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use std::vec;

    const HASH_KEY: &str = "testing_hash";
    const HASH_FIELDS_AND_VALUES: [(&str, u8); 5] =
        [("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8), ("f5", 16)];

    /// Generates a unique hash key that does not already exist.
    fn generate_random_testing_hash_key(con: &mut Connection) -> String {
        const TEST_HASH_KEY_BASE: &str = "testing_hash";
        const TEST_HASH_KEY_RANDOM_LENGTH: usize = 7;

        loop {
            let generated_hash_key = format!(
                "{}_{}",
                TEST_HASH_KEY_BASE,
                rng()
                    .sample_iter(&Alphanumeric)
                    .take(TEST_HASH_KEY_RANDOM_LENGTH)
                    .map(char::from)
                    .collect::<String>()
            );

            let hash_exists: bool = con.exists(&generated_hash_key).unwrap();

            if !hash_exists {
                println!("Generated random testing hash key: {}", &generated_hash_key);
                return generated_hash_key;
            }
        }
    }

    #[test]
    fn test_parse_redis_url() {
        let redis_url = "redis://127.0.0.1:1234/0".to_string();
        redis::parse_redis_url(&redis_url).unwrap();
        redis::parse_redis_url("unix:/var/run/redis/redis.sock").unwrap();
        assert!(redis::parse_redis_url("127.0.0.1").is_none());
    }

    #[test]
    fn test_redis_url_fromstr() {
        let _info: ConnectionInfo = "redis://127.0.0.1:1234/0".parse().unwrap();
    }

    #[test]
    fn test_args() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        redis::cmd("SET")
            .arg("key1")
            .arg(b"foo")
            .exec(&mut con)
            .unwrap();
        redis::cmd("SET")
            .arg(&["key2", "bar"])
            .exec(&mut con)
            .unwrap();

        assert_eq!(
            redis::cmd("MGET").arg(&["key1", "key2"]).query(&mut con),
            Ok(("foo".to_string(), b"bar".to_vec()))
        );
    }

    #[test]
    fn test_can_authenticate_with_username_and_password() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let username = "foo";
        let password = "bar";

        // adds a "foo" user with "GET permissions"
        let mut set_user_cmd = redis::Cmd::new();
        set_user_cmd
            .arg("ACL")
            .arg("SETUSER")
            .arg(username)
            .arg("on")
            .arg("+acl")
            .arg(format!(">{password}"));
        assert_eq!(con.req_command(&set_user_cmd), Ok(Value::Okay));

        let redis = redis_settings()
            .set_password(password)
            .set_username(username);
        let mut conn = redis::Client::open(ctx.server.connection_info().set_redis_settings(redis))
            .unwrap()
            .get_connection()
            .unwrap();

        let result: String = cmd("ACL").arg("whoami").query(&mut conn).unwrap();
        assert_eq!(result, username)
    }

    #[test]
    fn test_getset() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        redis::cmd("SET").arg("foo").arg(42).exec(&mut con).unwrap();
        assert_eq!(redis::cmd("GET").arg("foo").query(&mut con), Ok(42));

        redis::cmd("SET")
            .arg("bar")
            .arg("foo")
            .exec(&mut con)
            .unwrap();
        assert_eq!(
            redis::cmd("GET").arg("bar").query(&mut con),
            Ok(b"foo".to_vec())
        );
    }

    //unit test for key_type function
    #[test]
    fn test_key_type() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        //The key is a simple value
        redis::cmd("SET").arg("foo").arg(42).exec(&mut con).unwrap();
        let string_key_type = con.key_type("foo").unwrap();
        assert_eq!(string_key_type, ValueType::String);

        //The key is a list
        redis::cmd("LPUSH")
            .arg("list_bar")
            .arg("foo")
            .exec(&mut con)
            .unwrap();
        let list_key_type = con.key_type("list_bar").unwrap();
        assert_eq!(list_key_type, ValueType::List);

        //The key is a set
        redis::cmd("SADD")
            .arg("set_bar")
            .arg("foo")
            .exec(&mut con)
            .unwrap();
        let set_key_type = con.key_type("set_bar").unwrap();
        assert_eq!(set_key_type, ValueType::Set);

        //The key is a sorted set
        redis::cmd("ZADD")
            .arg("sorted_set_bar")
            .arg("1")
            .arg("foo")
            .exec(&mut con)
            .unwrap();
        let zset_key_type = con.key_type("sorted_set_bar").unwrap();
        assert_eq!(zset_key_type, ValueType::ZSet);

        //The key is a hash
        redis::cmd("HSET")
            .arg("hset_bar")
            .arg("hset_key_1")
            .arg("foo")
            .exec(&mut con)
            .unwrap();
        let hash_key_type = con.key_type("hset_bar").unwrap();
        assert_eq!(hash_key_type, ValueType::Hash);
    }

    #[test]
    fn test_client_tracking_doesnt_block_execution() {
        //It checks if the library distinguish a push-type message from the others and continues its normal operation.
        let ctx = TestContext::new();
        let mut con = ctx.connection();
        let (k1, k2): (i32, i32) = redis::pipe()
            .cmd("CLIENT")
            .arg("TRACKING")
            .arg("ON")
            .ignore()
            .cmd("GET")
            .arg("key_1")
            .ignore()
            .cmd("SET")
            .arg("key_1")
            .arg(42)
            .ignore()
            .cmd("SET")
            .arg("key_2")
            .arg(43)
            .ignore()
            .cmd("GET")
            .arg("key_1")
            .cmd("GET")
            .arg("key_2")
            .cmd("SET")
            .arg("key_1")
            .arg(45)
            .ignore()
            .query(&mut con)
            .unwrap();
        assert_eq!(k1, 42);
        assert_eq!(k2, 43);
        let num: i32 = con.get("key_1").unwrap().unwrap().parse().unwrap();
        assert_eq!(num, 45);
    }

    #[test]
    fn test_incr() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        redis::cmd("SET").arg("foo").arg(42).exec(&mut con).unwrap();
        assert_eq!(redis::cmd("INCR").arg("foo").query(&mut con), Ok(43usize));
    }

    #[test]
    fn test_ping() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let res: String = con.ping_message("foobar").unwrap();
        assert_eq!(&res, "foobar");
        let res: String = con.ping().unwrap();
        assert_eq!(&res, "PONG");
    }

    #[test]
    fn test_getdel() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        redis::cmd("SET").arg("foo").arg(42).exec(&mut con).unwrap();

        assert_eq!(con.get_del("foo"), Ok(Some(String::from("42"))));

        assert_eq!(
            redis::cmd("GET").arg("foo").query(&mut con),
            Ok(None::<usize>)
        );
    }

    #[test]
    fn test_getex() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        redis::cmd("SET")
            .arg("foo")
            .arg(42usize)
            .exec(&mut con)
            .unwrap();

        // Return of get_ex must match set value
        let ret_value =
            redis::Commands::get_ex::<_, usize>(&mut con, "foo", Expiry::EX(1)).unwrap();
        assert_eq!(ret_value, 42usize);

        // Get before expiry time must also return value
        sleep(Duration::from_millis(100));
        let delayed_get = con.get_int("foo").unwrap().unwrap();
        assert_eq!(delayed_get, 42);

        // Get after expiry time mustn't return value
        sleep(Duration::from_secs(1));
        let after_expire_get = con.get("foo").unwrap();
        assert_eq!(after_expire_get, None);

        // Persist option test prep
        redis::cmd("SET")
            .arg("foo")
            .arg(420usize)
            .exec(&mut con)
            .unwrap();

        // Return of get_ex with persist option must match set value
        let ret_value = con.get_ex("foo", Expiry::PERSIST).unwrap().unwrap();
        assert_eq!(ret_value, String::from("420"));

        // Get after persist get_ex must return value
        sleep(Duration::from_millis(200));
        let delayed_get = con.get_int("foo").unwrap();
        assert_eq!(delayed_get, Some(420));
    }

    #[test]
    fn test_info() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let info: redis::InfoDict = redis::cmd("INFO").query(&mut con).unwrap();
        assert_eq!(
            info.find(&"role"),
            Some(&redis::Value::SimpleString("master".to_string()))
        );
        assert_eq!(info.get("role"), Some("master".to_string()));
        assert_eq!(info.get("loading"), Some(false));
        assert!(!info.is_empty());
        assert!(info.contains_key(&"role"));
    }

    #[test]
    fn test_hash_ops() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        redis::cmd("HSET")
            .arg("foo")
            .arg("key_1")
            .arg(1)
            .exec(&mut con)
            .unwrap();
        redis::cmd("HSET")
            .arg("foo")
            .arg("key_2")
            .arg(2)
            .exec(&mut con)
            .unwrap();

        let h: HashMap<String, i32> = redis::cmd("HGETALL").arg("foo").query(&mut con).unwrap();
        assert_eq!(h.len(), 2);
        assert_eq!(h.get("key_1"), Some(&1i32));
        assert_eq!(h.get("key_2"), Some(&2i32));

        let h: BTreeMap<String, i32> = redis::cmd("HGETALL").arg("foo").query(&mut con).unwrap();
        assert_eq!(h.len(), 2);
        assert_eq!(h.get("key_1"), Some(&1i32));
        assert_eq!(h.get("key_2"), Some(&2i32));
    }

    #[test]
    fn test_hash_expiration() {
        let ctx = TestContext::new();
        // Hash expiration is only supported in Redis 7.4.0 and later.
        run_test_if_version_supported!(&(7, 4, 0));

        let mut con = ctx.connection();
        redis::cmd("HMSET")
            .arg("foo")
            .arg("f0")
            .arg("v0")
            .arg("f1")
            .arg("v1")
            .exec(&mut con)
            .unwrap();

        let result = con
            .hexpire("foo", 10, ExpireOption::NONE, &["f0", "f1"])
            .unwrap();
        assert_eq!(result, vec![1, 1]);

        let ttls = con.httl("foo", &["f0", "f1"]).unwrap();
        assert_eq!(ttls.len(), 2);
        assert_approx_eq!(ttls[0].raw(), 10, 3);
        assert_approx_eq!(ttls[1].raw(), 10, 3);

        let ttls = con.hpttl("foo", &["f0", "f1"]).unwrap();
        assert_eq!(ttls.len(), 2);
        assert_approx_eq!(ttls[0].raw(), 10000, 3000);
        assert_approx_eq!(ttls[1].raw(), 10000, 3000);

        let result = con
            .hexpire("foo", 10, ExpireOption::NX, &["f0", "f1"])
            .unwrap();
        // should return 0 because the keys already have an expiration time
        assert_eq!(result, vec![0, 0]);

        let result = con
            .hexpire("foo", 10, ExpireOption::XX, &["f0", "f1"])
            .unwrap();
        // should return 1 because the keys already have an expiration time
        assert_eq!(result, vec![1, 1]);

        let result = con
            .hpexpire("foo", 1000, ExpireOption::GT, &["f0", "f1"])
            .unwrap();
        // should return 0 because the keys already have an expiration time greater than 1000
        assert_eq!(result, vec![0, 0]);

        let result = con
            .hpexpire("foo", 1000, ExpireOption::LT, &["f0", "f1"])
            .unwrap();
        // should return 1 because the keys already have an expiration time less than 1000
        assert_eq!(result, vec![1, 1]);

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;
        let result = con
            .hexpire_at(
                "foo",
                (now_secs + 10) as i64,
                ExpireOption::GT,
                &["f0", "f1"],
            )
            .unwrap();
        assert_eq!(result, vec![1, 1]);

        let result = con.hexpire_time("foo", &["f0", "f1"]).unwrap();
        assert_eq!(result, vec![now_secs + 10, now_secs + 10]);
        let result = con.hpexpire_time("foo", &["f0", "f1"]).unwrap();
        assert_eq!(
            result,
            vec![now_secs * 1000 + 10_000, now_secs * 1000 + 10_000]
        );

        let result = con.hpersist("foo", &["f0", "f1"]).unwrap();
        assert_eq!(result, vec![1, 1]);
        let ttls = con.hpttl("foo", &["f0", "f1"]).unwrap();
        assert_eq!(ttls, vec![ExistsButNotRelevant, ExistsButNotRelevant]);

        assert_eq!(con.unlink(&["foo"]), Ok(1));
    }

    /// Verify that the hash contains exactly the specified fields with their corresponding values.
    fn verify_exact_hash_fields_and_values(
        con: &mut Connection,
        hash_key: &str,
        hash_fields_and_values: &[(&str, u8)],
    ) {
        let hash_fields: HashMap<String, u8> = redis::Commands::hgetall(con, hash_key).unwrap();
        assert_eq!(hash_fields.len(), hash_fields_and_values.len());

        for (field, value) in hash_fields_and_values {
            assert_eq!(hash_fields.get(*field), Some(value));
        }
    }

    #[inline(always)]
    fn verify_fields_absence_from_hash(
        hash_fields: &HashMap<String, String>,
        hash_fields_to_check: &[&str],
    ) {
        hash_fields_to_check.iter().for_each(|key| {
            assert!(!hash_fields.contains_key(*key));
        });
    }

    /// The test validates the following scenarios for the HGETDEL command:
    ///
    /// 1. It successfully deletes a single field from a given existing hash.
    /// 2. Attempting to delete a non-existing field from a given existing hash results in a NIL response.
    /// 3. It successfully deletes multiple fields from a given existing hash.
    /// 4. When used on a hash with only one field, it deletes the entire hash.
    /// 5. Attempting to delete a field from a non-existing hash results in a NIL response.
    #[test]
    fn test_hget_del() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
        let mut con = ctx.connection();
        // Create a hash with multiple fields and values that will be used for testing
        assert_eq!(con.hset_multiple(HASH_KEY, &HASH_FIELDS_AND_VALUES), Ok(()));

        // Delete the first field
        assert_eq!(
            redis::Commands::hget_del(&mut con, HASH_KEY, HASH_FIELDS_AND_VALUES[0].0),
            Ok([HASH_FIELDS_AND_VALUES[0].1])
        );

        let mut removed_fields = Vec::from([HASH_FIELDS_AND_VALUES[0].0]);

        // Verify that the field has been deleted
        let remaining_hash_fields: HashMap<String, String> = con.hgetall(HASH_KEY).unwrap();
        assert_eq!(
            remaining_hash_fields.len(),
            HASH_FIELDS_AND_VALUES.len() - removed_fields.len()
        );
        verify_fields_absence_from_hash(&remaining_hash_fields, &removed_fields);

        // Verify that a non-existing field returns NIL by attempting to delete the same field again
        assert_eq!(con.hget_del(HASH_KEY, &removed_fields), Ok(vec![None]));

        // Prepare additional fields for deletion
        let fields_to_delete = [
            HASH_FIELDS_AND_VALUES[1].0,
            HASH_FIELDS_AND_VALUES[2].0,
            HASH_FIELDS_AND_VALUES[3].0,
        ];

        // Delete the additional fields
        assert_eq!(
            redis::Commands::hget_del(&mut con, HASH_KEY, &fields_to_delete),
            Ok([
                HASH_FIELDS_AND_VALUES[1].1,
                HASH_FIELDS_AND_VALUES[2].1,
                HASH_FIELDS_AND_VALUES[3].1
            ])
        );

        removed_fields.extend_from_slice(&fields_to_delete);

        // Verify that all of the fields have been deleted
        let remaining_hash_fields: HashMap<String, String> = con.hgetall(HASH_KEY).unwrap();
        assert_eq!(
            remaining_hash_fields.len(),
            HASH_FIELDS_AND_VALUES.len() - removed_fields.len()
        );
        verify_fields_absence_from_hash(&remaining_hash_fields, &removed_fields);

        // Verify that removing the last field deletes the hash
        assert_eq!(
            redis::Commands::hget_del(&mut con, HASH_KEY, HASH_FIELDS_AND_VALUES[4].0),
            Ok([HASH_FIELDS_AND_VALUES[4].1])
        );
        assert_eq!(con.exists(HASH_KEY), Ok(false));

        // Verify that HGETDEL on a non-existing hash returns NIL
        assert_eq!(
            con.hget_del(HASH_KEY, HASH_FIELDS_AND_VALUES[4].0),
            Ok(vec![None])
        );
    }

    /// The test validates the following scenarios for the HGETEX command:
    ///
    /// 1. It successfully retrieves a single field from a given existing hash without setting its expiration.
    /// 2. It successfully retrieves multiple fields from a given existing hash without setting their expiration.
    /// 3. It successfully retrieves a single field from a given existing hash and sets its expiration to 1 second.
    ///    It verifies that the field has been set to expire and that it is no longer present in the hash after it expires.
    /// 4. Attempting to retrieve a non-existing field from a given existing hash returns in a NIL response.
    /// 5. It successfully retrieves multiple fields from a given existing hash and sets their expiration to 1 second.
    ///    It verifies that the fields have been set to expire and that they are no longer present in the hash after they expire.
    /// 6. Attempting to retrieve a field from a non-existing hash returns in a NIL response.
    #[test]
    fn test_hget_ex() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
        let mut con = ctx.connection();
        // Create a hash with multiple fields and values that will be used for testing
        assert_eq!(con.hset_multiple(HASH_KEY, &HASH_FIELDS_AND_VALUES), Ok(()));

        // Scenario 1
        // Retrieve a single field without setting its expiration
        assert_eq!(
            redis::Commands::hget_ex(
                &mut con,
                HASH_KEY,
                HASH_FIELDS_AND_VALUES[0].0,
                Expiry::PERSIST
            ),
            Ok([HASH_FIELDS_AND_VALUES[0].1])
        );
        assert_eq!(
            con.httl(HASH_KEY, HASH_FIELDS_AND_VALUES[0].0),
            Ok(vec![ExistsButNotRelevant])
        );

        // Scenario 2
        // Retrieve multiple fields at once without setting their expiration
        let fields_to_retrieve = [HASH_FIELDS_AND_VALUES[1].0, HASH_FIELDS_AND_VALUES[2].0];
        assert_eq!(
            redis::Commands::hget_ex(&mut con, HASH_KEY, &fields_to_retrieve, Expiry::PERSIST),
            Ok([HASH_FIELDS_AND_VALUES[1].1, HASH_FIELDS_AND_VALUES[2].1])
        );
        assert_eq!(
            con.httl(HASH_KEY, &fields_to_retrieve),
            Ok(vec![ExistsButNotRelevant, ExistsButNotRelevant])
        );

        // Scenario 3
        // Retrieve a single field and set its expiration to 1 second
        assert_eq!(
            redis::Commands::hget_ex(
                &mut con,
                HASH_KEY,
                HASH_FIELDS_AND_VALUES[0].0,
                Expiry::EX(1)
            ),
            Ok([HASH_FIELDS_AND_VALUES[0].1])
        );
        // Verify that the all fields are still present in the hash
        verify_exact_hash_fields_and_values(&mut con, HASH_KEY, &HASH_FIELDS_AND_VALUES);
        // Verify that the field has been set to expire
        assert_eq!(
            con.httl(HASH_KEY, HASH_FIELDS_AND_VALUES[0].0),
            Ok(vec![IntegerReply(1)])
        );
        // Wait for the field to expire and verify it
        sleep(Duration::from_millis(1100));

        let mut expired_fields = Vec::from([HASH_FIELDS_AND_VALUES[0].0]);

        let remaining_hash_fields: HashMap<String, String> = con.hgetall(HASH_KEY).unwrap();
        assert_eq!(
            remaining_hash_fields.len(),
            HASH_FIELDS_AND_VALUES.len() - expired_fields.len()
        );
        verify_fields_absence_from_hash(&remaining_hash_fields, &expired_fields);

        // Scenario 4
        // Verify that a non-existing field returns NIL by attempting to retrieve it with HGETEX
        assert_eq!(
            redis::Commands::hget_ex(&mut con, HASH_KEY, &expired_fields, Expiry::PERSIST),
            Ok([Value::Nil])
        );

        // Scenario 5
        // Retrieve multiple fields and set their expiration to 1 second
        let fields_to_expire = [
            HASH_FIELDS_AND_VALUES[1].0,
            HASH_FIELDS_AND_VALUES[2].0,
            HASH_FIELDS_AND_VALUES[3].0,
            HASH_FIELDS_AND_VALUES[4].0,
        ];
        let hash_field_values: Vec<u8> =
            redis::Commands::hget_ex(&mut con, HASH_KEY, &fields_to_expire, Expiry::EX(1)).unwrap();
        assert_eq!(hash_field_values.len(), fields_to_expire.len());

        for i in 0..fields_to_expire.len() {
            assert_eq!(hash_field_values[i], HASH_FIELDS_AND_VALUES[i + 1].1);
        }
        // Verify that all fields, except the first one, which has already expired, are still present in the hash
        verify_exact_hash_fields_and_values(&mut con, HASH_KEY, &HASH_FIELDS_AND_VALUES[1..]);
        // Verify that the fields have been set to expire
        assert_eq!(
            con.httl(HASH_KEY, &fields_to_expire),
            Ok(vec![IntegerReply(1); fields_to_expire.len()])
        );
        // Wait for the fields to expire and verify it
        sleep(Duration::from_millis(1100));

        expired_fields.extend_from_slice(&fields_to_expire);

        let remaining_hash_fields: HashMap<String, String> = con.hgetall(HASH_KEY).unwrap();
        assert_eq!(
            remaining_hash_fields.len(),
            HASH_FIELDS_AND_VALUES.len() - expired_fields.len()
        );
        verify_fields_absence_from_hash(&remaining_hash_fields, &expired_fields);

        // Scenario 6
        // Verify that HGETEX on a non-existing hash returns NIL
        assert_eq!(con.exists(HASH_KEY), Ok(false));
        assert_eq!(
            redis::Commands::hget_ex(&mut con, HASH_KEY, &expired_fields, Expiry::PERSIST),
            Ok(vec![Value::Nil; expired_fields.len()])
        );
    }

    /// The test validates the various expiration options for hash fields using the HGETEX command.
    ///
    /// It tests setting expiration using the EX, PX, EXAT, and PXAT options,
    /// as well as removing an existing expiration using the PERSIST option.
    #[test]
    fn test_hget_ex_field_expiration_options() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
        let mut con = ctx.connection();
        // Create a hash with multiple fields and values that will be used for testing
        assert_eq!(con.hset_multiple(HASH_KEY, &HASH_FIELDS_AND_VALUES), Ok(()));

        // Verify that initially all fields are present in the hash
        verify_exact_hash_fields_and_values(&mut con, HASH_KEY, &HASH_FIELDS_AND_VALUES);

        // Set the fields to expire in 1 second using different expiration options
        assert_eq!(
            redis::Commands::hget_ex(
                &mut con,
                HASH_KEY,
                HASH_FIELDS_AND_VALUES[0].0,
                Expiry::EX(1)
            ),
            Ok([HASH_FIELDS_AND_VALUES[0].1])
        );
        assert_eq!(
            redis::Commands::hget_ex(
                &mut con,
                HASH_KEY,
                HASH_FIELDS_AND_VALUES[1].0,
                Expiry::PX(1000)
            ),
            Ok([HASH_FIELDS_AND_VALUES[1].1])
        );
        let current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        assert_eq!(
            redis::Commands::hget_ex(
                &mut con,
                HASH_KEY,
                HASH_FIELDS_AND_VALUES[2].0,
                Expiry::EXAT(current_timestamp.as_secs() + 1)
            ),
            Ok([HASH_FIELDS_AND_VALUES[2].1])
        );
        assert_eq!(
            redis::Commands::hget_ex(
                &mut con,
                HASH_KEY,
                HASH_FIELDS_AND_VALUES[3].0,
                Expiry::PXAT(current_timestamp.as_millis() as u64 + 1000)
            ),
            Ok([HASH_FIELDS_AND_VALUES[3].1])
        );
        assert_eq!(
            redis::Commands::hget_ex(
                &mut con,
                HASH_KEY,
                HASH_FIELDS_AND_VALUES[4].0,
                Expiry::EX(1)
            ),
            Ok([HASH_FIELDS_AND_VALUES[4].1])
        );
        // Remove the expiration from the last field
        assert_eq!(
            redis::Commands::hget_ex(
                &mut con,
                HASH_KEY,
                HASH_FIELDS_AND_VALUES[4].0,
                Expiry::PERSIST
            ),
            Ok([HASH_FIELDS_AND_VALUES[4].1])
        );

        // Wait for the fields to expire and verify that only the last field remains in the hash
        sleep(Duration::from_millis(1100));
        verify_exact_hash_fields_and_values(&mut con, HASH_KEY, &[HASH_FIELDS_AND_VALUES[4]]);

        // Remove the hash
        assert_eq!(con.del(HASH_KEY), Ok(1));
    }

    ///  The test validates the following scenarios for the HSETEX command:
    ///
    ///  Tests the behavior of HSETEX with different field existence checks (FNX and FXX):
    ///     1. (FNX) successfully sets fields in a hash that does not exist and creates the hash.
    ///     2. (FNX) fails to set fields in a hash that already contains one or more of the fields.
    ///     3. (FXX) fails to set fields in a hash that does not have one or more of the fields.
    ///
    ///  Tests the behavior of HSETEX with and without expiration:
    ///
    ///     Note: All of the following tests, use FXX and operate on existing fields.
    ///
    ///     4. It successfully sets a single field without setting its expiration
    ///        and verifies that the value has been modified, but no expiration is set.
    ///     5. It successfully sets multiple fields without setting their expiration
    ///        and verifies that their values have been modified, but no expiration is set.
    ///     6. It successfully sets a single field with an expiration
    ///        and verifies that the value has been modified and the field is set to expire.
    ///     7. It successfully sets all fields with an expiration
    ///        and verifies that their values have been modified and the fields are set to expire.
    #[test]
    fn test_hset_ex() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
        let mut con = ctx.connection();

        let generated_hash_key = generate_random_testing_hash_key(&mut con);

        let hfe_options =
            HashFieldExpirationOptions::default().set_existence_check(FieldExistenceCheck::FNX);

        // Scenario 1
        // Verify that HSETEX with FNX on a hash that does not exist succeeds and creates the hash with the specified fields and values
        let fields_set_successfully: bool = con
            .hset_ex(&generated_hash_key, &hfe_options, &HASH_FIELDS_AND_VALUES)
            .unwrap();
        assert!(fields_set_successfully);

        // Verify that the hash has been created with the expected fields and values
        verify_exact_hash_fields_and_values(&mut con, &generated_hash_key, &HASH_FIELDS_AND_VALUES);

        // Scenario 2
        // Executing HSETEX with FNX on a hash that already contains a field should fail
        let fields_and_values_for_update = [HASH_FIELDS_AND_VALUES[0], ("NonExistingField", 1)];

        let field_set_successfully: bool = con
            .hset_ex(
                &generated_hash_key,
                &hfe_options,
                &fields_and_values_for_update,
            )
            .unwrap();
        assert!(!field_set_successfully);

        // Verify that the hash consists of its original fields
        verify_exact_hash_fields_and_values(&mut con, &generated_hash_key, &HASH_FIELDS_AND_VALUES);

        // Scenario 3
        // Executing HSETEX with FXX on a hash that does not have one or more of the fields should fail
        let hfe_options = hfe_options.set_existence_check(FieldExistenceCheck::FXX);

        let field_set_successfully: bool = con
            .hset_ex(
                &generated_hash_key,
                &hfe_options,
                &fields_and_values_for_update,
            )
            .unwrap();
        assert!(!field_set_successfully);

        // Verify that the hash consists of its original fields
        verify_exact_hash_fields_and_values(&mut con, &generated_hash_key, &HASH_FIELDS_AND_VALUES);

        // Scenario 4
        // Use the HSETEX command to double the value of the first field without setting its expiration
        let initial_fields = HASH_FIELDS_AND_VALUES.map(|(key, _)| key);

        let first_field_with_doubled_value =
            [(HASH_FIELDS_AND_VALUES[0].0, HASH_FIELDS_AND_VALUES[0].1 * 2)];
        let field_set_successfully: bool = con
            .hset_ex(
                &generated_hash_key,
                &hfe_options,
                &first_field_with_doubled_value,
            )
            .unwrap();
        assert!(field_set_successfully);

        // Verify that the field's value has been set to the new value
        let hash_fields: HashMap<String, u8> =
            redis::Commands::hgetall(&mut con, &generated_hash_key).unwrap();
        assert_eq!(hash_fields.len(), initial_fields.len());
        assert_eq!(
            hash_fields[first_field_with_doubled_value[0].0],
            first_field_with_doubled_value[0].1
        );

        // Verify that the field is not set to expire
        assert_eq!(
            con.httl(&generated_hash_key, first_field_with_doubled_value[0].0),
            Ok(vec![ExistsButNotRelevant])
        );

        // Scenario 5
        // Use the HSETEX command to double the original values of all fields without setting their expiration
        let fields_with_doubled_values: Vec<(&str, u8)> = HASH_FIELDS_AND_VALUES
            .iter()
            .map(|(field, value)| (*field, value * 2))
            .collect();
        let fields_set_successfully: bool = con
            .hset_ex(
                &generated_hash_key,
                &hfe_options,
                &fields_with_doubled_values,
            )
            .unwrap();
        assert!(fields_set_successfully);

        // Verify that the values of the fields have been set to the new values.
        verify_exact_hash_fields_and_values(
            &mut con,
            &generated_hash_key,
            &fields_with_doubled_values,
        );

        // Verify that the fields are not set to expire
        assert_eq!(
            con.httl(&generated_hash_key, &initial_fields),
            Ok(vec![ExistsButNotRelevant; initial_fields.len()])
        );

        // Scenario 6
        // Use the HSETEX command to triple the original value of the first field and set its expiration to 10 seconds
        let hfe_options = hfe_options.set_expiration(SetExpiry::EX(10));
        let first_field_with_tripled_value =
            [(HASH_FIELDS_AND_VALUES[0].0, HASH_FIELDS_AND_VALUES[0].1 * 3)];
        let field_set_successfully: bool = con
            .hset_ex(
                &generated_hash_key,
                &hfe_options,
                &first_field_with_tripled_value,
            )
            .unwrap();
        assert!(field_set_successfully);

        // Verify that the field's value has been set to the new value
        let hash_fields: HashMap<String, u8> =
            redis::Commands::hgetall(&mut con, &generated_hash_key).unwrap();
        assert_eq!(hash_fields.len(), initial_fields.len());
        assert_eq!(
            hash_fields[first_field_with_tripled_value[0].0],
            first_field_with_tripled_value[0].1
        );

        // Verify that the field was set to expire
        assert_eq!(
            con.httl(&generated_hash_key, first_field_with_tripled_value[0].0)
                .unwrap()[0],
            10isize
        );

        // Scenario 7
        // Use the HSETEX command to triple the values of all initial fields and set their expiration to 1 second
        let hfe_options = hfe_options.set_expiration(SetExpiry::EX(1));
        let fields_with_tripled_values: Vec<(&str, u8)> = HASH_FIELDS_AND_VALUES
            .iter()
            .map(|(field, value)| (*field, value * 3))
            .collect();
        let fields_set_successfully: bool = con
            .hset_ex(
                &generated_hash_key,
                &hfe_options,
                &fields_with_tripled_values,
            )
            .unwrap();
        assert!(fields_set_successfully);

        // Verify that the fields' values have been set to the new values
        verify_exact_hash_fields_and_values(
            &mut con,
            &generated_hash_key,
            &fields_with_tripled_values,
        );

        // Verify that the fields were set to expire
        assert_eq!(
            con.httl(&generated_hash_key, &initial_fields).unwrap(),
            vec![IntegerReply(1); initial_fields.len()]
        );

        // Wait for the fields to expire
        sleep(Duration::from_millis(1100));

        // Verify that the fields have expired
        let remaining_hash_fields: HashMap<String, String> =
            con.hgetall(&generated_hash_key).unwrap();
        assert_eq!(
            remaining_hash_fields.len(),
            HASH_FIELDS_AND_VALUES.len() - initial_fields.len()
        );
        verify_fields_absence_from_hash(&remaining_hash_fields, &initial_fields);
    }

    /// The test validates the various expiration options for hash fields using the HSETEX command.
    ///
    /// It tests setting expiration using the EX, PX, EXAT, and PXAT options,
    /// as well as keeping an existing expiration using the KEEPTTL option.
    #[test]
    fn test_hsetex_field_expiration_options() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
        let mut con = ctx.connection();
        // Create a hash with multiple fields and values that will be used for testing
        assert_eq!(con.hset_multiple(HASH_KEY, &HASH_FIELDS_AND_VALUES), Ok(()));

        // Verify that initially all fields are present in the hash
        verify_exact_hash_fields_and_values(&mut con, HASH_KEY, &HASH_FIELDS_AND_VALUES);

        // Set the fields to expire in 1 second using different expiration options
        let hfe_options = HashFieldExpirationOptions::default()
            .set_existence_check(FieldExistenceCheck::FXX)
            .set_expiration(SetExpiry::EX(1));

        let expiration_set_successfully: bool = con
            .hset_ex(HASH_KEY, &hfe_options, &[HASH_FIELDS_AND_VALUES[0]])
            .unwrap();
        assert!(expiration_set_successfully);

        let hfe_options = hfe_options.set_expiration(SetExpiry::PX(1000));
        let expiration_set_successfully: bool = con
            .hset_ex(HASH_KEY, &hfe_options, &[HASH_FIELDS_AND_VALUES[1]])
            .unwrap();
        assert!(expiration_set_successfully);

        let current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let hfe_options =
            hfe_options.set_expiration(SetExpiry::EXAT(current_timestamp.as_secs() + 1));
        let expiration_set_successfully: bool = con
            .hset_ex(HASH_KEY, &hfe_options, &[HASH_FIELDS_AND_VALUES[2]])
            .unwrap();
        assert!(expiration_set_successfully);

        let hfe_options = hfe_options
            .set_expiration(SetExpiry::PXAT(current_timestamp.as_millis() as u64 + 1000));
        let expiration_set_successfully: bool = con
            .hset_ex(HASH_KEY, &hfe_options, &[HASH_FIELDS_AND_VALUES[3]])
            .unwrap();
        assert!(expiration_set_successfully);

        let hfe_options = hfe_options.set_expiration(SetExpiry::EX(1));
        let expiration_set_successfully: bool = con
            .hset_ex(HASH_KEY, &hfe_options, &[HASH_FIELDS_AND_VALUES[4]])
            .unwrap();
        assert!(expiration_set_successfully);

        // Using KEEPTTL will preserve the 1 second set above
        let hfe_options = hfe_options.set_expiration(SetExpiry::KEEPTTL);
        let expiration_set_successfully: bool = con
            .hset_ex(HASH_KEY, &hfe_options, &[HASH_FIELDS_AND_VALUES[4]])
            .unwrap();
        assert!(expiration_set_successfully);

        // Wait for the fields to expire and verify it
        sleep(Duration::from_millis(1100));

        // Verify that all fields have expired and the hash no longer exists
        assert_eq!(con.exists(HASH_KEY), Ok(false));
    }

    #[test]
    fn test_hsetex_can_update_the_expiration_of_a_field_that_has_already_been_set_to_expire() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
        let mut con = ctx.connection();
        // Create a hash with multiple fields and values that will be used for testing
        assert_eq!(con.hset_multiple(HASH_KEY, &HASH_FIELDS_AND_VALUES), Ok(()));

        let hfe_options = HashFieldExpirationOptions::default()
            .set_existence_check(FieldExistenceCheck::FXX)
            .set_expiration(SetExpiry::EX(1));

        // Use the HSETEX command to set the first field to expire in 1 second
        let field_set_successfully: bool = con
            .hset_ex(HASH_KEY, &hfe_options, &[HASH_FIELDS_AND_VALUES[0]])
            .unwrap();
        assert!(field_set_successfully);

        // Use the HSETEX command again to set the timeout to 2 seconds
        let hfe_options = hfe_options.set_expiration(SetExpiry::EX(2));
        let field_set_successfully: bool = con
            .hset_ex(HASH_KEY, &hfe_options, &[HASH_FIELDS_AND_VALUES[0]])
            .unwrap();
        assert!(field_set_successfully);
        // Verify that all of the fields still have their initial values
        verify_exact_hash_fields_and_values(&mut con, HASH_KEY, &HASH_FIELDS_AND_VALUES);

        // Verify that the field was set to expire
        assert_eq!(
            con.httl(HASH_KEY, HASH_FIELDS_AND_VALUES[0].0).unwrap()[0],
            2isize
        );

        // Wait for the field to expire
        sleep(Duration::from_millis(2100));

        // Verify that the field has expired
        let remaining_hash_fields: HashMap<String, String> = con.hgetall(HASH_KEY).unwrap();
        assert_eq!(
            remaining_hash_fields.len(),
            HASH_FIELDS_AND_VALUES.len() - 1
        );
        verify_fields_absence_from_hash(&remaining_hash_fields, &[HASH_FIELDS_AND_VALUES[0].0]);

        // Remove the hash
        assert_eq!(con.del(HASH_KEY), Ok(1));
    }

    // Requires redis-server >= 4.0.0.
    // Not supported with the current appveyor/windows binary deployed.
    #[cfg(not(target_os = "windows"))]
    #[test]
    fn test_unlink() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        redis::cmd("SET").arg("foo").arg(42).exec(&mut con).unwrap();
        assert_eq!(redis::cmd("GET").arg("foo").query(&mut con), Ok(42));
        assert_eq!(con.unlink("foo"), Ok(1));

        redis::cmd("SET").arg("foo").arg(42).exec(&mut con).unwrap();
        redis::cmd("SET").arg("bar").arg(42).exec(&mut con).unwrap();
        assert_eq!(con.unlink(&["foo", "bar"]), Ok(2));
    }

    #[test]
    fn test_set_ops() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        assert_eq!(con.sadd("foo", &[1, 2, 3]), Ok(3));

        let mut s: Vec<i32> = redis::Commands::smembers(&mut con, "foo").unwrap();
        s.sort_unstable();
        assert_eq!(s.len(), 3);
        assert_eq!(&s, &[1, 2, 3]);

        let set: HashSet<i32> = redis::Commands::smembers(&mut con, "foo").unwrap();
        assert_eq!(set.len(), 3);
        assert!(set.contains(&1i32));
        assert!(set.contains(&2i32));
        assert!(set.contains(&3i32));

        let set: BTreeSet<i32> = redis::Commands::smembers(&mut con, "foo").unwrap();
        assert_eq!(set.len(), 3);
        assert!(set.contains(&1i32));
        assert!(set.contains(&2i32));
        assert!(set.contains(&3i32));
    }

    #[test]
    fn test_scan() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        assert_eq!(con.sadd("foo", &[1, 2, 3]), Ok(3));

        let (cur, mut s): (i32, Vec<i32>) = redis::cmd("SSCAN")
            .arg("foo")
            .arg(0)
            .query(&mut con)
            .unwrap();
        s.sort_unstable();
        assert_eq!(cur, 0i32);
        assert_eq!(s.len(), 3);
        assert_eq!(&s, &[1, 2, 3]);
    }

    #[test]
    fn test_optionals() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        redis::cmd("SET").arg("foo").arg(1).exec(&mut con).unwrap();

        let (a, b): (Option<i32>, Option<i32>) = redis::cmd("MGET")
            .arg("foo")
            .arg("missing")
            .query(&mut con)
            .unwrap();
        assert_eq!(a, Some(1i32));
        assert_eq!(b, None);

        let a = redis::cmd("GET")
            .arg("missing")
            .query(&mut con)
            .unwrap_or(0i32);
        assert_eq!(a, 0i32);
    }

    #[test]
    fn test_scanning() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();
        let mut unseen = HashSet::new();

        for x in 0..1000 {
            redis::cmd("SADD").arg("foo").arg(x).exec(&mut con).unwrap();
            unseen.insert(x);
        }

        let iter = redis::cmd("SSCAN")
            .arg("foo")
            .cursor_arg(0)
            .clone()
            .iter(&mut con)
            .unwrap();

        let iter = iter.map(std::result::Result::unwrap);

        for x in iter {
            let x: usize = x;
            unseen.remove(&x);
        }

        assert_eq!(unseen.len(), 0);
    }

    #[test]
    fn test_checked_scanning_error() {
        const KEY_COUNT: u32 = 1000;

        let ctx = TestContext::new();
        let mut con = ctx.connection();

        // Insert a bunch of keys with legit UTF-8 first
        for x in 0..KEY_COUNT {
            redis::cmd("SET")
                .arg(format!("foo{x}"))
                .arg(x)
                .exec(&mut con)
                .unwrap();
        }

        // This key is raw bytes, invalid UTF-8
        redis::cmd("SET")
            .arg(b"\xc3\x28")
            .arg("invalid")
            .exec(&mut con)
            .unwrap();

        // get an iterator for SCAN over all redis keys
        // Specify count=1 so we don't get the invalid UTF-8 scenario in the first scan
        let iter = con
            .scan_options::<String>(ScanOptions::default().with_count(1))
            .unwrap();

        let mut error_kind = None;
        let mut count = 0;

        // iterate over the entire keyspace till we reach the end
        for x in iter {
            if let Err(current_error) = x {
                if error_kind.is_some() {
                    panic!("Encountered multiple errors");
                }
                // we found the error case
                error_kind = Some(current_error.kind());
            } else {
                count += 1;
            }
        }

        // we should have been able to iterate over the entire keyspace EXCEPT the
        // key which failed to parse, so count should be 1000 (1000 valid keys)
        assert_eq!(count, KEY_COUNT);

        // make sure we encountered the error (i.e. instead of silent failure)
        assert_eq!(error_kind, Some(ErrorKind::Parse));
    }

    #[test]
    fn test_filtered_scanning() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();
        let mut unseen = HashSet::new();

        for x in 0..3000 {
            con.hset("foo", format!("key_{}_{}", x % 100, x), x)
                .unwrap();
            if x % 100 == 0 {
                unseen.insert(x);
            }
        }

        let iter = con
            .hscan_match::<&str, &str, (String, usize)>("foo", "key_0_*")
            .unwrap();

        let iter = iter.map(std::result::Result::unwrap);

        for (_field, value) in iter {
            unseen.remove(&value);
        }

        assert_eq!(unseen.len(), 0);
    }

    #[test]
    fn test_scan_with_options_works() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();
        for i in 0..20usize {
            con.append(format!("test/{i}"), i).unwrap();
            con.append(format!("other/{i}"), i).unwrap();
        }
        con.hset("test-hset", "test-field", "test-value").unwrap();

        // scan with pattern
        let opts = ScanOptions::default().with_count(20).with_pattern("test/*");
        let values = con.scan_options::<String>(opts).unwrap();
        let values: Vec<_> = values.collect();
        assert_eq!(values.len(), 20);
        // scan without pattern
        let opts = ScanOptions::default();
        let values = con.scan_options::<String>(opts).unwrap();
        let values: Vec<_> = values.collect();
        assert_eq!(values.len(), 41);
        // scan with type string
        let opts = ScanOptions::default().with_type("string");
        let values = con.scan_options::<String>(opts).unwrap();
        let values: Vec<_> = values.collect();
        assert_eq!(values.len(), 40);
        // scan with type hash
        let opts = ScanOptions::default().with_type("hash");
        let values = con.scan_options::<String>(opts).unwrap();
        let values: Vec<_> = values.collect();
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn test_pipeline() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let ((k1, k2),): ((i32, i32),) = redis::pipe()
            .cmd("SET")
            .arg("key_1")
            .arg(42)
            .ignore()
            .cmd("SET")
            .arg("key_2")
            .arg(43)
            .ignore()
            .cmd("MGET")
            .arg(&["key_1", "key_2"])
            .query(&mut con)
            .unwrap();

        assert_eq!(k1, 42);
        assert_eq!(k2, 43);
    }

    #[test]
    fn test_pipeline_with_err() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        redis::cmd("SET")
            .arg("x")
            .arg("x-value")
            .exec(&mut con)
            .unwrap();
        redis::cmd("SET")
            .arg("y")
            .arg("y-value")
            .exec(&mut con)
            .unwrap();

        redis::cmd("SLAVEOF")
            .arg("1.1.1.1")
            .arg("99")
            .exec(&mut con)
            .unwrap();

        let res = redis::pipe()
            .set("x", "another-x-value")
            .ignore()
            .get("y")
            .exec(&mut con);
        assert_eq!(
            res.unwrap_err().kind(),
            redis::ServerErrorKind::ReadOnly.into()
        );

        // Make sure we don't get leftover responses from the pipeline ("y-value"). See #436.
        let res = redis::cmd("GET")
            .arg("x")
            .query::<String>(&mut con)
            .unwrap();
        assert_eq!(res, "x-value");
    }

    #[test]
    fn test_pipeline_returns_server_errors() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();
        let mut pipe = redis::pipe();
        pipe.set("x", "x-value")
            .ignore()
            .hset("x", "field", "field_value")
            .ignore()
            .get("x");

        let res = pipe.exec(&mut con);
        let error_message = res.unwrap_err().to_string();
        assert_eq!(&error_message, "Pipeline failure: [(Index 1, error: \"WRONGTYPE\": Operation against a key holding the wrong kind of value)]");
    }

    #[test]
    fn test_empty_pipeline() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        redis::pipe().cmd("PING").ignore().exec(&mut con).unwrap();
    }

    #[test]
    fn test_pipeline_transaction() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let ((k1, k2),): ((i32, i32),) = redis::pipe()
            .atomic()
            .cmd("SET")
            .arg("key_1")
            .arg(42)
            .ignore()
            .cmd("SET")
            .arg("key_2")
            .arg(43)
            .ignore()
            .cmd("MGET")
            .arg(&["key_1", "key_2"])
            .query(&mut con)
            .unwrap();

        assert_eq!(k1, 42);
        assert_eq!(k2, 43);
    }

    #[test]
    fn test_pipeline_transaction_with_errors() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let _: () = con.set("x", 42).unwrap();

        // Make Redis a replica of a nonexistent master, thereby making it read-only.
        redis::cmd("slaveof")
            .arg("1.1.1.1")
            .arg("1")
            .exec(&mut con)
            .unwrap();

        // Ensure that a write command fails with a READONLY error
        let err = redis::pipe()
            .atomic()
            .ping()
            .set("x", 142)
            .ignore()
            .get("x")
            .set("x", 142)
            .query::<()>(&mut con)
            .unwrap_err();

        assert_eq!(err.kind(), ServerErrorKind::ExecAbort.into());
        let errors = err.into_server_errors().unwrap();
        assert_eq!(errors.len(), 2);
        assert_eq!(errors[0].0, 1);
        assert_eq!(errors[0].1.kind(), ServerErrorKind::ReadOnly.into());
        assert_eq!(errors[1].0, 3);
        assert_eq!(errors[1].1.kind(), ServerErrorKind::ReadOnly.into());

        let x: i32 = redis::Commands::get(&mut con, "x").unwrap();
        assert_eq!(x, 42);
    }

    #[test]
    fn test_pipeline_reuse_query() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let mut pl = redis::pipe();

        let ((k1,),): ((i32,),) = pl
            .cmd("SET")
            .arg("pkey_1")
            .arg(42)
            .ignore()
            .cmd("MGET")
            .arg(&["pkey_1"])
            .query(&mut con)
            .unwrap();

        assert_eq!(k1, 42);

        redis::cmd("DEL").arg("pkey_1").exec(&mut con).unwrap();

        // The internal commands vector of the pipeline still contains the previous commands.
        let ((k1,), (k2, k3)): ((i32,), (i32, i32)) = pl
            .cmd("SET")
            .arg("pkey_2")
            .arg(43)
            .ignore()
            .cmd("MGET")
            .arg(&["pkey_1"])
            .arg(&["pkey_2"])
            .query(&mut con)
            .unwrap();

        assert_eq!(k1, 42);
        assert_eq!(k2, 42);
        assert_eq!(k3, 43);
    }

    #[test]
    fn test_pipeline_reuse_query_clear() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let mut pl = redis::pipe();

        let ((k1,),): ((i32,),) = pl
            .cmd("SET")
            .arg("pkey_1")
            .arg(44)
            .ignore()
            .cmd("MGET")
            .arg(&["pkey_1"])
            .query(&mut con)
            .unwrap();
        pl.clear();

        assert_eq!(k1, 44);

        redis::cmd("DEL").arg("pkey_1").exec(&mut con).unwrap();

        let ((k1, k2),): ((bool, i32),) = pl
            .cmd("SET")
            .arg("pkey_2")
            .arg(45)
            .ignore()
            .cmd("MGET")
            .arg(&["pkey_1"])
            .arg(&["pkey_2"])
            .query(&mut con)
            .unwrap();
        pl.clear();

        assert!(!k1);
        assert_eq!(k2, 45);
    }

    #[test]
    fn test_pipeline_len() {
        let mut pl = redis::pipe();

        pl.cmd("PING").cmd("SET").arg(1);
        assert_eq!(pl.len(), 2);
    }

    #[test]
    fn test_pipeline_is_empty() {
        let mut pl = redis::pipe();

        assert!(pl.is_empty());
        pl.cmd("PING").cmd("SET").arg(1);
        assert!(!pl.is_empty());
    }

    #[test]
    fn test_real_transaction() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let key = "the_key";
        redis::cmd("SET").arg(key).arg(42).exec(&mut con).unwrap();

        loop {
            redis::cmd("WATCH").arg(key).exec(&mut con).unwrap();
            let val: isize = redis::cmd("GET").arg(key).query(&mut con).unwrap();
            let response: Option<(isize,)> = redis::pipe()
                .atomic()
                .cmd("SET")
                .arg(key)
                .arg(val + 1)
                .ignore()
                .cmd("GET")
                .arg(key)
                .query(&mut con)
                .unwrap();

            match response {
                None => {
                    continue;
                }
                Some(response) => {
                    assert_eq!(response, (43,));
                    break;
                }
            }
        }
    }

    #[test]
    fn test_real_transaction_highlevel() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let key = "the_key";
        redis::cmd("SET").arg(key).arg(42).exec(&mut con).unwrap();

        let response: (isize,) = redis::transaction(&mut con, &[key], |con, pipe| {
            let val: isize = redis::cmd("GET").arg(key).query(con)?;
            pipe.cmd("SET")
                .arg(key)
                .arg(val + 1)
                .ignore()
                .cmd("GET")
                .arg(key)
                .query(con)
        })
        .unwrap();

        assert_eq!(response, (43,));
    }

    #[test]
    fn test_pubsub() {
        use std::sync::{Arc, Barrier};
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        // Connection for subscriber api
        let mut pubsub_con = ctx.connection();
        let (tx, rx) = std::sync::mpsc::channel();
        // Only useful when RESP3 is enabled
        pubsub_con.set_push_sender(tx);

        // Barrier is used to make test thread wait to publish
        // until after the pubsub thread has subscribed.
        let barrier = Arc::new(Barrier::new(2));
        let pubsub_barrier = barrier.clone();

        let thread = spawn(move || {
            let mut pubsub = pubsub_con.as_pubsub();
            pubsub.subscribe("foo").unwrap();

            let _ = pubsub_barrier.wait();

            let msg = pubsub.get_message().unwrap();
            assert_eq!(msg.get_channel(), Ok("foo".to_string()));
            assert_eq!(msg.get_payload(), Ok(42));

            let msg = pubsub.get_message().unwrap();
            assert_eq!(msg.get_channel(), Ok("foo".to_string()));
            assert_eq!(msg.get_payload(), Ok(23));
        });

        let _ = barrier.wait();
        redis::cmd("PUBLISH")
            .arg("foo")
            .arg(42)
            .exec(&mut con)
            .unwrap();
        // We can also call the command directly
        assert_eq!(con.publish("foo", 23), Ok(1));

        thread.join().expect("Something went wrong");
        if ctx.protocol.supports_resp3() {
            // We expect all push messages to be here, since sync connection won't read in background
            // we can't receive push messages without requesting some command
            let PushInfo { kind, data } = rx.try_recv().unwrap();
            assert_eq!(
                (
                    PushKind::Subscribe,
                    vec![Value::BulkString("foo".as_bytes().to_vec()), Value::Int(1)]
                ),
                (kind, data)
            );
            let PushInfo { kind, data } = rx.try_recv().unwrap();
            assert_eq!(
                (
                    PushKind::Message,
                    vec![
                        Value::BulkString("foo".as_bytes().to_vec()),
                        Value::BulkString("42".as_bytes().to_vec())
                    ]
                ),
                (kind, data)
            );
            let PushInfo { kind, data } = rx.try_recv().unwrap();
            assert_eq!(
                (
                    PushKind::Message,
                    vec![
                        Value::BulkString("foo".as_bytes().to_vec()),
                        Value::BulkString("23".as_bytes().to_vec())
                    ]
                ),
                (kind, data)
            );
        }
    }

    #[test]
    fn test_pubsub_handles_timeout() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        // Connection for subscriber api
        let mut pubsub_con = ctx.connection();
        pubsub_con
            .set_read_timeout(Some(Duration::from_millis(5)))
            .unwrap();
        let mut pubsub_con = pubsub_con.as_pubsub();

        pubsub_con.subscribe("foo").unwrap();
        let err = pubsub_con.get_message().unwrap_err();
        assert!(err.is_timeout());

        con.publish("foo", "bar").unwrap();

        let msg = pubsub_con.get_message().unwrap();
        assert_eq!(msg.get_channel(), Ok("foo".to_string()));
        assert_eq!(msg.get_payload(), Ok("bar".to_string()));
    }

    #[test]
    fn test_pubsub_send_ping() {
        use std::sync::{Arc, Barrier};
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        // Connection for subscriber api
        let mut pubsub_con = ctx.connection();

        // Barrier is used to make test thread wait to publish
        // until after the pubsub thread has subscribed.
        let publish_barrier = Arc::new(Barrier::new(2));
        let pubsub_barrier = publish_barrier.clone();
        // Barrier is used to make pubsub thread wait to ping
        // until after the test thread has published.
        let ping_barrier = Arc::new(Barrier::new(2));
        let ping_barrier_clone = ping_barrier.clone();

        let thread = spawn(move || {
            let mut pubsub = pubsub_con.as_pubsub();
            pubsub.subscribe("foo").unwrap();

            let _ = pubsub_barrier.wait();
            let _ = ping_barrier_clone.wait();

            if ctx.protocol.supports_resp3() {
                let message: String = pubsub.ping().unwrap();
                assert_eq!(message, "PONG");
            } else {
                let message: Vec<String> = pubsub.ping().unwrap();
                assert_eq!(message, vec!["pong", ""]);
            }

            if ctx.protocol.supports_resp3() {
                let message: String = pubsub.ping_message("foobar").unwrap();
                assert_eq!(message, "foobar");
            } else {
                let message: Vec<String> = pubsub.ping_message("foobar").unwrap();
                assert_eq!(message, vec!["pong", "foobar"]);
            }

            let msg = pubsub.get_message().unwrap();
            assert_eq!(msg.get_channel(), Ok("foo".to_string()));
            assert_eq!(msg.get_payload(), Ok(42));

            let msg = pubsub.get_message().unwrap();
            assert_eq!(msg.get_channel(), Ok("foo".to_string()));
            assert_eq!(msg.get_payload(), Ok(23));
        });

        let _ = publish_barrier.wait();

        redis::cmd("PUBLISH")
            .arg("foo")
            .arg(42)
            .exec(&mut con)
            .unwrap();

        // We can also call the command directly
        assert_eq!(con.publish("foo", 23), Ok(1));

        ping_barrier.wait();

        thread.join().expect("Something went wrong");
    }

    #[test]
    fn pub_sub_subscription_to_multiple_channels() {
        let ctx = TestContext::new();
        let mut conn = ctx.connection();
        let mut pubsub_conn = conn.as_pubsub();
        pubsub_conn.subscribe(&["phonewave", "foo", "bar"]).unwrap();
        let mut publish_conn = ctx.connection();

        publish_conn.publish("phonewave", "banana").unwrap();
        let msg_payload: String = pubsub_conn.get_message().unwrap().get_payload().unwrap();
        assert_eq!("banana".to_string(), msg_payload);

        publish_conn.publish("foo", "foobar").unwrap();
        let msg_payload: String = pubsub_conn.get_message().unwrap().get_payload().unwrap();
        assert_eq!("foobar".to_string(), msg_payload);
    }

    #[test]
    fn test_pubsub_unsubscribe() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let (tx, rx) = std::sync::mpsc::channel();
        // Only useful when RESP3 is enabled
        con.set_push_sender(tx);
        {
            let mut pubsub = con.as_pubsub();
            pubsub.subscribe("foo").unwrap();
            pubsub.subscribe("bar").unwrap();
            pubsub.subscribe("baz").unwrap();
            pubsub.psubscribe("foo*").unwrap();
            pubsub.psubscribe("bar*").unwrap();
            pubsub.psubscribe("baz*").unwrap();
        }

        // Connection should be usable again for non-pubsub commands
        con.set("foo", "bar").unwrap();
        let value = con.get("foo").unwrap().unwrap();
        assert_eq!(&value[..], "bar");

        if ctx.protocol.supports_resp3() {
            // Since UNSUBSCRIBE and PUNSUBSCRIBE may give channel names in different orders, there is this weird test.
            let expected_values = vec![
                (PushKind::Subscribe, "foo".to_string()),
                (PushKind::Subscribe, "bar".to_string()),
                (PushKind::Subscribe, "baz".to_string()),
                (PushKind::PSubscribe, "foo*".to_string()),
                (PushKind::PSubscribe, "bar*".to_string()),
                (PushKind::PSubscribe, "baz*".to_string()),
                (PushKind::Unsubscribe, "foo".to_string()),
                (PushKind::Unsubscribe, "bar".to_string()),
                (PushKind::Unsubscribe, "baz".to_string()),
                (PushKind::PUnsubscribe, "foo*".to_string()),
                (PushKind::PUnsubscribe, "bar*".to_string()),
                (PushKind::PUnsubscribe, "baz*".to_string()),
            ];
            let mut received_values = vec![];
            for _ in &expected_values {
                let PushInfo { kind, data } = rx.try_recv().unwrap();
                let channel_name: String =
                    redis::from_redis_value_ref(data.first().unwrap()).unwrap();
                received_values.push((kind, channel_name));
            }
            for val in expected_values {
                assert!(received_values.contains(&val))
            }
        }
    }

    #[test]
    fn test_pubsub_subscribe_while_messages_are_sent() {
        let ctx = TestContext::new();
        let mut conn_external = ctx.connection();
        let mut conn_internal = ctx.connection();
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let (sender, receiver) = std::sync::mpsc::channel();
        // receive message from foo channel
        let thread = std::thread::spawn(move || {
            let mut pubsub = conn_internal.as_pubsub();
            pubsub.subscribe("foo").unwrap();
            sender.send(()).unwrap();
            loop {
                let msg = pubsub.get_message().unwrap();
                let channel = msg.get_channel_name();
                let content: i32 = msg.get_payload().unwrap();
                received
                    .lock()
                    .unwrap()
                    .push(format!("{channel}:{content}"));
                if content == -1 {
                    return;
                }
                if content == 5 {
                    // subscribe bar channel using the same pubsub
                    pubsub.subscribe("bar").unwrap();
                    sender.send(()).unwrap();
                }
            }
        });
        receiver.recv().unwrap();

        // send message to foo channel after channel is ready.
        for index in 0..10 {
            println!("publishing on foo {index}");
            redis::cmd("PUBLISH")
                .arg("foo")
                .arg(index)
                .query::<i32>(&mut conn_external)
                .unwrap();
        }
        receiver.recv().unwrap();
        redis::cmd("PUBLISH")
            .arg("bar")
            .arg(-1)
            .query::<i32>(&mut conn_external)
            .unwrap();
        thread.join().unwrap();
        assert_eq!(
            *received_clone.lock().unwrap(),
            (0..10)
                .map(|index| format!("foo:{index}"))
                .chain(std::iter::once("bar:-1".to_string()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_pubsub_unsubscribe_no_subs() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        {
            let _pubsub = con.as_pubsub();
        }

        // Connection should be usable again for non-pubsub commands
        con.set("foo", "bar").unwrap();
        let value = con.get("foo").unwrap().unwrap();
        assert_eq!(&value[..], "bar");
    }

    #[test]
    fn test_pubsub_unsubscribe_one_sub() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        {
            let mut pubsub = con.as_pubsub();
            pubsub.subscribe("foo").unwrap();
        }

        // Connection should be usable again for non-pubsub commands
        con.set("foo", "bar").unwrap();
        let value = con.get("foo").unwrap().unwrap();
        assert_eq!(&value[..], "bar");
    }

    #[test]
    fn test_pubsub_unsubscribe_one_sub_one_psub() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        {
            let mut pubsub = con.as_pubsub();
            pubsub.subscribe("foo").unwrap();
            pubsub.psubscribe("foo*").unwrap();
        }

        // Connection should be usable again for non-pubsub commands
        con.set("foo", "bar").unwrap();
        let value = con.get("foo").unwrap().unwrap();
        assert_eq!(&value[..], "bar");
    }

    #[test]
    fn scoped_pubsub() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        // Connection for subscriber api
        let mut pubsub_con = ctx.connection();

        let thread = spawn(move || {
            let mut count = 0;
            pubsub_con
                .subscribe(&["foo", "bar"], |msg| {
                    count += 1;
                    match count {
                        1 => {
                            assert_eq!(msg.get_channel(), Ok("foo".to_string()));
                            assert_eq!(msg.get_payload(), Ok(42));
                            ControlFlow::Continue
                        }
                        2 => {
                            assert_eq!(msg.get_channel(), Ok("bar".to_string()));
                            assert_eq!(msg.get_payload(), Ok(23));
                            ControlFlow::Break(())
                        }
                        _ => ControlFlow::Break(()),
                    }
                })
                .unwrap();

            pubsub_con
        });

        // Can't use a barrier in this case since there's no opportunity to run code
        // between channel subscription and blocking for messages.
        sleep(Duration::from_millis(100));

        redis::cmd("PUBLISH")
            .arg("foo")
            .arg(42)
            .exec(&mut con)
            .unwrap();
        assert_eq!(con.publish("bar", 23), Ok(1));

        // Wait for thread
        let mut pubsub_con = thread.join().expect("pubsub thread terminates ok");

        // Connection should be usable again for non-pubsub commands
        pubsub_con.set("foo", "bar").unwrap();
        let value = pubsub_con.get("foo").unwrap().unwrap();
        assert_eq!(&value[..], "bar");
    }

    #[test]
    fn test_tuple_args() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        redis::cmd("HMSET")
            .arg("my_key")
            .arg(&[("field_1", 42), ("field_2", 23)])
            .exec(&mut con)
            .unwrap();

        assert_eq!(
            redis::cmd("HGET")
                .arg("my_key")
                .arg("field_1")
                .query(&mut con),
            Ok(42)
        );
        assert_eq!(
            redis::cmd("HGET")
                .arg("my_key")
                .arg("field_2")
                .query(&mut con),
            Ok(23)
        );
    }

    #[test]
    fn test_nice_api() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        assert!(con.set("my_key", 42).is_ok());
        assert_eq!(con.get_int("my_key"), Ok(Some(42)));

        let (k1, k2): (i32, i32) = redis::pipe()
            .atomic()
            .set("key_1", 42)
            .ignore()
            .set("key_2", 43)
            .ignore()
            .get("key_1")
            .get("key_2")
            .query(&mut con)
            .unwrap();

        assert_eq!(k1, 42);
        assert_eq!(k2, 43);
    }

    #[test]
    fn test_auto_m_versions() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        assert!(con.mset(&[("key1", 1), ("key2", 2)]).is_ok());
        assert_eq!(con.mget_ints(&["key1", "key2"]), Ok(vec!(Some(1), Some(2))));
        assert_eq!(
            con.mget_ints(vec!["key1", "key2"]),
            Ok(vec!(Some(1), Some(2)))
        );
        assert_eq!(
            con.mget_ints(vec!["key1", "key2"]),
            Ok(vec!(Some(1), Some(2)))
        );
    }

    #[test]
    fn test_nice_hash_api() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        assert_eq!(
            con.hset_multiple("my_hash", &[("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8)]),
            Ok(())
        );

        let hm: HashMap<String, isize> = redis::Commands::hgetall(&mut con, "my_hash").unwrap();
        assert_eq!(hm.get("f1"), Some(&1));
        assert_eq!(hm.get("f2"), Some(&2));
        assert_eq!(hm.get("f3"), Some(&4));
        assert_eq!(hm.get("f4"), Some(&8));
        assert_eq!(hm.len(), 4);

        let hm: BTreeMap<String, isize> = redis::Commands::hgetall(&mut con, "my_hash").unwrap();
        assert_eq!(hm.get("f1"), Some(&1));
        assert_eq!(hm.get("f2"), Some(&2));
        assert_eq!(hm.get("f3"), Some(&4));
        assert_eq!(hm.get("f4"), Some(&8));
        assert_eq!(hm.len(), 4);

        let v: Vec<(String, isize)> = redis::Commands::hgetall(&mut con, "my_hash").unwrap();
        assert_eq!(
            v,
            vec![
                ("f1".to_string(), 1),
                ("f2".to_string(), 2),
                ("f3".to_string(), 4),
                ("f4".to_string(), 8),
            ]
        );

        assert_eq!(
            redis::Commands::hmget(&mut con, "my_hash", &["f2", "f4"]),
            Ok((2, 8))
        );
        assert_eq!(
            redis::Commands::hmget(&mut con, "my_hash", &["f2", "f4"]),
            Ok((2, 8))
        );
        assert_eq!(con.hincr("my_hash", "f1", 1), Ok(2.0));
        assert_eq!(con.hincr("my_hash", "f2", 1.5), Ok(3.5));
        assert_eq!(con.hexists("my_hash", "f2"), Ok(true));
        assert!(con.hdel("my_hash", &["f1", "f2"]).is_ok());
        assert_eq!(con.hexists("my_hash", "f2"), Ok(false));

        let iter: redis::Iter<'_, (String, isize)> = con.hscan("my_hash").unwrap();
        let mut found = HashSet::new();

        let iter = iter.map(std::result::Result::unwrap);

        for item in iter {
            found.insert(item);
        }

        assert_eq!(found.len(), 2);
        assert!(found.contains(&("f3".to_string(), 4)));
        assert!(found.contains(&("f4".to_string(), 8)));
    }

    #[test]
    fn test_nice_list_api() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        assert_eq!(con.rpush("my_list", &[1, 2, 3, 4]), Ok(4));
        assert_eq!(con.rpush("my_list", &[5, 6, 7, 8]), Ok(8));
        assert_eq!(con.llen("my_list"), Ok(8));

        assert_eq!(con.lpop("my_list", Default::default()), Ok(1));
        assert_eq!(con.llen("my_list"), Ok(7));

        assert_eq!(
            redis::Commands::lrange(&mut con, "my_list", 0, 2),
            Ok((2, 3, 4))
        );

        assert!(con.lset("my_list", 0, 4).is_ok());
        assert_eq!(
            redis::Commands::lrange(&mut con, "my_list", 0, 2),
            Ok((4, 3, 4))
        );

        #[cfg(not(windows))]
        //Windows version of redis is limited to v3.x
        {
            let my_list: Vec<u8> =
                redis::Commands::lrange(&mut con, "my_list", 0, 10).expect("To get range");
            assert_eq!(
                con.lpop("my_list", core::num::NonZeroUsize::new(10)),
                Ok(my_list)
            );
        }
    }

    #[test]
    fn test_tuple_decoding_regression() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        assert!(con.del("my_zset").is_ok());
        assert_eq!(con.zadd("my_zset", "one", 1), Ok(1));
        assert_eq!(con.zadd("my_zset", "two", 2), Ok(1));

        let vec = con.zrangebyscore_withscores("my_zset", 0, 10).unwrap();
        assert_eq!(vec.len(), 2);

        assert_eq!(con.del("my_zset"), Ok(1));

        let vec = con.zrangebyscore_withscores("my_zset", 0, 10).unwrap();
        assert_eq!(vec.len(), 0);
    }

    #[test]
    fn test_tuple_decoding_from_iter() {
        const KEY: &str = "my_iter_tuple";
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let map = HashMap::from([("one", 1), ("two", 2), ("three", 3)]);

        // Insert all pairs as entries of the hash `KEY`
        assert!(con.del(KEY).is_ok());
        for kv in map.iter() {
            assert_eq!(con.hset(KEY, kv.0, kv.1), Ok(1));
        }

        let iter = con.hscan::<_, (String, u32)>(KEY).unwrap();

        let mut counter = 0;
        for kv in iter {
            let kv = kv.unwrap();

            let (key, num) = kv;

            // Check if queried tuple is in the original map
            assert_eq!(map.get(key.as_str()).unwrap(), &num);
            counter += 1;
        }

        assert_eq!(con.del(KEY), Ok(1));

        // Check that original map has the same number of entries as hscan
        // returned
        assert_eq!(map.len(), counter);
    }

    #[test]
    fn test_setbit_and_getbit_single_offset() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        assert_eq!(con.setbit("bitvec", 10, true), Ok(false));
        assert_eq!(con.getbit("bitvec", 10), Ok(true));
    }

    #[test]
    fn test_bit_operations() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_2);
        let mut con = ctx.connection();

        fn perform_bitwise_operation<F>(str1: &str, str2: &str, op: F) -> String
        where
            F: Fn(u8, u8) -> u8,
        {
            let longer_string_length = str1.len().max(str2.len());

            // Pad the shorter string with zeroes (ASCII value 0) to match the length of the longer string
            let padded_str1 = format!("{str1:0<longer_string_length$}");
            let padded_str2 = format!("{str2:0<longer_string_length$}");

            padded_str1
                .chars()
                .zip(padded_str2.chars())
                .map(|(c1, c2)| {
                    let result = op(c1 as u8, c2 as u8);
                    result as char
                })
                .collect()
        }

        let (foobar, foobaz, result) = ("foobar", "foobaz", "result");

        assert_eq!(con.set(foobar, foobar), Ok(()));
        assert_eq!(con.set(foobaz, foobaz), Ok(()));

        assert_eq!(con.bit_and(result, &[foobar, foobaz]), Ok(foobar.len()));
        assert_eq!(
            con.get(result),
            Ok(Some(perform_bitwise_operation(foobar, foobaz, |a, b| a & b)))
        );

        assert_eq!(con.bit_or(result, &[foobar, foobaz]), Ok(foobar.len()));
        assert_eq!(
            con.get(result),
            Ok(Some(perform_bitwise_operation(foobar, foobaz, |a, b| a | b)))
        );

        assert_eq!(con.bit_xor(result, &[foobar, foobaz]), Ok(foobar.len()));
        assert_eq!(
            con.get(result),
            Ok(Some(perform_bitwise_operation(foobar, foobaz, |a, b| a ^ b)))
        );

        let key_values = BTreeMap::from([("key1", b"\x0F"), ("key2", b"\xF0"), ("key3", b"\xFF")]);

        for kv in key_values.iter() {
            assert_eq!(con.set(kv.0, kv.1), Ok(()));
        }

        let keys = key_values.keys().cloned().collect::<Vec<_>>();

        // BITOP AND
        assert_eq!(con.bit_and(result, &keys), Ok(1));
        let and_result: Vec<u8> = redis::Commands::get(&mut con, result).unwrap();
        assert_eq!(and_result, vec![0x00]); // 00000000

        // BITOP OR
        assert_eq!(con.bit_or(result, &keys), Ok(1));
        let or_result: Vec<u8> = redis::Commands::get(&mut con, result).unwrap();
        assert_eq!(or_result, vec![0xFF]); // 11111111

        // BITOP XOR
        assert_eq!(con.bit_xor(result, &keys), Ok(1));
        let xor_result: Vec<u8> = redis::Commands::get(&mut con, result).unwrap();
        assert_eq!(xor_result, vec![0x00]); // 00000000

        // BITOP NOT
        let random_key = keys.choose(&mut rng()).unwrap();
        assert_eq!(con.bit_not(result, random_key), Ok(1));
        let not_result: Vec<u8> = redis::Commands::get(&mut con, result).unwrap();
        assert_eq!(not_result, vec![!key_values.get(random_key).unwrap()[0]]);

        // BITOP DIFF
        // DIFF(K1, K2, K3) = K1 ∧ ¬(K2 ∨ K3) = Members of K1 that are not members of any of K2, K3
        assert_eq!(con.bit_diff(result, &keys), Ok(1));
        let diff_result: Vec<u8> = redis::Commands::get(&mut con, result).unwrap();
        assert_eq!(diff_result, vec![0x00]); // 00000000
        assert!(con.bit_diff(result, keys[0]).is_err());

        // BITOP DIFF1
        // DIFF1(K1, K2, K3) = ¬K1 ∧ (K2 ∨ K3) = Members of one or more of K2, K3, that are not members of K1
        assert_eq!(con.bit_diff1(result, &keys), Ok(1));
        let diff1_result: Vec<u8> = redis::Commands::get(&mut con, result).unwrap();
        assert_eq!(diff1_result, vec![0xF0]); // 11110000
        assert!(con.bit_diff1(result, keys[0]).is_err());

        // BITOP ANDOR
        // ANDOR(K1, K2, K3) = K1 ∧ (K2 ∨ K3) = Members of K1 that are also members of one or more of K2, K3
        assert_eq!(con.bit_and_or(result, &keys), Ok(1));
        let and_or_result: Vec<u8> = redis::Commands::get(&mut con, result).unwrap();
        assert_eq!(and_or_result, vec![0x0F]); // 00001111
        assert!(con.bit_and_or(result, keys[0]).is_err());

        // BITOP ONE
        // ONE(K1, K2, K3) =  { e | COUNT(e in K1, K2, K3) == 1 } = Members of exactly one of K1, K2, K3
        assert_eq!(con.bit_one(result, &keys), Ok(1));
        let one_result: Vec<u8> = redis::Commands::get(&mut con, result).unwrap();
        assert_eq!(one_result, vec![0x00]); // 00000000
    }

    #[test]
    fn test_redis_server_down() {
        let mut ctx = TestContext::new();
        let mut con = ctx.connection();

        let ping = redis::cmd("PING").query::<String>(&mut con);
        assert_eq!(ping, Ok("PONG".into()));

        ctx.stop_server();

        let ping = redis::cmd("PING").query::<String>(&mut con);

        assert!(ping.is_err());
        eprintln!("{}", ping.unwrap_err());
        assert!(!con.is_open());
    }

    #[test]
    fn test_zinterstore_weights() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        con.zadd_multiple("zset1", &[(1, "one"), (2, "two"), (4, "four")])
            .unwrap();
        con.zadd_multiple("zset2", &[(1, "one"), (2, "two"), (3, "three")])
            .unwrap();

        // zinterstore_weights
        assert_eq!(
            con.zinterstore_weights("out", &[("zset1", 2), ("zset2", 3)]),
            Ok(2)
        );

        assert_eq!(
            con.zrange_withscores("out", 0, -1),
            Ok(vec![("one".to_string(), 5.0), ("two".to_string(), 10.0)])
        );

        // zinterstore_min_weights
        assert_eq!(
            con.zinterstore_min_weights("out", &[("zset1", 2), ("zset2", 3)]),
            Ok(2)
        );

        assert_eq!(
            con.zrange_withscores("out", 0, -1),
            Ok(vec![("one".to_string(), 2.0), ("two".to_string(), 4.0),])
        );

        // zinterstore_max_weights
        assert_eq!(
            con.zinterstore_max_weights("out", &[("zset1", 2), ("zset2", 3)]),
            Ok(2)
        );

        assert_eq!(
            con.zrange_withscores("out", 0, -1),
            Ok(vec![("one".to_string(), 3.0), ("two".to_string(), 6.0),])
        );
    }

    #[test]
    fn test_zunionstore_weights() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        con.zadd_multiple("zset1", &[(1, "one"), (2, "two")])
            .unwrap();
        con.zadd_multiple("zset2", &[(1, "one"), (2, "two"), (3, "three")])
            .unwrap();

        // zunionstore_weights
        assert_eq!(
            con.zunionstore_weights("out", &[("zset1", 2), ("zset2", 3)]),
            Ok(3)
        );

        assert_eq!(
            con.zrange_withscores("out", 0, -1),
            Ok(vec![
                ("one".to_string(), 5.0),
                ("three".to_string(), 9.0),
                ("two".to_string(), 10.0)
            ])
        );
        // test converting to double
        assert_eq!(
            con.zrange_withscores("out", 0, -1),
            Ok(vec![
                ("one".to_string(), 5.0),
                ("three".to_string(), 9.0),
                ("two".to_string(), 10.0)
            ])
        );

        // zunionstore_min_weights
        assert_eq!(
            con.zunionstore_min_weights("out", &[("zset1", 2), ("zset2", 3)]),
            Ok(3)
        );

        assert_eq!(
            con.zrange_withscores("out", 0, -1),
            Ok(vec![
                ("one".to_string(), 2.0),
                ("two".to_string(), 4.0),
                ("three".to_string(), 9.0)
            ])
        );

        // zunionstore_max_weights
        assert_eq!(
            con.zunionstore_max_weights("out", &[("zset1", 2), ("zset2", 3)]),
            Ok(3)
        );

        assert_eq!(
            con.zrange_withscores("out", 0, -1),
            Ok(vec![
                ("one".to_string(), 3.0),
                ("two".to_string(), 6.0),
                ("three".to_string(), 9.0)
            ])
        );
    }

    #[test]
    fn test_zrembylex() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let setname = "myzset";
        assert_eq!(
            con.zadd_multiple(
                setname,
                &[
                    (0, "apple"),
                    (0, "banana"),
                    (0, "carrot"),
                    (0, "durian"),
                    (0, "eggplant"),
                    (0, "grapes"),
                ],
            ),
            Ok(6)
        );

        // Will remove "banana", "carrot", "durian" and "eggplant"
        let num_removed = con.zrembylex(setname, "[banana", "[eggplant").unwrap();
        assert_eq!(4, num_removed);

        let remaining: Vec<String> = con.zrange(setname, 0, -1).unwrap();
        assert_eq!(remaining, vec!["apple".to_string(), "grapes".to_string()]);
    }

    // Requires redis-server >= 6.2.0.
    // Not supported with the current appveyor/windows binary deployed.
    #[cfg(not(target_os = "windows"))]
    #[test]
    fn test_zrandmember() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let setname = "myzrandset";
        con.zadd(setname, "one", 1).unwrap();

        let result: String = con.zrandmember(setname, None).unwrap();
        assert_eq!(result, "one".to_string());

        let result: Vec<String> = con.zrandmember(setname, Some(1)).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "one".to_string());

        let result: Vec<String> = con.zrandmember(setname, Some(2)).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "one".to_string());

        assert_eq!(
            con.zadd_multiple(
                setname,
                &[(2, "two"), (3, "three"), (4, "four"), (5, "five")]
            ),
            Ok(4)
        );

        let results: Vec<String> = con.zrandmember(setname, Some(5)).unwrap();
        assert_eq!(results.len(), 5);

        let results: Vec<String> = con.zrandmember(setname, Some(-5)).unwrap();
        assert_eq!(results.len(), 5);

        if !ctx.protocol.supports_resp3() {
            let results: Vec<String> = con.zrandmember_withscores(setname, 5).unwrap();
            assert_eq!(results.len(), 10);

            let results: Vec<String> = con.zrandmember_withscores(setname, -5).unwrap();
            assert_eq!(results.len(), 10);
        }

        let results: Vec<(String, f64)> = con.zrandmember_withscores(setname, 5).unwrap();
        assert_eq!(results.len(), 5);

        let results: Vec<(String, f64)> = con.zrandmember_withscores(setname, -5).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_sismember() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let setname = "myset";
        assert_eq!(con.sadd(setname, &["a"]), Ok(1));

        let result: bool = con.sismember(setname, "a").unwrap();
        assert!(result);

        let result: bool = con.sismember(setname, "b").unwrap();
        assert!(!result);
    }

    // Requires redis-server >= 6.2.0.
    // Not supported with the current appveyor/windows binary deployed.
    #[cfg(not(target_os = "windows"))]
    #[test]
    fn test_smismember() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let setname = "myset";
        assert_eq!(con.sadd(setname, &["a", "b", "c"]), Ok(3));
        let results: Vec<bool> = con.smismember(setname, &["0", "a", "b", "c", "x"]).unwrap();
        assert_eq!(results, vec![false, true, true, true, false]);
    }

    #[test]
    fn test_object_commands() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        con.set("object_key_str", "object_value_str").unwrap();
        con.set("object_key_int", 42).unwrap();

        assert_eq!(
            con.object_encoding("object_key_str").unwrap().unwrap(),
            "embstr"
        );

        assert_eq!(
            con.object_encoding("object_key_int").unwrap().unwrap(),
            "int"
        );

        assert!(con.object_idletime("object_key_str").unwrap().unwrap() <= 1);
        assert_eq!(con.object_refcount("object_key_str").unwrap().unwrap(), 1);

        // Needed for OBJECT FREQ and can't be set before object_idletime
        // since that will break getting the idletime before idletime adjuts
        redis::cmd("CONFIG")
            .arg("SET")
            .arg(b"maxmemory-policy")
            .arg("allkeys-lfu")
            .exec(&mut con)
            .unwrap();

        con.get("object_key_str").unwrap();
        // since maxmemory-policy changed, freq should reset to 1 since we only called
        // get after that
        assert_eq!(con.object_freq("object_key_str").unwrap().unwrap(), 1);
    }

    #[test]
    fn test_mget() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        con.set(1, "1").unwrap();
        let data: Vec<String> = con
            .mget(&[1])
            .unwrap()
            .into_iter()
            .map(|s| s.unwrap())
            .collect();
        assert_eq!(data, vec!["1"]);

        con.set(2, "2").unwrap();
        let data: Vec<String> = con
            .mget(&[1, 2])
            .unwrap()
            .into_iter()
            .map(|s| s.unwrap())
            .collect();
        assert_eq!(data, vec!["1", "2"]);

        let data: Vec<Option<String>> = con.mget(&[4]).unwrap();
        assert_eq!(data, vec![None]);

        let data: Vec<Option<String>> = con.mget(&[2, 4]).unwrap();
        assert_eq!(data, vec![Some("2".to_string()), None]);
    }

    #[test]
    fn test_variable_length_get() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        con.set(1, "1").unwrap();
        let keys = vec![1];
        assert_eq!(keys.len(), 1);
        let data: Vec<String> = redis::Commands::mget(&mut con, &keys).unwrap();
        assert_eq!(data, vec!["1"]);
    }

    #[test]
    fn test_multi_generics() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        assert_eq!(con.sadd(b"set1", vec![5, 42]), Ok(2));
        assert_eq!(con.sadd(999_i64, vec![42, 123]), Ok(2));
        con.rename(999_i64, b"set2").unwrap();
        assert_eq!(con.sunionstore("res", &[b"set1", b"set2"]), Ok(3));
    }

    #[test]
    fn test_set_options_with_get() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let opts = SetOptions::default().get(true);
        let data: Option<String> = con.set_options(1, "1", opts).unwrap();
        assert_eq!(data, None);

        let opts = SetOptions::default().get(true);
        let data: Option<String> = con.set_options(1, "1", opts).unwrap();
        assert_eq!(data, Some("1".to_string()));
    }

    #[test]
    fn test_set_options_options() {
        let empty = SetOptions::default();
        assert_eq!(ToRedisArgs::to_redis_args(&empty).len(), 0);

        let opts = SetOptions::default()
            .conditional_set(ExistenceCheck::NX)
            .get(true)
            .with_expiration(SetExpiry::PX(1000));

        assert_args!(&opts, "NX", "GET", "PX", "1000");

        let opts = SetOptions::default()
            .conditional_set(ExistenceCheck::XX)
            .get(true)
            .with_expiration(SetExpiry::PX(1000));

        assert_args!(&opts, "XX", "GET", "PX", "1000");

        let opts = SetOptions::default()
            .conditional_set(ExistenceCheck::XX)
            .with_expiration(SetExpiry::KEEPTTL);

        assert_args!(&opts, "XX", "KEEPTTL");

        let opts = SetOptions::default()
            .conditional_set(ExistenceCheck::XX)
            .with_expiration(SetExpiry::EXAT(100));

        assert_args!(&opts, "XX", "EXAT", "100");

        let opts = SetOptions::default().with_expiration(SetExpiry::EX(1000));

        assert_args!(&opts, "EX", "1000");
    }

    #[test]
    fn test_set_options_value_comparison() {
        let empty = SetOptions::default();
        assert_eq!(ToRedisArgs::to_redis_args(&empty).len(), 0);
        const OLD_VALUE: &str = "old_value";
        const DIGEST: &str = "digest";

        // Test all value comparison options
        let opts = SetOptions::default().value_comparison(ValueComparison::ifeq(OLD_VALUE));
        assert_args!(&opts, "IFEQ", OLD_VALUE);
        let opts = SetOptions::default().value_comparison(ValueComparison::ifne(OLD_VALUE));
        assert_args!(&opts, "IFNE", OLD_VALUE);
        let opts = SetOptions::default().value_comparison(ValueComparison::ifdeq(DIGEST));
        assert_args!(&opts, "IFDEQ", DIGEST);
        let opts = SetOptions::default().value_comparison(ValueComparison::ifdne(DIGEST));
        assert_args!(&opts, "IFDNE", DIGEST);

        // Test combinations with other parameters
        let opts = SetOptions::default()
            .conditional_set(ExistenceCheck::NX)
            .value_comparison(ValueComparison::ifeq(OLD_VALUE))
            .get(true)
            .with_expiration(SetExpiry::PX(1000));
        assert_args!(&opts, "NX", "IFEQ", OLD_VALUE, "GET", "PX", "1000");
        let opts = SetOptions::default()
            .conditional_set(ExistenceCheck::XX)
            .value_comparison(ValueComparison::ifne(OLD_VALUE))
            .get(true)
            .with_expiration(SetExpiry::EX(1000));
        assert_args!(&opts, "XX", "IFNE", OLD_VALUE, "GET", "EX", "1000");
        let opts = SetOptions::default()
            .conditional_set(ExistenceCheck::NX)
            .value_comparison(ValueComparison::ifdeq(DIGEST))
            .get(true)
            .with_expiration(SetExpiry::PX(1000));
        assert_args!(&opts, "NX", "IFDEQ", DIGEST, "GET", "PX", "1000");
        let opts = SetOptions::default()
            .conditional_set(ExistenceCheck::XX)
            .value_comparison(ValueComparison::ifdne(DIGEST))
            .get(true)
            .with_expiration(SetExpiry::EX(1000));
        assert_args!(&opts, "XX", "IFDNE", DIGEST, "GET", "EX", "1000");
    }

    /// The test validates the IFEQ value comparison option for the SET command
    #[test]
    fn test_set_value_comparison_value_equals() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
        let mut con = ctx.connection();

        let key = "test_ifeq_key";
        let non_existent_key = "test_ifeq_non_existent_key";
        let initial_value = "initial_value";
        let updated_value = "updated_value";
        let non_matching_value = "non_matching_value";

        // Set initial value
        assert_eq!(con.set(key, initial_value), Ok(()));

        // IFEQ with matching value should succeed and update the value
        let result = con
            .set_options(
                key,
                updated_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifeq(initial_value))
                    .get(true),
            )
            .unwrap();
        // When GET is specified, the previous value is returned, regardless of the conditional (IFEQ) result
        assert_eq!(result, Some(initial_value.to_string()));
        // Verify that the value was updated
        assert_eq!(con.get(key).unwrap(), Some(updated_value.to_string()));

        // IFEQ with non-existent key should not create the key
        let result = con
            .set_options(
                non_existent_key,
                updated_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifeq(initial_value))
                    .get(true),
            )
            .unwrap();
        assert_eq!(result, None);
        assert!(!con.exists(non_existent_key).unwrap());

        // IFEQ with non-matching value should not update the value
        let result = con
            .set_options(
                key,
                initial_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifeq(non_matching_value))
                    .get(true),
            )
            .unwrap();
        // When GET is specified, the previous value is returned, regardless of the conditional (IFEQ) result
        assert_eq!(result, Some(updated_value.to_string()));
        // Verify that the value was not updated
        assert_eq!(con.get(key).unwrap(), Some(updated_value.to_string()));
    }

    /// The test validates the IFNE value comparison option for the SET command
    #[test]
    fn test_set_value_comparison_value_not_equals() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
        let mut con = ctx.connection();

        let key = "test_ifne_key";
        let non_existent_key = "test_ifne_non_existent_key";
        let initial_value = "initial_value";
        let updated_value = "updated_value";
        let non_matching_value = "non_matching_value";

        // Set initial value
        assert_eq!(con.set(key, initial_value), Ok(()));

        // IFNE with non-matching value should succeed and update the value
        let result = con
            .set_options(
                key,
                updated_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifne(non_matching_value))
                    .get(true),
            )
            .unwrap();
        // When GET is specified, the previous value is returned, regardless of the conditional (IFNE) result
        assert_eq!(result, Some(initial_value.to_string()));
        // Verify that the value was updated
        assert_eq!(con.get(key).unwrap(), Some(updated_value.to_string()));

        // IFNE with non-existent key should create the key
        assert!(!con.exists(non_existent_key).unwrap());
        let result = con
            .set_options(
                non_existent_key,
                initial_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifne(non_matching_value))
                    .get(true),
            )
            .unwrap();
        // When GET is specified and the key didn't exist, the result is NIL
        assert_eq!(result, None);
        // Verify that the key was created
        assert_eq!(
            con.get(non_existent_key).unwrap(),
            Some(initial_value.to_string())
        );

        // Test IFNE with matching value should not update the value
        let result = con
            .set_options(
                key,
                initial_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifne(updated_value))
                    .get(true),
            )
            .unwrap();
        // When GET is specified, the previous value is returned, regardless of the conditional (IFNE) result
        assert_eq!(result, Some(updated_value.to_string()));
        // Verify that the value was not updated
        assert_eq!(con.get(key).unwrap(), Some(updated_value.to_string()));
    }

    /// The test validates the DIGEST command
    #[test]
    fn test_digest_command() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
        let mut con = ctx.connection();

        let key = "test_digest_key";
        let non_existent_key = "test_digest_non_existent_key";

        let value_with_leading_zeroes_digest = "v8lf0c11xh8ymlqztfd3eeq16kfn4sspw7fqmnuuq3k3t75em5wdizgcdw7uc26nnf961u2jkfzkjytls2kwlj7626sd";
        let leading_zeroes_digest = calculate_value_digest(value_with_leading_zeroes_digest);
        // Validate that the 1st four bytes are all zeroes and they are not omitted
        assert_eq!(&leading_zeroes_digest[..4], "0000");
        assert!(is_valid_16_bytes_hex_digest(&leading_zeroes_digest));

        let empty_string = String::new();
        let empty_string_digest = calculate_value_digest(&empty_string);
        assert!(is_valid_16_bytes_hex_digest(&empty_string_digest));

        use rand::RngCore;
        let mut rng = rand::rng();
        let mut random_bytes = vec![0u8; 1024];
        rng.fill_bytes(&mut random_bytes);
        let random_bytes_digest = calculate_value_digest(&random_bytes);
        println!("random_bytes_digest: {:?}", random_bytes_digest);
        assert!(is_valid_16_bytes_hex_digest(&random_bytes_digest));

        // Validate that the DIGEST command returns NIL when the given key doesn't exist
        assert_eq!(con.digest(non_existent_key).unwrap(), None);

        // Validate that the DIGEST command returns the same digest as the calculate_value_digest function
        assert_eq!(con.set(key, value_with_leading_zeroes_digest), Ok(()));
        assert_eq!(con.digest(key).unwrap(), Some(leading_zeroes_digest));

        assert_eq!(con.set(key, empty_string), Ok(()));
        assert_eq!(con.digest(key).unwrap(), Some(empty_string_digest));

        assert_eq!(con.set(key, random_bytes), Ok(()));
        assert_eq!(con.digest(key).unwrap(), Some(random_bytes_digest));

        // Validate that the DIGEST command fails when the key is not a string
        assert!(con.hset_multiple(key, &[("f1", 1), ("f2", 2)]).is_err());
    }

    /// The test validates the IFDEQ value comparison option for the SET command
    #[test]
    fn test_set_value_comparison_digest_equals() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
        let mut con = ctx.connection();

        let key = "test_ifdeq_key";
        let non_existent_key = "test_ifdeq_non_existent_key";
        let initial_value = "initial_value";
        let updated_value = "updated_value";
        let non_matching_value = "non_matching_value";

        // Set initial value
        assert_eq!(con.set(key, initial_value), Ok(()));

        // Calculate the digest of the initial value
        let initial_value_digest = calculate_value_digest(initial_value);

        // IFDEQ with matching digest should succeed and update the value
        let result = con
            .set_options(
                key,
                updated_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifdeq(&initial_value_digest))
                    .get(true),
            )
            .unwrap();
        // When GET is specified, the previous value is returned, regardless of the conditional (IFDEQ) result
        assert_eq!(result, Some(initial_value.to_string()));
        // Verify that the value was updated
        assert_eq!(con.get(key).unwrap(), Some(updated_value.to_string()));

        // IFDEQ with non-existent key should not create the key
        let result = con
            .set_options(
                non_existent_key,
                updated_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifdeq(&initial_value_digest))
                    .get(true),
            )
            .unwrap();
        assert_eq!(result, None);
        assert!(!con.exists(non_existent_key).unwrap());

        // IFDEQ with non-matching digest should not update the value
        let non_matching_digest = calculate_value_digest(non_matching_value);
        let result = con
            .set_options(
                key,
                initial_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifdeq(&non_matching_digest))
                    .get(true),
            )
            .unwrap();
        // When GET is specified, the previous value is returned, regardless of the conditional (IFDEQ) result
        assert_eq!(result, Some(updated_value.to_string()));
        // Verify that the value was not updated
        assert_eq!(con.get(key).unwrap(), Some(updated_value.to_string()));
    }

    /// The test validates the IFDNE value comparison option for the SET command
    #[test]
    fn test_set_value_comparison_digest_not_equals() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
        let mut con = ctx.connection();

        let key = "test_ifdne_key";
        let non_existent_key = "test_ifdne_non_existent_key";
        let initial_value = "initial_value";
        let updated_value = "updated_value";
        let non_matching_value = "non_matching_value";

        // Set initial value
        assert_eq!(con.set(key, initial_value), Ok(()));

        // IFDNE with non-matching digest should succeed and update the value
        let non_matching_digest = calculate_value_digest(non_matching_value);
        let result = con
            .set_options(
                key,
                updated_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifdne(&non_matching_digest))
                    .get(true),
            )
            .unwrap();
        // When GET is specified, the previous value is returned, regardless of the conditional (IFDNE) result
        assert_eq!(result, Some(initial_value.to_string()));
        // Verify that the value was updated
        assert_eq!(con.get(key).unwrap(), Some(updated_value.to_string()));

        // IFDNE with non-existent key should create the key
        let result = con
            .set_options(
                non_existent_key,
                initial_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifdne(&non_matching_digest))
                    .get(true),
            )
            .unwrap();
        // When GET is specified and the key didn't exist, the result is NIL
        assert_eq!(result, None);
        // Verify that the key was created
        assert_eq!(
            con.get(non_existent_key).unwrap(),
            Some(initial_value.to_string())
        );

        // IFDNE with matching digest should not update the value

        // Calculate the digest of the UPDATED value!
        let updated_value_digest = calculate_value_digest(updated_value);

        let result = con
            .set_options(
                key,
                non_matching_value,
                SetOptions::default()
                    .value_comparison(ValueComparison::ifdne(&updated_value_digest))
                    .get(true),
            )
            .unwrap();
        // When GET is specified, the previous value is returned, regardless of the conditional (IFDNE) result
        assert_eq!(result, Some(updated_value.to_string()));
        // Verify that the value was not updated
        assert_eq!(con.get(key).unwrap(), Some(updated_value.to_string()));
    }

    #[test]
    fn test_del_ex() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
        let mut con = ctx.connection();

        let key = "test_del_ex_key";
        let non_existent_key = "test_del_ex_non_existent_key";
        let initial_value = "initial_value";
        let non_matching_value = "non_matching_value";
        let initial_value_digest = calculate_value_digest(initial_value);
        let non_matching_digest = calculate_value_digest(non_matching_value);

        let value_comparisons = [
            ValueComparison::ifeq(initial_value),
            ValueComparison::ifne(initial_value),
            ValueComparison::ifdeq(&initial_value_digest),
            ValueComparison::ifdne(&initial_value_digest),
        ];

        // IFEQ tests
        assert_eq!(con.set(key, initial_value), Ok(()));
        // IFEQ with non-matching value should not delete the key
        assert_eq!(
            con.del_ex(key, ValueComparison::ifeq(non_matching_value)),
            Ok(0)
        );
        assert!(con.exists(key).unwrap());
        // IFEQ with matching value should succeed and delete the key
        assert_eq!(con.del_ex(key, ValueComparison::ifeq(initial_value)), Ok(1));
        assert!(!con.exists(key).unwrap());

        // IFNE tests
        assert_eq!(con.set(key, initial_value), Ok(()));
        // IFNE with matching value should not delete the key
        assert_eq!(con.del_ex(key, ValueComparison::ifne(initial_value)), Ok(0));
        assert!(con.exists(key).unwrap());
        // IFNE with non-matching value should succeed and delete the key
        assert_eq!(
            con.del_ex(key, ValueComparison::ifne(non_matching_value)),
            Ok(1)
        );
        assert!(!con.exists(key).unwrap());

        // IFDEQ tests
        assert_eq!(con.set(key, initial_value), Ok(()));
        // IFDEQ with non-matching digest should not delete the key
        assert_eq!(
            con.del_ex(key, ValueComparison::ifdeq(&non_matching_digest)),
            Ok(0)
        );
        assert!(con.exists(key).unwrap());
        // IFDEQ with matching digest should succeed and delete the key
        assert_eq!(
            con.del_ex(key, ValueComparison::ifdeq(&initial_value_digest)),
            Ok(1)
        );
        assert!(!con.exists(key).unwrap());

        // IFDNE tests
        assert_eq!(con.set(key, initial_value), Ok(()));
        // IFDNE with matching digest should not delete the key
        assert_eq!(
            con.del_ex(key, ValueComparison::ifdne(&initial_value_digest)),
            Ok(0)
        );
        assert!(con.exists(key).unwrap());
        // IFDNE with non-matching digest should succeed and delete the key
        assert_eq!(
            con.del_ex(key, ValueComparison::ifdne(&non_matching_digest)),
            Ok(1)
        );
        assert!(!con.exists(key).unwrap());

        // Verify that all comparisons work with non-existent keys and return 0
        for value_comparison in &value_comparisons {
            assert_eq!(
                con.del_ex(non_existent_key, value_comparison.clone()),
                Ok(0)
            );
        }

        // Verify that all comparisons return an error when used with a value that is not a string
        assert_eq!(con.hset_multiple(key, &[("f1", 1), ("f2", 2)]), Ok(()));
        for value_comparison in &value_comparisons {
            assert!(con.del_ex(key, value_comparison.clone()).is_err());
        }
    }

    /// Test MSetOptions serialization to Redis arguments
    #[test]
    fn test_mset_options() {
        assert_eq!(ToRedisArgs::to_redis_args(&MSetOptions::default()).len(), 0);

        // Test with NX & XX options
        let opts = MSetOptions::default().conditional_set(ExistenceCheck::NX);
        assert_args!(&opts, "NX");
        let opts = MSetOptions::default().conditional_set(ExistenceCheck::XX);
        assert_args!(&opts, "XX");

        // Test with expiration options
        let opts = MSetOptions::default().with_expiration(SetExpiry::EX(60));
        assert_args!(&opts, "EX", "60");
        let opts = MSetOptions::default().with_expiration(SetExpiry::PX(1000));
        assert_args!(&opts, "PX", "1000");
        let opts = MSetOptions::default().with_expiration(SetExpiry::EXAT(1234567890));
        assert_args!(&opts, "EXAT", "1234567890");
        let opts = MSetOptions::default().with_expiration(SetExpiry::PXAT(1234567890000));
        assert_args!(&opts, "PXAT", "1234567890000");
        let opts = MSetOptions::default().with_expiration(SetExpiry::KEEPTTL);
        assert_args!(&opts, "KEEPTTL");

        // Test combinations
        let opts = MSetOptions::default()
            .conditional_set(ExistenceCheck::NX)
            .with_expiration(SetExpiry::EX(60));
        assert_args!(&opts, "NX", "EX", "60");

        let opts = MSetOptions::default()
            .conditional_set(ExistenceCheck::XX)
            .with_expiration(SetExpiry::PX(1000));
        assert_args!(&opts, "XX", "PX", "1000");

        let opts = MSetOptions::default()
            .conditional_set(ExistenceCheck::NX)
            .with_expiration(SetExpiry::KEEPTTL);
        assert_args!(&opts, "NX", "KEEPTTL");
    }

    /// Test the MSETEX command with the NX existence option
    #[test]
    fn test_mset_ex_nx() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
        let mut con = ctx.connection();

        let key1 = "mset_ex_nx_key1";
        let key2 = "mset_ex_nx_key2";
        let key3 = "mset_ex_nx_key3";

        let initial_value1 = "initial_value1";
        let initial_value2 = "initial_value2";
        let initial_value3 = "initial_value3";

        let updated_value1 = "updated_value1";
        let updated_value2 = "updated_value2";

        let opts = MSetOptions::default().conditional_set(ExistenceCheck::NX);

        // Test 1: Setting multiple keys with NX should succeed when none of them exists
        assert!(con
            .mset_ex(&[(key1, initial_value1), (key2, initial_value2)], opts)
            .unwrap());
        // Verify that the keys were set
        assert_eq!(con.get(key1), Ok(Some(initial_value1.to_string())));
        assert_eq!(con.get(key2), Ok(Some(initial_value2.to_string())));

        // Test 2: Setting the same keys with NX should fail
        assert!(!con
            .mset_ex(&[(key1, updated_value1), (key2, updated_value2)], opts)
            .unwrap());
        // Verify that the values were not changed
        assert_eq!(con.get(key1), Ok(Some(initial_value1.to_string())));
        assert_eq!(con.get(key2), Ok(Some(initial_value2.to_string())));

        // Test 3: Setting keys with NX should fail when there is an existing one among them
        assert!(!con
            .mset_ex(&[(key1, updated_value1), (key3, initial_value3)], opts)
            .unwrap());
        // Verify that key3 was not created
        assert!(!con.exists(key3).unwrap());
    }

    /// Test the MSETEX command with the XX existence option
    #[test]
    fn test_mset_ex_xx() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
        let mut con = ctx.connection();

        let key1 = "mset_ex_xx_key1";
        let key2 = "mset_ex_xx_key2";
        let key3 = "mset_ex_xx_key3";

        let initial_value1 = "initial_value1";
        let initial_value2 = "initial_value2";
        let initial_value3 = "initial_value3";

        let updated_value1 = "updated_value1";
        let updated_value2 = "updated_value2";

        // Test 1: Setting keys with XX should fail when they don't exist
        let opts = MSetOptions::default().conditional_set(ExistenceCheck::XX);
        assert!(!con
            .mset_ex(&[(key1, initial_value1), (key2, initial_value2)], opts)
            .unwrap());
        assert!(!con.exists(key1).unwrap());
        assert!(!con.exists(key2).unwrap());

        // Create the keys with their initial values
        let opts = opts.conditional_set(ExistenceCheck::NX);
        assert!(con
            .mset_ex(&[(key1, initial_value1), (key2, initial_value2)], opts)
            .unwrap());

        let opts = opts.conditional_set(ExistenceCheck::XX);
        // Test 2: Updating existing keys with XX should succeed
        assert!(con
            .mset_ex(&[(key1, updated_value1), (key2, updated_value2)], opts)
            .unwrap());
        // Verify that the values were updated
        assert_eq!(con.get(key1), Ok(Some(updated_value1.to_string())));
        assert_eq!(con.get(key2), Ok(Some(updated_value2.to_string())));

        // Test 3: Setting keys with XX should fail when there is a non-existing one among them
        assert!(!con
            .mset_ex(&[(key1, updated_value1), (key3, initial_value3)], opts)
            .unwrap());

        // Verify key1 was not changed and key3 was not created
        assert_eq!(con.get(key1), Ok(Some(updated_value1.to_string())));
        assert!(!con.exists(key3).unwrap());
    }

    /// Test the MSETEX command with all supported expiration options
    #[test]
    fn test_mset_ex_expiration_options() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_4);
        let mut con = ctx.connection();

        let current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        // Create a list of key-value pairs and their corresponding expiration options
        // Note that the last two key-value pairs are used to test the KEEPTTL option
        let key_values = [
            ("mset_ex_exp_key1", "initial_value1"),
            ("mset_ex_exp_key2", "initial_value2"),
            ("mset_ex_exp_key3", "initial_value3"),
            ("mset_ex_exp_key4", "initial_value4"),
            ("mset_ex_exp_key5", "initial_value5"),
            ("mset_ex_exp_key6", "initial_value6"),
            ("mset_ex_exp_key7", "initial_value7"),
            ("mset_ex_exp_key8", "initial_value8"),
            ("mset_ex_exp_key9", "initial_value9"),
            ("mset_ex_exp_key10", "initial_value10"),
            ("mset_ex_exp_key9", "updated_value9"),
            ("mset_ex_exp_key10", "updated_value10"),
        ];

        let existence_checks_and_expirations = [
            (ExistenceCheck::NX, SetExpiry::EX(1)),
            (ExistenceCheck::NX, SetExpiry::PX(1000)),
            (
                ExistenceCheck::NX,
                SetExpiry::EXAT(current_timestamp.as_secs() + 1),
            ),
            (
                ExistenceCheck::NX,
                SetExpiry::PXAT(current_timestamp.as_millis() as u64 + 1000),
            ),
            (ExistenceCheck::NX, SetExpiry::EX(1)),
            (ExistenceCheck::XX, SetExpiry::KEEPTTL),
        ];
        assert!(key_values.len() % 2 == 0);
        assert!(key_values.len() == existence_checks_and_expirations.len() * 2);

        let opts = MSetOptions::default();
        for (j, (existence_check, expiry)) in existence_checks_and_expirations.iter().enumerate() {
            let i = 2 * j;

            let opts = opts
                .conditional_set(*existence_check)
                .with_expiration(*expiry);
            assert!(con
                .mset_ex(&[key_values[i], key_values[i + 1]], opts)
                .unwrap());
            assert_eq!(
                con.mget((key_values[i].0, key_values[i + 1].0)),
                Ok(vec![
                    Some(key_values[i].1.to_string()),
                    Some(key_values[i + 1].1.to_string())
                ])
            );
        }

        // Wait for the keys to expire
        sleep(Duration::from_millis(1100));

        // Verify that the keys have expired
        for key_value in &key_values {
            assert_eq!(con.exists(key_value.0), Ok(false));
        }
    }

    #[test]
    fn test_copy_options() {
        let empty = CopyOptions::default();
        assert_eq!(ToRedisArgs::to_redis_args(&empty).len(), 0);

        let opts = CopyOptions::default().db(123).replace(true);

        assert_args!(&opts, "DB", "123", "REPLACE");
    }

    #[test]
    fn test_copy() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        let opts = CopyOptions::default();
        con.set("key1", "value1").unwrap();
        let did_copy = con.copy("key1", "key2", opts).unwrap();
        // destination was free; should copy
        assert!(did_copy);
        assert_eq!(con.get("key2").unwrap(), Some("value1".to_string()));

        let did_copy2 = con.copy("notakey", "key3", opts).unwrap();
        // source does not exist; should not copy
        assert!(!did_copy2);
        assert_eq!(con.get("key3").unwrap(), None);

        con.set("key4", "value4").unwrap();
        let did_copy3 = con.copy("key1", "key4", opts).unwrap();
        // destination already exists; should not copy
        assert!(!did_copy3);
        assert_eq!(con.get("key4").unwrap(), Some("value4".to_string()));

        let did_copy4 = con.copy("key1", "key4", opts.replace(true)).unwrap();
        assert!(did_copy4);
        assert_eq!(con.get("key4").unwrap(), Some("value1".to_string()));
    }

    #[test]
    fn test_expire_time() {
        let ctx = TestContext::new();
        // EXPIRETIME/PEXPIRETIME is available from Redis version 7.4.0
        run_test_if_version_supported!(&(7, 4, 0));

        let mut con = ctx.connection();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;
        con.set_options(
            "foo",
            "bar",
            SetOptions::default().with_expiration(SetExpiry::EXAT(now as u64 + 10)),
        )
        .unwrap();
        let expire_time_seconds = match con.expire_time("foo").unwrap() {
            IntegerReply(expire_time) => expire_time,
            _ => panic!("Expected a value"),
        };
        assert_eq!(expire_time_seconds, now + 10);

        con.set_options(
            "foo",
            "bar",
            SetOptions::default().with_expiration(SetExpiry::PXAT(now as u64 * 1000 + 12_000)),
        )
        .unwrap();
        let expire_time_milliseconds = match con.pexpire_time("foo").unwrap() {
            IntegerReply(expire_time) => expire_time,
            _ => panic!("Expected a value"),
        };
        assert_eq!(expire_time_milliseconds, now * 1000 + 12_000);
    }

    #[test]
    fn test_timeout_leaves_usable_connection() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        con.set("key", "value").unwrap();

        con.set_read_timeout(Some(Duration::from_millis(1)))
            .unwrap();

        // send multiple requests that timeout.
        for _ in 0..3 {
            let res = con.blpop("foo", 0.03);
            assert!(res.unwrap_err().is_timeout());
            assert!(con.is_open());
        }

        sleep(Duration::from_millis(100));

        // we might need to flush temporary errors from the system
        for _ in 0..100 {
            if cmd("PING").exec(&mut con).is_ok() {
                break;
            }
            sleep(Duration::from_millis(1));
        }

        let res = con.get("key").unwrap().unwrap();
        assert_eq!(res, "value");
    }

    #[test]
    fn test_timeout_in_middle_of_message_leaves_connection_usable() {
        use std::io::Write;

        fn fake_redis(listener: std::net::TcpListener) {
            let mut stream = listener.incoming().next().unwrap().unwrap();

            let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());

            // handle initial handshake if sent

            let mut pipeline = redis::pipe();
            pipeline
                .cmd("CLIENT")
                .arg("SETINFO")
                .arg("LIB-NAME")
                .arg("redis-rs");
            pipeline
                .cmd("CLIENT")
                .arg("SETINFO")
                .arg("LIB-VER")
                .arg(env!("CARGO_PKG_VERSION"));
            let expected_length = pipeline.get_packed_pipeline().len();
            let mut buf = vec![0; expected_length];
            reader.read_exact(&mut buf).unwrap();
            stream.write_all(b"$2\r\nOK\r\n$2\r\nOK\r\n").unwrap();

            // reply with canned responses to known requests.
            loop {
                let expected_length = cmd("GET").arg("key1").get_packed_command().len();
                let mut buf = vec![0; expected_length];
                reader.read_exact(&mut buf).unwrap();

                if buf == cmd("GET").arg("key1").get_packed_command() {
                    // split the response so half is sent before the timeout
                    stream.write_all(b"$4\r\nk").unwrap();
                    sleep(Duration::from_millis(100));
                    stream.write_all(b"ey1\r\n").unwrap();
                } else if buf == cmd("GET").arg("key2").get_packed_command() {
                    stream.write_all(b"$4\r\nkey2\r\n").unwrap();
                    return;
                } else {
                    panic!("Invalid command {}", String::from_utf8(buf).unwrap());
                }
            }
        }

        let listener = get_listener_on_free_port();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || fake_redis(listener));

        let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();
        let mut con = client.get_connection().unwrap();
        con.set_read_timeout(Some(Duration::from_millis(10)))
            .unwrap();

        let res = con.get("key1");
        assert!(res.unwrap_err().is_timeout());
        assert!(con.is_open());

        sleep(Duration::from_millis(100));

        let value = con.get("key2").unwrap().unwrap();
        assert_eq!(value, "key2");
    }

    #[test]
    fn test_blocking_sorted_set_api() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        // setup version & input data followed by assertions that take into account Redis version
        // BZPOPMIN & BZPOPMAX are available from Redis version 5.0.0
        // BZMPOP is available from Redis version 7.0.0

        let redis_version = ctx.get_version();
        assert!(redis_version.0 >= 5);

        assert!(con.zadd("a", "1a", 1).is_ok());
        assert!(con.zadd("b", "2b", 2).is_ok());
        assert!(con.zadd("c", "3c", 3).is_ok());
        assert!(con.zadd("d", "4d", 4).is_ok());
        assert!(con.zadd("a", "5a", 5).is_ok());
        assert!(con.zadd("b", "6b", 6).is_ok());
        assert!(con.zadd("c", "7c", 7).is_ok());
        assert!(con.zadd("d", "8d", 8).is_ok());

        let min = con.bzpopmin("b", 0.0);
        let max = con.bzpopmax("b", 0.0);

        assert_eq!(
            min.unwrap().unwrap(),
            (String::from("b"), String::from("2b"), 2.0)
        );
        assert_eq!(
            max.unwrap().unwrap(),
            (String::from("b"), String::from("6b"), 6.0)
        );

        if redis_version.0 >= 7 {
            let min = con.bzmpop_min(0.0, vec!["a", "b", "c", "d"].as_slice(), 1);
            let max = con.bzmpop_max(0.0, vec!["a", "b", "c", "d"].as_slice(), 1);

            assert_eq!(min.unwrap().unwrap().1[0], (String::from("1a"), 1.0));
            assert_eq!(max.unwrap().unwrap().1[0], (String::from("5a"), 5.0));
        }
    }

    #[test]
    fn test_sorted_set_add_options() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();

        // Scenario 1
        // Add only, no update
        let options = SortedSetAddOptions::add_only();
        assert_eq!(con.zadd_options("1", "1a", 1, &options), Ok(1));
        assert_eq!(con.zscore("1", "1a"), Ok(Some(1.0)));

        assert_eq!(con.zadd_options("1", "1a", 100, &options), Ok(0));
        assert_eq!(con.zscore("1", "1a"), Ok(Some(1.0)));

        // Scenario 2
        // Update only, no add
        let options = SortedSetAddOptions::update_only(None);
        assert_eq!(con.zadd_options("1", "1a", 5, &options), Ok(0));
        assert_eq!(con.zscore("1", "1a"), Ok(Some(5.0)));

        assert_eq!(con.zadd_options("1", "1b", 5, &options), Ok(0));
        assert_eq!(con.zscore("1", "1b"), Ok(None));

        let options = options.include_changed_count();

        assert_eq!(con.zadd_options("1", "1a", 6, &options), Ok(1));
        assert_eq!(con.zscore("1", "1a"), Ok(Some(6.0)));

        assert_eq!(con.zadd_options("1", "1b", 5, &options), Ok(0));
        assert_eq!(con.zscore("1", "1b"), Ok(None));

        // Scenario 3
        // Update only if less
        let options = SortedSetAddOptions::update_only(Some(UpdateCheck::LT));
        assert_eq!(con.zadd_options("1", "1a", 10, &options), Ok(0));
        assert_eq!(con.zscore("1", "1a"), Ok(Some(6.0)));

        assert_eq!(con.zadd_options("1", "1a", 1, &options), Ok(0));
        assert_eq!(con.zscore("1", "1a"), Ok(Some(1.0)));

        // Scenario 4
        // Update only if greater
        let options = SortedSetAddOptions::update_only(Some(UpdateCheck::GT));
        assert_eq!(con.zadd_options("1", "1a", 0, &options), Ok(0));
        assert_eq!(con.zscore("1", "1a"), Ok(Some(1.0)));

        assert_eq!(con.zadd_options("1", "1a", 5, &options), Ok(0));
        assert_eq!(con.zscore("1", "1a"), Ok(Some(5.0)));

        // Scenario 5
        // Add or Update
        let options = SortedSetAddOptions::add_or_update(None);
        assert_eq!(con.zscore("5", "5a"), Ok(None));
        assert_eq!(con.zadd_options("5", "5a", 5, &options), Ok(1));
        assert_eq!(con.zscore("5", "5a"), Ok(Some(5.0)));

        assert_eq!(con.zadd_options("5", "5a", 10, &options), Ok(0));
        assert_eq!(con.zscore("5", "5a"), Ok(Some(10.0)));

        let options = options.include_changed_count();
        assert_eq!(con.zadd_options("5", "5a", 5, &options), Ok(1));
        assert_eq!(con.zscore("5", "5a"), Ok(Some(5.0)));

        // Scenario 6
        // Increment
        let options = SortedSetAddOptions::default()
            .increment_score()
            .include_changed_count();
        assert_eq!(con.zadd_options("6", "6a", 6, &options), Ok(6));
        assert_eq!(con.zscore("6", "6a"), Ok(Some(6.0)));

        assert_eq!(con.zadd_options("6", "6a", 6, &options), Ok(12));
        assert_eq!(con.zscore("6", "6a"), Ok(Some(12.0)));
    }

    #[test]
    #[cfg(feature = "vector-sets")]
    fn test_vector_sets_basic_operations() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
        let mut con = ctx.connection();

        let key = "test_points";

        let points_data: Vec<(&'static str, [f64; 2])> = vec![
            ("pt:A", [1.0, 1.0]),
            ("pt:B", [-1.0, -1.0]),
            ("pt:C", [-1.0, 1.0]),
            ("pt:D", [1.0, -1.0]),
            ("pt:E", [1.0, 0.0]),
        ];

        // Add the point vectors to a set.
        for (name, coordinates) in &points_data {
            assert_eq!(
                con.vadd(
                    key,
                    VectorAddInput::Values(EmbeddingInput::Float64(coordinates)),
                    name
                ),
                Ok(true)
            );

            // Test 1: Verify the embeddings of the point are correct
            let current_point_embeddings: Vec<f64> = con.vemb(key, name).unwrap();

            for (idx, current_point_embedding) in current_point_embeddings.iter().enumerate() {
                // Note: The values will typically not be exactly the same as those supplied when the vector was added,
                // as quantization is applied to improve performance. This is why an approximate equality check is used.
                assert_approx_eq!(current_point_embedding, coordinates[idx], 0.001);
            }
        }

        // Test 2: Verify that the vector set contains the expected number of vectors
        // and that all of them share the correct dimensionality.
        assert_eq!(con.vcard(key), Ok(points_data.len()));
        assert_eq!(con.vdim(key), Ok(points_data[0].1.len()));

        // Test 3: Adding and removing a point
        let point_to_remove = "pt:F";

        // Specifying the coordinates of the point as an array of strings is valid.
        assert_eq!(
            con.vadd(
                key,
                VectorAddInput::Values(EmbeddingInput::String(&["0", "0"])),
                point_to_remove
            ),
            Ok(true)
        );
        assert_eq!(con.vcard(key), Ok(points_data.len() + 1));
        assert_eq!(con.vrem(key, point_to_remove), Ok(true));
        assert_eq!(con.vcard(key), Ok(points_data.len()));

        // Test 4: Setting attributes to a point and retrieving them
        let attributes_target_point = "pt:A";
        let point_attributes = json!({
            "name": "Point A",
            "description": "First point added"
        });

        assert_eq!(
            con.vsetattr(key, attributes_target_point, &point_attributes),
            Ok(true)
        );

        assert_eq!(
            con.vgetattr(key, attributes_target_point),
            Ok(Some(point_attributes.to_string()))
        );

        // Test 5: Deleting attributes from a point
        assert_eq!(
            con.vsetattr(key, attributes_target_point, &String::new()),
            Ok(true)
        );

        assert_eq!(con.vgetattr(key, attributes_target_point), Ok(None));
    }

    #[test]
    #[cfg(feature = "vector-sets")]
    fn test_vector_sets_similarity_search() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
        let mut con = ctx.connection();

        let key = "test_points_for_similarity_search";

        let points_data: Vec<(&'static str, [f64; 2], serde_json::Value)> = vec![
            (
                "pt:A",
                [1.0, 1.0],
                json!({ "size": "large", "price": 18.99 }),
            ),
            (
                "pt:B",
                [-1.0, -1.0],
                json!({ "size": "large", "price": 35.99 }),
            ),
            (
                "pt:C",
                [-1.0, 1.0],
                json!({ "size": "large", "price": 25.99 }),
            ),
            (
                "pt:D",
                [1.0, -1.0],
                json!({ "size": "small", "price": 21.00 }),
            ),
            (
                "pt:E",
                [1.0, 0.0],
                json!({ "size": "small", "price": 17.75 }),
            ),
        ];

        let point_of_interest = "pt:A";

        // Add the point vectors to a set.
        for (name, coordinates, attributes) in &points_data {
            let opts = VAddOptions::default().set_attributes(attributes.clone());
            assert_eq!(
                con.vadd_options(
                    key,
                    VectorAddInput::Values(EmbeddingInput::Float64(coordinates)),
                    name,
                    &opts
                ),
                Ok(true)
            );
        }

        // Test 1: Search using Element variant (ELE)
        let element_search_results: Value = con
            .vsim(key, VectorSimilaritySearchInput::Element(point_of_interest))
            .unwrap();
        if let Value::Array(results_array) = &element_search_results {
            assert_eq!(
                results_array[0],
                Value::BulkString(point_of_interest.as_bytes().to_vec())
            );
        } else {
            panic!("Expected array result from VSIM, got {element_search_results:?}");
        }

        // Test 2: Search using the FP32 and VALUES variants
        let query_vector_f32 = vec![0.9f32, 0.1f32];
        let query_vector_f64 = vec![0.9, 0.1];
        let query_vector_strings = vec!["0.9", "0.1"];

        // FP32 variant
        let fp32_search_results: Value = con
            .vsim(key, VectorSimilaritySearchInput::Fp32(&query_vector_f32))
            .unwrap();
        // Values variant with f32
        let f32_search_results: Value = con
            .vsim(
                key,
                VectorSimilaritySearchInput::Values(EmbeddingInput::Float32(&query_vector_f32)),
            )
            .unwrap();
        // Values variant with f64
        let f64_search_results: Value = con
            .vsim(
                key,
                VectorSimilaritySearchInput::Values(EmbeddingInput::Float64(&query_vector_f64)),
            )
            .unwrap();
        // Values variant with strings
        let strings_search_results: Value = con
            .vsim(
                key,
                VectorSimilaritySearchInput::Values(EmbeddingInput::String(&query_vector_strings)),
            )
            .unwrap();

        assert_eq!(fp32_search_results, f32_search_results);
        assert_eq!(f32_search_results, f64_search_results);
        assert_eq!(f64_search_results, strings_search_results);

        // Test 3: Search using the VSIM options to perform a similarity search with scores and a limited number of results
        let elements_count = 4;
        let opts = VSimOptions::default()
            .set_with_scores(true)
            .set_count(elements_count);

        let element_search_results: Value = con
            .vsim_options(
                key,
                VectorSimilaritySearchInput::Element(point_of_interest),
                &opts,
            )
            .unwrap();

        // The WITHSCORES option changes the output format of the response, based on the version of the RESP protocol in use.
        match &element_search_results {
            Value::Array(results_array) => {
                assert_eq!(results_array.len(), elements_count * 2);
                // Expect an exact match with the point of interest.
                assert_eq!(
                    results_array[0],
                    Value::BulkString(point_of_interest.as_bytes().to_vec())
                );
                assert_eq!(results_array[1], Value::BulkString("1".as_bytes().to_vec()));
            }
            Value::Map(results_map) => {
                assert_eq!(results_map.len(), elements_count);
                // Find the point of interest.
                let point_key = Value::BulkString(point_of_interest.as_bytes().to_vec());
                let score =
                    results_map
                        .iter()
                        .find_map(|(k, v)| if k == &point_key { Some(v) } else { None });

                assert!(
                    score.is_some(),
                    "Point of interest not found in results map"
                );

                // Verify the point of interest is present with score 1.0.
                // In RESP3, the score would be a Double (1.0) instead of a BulkString.
                match score.unwrap() {
                    Value::Double(val) => assert_approx_eq!(*val, 1.0, 0.001),
                    other => panic!("Unexpected score format: {other:?}"),
                }
            }
            other => {
                panic!("Expected array or map result from VSIM, got {other:?}");
            }
        }

        // Test 4: Search using the VSIM options to perform a similarity search with a filter
        let opts = VSimOptions::default().set_filter_expression(".size == \"large\"");
        let element_search_results: Value = con
            .vsim_options(
                key,
                VectorSimilaritySearchInput::Element(point_of_interest),
                &opts,
            )
            .unwrap();

        if let Value::Array(results_array) = &element_search_results {
            assert_eq!(results_array.len(), 3);
            assert_eq!(
                results_array[0],
                Value::BulkString("pt:A".as_bytes().to_vec())
            );
            assert_eq!(
                results_array[1],
                Value::BulkString("pt:C".as_bytes().to_vec())
            );
            assert_eq!(
                results_array[2],
                Value::BulkString("pt:B".as_bytes().to_vec())
            );
        } else {
            panic!("Expected array result from VSIM, got {element_search_results:?}");
        }

        let opts =
            VSimOptions::default().set_filter_expression(".size == \"large\" && .price > 20.00");
        let element_search_results: Value = con
            .vsim_options(
                key,
                VectorSimilaritySearchInput::Element(point_of_interest),
                &opts,
            )
            .unwrap();
        if let Value::Array(results_array) = &element_search_results {
            assert_eq!(results_array.len(), 2);
            assert_eq!(
                results_array[0],
                Value::BulkString("pt:C".as_bytes().to_vec())
            );
            assert_eq!(
                results_array[1],
                Value::BulkString("pt:B".as_bytes().to_vec())
            );
        } else {
            panic!("Expected array result from VSIM, got {element_search_results:?}");
        }
    }

    #[test]
    #[cfg(feature = "vector-sets")]
    fn test_vector_sets_auxiliary_commands() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
        let mut con = ctx.connection();

        let key = "test_points_for_auxiliary_commands";
        let non_existent_key = "non_existent_key";

        let points_data: Vec<(&'static str, [f32; 2])> = vec![
            ("pt:A", [1.0f32, 1.0]),
            ("pt:B", [-1.0f32, -1.0]),
            ("pt:C", [-1.0f32, 1.0]),
            ("pt:D", [1.0f32, -1.0]),
            ("pt:E", [1.0f32, 0.0]),
        ];

        let point_of_interest = "pt:A";
        let point_attributes = json!({
            "name": "Point A",
            "description": "First point added"
        });

        let reduction_dimension = 3;
        let quantization = VectorQuantization::Q8;
        let (expected_embedding_response_length, expected_embedding_response_quantization) =
            match quantization {
                VectorQuantization::NoQuant => (3, "f32"),
                VectorQuantization::Q8 => (4, "int8"),
                VectorQuantization::Bin => (3, "bin"),
                _ => panic!("unknown quantization {quantization:?}"),
            };
        let max_number_of_links = 4;

        // Add the point vectors to a set using the extended version of the VADD command with the FP32 variant.
        for (name, coordinates) in &points_data {
            let mut opts = VAddOptions::default()
                .set_reduction_dimension(reduction_dimension)
                .set_check_and_set_style(true)
                .set_quantization(quantization)
                .set_build_exploration_factor(300)
                .set_max_number_of_links(max_number_of_links);

            // Add attributes to the point of interest.
            if name == &point_of_interest {
                opts = opts.set_attributes(point_attributes.clone());
            }

            assert_eq!(
                con.vadd_options(key, VectorAddInput::Fp32(coordinates), name, &opts),
                Ok(true)
            );

            let current_point_embeddings: Vec<f32> = con.vemb(key, name).unwrap();
            // Check that the embeddings have been reduced to the specified dimension.
            assert_eq!(current_point_embeddings.len(), reduction_dimension);

            let current_point_embeddings_raw: Value = con
                .vemb_options(
                    key,
                    name,
                    &VEmbOptions::default().set_raw_representation(true),
                )
                .unwrap();

            if let Value::Array(embeddings_array) = &current_point_embeddings_raw {
                assert_eq!(embeddings_array.len(), expected_embedding_response_length);
                assert_eq!(
                    embeddings_array[0],
                    Value::SimpleString(expected_embedding_response_quantization.to_string())
                );
            } else {
                panic!("Expected array result from VEMB RAW, got {current_point_embeddings_raw:?}");
            }
        }

        // VINFO testing section
        let vector_set_information = con.vinfo(key).unwrap().unwrap();
        // The response is structured as a list of key-value pairs.
        // Extract some properties from the response and validate that they match the expected values.
        assert_eq!(
            vector_set_information.get("quant-type"),
            Some(&Value::SimpleString(
                expected_embedding_response_quantization.to_string()
            ))
        );
        assert_eq!(
            vector_set_information.get("hnsw-m"),
            Some(&Value::Int(max_number_of_links as i64))
        );
        assert_eq!(
            vector_set_information.get("vector-dim"),
            Some(&Value::Int(reduction_dimension as i64))
        );
        assert_eq!(
            vector_set_information.get("size"),
            Some(&Value::Int(points_data.len() as i64))
        );
        assert_eq!(
            vector_set_information.get("attributes-count"),
            Some(&Value::Int(1))
        );
        // VINFO returns NIL for non-existent keys, which is represented as None.
        assert_eq!(con.vinfo(non_existent_key), Ok(None));

        // VLINKS testing section
        let links: Value = con.vlinks(key, point_of_interest).unwrap();
        println!("vlinks: {links:?}\n");
        if let Value::Array(layers) = &links {
            assert!(
                !layers.is_empty(),
                "[VLINKS] Should return at least one layer"
            );

            for (i, layer) in layers.iter().enumerate() {
                println!("Layer {i}: {layer:?}");
                assert!(matches!(layer, Value::Array(_)),
                    "[VLINKS] Expected an array result representing the links in layer {i}, got {layer:?}");
            }
        } else {
            panic!("[VLINKS] Expected an array result representing the layers, got {links:?}");
        }

        let links_with_scores: Value = con.vlinks_with_scores(key, point_of_interest).unwrap();
        println!("vlinks_with_scores: {links_with_scores:?}\n");
        if let Value::Array(layers_with_scores) = &links_with_scores {
            assert!(
                !layers_with_scores.is_empty(),
                "[VLINKS WITH SCORES] Should return at least one layer"
            );

            if let Value::Array(layers_without_scores) = &links {
                assert_eq!(
                    layers_with_scores.len(),
                    layers_without_scores.len(),
                    "[VLINKS WITH SCORES] Should return the same number of layers as [VLINKS]"
                );

                for (i, (layer_with_scores, layer_without_scores)) in layers_with_scores
                    .iter()
                    .zip(layers_without_scores.iter())
                    .enumerate()
                {
                    assert!(matches!(layer_with_scores, Value::Array(_) | Value::Map(_)),
                        "[VLINKS WITH SCORES] Expected an array or map result representing the links in layer {i} along with their scores, got {layer_with_scores:?}");

                    println!("Layer {i} without scores: {layer_without_scores:?}");
                    println!("Layer {i} with scores: {layer_with_scores:?}");

                    // Layers must have twice as many elements than normal when scores are included.
                    match (layer_with_scores, layer_without_scores) {
                        (
                            Value::Array(connections_with_scores),
                            Value::Array(connections_without_scores),
                        ) => {
                            assert_eq!(
                                connections_with_scores.len(),
                                connections_without_scores.len() * 2,
                                "[VLINKS WITH SCORES] Layer {i} must have twice as many elements when returning scores"
                            );
                        }
                        (
                            Value::Map(connections_with_scores),
                            Value::Array(connections_without_scores),
                        ) => {
                            assert_eq!(
                                connections_with_scores.len(),
                                connections_without_scores.len(),
                                "[VLINKS WITH SCORES] Layer {i} must have the same number of entries in map format when returning scores"
                            );
                        }
                        _ => {
                            panic!("[VLINKS WITH SCORES] Unexpected format combination for layer {i}: {layer_with_scores:?} and {layer_without_scores:?}");
                        }
                    }
                }
            }
        } else {
            panic!("[VLINKS WITH SCORES] Expected an array result representing the layers, got {links_with_scores:?}");
        }

        // VRANDMEMBER testing section
        let point_names: HashSet<String> = points_data
            .iter()
            .map(|(name, _)| name.to_string())
            .collect();

        let random_member: Option<String> = con.vrandmember(key).unwrap();
        assert!(point_names.contains(&random_member.unwrap()));

        // When called with a positive count, returns up to that many distinct elements.
        let random_members: Vec<String> = con
            .vrandmember_multiple(key, points_data.len() / 2)
            .unwrap();
        assert_eq!(random_members.len(), points_data.len() / 2);

        let all_points_present = random_members.iter().all(|name| point_names.contains(name));
        assert!(all_points_present);

        // When the count exceeds the number of elements, the entire set is returned.
        let random_members: Vec<String> = con
            .vrandmember_multiple(key, points_data.len() + 1)
            .unwrap();
        assert_eq!(random_members.len(), points_data.len());
        let all_points_present = random_members.iter().all(|name| point_names.contains(name));
        assert!(all_points_present);

        // When the key does not exist, the command returns NIL if no count is given, or an empty array if a count is provided.
        assert_eq!(con.vrandmember(non_existent_key), Ok(None));
        assert_eq!(
            con.vrandmember_multiple(non_existent_key, 1),
            Ok(Vec::<String>::new())
        );

        // VDELATTR testing section
        assert_eq!(
            con.vgetattr(key, point_of_interest),
            Ok(Some(point_attributes.to_string()))
        );
        // Remove the attributes from the point of interest using the VDELATTR utility command.
        assert_eq!(con.vdelattr(key, point_of_interest), Ok(true));
        assert_eq!(con.vgetattr(key, point_of_interest), Ok(None));
    }

    #[test]
    #[cfg(feature = "vector-sets")]
    fn test_vector_sets_edge_cases() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_0);
        let mut con = ctx.connection();

        let non_existent_key = "non_existent_key";
        let non_existent_element = "non_existent_element";

        // VCARD returns 0 for non-existent keys.
        assert_eq!(con.vcard(non_existent_key), Ok(0));

        // VDIM returns an error for non-existent keys.
        let result = con.vdim(non_existent_key);
        assert!(result.is_err(), "Expected an error for non-existent key");
        let error = result.unwrap_err();
        assert_eq!(error.kind(), redis::ServerErrorKind::ResponseError.into());
        assert!(
            error.to_string().contains("key does not exist"),
            "Expected error message = 'key does not exist', got: {error}"
        );

        let key = "test_points_for_edge_cases";

        let points_data: Vec<(&'static str, [f64; 2])> = vec![
            ("pt:A", [1.0, 1.0]),
            ("pt:B", [-1.0, -1.0]),
            ("pt:C", [-1.0, 1.0]),
            ("pt:D", [1.0, -1.0]),
            ("pt:E", [1.0, 0.0]),
        ];

        let point_of_interest = "pt:A";

        for (name, coordinates) in &points_data {
            assert_eq!(
                con.vadd(
                    key,
                    VectorAddInput::Values(EmbeddingInput::Float64(coordinates)),
                    name
                ),
                Ok(true)
            );
        }

        // VADD returns an error when it fails.
        // Force the dimensionality mismatch by adding a 3D vector.
        let result = con.vadd(
            key,
            VectorAddInput::Values(EmbeddingInput::Float32(&[1.0, 1.5, 2.0])),
            "pt:F",
        );
        assert!(
            result.is_err(),
            "Expected an error for dimensionality mismatch"
        );
        let error = result.unwrap_err();
        assert_eq!(error.kind(), redis::ServerErrorKind::ResponseError.into());

        // VEMB returns NIL for non-existent keys or elements.
        assert_eq!(
            con.vemb(non_existent_key, point_of_interest),
            Ok(Value::Nil)
        );
        assert_eq!(con.vemb(key, non_existent_element), Ok(Value::Nil));

        // VREM returns false for non-existent keys or elements.
        assert_eq!(con.vrem(non_existent_key, point_of_interest), Ok(false));
        assert_eq!(con.vrem(key, non_existent_element), Ok(false));

        // VSETATTR returns false when setting attributes to non-existent keys or elements.
        let sample_attribute = json!({"name": "Sample attribute"});
        assert_eq!(
            con.vsetattr(non_existent_key, point_of_interest, &sample_attribute),
            Ok(false)
        );
        assert_eq!(
            con.vsetattr(key, non_existent_element, &sample_attribute),
            Ok(false)
        );

        // VGETATTR returns false when attempting to get attributes from non-existent keys or elements.
        assert_eq!(con.vgetattr(non_existent_key, point_of_interest), Ok(None));
        assert_eq!(con.vgetattr(key, non_existent_element), Ok(None));

        // VLINKS returns NIL for non-existent keys or elements.
        assert_eq!(
            con.vlinks(non_existent_key, point_of_interest),
            Ok(Value::Nil)
        );
        assert_eq!(con.vlinks(key, non_existent_element), Ok(Value::Nil));

        // VRANDMEMBER with count 0 returns an empty array (vector).
        assert_eq!(
            con.vrandmember_multiple(non_existent_key, 0),
            Ok(Vec::<String>::new())
        );

        // VSIM returns an empty array for similarity searches with non-existent keys.
        let search_results: Value = con
            .vsim(
                non_existent_key,
                VectorSimilaritySearchInput::Values(EmbeddingInput::Float64(&points_data[0].1)),
            )
            .unwrap();
        assert_eq!(search_results, Value::Array(Vec::<Value>::new()));

        let search_results: Value = con
            .vsim(
                non_existent_key,
                VectorSimilaritySearchInput::Element(point_of_interest),
            )
            .unwrap();
        assert_eq!(search_results, Value::Array(Vec::<Value>::new()));

        // VSIM returns an error for similarity searches with non-existent elements.
        let result: RedisResult<Value> = con.vsim(
            key,
            VectorSimilaritySearchInput::Element(non_existent_element),
        );
        assert!(
            result.is_err(),
            "Expected an error for non-existent element"
        );
        let error = result.unwrap_err();
        assert_eq!(error.kind(), redis::ServerErrorKind::ResponseError.into());
    }

    #[test]
    fn test_push_manager() {
        let ctx = TestContext::new();
        let redis = RedisConnectionInfo::default().set_protocol(ProtocolVersion::RESP3);
        let connection_info = ctx.server.connection_info().set_redis_settings(redis);

        let client = redis::Client::open(connection_info).unwrap();

        let mut con = client.get_connection().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        con.set_push_sender(tx);
        let _ = cmd("CLIENT")
            .arg("TRACKING")
            .arg("ON")
            .exec(&mut con)
            .unwrap();
        let pipe = build_simple_pipeline_for_invalidation();
        for _ in 0..10 {
            let _: RedisResult<()> = pipe.query(&mut con);
            con.get_int("key_1").unwrap();
            let PushInfo { kind, data } = rx.try_recv().unwrap();
            assert_eq!(
                (
                    PushKind::Invalidate,
                    vec![Value::Array(vec![Value::BulkString(
                        "key_1".as_bytes().to_vec()
                    )])]
                ),
                (kind, data)
            );
        }
        let (new_tx, new_rx) = std::sync::mpsc::channel();
        con.set_push_sender(new_tx.clone());
        drop(rx);
        let _: RedisResult<()> = pipe.query(&mut con);
        con.get_int("key_1").unwrap();
        let PushInfo { kind, data } = new_rx.try_recv().unwrap();
        assert_eq!(
            (
                PushKind::Invalidate,
                vec![Value::Array(vec![Value::BulkString(
                    "key_1".as_bytes().to_vec()
                )])]
            ),
            (kind, data)
        );

        {
            drop(new_rx);
            for _ in 0..10 {
                let _: RedisResult<()> = pipe.query(&mut con);
                let v = con.get_int("key_1").unwrap();
                assert_eq!(v, Some(42));
            }
        }
    }

    #[test]
    fn test_push_manager_disconnection() {
        let ctx = TestContext::new();
        let redis = RedisConnectionInfo::default().set_protocol(ProtocolVersion::RESP3);
        let connection_info = ctx.server.connection_info().set_redis_settings(redis);
        let client = redis::Client::open(connection_info).unwrap();

        let mut con = client.get_connection().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        con.set_push_sender(tx.clone());

        let _: () = con.set("A", "1").unwrap();
        assert_eq!(
            rx.try_recv().unwrap_err(),
            std::sync::mpsc::TryRecvError::Empty
        );
        drop(ctx);
        let x: RedisResult<()> = con.set("A", "1");
        assert!(x.is_err());
        assert_eq!(rx.try_recv().unwrap().kind, PushKind::Disconnection);
    }

    #[test]
    fn test_raw_pubsub_with_push_manager() {
        // Tests PubSub usage with raw connection.
        let ctx = TestContext::new();
        if !ctx.protocol.supports_resp3() {
            return;
        }
        let mut con = ctx.connection();

        let (tx, rx) = std::sync::mpsc::channel();
        let mut pubsub_con = ctx.connection();
        pubsub_con.set_push_sender(tx);

        pubsub_con.subscribe_resp3("foo").unwrap();
        pubsub_con.psubscribe_resp3("bar*").unwrap();

        // We are using different redis connection to send PubSub message but it's okay to re-use the same connection.
        redis::cmd("PUBLISH")
            .arg("foo")
            .arg(42)
            .exec(&mut con)
            .unwrap();
        // We can also call the command directly
        assert_eq!(con.publish("barvaz", 23), Ok(1));

        // In sync connection it can't receive push messages from socket without requesting some command
        redis::cmd("PING").exec(&mut pubsub_con).unwrap();

        // we don't assume any order on the received messages, so we first receive them and them check that they exist regardless of order
        let mut messages = vec![];
        for _ in 0..4 {
            let PushInfo { kind, data } = rx.try_recv().unwrap();
            messages.push((kind, data));
        }
        assert!(
            messages.contains(&(
                PushKind::Subscribe,
                vec![Value::BulkString("foo".as_bytes().to_vec()), Value::Int(1)]
            )),
            "{messages:?}"
        );
        assert!(
            messages.contains(&(
                PushKind::PSubscribe,
                vec![Value::BulkString("bar*".as_bytes().to_vec()), Value::Int(2)]
            )),
            "{messages:?}"
        );
        assert!(
            messages.contains(&(
                PushKind::Message,
                vec![
                    Value::BulkString("foo".as_bytes().to_vec()),
                    Value::BulkString("42".as_bytes().to_vec())
                ]
            )),
            "{messages:?}"
        );
        assert!(
            messages.contains(&(
                PushKind::PMessage,
                vec![
                    Value::BulkString("bar*".as_bytes().to_vec()),
                    Value::BulkString("barvaz".as_bytes().to_vec()),
                    Value::BulkString("23".as_bytes().to_vec())
                ]
            )),
            "{messages:?}"
        );

        pubsub_con.unsubscribe_resp3("foo").unwrap();
        pubsub_con.punsubscribe_resp3("bar*").unwrap();
        pubsub_con.ping().unwrap();

        assert_eq!(con.publish("foo", 23), Ok(0));
        assert_eq!(con.publish("barvaz", 42), Ok(0));

        // We have received verification from Redis that it's unsubscribed to channel.
        let PushInfo { kind, data } = rx.try_recv().unwrap();
        assert_eq!(
            (
                PushKind::Unsubscribe,
                vec![Value::BulkString("foo".as_bytes().to_vec()), Value::Int(1)]
            ),
            (kind, data)
        );
        let PushInfo { kind, data } = rx.try_recv().unwrap();
        assert_eq!(
            (
                PushKind::PUnsubscribe,
                vec![Value::BulkString("bar*".as_bytes().to_vec()), Value::Int(0)]
            ),
            (kind, data)
        );

        // check that no additional message was sent.
        rx.try_recv().unwrap_err();
    }

    #[test]
    fn test_select_db() {
        let ctx = TestContext::new();
        let redis = redis_settings().set_db(5);
        let connection_info = ctx.server.connection_info().set_redis_settings(redis);

        let client = redis::Client::open(connection_info).unwrap();
        let mut connection = client.get_connection().unwrap();
        let info: String = redis::cmd("CLIENT")
            .arg("info")
            .query(&mut connection)
            .unwrap();
        assert!(info.contains("db=5"));
    }

    #[test]
    fn test_client_getname() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();
        redis::cmd("CLIENT")
            .arg("SETNAME")
            .arg("connection-name")
            .exec(&mut con)
            .unwrap();
        let res: String = con.client_getname().unwrap().unwrap();
        assert_eq!(res, "connection-name");
    }

    #[test]
    fn test_client_id() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();
        let _num = con.client_id().unwrap();
    }

    #[test]
    fn test_client_setname() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();
        assert_eq!(con.client_setname("connection-name"), Ok(()));
        let res: String = redis::cmd("CLIENT").arg("GETNAME").query(&mut con).unwrap();
        assert_eq!(res, "connection-name");
    }

    #[test]
    fn test_role_primary() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();
        let ret = redis::cmd("ROLE").query::<Role>(&mut con).unwrap();
        assert!(matches!(ret, Role::Primary { .. }));
    }

    #[test]
    fn test_connection_timeout_on_busy_server() {
        let ctx = TestContext::new();
        let mut con = ctx.connection();
        // we stop the server on another thread, in order to move forwardd while the server is stopped.
        let handle = thread::spawn(move || {
            cmd("DEBUG")
                .arg("SLEEP")
                .arg("0.1")
                .query::<()>(&mut con)
                .unwrap();
        });

        // we wait, to ensure that the debug sleep has started.
        thread::sleep(Duration::from_millis(5));
        // we set a DB, in order to force calling requests on the server.
        let redis = redis_settings().set_db(1);
        let connection_info = ctx.server.connection_info().set_redis_settings(redis);

        let client = Client::open(connection_info).unwrap();
        let try_connect = client.get_connection_with_timeout(Duration::from_millis(2));

        assert!(try_connect.is_err_and(|err| { err.is_timeout() }));

        handle.join().unwrap();
    }

    #[test]
    fn fail_on_empty_command() {
        let ctx = TestContext::new();
        let mut connection = ctx.connection();

        let error: RedisError = redis::Pipeline::new()
            .query::<String>(&mut connection)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Client);
        assert_eq!(error.to_string(), "empty command - Client");

        let error: RedisError = redis::Cmd::new()
            .query::<String>(&mut connection)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Client);
        assert_eq!(error.to_string(), "empty command - Client");
    }
}
