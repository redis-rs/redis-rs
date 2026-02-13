mod support;

#[cfg(test)]
mod basic_async {
    use std::{collections::HashMap, time::Duration};

    use crate::support::*;
    use assert_matches::assert_matches;
    use futures::{StreamExt, prelude::*};
    use futures_time::{future::FutureExt, task::sleep};
    #[cfg(feature = "json")]
    use redis::JsonAsyncCommands;
    use redis::{
        AsyncCommands, ErrorKind, IntoConnectionInfo, ParsingError, ProtocolVersion, PushKind,
        RedisConnectionInfo, RedisError, RedisResult, ScanOptions, ServerErrorKind, Value,
        aio::ConnectionLike, cmd, pipe,
    };
    #[cfg(feature = "json")]
    use redis_test::server::Module;
    use redis_test::server::{redis_settings, use_protocol};
    use rstest::rstest;
    use test_macros::async_test;
    use tokio::sync::mpsc::error::TryRecvError;

    #[rstest::rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    #[should_panic(expected = "Internal thread panicked")]
    fn test_block_on_all_panics_from_spawns(#[case] runtime: RuntimeType) {
        fn spawn<T>(fut: impl std::future::Future<Output = T> + Send + Sync + 'static)
        where
            T: Send + 'static,
        {
            match tokio::runtime::Handle::try_current() {
                Ok(tokio_runtime) => {
                    tokio_runtime.spawn(fut);
                }
                Err(_) => {
                    #[cfg(feature = "smol-comp")]
                    smol::spawn(fut).detach();
                    #[cfg(not(feature = "smol-comp"))]
                    unreachable!()
                }
            }
        }

        use std::sync::{Arc, atomic::AtomicBool};

        let slept = Arc::new(AtomicBool::new(false));
        let slept_clone = slept.clone();
        block_on_all(
            async {
                spawn(async move {
                    futures_time::task::sleep(futures_time::time::Duration::from_millis(1)).await;
                    slept_clone.store(true, std::sync::atomic::Ordering::Relaxed);
                    panic!("As it should");
                });

                loop {
                    futures_time::task::sleep(futures_time::time::Duration::from_millis(2)).await;
                    if slept.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
            },
            runtime,
        );
    }

    #[async_test]
    async fn args(mut con: impl ConnectionLike) {
        redis::cmd("SET")
            .arg("key1")
            .arg(b"foo")
            .exec_async(&mut con)
            .await
            .unwrap();
        redis::cmd("SET")
            .arg(&["key2", "bar"])
            .exec_async(&mut con)
            .await
            .unwrap();
        let result = redis::cmd("MGET")
            .arg(&["key1", "key2"])
            .query_async(&mut con)
            .await;
        assert_eq!(result, Ok(("foo".to_string(), b"bar".to_vec())));
    }

    #[async_test]
    async fn no_response_skips_response_even_on_error(mut con: impl ConnectionLike) {
        redis::cmd("SET")
            .arg("key")
            .arg(b"foo")
            .set_no_response(true)
            .exec_async(&mut con)
            .await
            .unwrap();

        // this should error, since we hset a string value, but we shouldn't receive the error, because we ignore the response
        redis::cmd("HSET")
            .arg("key")
            .arg(b"foo")
            .arg("bar")
            .set_no_response(true)
            .exec_async(&mut con)
            .await
            .unwrap();

        let result = redis::cmd("GET").arg("key").query_async(&mut con).await;
        assert_eq!(result, Ok("foo".to_string()));
    }

    #[cfg(feature = "tokio-comp")]
    #[tokio::test]
    async fn works_with_paused_time_when_no_timeouts_are_set() {
        use redis::AsyncConnectionConfig;
        tokio::time::pause();
        async fn test(mut conn: impl ConnectionLike) {
            // Force the Redis command to take enough time that we have to park the task.  If
            // any timeouts have been created, Tokio will then auto-advance the paused clock to
            // the timeout's expiry time, resulting in a "timed out" error.
            redis::cmd("EVAL")
                .arg(
                    r#"
                          local function now()
                             local t = redis.call("TIME")
                             return t[1] + 0.000001 * t[2]
                          end
                          local t = now() + 0.5
                          while now() < t do end
                        "#,
                )
                .arg(0)
                .exec_async(&mut conn)
                .await
                .unwrap();
        }
        let ctx = TestContext::new();

        let conn = ctx
            .client
            .get_multiplexed_async_connection_with_config(
                &AsyncConnectionConfig::new()
                    .set_connection_timeout(None)
                    .set_response_timeout(None),
            )
            .await
            .unwrap();
        test(conn).await;

        #[cfg(feature = "connection-manager")]
        {
            use redis::aio::ConnectionManagerConfig;

            let conn = ctx
                .client
                .get_connection_manager_with_config(
                    ConnectionManagerConfig::new()
                        .set_connection_timeout(None)
                        .set_response_timeout(None),
                )
                .await
                .unwrap();
            test(conn).await
        };
    }

    #[async_test]
    async fn can_authenticate_with_username_and_password() {
        let ctx = TestContext::new();
        let mut con = ctx.async_connection().await.unwrap();

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
        assert_eq!(con.req_packed_command(&set_user_cmd).await, Ok(Value::Okay));

        let redis = redis_settings()
            .set_username(username)
            .set_password(password);
        let connection_info = ctx.server.connection_info().set_redis_settings(redis);
        let mut conn = redis::Client::open(connection_info)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let result: String = cmd("ACL")
            .arg("whoami")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(result, username);
    }

    #[async_test]
    async fn nice_hash_api(mut connection: impl AsyncCommands) {
        assert_eq!(
            connection
                .hset_multiple("my_hash", &[("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8)])
                .await,
            Ok(())
        );

        let hm: HashMap<String, isize> = connection.hgetall("my_hash").await.unwrap();
        assert_eq!(hm.len(), 4);
        assert_eq!(hm.get("f1"), Some(&1));
        assert_eq!(hm.get("f2"), Some(&2));
        assert_eq!(hm.get("f3"), Some(&4));
        assert_eq!(hm.get("f4"), Some(&8));
    }

    #[async_test]
    async fn nice_hash_api_in_pipe(mut connection: impl AsyncCommands) {
        assert_eq!(
            connection
                .hset_multiple("my_hash", &[("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8)])
                .await,
            Ok(())
        );

        let mut pipe = redis::pipe();
        pipe.cmd("HGETALL").arg("my_hash");
        let mut vec: Vec<HashMap<String, isize>> = pipe.query_async(&mut connection).await.unwrap();
        assert_eq!(vec.len(), 1);
        let hash = vec.pop().unwrap();
        assert_eq!(hash.len(), 4);
        assert_eq!(hash.get("f1"), Some(&1));
        assert_eq!(hash.get("f2"), Some(&2));
        assert_eq!(hash.get("f3"), Some(&4));
        assert_eq!(hash.get("f4"), Some(&8));
    }

    #[async_test]
    async fn dont_panic_on_closed_multiplexed_connection() {
        let ctx = TestContext::new();
        let client = ctx.client.clone();
        let connect = client.get_multiplexed_async_connection();
        drop(ctx);

        connect
            .and_then(|con| async move {
                let cmd = move || {
                    let mut con = con.clone();
                    async move {
                        redis::cmd("SET")
                            .arg("key1")
                            .arg(b"foo")
                            .query_async(&mut con)
                            .await
                    }
                };
                let result: RedisResult<()> = cmd().await;
                assert_eq!(result.as_ref().unwrap_err().kind(), redis::ErrorKind::Io);
                cmd().await
            })
            .map(|result| {
                assert_eq!(result.as_ref().unwrap_err().kind(), redis::ErrorKind::Io);
            })
            .await;
    }

    #[async_test]
    async fn pipeline_transaction(mut con: impl ConnectionLike) {
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("SET")
            .arg("key_1")
            .arg(42)
            .ignore()
            .cmd("SET")
            .arg("key_2")
            .arg(43)
            .ignore()
            .cmd("MGET")
            .arg(&["key_1", "key_2"]);
        pipe.query_async(&mut con)
            .map_ok(|((k1, k2),): ((i32, i32),)| {
                assert_eq!(k1, 42);
                assert_eq!(k2, 43);
            })
            .await
            .unwrap();
    }

    #[async_test]
    async fn client_tracking_doesnt_block_execution(mut con: impl AsyncCommands) {
        //It checks if the library distinguish a push-type message from the others and continues its normal operation.

        let mut pipe = redis::pipe();
        pipe.cmd("CLIENT")
            .arg("TRACKING")
            .arg("ON")
            .ignore()
            .cmd("GET")
            .arg("key_1")
            .ignore()
            .cmd("SET")
            .arg("key_1")
            .arg(42)
            .ignore();
        let _: RedisResult<()> = pipe.query_async(&mut con).await;
        let num: i32 = con.get("key_1").await.unwrap();
        assert_eq!(num, 42);
    }

    #[async_test]
    async fn pipeline_transaction_with_errors(mut con: impl AsyncCommands) {
        con.set::<_, _, ()>("x", 42).await.unwrap();

        // Make Redis a replica of a nonexistent master, thereby making it read-only.
        redis::cmd("slaveof")
            .arg("1.1.1.1")
            .arg("1")
            .exec_async(&mut con)
            .await
            .unwrap();

        // Ensure that a write command fails with a READONLY error
        let err = redis::pipe()
            .atomic()
            .ping()
            .set("x", 142)
            .ignore()
            .get("x")
            .set("x", 142)
            .query_async::<()>(&mut con)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ServerErrorKind::ExecAbort.into());
        let errors = err.into_server_errors().unwrap();
        assert_eq!(errors.len(), 2);
        assert_eq!(errors[0].0, 1);
        assert_eq!(errors[0].1.kind(), ServerErrorKind::ReadOnly.into());
        assert_eq!(errors[1].0, 3);
        assert_eq!(errors[1].1.kind(), ServerErrorKind::ReadOnly.into());

        let x: i32 = con.get("x").await.unwrap();
        assert_eq!(x, 42);
    }

    #[async_test]
    #[cfg(feature = "json")]
    async fn module_json_and_pipeline_transaction_with_ignore_errors() {
        let ctx = TestContext::with_modules(&[Module::Json], false);
        let mut con = ctx.async_connection().await.unwrap();
        con.set::<_, _, ()>("x", 42).await.unwrap();
        con.json_set::<_, _, _, ()>("y", "$", &serde_json::json!({"path": "value"}))
            .await
            .unwrap();

        let mut pipeline = redis::pipe();
        pipeline
            .atomic()
            .ping()
            .set("x", 142)
            .ignore()
            .json_get("x", ".path")
            .unwrap()
            .ignore()
            .json_get("x", ".path")
            .unwrap()
            .json_get("y", ".path")
            .unwrap()
            .json_get("y", ".other")
            .unwrap()
            .get("x");

        type IgnoreErrorsResult = (
            RedisResult<Value>,
            RedisResult<Value>,
            RedisResult<Value>,
            RedisResult<Value>,
            RedisResult<Value>,
        );

        let result: IgnoreErrorsResult = pipeline
            .ignore_errors()
            .query_async(&mut con)
            .await
            .unwrap();

        assert_eq!(result.0.unwrap(), Value::SimpleString("PONG".to_string()));
        assert_eq!(
            result.2.unwrap(),
            Value::BulkString("\"value\"".as_bytes().to_vec())
        );
        assert_eq!(
            result.4.unwrap(),
            Value::BulkString("142".as_bytes().to_vec())
        );

        assert_eq!(result.1.unwrap_err().code(), Some("Existing"));
        assert_eq!(
            result.3.unwrap_err().kind(),
            ServerErrorKind::ResponseError.into()
        );
    }

    #[async_test]
    async fn pipeline_with_ignore_errors(mut con: impl AsyncCommands) {
        con.set::<_, _, ()>("x", 42).await.unwrap();

        let mut pipeline = redis::pipe();
        pipeline
            .ping()
            .set("x", 142)
            .ignore()
            .cmd("JSON.GET")
            .arg("x")
            .arg(".path")
            .get("x");

        let result: Vec<RedisResult<Value>> = pipeline
            .ignore_errors()
            .query_async(&mut con)
            .await
            .unwrap();

        assert_eq!(
            result[0].clone().unwrap(),
            Value::SimpleString("PONG".to_string())
        );
        assert_eq!(
            result[2].clone().unwrap(),
            Value::BulkString("142".as_bytes().to_vec())
        );

        assert_eq!(
            result[1].clone().unwrap_err().kind(),
            ServerErrorKind::ResponseError.into()
        );
    }

    #[async_test]
    async fn pipeline_returns_server_errors(mut con: impl AsyncCommands) {
        let mut pipe = redis::pipe();
        pipe.set("x", "x-value")
            .ignore()
            .hset("x", "field", "field_value")
            .ignore()
            .get("x");

        let res = pipe.exec_async(&mut con).await;
        let error_message = res.unwrap_err().to_string();
        assert_eq!(
            &error_message,
            "Pipeline failure: [(Index 1, error: \"WRONGTYPE\": Operation against a key holding the wrong kind of value)]"
        );
    }

    fn test_cmd(con: impl AsyncCommands + Clone, i: i32) -> impl Future<Output = ()> + Send {
        let mut con = con.clone();
        async move {
            let key = format!("key{i}");
            let key_2 = key.clone();
            let key2 = format!("key{i}_2");
            let key2_2 = key2.clone();

            let foo_val = format!("foo{i}");

            redis::cmd("SET")
                .arg(&key[..])
                .arg(foo_val.as_bytes())
                .exec_async(&mut con)
                .await
                .unwrap();
            redis::cmd("SET")
                .arg(&[&key2, "bar"])
                .exec_async(&mut con)
                .await
                .unwrap();
            redis::cmd("MGET")
                .arg(&[&key_2, &key2_2])
                .query_async(&mut con)
                .map(|result| {
                    assert_eq!(Ok((foo_val, b"bar".to_vec())), result);
                })
                .await;
        }
    }

    #[async_test]
    async fn pipe_over_multiplexed_connection(mut con: impl ConnectionLike) {
        let mut pipe = pipe();
        pipe.zrange("zset", 0, 0);
        pipe.zrange("zset", 0, 0);
        let frames = con.req_packed_commands(&pipe, 0, 2).await.unwrap();
        assert_eq!(frames.len(), 2);
        assert!(matches!(frames[0], redis::Value::Array(_)));
        assert!(matches!(frames[1], redis::Value::Array(_)));
    }

    #[async_test]
    async fn running_multiple_commands(con: impl AsyncCommands + Clone) {
        let cmds = (0..100).map(move |i| test_cmd(con.clone(), i));
        future::join_all(cmds).await;
    }

    #[async_test]
    async fn transaction_multiplexed_connection(con: impl ConnectionLike + Clone) {
        let cmds = (0..100).map(move |i| {
            let mut con = con.clone();
            async move {
                let foo_val = i;
                let bar_val = format!("bar{i}");

                let mut pipe = redis::pipe();
                pipe.atomic()
                    .cmd("SET")
                    .arg("key")
                    .arg(foo_val)
                    .ignore()
                    .cmd("SET")
                    .arg(&["key2", &bar_val[..]])
                    .ignore()
                    .cmd("MGET")
                    .arg(&["key", "key2"]);

                pipe.query_async(&mut con)
                    .map(move |result| {
                        assert_eq!(Ok(((foo_val, bar_val.into_bytes()),)), result);
                        result
                    })
                    .await
            }
        });
        future::try_join_all(cmds)
            .map_ok(|results| {
                assert_eq!(results.len(), 100);
            })
            .map_err(|err| panic!("{err}"))
            .await
            .unwrap();
    }

    #[async_test]
    async fn async_scanning(mut con: impl ConnectionLike + Send) {
        let mut unseen = std::collections::HashSet::new();

        for x in 0..1000 {
            redis::cmd("SADD")
                .arg("foo")
                .arg(x)
                .exec_async(&mut con)
                .await
                .unwrap();
            unseen.insert(x);
        }

        let mut iter = redis::cmd("SSCAN")
            .arg("foo")
            .cursor_arg(0)
            .clone()
            .iter_async(&mut con)
            .await
            .unwrap();

        while let Some(x) = iter.next_item().await {
            let x = x.unwrap();

            // if this assertion fails, too many items were returned by the iterator.
            assert!(unseen.remove(&x));
        }

        assert!(unseen.is_empty());
    }

    #[async_test]
    async fn async_scanning_iterative(mut con: impl ConnectionLike + Send) {
        let mut unseen = std::collections::HashSet::new();

        for x in 0..1000 {
            let key_name = format!("key.{x}");
            redis::cmd("SET")
                .arg(key_name.clone())
                .arg("foo")
                .exec_async(&mut con)
                .await
                .unwrap();
            unseen.insert(key_name.clone());
        }

        let mut iter = redis::cmd("SCAN")
            .cursor_arg(0)
            .arg("MATCH")
            .arg("key*")
            .arg("COUNT")
            .arg(1)
            .clone()
            .iter_async::<String>(&mut con)
            .await
            .unwrap();

        while let Some(item) = iter.next_item().await {
            let item = item.unwrap();

            // if this assertion fails, too many items were returned by the iterator.
            assert!(unseen.remove(&item));
        }

        assert!(unseen.is_empty());
    }

    #[async_test]
    async fn async_scanning_stream(mut con: impl ConnectionLike + Sync + Send) {
        let mut unseen = std::collections::HashSet::new();

        for x in 0..1000 {
            let key_name = format!("key.{x}");
            redis::cmd("SET")
                .arg(key_name.clone())
                .arg("foo")
                .exec_async(&mut con)
                .await
                .unwrap();
            unseen.insert(key_name.clone());
        }

        let mut iter = redis::cmd("SCAN")
            .cursor_arg(0)
            .arg("MATCH")
            .arg("key*")
            .arg("COUNT")
            .arg(1)
            .clone()
            .iter_async::<String>(&mut con)
            .await
            .unwrap();

        while let Some(item) = iter.next_item().await {
            let item = item.unwrap();

            // if this assertion fails, too many items were returned by the iterator.
            assert!(unseen.remove(&item));
        }

        assert!(unseen.is_empty());
    }

    #[async_test]
    async fn response_timeout_multiplexed_connection() {
        let ctx = TestContext::new();

        let mut connection = ctx.async_connection().await.unwrap();
        connection.set_response_timeout(Duration::from_millis(1));
        let mut cmd = redis::Cmd::new();
        cmd.arg("BLPOP").arg("foo").arg(0); // 0 timeout blocks indefinitely
        let result = connection.req_packed_command(&cmd).await;
        assert_matches!(result, Err(_));
        assert!(result.unwrap_err().is_timeout());
    }

    #[async_test]
    #[cfg(feature = "script")]
    async fn script(mut con: impl ConnectionLike) {
        // Note this test runs both scripts twice to test when they have already been loaded
        // into Redis and when they need to be loaded in
        let script1 = redis::Script::new("return redis.call('SET', KEYS[1], ARGV[1])");
        let script2 = redis::Script::new("return redis.call('GET', KEYS[1])");
        let script3 = redis::Script::new("return redis.call('KEYS', '*')");
        script1
            .key("key1")
            .arg("foo")
            .invoke_async::<()>(&mut con)
            .await
            .unwrap();
        let val: String = script2.key("key1").invoke_async(&mut con).await.unwrap();
        assert_eq!(val, "foo");
        let keys: Vec<String> = script3.invoke_async(&mut con).await.unwrap();
        assert_eq!(keys, ["key1"]);
        script1
            .key("key1")
            .arg("bar")
            .invoke_async::<()>(&mut con)
            .await
            .unwrap();
        let val: String = script2.key("key1").invoke_async(&mut con).await.unwrap();
        assert_eq!(val, "bar");
        let keys: Vec<String> = script3.invoke_async(&mut con).await.unwrap();
        assert_eq!(keys, ["key1"]);
    }

    #[async_test]
    #[cfg(feature = "script")]
    async fn script_load(mut con: impl ConnectionLike) {
        let script = redis::Script::new("return 'Hello World'");

        let hash = script.prepare_invoke().load_async(&mut con).await.unwrap();
        assert_eq!(hash, script.get_hash().to_string());
    }

    #[async_test]
    #[cfg(feature = "script")]
    async fn script_returning_complex_type(mut con: impl ConnectionLike) {
        redis::Script::new("return {1, ARGV[1], true}")
            .arg("hello")
            .invoke_async(&mut con)
            .map_ok(|(i, s, b): (i32, String, bool)| {
                assert_eq!(i, 1);
                assert_eq!(s, "hello");
                assert!(b);
            })
            .await
            .unwrap()
    }

    // Allowing `nth(0)` for similarity with the following `nth(1)`.
    // Allowing `let ()` as `query_async` requires the type it converts the result to.
    #[allow(clippy::let_unit_value, clippy::iter_nth_zero)]
    #[async_test]
    async fn io_error_on_kill_issue_320() {
        let ctx = TestContext::new();

        let mut conn_to_kill = ctx.async_connection().await.unwrap();
        kill_client_async(&mut conn_to_kill, &ctx.client)
            .await
            .unwrap();
        let mut killed_client = conn_to_kill;

        let err = loop {
            let _ = match killed_client.get::<_, Option<String>>("a").await {
                // We are racing against the server being shutdown so try until we a get an io error
                Ok(_) => sleep(Duration::from_millis(50).into()).await,
                Err(err) => break err,
            };
        };
        assert_eq!(err.kind(), ErrorKind::Io);
    }

    #[async_test]
    async fn invalid_password_issue_343() {
        let ctx = TestContext::new();

        let redis = RedisConnectionInfo::default().set_password("asdcasc");
        let connection_info = ctx
            .server
            .client_addr()
            .clone()
            .into_connection_info()
            .unwrap()
            .set_redis_settings(redis);

        let client = redis::Client::open(connection_info).unwrap();

        let err = client
            .get_multiplexed_async_connection()
            .await
            .err()
            .unwrap();
        assert_eq!(
            err.kind(),
            ErrorKind::AuthenticationFailed,
            "Unexpected error: {err}",
        );
    }

    #[async_test]
    async fn scan_with_options_works(mut con: impl AsyncCommands) {
        for i in 0..20usize {
            let _: () = con.append(format!("test/{i}"), i).await.unwrap();
            let _: () = con.append(format!("other/{i}"), i).await.unwrap();
        }
        // scan with pattern
        let opts = ScanOptions::default().with_count(20).with_pattern("test/*");
        let values = con.scan_options::<String>(opts).await.unwrap();
        let values: Vec<_> = values
            .collect()
            .timeout(futures_time::time::Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(values.len(), 20);

        // scan without pattern
        let opts = ScanOptions::default();
        let values = con.scan_options::<String>(opts).await.unwrap();
        let values: Vec<_> = values
            .collect()
            .timeout(futures_time::time::Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(values.len(), 40);
    }

    // Test issue of Stream trait blocking if we try to iterate more than 10 items
    // https://github.com/mitsuhiko/redis-rs/issues/537 and https://github.com/mitsuhiko/redis-rs/issues/583
    #[async_test]
    async fn issue_stream_blocks(mut con: impl AsyncCommands) {
        for i in 0..20usize {
            let _: () = con.append(format!("test/{i}"), i).await.unwrap();
        }
        let values = con.scan_match::<&str, String>("test/*").await.unwrap();
        async move {
            let values: Vec<_> = values.collect().await;
            assert_eq!(values.len(), 20);
        }
        .timeout(futures_time::time::Duration::from_millis(100))
        .await
        .unwrap();
    }

    // Test issue of AsyncCommands::scan returning the wrong number of keys
    // https://github.com/redis-rs/redis-rs/issues/759
    #[async_test]
    async fn issue_async_commands_scan_broken(mut con: impl AsyncCommands) {
        let mut keys: Vec<String> = (0..100).map(|k| format!("async-key{k}")).collect();
        keys.sort();
        for key in &keys {
            let _: () = con.set(key, b"foo").await.unwrap();
        }

        let iter: redis::AsyncIter<String> = con.scan().await.unwrap();
        let mut keys_from_redis: Vec<_> = iter.map(std::result::Result::unwrap).collect().await;
        keys_from_redis.sort();
        assert_eq!(keys, keys_from_redis);
        assert_eq!(keys.len(), 100);
    }

    mod pub_sub {
        use std::time::Duration;

        use super::*;

        #[async_test]
        async fn pub_sub_subscription() {
            let ctx = TestContext::new();

            let mut pubsub_conn = ctx.async_pubsub().await.unwrap();
            let _: () = pubsub_conn.subscribe("phonewave").await.unwrap();
            let mut pubsub_stream = pubsub_conn.on_message();
            let mut publish_conn = ctx.async_connection().await.unwrap();
            let _: () = publish_conn.publish("phonewave", "banana").await.unwrap();

            let repeats = 6;
            for _ in 0..repeats {
                let _: () = publish_conn.publish("phonewave", "banana").await.unwrap();
            }

            for _ in 0..repeats {
                let message: String = pubsub_stream.next().await.unwrap().get_payload().unwrap();

                assert_eq!("banana".to_string(), message);
            }
        }

        #[async_test]
        async fn pub_sub_subscription_to_multiple_channels() {
            let ctx = TestContext::new();

            let mut pubsub_conn = ctx.async_pubsub().await.unwrap();
            let _: () = pubsub_conn
                .subscribe(&["phonewave", "foo", "bar"])
                .await
                .unwrap();
            let mut pubsub_stream = pubsub_conn.on_message();
            let mut publish_conn = ctx.async_connection().await.unwrap();
            let _: () = publish_conn.publish("phonewave", "banana").await.unwrap();

            let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload().unwrap();
            assert_eq!("banana".to_string(), msg_payload);

            let _: () = publish_conn.publish("foo", "foobar").await.unwrap();
            let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload().unwrap();
            assert_eq!("foobar".to_string(), msg_payload);
        }

        // Test issue of AsyncCommands::scan not returning keys because wrong assumptions about the key type were made
        // https://github.com/redis-rs/redis-rs/issues/1309
        #[async_test]
        async fn issue_async_commands_scan_finishing_prematurely(mut con: impl AsyncCommands) {
            const PREFIX: &str = "async-key";
            const NUM_KEYS: usize = 100;

            /// Container that is constructed from a string that has [`PREFIX`] as prefix
            struct Container(String);

            impl redis::FromRedisValue for Container {
                fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
                    let text = String::from_redis_value(v.clone()).unwrap();

                    // If container does not start with [`PREFIX`], return error
                    if !text.starts_with(PREFIX) {
                        // hack to create a parsing error
                        return Err(u64::from_redis_value(v).unwrap_err());
                    }

                    Ok(Container(text))
                }
            }

            // Insert 100 keys but one with an incorrect prefix
            let keys: Vec<String> = (0..NUM_KEYS)
                .map(|i| format!("{}{i}", if i == 50 { "NOPE" } else { PREFIX }))
                .collect();

            for key in &keys {
                let _: () = con.set(key, "bar".as_bytes()).await.unwrap();
            }

            // Query all keys
            let mut iter: redis::AsyncIter<Container> = con.scan().await.unwrap();

            let mut error = None;
            let mut count = 0;

            while let Some(key) = iter.next_item().await {
                match key {
                    Ok(key) => {
                        assert!(key.0.starts_with(PREFIX));
                        count += 1;
                    }
                    Err(_) if error.is_some() => {
                        panic!("Encountered multiple errors");
                    }
                    Err(e) => error = Some(e.kind()),
                };
            }

            // Assert that the number of visited keys is all keys minus
            // the one invalid key
            assert_eq!(count, NUM_KEYS - 1);

            // Assert that the encountered error is a type error
            assert_eq!(error, Some(ErrorKind::Parse));
        }

        #[async_test]
        async fn pub_sub_unsubscription() {
            const SUBSCRIPTION_KEY: &str = "phonewave-pub-sub-unsubscription";

            let ctx = TestContext::new();

            let mut pubsub_conn = ctx.async_pubsub().await.unwrap();
            pubsub_conn.subscribe(SUBSCRIPTION_KEY).await.unwrap();
            pubsub_conn.unsubscribe(SUBSCRIPTION_KEY).await.unwrap();

            let mut conn = ctx.async_connection().await.unwrap();
            let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                .arg("NUMSUB")
                .arg(SUBSCRIPTION_KEY)
                .query_async(&mut conn)
                .await
                .unwrap();
            let subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
            assert_eq!(subscription_count, 0);
        }

        #[async_test]
        async fn can_receive_messages_while_sending_requests_from_split_pub_sub() {
            let ctx = TestContext::new();

            let (mut sink, mut stream) = ctx.async_pubsub().await.unwrap().split();
            let mut publish_conn = ctx.async_connection().await.unwrap();

            let _: () = sink.subscribe("phonewave").await.unwrap();
            let repeats = 6;
            for _ in 0..repeats {
                let _: () = publish_conn.publish("phonewave", "banana").await.unwrap();
            }

            for _ in 0..repeats {
                let message: String = stream.next().await.unwrap().get_payload().unwrap();

                assert_eq!("banana".to_string(), message);
            }
        }

        #[async_test]
        async fn can_send_ping_on_split_pubsub() {
            let ctx = TestContext::new();

            let (mut sink, mut stream) = ctx.async_pubsub().await.unwrap().split();
            let mut publish_conn = ctx.async_connection().await.unwrap();

            let _: () = sink.subscribe("phonewave").await.unwrap();

            // we publish before the ping, to verify that published messages don't distort the ping's resuilt.
            let repeats = 6;
            for _ in 0..repeats {
                let _: () = publish_conn.publish("phonewave", "banana").await.unwrap();
            }

            if ctx.protocol.supports_resp3() {
                let message: String = sink.ping().await.unwrap();
                assert_eq!(message, "PONG");
            } else {
                let message: Vec<String> = sink.ping().await.unwrap();
                assert_eq!(message, vec!["pong", ""]);
            }

            if ctx.protocol.supports_resp3() {
                let message: String = sink.ping_message("foobar").await.unwrap();
                assert_eq!(message, "foobar");
            } else {
                let message: Vec<String> = sink.ping_message("foobar").await.unwrap();
                assert_eq!(message, vec!["pong", "foobar"]);
            }

            for _ in 0..repeats {
                let message: String = stream.next().await.unwrap().get_payload().unwrap();

                assert_eq!("banana".to_string(), message);
            }

            // after the stream is closed, pinging should fail.
            drop(stream);
            let err = sink.ping_message::<()>("foobar").await.unwrap_err();
            assert!(err.is_unrecoverable_error());
        }

        #[async_test]
        async fn can_receive_messages_from_split_pub_sub_after_sink_was_dropped() {
            let ctx = TestContext::new();

            let (mut sink, mut stream) = ctx.async_pubsub().await.unwrap().split();
            let mut publish_conn = ctx.async_connection().await.unwrap();

            let _: () = sink.subscribe("phonewave").await.unwrap();
            drop(sink);
            let repeats = 6;
            for _ in 0..repeats {
                let _: () = publish_conn.publish("phonewave", "banana").await.unwrap();
            }

            for _ in 0..repeats {
                let message: String = stream.next().await.unwrap().get_payload().unwrap();

                assert_eq!("banana".to_string(), message);
            }
        }

        #[async_test]
        async fn can_receive_messages_from_split_pub_sub_after_into_on_message() {
            let ctx = TestContext::new();

            let mut pubsub = ctx.async_pubsub().await.unwrap();
            let mut publish_conn = ctx.async_connection().await.unwrap();

            let _: () = pubsub.subscribe("phonewave").await.unwrap();
            let mut stream = pubsub.into_on_message();
            // wait a bit
            sleep(Duration::from_secs(2).into()).await;
            let repeats = 6;
            for _ in 0..repeats {
                let _: () = publish_conn.publish("phonewave", "banana").await.unwrap();
            }

            for _ in 0..repeats {
                let message: String = stream.next().await.unwrap().get_payload().unwrap();

                assert_eq!("banana".to_string(), message);
            }
        }

        #[async_test]
        async fn cannot_subscribe_on_split_pub_sub_after_stream_was_dropped() {
            let ctx = TestContext::new();

            let (mut sink, stream) = ctx.async_pubsub().await.unwrap().split();
            drop(stream);

            assert_matches!(sink.subscribe("phonewave").await, Err(_));
        }

        #[async_test]
        async fn automatic_unsubscription() {
            const SUBSCRIPTION_KEY: &str = "phonewave-automatic-unsubscription";

            let ctx = TestContext::new();

            let mut pubsub_conn = ctx.async_pubsub().await.unwrap();
            pubsub_conn.subscribe(SUBSCRIPTION_KEY).await.unwrap();
            drop(pubsub_conn);

            let mut conn = ctx.async_connection().await.unwrap();
            let mut subscription_count = 1;
            // Allow for the unsubscription to occur within 5 seconds
            for _ in 0..100 {
                let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                    .arg("NUMSUB")
                    .arg(SUBSCRIPTION_KEY)
                    .query_async(&mut conn)
                    .await
                    .unwrap();
                subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
                if subscription_count == 0 {
                    break;
                }

                sleep(Duration::from_millis(50).into()).await;
            }
            assert_eq!(subscription_count, 0);
        }

        #[async_test]
        async fn automatic_unsubscription_on_split() {
            const SUBSCRIPTION_KEY: &str = "phonewave-automatic-unsubscription-on-split";

            let ctx = TestContext::new();

            let (mut sink, stream) = ctx.async_pubsub().await.unwrap().split();
            sink.subscribe(SUBSCRIPTION_KEY).await.unwrap();
            let mut conn = ctx.async_connection().await.unwrap();
            sleep(Duration::from_millis(100).into()).await;

            let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                .arg("NUMSUB")
                .arg(SUBSCRIPTION_KEY)
                .query_async(&mut conn)
                .await
                .unwrap();
            let mut subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
            assert_eq!(subscription_count, 1);

            drop(stream);

            // Allow for the unsubscription to occur within 5 seconds
            for _ in 0..100 {
                let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                    .arg("NUMSUB")
                    .arg(SUBSCRIPTION_KEY)
                    .query_async(&mut conn)
                    .await
                    .unwrap();
                subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
                if subscription_count == 0 {
                    break;
                }

                sleep(Duration::from_millis(50).into()).await;
            }
            assert_eq!(subscription_count, 0);

            // verify that the sink is unusable after the stream is dropped.
            let err = sink.subscribe(SUBSCRIPTION_KEY).await.unwrap_err();
            assert!(err.is_unrecoverable_error(), "{err}");
        }

        #[async_test]
        async fn pipe_errors_do_not_affect_subsequent_commands(mut conn: impl AsyncCommands) {
            conn.lpush::<&str, &str, ()>("key", "value").await.unwrap();

            redis::pipe()
                        .get("key") // WRONGTYPE
                        .llen("key")
                        .exec_async(&mut conn)
                        .await.unwrap_err();

            let list: Vec<String> = conn.lrange("key", 0, -1).await.unwrap();

            assert_eq!(list, vec!["value".to_owned()]);
        }

        #[async_test]
        async fn multiplexed_pub_sub_subscribe_on_multiple_channels() {
            let ctx = TestContext::new();
            if !ctx.protocol.supports_resp3() {
                return;
            }

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
            let mut conn = ctx
                .client
                .get_multiplexed_async_connection_with_config(&config)
                .await
                .unwrap();
            let _: () = conn.subscribe(&["phonewave", "foo", "bar"]).await.unwrap();
            let mut publish_conn = ctx.async_connection().await.unwrap();

            let msg_payload = rx.recv().await.unwrap();
            assert_eq!(msg_payload.kind, PushKind::Subscribe);

            let _: () = publish_conn.publish("foo", "foobar").await.unwrap();

            let msg_payload = rx.recv().await.unwrap();
            assert_eq!(msg_payload.kind, PushKind::Subscribe);
            let msg_payload = rx.recv().await.unwrap();
            assert_eq!(msg_payload.kind, PushKind::Subscribe);
            let msg_payload = rx.recv().await.unwrap();
            assert_eq!(msg_payload.kind, PushKind::Message);
        }

        #[async_test]
        async fn non_transaction_errors_do_not_affect_other_results_in_pipeline(
            mut conn: impl AsyncCommands,
        ) {
            conn.lpush::<&str, &str, ()>("key", "value").await.unwrap();

            let mut results: Vec<Value> = conn
                .req_packed_commands(
                    redis::pipe()
                        .get("key") // WRONGTYPE
                                .llen("key"),
                    0,
                    2,
                )
                .await
                .unwrap();

            assert_eq!(results.pop().unwrap(), Value::Int(1));
            assert_matches!(results.pop().unwrap().extract_error(), Err(_));
        }

        #[async_test]
        async fn pub_sub_multiple() {
            let ctx = TestContext::new();
            let redis = RedisConnectionInfo::default().set_protocol(ProtocolVersion::RESP3);
            let connection_info = ctx.server.connection_info().set_redis_settings(redis);
            let client = redis::Client::open(connection_info).unwrap();

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
            let mut conn = client
                .get_multiplexed_async_connection_with_config(&config)
                .await
                .unwrap();
            let pub_count = 10;
            let channel_name = "phonewave".to_string();
            conn.subscribe(channel_name.clone()).await.unwrap();
            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::Subscribe);

            let mut publish_conn = ctx.async_connection().await.unwrap();
            for i in 0..pub_count {
                let _: () = publish_conn
                    .publish(channel_name.clone(), format!("banana {i}"))
                    .await
                    .unwrap();
            }
            for i in 0..pub_count {
                let push = rx.recv().await.unwrap();
                assert_eq!(push.kind, PushKind::Message);
                assert_eq!(
                    push.data,
                    vec![
                        Value::BulkString("phonewave".as_bytes().to_vec()),
                        Value::BulkString(format!("banana {i}").into_bytes())
                    ]
                );
            }
            assert_matches!(rx.try_recv(), Err(_));

            //Lets test if unsubscribing from individual channel subscription works
            let _: () = publish_conn
                .publish(channel_name.clone(), "banana!")
                .await
                .unwrap();
            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::Message);
            assert_eq!(
                push.data,
                vec![
                    Value::BulkString("phonewave".as_bytes().to_vec()),
                    Value::BulkString("banana!".as_bytes().to_vec())
                ]
            );

            //Giving none for channel id should unsubscribe all subscriptions from that channel and send unsubcribe command to server.
            conn.unsubscribe(channel_name.clone()).await.unwrap();
            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::Unsubscribe);
            let _: () = publish_conn
                .publish(channel_name.clone(), "banana!")
                .await
                .unwrap();
            //Let's wait for 100ms to make sure there is nothing in channel.
            sleep(Duration::from_millis(100).into()).await;
            assert_matches!(rx.try_recv(), Err(_));
        }

        #[async_test]
        async fn pub_sub_requires_resp3() {
            if use_protocol().supports_resp3() {
                return;
            }
            let ctx = TestContext::new();
            let mut conn = ctx.async_connection().await.unwrap();

            let res = conn.subscribe("foo").await;

            assert_eq!(
                res.unwrap_err().kind(),
                redis::ErrorKind::InvalidClientConfig
            );
        }

        #[async_test]
        async fn push_sender_send_on_disconnect() {
            let ctx = TestContext::new();
            let redis = RedisConnectionInfo::default().set_protocol(ProtocolVersion::RESP3);
            let connection_info = ctx.server.connection_info().set_redis_settings(redis);
            let client = redis::Client::open(connection_info).unwrap();

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
            let mut conn = client
                .get_multiplexed_async_connection_with_config(&config)
                .await
                .unwrap();

            let _: () = conn.set("A", "1").await.unwrap();
            assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
            kill_client_async(&mut conn, &ctx.client).await.unwrap();

            assert_eq!(rx.recv().await.unwrap().kind, PushKind::Disconnection);
        }

        #[cfg(feature = "connection-manager")]
        #[async_test]
        async fn manager_should_resubscribe_to_pubsub_channels_after_disconnect() {
            let ctx = TestContext::new();
            if !ctx.protocol.supports_resp3() {
                return;
            }
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            let max_delay_between_attempts = Duration::from_millis(2);
            let config = redis::aio::ConnectionManagerConfig::new()
                .set_push_sender(tx)
                .set_automatic_resubscription()
                .set_max_delay(max_delay_between_attempts);

            let mut pubsub_conn = ctx
                .client
                .get_connection_manager_with_config(config)
                .await
                .unwrap();
            let _: () = pubsub_conn
                .subscribe(&["phonewave", "foo", "bar"])
                .await
                .unwrap();
            let _: () = pubsub_conn.psubscribe(&["zoom*"]).await.unwrap();
            let _: () = pubsub_conn.unsubscribe("foo").await.unwrap();

            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::Subscribe);
            assert_eq!(
                push.data,
                vec![Value::BulkString(b"phonewave".to_vec()), Value::Int(1)]
            );
            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::Subscribe);
            assert_eq!(
                push.data,
                vec![Value::BulkString(b"foo".to_vec()), Value::Int(2)]
            );
            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::Subscribe);
            assert_eq!(
                push.data,
                vec![Value::BulkString(b"bar".to_vec()), Value::Int(3)]
            );
            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::PSubscribe);
            assert_eq!(
                push.data,
                vec![Value::BulkString(b"zoom*".to_vec()), Value::Int(4)]
            );
            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::Unsubscribe);
            assert_eq!(
                push.data,
                vec![Value::BulkString(b"foo".to_vec()), Value::Int(3)]
            );

            let addr = ctx.server.client_addr().clone();
            drop(ctx);
            // a yield, to let the connection manager to notice the broken connection.
            // this is required to reduce differences in test runs between smol & tokio runtime.
            sleep(Duration::from_millis(1).into()).await;
            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::Disconnection);
            let ctx = TestContext::new_with_addr(addr);

            let push1 = rx.recv().await.unwrap();
            assert_eq!(push1.kind, PushKind::Subscribe);
            // we don't know the order that the resubscription requests will be sent in, so we check if both were received, in either order.
            let push2 = rx.recv().await.unwrap();
            assert_eq!(push2.kind, PushKind::Subscribe);
            assert!(
                (push1.data == vec![Value::BulkString(b"phonewave".to_vec()), Value::Int(1)]
                    && push2.data == vec![Value::BulkString(b"bar".to_vec()), Value::Int(2)])
                    || (push1.data == vec![Value::BulkString(b"bar".to_vec()), Value::Int(1)]
                        && push2.data
                            == vec![Value::BulkString(b"phonewave".to_vec()), Value::Int(2)])
            );
            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::PSubscribe);
            assert_eq!(
                push.data,
                vec![Value::BulkString(b"zoom*".to_vec()), Value::Int(3)]
            );

            let mut publish_conn = ctx.async_connection().await.unwrap();
            let _: () = publish_conn.publish("phonewave", "banana").await.unwrap();

            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::Message);
            assert_eq!(
                push.data,
                vec![
                    Value::BulkString(b"phonewave".to_vec()),
                    Value::BulkString(b"banana".to_vec())
                ]
            );

            // this should be skipped, because we unsubscribed from foo
            let _: () = publish_conn.publish("foo", "goo").await.unwrap();
            let _: () = publish_conn.publish("zoomer", "foobar").await.unwrap();
            let push = rx.recv().await.unwrap();
            assert_eq!(push.kind, PushKind::PMessage);
            assert_eq!(
                push.data,
                vec![
                    Value::BulkString(b"zoom*".to_vec()),
                    Value::BulkString(b"zoomer".to_vec()),
                    Value::BulkString(b"foobar".to_vec())
                ]
            );

            // no more messages should be sent.
            assert_matches!(rx.try_recv(), Err(_));
        }
    }

    #[async_test]
    async fn async_basic_pipe_with_parsing_error(mut conn: impl ConnectionLike) {
        // Tests a specific case involving repeated errors in transactions.

        // create a transaction where 2 errors are returned.
        // we call EVALSHA twice with no loaded script, thus triggering 2 errors.
        redis::pipe()
            .atomic()
            .cmd("EVALSHA")
            .arg("foobar")
            .arg(0)
            .cmd("EVALSHA")
            .arg("foobar")
            .arg(0)
            .query_async::<((), ())>(&mut conn)
            .await
            .expect_err("should return an error");

        assert!(
            // Arbitrary Redis command that should not return an error.
            redis::cmd("SMEMBERS")
                .arg("nonexistent_key")
                .query_async::<Vec<String>>(&mut conn)
                .await
                .is_ok(),
            "Failed transaction should not interfere with future calls."
        );
    }

    #[async_test]
    #[cfg(feature = "connection-manager")]
    async fn connection_manager_reconnect_after_delay() {
        let max_delay_between_attempts = Duration::from_millis(2);
        let mut config = redis::aio::ConnectionManagerConfig::new()
            .set_exponent_base(10000.0)
            .set_max_delay(max_delay_between_attempts);

        let tempdir = tempfile::Builder::new()
            .prefix("redis")
            .tempdir()
            .expect("failed to create tempdir");
        let tls_files = redis_test::utils::build_keys_and_certs_for_tls(&tempdir);

        let ctx = TestContext::with_tls(tls_files.clone(), false);
        let protocol = ctx.protocol;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        if ctx.protocol.supports_resp3() {
            config = config.set_push_sender(tx);
        }
        let mut manager =
            redis::aio::ConnectionManager::new_with_config(ctx.client.clone(), config)
                .await
                .unwrap();
        let addr = ctx.server.client_addr().clone();
        drop(ctx);
        let result: RedisResult<redis::Value> = manager.set("foo", "bar").await;
        // we expect a connection failure error.
        assert!(result.unwrap_err().is_unrecoverable_error());
        if protocol.supports_resp3() {
            assert_eq!(rx.recv().await.unwrap().kind, PushKind::Disconnection);
        }

        let _server = redis_test::server::RedisServer::new_with_addr_and_modules(addr, &[], false);

        for _ in 0..5 {
            let Ok(result) = manager.set::<_, _, Value>("foo", "bar").await else {
                sleep(Duration::from_millis(3).into()).await;
                continue;
            };
            assert_eq!(result, redis::Value::Okay);
            if protocol.supports_resp3() {
                assert_matches!(rx.try_recv(), Err(_));
            }
            return;
        }
        panic!("failed to reconnect");
    }

    #[cfg(feature = "connection-manager")]
    #[async_test]
    async fn manager_should_reconnect_without_actions_if_resp3_is_set() {
        let ctx = TestContext::new();
        if !ctx.protocol.supports_resp3() {
            return;
        }

        let max_delay_between_attempts = Duration::from_millis(2);
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_exponent_base(10000.0)
            .set_max_delay(max_delay_between_attempts);

        let mut conn = ctx
            .client
            .get_connection_manager_with_config(config)
            .await
            .unwrap();

        let addr = ctx.server.client_addr().clone();
        drop(ctx);
        let _ctx = TestContext::new_with_addr(addr);

        sleep(Duration::from_secs_f32(0.01).into()).await;

        assert_matches!(cmd("PING").exec_async(&mut conn).await, Ok(_));
    }

    #[cfg(feature = "connection-manager")]
    #[async_test]
    async fn manager_should_completely_disconnect_when_drop() {
        let ctx = TestContext::new();
        let redis = RedisConnectionInfo::default().set_protocol(ProtocolVersion::RESP3);
        let connection_info = ctx.server.connection_info().set_redis_settings(redis);
        let client = redis::Client::open(connection_info).unwrap();

        let number_of_connections;

        {
            let mut conn = client
                .get_connection_manager_with_config(redis::aio::ConnectionManagerConfig::new())
                .await
                .unwrap();
            let connections: String = cmd("CLIENT")
                .arg("LIST")
                .query_async(&mut conn)
                .await
                .unwrap();

            number_of_connections = connections.lines().collect::<Vec<_>>().len();
        }
        {
            let mut conn = client
                .get_connection_manager_with_config(redis::aio::ConnectionManagerConfig::new())
                .await
                .unwrap();
            let connections: String = cmd("CLIENT")
                .arg("LIST")
                .query_async(&mut conn)
                .await
                .unwrap();

            assert_eq!(
                number_of_connections,
                connections.lines().collect::<Vec<_>>().len()
            );
        }
    }

    #[cfg(feature = "connection-manager")]
    #[async_test]
    async fn manager_should_reconnect_without_actions_if_push_sender_is_set_even_after_sender_returns_error()
     {
        let ctx = TestContext::new();
        if !ctx.protocol.supports_resp3() {
            return;
        }

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let max_delay_between_attempts = Duration::from_millis(2);
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_exponent_base(10000.0)
            .set_push_sender(tx)
            .set_max_delay(max_delay_between_attempts);

        let mut conn = ctx
            .client
            .get_connection_manager_with_config(config)
            .await
            .unwrap();

        let addr = ctx.server.client_addr().clone();
        // drop once, to trigger reconnect and sending the push message
        drop(ctx);
        let push = rx.recv().await.unwrap();
        assert_eq!(push.kind, PushKind::Disconnection);
        let _ctx = TestContext::new_with_addr(addr.clone());

        assert_matches!(cmd("PING").exec_async(&mut conn).await, Ok(_));
        assert_matches!(rx.try_recv(), Err(_));

        // drop again, to verify that the mechanism works even after the sender returned an error.
        drop(_ctx);
        let push = rx.recv().await.unwrap();
        assert_eq!(push.kind, PushKind::Disconnection);
        let _ctx = TestContext::new_with_addr(addr);

        sleep(Duration::from_secs_f32(0.01).into()).await;
        assert_matches!(cmd("PING").exec_async(&mut conn).await, Ok(_));
        assert_matches!(rx.try_recv(), Err(_));
    }

    #[async_test]
    async fn multiplexed_connection_kills_connection_on_drop_even_when_blocking() {
        let ctx = TestContext::new();

        let mut conn = ctx.async_connection().await.unwrap();
        let mut connection_to_dispose_of = ctx.async_connection().await.unwrap();
        connection_to_dispose_of.set_response_timeout(Duration::from_millis(1));

        async fn count_ids(conn: &mut impl redis::aio::ConnectionLike) -> RedisResult<usize> {
            let initial_connections: String =
                cmd("CLIENT").arg("LIST").query_async(conn).await.unwrap();

            Ok(initial_connections
                .as_bytes()
                .windows(3)
                .filter(|substr| substr == b"id=")
                .count())
        }

        assert_eq!(count_ids(&mut conn).await.unwrap(), 2);

        let command_that_blocks = cmd("BLPOP")
            .arg("foo")
            .arg(0)
            .exec_async(&mut connection_to_dispose_of)
            .await;

        let err = command_that_blocks.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Io);

        drop(connection_to_dispose_of);

        sleep(Duration::from_millis(10).into()).await;

        assert_eq!(count_ids(&mut conn).await.unwrap(), 1);
    }

    #[async_test]
    async fn monitor() {
        let ctx = TestContext::new();

        let mut conn = ctx.async_connection().await.unwrap();
        let monitor_conn = ctx.client.get_async_monitor().await.unwrap();
        let mut stream = monitor_conn.into_on_message();

        let _: () = conn.set("foo", "bar").await.unwrap();

        let msg: String = stream.next().await.unwrap();
        assert!(msg.ends_with("\"SET\" \"foo\" \"bar\""));

        drop(ctx);

        assert!(stream.next().await.is_none());
    }

    #[cfg(feature = "tls-rustls")]
    mod mtls_test {
        use super::*;

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn test_should_connect_mtls(#[case] runtime: RuntimeType) {
            let ctx = TestContext::new_with_mtls();

            let client =
                build_single_client(ctx.server.connection_info(), &ctx.server.tls_paths, true)
                    .unwrap();
            let connect = client.get_multiplexed_async_connection();
            block_on_all(
                async move {
                    let mut con = connect.await.unwrap();

                    redis::cmd("SET")
                        .arg("key1")
                        .arg(b"foo")
                        .exec_async(&mut con)
                        .await
                        .unwrap();
                    let result = redis::cmd("GET").arg(&["key1"]).query_async(&mut con).await;
                    assert_eq!(result, Ok("foo".to_string()));
                },
                runtime,
            );
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn test_should_not_connect_if_tls_active(#[case] runtime: RuntimeType) {
            let ctx = TestContext::new_with_mtls();

            let client =
                build_single_client(ctx.server.connection_info(), &ctx.server.tls_paths, false)
                    .unwrap();
            let connect = client.get_multiplexed_async_connection();
            let result = block_on_all(
                async move {
                    let mut con = connect.await?;
                    redis::cmd("SET")
                        .arg("key1")
                        .arg(b"foo")
                        .exec_async(&mut con)
                        .await
                        .unwrap();
                    let result = redis::cmd("GET").arg(&["key1"]).query_async(&mut con).await;
                    assert_eq!(result, Ok("foo".to_string()));
                    result
                },
                runtime,
            );

            // depends on server type set (REDISRS_SERVER_TYPE)
            match ctx.server.connection_info().addr() {
                redis::ConnectionAddr::TcpTls { .. } => {
                    if result.is_ok() {
                        panic!(
                            "Must NOT be able to connect without client credentials if server accepts TLS"
                        );
                    }
                }
                _ => {
                    if result.is_err() {
                        panic!(
                            "Must be able to connect without client credentials if server does NOT accept TLS"
                        );
                    }
                }
            }
        }
    }

    #[async_test]
    #[cfg(feature = "connection-manager")]
    async fn resp3_pushes_connection_manager() {
        let ctx = TestContext::new();
        let redis = RedisConnectionInfo::default().set_protocol(ProtocolVersion::RESP3);
        let connection_info = ctx.server.connection_info().set_redis_settings(redis);
        let client = redis::Client::open(connection_info).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let config = redis::aio::ConnectionManagerConfig::new().set_push_sender(tx);
        let mut manager = redis::aio::ConnectionManager::new_with_config(client, config)
            .await
            .unwrap();
        manager
            .send_packed_command(cmd("CLIENT").arg("TRACKING").arg("ON"))
            .await
            .unwrap();
        let pipe = build_simple_pipeline_for_invalidation();
        let _: RedisResult<()> = pipe.query_async(&mut manager).await;
        let _: i32 = manager.get("key_1").await.unwrap();
        let redis::PushInfo { kind, data } = rx.try_recv().unwrap();
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

    #[async_test]
    async fn select_db() {
        let ctx = TestContext::new();
        let redis = redis_settings().set_db(5);
        let connection_info = ctx.server.connection_info().set_redis_settings(redis);
        let client = redis::Client::open(connection_info).unwrap();

        let mut connection = client.get_multiplexed_async_connection().await.unwrap();

        let info: String = redis::cmd("CLIENT")
            .arg("info")
            .query_async(&mut connection)
            .await
            .unwrap();
        assert!(info.contains("db=5"));
    }

    #[async_test]
    async fn multiplexed_connection_send_single_disconnect_on_connection_failure() {
        let mut ctx = TestContext::new();
        if !ctx.protocol.supports_resp3() {
            return;
        }

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
        let _res = ctx
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .unwrap();
        drop(config);
        ctx.stop_server();

        assert_eq!(rx.recv().await.unwrap().kind, PushKind::Disconnection);
        sleep(Duration::from_millis(1).into()).await;
        assert_matches!(rx.try_recv(), Err(_));
        assert!(rx.is_closed());
    }

    #[async_test]
    async fn fail_on_empty_command() {
        let ctx = TestContext::new();
        let mut connection = ctx.async_connection().await.unwrap();

        let error: RedisError = redis::Pipeline::new()
            .query_async::<String>(&mut connection)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Client);
        assert_eq!(error.to_string(), "empty command - Client");

        let error: RedisError = redis::Cmd::new()
            .query_async::<String>(&mut connection)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Client);
        assert_eq!(error.to_string(), "empty command - Client");
    }
}
