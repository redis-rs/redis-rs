mod support;

#[cfg(test)]
mod basic_async {
    use std::{collections::HashMap, time::Duration};

    use futures::{prelude::*, StreamExt};
    use futures_time::{future::FutureExt, task::sleep};
    #[cfg(feature = "connection-manager")]
    use redis::aio::ConnectionManager;
    use redis::{
        aio::{ConnectionLike, MultiplexedConnection},
        cmd, pipe, AsyncCommands, ConnectionInfo, ErrorKind, ProtocolVersion, PushKind,
        RedisConnectionInfo, RedisError, RedisFuture, RedisResult, ScanOptions, ToRedisArgs, Value,
    };
    use redis_test::server::use_protocol;
    use rstest::rstest;
    use tokio::sync::mpsc::error::TryRecvError;

    use crate::support::*;

    #[derive(Clone)]
    enum Wrapper {
        MultiplexedConnection(MultiplexedConnection),
        #[cfg(feature = "connection-manager")]
        ConnectionManager(ConnectionManager),
    }

    #[cfg(feature = "connection-manager")]
    impl From<ConnectionManager> for Wrapper {
        fn from(conn: ConnectionManager) -> Self {
            Self::ConnectionManager(conn)
        }
    }
    impl From<MultiplexedConnection> for Wrapper {
        fn from(conn: MultiplexedConnection) -> Self {
            Self::MultiplexedConnection(conn)
        }
    }

    impl ConnectionLike for Wrapper {
        fn req_packed_command<'a>(&'a mut self, cmd: &'a redis::Cmd) -> RedisFuture<'a, Value> {
            match self {
                Wrapper::MultiplexedConnection(conn) => conn.req_packed_command(cmd),
                #[cfg(feature = "connection-manager")]
                Wrapper::ConnectionManager(conn) => conn.req_packed_command(cmd),
            }
        }

        fn req_packed_commands<'a>(
            &'a mut self,
            cmd: &'a redis::Pipeline,
            offset: usize,
            count: usize,
        ) -> RedisFuture<'a, Vec<Value>> {
            match self {
                Wrapper::MultiplexedConnection(conn) => {
                    conn.req_packed_commands(cmd, offset, count)
                }
                #[cfg(feature = "connection-manager")]
                Wrapper::ConnectionManager(conn) => conn.req_packed_commands(cmd, offset, count),
            }
        }

        fn get_db(&self) -> i64 {
            match self {
                Wrapper::MultiplexedConnection(conn) => conn.get_db(),
                #[cfg(feature = "connection-manager")]
                Wrapper::ConnectionManager(conn) => conn.get_db(),
            }
        }
    }

    impl Wrapper {
        async fn subscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
            match self {
                Wrapper::MultiplexedConnection(conn) => conn.subscribe(channel_name).await,
                #[cfg(feature = "connection-manager")]
                Wrapper::ConnectionManager(conn) => conn.subscribe(channel_name).await,
            }
        }
    }

    fn test_with_all_connection_types_with_context<Fut, T>(
        setup: impl Fn(),
        test: impl Fn(TestContext, Wrapper) -> Fut,
        runtime: RuntimeType,
    ) where
        Fut: Future<Output = redis::RedisResult<T>>,
    {
        block_on_all(
            async move {
                setup();
                let ctx = TestContext::new();
                let conn = ctx.async_connection().await.unwrap().into();
                test(ctx, conn).await.unwrap();

                #[cfg(feature = "connection-manager")]
                {
                    let ctx = TestContext::new();
                    let conn = ctx.client.get_connection_manager().await.unwrap().into();
                    test(ctx, conn).await
                }
                .unwrap();

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    fn test_with_all_connection_types_with_setup<Fut, T>(
        setup: impl Fn(),
        test: impl Fn(Wrapper) -> Fut,
        runtime: RuntimeType,
    ) where
        Fut: Future<Output = redis::RedisResult<T>>,
    {
        test_with_all_connection_types_with_context(
            setup,
            |_ctx, conn| async {
                let res = test(conn).await;
                // we drop it here in order to ensure that the context isn't dropped before `test` completes.
                drop(_ctx);
                res
            },
            runtime,
        )
    }

    fn test_with_all_connection_types<Fut, T>(test: impl Fn(Wrapper) -> Fut, runtime: RuntimeType)
    where
        Fut: Future<Output = redis::RedisResult<T>>,
    {
        test_with_all_connection_types_with_setup(|| {}, test, runtime)
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_args(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut con| async move {
                redis::cmd("SET")
                    .arg("key1")
                    .arg(b"foo")
                    .exec_async(&mut con)
                    .await?;
                redis::cmd("SET")
                    .arg(&["key2", "bar"])
                    .exec_async(&mut con)
                    .await?;
                let result = redis::cmd("MGET")
                    .arg(&["key1", "key2"])
                    .query_async(&mut con)
                    .await;
                assert_eq!(result, Ok(("foo".to_string(), b"bar".to_vec())));
                result
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    fn test_works_with_paused_time(#[case] runtime: RuntimeType) {
        test_with_all_connection_types_with_setup(
            || tokio::time::pause(),
            |mut conn| async move {
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
                Ok(())
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_can_authenticate_with_username_and_password(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
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

                let mut conn = redis::Client::open(ConnectionInfo {
                    addr: ctx.server.client_addr().clone(),
                    redis: RedisConnectionInfo {
                        username: Some(username.to_string()),
                        password: Some(password.to_string()),
                        ..Default::default()
                    },
                })
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
                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_nice_hash_api(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut connection| async move {
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
                Ok(())
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_nice_hash_api_in_pipe(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut connection| async move {
                assert_eq!(
                    connection
                        .hset_multiple("my_hash", &[("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8)])
                        .await,
                    Ok(())
                );

                let mut pipe = redis::pipe();
                pipe.cmd("HGETALL").arg("my_hash");
                let mut vec: Vec<HashMap<String, isize>> =
                    pipe.query_async(&mut connection).await.unwrap();
                assert_eq!(vec.len(), 1);
                let hash = vec.pop().unwrap();
                assert_eq!(hash.len(), 4);
                assert_eq!(hash.get("f1"), Some(&1));
                assert_eq!(hash.get("f2"), Some(&2));
                assert_eq!(hash.get("f3"), Some(&4));
                assert_eq!(hash.get("f4"), Some(&8));

                Ok(())
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn dont_panic_on_closed_multiplexed_connection(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        let client = ctx.client.clone();
        let connect = client.get_multiplexed_async_connection();
        drop(ctx);

        block_on_all(
            async move {
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
                        assert_eq!(
                            result.as_ref().unwrap_err().kind(),
                            redis::ErrorKind::IoError,
                            "{}",
                            result.as_ref().unwrap_err()
                        );
                        cmd().await
                    })
                    .map(|result| {
                        assert_eq!(
                            result.as_ref().unwrap_err().kind(),
                            redis::ErrorKind::IoError,
                            "{}",
                            result.as_ref().unwrap_err()
                        );
                    })
                    .await;
                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_pipeline_transaction(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut con| async move {
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
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_client_tracking_doesnt_block_execution(#[case] runtime: RuntimeType) {
        //It checks if the library distinguish a push-type message from the others and continues its normal operation.
        test_with_all_connection_types(
            |mut con| async move {
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
                Ok(())
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_pipeline_transaction_with_errors(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut con| async move {
                con.set::<_, _, ()>("x", 42).await.unwrap();

                // Make Redis a replica of a nonexistent master, thereby making it read-only.
                redis::cmd("slaveof")
                    .arg("1.1.1.1")
                    .arg("1")
                    .exec_async(&mut con)
                    .await
                    .unwrap();

                // Ensure that a write command fails with a READONLY error
                let err: RedisResult<()> = redis::pipe()
                    .atomic()
                    .set("x", 142)
                    .ignore()
                    .get("x")
                    .query_async(&mut con)
                    .await;

                assert_eq!(err.unwrap_err().kind(), ErrorKind::ReadOnly);

                let x: i32 = con.get("x").await.unwrap();
                assert_eq!(x, 42);

                Ok::<_, RedisError>(())
            },
            runtime,
        );
    }

    fn test_cmd(con: &Wrapper, i: i32) -> impl Future<Output = RedisResult<()>> + Send {
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
                .await?;
            redis::cmd("SET")
                .arg(&[&key2, "bar"])
                .exec_async(&mut con)
                .await?;
            redis::cmd("MGET")
                .arg(&[&key_2, &key2_2])
                .query_async(&mut con)
                .map(|result| {
                    assert_eq!(Ok((foo_val, b"bar".to_vec())), result);
                    Ok(())
                })
                .await
        }
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_pipe_over_multiplexed_connection(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut con| async move {
                let mut pipe = pipe();
                pipe.zrange("zset", 0, 0);
                pipe.zrange("zset", 0, 0);
                let frames = con.req_packed_commands(&pipe, 0, 2).await?;
                assert_eq!(frames.len(), 2);
                assert!(matches!(frames[0], redis::Value::Array(_)));
                assert!(matches!(frames[1], redis::Value::Array(_)));
                RedisResult::Ok(())
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_running_multiple_commands(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |con| async move {
                let cmds = (0..100).map(move |i| test_cmd(&con, i));
                future::try_join_all(cmds)
                    .map_ok(|results| {
                        assert_eq!(results.len(), 100);
                    })
                    .map_err(|err| panic!("{}", err))
                    .await
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_transaction_multiplexed_connection(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |con| async move {
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
                    .map_err(|err| panic!("{}", err))
                    .await
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_async_scanning(#[values(2, 1000)] batch_size: usize, #[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut con| async move {
                let mut unseen = std::collections::HashSet::new();

                for x in 0..batch_size {
                    redis::cmd("SADD")
                        .arg("foo")
                        .arg(x)
                        .exec_async(&mut con)
                        .await?;
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
                    // if this assertion fails, too many items were returned by the iterator.
                    assert!(unseen.remove(&x));
                }

                assert!(unseen.is_empty());
                Ok(())
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_async_scanning_iterative(
        #[values(2, 1000)] batch_size: usize,
        #[case] runtime: RuntimeType,
    ) {
        test_with_all_connection_types(
            |mut con| async move {
                let mut unseen = std::collections::HashSet::new();

                for x in 0..batch_size {
                    let key_name = format!("key.{}", x);
                    redis::cmd("SET")
                        .arg(key_name.clone())
                        .arg("foo")
                        .exec_async(&mut con)
                        .await?;
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
                    // if this assertion fails, too many items were returned by the iterator.
                    assert!(unseen.remove(&item));
                }

                assert!(unseen.is_empty());
                Ok(())
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_async_scanning_stream(
        #[values(2, 1000)] batch_size: usize,
        #[case] runtime: RuntimeType,
    ) {
        test_with_all_connection_types(
            |mut con| async move {
                let mut unseen = std::collections::HashSet::new();

                for x in 0..batch_size {
                    let key_name = format!("key.{}", x);
                    redis::cmd("SET")
                        .arg(key_name.clone())
                        .arg("foo")
                        .exec_async(&mut con)
                        .await?;
                    unseen.insert(key_name.clone());
                }

                let iter = redis::cmd("SCAN")
                    .cursor_arg(0)
                    .arg("MATCH")
                    .arg("key*")
                    .arg("COUNT")
                    .arg(1)
                    .clone()
                    .iter_async::<String>(&mut con)
                    .await
                    .unwrap();

                let collection: Vec<String> = iter.collect().await;
                for item in collection {
                    // if this assertion fails, too many items were returned by the iterator.
                    assert!(unseen.remove(&item));
                }

                assert!(unseen.is_empty());
                Ok(())
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_response_timeout_multiplexed_connection(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let mut connection = ctx.async_connection().await.unwrap();
                connection.set_response_timeout(Duration::from_millis(1));
                let mut cmd = redis::Cmd::new();
                cmd.arg("BLPOP").arg("foo").arg(0); // 0 timeout blocks indefinitely
                let result = connection.req_packed_command(&cmd).await;
                assert!(result.is_err());
                assert!(result.unwrap_err().is_timeout());
                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    #[cfg(feature = "script")]
    fn test_script(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut con| async move {
                // Note this test runs both scripts twice to test when they have already been loaded
                // into Redis and when they need to be loaded in
                let script1 = redis::Script::new("return redis.call('SET', KEYS[1], ARGV[1])");
                let script2 = redis::Script::new("return redis.call('GET', KEYS[1])");
                let script3 = redis::Script::new("return redis.call('KEYS', '*')");
                script1
                    .key("key1")
                    .arg("foo")
                    .invoke_async::<()>(&mut con)
                    .await?;
                let val: String = script2.key("key1").invoke_async(&mut con).await?;
                assert_eq!(val, "foo");
                let keys: Vec<String> = script3.invoke_async(&mut con).await?;
                assert_eq!(keys, ["key1"]);
                script1
                    .key("key1")
                    .arg("bar")
                    .invoke_async::<()>(&mut con)
                    .await?;
                let val: String = script2.key("key1").invoke_async(&mut con).await?;
                assert_eq!(val, "bar");
                let keys: Vec<String> = script3.invoke_async(&mut con).await?;
                assert_eq!(keys, ["key1"]);
                Ok::<_, RedisError>(())
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    #[cfg(feature = "script")]
    fn test_script_load(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut con| async move {
                let script = redis::Script::new("return 'Hello World'");

                let hash = script.prepare_invoke().load_async(&mut con).await.unwrap();
                assert_eq!(hash, script.get_hash().to_string());
                Ok(())
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    #[cfg(feature = "script")]
    fn test_script_returning_complex_type(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut con| async move {
                redis::Script::new("return {1, ARGV[1], true}")
                    .arg("hello")
                    .invoke_async(&mut con)
                    .map_ok(|(i, s, b): (i32, String, bool)| {
                        assert_eq!(i, 1);
                        assert_eq!(s, "hello");
                        assert!(b);
                    })
                    .await
            },
            runtime,
        );
    }

    // Allowing `nth(0)` for similarity with the following `nth(1)`.
    // Allowing `let ()` as `query_async` requires the type it converts the result to.
    #[allow(clippy::let_unit_value, clippy::iter_nth_zero)]
    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn io_error_on_kill_issue_320(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
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
                assert_eq!(err.kind(), ErrorKind::IoError);
                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn invalid_password_issue_343(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let coninfo = redis::ConnectionInfo {
                    addr: ctx.server.client_addr().clone(),
                    redis: redis::RedisConnectionInfo {
                        password: Some("asdcasc".to_string()),
                        ..Default::default()
                    },
                };
                let client = redis::Client::open(coninfo).unwrap();

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
                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_scan_with_options_works(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut con| async move {
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
                Ok(())
            },
            runtime,
        );
    }

    // Test issue of Stream trait blocking if we try to iterate more than 10 items
    // https://github.com/mitsuhiko/redis-rs/issues/537 and https://github.com/mitsuhiko/redis-rs/issues/583
    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_issue_stream_blocks(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut con| async move {
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
                Ok(())
            },
            runtime,
        );
    }

    #[rstest]
    // Test issue of AsyncCommands::scan returning the wrong number of keys
    // https://github.com/redis-rs/redis-rs/issues/759
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_issue_async_commands_scan_broken(#[case] runtime: RuntimeType) {
        test_with_all_connection_types(
            |mut con| async move {
                let mut keys: Vec<String> = (0..100).map(|k| format!("async-key{k}")).collect();
                keys.sort();
                for key in &keys {
                    let _: () = con.set(key, b"foo").await.unwrap();
                }

                let iter: redis::AsyncIter<String> = con.scan().await.unwrap();
                let mut keys_from_redis: Vec<_> = iter.collect().await;
                keys_from_redis.sort();
                assert_eq!(keys, keys_from_redis);
                assert_eq!(keys.len(), 100);
                Ok(())
            },
            runtime,
        );
    }

    mod pub_sub {
        use std::time::Duration;

        use super::*;

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn pub_sub_subscription(#[case] runtime: RuntimeType) {
            let ctx = TestContext::new();
            block_on_all(
                async move {
                    let mut pubsub_conn = ctx.async_pubsub().await?;
                    let _: () = pubsub_conn.subscribe("phonewave").await?;
                    let mut pubsub_stream = pubsub_conn.on_message();
                    let mut publish_conn = ctx.async_connection().await?;
                    let _: () = publish_conn.publish("phonewave", "banana").await?;

                    let repeats = 6;
                    for _ in 0..repeats {
                        let _: () = publish_conn.publish("phonewave", "banana").await?;
                    }

                    for _ in 0..repeats {
                        let message: String =
                            pubsub_stream.next().await.unwrap().get_payload().unwrap();

                        assert_eq!("banana".to_string(), message);
                    }

                    Ok(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn pub_sub_subscription_to_multiple_channels(#[case] runtime: RuntimeType) {
            use redis::RedisError;

            let ctx = TestContext::new();
            block_on_all(
                async move {
                    let mut pubsub_conn = ctx.async_pubsub().await?;
                    let _: () = pubsub_conn.subscribe(&["phonewave", "foo", "bar"]).await?;
                    let mut pubsub_stream = pubsub_conn.on_message();
                    let mut publish_conn = ctx.async_connection().await?;
                    let _: () = publish_conn.publish("phonewave", "banana").await?;

                    let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
                    assert_eq!("banana".to_string(), msg_payload);

                    let _: () = publish_conn.publish("foo", "foobar").await?;
                    let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
                    assert_eq!("foobar".to_string(), msg_payload);

                    Ok::<_, RedisError>(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn pub_sub_unsubscription(#[case] runtime: RuntimeType) {
            const SUBSCRIPTION_KEY: &str = "phonewave-pub-sub-unsubscription";

            let ctx = TestContext::new();
            block_on_all(
                async move {
                    let mut pubsub_conn = ctx.async_pubsub().await?;
                    pubsub_conn.subscribe(SUBSCRIPTION_KEY).await?;
                    pubsub_conn.unsubscribe(SUBSCRIPTION_KEY).await?;

                    let mut conn = ctx.async_connection().await?;
                    let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                        .arg("NUMSUB")
                        .arg(SUBSCRIPTION_KEY)
                        .query_async(&mut conn)
                        .await?;
                    let subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
                    assert_eq!(subscription_count, 0);

                    Ok(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn can_receive_messages_while_sending_requests_from_split_pub_sub(
            #[case] runtime: RuntimeType,
        ) {
            let ctx = TestContext::new();
            block_on_all(
                async move {
                    let (mut sink, mut stream) = ctx.async_pubsub().await?.split();
                    let mut publish_conn = ctx.async_connection().await?;

                    let _: () = sink.subscribe("phonewave").await?;
                    let repeats = 6;
                    for _ in 0..repeats {
                        let _: () = publish_conn.publish("phonewave", "banana").await?;
                    }

                    for _ in 0..repeats {
                        let message: String = stream.next().await.unwrap().get_payload().unwrap();

                        assert_eq!("banana".to_string(), message);
                    }

                    Ok(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn can_send_ping_on_split_pubsub(#[case] runtime: RuntimeType) {
            let ctx = TestContext::new();
            block_on_all(
                async move {
                    let (mut sink, mut stream) = ctx.async_pubsub().await?.split();
                    let mut publish_conn = ctx.async_connection().await?;

                    let _: () = sink.subscribe("phonewave").await?;

                    // we publish before the ping, to verify that published messages don't distort the ping's resuilt.
                    let repeats = 6;
                    for _ in 0..repeats {
                        let _: () = publish_conn.publish("phonewave", "banana").await?;
                    }

                    if ctx.protocol == ProtocolVersion::RESP3 {
                        let message: String = sink.ping().await?;
                        assert_eq!(message, "PONG");
                    } else {
                        let message: Vec<String> = sink.ping().await?;
                        assert_eq!(message, vec!["pong", ""]);
                    }

                    if ctx.protocol == ProtocolVersion::RESP3 {
                        let message: String = sink.ping_message("foobar").await?;
                        assert_eq!(message, "foobar");
                    } else {
                        let message: Vec<String> = sink.ping_message("foobar").await?;
                        assert_eq!(message, vec!["pong", "foobar"]);
                    }

                    for _ in 0..repeats {
                        let message: String = stream.next().await.unwrap().get_payload()?;

                        assert_eq!("banana".to_string(), message);
                    }

                    // after the stream is closed, pinging should fail.
                    drop(stream);
                    let err = sink.ping_message::<()>("foobar").await.unwrap_err();
                    assert!(err.is_unrecoverable_error());

                    Ok(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn can_receive_messages_from_split_pub_sub_after_sink_was_dropped(
            #[case] runtime: RuntimeType,
        ) {
            let ctx = TestContext::new();
            block_on_all(
                async move {
                    let (mut sink, mut stream) = ctx.async_pubsub().await?.split();
                    let mut publish_conn = ctx.async_connection().await?;

                    let _: () = sink.subscribe("phonewave").await?;
                    drop(sink);
                    let repeats = 6;
                    for _ in 0..repeats {
                        let _: () = publish_conn.publish("phonewave", "banana").await?;
                    }

                    for _ in 0..repeats {
                        let message: String = stream.next().await.unwrap().get_payload().unwrap();

                        assert_eq!("banana".to_string(), message);
                    }

                    Ok(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn can_receive_messages_from_split_pub_sub_after_into_on_message(
            #[case] runtime: RuntimeType,
        ) {
            let ctx = TestContext::new();
            block_on_all(
                async move {
                    let mut pubsub = ctx.async_pubsub().await?;
                    let mut publish_conn = ctx.async_connection().await?;

                    let _: () = pubsub.subscribe("phonewave").await?;
                    let mut stream = pubsub.into_on_message();
                    // wait a bit
                    sleep(Duration::from_secs(2).into()).await;
                    let repeats = 6;
                    for _ in 0..repeats {
                        let _: () = publish_conn.publish("phonewave", "banana").await?;
                    }

                    for _ in 0..repeats {
                        let message: String = stream.next().await.unwrap().get_payload().unwrap();

                        assert_eq!("banana".to_string(), message);
                    }

                    Ok(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn cannot_subscribe_on_split_pub_sub_after_stream_was_dropped(
            #[case] runtime: RuntimeType,
        ) {
            let ctx = TestContext::new();
            block_on_all(
                async move {
                    let (mut sink, stream) = ctx.async_pubsub().await?.split();
                    drop(stream);

                    assert!(sink.subscribe("phonewave").await.is_err());

                    Ok(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn automatic_unsubscription(#[case] runtime: RuntimeType) {
            const SUBSCRIPTION_KEY: &str = "phonewave-automatic-unsubscription";

            let ctx = TestContext::new();
            block_on_all(
                async move {
                    let mut pubsub_conn = ctx.async_pubsub().await?;
                    pubsub_conn.subscribe(SUBSCRIPTION_KEY).await?;
                    drop(pubsub_conn);

                    let mut conn = ctx.async_connection().await?;
                    let mut subscription_count = 1;
                    // Allow for the unsubscription to occur within 5 seconds
                    for _ in 0..100 {
                        let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                            .arg("NUMSUB")
                            .arg(SUBSCRIPTION_KEY)
                            .query_async(&mut conn)
                            .await?;
                        subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
                        if subscription_count == 0 {
                            break;
                        }

                        sleep(Duration::from_millis(50).into()).await;
                    }
                    assert_eq!(subscription_count, 0);

                    Ok::<_, RedisError>(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn automatic_unsubscription_on_split(#[case] runtime: RuntimeType) {
            const SUBSCRIPTION_KEY: &str = "phonewave-automatic-unsubscription-on-split";

            let ctx = TestContext::new();
            block_on_all(
                async move {
                    let (mut sink, stream) = ctx.async_pubsub().await?.split();
                    sink.subscribe(SUBSCRIPTION_KEY).await?;
                    let mut conn = ctx.async_connection().await?;
                    sleep(Duration::from_millis(100).into()).await;

                    let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                        .arg("NUMSUB")
                        .arg(SUBSCRIPTION_KEY)
                        .query_async(&mut conn)
                        .await?;
                    let mut subscription_count =
                        *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
                    assert_eq!(subscription_count, 1);

                    drop(stream);

                    // Allow for the unsubscription to occur within 5 seconds
                    for _ in 0..100 {
                        let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                            .arg("NUMSUB")
                            .arg(SUBSCRIPTION_KEY)
                            .query_async(&mut conn)
                            .await?;
                        subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
                        if subscription_count == 0 {
                            break;
                        }

                        sleep(Duration::from_millis(50).into()).await;
                    }
                    assert_eq!(subscription_count, 0);

                    // verify that the sink is unusable after the stream is dropped.
                    let err = sink.subscribe(SUBSCRIPTION_KEY).await.unwrap_err();
                    assert!(err.is_unrecoverable_error(), "{err:?}");

                    Ok::<_, RedisError>(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn pipe_errors_do_not_affect_subsequent_commands(#[case] runtime: RuntimeType) {
            test_with_all_connection_types(
                |mut conn| async move {
                    conn.lpush::<&str, &str, ()>("key", "value").await?;

                    redis::pipe()
                .get("key") // WRONGTYPE
                .llen("key")
                .exec_async(&mut conn)
                .await.unwrap_err();

                    let list: Vec<String> = conn.lrange("key", 0, -1).await?;

                    assert_eq!(list, vec!["value".to_owned()]);

                    Ok::<_, RedisError>(())
                },
                runtime,
            );
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn multiplexed_pub_sub_subscribe_on_multiple_channels(#[case] runtime: RuntimeType) {
            let ctx = TestContext::new();
            if ctx.protocol == ProtocolVersion::RESP2 {
                return;
            }
            block_on_all(
                async move {
                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                    let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
                    let mut conn = ctx
                        .client
                        .get_multiplexed_async_connection_with_config(&config)
                        .await?;
                    let _: () = conn.subscribe(&["phonewave", "foo", "bar"]).await?;
                    let mut publish_conn = ctx.async_connection().await?;

                    let msg_payload = rx.recv().await.unwrap();
                    assert_eq!(msg_payload.kind, PushKind::Subscribe);

                    let _: () = publish_conn.publish("foo", "foobar").await?;

                    let msg_payload = rx.recv().await.unwrap();
                    assert_eq!(msg_payload.kind, PushKind::Subscribe);
                    let msg_payload = rx.recv().await.unwrap();
                    assert_eq!(msg_payload.kind, PushKind::Subscribe);
                    let msg_payload = rx.recv().await.unwrap();
                    assert_eq!(msg_payload.kind, PushKind::Message);

                    Ok(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn non_transaction_errors_do_not_affect_other_results_in_pipeline(
            #[case] runtime: RuntimeType,
        ) {
            test_with_all_connection_types(
                |mut conn| async move {
                    conn.lpush::<&str, &str, ()>("key", "value").await?;

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
                    assert!(results.pop().unwrap().extract_error().is_err());

                    Ok::<_, RedisError>(())
                },
                runtime,
            );
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn pub_sub_multiple(#[case] runtime: RuntimeType) {
            use redis::RedisError;

            let ctx = TestContext::new();
            let mut connection_info = ctx.server.connection_info();
            connection_info.redis.protocol = ProtocolVersion::RESP3;
            let client = redis::Client::open(connection_info).unwrap();

            block_on_all(
                async move {
                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                    let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
                    let mut conn = client
                        .get_multiplexed_async_connection_with_config(&config)
                        .await?;
                    let pub_count = 10;
                    let channel_name = "phonewave".to_string();
                    conn.subscribe(channel_name.clone()).await?;
                    let push = rx.recv().await.unwrap();
                    assert_eq!(push.kind, PushKind::Subscribe);

                    let mut publish_conn = ctx.async_connection().await?;
                    for i in 0..pub_count {
                        let _: () = publish_conn
                            .publish(channel_name.clone(), format!("banana {i}"))
                            .await?;
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
                    assert!(rx.try_recv().is_err());

                    //Lets test if unsubscribing from individual channel subscription works
                    let _: () = publish_conn
                        .publish(channel_name.clone(), "banana!")
                        .await?;
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
                    conn.unsubscribe(channel_name.clone()).await?;
                    let push = rx.recv().await.unwrap();
                    assert_eq!(push.kind, PushKind::Unsubscribe);
                    let _: () = publish_conn
                        .publish(channel_name.clone(), "banana!")
                        .await?;
                    //Let's wait for 100ms to make sure there is nothing in channel.
                    sleep(Duration::from_millis(100).into()).await;
                    assert!(rx.try_recv().is_err());

                    Ok::<_, RedisError>(())
                },
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn pub_sub_requires_resp3(#[case] runtime: RuntimeType) {
            if use_protocol() != ProtocolVersion::RESP2 {
                return;
            }
            test_with_all_connection_types(
                |mut conn| async move {
                    let res = conn.subscribe("foo").await;

                    assert_eq!(
                        res.unwrap_err().kind(),
                        redis::ErrorKind::InvalidClientConfig
                    );

                    Ok(())
                },
                runtime,
            );
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn push_sender_send_on_disconnect(#[case] runtime: RuntimeType) {
            use redis::RedisError;

            let ctx = TestContext::new();
            let mut connection_info = ctx.server.connection_info();
            connection_info.redis.protocol = ProtocolVersion::RESP3;
            let client = redis::Client::open(connection_info).unwrap();

            block_on_all(
                async move {
                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                    let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
                    let mut conn = client
                        .get_multiplexed_async_connection_with_config(&config)
                        .await?;

                    let _: () = conn.set("A", "1").await?;
                    assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
                    kill_client_async(&mut conn, &ctx.client).await.unwrap();

                    assert_eq!(rx.recv().await.unwrap().kind, PushKind::Disconnection);

                    Ok::<_, RedisError>(())
                },
                runtime,
            )
            .unwrap();
        }

        #[cfg(feature = "connection-manager")]
        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[case::async_std(RuntimeType::AsyncStd)]
        fn manager_should_resubscribe_to_pubsub_channels_after_disconnect(
            #[case] runtime: RuntimeType,
        ) {
            let ctx = TestContext::new();
            if ctx.protocol == ProtocolVersion::RESP2 {
                return;
            }
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            let max_delay_between_attempts = 2;
            let config = redis::aio::ConnectionManagerConfig::new()
                .set_push_sender(tx)
                .set_automatic_resubscription()
                .set_max_delay(max_delay_between_attempts);

            block_on_all(
                async move {
                    let mut pubsub_conn = ctx
                        .client
                        .get_connection_manager_with_config(config)
                        .await?;
                    let _: () = pubsub_conn.subscribe(&["phonewave", "foo", "bar"]).await?;
                    let _: () = pubsub_conn.psubscribe(&["zoom*"]).await?;
                    let _: () = pubsub_conn.unsubscribe("foo").await?;

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
                    // this is required to reduce differences in test runs between async-std & tokio runtime.
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
                        (push1.data
                            == vec![Value::BulkString(b"phonewave".to_vec()), Value::Int(1)]
                            && push2.data
                                == vec![Value::BulkString(b"bar".to_vec()), Value::Int(2)])
                            || (push1.data
                                == vec![Value::BulkString(b"bar".to_vec()), Value::Int(1)]
                                && push2.data
                                    == vec![
                                        Value::BulkString(b"phonewave".to_vec()),
                                        Value::Int(2)
                                    ])
                    );
                    let push = rx.recv().await.unwrap();
                    assert_eq!(push.kind, PushKind::PSubscribe);
                    assert_eq!(
                        push.data,
                        vec![Value::BulkString(b"zoom*".to_vec()), Value::Int(3)]
                    );

                    let mut publish_conn = ctx.async_connection().await?;
                    let _: () = publish_conn.publish("phonewave", "banana").await?;

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
                    let _: () = publish_conn.publish("foo", "goo").await?;
                    let _: () = publish_conn.publish("zoomer", "foobar").await?;
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
                    assert!(rx.try_recv().is_err());

                    Ok::<_, RedisError>(())
                },
                runtime,
            )
            .unwrap();
        }
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_async_basic_pipe_with_parsing_error(#[case] runtime: RuntimeType) {
        // Tests a specific case involving repeated errors in transactions.
        test_with_all_connection_types(
            |mut conn| async move {
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

                Ok::<_, redis::RedisError>(())
            },
            runtime,
        )
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    #[cfg(feature = "connection-manager")]
    fn test_connection_manager_reconnect_after_delay(#[case] runtime: RuntimeType) {
        let max_delay_between_attempts = 2;

        let mut config = redis::aio::ConnectionManagerConfig::new()
            .set_factor(10000)
            .set_max_delay(max_delay_between_attempts);

        let tempdir = tempfile::Builder::new()
            .prefix("redis")
            .tempdir()
            .expect("failed to create tempdir");
        let tls_files = redis_test::utils::build_keys_and_certs_for_tls(&tempdir);

        let ctx = TestContext::with_tls(tls_files.clone(), false);
        let protocol = ctx.protocol;
        block_on_all(
            async move {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                if ctx.protocol != ProtocolVersion::RESP2 {
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
                if protocol != ProtocolVersion::RESP2 {
                    assert_eq!(rx.recv().await.unwrap().kind, PushKind::Disconnection);
                }

                let _server =
                    redis_test::server::RedisServer::new_with_addr_and_modules(addr, &[], false);

                for _ in 0..5 {
                    let Ok(result) = manager.set::<_, _, Value>("foo", "bar").await else {
                        sleep(Duration::from_millis(3).into()).await;
                        continue;
                    };
                    assert_eq!(result, redis::Value::Okay);
                    if protocol != ProtocolVersion::RESP2 {
                        assert!(rx.try_recv().is_err());
                    }
                    return Ok(());
                }
                panic!("failed to reconnect");
            },
            runtime,
        )
        .unwrap();
    }

    #[cfg(feature = "connection-manager")]
    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn manager_should_reconnect_without_actions_if_resp3_is_set(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        if ctx.protocol == ProtocolVersion::RESP2 {
            return;
        }

        let max_delay_between_attempts = 2;
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_factor(10000)
            .set_max_delay(max_delay_between_attempts);

        block_on_all(
            async move {
                let mut conn = ctx
                    .client
                    .get_connection_manager_with_config(config)
                    .await?;

                let addr = ctx.server.client_addr().clone();
                drop(ctx);
                let _ctx = TestContext::new_with_addr(addr);

                sleep(Duration::from_secs_f32(0.01).into()).await;

                assert!(cmd("PING").exec_async(&mut conn).await.is_ok());

                Ok::<_, RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[cfg(feature = "connection-manager")]
    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn manager_should_reconnect_without_actions_if_push_sender_is_set_even_after_sender_returns_error(
        #[case] runtime: RuntimeType,
    ) {
        let ctx = TestContext::new();
        if ctx.protocol == ProtocolVersion::RESP2 {
            return;
        }
        println!("running");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let max_delay_between_attempts = 2;
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_factor(10000)
            .set_push_sender(tx)
            .set_max_delay(max_delay_between_attempts);

        block_on_all(
            async move {
                let mut conn = ctx
                    .client
                    .get_connection_manager_with_config(config)
                    .await?;

                let addr = ctx.server.client_addr().clone();
                // drop once, to trigger reconnect and sending the push message
                drop(ctx);
                let push = rx.recv().await.unwrap();
                assert_eq!(push.kind, PushKind::Disconnection);
                let _ctx = TestContext::new_with_addr(addr.clone());

                assert!(cmd("PING").exec_async(&mut conn).await.is_ok());
                assert!(rx.try_recv().is_err());

                // drop again, to verify that the mechanism works even after the sender returned an error.
                drop(_ctx);
                let push = rx.recv().await.unwrap();
                assert_eq!(push.kind, PushKind::Disconnection);
                let _ctx = TestContext::new_with_addr(addr);

                sleep(Duration::from_secs_f32(0.01).into()).await;
                assert!(cmd("PING").exec_async(&mut conn).await.is_ok());
                assert!(rx.try_recv().is_err());

                Ok::<_, RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_multiplexed_connection_kills_connection_on_drop_even_when_blocking(
        #[case] runtime: RuntimeType,
    ) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let mut conn = ctx.async_connection().await.unwrap();
                let mut connection_to_dispose_of = ctx.async_connection().await.unwrap();
                connection_to_dispose_of.set_response_timeout(Duration::from_millis(1));

                async fn count_ids(
                    conn: &mut impl redis::aio::ConnectionLike,
                ) -> RedisResult<usize> {
                    let initial_connections: String =
                        cmd("CLIENT").arg("LIST").query_async(conn).await?;

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
                assert_eq!(err.kind(), ErrorKind::IoError);

                drop(connection_to_dispose_of);

                sleep(Duration::from_millis(10).into()).await;

                assert_eq!(count_ids(&mut conn).await.unwrap(), 1);

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_monitor(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let mut conn = ctx.async_connection().await.unwrap();
                let monitor_conn = ctx.client.get_async_monitor().await.unwrap();
                let mut stream = monitor_conn.into_on_message();

                let _: () = conn.set("foo", "bar").await?;

                let msg: String = stream.next().await.unwrap();
                assert!(msg.ends_with("\"SET\" \"foo\" \"bar\""));

                drop(ctx);

                assert!(stream.next().await.is_none());

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[cfg(feature = "tls-rustls")]
    mod mtls_test {
        use super::*;

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn test_should_connect_mtls(#[case] runtime: RuntimeType) {
            let ctx = TestContext::new_with_mtls();

            let client =
                build_single_client(ctx.server.connection_info(), &ctx.server.tls_paths, true)
                    .unwrap();
            let connect = client.get_multiplexed_async_connection();
            block_on_all(
                connect.and_then(|mut con| async move {
                    redis::cmd("SET")
                        .arg("key1")
                        .arg(b"foo")
                        .exec_async(&mut con)
                        .await?;
                    let result = redis::cmd("GET").arg(&["key1"]).query_async(&mut con).await;
                    assert_eq!(result, Ok("foo".to_string()));
                    result
                }),
                runtime,
            )
            .unwrap();
        }

        #[rstest]
        #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
        #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
        #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
        fn test_should_not_connect_if_tls_active(#[case] runtime: RuntimeType) {
            let ctx = TestContext::new_with_mtls();

            let client =
                build_single_client(ctx.server.connection_info(), &ctx.server.tls_paths, false)
                    .unwrap();
            let connect = client.get_multiplexed_async_connection();
            let result = block_on_all(
                connect.and_then(|mut con| async move {
                    redis::cmd("SET")
                        .arg("key1")
                        .arg(b"foo")
                        .exec_async(&mut con)
                        .await?;
                    let result = redis::cmd("GET").arg(&["key1"]).query_async(&mut con).await;
                    assert_eq!(result, Ok("foo".to_string()));
                    result
                }),
                runtime,
            );

            // depends on server type set (REDISRS_SERVER_TYPE)
            match ctx.server.connection_info() {
                redis::ConnectionInfo {
                    addr: redis::ConnectionAddr::TcpTls { .. },
                    ..
                } => {
                    if result.is_ok() {
                        panic!("Must NOT be able to connect without client credentials if server accepts TLS");
                    }
                }
                _ => {
                    if result.is_err() {
                        panic!("Must be able to connect without client credentials if server does NOT accept TLS");
                    }
                }
            }
        }
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    #[cfg(feature = "connection-manager")]
    fn test_resp3_pushes_connection_manager(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        let mut connection_info = ctx.server.connection_info();
        connection_info.redis.protocol = ProtocolVersion::RESP3;
        let client = redis::Client::open(connection_info).unwrap();

        block_on_all(
            async move {
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

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_select_db(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        let mut connection_info = ctx.client.get_connection_info().clone();
        connection_info.redis.db = 5;
        let client = redis::Client::open(connection_info).unwrap();
        block_on_all(
            async move {
                let mut connection = client.get_multiplexed_async_connection().await.unwrap();

                let info: String = redis::cmd("CLIENT")
                    .arg("info")
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                assert!(info.contains("db=5"));

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn test_multiplexed_connection_send_single_disconnect_on_connection_failure(
        #[case] runtime: RuntimeType,
    ) {
        let mut ctx = TestContext::new();
        if ctx.protocol == ProtocolVersion::RESP2 {
            return;
        }
        block_on_all(
            async move {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
                let _res = ctx
                    .client
                    .get_multiplexed_async_connection_with_config(&config)
                    .await?;
                drop(config);
                ctx.stop_server();

                assert_eq!(rx.recv().await.unwrap().kind, PushKind::Disconnection);
                sleep(Duration::from_millis(1).into()).await;
                assert!(rx.try_recv().is_err());
                assert!(rx.is_closed());

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }
}
