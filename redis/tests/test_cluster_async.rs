#![allow(unknown_lints, dependency_on_unit_never_type_fallback)]
#![cfg(feature = "cluster-async")]
mod support;

#[cfg(test)]
mod cluster_async {
    use std::{
        collections::{HashMap, HashSet},
        net::{IpAddr, SocketAddr},
        str::from_utf8,
        sync::{
            atomic::{self, AtomicBool, AtomicI32, AtomicU16, AtomicU32, Ordering},
            Arc,
        },
        time::Duration,
    };

    use futures::prelude::*;
    use futures_time::task::sleep;
    use once_cell::sync::Lazy;

    use redis::{
        aio::{ConnectionLike, MultiplexedConnection},
        cluster::ClusterClient,
        cluster_async::{testing::MANAGEMENT_CONN_NAME, ClusterConnection, Connect},
        cluster_routing::{
            MultipleNodeRoutingInfo, Route, RoutingInfo, SingleNodeRoutingInfo, SlotAddr,
        },
        cluster_topology::{get_slot, DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES},
        cmd, from_owned_redis_value, parse_redis_value, AsyncCommands, Cmd, ErrorKind,
        FromRedisValue, InfoDict, IntoConnectionInfo, ProtocolVersion, PubSubChannelOrPattern,
        PubSubSubscriptionInfo, PubSubSubscriptionKind, PushInfo, PushKind, RedisError,
        RedisFuture, RedisResult, Script, Value,
    };

    use crate::support::*;

    use tokio::sync::mpsc;

    #[test]
    fn test_async_cluster_basic_cmd() {
        let cluster = TestClusterContext::new();

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            cmd("SET")
                .arg("test")
                .arg("test_data")
                .query_async(&mut connection)
                .await?;
            let res: String = cmd("GET")
                .arg("test")
                .clone()
                .query_async(&mut connection)
                .await?;
            assert_eq!(res, "test_data");
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_basic_eval() {
        let cluster = TestClusterContext::new();

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let res: String = cmd("EVAL")
                .arg(r#"redis.call("SET", KEYS[1], ARGV[1]); return redis.call("GET", KEYS[1])"#)
                .arg(1)
                .arg("key")
                .arg("test")
                .query_async(&mut connection)
                .await?;
            assert_eq!(res, "test");
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_basic_script() {
        let cluster = TestClusterContext::new();

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let res: String = Script::new(
                r#"redis.call("SET", KEYS[1], ARGV[1]); return redis.call("GET", KEYS[1])"#,
            )
            .key("key")
            .arg("test")
            .invoke_async(&mut connection)
            .await?;
            assert_eq!(res, "test");
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_route_flush_to_specific_node() {
        let cluster = TestClusterContext::new();

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let _: () = connection.set("foo", "bar").await.unwrap();
            let _: () = connection.set("bar", "foo").await.unwrap();

            let res: String = connection.get("foo").await.unwrap();
            assert_eq!(res, "bar".to_string());
            let res2: Option<String> = connection.get("bar").await.unwrap();
            assert_eq!(res2, Some("foo".to_string()));

            let route =
                redis::cluster_routing::Route::new(1, redis::cluster_routing::SlotAddr::Master);
            let single_node_route =
                redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(route);
            let routing = RoutingInfo::SingleNode(single_node_route);
            assert_eq!(
                connection
                    .route_command(&redis::cmd("FLUSHALL"), routing)
                    .await
                    .unwrap(),
                Value::Okay
            );
            let res: String = connection.get("foo").await.unwrap();
            assert_eq!(res, "bar".to_string());
            let res2: Option<String> = connection.get("bar").await.unwrap();
            assert_eq!(res2, None);
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_route_flush_to_node_by_address() {
        let cluster = TestClusterContext::new();

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let mut cmd = redis::cmd("INFO");
            // The other sections change with time.
            // TODO - after we remove support of redis 6, we can add more than a single section - .arg("Persistence").arg("Memory").arg("Replication")
            cmd.arg("Clients");
            let value = connection
                .route_command(
                    &cmd,
                    RoutingInfo::MultiNode((MultipleNodeRoutingInfo::AllNodes, None)),
                )
                .await
                .unwrap();

            let info_by_address = from_owned_redis_value::<HashMap<String, String>>(value).unwrap();
            // find the info of the first returned node
            let (address, info) = info_by_address.into_iter().next().unwrap();
            let mut split_address = address.split(':');
            let host = split_address.next().unwrap().to_string();
            let port = split_address.next().unwrap().parse().unwrap();

            let value = connection
                .route_command(
                    &cmd,
                    RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress { host, port }),
                )
                .await
                .unwrap();
            let new_info = from_owned_redis_value::<String>(value).unwrap();

            assert_eq!(new_info, info);
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_route_info_to_nodes() {
        let cluster = TestClusterContext::new_with_config(RedisClusterConfiguration {
            nodes: 12,
            replicas: 1,
            ..Default::default()
        });

        let split_to_addresses_and_info = |res| -> (Vec<String>, Vec<String>) {
            if let Value::Map(values) = res {
                let mut pairs: Vec<_> = values
                    .into_iter()
                    .map(|(key, value)| {
                        (
                            redis::from_redis_value::<String>(&key).unwrap(),
                            redis::from_redis_value::<String>(&value).unwrap(),
                        )
                    })
                    .collect();
                pairs.sort_by(|(address1, _), (address2, _)| address1.cmp(address2));
                pairs.into_iter().unzip()
            } else {
                unreachable!("{:?}", res);
            }
        };

        block_on_all(async move {
            let cluster_addresses: Vec<_> = cluster
                .cluster
                .servers
                .iter()
                .map(|server| server.connection_info())
                .collect();
            let client = ClusterClient::builder(cluster_addresses.clone())
                .read_from_replicas()
                .build()?;
            let mut connection = client.get_async_connection(None).await?;

            let route_to_all_nodes = redis::cluster_routing::MultipleNodeRoutingInfo::AllNodes;
            let routing = RoutingInfo::MultiNode((route_to_all_nodes, None));
            let res = connection
                .route_command(&redis::cmd("INFO"), routing)
                .await
                .unwrap();
            let (addresses, infos) = split_to_addresses_and_info(res);

            let mut cluster_addresses: Vec<_> = cluster_addresses
                .into_iter()
                .map(|info| info.addr.to_string())
                .collect();
            cluster_addresses.sort();

            assert_eq!(addresses.len(), 12);
            assert_eq!(addresses, cluster_addresses);
            assert_eq!(infos.len(), 12);
            for i in 0..12 {
                let split: Vec<_> = addresses[i].split(':').collect();
                assert!(infos[i].contains(&format!("tcp_port:{}", split[1])));
            }

            let route_to_all_primaries =
                redis::cluster_routing::MultipleNodeRoutingInfo::AllMasters;
            let routing = RoutingInfo::MultiNode((route_to_all_primaries, None));
            let res = connection
                .route_command(&redis::cmd("INFO"), routing)
                .await
                .unwrap();
            let (addresses, infos) = split_to_addresses_and_info(res);
            assert_eq!(addresses.len(), 6);
            assert_eq!(infos.len(), 6);
            // verify that all primaries have the correct port & host, and are marked as primaries.
            for i in 0..6 {
                assert!(cluster_addresses.contains(&addresses[i]));
                let split: Vec<_> = addresses[i].split(':').collect();
                assert!(infos[i].contains(&format!("tcp_port:{}", split[1])));
                assert!(infos[i].contains("role:primary") || infos[i].contains("role:master"));
            }

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_resp3() {
        if use_protocol() == ProtocolVersion::RESP2 {
            return;
        }
        block_on_all(async move {
            let cluster = TestClusterContext::new();

            let mut connection = cluster.async_connection(None).await;

            let hello: HashMap<String, Value> = redis::cmd("HELLO")
                .query_async(&mut connection)
                .await
                .unwrap();
            assert_eq!(hello.get("proto").unwrap(), &Value::Int(3));

            let _: () = connection.hset("hash", "foo", "baz").await.unwrap();
            let _: () = connection.hset("hash", "bar", "foobar").await.unwrap();
            let result: Value = connection.hgetall("hash").await.unwrap();

            assert_eq!(
                result,
                Value::Map(vec![
                    (
                        Value::BulkString("foo".as_bytes().to_vec()),
                        Value::BulkString("baz".as_bytes().to_vec())
                    ),
                    (
                        Value::BulkString("bar".as_bytes().to_vec()),
                        Value::BulkString("foobar".as_bytes().to_vec())
                    )
                ])
            );

            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_basic_pipe() {
        let cluster = TestClusterContext::new();

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let mut pipe = redis::pipe();
            pipe.add_command(cmd("SET").arg("test").arg("test_data").clone());
            pipe.add_command(cmd("SET").arg("{test}3").arg("test_data3").clone());
            pipe.query_async(&mut connection).await?;
            let res: String = connection.get("test").await?;
            assert_eq!(res, "test_data");
            let res: String = connection.get("{test}3").await?;
            assert_eq!(res, "test_data3");
            Ok::<_, RedisError>(())
        })
        .unwrap()
    }

    #[test]
    fn test_async_cluster_multi_shard_commands() {
        let cluster = TestClusterContext::new();

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;

            let res: String = connection
                .mset(&[("foo", "bar"), ("bar", "foo"), ("baz", "bazz")])
                .await?;
            assert_eq!(res, "OK");
            let res: Vec<String> = connection.mget(&["baz", "foo", "bar"]).await?;
            assert_eq!(res, vec!["bazz", "bar", "foo"]);
            Ok::<_, RedisError>(())
        })
        .unwrap()
    }

    #[test]
    fn test_async_cluster_basic_failover() {
        block_on_all(async move {
            test_failover(
                &TestClusterContext::new_with_config(
                    RedisClusterConfiguration::single_replica_config(),
                ),
                10,
                123,
                false,
            )
            .await;
            Ok::<_, RedisError>(())
        })
        .unwrap()
    }

    async fn do_failover(
        redis: &mut redis::aio::MultiplexedConnection,
    ) -> Result<(), anyhow::Error> {
        cmd("CLUSTER").arg("FAILOVER").query_async(redis).await?;
        Ok(())
    }

    // parameter `_mtls_enabled` can only be used if `feature = tls-rustls` is active
    #[allow(dead_code)]
    async fn test_failover(
        env: &TestClusterContext,
        requests: i32,
        value: i32,
        _mtls_enabled: bool,
    ) {
        let completed = Arc::new(AtomicI32::new(0));

        let connection = env.async_connection(None).await;
        let mut node_conns: Vec<MultiplexedConnection> = Vec::new();

        'outer: loop {
            node_conns.clear();
            let cleared_nodes = async {
                for server in env.cluster.iter_servers() {
                    let addr = server.client_addr();

                    #[cfg(feature = "tls-rustls")]
                    let client = build_single_client(
                        server.connection_info(),
                        &server.tls_paths,
                        _mtls_enabled,
                    )
                    .unwrap_or_else(|e| panic!("Failed to connect to '{addr}': {e}"));

                    #[cfg(not(feature = "tls-rustls"))]
                    let client = redis::Client::open(server.connection_info())
                        .unwrap_or_else(|e| panic!("Failed to connect to '{addr}': {e}"));

                    let mut conn = client
                        .get_multiplexed_async_connection(None)
                        .await
                        .unwrap_or_else(|e| panic!("Failed to get connection: {e}"));

                    let info: InfoDict = redis::Cmd::new()
                        .arg("INFO")
                        .query_async(&mut conn)
                        .await
                        .expect("INFO");
                    let role: String = info.get("role").expect("cluster role");

                    if role == "master" {
                        tokio::time::timeout(std::time::Duration::from_secs(3), async {
                            Ok(redis::Cmd::new()
                                .arg("FLUSHALL")
                                .query_async(&mut conn)
                                .await?)
                        })
                        .await
                        .unwrap_or_else(|err| Err(anyhow::Error::from(err)))?;
                    }

                    node_conns.push(conn);
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            match cleared_nodes {
                Ok(()) => break 'outer,
                Err(err) => {
                    // Failed to clear the databases, retry
                    tracing::warn!("{}", err);
                }
            }
        }

        (0..requests + 1)
            .map(|i| {
                let mut connection = connection.clone();
                let mut node_conns = node_conns.clone();
                let completed = completed.clone();
                async move {
                    if i == requests / 2 {
                        // Failover all the nodes, error only if all the failover requests error
                        let mut results = future::join_all(
                            node_conns
                                .iter_mut()
                                .map(|conn| Box::pin(do_failover(conn))),
                        )
                        .await;
                        if results.iter().all(|res| res.is_err()) {
                            results.pop().unwrap()
                        } else {
                            Ok::<_, anyhow::Error>(())
                        }
                    } else {
                        let key = format!("test-{value}-{i}");
                        cmd("SET")
                            .arg(&key)
                            .arg(i)
                            .clone()
                            .query_async(&mut connection)
                            .await?;
                        let res: i32 = cmd("GET")
                            .arg(key)
                            .clone()
                            .query_async(&mut connection)
                            .await?;
                        assert_eq!(res, i);
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, anyhow::Error>(())
                    }
                }
            })
            .collect::<stream::FuturesUnordered<_>>()
            .try_collect()
            .await
            .unwrap_or_else(|e| panic!("{e}"));

        assert_eq!(
            completed.load(Ordering::SeqCst),
            requests,
            "Some requests never completed!"
        );
    }

    static ERROR: Lazy<AtomicBool> = Lazy::new(Default::default);

    #[derive(Clone)]
    struct ErrorConnection {
        inner: MultiplexedConnection,
    }

    impl Connect for ErrorConnection {
        fn connect<'a, T>(
            info: T,
            response_timeout: std::time::Duration,
            connection_timeout: std::time::Duration,
            socket_addr: Option<SocketAddr>,
            push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
        ) -> RedisFuture<'a, (Self, Option<IpAddr>)>
        where
            T: IntoConnectionInfo + Send + 'a,
        {
            Box::pin(async move {
                let (inner, _ip) = MultiplexedConnection::connect(
                    info,
                    response_timeout,
                    connection_timeout,
                    socket_addr,
                    push_sender,
                )
                .await?;
                Ok((ErrorConnection { inner }, None))
            })
        }
    }

    impl ConnectionLike for ErrorConnection {
        fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
            if ERROR.load(Ordering::SeqCst) {
                Box::pin(async move { Err(RedisError::from((redis::ErrorKind::Moved, "ERROR"))) })
            } else {
                self.inner.req_packed_command(cmd)
            }
        }

        fn req_packed_commands<'a>(
            &'a mut self,
            pipeline: &'a redis::Pipeline,
            offset: usize,
            count: usize,
        ) -> RedisFuture<'a, Vec<Value>> {
            self.inner.req_packed_commands(pipeline, offset, count)
        }

        fn get_db(&self) -> i64 {
            self.inner.get_db()
        }
    }

    #[test]
    fn test_async_cluster_error_in_inner_connection() {
        let cluster = TestClusterContext::new();

        block_on_all(async move {
            let mut con = cluster.async_generic_connection::<ErrorConnection>().await;

            ERROR.store(false, Ordering::SeqCst);
            let r: Option<i32> = con.get("test").await?;
            assert_eq!(r, None::<i32>);

            ERROR.store(true, Ordering::SeqCst);

            let result: RedisResult<()> = con.get("test").await;
            assert_eq!(
                result,
                Err(RedisError::from((redis::ErrorKind::Moved, "ERROR")))
            );

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
    fn test_async_cluster_async_std_basic_cmd() {
        let cluster = TestClusterContext::new();

        block_on_all_using_async_std(async {
            let mut connection = cluster.async_connection(None).await;
            redis::cmd("SET")
                .arg("test")
                .arg("test_data")
                .query_async(&mut connection)
                .await?;
            redis::cmd("GET")
                .arg("test")
                .clone()
                .query_async(&mut connection)
                .map_ok(|res: String| {
                    assert_eq!(res, "test_data");
                })
                .await
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_can_connect_to_server_that_sends_cluster_slots_without_host_name() {
        let name =
            "test_async_cluster_can_connect_to_server_that_sends_cluster_slots_without_host_name";

        let MockEnv {
            runtime,
            async_connection: mut connection,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], _| {
            if contains_slice(cmd, b"PING") {
                Err(Ok(Value::SimpleString("OK".into())))
            } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                Err(Ok(Value::Array(vec![Value::Array(vec![
                    Value::Int(0),
                    Value::Int(16383),
                    Value::Array(vec![
                        Value::BulkString("".as_bytes().to_vec()),
                        Value::Int(6379),
                    ]),
                ])])))
            } else {
                Err(Ok(Value::Nil))
            }
        });

        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Value>(&mut connection),
        );

        assert_eq!(value, Ok(Value::Nil));
    }

    #[test]
    fn test_async_cluster_can_connect_to_server_that_sends_cluster_slots_with_null_host_name() {
        let name =
            "test_async_cluster_can_connect_to_server_that_sends_cluster_slots_with_null_host_name";

        let MockEnv {
            runtime,
            async_connection: mut connection,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], _| {
            if contains_slice(cmd, b"PING") {
                Err(Ok(Value::SimpleString("OK".into())))
            } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                Err(Ok(Value::Array(vec![Value::Array(vec![
                    Value::Int(0),
                    Value::Int(16383),
                    Value::Array(vec![Value::Nil, Value::Int(6379)]),
                ])])))
            } else {
                Err(Ok(Value::Nil))
            }
        });

        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Value>(&mut connection),
        );

        assert_eq!(value, Ok(Value::Nil));
    }

    #[test]
    fn test_async_cluster_cannot_connect_to_server_with_unknown_host_name() {
        let name = "test_async_cluster_cannot_connect_to_server_with_unknown_host_name";
        let handler = move |cmd: &[u8], _| {
            if contains_slice(cmd, b"PING") {
                Err(Ok(Value::SimpleString("OK".into())))
            } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                Err(Ok(Value::Array(vec![Value::Array(vec![
                    Value::Int(0),
                    Value::Int(16383),
                    Value::Array(vec![
                        Value::BulkString("?".as_bytes().to_vec()),
                        Value::Int(6379),
                    ]),
                ])])))
            } else {
                Err(Ok(Value::Nil))
            }
        };
        let client_builder = ClusterClient::builder(vec![&*format!("redis://{name}")]);
        let client: ClusterClient = client_builder.build().unwrap();
        let _handler = MockConnectionBehavior::register_new(name, Arc::new(handler));
        let connection = client.get_generic_connection::<MockConnection>(None);
        assert!(connection.is_err());
        let err = connection.err().unwrap();
        assert!(err
            .to_string()
            .contains("Error parsing slots: No healthy node found"))
    }

    #[test]
    fn test_async_cluster_can_connect_to_server_that_sends_cluster_slots_with_partial_nodes_with_unknown_host_name(
    ) {
        let name = "test_async_cluster_can_connect_to_server_that_sends_cluster_slots_with_partial_nodes_with_unknown_host_name";

        let MockEnv {
            runtime,
            async_connection: mut connection,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], _| {
            if contains_slice(cmd, b"PING") {
                Err(Ok(Value::SimpleString("OK".into())))
            } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                Err(Ok(Value::Array(vec![
                    Value::Array(vec![
                        Value::Int(0),
                        Value::Int(7000),
                        Value::Array(vec![
                            Value::BulkString(name.as_bytes().to_vec()),
                            Value::Int(6379),
                        ]),
                    ]),
                    Value::Array(vec![
                        Value::Int(7001),
                        Value::Int(16383),
                        Value::Array(vec![
                            Value::BulkString("?".as_bytes().to_vec()),
                            Value::Int(6380),
                        ]),
                    ]),
                ])))
            } else {
                Err(Ok(Value::Nil))
            }
        });

        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Value>(&mut connection),
        );

        assert_eq!(value, Ok(Value::Nil));
    }

    #[test]
    fn test_async_cluster_retries() {
        let name = "tryagain";

        let requests = atomic::AtomicUsize::new(0);
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(5),
            name,
            move |cmd: &[u8], _| {
                respond_startup(name, cmd)?;

                match requests.fetch_add(1, atomic::Ordering::SeqCst) {
                    0..=4 => Err(parse_redis_value(b"-TRYAGAIN mock\r\n")),
                    _ => Err(Ok(Value::BulkString(b"123".to_vec()))),
                }
            },
        );

        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(&mut connection),
        );

        assert_eq!(value, Ok(Some(123)));
    }

    #[test]
    fn test_async_cluster_tryagain_exhaust_retries() {
        let name = "tryagain_exhaust_retries";

        let requests = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(2),
            name,
            {
                let requests = requests.clone();
                move |cmd: &[u8], _| {
                    respond_startup(name, cmd)?;
                    requests.fetch_add(1, atomic::Ordering::SeqCst);
                    Err(parse_redis_value(b"-TRYAGAIN mock\r\n"))
                }
            },
        );

        let result = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(&mut connection),
        );

        match result {
            Ok(_) => panic!("result should be an error"),
            Err(e) => match e.kind() {
                ErrorKind::TryAgain => {}
                _ => panic!("Expected TryAgain but got {:?}", e.kind()),
            },
        }
        assert_eq!(requests.load(atomic::Ordering::SeqCst), 3);
    }

    // Obtain the view index associated with the node with [called_port] port
    fn get_node_view_index(num_of_views: usize, ports: &Vec<u16>, called_port: u16) -> usize {
        let port_index = ports
            .iter()
            .position(|&p| p == called_port)
            .unwrap_or_else(|| {
                panic!(
                    "CLUSTER SLOTS was called with unknown port: {called_port}; Known ports: {:?}",
                    ports
                )
            });
        // If we have less views than nodes, use the last view
        if port_index < num_of_views {
            port_index
        } else {
            num_of_views - 1
        }
    }
    #[test]
    fn test_async_cluster_move_error_when_new_node_is_added() {
        let name = "rebuild_with_extra_nodes";

        let requests = atomic::AtomicUsize::new(0);
        let started = atomic::AtomicBool::new(false);
        let refreshed_map = HashMap::from([
            (6379, atomic::AtomicBool::new(false)),
            (6380, atomic::AtomicBool::new(false)),
        ]);

        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], port| {
            if !started.load(atomic::Ordering::SeqCst) {
                respond_startup(name, cmd)?;
            }
            started.store(true, atomic::Ordering::SeqCst);

            if contains_slice(cmd, b"PING") || contains_slice(cmd, b"SETNAME") {
                return Err(Ok(Value::SimpleString("OK".into())));
            }

            let i = requests.fetch_add(1, atomic::Ordering::SeqCst);

            let is_get_cmd = contains_slice(cmd, b"GET");
            let get_response = Err(Ok(Value::BulkString(b"123".to_vec())));
            match i {
                // Respond that the key exists on a node that does not yet have a connection:
                0 => Err(parse_redis_value(
                    format!("-MOVED 123 {name}:6380\r\n").as_bytes(),
                )),
                _ => {
                    if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                        // Should not attempt to refresh slots more than once,
                        // so we expect a single CLUSTER NODES request for each node
                        assert!(!refreshed_map
                            .get(&port)
                            .unwrap()
                            .swap(true, Ordering::SeqCst));
                        Err(Ok(Value::Array(vec![
                            Value::Array(vec![
                                Value::Int(0),
                                Value::Int(1),
                                Value::Array(vec![
                                    Value::BulkString(name.as_bytes().to_vec()),
                                    Value::Int(6379),
                                ]),
                            ]),
                            Value::Array(vec![
                                Value::Int(2),
                                Value::Int(16383),
                                Value::Array(vec![
                                    Value::BulkString(name.as_bytes().to_vec()),
                                    Value::Int(6380),
                                ]),
                            ]),
                        ])))
                    } else {
                        assert_eq!(port, 6380);
                        assert!(is_get_cmd, "{:?}", std::str::from_utf8(cmd));
                        get_response
                    }
                }
            }
        });

        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(&mut connection),
        );

        assert_eq!(value, Ok(Some(123)));
    }

    fn test_async_cluster_refresh_topology_after_moved_assert_get_succeed_and_expected_retries(
        slots_config_vec: Vec<Vec<MockSlotRange>>,
        ports: Vec<u16>,
        has_a_majority: bool,
    ) {
        assert!(!ports.is_empty() && !slots_config_vec.is_empty());
        let name = "refresh_topology_moved";
        let num_of_nodes = ports.len();
        let requests = atomic::AtomicUsize::new(0);
        let started = atomic::AtomicBool::new(false);
        let refresh_calls = Arc::new(atomic::AtomicUsize::new(0));
        let refresh_calls_cloned = refresh_calls.clone();
        let MockEnv {
            runtime,
            async_connection: mut connection,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], port| {
            if !started.load(atomic::Ordering::SeqCst) {
                respond_startup_with_replica_using_config(
                    name,
                    cmd,
                    Some(slots_config_vec[0].clone()),
                )?;
            }
            started.store(true, atomic::Ordering::SeqCst);

            if contains_slice(cmd, b"PING") {
                return Err(Ok(Value::SimpleString("OK".into())));
            }

            let i = requests.fetch_add(1, atomic::Ordering::SeqCst);
            let is_get_cmd = contains_slice(cmd, b"GET");
            let get_response = Err(Ok(Value::BulkString(b"123".to_vec())));
            let moved_node = ports[0];
            match i {
                // Respond that the key exists on a node that does not yet have a connection:
                0 => Err(parse_redis_value(
                    format!("-MOVED 123 {name}:{moved_node}\r\n").as_bytes(),
                )),
                _ => {
                    if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                        refresh_calls_cloned.fetch_add(1, atomic::Ordering::SeqCst);
                        let view_index = get_node_view_index(slots_config_vec.len(), &ports, port);
                        Err(Ok(create_topology_from_config(
                            name,
                            slots_config_vec[view_index].clone(),
                        )))
                    } else {
                        assert_eq!(port, moved_node);
                        assert!(is_get_cmd, "{:?}", std::str::from_utf8(cmd));
                        get_response
                    }
                }
            }
        });
        runtime.block_on(async move {
        let res = cmd("GET")
            .arg("test")
            .query_async::<_, Option<i32>>(&mut connection)
            .await;
        assert_eq!(res, Ok(Some(123)));
        // If there is a majority in the topology views, or if it's a 2-nodes cluster, we shall be able to calculate the topology on the first try, 
        // so each node will be queried only once with CLUSTER SLOTS.
        // Otherwise, if we don't have a majority, we expect to see the refresh_slots function being called with the maximum retry number.
        let expected_calls = if has_a_majority || num_of_nodes == 2 {num_of_nodes} else {DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES * num_of_nodes};
        let mut refreshed_calls = 0;
        for _ in 0..100 {
            refreshed_calls = refresh_calls.load(atomic::Ordering::Relaxed);
            if refreshed_calls == expected_calls {
                return;
            } else {
                let sleep_duration = core::time::Duration::from_millis(100);
                #[cfg(feature = "tokio-comp")]
                tokio::time::sleep(sleep_duration).await;

                #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
                async_std::task::sleep(sleep_duration).await;
            }
        }
        panic!("Failed to reach to the expected topology refresh retries. Found={refreshed_calls}, Expected={expected_calls}")
    });
    }

    fn test_async_cluster_refresh_topology_in_client_init_get_succeed(
        slots_config_vec: Vec<Vec<MockSlotRange>>,
        ports: Vec<u16>,
    ) {
        assert!(!ports.is_empty() && !slots_config_vec.is_empty());
        let name = "refresh_topology_client_init";
        let started = atomic::AtomicBool::new(false);
        let MockEnv {
            runtime,
            async_connection: mut connection,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder::<String>(
                ports
                    .iter()
                    .map(|port| format!("redis://{name}:{port}"))
                    .collect::<Vec<_>>(),
            ),
            name,
            move |cmd: &[u8], port| {
                let is_started = started.load(atomic::Ordering::SeqCst);
                if !is_started {
                    if contains_slice(cmd, b"PING") || contains_slice(cmd, b"SETNAME") {
                        return Err(Ok(Value::SimpleString("OK".into())));
                    } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                        let view_index = get_node_view_index(slots_config_vec.len(), &ports, port);
                        return Err(Ok(create_topology_from_config(
                            name,
                            slots_config_vec[view_index].clone(),
                        )));
                    } else if contains_slice(cmd, b"READONLY") {
                        return Err(Ok(Value::SimpleString("OK".into())));
                    }
                }
                started.store(true, atomic::Ordering::SeqCst);
                if contains_slice(cmd, b"PING") {
                    return Err(Ok(Value::SimpleString("OK".into())));
                }

                let is_get_cmd = contains_slice(cmd, b"GET");
                let get_response = Err(Ok(Value::BulkString(b"123".to_vec())));
                {
                    assert!(is_get_cmd, "{:?}", std::str::from_utf8(cmd));
                    get_response
                }
            },
        );
        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(&mut connection),
        );

        assert_eq!(value, Ok(Some(123)));
    }

    fn generate_topology_view(
        ports: &[u16],
        interval: usize,
        full_slot_coverage: bool,
    ) -> Vec<MockSlotRange> {
        let mut slots_res = vec![];
        let mut start_pos: usize = 0;
        for (idx, port) in ports.iter().enumerate() {
            let end_pos: usize = if idx == ports.len() - 1 && full_slot_coverage {
                16383
            } else {
                start_pos + interval
            };
            let mock_slot = MockSlotRange {
                primary_port: *port,
                replica_ports: vec![],
                slot_range: (start_pos as u16..end_pos as u16),
            };
            slots_res.push(mock_slot);
            start_pos = end_pos + 1;
        }
        slots_res
    }

    fn get_ports(num_of_nodes: usize) -> Vec<u16> {
        (6379_u16..6379 + num_of_nodes as u16).collect()
    }

    fn get_no_majority_topology_view(ports: &[u16]) -> Vec<Vec<MockSlotRange>> {
        let mut result = vec![];
        let mut full_coverage = true;
        for i in 0..ports.len() {
            result.push(generate_topology_view(ports, i + 1, full_coverage));
            full_coverage = !full_coverage;
        }
        result
    }

    fn get_topology_with_majority(ports: &[u16]) -> Vec<Vec<MockSlotRange>> {
        let view: Vec<MockSlotRange> = generate_topology_view(ports, 10, true);
        let result: Vec<_> = ports.iter().map(|_| view.clone()).collect();
        result
    }

    #[test]
    fn test_async_cluster_refresh_topology_after_moved_error_all_nodes_agree_get_succeed() {
        let ports = get_ports(3);
        test_async_cluster_refresh_topology_after_moved_assert_get_succeed_and_expected_retries(
            get_topology_with_majority(&ports),
            ports,
            true,
        );
    }

    #[test]
    fn test_async_cluster_refresh_topology_in_client_init_all_nodes_agree_get_succeed() {
        let ports = get_ports(3);
        test_async_cluster_refresh_topology_in_client_init_get_succeed(
            get_topology_with_majority(&ports),
            ports,
        );
    }

    #[test]
    fn test_async_cluster_refresh_topology_after_moved_error_with_no_majority_get_succeed() {
        for num_of_nodes in 2..4 {
            let ports = get_ports(num_of_nodes);
            test_async_cluster_refresh_topology_after_moved_assert_get_succeed_and_expected_retries(
                get_no_majority_topology_view(&ports),
                ports,
                false,
            );
        }
    }

    #[test]
    fn test_async_cluster_refresh_topology_in_client_init_with_no_majority_get_succeed() {
        for num_of_nodes in 2..4 {
            let ports = get_ports(num_of_nodes);
            test_async_cluster_refresh_topology_in_client_init_get_succeed(
                get_no_majority_topology_view(&ports),
                ports,
            );
        }
    }

    #[test]
    fn test_async_cluster_ask_redirect() {
        let name = "node";
        let completed = Arc::new(AtomicI32::new(0));
        let MockEnv {
            async_connection: mut connection,
            handler: _handler,
            runtime,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]),
            name,
            {
                move |cmd: &[u8], port| {
                    respond_startup_two_nodes(name, cmd)?;
                    // Error twice with io-error, ensure connection is reestablished w/out calling
                    // other node (i.e., not doing a full slot rebuild)
                    let count = completed.fetch_add(1, Ordering::SeqCst);
                    match port {
                        6379 => match count {
                            0 => Err(parse_redis_value(b"-ASK 14000 node:6380\r\n")),
                            _ => panic!("Node should not be called now"),
                        },
                        6380 => match count {
                            1 => {
                                assert!(contains_slice(cmd, b"ASKING"));
                                Err(Ok(Value::Okay))
                            }
                            2 => {
                                assert!(contains_slice(cmd, b"GET"));
                                Err(Ok(Value::BulkString(b"123".to_vec())))
                            }
                            _ => panic!("Node should not be called now"),
                        },
                        _ => panic!("Wrong node"),
                    }
                }
            },
        );

        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(&mut connection),
        );

        assert_eq!(value, Ok(Some(123)));
    }

    #[test]
    fn test_async_cluster_ask_save_new_connection() {
        let name = "node";
        let ping_attempts = Arc::new(AtomicI32::new(0));
        let ping_attempts_clone = ping_attempts.clone();
        let MockEnv {
            async_connection: mut connection,
            handler: _handler,
            runtime,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]),
            name,
            {
                move |cmd: &[u8], port| {
                    if port != 6391 {
                        respond_startup_two_nodes(name, cmd)?;
                        return Err(parse_redis_value(b"-ASK 14000 node:6391\r\n"));
                    }

                    if contains_slice(cmd, b"PING") {
                        ping_attempts_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    respond_startup_two_nodes(name, cmd)?;
                    Err(Ok(Value::Okay))
                }
            },
        );

        for _ in 0..4 {
            runtime
                .block_on(
                    cmd("GET")
                        .arg("test")
                        .query_async::<_, Value>(&mut connection),
                )
                .unwrap();
        }

        assert_eq!(ping_attempts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_async_cluster_reset_routing_if_redirect_fails() {
        let name = "test_async_cluster_reset_routing_if_redirect_fails";
        let completed = Arc::new(AtomicI32::new(0));
        let MockEnv {
            async_connection: mut connection,
            handler: _handler,
            runtime,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], port| {
            if port != 6379 && port != 6380 {
                return Err(Err(RedisError::from(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "mock-io-error",
                ))));
            }
            respond_startup_two_nodes(name, cmd)?;
            let count = completed.fetch_add(1, Ordering::SeqCst);
            match (port, count) {
                // redirect once to non-existing node
                (6379, 0) => Err(parse_redis_value(
                    format!("-ASK 14000 {name}:9999\r\n").as_bytes(),
                )),
                // accept the next request
                (6379, 1) => {
                    assert!(contains_slice(cmd, b"GET"));
                    Err(Ok(Value::BulkString(b"123".to_vec())))
                }
                _ => panic!("Wrong node. port: {port}, received count: {count}"),
            }
        });

        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(&mut connection),
        );

        assert_eq!(value, Ok(Some(123)));
    }

    #[test]
    fn test_async_cluster_ask_redirect_even_if_original_call_had_no_route() {
        let name = "node";
        let completed = Arc::new(AtomicI32::new(0));
        let MockEnv {
            async_connection: mut connection,
            handler: _handler,
            runtime,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]),
            name,
            {
                move |cmd: &[u8], port| {
                    respond_startup_two_nodes(name, cmd)?;
                    // Error twice with io-error, ensure connection is reestablished w/out calling
                    // other node (i.e., not doing a full slot rebuild)
                    let count = completed.fetch_add(1, Ordering::SeqCst);
                    if count == 0 {
                        return Err(parse_redis_value(b"-ASK 14000 node:6380\r\n"));
                    }
                    match port {
                        6380 => match count {
                            1 => {
                                assert!(
                                    contains_slice(cmd, b"ASKING"),
                                    "{:?}",
                                    std::str::from_utf8(cmd)
                                );
                                Err(Ok(Value::Okay))
                            }
                            2 => {
                                assert!(contains_slice(cmd, b"EVAL"));
                                Err(Ok(Value::Okay))
                            }
                            _ => panic!("Node should not be called now"),
                        },
                        _ => panic!("Wrong node"),
                    }
                }
            },
        );

        let value = runtime.block_on(
            cmd("EVAL") // Eval command has no directed, and so is redirected randomly
                .query_async::<_, Value>(&mut connection),
        );

        assert_eq!(value, Ok(Value::Okay));
    }

    #[test]
    fn test_async_cluster_ask_error_when_new_node_is_added() {
        let name = "ask_with_extra_nodes";

        let requests = atomic::AtomicUsize::new(0);
        let started = atomic::AtomicBool::new(false);

        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], port| {
            if !started.load(atomic::Ordering::SeqCst) {
                respond_startup(name, cmd)?;
            }
            started.store(true, atomic::Ordering::SeqCst);

            if contains_slice(cmd, b"PING") || contains_slice(cmd, b"SETNAME") {
                return Err(Ok(Value::SimpleString("OK".into())));
            }

            let i = requests.fetch_add(1, atomic::Ordering::SeqCst);

            match i {
                // Respond that the key exists on a node that does not yet have a connection:
                0 => Err(parse_redis_value(
                    format!("-ASK 123 {name}:6380\r\n").as_bytes(),
                )),
                1 => {
                    assert_eq!(port, 6380);
                    assert!(contains_slice(cmd, b"ASKING"));
                    Err(Ok(Value::Okay))
                }
                2 => {
                    assert_eq!(port, 6380);
                    assert!(contains_slice(cmd, b"GET"));
                    Err(Ok(Value::BulkString(b"123".to_vec())))
                }
                _ => {
                    panic!("Unexpected request: {:?}", cmd);
                }
            }
        });

        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(&mut connection),
        );

        assert_eq!(value, Ok(Some(123)));
    }

    #[test]
    fn test_async_cluster_replica_read() {
        let name = "node";

        // requests should route to replica
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |cmd: &[u8], port| {
                respond_startup_with_replica(name, cmd)?;
                match port {
                    6380 => Err(Ok(Value::BulkString(b"123".to_vec()))),
                    _ => panic!("Wrong node"),
                }
            },
        );

        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(&mut connection),
        );
        assert_eq!(value, Ok(Some(123)));

        // requests should route to primary
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |cmd: &[u8], port| {
                respond_startup_with_replica(name, cmd)?;
                match port {
                    6379 => Err(Ok(Value::SimpleString("OK".into()))),
                    _ => panic!("Wrong node"),
                }
            },
        );

        let value = runtime.block_on(
            cmd("SET")
                .arg("test")
                .arg("123")
                .query_async::<_, Option<Value>>(&mut connection),
        );
        assert_eq!(value, Ok(Some(Value::SimpleString("OK".to_owned()))));
    }

    fn test_async_cluster_fan_out(
        command: &'static str,
        expected_ports: Vec<u16>,
        slots_config: Option<Vec<MockSlotRange>>,
    ) {
        let name = "node";
        let found_ports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ports_clone = found_ports.clone();
        let mut cmd = Cmd::new();
        for arg in command.split_whitespace() {
            cmd.arg(arg);
        }
        let packed_cmd = cmd.get_packed_command();
        // requests should route to replica
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(
                    name,
                    received_cmd,
                    slots_config.clone(),
                )?;
                if received_cmd == packed_cmd {
                    ports_clone.lock().unwrap().push(port);
                    return Err(Ok(Value::SimpleString("OK".into())));
                }
                Ok(())
            },
        );

        let _ = runtime.block_on(cmd.query_async::<_, Option<()>>(&mut connection));
        found_ports.lock().unwrap().sort();
        // MockEnv creates 2 mock connections.
        assert_eq!(*found_ports.lock().unwrap(), expected_ports);
    }

    #[test]
    fn test_async_cluster_fan_out_to_all_primaries() {
        test_async_cluster_fan_out("FLUSHALL", vec![6379, 6381], None);
    }

    #[test]
    fn test_async_cluster_fan_out_to_all_nodes() {
        test_async_cluster_fan_out("CONFIG SET", vec![6379, 6380, 6381, 6382], None);
    }

    #[test]
    fn test_async_cluster_fan_out_once_to_each_primary_when_no_replicas_are_available() {
        test_async_cluster_fan_out(
            "CONFIG SET",
            vec![6379, 6381],
            Some(vec![
                MockSlotRange {
                    primary_port: 6379,
                    replica_ports: Vec::new(),
                    slot_range: (0..8191),
                },
                MockSlotRange {
                    primary_port: 6381,
                    replica_ports: Vec::new(),
                    slot_range: (8192..16383),
                },
            ]),
        );
    }

    #[test]
    fn test_async_cluster_fan_out_once_even_if_primary_has_multiple_slot_ranges() {
        test_async_cluster_fan_out(
            "CONFIG SET",
            vec![6379, 6380, 6381, 6382],
            Some(vec![
                MockSlotRange {
                    primary_port: 6379,
                    replica_ports: vec![6380],
                    slot_range: (0..4000),
                },
                MockSlotRange {
                    primary_port: 6381,
                    replica_ports: vec![6382],
                    slot_range: (4001..8191),
                },
                MockSlotRange {
                    primary_port: 6379,
                    replica_ports: vec![6380],
                    slot_range: (8192..8200),
                },
                MockSlotRange {
                    primary_port: 6381,
                    replica_ports: vec![6382],
                    slot_range: (8201..16383),
                },
            ]),
        );
    }

    #[test]
    fn test_async_cluster_route_according_to_passed_argument() {
        let name = "test_async_cluster_route_according_to_passed_argument";

        let touched_ports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let cloned_ports = touched_ports.clone();

        // requests should route to replica
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |cmd: &[u8], port| {
                respond_startup_with_replica(name, cmd)?;
                cloned_ports.lock().unwrap().push(port);
                Err(Ok(Value::Nil))
            },
        );

        let mut cmd = cmd("GET");
        cmd.arg("test");
        let _ = runtime.block_on(connection.route_command(
            &cmd,
            RoutingInfo::MultiNode((MultipleNodeRoutingInfo::AllMasters, None)),
        ));
        {
            let mut touched_ports = touched_ports.lock().unwrap();
            touched_ports.sort();
            assert_eq!(*touched_ports, vec![6379, 6381]);
            touched_ports.clear();
        }

        let _ = runtime.block_on(connection.route_command(
            &cmd,
            RoutingInfo::MultiNode((MultipleNodeRoutingInfo::AllNodes, None)),
        ));
        {
            let mut touched_ports = touched_ports.lock().unwrap();
            touched_ports.sort();
            assert_eq!(*touched_ports, vec![6379, 6380, 6381, 6382]);
            touched_ports.clear();
        }

        let _ = runtime.block_on(connection.route_command(
            &cmd,
            RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                host: name.to_string(),
                port: 6382,
            }),
        ));
        {
            let mut touched_ports = touched_ports.lock().unwrap();
            touched_ports.sort();
            assert_eq!(*touched_ports, vec![6382]);
            touched_ports.clear();
        }
    }

    #[test]
    fn test_async_cluster_fan_out_and_aggregate_numeric_response_with_min() {
        let name = "test_async_cluster_fan_out_and_aggregate_numeric_response";
        let mut cmd = Cmd::new();
        cmd.arg("SLOWLOG").arg("LEN");

        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;

                let res = 6383 - port as i64;
                Err(Ok(Value::Int(res))) // this results in 1,2,3,4
            },
        );

        let result = runtime
            .block_on(cmd.query_async::<_, i64>(&mut connection))
            .unwrap();
        assert_eq!(result, 10, "{result}");
    }

    #[test]
    fn test_async_cluster_fan_out_and_aggregate_logical_array_response() {
        let name = "test_async_cluster_fan_out_and_aggregate_logical_array_response";
        let mut cmd = Cmd::new();
        cmd.arg("SCRIPT")
            .arg("EXISTS")
            .arg("foo")
            .arg("bar")
            .arg("baz")
            .arg("barvaz");

        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;

                if port == 6381 {
                    return Err(Ok(Value::Array(vec![
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(1),
                        Value::Int(1),
                    ])));
                } else if port == 6379 {
                    return Err(Ok(Value::Array(vec![
                        Value::Int(0),
                        Value::Int(1),
                        Value::Int(0),
                        Value::Int(1),
                    ])));
                }

                panic!("unexpected port {port}");
            },
        );

        let result = runtime
            .block_on(cmd.query_async::<_, Vec<i64>>(&mut connection))
            .unwrap();
        assert_eq!(result, vec![0, 0, 0, 1], "{result:?}");
    }

    #[test]
    fn test_async_cluster_fan_out_and_return_one_succeeded_response() {
        let name = "test_async_cluster_fan_out_and_return_one_succeeded_response";
        let mut cmd = Cmd::new();
        cmd.arg("SCRIPT").arg("KILL");
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                if port == 6381 {
                    return Err(Ok(Value::Okay));
                } else if port == 6379 {
                    return Err(Err((
                        ErrorKind::NotBusy,
                        "No scripts in execution right now",
                    )
                        .into()));
                }

                panic!("unexpected port {port}");
            },
        );

        let result = runtime
            .block_on(cmd.query_async::<_, Value>(&mut connection))
            .unwrap();
        assert_eq!(result, Value::Okay, "{result:?}");
    }

    #[test]
    fn test_async_cluster_fan_out_and_fail_one_succeeded_if_there_are_no_successes() {
        let name = "test_async_cluster_fan_out_and_fail_one_succeeded_if_there_are_no_successes";
        let mut cmd = Cmd::new();
        cmd.arg("SCRIPT").arg("KILL");
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], _port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;

                Err(Err((
                    ErrorKind::NotBusy,
                    "No scripts in execution right now",
                )
                    .into()))
            },
        );

        let result = runtime
            .block_on(cmd.query_async::<_, Value>(&mut connection))
            .unwrap_err();
        assert_eq!(result.kind(), ErrorKind::NotBusy, "{:?}", result.kind());
    }

    #[test]
    fn test_async_cluster_fan_out_and_return_all_succeeded_response() {
        let name = "test_async_cluster_fan_out_and_return_all_succeeded_response";
        let cmd = cmd("FLUSHALL");
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], _port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                Err(Ok(Value::Okay))
            },
        );

        let result = runtime
            .block_on(cmd.query_async::<_, Value>(&mut connection))
            .unwrap();
        assert_eq!(result, Value::Okay, "{result:?}");
    }

    #[test]
    fn test_async_cluster_fan_out_and_fail_all_succeeded_if_there_is_a_single_failure() {
        let name = "test_async_cluster_fan_out_and_fail_all_succeeded_if_there_is_a_single_failure";
        let cmd = cmd("FLUSHALL");
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                if port == 6381 {
                    return Err(Err((
                        ErrorKind::NotBusy,
                        "No scripts in execution right now",
                    )
                        .into()));
                }
                Err(Ok(Value::Okay))
            },
        );

        let result = runtime
            .block_on(cmd.query_async::<_, Value>(&mut connection))
            .unwrap_err();
        assert_eq!(result.kind(), ErrorKind::NotBusy, "{:?}", result.kind());
    }

    #[test]
    fn test_async_cluster_fan_out_and_return_one_succeeded_ignoring_empty_values() {
        let name = "test_async_cluster_fan_out_and_return_one_succeeded_ignoring_empty_values";
        let cmd = cmd("RANDOMKEY");
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                if port == 6381 {
                    return Err(Ok(Value::BulkString("foo".as_bytes().to_vec())));
                }
                Err(Ok(Value::Nil))
            },
        );

        let result = runtime
            .block_on(cmd.query_async::<_, String>(&mut connection))
            .unwrap();
        assert_eq!(result, "foo", "{result:?}");
    }

    #[test]
    fn test_async_cluster_fan_out_and_return_map_of_results_for_special_response_policy() {
        let name = "foo";
        let mut cmd = Cmd::new();
        cmd.arg("LATENCY").arg("LATEST");
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                Err(Ok(Value::BulkString(
                    format!("latency: {port}").into_bytes(),
                )))
            },
        );

        // TODO once RESP3 is in, return this as a map
        let mut result = runtime
            .block_on(cmd.query_async::<_, Vec<(String, String)>>(&mut connection))
            .unwrap();
        result.sort();
        assert_eq!(
            result,
            vec![
                (format!("{name}:6379"), "latency: 6379".to_string()),
                (format!("{name}:6380"), "latency: 6380".to_string()),
                (format!("{name}:6381"), "latency: 6381".to_string()),
                (format!("{name}:6382"), "latency: 6382".to_string())
            ],
            "{result:?}"
        );
    }

    #[test]
    fn test_async_cluster_fan_out_and_combine_arrays_of_values() {
        let name = "foo";
        let cmd = cmd("KEYS");
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                Err(Ok(Value::Array(vec![Value::BulkString(
                    format!("key:{port}").into_bytes(),
                )])))
            },
        );

        let mut result = runtime
            .block_on(cmd.query_async::<_, Vec<String>>(&mut connection))
            .unwrap();
        result.sort();
        assert_eq!(
            result,
            vec!["key:6379".to_string(), "key:6381".to_string(),],
            "{result:?}"
        );
    }

    #[test]
    fn test_async_cluster_split_multi_shard_command_and_combine_arrays_of_values() {
        let name = "test_async_cluster_split_multi_shard_command_and_combine_arrays_of_values";
        let mut cmd = cmd("MGET");
        cmd.arg("foo").arg("bar").arg("baz");
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                let cmd_str = std::str::from_utf8(received_cmd).unwrap();
                let results = ["foo", "bar", "baz"]
                    .iter()
                    .filter_map(|expected_key| {
                        if cmd_str.contains(expected_key) {
                            Some(Value::BulkString(
                                format!("{expected_key}-{port}").into_bytes(),
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();
                Err(Ok(Value::Array(results)))
            },
        );

        let result = runtime
            .block_on(cmd.query_async::<_, Vec<String>>(&mut connection))
            .unwrap();
        assert_eq!(result, vec!["foo-6382", "bar-6380", "baz-6380"]);
    }

    #[test]
    fn test_async_cluster_handle_asking_error_in_split_multi_shard_command() {
        let name = "test_async_cluster_handle_asking_error_in_split_multi_shard_command";
        let mut cmd = cmd("MGET");
        cmd.arg("foo").arg("bar").arg("baz");
        let asking_called = Arc::new(AtomicU16::new(0));
        let asking_called_cloned = asking_called.clone();
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).read_from_replicas(),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                let cmd_str = std::str::from_utf8(received_cmd).unwrap();
                if cmd_str.contains("ASKING") && port == 6382 {
                    asking_called_cloned.fetch_add(1, Ordering::Relaxed);
                }
                if port == 6380 && cmd_str.contains("baz") {
                    return Err(parse_redis_value(
                        format!("-ASK 14000 {name}:6382\r\n").as_bytes(),
                    ));
                }
                let results = ["foo", "bar", "baz"]
                    .iter()
                    .filter_map(|expected_key| {
                        if cmd_str.contains(expected_key) {
                            Some(Value::BulkString(
                                format!("{expected_key}-{port}").into_bytes(),
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();
                Err(Ok(Value::Array(results)))
            },
        );

        let result = runtime
            .block_on(cmd.query_async::<_, Vec<String>>(&mut connection))
            .unwrap();
        assert_eq!(result, vec!["foo-6382", "bar-6380", "baz-6382"]);
        assert_eq!(asking_called.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_async_cluster_pass_errors_from_split_multi_shard_command() {
        let name = "test_async_cluster_pass_errors_from_split_multi_shard_command";
        let mut cmd = cmd("MGET");
        cmd.arg("foo").arg("bar").arg("baz");
        let MockEnv {
            runtime,
            async_connection: mut connection,
            ..
        } = MockEnv::new(name, move |received_cmd: &[u8], port| {
            respond_startup_with_replica_using_config(name, received_cmd, None)?;
            let cmd_str = std::str::from_utf8(received_cmd).unwrap();
            if cmd_str.contains("foo") || cmd_str.contains("baz") {
                Err(Err((ErrorKind::IoError, "error").into()))
            } else {
                Err(Ok(Value::Array(vec![Value::BulkString(
                    format!("{port}").into_bytes(),
                )])))
            }
        });

        let result = runtime
            .block_on(cmd.query_async::<_, Vec<String>>(&mut connection))
            .unwrap_err();
        assert_eq!(result.kind(), ErrorKind::IoError);
    }

    #[test]
    fn test_async_cluster_handle_missing_slots_in_split_multi_shard_command() {
        let name = "test_async_cluster_handle_missing_slots_in_split_multi_shard_command";
        let mut cmd = cmd("MGET");
        cmd.arg("foo").arg("bar").arg("baz");
        let MockEnv {
            runtime,
            async_connection: mut connection,
            ..
        } = MockEnv::new(name, move |received_cmd: &[u8], port| {
            respond_startup_with_replica_using_config(
                name,
                received_cmd,
                Some(vec![MockSlotRange {
                    primary_port: 6381,
                    replica_ports: vec![6382],
                    slot_range: (8192..16383),
                }]),
            )?;
            Err(Ok(Value::Array(vec![Value::BulkString(
                format!("{port}").into_bytes(),
            )])))
        });

        let result = runtime
            .block_on(cmd.query_async::<_, Vec<String>>(&mut connection))
            .unwrap_err();
        assert!(
            matches!(result.kind(), ErrorKind::ClusterConnectionNotFound)
                || result.is_connection_dropped()
        );
    }

    #[test]
    fn test_async_cluster_with_username_and_password() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(|builder| {
            builder
                .username(RedisCluster::username().to_string())
                .password(RedisCluster::password().to_string())
        });
        cluster.disable_default_user();

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            cmd("SET")
                .arg("test")
                .arg("test_data")
                .query_async(&mut connection)
                .await?;
            let res: String = cmd("GET")
                .arg("test")
                .clone()
                .query_async(&mut connection)
                .await?;
            assert_eq!(res, "test_data");
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_io_error() {
        let name = "node";
        let completed = Arc::new(AtomicI32::new(0));
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(2),
            name,
            move |cmd: &[u8], port| {
                respond_startup_two_nodes(name, cmd)?;
                // Error twice with io-error, ensure connection is reestablished w/out calling
                // other node (i.e., not doing a full slot rebuild)
                match port {
                    6380 => panic!("Node should not be called"),
                    _ => match completed.fetch_add(1, Ordering::SeqCst) {
                        0..=1 => Err(Err(RedisError::from(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "mock-io-error",
                        )))),
                        _ => Err(Ok(Value::BulkString(b"123".to_vec()))),
                    },
                }
            },
        );

        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(&mut connection),
        );

        assert_eq!(value, Ok(Some(123)));
    }

    #[test]
    fn test_async_cluster_non_retryable_error_should_not_retry() {
        let name = "node";
        let completed = Arc::new(AtomicI32::new(0));
        let MockEnv {
            async_connection: mut connection,
            handler: _handler,
            runtime,
            ..
        } = MockEnv::new(name, {
            let completed = completed.clone();
            move |cmd: &[u8], _| {
                respond_startup_two_nodes(name, cmd)?;
                // Error twice with io-error, ensure connection is reestablished w/out calling
                // other node (i.e., not doing a full slot rebuild)
                completed.fetch_add(1, Ordering::SeqCst);
                Err(Err((ErrorKind::ReadOnly, "").into()))
            }
        });

        let value = runtime.block_on(
            cmd("GET")
                .arg("test")
                .query_async::<_, Option<i32>>(&mut connection),
        );

        match value {
            Ok(_) => panic!("result should be an error"),
            Err(e) => match e.kind() {
                ErrorKind::ReadOnly => {}
                _ => panic!("Expected ReadOnly but got {:?}", e.kind()),
            },
        }
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_async_cluster_read_from_primary() {
        let name = "node";
        let found_ports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ports_clone = found_ports.clone();
        let MockEnv {
            runtime,
            async_connection: mut connection,
            ..
        } = MockEnv::new(name, move |received_cmd: &[u8], port| {
            respond_startup_with_replica_using_config(
                name,
                received_cmd,
                Some(vec![
                    MockSlotRange {
                        primary_port: 6379,
                        replica_ports: vec![6380, 6381],
                        slot_range: (0..8191),
                    },
                    MockSlotRange {
                        primary_port: 6382,
                        replica_ports: vec![6383, 6384],
                        slot_range: (8192..16383),
                    },
                ]),
            )?;
            ports_clone.lock().unwrap().push(port);
            Err(Ok(Value::Nil))
        });

        runtime.block_on(async {
            cmd("GET")
                .arg("foo")
                .query_async::<_, ()>(&mut connection)
                .await
                .unwrap();
            cmd("GET")
                .arg("bar")
                .query_async::<_, ()>(&mut connection)
                .await
                .unwrap();
            cmd("GET")
                .arg("foo")
                .query_async::<_, ()>(&mut connection)
                .await
                .unwrap();
            cmd("GET")
                .arg("bar")
                .query_async::<_, ()>(&mut connection)
                .await
                .unwrap();
        });

        found_ports.lock().unwrap().sort();
        assert_eq!(*found_ports.lock().unwrap(), vec![6379, 6379, 6382, 6382]);
    }

    #[test]
    fn test_async_cluster_round_robin_read_from_replica() {
        let name = "node";
        let found_ports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ports_clone = found_ports.clone();
        let MockEnv {
            runtime,
            async_connection: mut connection,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).read_from_replicas(),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(
                    name,
                    received_cmd,
                    Some(vec![
                        MockSlotRange {
                            primary_port: 6379,
                            replica_ports: vec![6380, 6381],
                            slot_range: (0..8191),
                        },
                        MockSlotRange {
                            primary_port: 6382,
                            replica_ports: vec![6383, 6384],
                            slot_range: (8192..16383),
                        },
                    ]),
                )?;
                ports_clone.lock().unwrap().push(port);
                Err(Ok(Value::Nil))
            },
        );

        runtime.block_on(async {
            cmd("GET")
                .arg("foo")
                .query_async::<_, ()>(&mut connection)
                .await
                .unwrap();
            cmd("GET")
                .arg("bar")
                .query_async::<_, ()>(&mut connection)
                .await
                .unwrap();
            cmd("GET")
                .arg("foo")
                .query_async::<_, ()>(&mut connection)
                .await
                .unwrap();
            cmd("GET")
                .arg("bar")
                .query_async::<_, ()>(&mut connection)
                .await
                .unwrap();
        });

        found_ports.lock().unwrap().sort();
        assert_eq!(*found_ports.lock().unwrap(), vec![6380, 6381, 6383, 6384]);
    }

    fn get_queried_node_id_if_master(cluster_nodes_output: Value) -> Option<String> {
        // Returns the node ID of the connection that was queried for CLUSTER NODES (using the 'myself' flag), if it's a master.
        // Otherwise, returns None.
        let get_node_id = |str: &str| {
            let parts: Vec<&str> = str.split('\n').collect();
            for node_entry in parts {
                if node_entry.contains("myself") && node_entry.contains("master") {
                    let node_entry_parts: Vec<&str> = node_entry.split(' ').collect();
                    let node_id = node_entry_parts[0];
                    return Some(node_id.to_string());
                }
            }
            None
        };

        match cluster_nodes_output {
            Value::BulkString(val) => match from_utf8(&val) {
                Ok(str_res) => get_node_id(str_res),
                Err(e) => panic!("failed to decode INFO response: {:?}", e),
            },
            Value::VerbatimString { format: _, text } => get_node_id(&text),
            _ => panic!("Recieved unexpected response: {:?}", cluster_nodes_output),
        }
    }

    #[test]
    fn test_async_cluster_handle_complete_server_disconnect_without_panicking() {
        let cluster =
            TestClusterContext::new_with_cluster_client_builder(|builder| builder.retries(2));
        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            drop(cluster);
            for _ in 0..5 {
                let cmd = cmd("PING");
                let result = connection
                    .route_command(&cmd, RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random))
                    .await;
                // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
                assert!(result.is_err());
                // This will route to all nodes - different path through the code.
                let result = connection.req_packed_command(&cmd).await;
                // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
                assert!(result.is_err());
            }
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_reconnect_after_complete_server_disconnect() {
        let cluster =
            TestClusterContext::new_with_cluster_client_builder(|builder| builder.retries(2));

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            drop(cluster);
            for _ in 0..5 {
                let cmd = cmd("PING");

                let result = connection
                    .route_command(&cmd, RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random))
                    .await;
                // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
                assert!(result.is_err());

                // This will route to all nodes - different path through the code.
                let result = connection.req_packed_command(&cmd).await;
                // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
                assert!(result.is_err());

                let _cluster = TestClusterContext::new_with_cluster_client_builder(|builder| {
                    builder.retries(2)
                });

                let result = connection.req_packed_command(&cmd).await.unwrap();
                assert_eq!(result, Value::SimpleString("PONG".to_string()));
            }
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_restore_resp3_pubsub_state_after_complete_server_disconnect() {
        // let cluster = TestClusterContext::new_with_cluster_client_builder(
        //     3,
        //     0,
        //     |builder| builder.retries(3).use_protocol(ProtocolVersion::RESP3),
        //     //|builder| builder.retries(3),
        //     false,
        // );

        // block_on_all(async move {
        //     let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
        //     let mut connection = cluster.async_connection(Some(tx.clone())).await;
        //     // assuming the implementation of TestCluster assigns the slots monotonicaly incerasing with the nodes
        //     let route_0 = redis::cluster_routing::Route::new(0, redis::cluster_routing::SlotAddr::Master);
        //     let node_0_route = redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(route_0);
        //     let route_2 = redis::cluster_routing::Route::new(16 * 1024 - 1, redis::cluster_routing::SlotAddr::Master);
        //     let node_2_route = redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(route_2);

        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("SUBSCRIBE").arg("test_channel"), RoutingInfo::SingleNode(node_0_route.clone()))
        //     //.route_command(&redis::Cmd::new().arg("SUBSCRIBE").arg("test_channel"), RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random))
        //     .await;

        //     assert_eq!(
        //         result,
        //         Ok(Value::Push {
        //             kind: PushKind::Subscribe,
        //             data: vec![Value::BulkString("test_channel".into()), Value::Int(1)],
        //         })
        //     );

        //     // pull out all the subscribe notification, this push notification is due to the previous subscribe command
        //     let result = rx.recv().await;
        //     assert!(result.is_some());
        //     let PushInfo { kind, data } = result.unwrap();
        //     assert_eq!(
        //         (kind, data),
        //         (
        //             PushKind::Subscribe,
        //             vec![
        //                 Value::BulkString("test_channel".as_bytes().to_vec()),
        //                 Value::Int(1),
        //             ]
        //         )
        //     );

        //     // ensure subscription, routing on the same node, expected return Int(1)
        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel").arg("test_message_from_node_0"), RoutingInfo::SingleNode(node_0_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(1))
        //     );

        //     // ensure subscription, routing on different node, expected return Int(0)
        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel").arg("test_message_from_node_2"), RoutingInfo::SingleNode(node_2_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(0))
        //     );

        //     for i in vec![0, 2] {
        //         let result = rx.recv().await;
        //         assert!(result.is_some());
        //         let PushInfo { kind, data } = result.unwrap();
        //         println!("^^^^^^^^^ '{:?} -> {:?}'", kind, data);
        //         assert_eq!(
        //             (kind, data),
        //             (
        //                 PushKind::Message,
        //                 vec![
        //                     Value::BulkString("test_channel".into()),
        //                     Value::BulkString(format!("test_message_from_node_{}", i).into()),
        //                 ]
        //             )
        //         );
        //     }

        //     // drop and recreate cluster and connections
        //     drop(cluster);
        //     println!("*********** DROPPED **********");

        //     let cluster = TestClusterContext::new_with_cluster_client_builder(
        //         3,
        //         0,
        //         |builder| builder.retries(3).use_protocol(ProtocolVersion::RESP3),
        //         //|builder| builder.retries(3),
        //         false,
        //     );

        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel").arg("test_message_from_node_0"), RoutingInfo::SingleNode(node_0_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(1))
        //     );

        //     //sleep(futures_time::time::Duration::from_secs(15)).await;
        //     //return Ok(());

        //     let cluster = TestClusterContext::new_with_cluster_client_builder(
        //         3,
        //         0,
        //         |builder| builder.retries(3).use_protocol(ProtocolVersion::RESP3),
        //         //|builder| builder.retries(3),
        //         false,
        //     );

        //     // ensure subscription state restore
        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel").arg("test_message_from_node_0"), RoutingInfo::SingleNode(node_0_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(1))
        //     );

        //     // non-subscribed channel
        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel_1").arg("should_not_receive"), RoutingInfo::SingleNode(node_0_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(0))
        //     );

        //     // ensure subscription, routing on different node, expected return Int(0)
        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel").arg("test_message_from_node_2"), RoutingInfo::SingleNode(node_2_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(0))
        //     );

        //     // should produce an arbitrary number of 'disconnected' notifications - 1 for the intitial try after the drop and an unknown? amout during reconnecting procedure
        //     // Notifications become available ONLY after we try to send the commands, since push manager does not register TCP disconnect on a idle socket
        //     // Remove the any amount of 'disconnected' notifications
        //     sleep(futures_time::time::Duration::from_secs(1)).await;
        //     //let mut result = rx.recv().await;
        //     let mut result = rx.try_recv();
        //     assert!(result.is_ok());
        //     //assert!(result.is_some());
        //     loop {
        //         let kind = result.clone().unwrap().kind;
        //         if kind != PushKind::Disconnection && kind != PushKind::Subscribe {
        //             break;
        //         }
        //         // result = rx.recv().await;
        //         // assert!(result.is_some());
        //         result = rx.try_recv();
        //         assert!(result.is_ok());
        //     }

        //     // ensure messages test_message_from_node_0 and test_message_from_node_2
        //     let mut msg_from_0 = false;
        //     let mut msg_from_2 = false;
        //     while !msg_from_0 && !msg_from_2 {
        //         let mut result = rx.recv().await;
        //         assert!(result.is_some());
        //         let PushInfo { kind, data } = result.unwrap();

        //         assert!(kind == PushKind::Disconnection || kind == PushKind::Subscribe || kind == PushKind::Message);
        //         if kind == PushKind::Disconnection || kind == PushKind::Subscribe {
        //             // ignore
        //             continue;
        //         }

        //         if data == vec![
        //             Value::BulkString("test_channel".into()),
        //             Value::BulkString("test_message_from_node_0".into())] {
        //             assert!(!msg_from_0);
        //             msg_from_0 = true;
        //         }
        //         else if data == vec![
        //             Value::BulkString("test_channel".into()),
        //             Value::BulkString("test_message_from_node_2".into())] {
        //             assert!(!msg_from_2);
        //             msg_from_2 = true;
        //         }
        //         else {
        //             assert!(false, "Unexpected message received");
        //         }
        //     }

        //     // let mut msg_from_0 = false;
        //     // let mut msg_from_2 = false;
        //     // while !msg_from_2 {
        //     //     let mut result = rx.recv().await;
        //     //     assert!(result.is_some());
        //     //     let PushInfo { kind, data } = result.unwrap();

        //     //     assert!(kind == PushKind::Disconnection || kind == PushKind::Subscribe || kind == PushKind::Message);
        //     //     if kind == PushKind::Disconnection || kind == PushKind::Subscribe {
        //     //         // ignore
        //     //         continue;
        //     //     }

        //     //     if data == vec![
        //     //         Value::BulkString("test_channel".into()),
        //     //         Value::BulkString("test_message_from_node_2".into())] {
        //     //         assert!(!msg_from_2);
        //     //         msg_from_2 = true;
        //     //     }
        //     //     else {
        //     //         assert!(false, "Unexpected message received");
        //     //     }
        //     // }

        //     Ok(())
        // })
        // .unwrap();
    }

    #[test]
    fn test_async_cluster_restore_resp3_pubsub_state_after_scale_in() {

        // let client_subscriptions = PubSubSubscriptionInfo::from(
        //     [
        //         (PubSubSubscriptionKind::Exact, HashSet::from(
        //             [
        //                 // test_channel_? is used as it maps to the last node in both 3 and 6 node config
        //                 // (assuming slots allocation is monotonicaly increasing starting from node 0)
        //                 PubSubChannelOrPattern::from(b"test_channel_?")
        //             ])
        //         )
        //     ]
        // );

        // let cluster = TestClusterContext::new_with_cluster_client_builder(
        //     6,
        //     0,
        //     |builder| builder
        //     .retries(3)
        //     .use_protocol(ProtocolVersion::RESP3)
        //     .pubsub_subscriptions(client_subscriptions.clone()),
        //     false,
        // );

        // block_on_all(async move {
        //     let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
        //     let mut connection = cluster.async_connection(Some(tx.clone())).await;

        //     // short sleep to allow the server to push subscription notification
        //     sleep(futures_time::time::Duration::from_secs(1)).await;
        //     let result = rx.try_recv();
        //     assert!(result.is_ok());
        //     let PushInfo { kind, data } = result.unwrap();
        //     assert_eq!(
        //         (kind, data),
        //         (
        //             PushKind::Subscribe,
        //             vec![
        //                 Value::BulkString("test_channel_?".into()),
        //                 Value::Int(1),
        //             ]
        //         )
        //     );

        //     let slot_14212 = get_slot(b"test_channel_?");
        //     assert_eq!(slot_14212, 14212);
        //     let slot_14212_route = redis::cluster_routing::Route::new(slot_14212, redis::cluster_routing::SlotAddr::Master);
        //     let node_5_route = redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(slot_14212_route);

        //     let result = connection
        //     //.route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel_?").arg("test_msg"), RoutingInfo::SingleNode(node_5_route.clone()))
        //     .route_command(&redis::Cmd::new().arg("PING"), RoutingInfo::SingleNode(node_5_route.clone()))
        //     .await;
        //     // let slot_0_route = redis::cluster_routing::Route::new(0, redis::cluster_routing::SlotAddr::Master);
        //     // let node_0_route = redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(slot_0_route);

        //     let result = cmd("PUBLISH")
        //     .arg("test_channel_?")
        //     .arg("test_message")
        //     .query_async(&mut connection)
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(1))
        //     );

        //     sleep(futures_time::time::Duration::from_secs(1)).await;
        //     let result = rx.try_recv();
        //     assert!(result.is_ok());
        //     let PushInfo { kind, data } = result.unwrap();
        //     assert_eq!(
        //         (kind, data),
        //         (
        //             PushKind::Message,
        //             vec![
        //                 Value::BulkString("test_channel_?".into()),
        //                 Value::BulkString(format!("test_message").into()),
        //             ]
        //         )
        //     );

        //     // simulate scale in
        //     drop(cluster);
        //     println!("*********** DROPPED **********");
        //     let cluster = TestClusterContext::new_with_cluster_client_builder(
        //         3,
        //         0,
        //         |builder| builder
        //         .retries(6)
        //         .use_protocol(ProtocolVersion::RESP3)
        //         .pubsub_subscriptions(client_subscriptions.clone()),
        //         false,
        //     );

        //     sleep(futures_time::time::Duration::from_secs(3)).await;

        //     //ensure subscription notification due to resubscription
        //     // let result = cmd("PUBLISH")
        //     // .arg("test_channel_?")
        //     // .arg("test_message")
        //     // .query_async(&mut connection)
        //     // .await;
        //     let result = connection
        //     //.route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel_?").arg("test_msg"), RoutingInfo::SingleNode(node_5_route.clone()))
        //     .route_command(&redis::Cmd::new().arg("PING"), RoutingInfo::SingleNode(node_5_route.clone()))
        //     .await;
        //     // assert_eq!(
        //     //     result,
        //     //     Ok(Value::Int(1))
        //     // );

        //     let slot_14212 = get_slot(b"test_channel_?");
        //     assert_eq!(slot_14212, 14212);
        //     let slot_14212_route = redis::cluster_routing::Route::new(slot_14212, redis::cluster_routing::SlotAddr::Master);
        //     let node_2_route = redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(slot_14212_route);
        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel_?").arg("test_message"), RoutingInfo::SingleNode(node_2_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(1))
        //     );

        //     sleep(futures_time::time::Duration::from_secs(1)).await;
        //     let result = rx.try_recv();
        //     assert!(result.is_ok());
        //     let PushInfo { kind, data } = result.unwrap();
        //     assert_eq!(
        //         (kind, data),
        //         (
        //             PushKind::Subscribe,
        //             vec![
        //                 Value::BulkString("test_channel_?".into()),
        //                 Value::Int(1),
        //             ]
        //         )
        //     );

        //     let result = rx.try_recv();
        //     assert!(result.is_ok());
        //     let PushInfo { kind, data } = result.unwrap();
        //     assert_eq!(
        //         (kind, data),
        //         (
        //             PushKind::Disconnection,
        //             vec![],
        //         )
        //     );

        //     return Ok(());

        //     // Subscribe on the slot 14212, this slot will reside on the last node in both 3 and 6 nodes cluster,
        //     // When the cluster is recreated with 3 nodes, this slot will reside on different network address.
        //     // Assuming the implementation of TestCluster assigns the slots monotonicaly incerasing with the nodes
        //     let slot_14212 = get_slot(b"test_channel_?");
        //     assert_eq!(slot_14212, 14212);
        //     let slot_14212_route = redis::cluster_routing::Route::new(slot_14212, redis::cluster_routing::SlotAddr::Master);
        //     let node_5_route = redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(slot_14212_route);

        //     let slot_0_route = redis::cluster_routing::Route::new(0, redis::cluster_routing::SlotAddr::Master);
        //     let node_0_route = redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(slot_0_route);

        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("SUBSCRIBE").arg("test_channel_?"), RoutingInfo::SingleNode(node_5_route.clone()))
        //     //.route_command(&redis::Cmd::new().arg("SUBSCRIBE").arg("test_channel"), RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random))
        //     .await;

        //     assert_eq!(
        //         result,
        //         Ok(Value::Push {
        //             kind: PushKind::Subscribe,
        //             data: vec![Value::BulkString("test_channel_?".into()), Value::Int(1)],
        //         })
        //     );

        //     // pull out all the subscribe notification, this push notification is due to the previous subscribe command
        //     let result = rx.recv().await;
        //     assert!(result.is_some());
        //     let PushInfo { kind, data } = result.unwrap();
        //     assert_eq!(
        //         (kind, data),
        //         (
        //             PushKind::Subscribe,
        //             vec![
        //                 Value::BulkString("test_channel_?".as_bytes().to_vec()),
        //                 Value::Int(1),
        //             ]
        //         )
        //     );

        //     // ensure subscription, routing on the last node, expected return Int(1)
        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel_?").arg("test_message_from_node_5"), RoutingInfo::SingleNode(node_5_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(1))
        //     );

        //     // ensure subscription, routing on the first node, expected return Int(0)
        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel_?").arg("test_message_from_node_0"), RoutingInfo::SingleNode(node_0_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(0))
        //     );

        //     for i in vec![5, 0] {
        //         let result = rx.recv().await;
        //         assert!(result.is_some());
        //         let PushInfo { kind, data } = result.unwrap();
        //         println!("^^^^^^^^^ '{:?} -> {:?}'", kind, data);
        //         assert_eq!(
        //             (kind, data),
        //             (
        //                 PushKind::Message,
        //                 vec![
        //                     Value::BulkString("test_channel_?".into()),
        //                     Value::BulkString(format!("test_message_from_node_{}", i).into()),
        //                 ]
        //             )
        //         );
        //     }

        //     // drop and recreate cluster and connections
        //     drop(cluster);
        //     println!("*********** DROPPED **********");

        //     let cluster = TestClusterContext::new_with_cluster_client_builder(
        //         3,
        //         0,
        //         |builder| builder.retries(3).use_protocol(ProtocolVersion::RESP3),
        //         //|builder| builder.retries(3),
        //         false,
        //     );

        //     // ensure subscription state restore
        //     let node_2_route = redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(slot_14212_route);
        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel_?").arg("test_message_from_node_2"), RoutingInfo::SingleNode(node_2_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(1))
        //     );

        //     // non-subscribed channel
        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel_1").arg("should_not_receive"), RoutingInfo::SingleNode(node_0_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(0))
        //     );

        //     // ensure subscription, routing on different node, expected return Int(0)
        //     let result = connection
        //     .route_command(&redis::Cmd::new().arg("PUBLISH").arg("test_channel_?").arg("test_message_from_node_2"), RoutingInfo::SingleNode(node_0_route.clone()))
        //     .await;
        //     assert_eq!(
        //         result,
        //         Ok(Value::Int(0))
        //     );

        //     // should produce an arbitrary number of 'disconnected' notifications - 1 for the intitial try after the drop and an unknown? amout during reconnecting procedure
        //     // Notifications become available ONLY after we try to send the commands, since push manager does not register TCP disconnect on a idle socket
        //     // Remove the any amount of 'disconnected' notifications
        //     sleep(futures_time::time::Duration::from_secs(1)).await;
        //     //let mut result = rx.recv().await;
        //     let mut result = rx.try_recv();
        //     assert!(result.is_ok());
        //     //assert!(result.is_some());
        //     loop {
        //         let kind = result.clone().unwrap().kind;
        //         if kind != PushKind::Disconnection && kind != PushKind::Subscribe {
        //             break;
        //         }
        //         // result = rx.recv().await;
        //         // assert!(result.is_some());
        //         result = rx.try_recv();
        //         assert!(result.is_ok());
        //     }

        //     // ensure messages test_message_from_node_0 and test_message_from_node_2
        //     let mut msg_from_0 = false;
        //     let mut msg_from_2 = false;
        //     while !msg_from_0 && !msg_from_2 {
        //         let mut result = rx.recv().await;
        //         assert!(result.is_some());
        //         let PushInfo { kind, data } = result.unwrap();

        //         assert!(kind == PushKind::Disconnection || kind == PushKind::Subscribe || kind == PushKind::Message);
        //         if kind == PushKind::Disconnection || kind == PushKind::Subscribe {
        //             // ignore
        //             continue;
        //         }

        //         if data == vec![
        //             Value::BulkString("test_channel".into()),
        //             Value::BulkString("test_message_from_node_0".into())] {
        //             assert!(!msg_from_0);
        //             msg_from_0 = true;
        //         }
        //         else if data == vec![
        //             Value::BulkString("test_channel".into()),
        //             Value::BulkString("test_message_from_node_2".into())] {
        //             assert!(!msg_from_2);
        //             msg_from_2 = true;
        //         }
        //         else {
        //             assert!(false, "Unexpected message received");
        //         }
        //     }

        //     Ok(())
        // })
        // .unwrap();
    }

    //#[allow(unreachable_code)]
    #[test]
    fn test_async_cluster_resp3_pubsub() {
        let redis_ver = std::env::var("REDIS_VERSION").unwrap_or_default();
        let use_sharded = redis_ver.starts_with("7.");

        let mut client_subscriptions = PubSubSubscriptionInfo::from([
            (
                PubSubSubscriptionKind::Exact,
                HashSet::from([PubSubChannelOrPattern::from("test_channel_?".as_bytes())]),
            ),
            (
                PubSubSubscriptionKind::Pattern,
                HashSet::from([
                    PubSubChannelOrPattern::from("test_*".as_bytes()),
                    PubSubChannelOrPattern::from("*".as_bytes()),
                ]),
            ),
        ]);

        if use_sharded {
            client_subscriptions.insert(
                PubSubSubscriptionKind::Sharded,
                HashSet::from([PubSubChannelOrPattern::from("test_channel_?".as_bytes())]),
            );
        }

        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| {
                builder
                    .retries(3)
                    .use_protocol(ProtocolVersion::RESP3)
                    .pubsub_subscriptions(client_subscriptions.clone())
            },
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx.clone())).await;

            // short sleep to allow the server to push subscription notification
            sleep(futures_time::time::Duration::from_secs(1)).await;

            let mut subscribe_cnt = client_subscriptions[&PubSubSubscriptionKind::Exact].len();
            let mut psubscribe_cnt = client_subscriptions[&PubSubSubscriptionKind::Pattern].len();
            let mut ssubscribe_cnt = 0;
            if let Some(sharded_shubs) = client_subscriptions.get(&PubSubSubscriptionKind::Sharded)
            {
                ssubscribe_cnt += sharded_shubs.len()
            }
            for _ in 0..(subscribe_cnt + psubscribe_cnt + ssubscribe_cnt) {
                let result = rx.try_recv();
                assert!(result.is_ok());
                let PushInfo { kind, data: _ } = result.unwrap();
                assert!(
                    kind == PushKind::Subscribe
                        || kind == PushKind::PSubscribe
                        || kind == PushKind::SSubscribe
                );
                if kind == PushKind::Subscribe {
                    subscribe_cnt -= 1;
                } else if kind == PushKind::PSubscribe {
                    psubscribe_cnt -= 1;
                } else {
                    ssubscribe_cnt -= 1;
                }
            }

            assert!(subscribe_cnt == 0);
            assert!(psubscribe_cnt == 0);
            assert!(ssubscribe_cnt == 0);

            let slot_14212 = get_slot(b"test_channel_?");
            assert_eq!(slot_14212, 14212);
            //let slot_14212_route = redis::cluster_routing::Route::new(slot_14212, redis::cluster_routing::SlotAddr::Master);
            //let node_5_route = redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(slot_14212_route);

            let slot_0_route =
                redis::cluster_routing::Route::new(0, redis::cluster_routing::SlotAddr::Master);
            let node_0_route =
                redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(slot_0_route);

            // node 0 route is used to ensure that the publish is propagated correctly
            let result = connection
                .route_command(
                    redis::Cmd::new()
                        .arg("PUBLISH")
                        .arg("test_channel_?")
                        .arg("test_message"),
                    RoutingInfo::SingleNode(node_0_route.clone()),
                )
                .await;
            assert!(result.is_ok());

            sleep(futures_time::time::Duration::from_secs(1)).await;

            let mut pmsg_cnt = 0;
            let mut msg_cnt = 0;
            for _ in 0..3 {
                let result = rx.try_recv();
                assert!(result.is_ok());
                let PushInfo { kind, data: _ } = result.unwrap();
                assert!(kind == PushKind::Message || kind == PushKind::PMessage);
                if kind == PushKind::Message {
                    msg_cnt += 1;
                } else {
                    pmsg_cnt += 1;
                }
            }
            assert_eq!(msg_cnt, 1);
            assert_eq!(pmsg_cnt, 2);

            if use_sharded {
                let result = cmd("SPUBLISH")
                    .arg("test_channel_?")
                    .arg("test_message")
                    .query_async(&mut connection)
                    .await;
                assert_eq!(result, Ok(Value::Int(1)));

                sleep(futures_time::time::Duration::from_secs(1)).await;
                let result = rx.try_recv();
                assert!(result.is_ok());
                let PushInfo { kind, data } = result.unwrap();
                assert_eq!(
                    (kind, data),
                    (
                        PushKind::SMessage,
                        vec![
                            Value::BulkString("test_channel_?".into()),
                            Value::BulkString("test_message".into()),
                        ]
                    )
                );
            }

            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_periodic_checks_update_topology_after_failover() {
        // This test aims to validate the functionality of periodic topology checks by detecting and updating topology changes.
        // We will repeatedly execute CLUSTER NODES commands against the primary node responsible for slot 0, recording its node ID.
        // Once we've successfully completed commands with the current primary, we will initiate a failover within the same shard.
        // Since we are not executing key-based commands, we won't encounter MOVED errors that trigger a slot refresh.
        // Consequently, we anticipate that only the periodic topology check will detect this change and trigger topology refresh.
        // If successful, the node to which we route the CLUSTER NODES command should be the newly promoted node with a different node ID.
        let cluster = TestClusterContext::new_with_config_and_builder(
            RedisClusterConfiguration {
                nodes: 6,
                replicas: 1,
                ..Default::default()
            },
            |builder| builder.periodic_topology_checks(Duration::from_millis(10)),
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let mut prev_master_id = "".to_string();
            let max_requests = 5000;
            let mut i = 0;
            loop {
                if i == 10 {
                    let mut cmd = redis::cmd("CLUSTER");
                    cmd.arg("FAILOVER");
                    cmd.arg("TAKEOVER");
                    let res = connection
                        .route_command(
                            &cmd,
                            RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(
                                Route::new(0, SlotAddr::ReplicaRequired),
                            )),
                        )
                        .await;
                    assert!(res.is_ok());
                } else if i == max_requests {
                    break;
                } else {
                    let mut cmd = redis::cmd("CLUSTER");
                    cmd.arg("NODES");
                    let res = connection
                        .route_command(
                            &cmd,
                            RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(
                                Route::new(0, SlotAddr::Master),
                            )),
                        )
                        .await
                        .expect("Failed executing CLUSTER NODES");
                    let node_id = get_queried_node_id_if_master(res);
                    if let Some(current_master_id) = node_id {
                        if prev_master_id.is_empty() {
                            prev_master_id = current_master_id;
                        } else if prev_master_id != current_master_id {
                            return Ok::<_, RedisError>(());
                        }
                    }
                }
                i += 1;
                let _ = sleep(futures_time::time::Duration::from_millis(10)).await;
            }
            panic!("Topology change wasn't found!");
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_recover_disconnected_management_connections() {
        // This test aims to verify that the management connections used for periodic checks are reconnected, in case that they get killed.
        // In order to test this, we choose a single node, kill all connections to it which aren't user connections, and then wait until new
        // connections are created.
        let cluster = TestClusterContext::new_with_cluster_client_builder(|builder| {
            builder.periodic_topology_checks(Duration::from_millis(10))
        });

        block_on_all(async move {
            let routing = RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(Route::new(
                1,
                SlotAddr::Master,
            )));

            let mut connection = cluster.async_connection(None).await;
            let max_requests = 5000;

            let connections =
                get_clients_names_to_ids(&mut connection, routing.clone().into()).await;
            assert!(connections.contains_key(MANAGEMENT_CONN_NAME));
            let management_conn_id = connections.get(MANAGEMENT_CONN_NAME).unwrap();

            // Get the connection ID of the management connection
            kill_connection(&mut connection, management_conn_id).await;

            let connections =
                get_clients_names_to_ids(&mut connection, routing.clone().into()).await;
            assert!(!connections.contains_key(MANAGEMENT_CONN_NAME));

            for _ in 0..max_requests {
                let _ = sleep(futures_time::time::Duration::from_millis(10)).await;

                let connections =
                    get_clients_names_to_ids(&mut connection, routing.clone().into()).await;
                if connections.contains_key(MANAGEMENT_CONN_NAME) {
                    return Ok(());
                }
            }

            panic!("Topology connection didn't reconnect!");
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_with_client_name() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(|builder| {
            builder.client_name(RedisCluster::client_name().to_string())
        });

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let client_info: String = cmd("CLIENT")
                .arg("INFO")
                .query_async(&mut connection)
                .await
                .unwrap();

            let client_attrs = parse_client_info(&client_info);

            assert!(
                client_attrs.contains_key("name"),
                "Could not detect the 'name' attribute in CLIENT INFO output"
            );

            assert_eq!(
                client_attrs["name"],
                RedisCluster::client_name(),
                "Incorrect client name, expecting: {}, got {}",
                RedisCluster::client_name(),
                client_attrs["name"]
            );
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_reroute_from_replica_if_in_loading_state() {
        /* Test replica in loading state. The expected behaviour is that the request will be directed to a different replica or the primary.
        depends on the read from replica policy. */
        let name = "test_async_cluster_reroute_from_replica_if_in_loading_state";

        let load_errors: Arc<_> = Arc::new(std::sync::Mutex::new(vec![]));
        let load_errors_clone = load_errors.clone();

        // requests should route to replica
        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).read_from_replicas(),
            name,
            move |cmd: &[u8], port| {
                respond_startup_with_replica_using_config(
                    name,
                    cmd,
                    Some(vec![MockSlotRange {
                        primary_port: 6379,
                        replica_ports: vec![6380, 6381],
                        slot_range: (0..16383),
                    }]),
                )?;
                match port {
                    6380 | 6381 => {
                        load_errors_clone.lock().unwrap().push(port);
                        Err(parse_redis_value(b"-LOADING\r\n"))
                    }
                    6379 => Err(Ok(Value::BulkString(b"123".to_vec()))),
                    _ => panic!("Wrong node"),
                }
            },
        );
        for _n in 0..3 {
            let value = runtime.block_on(
                cmd("GET")
                    .arg("test")
                    .query_async::<_, Option<i32>>(&mut connection),
            );
            assert_eq!(value, Ok(Some(123)));
        }

        let mut load_errors_guard = load_errors.lock().unwrap();
        load_errors_guard.sort();

        // We expected to get only 2 loading error since the 2 replicas are in loading state.
        // The third iteration will be directed to the primary since the connections of the replicas were removed.
        assert_eq!(*load_errors_guard, vec![6380, 6381]);
    }

    #[test]
    fn test_async_cluster_read_from_primary_when_primary_loading() {
        // Test primary in loading state. The expected behaviour is that the request will be retried until the primary is no longer in loading state.
        let name = "test_async_cluster_read_from_primary_when_primary_loading";

        const RETRIES: u32 = 3;
        const ITERATIONS: u32 = 2;
        let load_errors = Arc::new(AtomicU32::new(0));
        let load_errors_clone = load_errors.clone();

        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]),
            name,
            move |cmd: &[u8], port| {
                respond_startup_with_replica_using_config(
                    name,
                    cmd,
                    Some(vec![MockSlotRange {
                        primary_port: 6379,
                        replica_ports: vec![6380, 6381],
                        slot_range: (0..16383),
                    }]),
                )?;
                match port {
                    6379 => {
                        let attempts = load_errors_clone.fetch_add(1, Ordering::Relaxed) + 1;
                        if attempts % RETRIES == 0 {
                            Err(Ok(Value::BulkString(b"123".to_vec())))
                        } else {
                            Err(parse_redis_value(b"-LOADING\r\n"))
                        }
                    }
                    _ => panic!("Wrong node"),
                }
            },
        );
        for _n in 0..ITERATIONS {
            runtime
                .block_on(
                    cmd("GET")
                        .arg("test")
                        .query_async::<_, Value>(&mut connection),
                )
                .unwrap();
        }

        assert_eq!(load_errors.load(Ordering::Relaxed), ITERATIONS * RETRIES);
    }

    #[test]
    fn test_async_cluster_can_be_created_with_partial_slot_coverage() {
        let name = "test_async_cluster_can_be_created_with_partial_slot_coverage";
        let slots_config = Some(vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![],
                slot_range: (0..8000),
            },
            MockSlotRange {
                primary_port: 6381,
                replica_ports: vec![],
                slot_range: (8201..16380),
            },
        ]);

        let MockEnv {
            async_connection: mut connection,
            handler: _handler,
            runtime,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_replicas(),
            name,
            move |received_cmd: &[u8], _| {
                respond_startup_with_replica_using_config(
                    name,
                    received_cmd,
                    slots_config.clone(),
                )?;
                Err(Ok(Value::SimpleString("PONG".into())))
            },
        );

        let res = runtime.block_on(connection.req_packed_command(&redis::cmd("PING")));
        assert!(res.is_ok());
    }

    #[test]
    fn test_async_cluster_handle_complete_server_disconnect_without_panicking() {
        let cluster =
            TestClusterContext::new_with_cluster_client_builder(|builder| builder.retries(2));
        block_on_all(async move {
            let mut connection = cluster.async_connection().await;
            drop(cluster);
            for _ in 0..5 {
                let cmd = cmd("PING");
                let result = connection
                    .route_command(&cmd, RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random))
                    .await;
                // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
                assert!(result.is_err());
                // This will route to all nodes - different path through the code.
                let result = connection.req_packed_command(&cmd).await;
                // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
                assert!(result.is_err());
            }
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_reconnect_after_complete_server_disconnect() {
        let cluster =
            TestClusterContext::new_with_cluster_client_builder(|builder| builder.retries(2));

        block_on_all(async move {
            let ports: Vec<_> = cluster
                .nodes
                .iter()
                .map(|info| match info.addr {
                    redis::ConnectionAddr::Tcp(_, port) => port,
                    redis::ConnectionAddr::TcpTls { port, .. } => port,
                    redis::ConnectionAddr::Unix(_) => panic!("no unix sockets in cluster tests"),
                })
                .collect();

            let mut connection = cluster.async_connection().await;
            drop(cluster);

            let cmd = cmd("PING");

            let result = connection
                .route_command(&cmd, RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random))
                .await;
            // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
            assert!(result.is_err());

            // This will route to all nodes - different path through the code.
            let result = connection.req_packed_command(&cmd).await;
            // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
            assert!(result.is_err());

            let _cluster = RedisCluster::new(RedisClusterConfiguration {
                ports: ports.clone(),
                ..Default::default()
            });

            let result = connection.req_packed_command(&cmd).await.unwrap();
            assert_eq!(result, Value::SimpleString("PONG".to_string()));

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_reconnect_after_complete_server_disconnect_route_to_many() {
        let cluster =
            TestClusterContext::new_with_cluster_client_builder(|builder| builder.retries(3));

        block_on_all(async move {
            let ports: Vec<_> = cluster
                .nodes
                .iter()
                .map(|info| match info.addr {
                    redis::ConnectionAddr::Tcp(_, port) => port,
                    redis::ConnectionAddr::TcpTls { port, .. } => port,
                    redis::ConnectionAddr::Unix(_) => panic!("no unix sockets in cluster tests"),
                })
                .collect();

            let mut connection = cluster.async_connection().await;
            drop(cluster);

            // recreate cluster
            let _cluster = RedisCluster::new(RedisClusterConfiguration {
                ports: ports.clone(),
                ..Default::default()
            });

            let cmd = cmd("PING");
            // explicitly route to all primaries and request all succeeded
            let result = connection
                .route_command(
                    &cmd,
                    RoutingInfo::MultiNode((
                        MultipleNodeRoutingInfo::AllMasters,
                        Some(redis::cluster_routing::ResponsePolicy::AllSucceeded),
                    )),
                )
                .await;
            assert!(result.is_ok());

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_async_cluster_saves_reconnected_connection() {
        let name = "test_async_cluster_saves_reconnected_connection";
        let ping_attempts = Arc::new(AtomicI32::new(0));
        let ping_attempts_clone = ping_attempts.clone();
        let get_attempts = AtomicI32::new(0);

        let MockEnv {
            runtime,
            async_connection: mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(1),
            name,
            move |cmd: &[u8], port| {
                if port == 6380 {
                    respond_startup_two_nodes(name, cmd)?;
                    return Err(parse_redis_value(
                        format!("-MOVED 123 {name}:6379\r\n").as_bytes(),
                    ));
                }

                if contains_slice(cmd, b"PING") {
                    let connect_attempt = ping_attempts_clone.fetch_add(1, Ordering::Relaxed);
                    let past_get_attempts = get_attempts.load(Ordering::Relaxed);
                    // We want connection checks to fail after the first GET attempt, until it retries. Hence, we wait for 5 PINGs -
                    // 1. initial connection,
                    // 2. refresh slots on client creation,
                    // 3. refresh_connections `check_connection` after first GET failed,
                    // 4. refresh_connections `connect_and_check` after first GET failed,
                    // 5. reconnect on 2nd GET attempt.
                    // more than 5 attempts mean that the server reconnects more than once, which is the behavior we're testing against.
                    if past_get_attempts != 1 || connect_attempt > 3 {
                        respond_startup_two_nodes(name, cmd)?;
                    }
                    if connect_attempt > 5 {
                        panic!("Too many pings!");
                    }
                    Err(Err(RedisError::from(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "mock-io-error",
                    ))))
                } else {
                    respond_startup_two_nodes(name, cmd)?;
                    let past_get_attempts = get_attempts.fetch_add(1, Ordering::Relaxed);
                    // we fail the initial GET request, and after that we'll fail the first reconnect attempt, in the `refresh_connections` attempt.
                    if past_get_attempts == 0 {
                        // Error once with io-error, ensure connection is reestablished w/out calling
                        // other node (i.e., not doing a full slot rebuild)
                        Err(Err(RedisError::from(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "mock-io-error",
                        ))))
                    } else {
                        Err(Ok(Value::BulkString(b"123".to_vec())))
                    }
                }
            },
        );

        for _ in 0..4 {
            let value = runtime.block_on(
                cmd("GET")
                    .arg("test")
                    .query_async::<_, Option<i32>>(&mut connection),
            );

            assert_eq!(value, Ok(Some(123)));
        }
        // If you need to change the number here due to a change in the cluster, you probably also need to adjust the test.
        // See the PING counts above to explain why 5 is the target number.
        assert_eq!(ping_attempts.load(Ordering::Acquire), 5);
    }

    #[test]
    fn test_async_cluster_periodic_checks_use_management_connection() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(|builder| {
            builder.periodic_topology_checks(Duration::from_millis(10))
        });

        block_on_all(async move {
        let mut connection = cluster.async_connection(None).await;
        let mut client_list = "".to_string();
        let max_requests = 1000;
        let mut i = 0;
        loop {
            if i == max_requests {
                break;
            } else {
                client_list = cmd("CLIENT")
                    .arg("LIST")
                    .query_async::<_, String>(&mut connection)
                    .await
                    .expect("Failed executing CLIENT LIST");
                let mut client_list_parts = client_list.split('\n');
                if client_list_parts
                .any(|line| line.contains(MANAGEMENT_CONN_NAME) && line.contains("cmd=cluster")) 
                && client_list.matches(MANAGEMENT_CONN_NAME).count() == 1 {
                    return Ok::<_, RedisError>(());
                }
            }
            i += 1;
            let _ = sleep(futures_time::time::Duration::from_millis(10)).await;
        }
        panic!("Couldn't find a management connection or the connection wasn't used to execute CLUSTER SLOTS {:?}", client_list);
    })
    .unwrap();
    }

    async fn get_clients_names_to_ids(
        connection: &mut ClusterConnection,
        routing: Option<RoutingInfo>,
    ) -> HashMap<String, String> {
        let mut client_list_cmd = redis::cmd("CLIENT");
        client_list_cmd.arg("LIST");
        let value = match routing {
            Some(routing) => connection.route_command(&client_list_cmd, routing).await,
            None => connection.req_packed_command(&client_list_cmd).await,
        }
        .unwrap();
        let string = String::from_owned_redis_value(value).unwrap();
        string
            .split('\n')
            .filter_map(|line| {
                if line.is_empty() {
                    return None;
                }
                let key_values = line
                    .split(' ')
                    .filter_map(|value| {
                        let mut split = value.split('=');
                        match (split.next(), split.next()) {
                            (Some(key), Some(val)) => Some((key, val)),
                            _ => None,
                        }
                    })
                    .collect::<HashMap<_, _>>();
                match (key_values.get("name"), key_values.get("id")) {
                    (Some(key), Some(val)) if !val.is_empty() => {
                        Some((key.to_string(), val.to_string()))
                    }
                    _ => None,
                }
            })
            .collect()
    }

    async fn kill_connection(killer_connection: &mut ClusterConnection, connection_to_kill: &str) {
        let mut cmd = redis::cmd("CLIENT");
        cmd.arg("KILL");
        cmd.arg("ID");
        cmd.arg(connection_to_kill);
        // Kill the management connection in the primary node that holds slot 0
        assert!(killer_connection
            .route_command(
                &cmd,
                RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(Route::new(
                    0,
                    SlotAddr::Master,
                )),),
            )
            .await
            .is_ok());
    }

    #[test]
    fn test_async_cluster_only_management_connection_is_reconnected_after_connection_failure() {
        // This test will check two aspects:
        // 1. Ensuring that after a disconnection in the management connection, a new management connection is established.
        // 2. Confirming that a failure in the management connection does not impact the user connection, which should remain intact.
        let cluster = TestClusterContext::new_with_cluster_client_builder(|builder| {
            builder.periodic_topology_checks(Duration::from_millis(10))
        });
        block_on_all(async move {
        let mut connection = cluster.async_connection(None).await;
        let _client_list = "".to_string();
        let max_requests = 500;
        let mut i = 0;
        // Set the name of the client connection to 'user-connection', so we'll be able to identify it later on
        assert!(cmd("CLIENT")
            .arg("SETNAME")
            .arg("user-connection")
            .query_async::<_, Value>(&mut connection)
            .await
            .is_ok());
        // Get the client list
        let names_to_ids = get_clients_names_to_ids(&mut connection, Some(RoutingInfo::SingleNode(
            SingleNodeRoutingInfo::SpecificNode(Route::new(0, SlotAddr::Master))))).await;

        // Get the connection ID of 'user-connection'
        let user_conn_id = names_to_ids.get("user-connection").unwrap();
        // Get the connection ID of the management connection
        let management_conn_id = names_to_ids.get(MANAGEMENT_CONN_NAME).unwrap();
        // Get another connection that will be used to kill the management connection
        let mut killer_connection = cluster.async_connection(None).await;
        kill_connection(&mut killer_connection, management_conn_id).await;
        loop {
            // In this loop we'll wait for the new management connection to be established
            if i == max_requests {
                break;
            } else {
                let names_to_ids = get_clients_names_to_ids(&mut connection, Some(RoutingInfo::SingleNode(
                    SingleNodeRoutingInfo::SpecificNode(Route::new(0, SlotAddr::Master))))).await;
                if names_to_ids.contains_key(MANAGEMENT_CONN_NAME) {
                    // A management connection is found
                    let curr_management_conn_id =
                    names_to_ids.get(MANAGEMENT_CONN_NAME).unwrap();
                    let curr_user_conn_id =
                    names_to_ids.get("user-connection").unwrap();
                    // Confirm that the management connection has a new connection ID, and verify that the user connection remains unaffected.
                    if (curr_management_conn_id != management_conn_id)
                        && (curr_user_conn_id == user_conn_id)
                    {
                        return Ok::<_, RedisError>(());
                    }
                } else {
                    i += 1;
                    let _ = sleep(futures_time::time::Duration::from_millis(50)).await;
                    continue;
                }
            }
        }
        panic!(
            "No reconnection of the management connection found, or there was an unwantedly reconnection of the user connections.
            \nprev_management_conn_id={:?},prev_user_conn_id={:?}\nclient list={:?}",
            management_conn_id, user_conn_id, names_to_ids
        );
    })
    .unwrap();
    }

    #[cfg(feature = "tls-rustls")]
    mod mtls_test {
        use crate::support::mtls_test::create_cluster_client_from_cluster;
        use redis::ConnectionInfo;

        use super::*;

        #[test]
        fn test_async_cluster_basic_cmd_with_mtls() {
            let cluster = TestClusterContext::new_with_mtls();
            block_on_all(async move {
                let client = create_cluster_client_from_cluster(&cluster, true).unwrap();
                let mut connection = client.get_async_connection(None).await.unwrap();
                cmd("SET")
                    .arg("test")
                    .arg("test_data")
                    .query_async(&mut connection)
                    .await?;
                let res: String = cmd("GET")
                    .arg("test")
                    .clone()
                    .query_async(&mut connection)
                    .await?;
                assert_eq!(res, "test_data");
                Ok::<_, RedisError>(())
            })
            .unwrap();
        }

        #[test]
        fn test_async_cluster_should_not_connect_without_mtls_enabled() {
            let cluster = TestClusterContext::new_with_mtls();
            block_on_all(async move {
            let client = create_cluster_client_from_cluster(&cluster, false).unwrap();
            let connection = client.get_async_connection(None).await;
            match cluster.cluster.servers.first().unwrap().connection_info() {
                ConnectionInfo {
                    addr: redis::ConnectionAddr::TcpTls { .. },
                    ..
            } => {
                if connection.is_ok() {
                    panic!("Must NOT be able to connect without client credentials if server accepts TLS");
                }
            }
            _ => {
                if let Err(e) = connection {
                    panic!("Must be able to connect without client credentials if server does NOT accept TLS: {e:?}");
                }
            }
            }
            Ok::<_, RedisError>(())
        }).unwrap();
        }
    }
}
