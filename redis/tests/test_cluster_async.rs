#![cfg(feature = "cluster-async")]
mod support;
use std::sync::{
    atomic::{self, AtomicBool, AtomicI32, AtomicU16, Ordering},
    Arc,
};

use futures::prelude::*;
use once_cell::sync::Lazy;
use redis::{
    aio::{ConnectionLike, MultiplexedConnection},
    cluster::ClusterClient,
    cluster_async::Connect,
    cluster_routing::{MultipleNodeRoutingInfo, RoutingInfo, SingleNodeRoutingInfo},
    cmd, parse_redis_value, AsyncCommands, Cmd, ErrorKind, InfoDict, IntoConnectionInfo,
    RedisError, RedisFuture, RedisResult, Script, Value,
};

use crate::support::*;

#[test]
fn test_async_cluster_basic_cmd() {
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;
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
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;
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
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;
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
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;
        let _: () = connection.set("foo", "bar").await.unwrap();
        let _: () = connection.set("bar", "foo").await.unwrap();

        let res: String = connection.get("foo").await.unwrap();
        assert_eq!(res, "bar".to_string());
        let res2: Option<String> = connection.get("bar").await.unwrap();
        assert_eq!(res2, Some("foo".to_string()));

        let route = redis::cluster_routing::Route::new(1, redis::cluster_routing::SlotAddr::Master);
        let single_node_route = redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(route);
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
fn test_async_cluster_route_info_to_nodes() {
    let cluster = TestClusterContext::new(12, 1);

    let split_to_addresses_and_info = |res| -> (Vec<String>, Vec<String>) {
        if let Value::Bulk(values) = res {
            let mut pairs: Vec<_> = values
                .into_iter()
                .map(|value| redis::from_redis_value::<(String, String)>(&value).unwrap())
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
        let mut connection = client.get_async_connection().await?;

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

        let route_to_all_primaries = redis::cluster_routing::MultipleNodeRoutingInfo::AllMasters;
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
fn test_async_cluster_basic_pipe() {
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;
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
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;

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
        test_failover(&TestClusterContext::new(6, 1), 10, 123, false).await;
        Ok::<_, RedisError>(())
    })
    .unwrap()
}

async fn do_failover(redis: &mut redis::aio::MultiplexedConnection) -> Result<(), anyhow::Error> {
    cmd("CLUSTER").arg("FAILOVER").query_async(redis).await?;
    Ok(())
}

// parameter `_mtls_enabled` can only be used if `feature = tls-rustls` is active
#[allow(dead_code)]
async fn test_failover(env: &TestClusterContext, requests: i32, value: i32, _mtls_enabled: bool) {
    let completed = Arc::new(AtomicI32::new(0));

    let connection = env.async_connection().await;
    let mut node_conns: Vec<MultiplexedConnection> = Vec::new();

    'outer: loop {
        node_conns.clear();
        let cleared_nodes = async {
            for server in env.cluster.iter_servers() {
                let addr = server.client_addr();

                #[cfg(feature = "tls-rustls")]
                let client =
                    build_single_client(server.connection_info(), &server.tls_paths, _mtls_enabled)
                        .unwrap_or_else(|e| panic!("Failed to connect to '{addr}': {e}"));

                #[cfg(not(feature = "tls-rustls"))]
                let client = redis::Client::open(server.connection_info())
                    .unwrap_or_else(|e| panic!("Failed to connect to '{addr}': {e}"));

                let mut conn = client
                    .get_multiplexed_async_connection()
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
                log::warn!("{}", err);
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
    ) -> RedisFuture<'a, Self>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        Box::pin(async move {
            let inner =
                MultiplexedConnection::connect(info, response_timeout, connection_timeout).await?;
            Ok(ErrorConnection { inner })
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
    let cluster = TestClusterContext::new(3, 0);

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
    let cluster = TestClusterContext::new(3, 0);

    block_on_all_using_async_std(async {
        let mut connection = cluster.async_connection().await;
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
                _ => Err(Ok(Value::Data(b"123".to_vec()))),
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

#[test]
fn test_async_cluster_move_error_when_new_node_is_added() {
    let name = "rebuild_with_extra_nodes";

    let requests = atomic::AtomicUsize::new(0);
    let started = atomic::AtomicBool::new(false);
    let refreshed = atomic::AtomicBool::new(false);

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

        if contains_slice(cmd, b"PING") {
            return Err(Ok(Value::Status("OK".into())));
        }

        let i = requests.fetch_add(1, atomic::Ordering::SeqCst);

        let is_get_cmd = contains_slice(cmd, b"GET");
        let get_response = Err(Ok(Value::Data(b"123".to_vec())));
        match i {
            // Respond that the key exists on a node that does not yet have a connection:
            0 => Err(parse_redis_value(
                format!("-MOVED 123 {name}:6380\r\n").as_bytes(),
            )),
            _ => {
                if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                    // Should not attempt to refresh slots more than once:
                    assert!(!refreshed.swap(true, Ordering::SeqCst));
                    Err(Ok(Value::Bulk(vec![
                        Value::Bulk(vec![
                            Value::Int(0),
                            Value::Int(1),
                            Value::Bulk(vec![
                                Value::Data(name.as_bytes().to_vec()),
                                Value::Int(6379),
                            ]),
                        ]),
                        Value::Bulk(vec![
                            Value::Int(2),
                            Value::Int(16383),
                            Value::Bulk(vec![
                                Value::Data(name.as_bytes().to_vec()),
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
                            Err(Ok(Value::Data(b"123".to_vec())))
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
                Err(Ok(Value::Data(b"123".to_vec())))
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

        if contains_slice(cmd, b"PING") {
            return Err(Ok(Value::Status("OK".into())));
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
                Err(Ok(Value::Data(b"123".to_vec())))
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
                6380 => Err(Ok(Value::Data(b"123".to_vec()))),
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
                6379 => Err(Ok(Value::Status("OK".into()))),
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
    assert_eq!(value, Ok(Some(Value::Status("OK".to_owned()))));
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
            respond_startup_with_replica_using_config(name, received_cmd, slots_config.clone())?;
            if received_cmd == packed_cmd {
                ports_clone.lock().unwrap().push(port);
                return Err(Ok(Value::Status("OK".into())));
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
    let name = "node";

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
                return Err(Ok(Value::Bulk(vec![
                    Value::Int(0),
                    Value::Int(0),
                    Value::Int(1),
                    Value::Int(1),
                ])));
            } else if port == 6379 {
                return Err(Ok(Value::Bulk(vec![
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
                return Err(Ok(Value::Data("foo".as_bytes().to_vec())));
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
            Err(Ok(Value::Data(format!("latency: {port}").into_bytes())))
        },
    );

    // TODO once RESP3 is in, return this as a map
    let mut result = runtime
        .block_on(cmd.query_async::<_, Vec<Vec<String>>>(&mut connection))
        .unwrap();
    result.sort();
    assert_eq!(
        result,
        vec![
            vec![format!("{name}:6379"), "latency: 6379".to_string()],
            vec![format!("{name}:6380"), "latency: 6380".to_string()],
            vec![format!("{name}:6381"), "latency: 6381".to_string()],
            vec![format!("{name}:6382"), "latency: 6382".to_string()]
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
            Err(Ok(Value::Bulk(vec![Value::Data(
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
                        Some(Value::Data(format!("{expected_key}-{port}").into_bytes()))
                    } else {
                        None
                    }
                })
                .collect();
            Err(Ok(Value::Bulk(results)))
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
                        Some(Value::Data(format!("{expected_key}-{port}").into_bytes()))
                    } else {
                        None
                    }
                })
                .collect();
            Err(Ok(Value::Bulk(results)))
        },
    );

    let result = runtime
        .block_on(cmd.query_async::<_, Vec<String>>(&mut connection))
        .unwrap();
    assert_eq!(result, vec!["foo-6382", "bar-6380", "baz-6382"]);
    assert_eq!(asking_called.load(Ordering::Relaxed), 1);
}

#[test]
fn test_async_cluster_with_username_and_password() {
    let cluster = TestClusterContext::new_with_cluster_client_builder(
        3,
        0,
        |builder| {
            builder
                .username(RedisCluster::username().to_string())
                .password(RedisCluster::password().to_string())
        },
        false,
    );
    cluster.disable_default_user();

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;
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
                    _ => Err(Ok(Value::Data(b"123".to_vec()))),
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
            respond_startup_with_replica_using_config(name, received_cmd, slots_config.clone())?;
            Err(Ok(Value::Status("PONG".into())))
        },
    );

    let res = runtime.block_on(connection.req_packed_command(&redis::cmd("PING")));
    assert!(res.is_ok());
}

#[test]
fn test_async_cluster_handle_complete_server_disconnect_without_panicking() {
    let cluster = TestClusterContext::new_with_cluster_client_builder(
        3,
        0,
        |builder| builder.retries(2),
        false,
    );
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
    let cluster = TestClusterContext::new_with_cluster_client_builder(
        3,
        0,
        |builder| builder.retries(2),
        false,
    );

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

            let _cluster = TestClusterContext::new_with_cluster_client_builder(
                3,
                0,
                |builder| builder.retries(2),
                false,
            );

            let result = connection.req_packed_command(&cmd).await.unwrap();
            assert_eq!(result, Value::Status("PONG".to_string()));
        }
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
                    Err(Ok(Value::Data(b"123".to_vec())))
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

#[cfg(feature = "tls-rustls")]
mod mtls_test {
    use crate::support::mtls_test::create_cluster_client_from_cluster;
    use redis::ConnectionInfo;

    use super::*;

    #[test]
    fn test_async_cluster_basic_cmd_with_mtls() {
        let cluster = TestClusterContext::new_with_mtls(3, 0);
        block_on_all(async move {
            let client = create_cluster_client_from_cluster(&cluster, true).unwrap();
            let mut connection = client.get_async_connection().await.unwrap();
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
        let cluster = TestClusterContext::new_with_mtls(3, 0);
        block_on_all(async move {
            let client = create_cluster_client_from_cluster(&cluster, false).unwrap();
            let connection = client.get_async_connection().await;
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
