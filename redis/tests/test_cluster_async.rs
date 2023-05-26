#![cfg(feature = "cluster-async")]
mod support;
use std::sync::{
    atomic::{self, AtomicI32},
    atomic::{AtomicBool, Ordering},
    Arc,
};

use futures::prelude::*;
use futures::stream;
use once_cell::sync::Lazy;
use redis::{
    aio::{ConnectionLike, MultiplexedConnection},
    cluster::ClusterClient,
    cluster_async::Connect,
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

#[ignore] // TODO Handle running SCRIPT LOAD on all masters
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

#[ignore] // TODO Handle pipe where the keys do not all go to the same node
#[test]
fn test_async_cluster_basic_pipe() {
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;
        let mut pipe = redis::pipe();
        pipe.add_command(cmd("SET").arg("test").arg("test_data").clone());
        pipe.add_command(cmd("SET").arg("test3").arg("test_data3").clone());
        pipe.query_async(&mut connection).await?;
        let res: String = connection.get("test").await?;
        assert_eq!(res, "test_data");
        let res: String = connection.get("test3").await?;
        assert_eq!(res, "test_data3");
        Ok::<_, RedisError>(())
    })
    .unwrap()
}

#[test]
fn test_async_cluster_basic_failover() {
    block_on_all(async move {
        test_failover(&TestClusterContext::new(6, 1), 10, 123).await;
        Ok::<_, RedisError>(())
    })
    .unwrap()
}

async fn do_failover(redis: &mut redis::aio::MultiplexedConnection) -> Result<(), anyhow::Error> {
    cmd("CLUSTER").arg("FAILOVER").query_async(redis).await?;
    Ok(())
}

async fn test_failover(env: &TestClusterContext, requests: i32, value: i32) {
    let completed = Arc::new(AtomicI32::new(0));

    let connection = env.async_connection().await;
    let mut node_conns: Vec<MultiplexedConnection> = Vec::new();

    'outer: loop {
        node_conns.clear();
        let cleared_nodes = async {
            for server in env.cluster.iter_servers() {
                let addr = server.client_addr();
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
                    node_conns
                        .iter_mut()
                        .map(do_failover)
                        .collect::<stream::FuturesUnordered<_>>()
                        .fold(
                            Err(anyhow::anyhow!("None")),
                            |acc: Result<(), _>, result: Result<(), _>| async move {
                                acc.or(result)
                            },
                        )
                        .await
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
    fn connect<'a, T>(info: T) -> RedisFuture<'a, Self>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        Box::pin(async {
            let inner = MultiplexedConnection::connect(info).await?;
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
fn test_async_cluster_rebuild_with_extra_nodes() {
    let name = "rebuild_with_extra_nodes";

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
            // Respond that the key exists elswehere (the slot, 123, is unused in the
            // implementation)
            0 => Err(parse_redis_value(b"-MOVED 123\r\n")),
            // Respond with the new masters
            1 => Err(Ok(Value::Bulk(vec![
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
            ]))),
            _ => {
                // Check that the correct node receives the request after rebuilding
                assert_eq!(port, 6380);
                Err(Ok(Value::Data(b"123".to_vec())))
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

#[test]
fn test_async_cluster_with_username_and_password() {
    let cluster = TestClusterContext::new_with_cluster_client_builder(3, 0, |builder| {
        builder
            .username(RedisCluster::username().to_string())
            .password(RedisCluster::password().to_string())
    });
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
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")]),
        name,
        {
            let completed = completed.clone();
            move |cmd: &[u8], _| {
                respond_startup_two_nodes(name, cmd)?;
                // Error twice with io-error, ensure connection is reestablished w/out calling
                // other node (i.e., not doing a full slot rebuild)
                completed.fetch_add(1, Ordering::SeqCst);
                Err(parse_redis_value(b"-ERR mock\r\n"))
            }
        },
    );

    let value = runtime.block_on(
        cmd("GET")
            .arg("test")
            .query_async::<_, Option<i32>>(&mut connection),
    );

    match value {
        Ok(_) => panic!("result should be an error"),
        Err(e) => match e.kind() {
            ErrorKind::ResponseError => {}
            _ => panic!("Expected ResponseError but got {:?}", e.kind()),
        },
    }
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}
