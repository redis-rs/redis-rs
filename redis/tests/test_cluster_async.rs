#![cfg(feature = "cluster-async")]
mod support;
use std::{
    cell::Cell,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use futures::prelude::*;
use futures::stream;
use once_cell::sync::Lazy;
use proptest::proptest;
use redis::{
    aio::{ConnectionLike, MultiplexedConnection},
    cluster_async::Connect,
    cmd, AsyncCommands, Cmd, InfoDict, IntoConnectionInfo, RedisError, RedisFuture, RedisResult,
    Script, Value,
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
fn test_async_cluster_proptests() {
    let cluster = Arc::new(TestClusterContext::new(6, 1));

    proptest!(
        proptest::prelude::ProptestConfig { cases: 30, failure_persistence: None, .. Default::default() },
        |(requests in 0..15i32, value in 0..i32::max_value())| {
            let cluster = cluster.clone();
            block_on_all(async move { test_failover(&cluster, requests, value).await; });
        }
    );
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
    let completed = Cell::new(0);
    let completed = &completed;

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
                    completed.set(completed.get() + 1);
                    Ok::<_, anyhow::Error>(())
                }
            }
        })
        .collect::<stream::FuturesUnordered<_>>()
        .collect::<Vec<Result<(), anyhow::Error>>>()
        .await;

    assert_eq!(completed.get(), requests, "Some requests never completed!");
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
