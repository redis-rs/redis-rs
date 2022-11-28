use std::{
    cell::Cell,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, MutexGuard,
    },
};

use {
    futures::{prelude::*, stream},
    once_cell::sync::Lazy,
    proptest::proptest,
    tokio::runtime::Runtime,
};

use redis_cluster_async::{
    redis::{
        aio::{ConnectionLike, MultiplexedConnection},
        cmd, AsyncCommands, Cmd, IntoConnectionInfo, RedisError, RedisFuture, RedisResult, Script,
        Value,
    },
    Client, Connect,
};

const REDIS_URL: &str = "redis://127.0.0.1:7000/";

pub struct RedisProcess;
pub struct RedisLock(MutexGuard<'static, RedisProcess>);

impl RedisProcess {
    // Blocks until we have sole access.
    pub fn lock() -> RedisLock {
        static REDIS: Lazy<Mutex<RedisProcess>> = Lazy::new(|| Mutex::new(RedisProcess {}));

        // If we panic in a test we don't want subsequent to fail because of a poisoned error
        let redis_lock = REDIS
            .lock()
            .unwrap_or_else(|poison_error| poison_error.into_inner());
        RedisLock(redis_lock)
    }
}

// ----------------------------------------------------------------------------

pub struct RuntimeEnv {
    pub redis: RedisEnv,
    pub runtime: Runtime,
}

impl RuntimeEnv {
    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        let redis = runtime.block_on(RedisEnv::new());
        Self { runtime, redis }
    }
}
pub struct RedisEnv {
    _redis_lock: RedisLock,
    pub client: Client,
    nodes: Vec<redis::aio::MultiplexedConnection>,
}

impl RedisEnv {
    pub async fn new() -> Self {
        let _ = env_logger::try_init();

        let redis_lock = RedisProcess::lock();

        let redis_client = redis::Client::open(REDIS_URL)
            .unwrap_or_else(|_| panic!("Failed to connect to '{}'", REDIS_URL));

        let mut master_urls = Vec::new();
        let mut nodes = Vec::new();

        'outer: loop {
            let node_infos = async {
                let mut conn = redis_client.get_multiplexed_tokio_connection().await?;
                Self::cluster_info(&mut conn).await
            }
            .await
            .expect("Unable to query nodes for information");
            // Wait for the cluster to stabilize
            if node_infos.iter().filter(|(_, master)| *master).count() == 3 {
                let cleared_nodes = async {
                    master_urls.clear();
                    nodes.clear();
                    // Clear databases:
                    for (url, master) in node_infos {
                        let redis_client = redis::Client::open(&url[..])
                            .unwrap_or_else(|_| panic!("Failed to connect to '{}'", url));
                        let mut conn = redis_client.get_multiplexed_tokio_connection().await?;

                        if master {
                            master_urls.push(url.to_string());
                            let () =
                                tokio::time::timeout(std::time::Duration::from_secs(3), async {
                                    Ok(redis::Cmd::new()
                                        .arg("FLUSHALL")
                                        .query_async(&mut conn)
                                        .await?)
                                })
                                .await
                                .unwrap_or_else(|err| Err(anyhow::Error::from(err)))?;
                        }

                        nodes.push(conn);
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
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let client = Client::open(master_urls.iter().map(|s| &s[..]).collect()).unwrap();

        RedisEnv {
            client,
            nodes,
            _redis_lock: redis_lock,
        }
    }

    async fn cluster_info<T>(redis_client: &mut T) -> RedisResult<Vec<(String, bool)>>
    where
        T: Clone + redis::aio::ConnectionLike + Send + 'static,
    {
        redis::cmd("CLUSTER")
            .arg("NODES")
            .query_async(redis_client)
            .await
            .map(|s: String| {
                s.lines()
                    .map(|line| {
                        let mut iter = line.split(' ');
                        let port = iter
                            .by_ref()
                            .nth(1)
                            .expect("Node ip")
                            .splitn(2, '@')
                            .next()
                            .unwrap()
                            .splitn(2, ':')
                            .nth(1)
                            .unwrap();
                        (
                            format!("redis://localhost:{}", port),
                            iter.next().expect("master").contains("master"),
                        )
                    })
                    .collect::<Vec<_>>()
            })
    }
}

#[tokio::test]
async fn basic_cmd() {
    let env = RedisEnv::new().await;
    let client = env.client;
    async {
        let mut connection = client.get_connection().await?;
        let () = cmd("SET")
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
        Ok(())
    }
    .await
    .map_err(|err: RedisError| err)
    .unwrap()
}

#[tokio::test]
async fn basic_eval() {
    let env = RedisEnv::new().await;
    let client = env.client;
    async {
        let mut connection = client.get_connection().await?;
        let res: String = cmd("EVAL")
            .arg(r#"redis.call("SET", KEYS[1], ARGV[1]); return redis.call("GET", KEYS[1])"#)
            .arg(1)
            .arg("key")
            .arg("test")
            .query_async(&mut connection)
            .await?;
        assert_eq!(res, "test");
        Ok(())
    }
    .await
    .map_err(|err: RedisError| err)
    .unwrap()
}

#[ignore] // TODO Handle running SCRIPT LOAD on all masters
#[tokio::test]
async fn basic_script() {
    let env = RedisEnv::new().await;
    let client = env.client;
    async {
        let mut connection = client.get_connection().await?;
        let res: String = Script::new(
            r#"redis.call("SET", KEYS[1], ARGV[1]); return redis.call("GET", KEYS[1])"#,
        )
        .key("key")
        .arg("test")
        .invoke_async(&mut connection)
        .await?;
        assert_eq!(res, "test");
        Ok(())
    }
    .await
    .map_err(|err: RedisError| err)
    .unwrap()
}

#[ignore] // TODO Handle pipe where the keys do not all go to the same node
#[tokio::test]
async fn basic_pipe() {
    let env = RedisEnv::new().await;
    let client = env.client;
    async {
        let mut connection = client.get_connection().await?;
        let mut pipe = redis::pipe();
        pipe.add_command(cmd("SET").arg("test").arg("test_data").clone());
        pipe.add_command(cmd("SET").arg("test3").arg("test_data3").clone());
        let () = pipe.query_async(&mut connection).await?;
        let res: String = connection.get("test").await?;
        assert_eq!(res, "test_data");
        let res: String = connection.get("test3").await?;
        assert_eq!(res, "test_data3");
        Ok(())
    }
    .await
    .map_err(|err: RedisError| err)
    .unwrap()
}

#[test]
fn proptests() {
    let env = std::cell::RefCell::new(FailoverEnv::new());

    proptest!(
        proptest::prelude::ProptestConfig { cases: 30, failure_persistence: None, .. Default::default() },
        |(requests in 0..15, value in 0..i32::max_value())| {
            test_failover(&mut env.borrow_mut(), requests, value)
        }
    );
}

#[test]
fn basic_failover() {
    test_failover(&mut FailoverEnv::new(), 10, 123);
}

struct FailoverEnv {
    env: RuntimeEnv,
    connection: redis_cluster_async::Connection,
}

impl FailoverEnv {
    fn new() -> Self {
        let env = RuntimeEnv::new();
        let connection = env
            .runtime
            .block_on(env.redis.client.get_connection())
            .unwrap();

        FailoverEnv { env, connection }
    }
}

async fn do_failover(redis: &mut redis::aio::MultiplexedConnection) -> Result<(), anyhow::Error> {
    cmd("CLUSTER").arg("FAILOVER").query_async(redis).await?;
    Ok(())
}

fn test_failover(env: &mut FailoverEnv, requests: i32, value: i32) {
    let completed = Cell::new(0);
    let completed = &completed;

    let FailoverEnv { env, connection } = env;

    let nodes = env.redis.nodes.clone();

    let test_future = async {
        (0..requests + 1)
            .map(|i| {
                let mut connection = connection.clone();
                let mut nodes = nodes.clone();
                async move {
                    if i == requests / 2 {
                        // Failover all the nodes, error only if all the failover requests error
                        nodes
                            .iter_mut()
                            .map(|node| do_failover(node))
                            .collect::<stream::FuturesUnordered<_>>()
                            .fold(
                                Err(anyhow::anyhow!("None")),
                                |acc: Result<(), _>, result: Result<(), _>| async move {
                                    acc.or_else(|_| result)
                                },
                            )
                            .await
                    } else {
                        let key = format!("test-{}-{}", value, i);
                        let () = cmd("SET")
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
            .try_collect()
            .await
    };
    env.runtime
        .block_on(test_future)
        .unwrap_or_else(|err| panic!("{}", err));
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

#[tokio::test]
async fn error_in_inner_connection() -> Result<(), anyhow::Error> {
    let _ = env_logger::try_init();

    let env = RedisEnv::new().await;
    let mut con = env
        .client
        .get_generic_connection::<ErrorConnection>()
        .await?;

    ERROR.store(false, Ordering::SeqCst);
    let r: Option<i32> = con.get("test").await?;
    assert_eq!(r, None::<i32>);

    ERROR.store(true, Ordering::SeqCst);

    let result: RedisResult<()> = con.get("test").await;
    assert_eq!(
        result,
        Err(RedisError::from((redis::ErrorKind::Moved, "ERROR")))
    );

    Ok(())
}
