use std::{
    collections::HashMap,
    sync::{atomic, Arc, RwLock},
};

use {
    futures::future,
    once_cell::sync::Lazy,
    redis::{
        aio::ConnectionLike,
        cluster_async::{Client, Connect},
        cmd, parse_redis_value, IntoConnectionInfo, RedisFuture, RedisResult, Value,
    },
    tokio::runtime::Runtime,
};

type Handler = Arc<dyn Fn(&redis::Cmd, u16) -> Result<(), RedisResult<Value>> + Send + Sync>;

static HANDLERS: Lazy<RwLock<HashMap<String, Handler>>> = Lazy::new(Default::default);

#[derive(Clone)]
pub struct MockConnection {
    handler: Handler,
    port: u16,
}

impl Connect for MockConnection {
    fn connect<'a, T>(info: T) -> RedisFuture<'a, Self>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        let info = info.into_connection_info().unwrap();

        let (name, port) = match &info.addr {
            redis::ConnectionAddr::Tcp(addr, port) => (addr, *port),
            _ => unreachable!(),
        };
        Box::pin(future::ok(MockConnection {
            handler: HANDLERS
                .read()
                .unwrap()
                .get(name)
                .unwrap_or_else(|| panic!("Handler `{}` were not installed", name))
                .clone(),
            port,
        }))
    }
}

fn contains_slice(xs: &[u8], ys: &[u8]) -> bool {
    for i in 0..xs.len() {
        if xs[i..].starts_with(ys) {
            return true;
        }
    }
    false
}

fn respond_startup(name: &str, cmd: &[u8]) -> Result<(), RedisResult<Value>> {
    if contains_slice(cmd, b"PING") {
        Err(Ok(Value::Status("OK".into())))
    } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
        Err(Ok(Value::Bulk(vec![Value::Bulk(vec![
            Value::Int(0),
            Value::Int(16383),
            Value::Bulk(vec![
                Value::Data(name.as_bytes().to_vec()),
                Value::Int(6379),
            ]),
        ])])))
    } else {
        Ok(())
    }
}

impl ConnectionLike for MockConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a redis::Cmd) -> RedisFuture<'a, Value> {
        Box::pin(future::ready(
            (self.handler)(cmd, self.port).expect_err("Handler did not specify a response"),
        ))
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        _pipeline: &'a redis::Pipeline,
        _offset: usize,
        _count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(future::ok(vec![]))
    }

    fn get_db(&self) -> i64 {
        0
    }
}

pub struct MockEnv {
    runtime: Runtime,
    client: redis::cluster_async::Client,
    connection: redis::cluster_async::Connection<MockConnection>,
    #[allow(unused)]
    handler: RemoveHandler,
}

struct RemoveHandler(String);

impl Drop for RemoveHandler {
    fn drop(&mut self) {
        HANDLERS.write().unwrap().remove(&self.0);
    }
}

impl MockEnv {
    fn new(
        id: &str,
        handler: impl Fn(&[u8], u16) -> Result<(), RedisResult<Value>> + Send + Sync + 'static,
    ) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        let id = id.to_string();
        HANDLERS.write().unwrap().insert(
            id.clone(),
            Arc::new(move |cmd, port| handler(&cmd.get_packed_command(), port)),
        );

        let client = Client::open(vec![&*format!("redis://{}", id)]).unwrap();
        let connection = runtime.block_on(client.get_generic_connection()).unwrap();
        MockEnv {
            runtime,
            client,
            connection,
            handler: RemoveHandler(id),
        }
    }
}

#[test]
fn test_async_cluster_tryagain_simple() {
    let _ = env_logger::try_init();
    let name = "tryagain";

    let requests = atomic::AtomicUsize::new(0);
    let MockEnv {
        runtime,
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::new(name, move |cmd: &[u8], _| {
        respond_startup(name, cmd)?;

        match requests.fetch_add(1, atomic::Ordering::SeqCst) {
            0..=1 => Err(parse_redis_value(b"-TRYAGAIN mock\r\n")),
            _ => Err(Ok(Value::Data(b"123".to_vec()))),
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
fn test_async_cluster_tryagain_exhaust_retries() {
    let _ = env_logger::try_init();
    let name = "tryagain_exhaust_retries";

    let requests = Arc::new(atomic::AtomicUsize::new(0));

    let MockEnv {
        runtime,
        mut client,
        handler: _handler,
        ..
    } = MockEnv::new(name, {
        let requests = requests.clone();
        move |cmd: &[u8], _| {
            respond_startup(name, cmd)?;
            requests.fetch_add(1, atomic::Ordering::SeqCst);
            Err(parse_redis_value(b"-TRYAGAIN mock\r\n"))
        }
    });

    let mut connection = runtime
        .block_on(
            client
                .set_retries(Some(2))
                .get_generic_connection::<MockConnection>(),
        )
        .unwrap();

    let result = runtime.block_on(
        cmd("GET")
            .arg("test")
            .query_async::<_, Option<i32>>(&mut connection),
    );

    assert_eq!(
        result.map_err(|err| err.to_string()),
        Err("An error was signalled by the server: mock".to_string())
    );
    assert_eq!(requests.load(atomic::Ordering::SeqCst), 3);
}

#[test]
fn test_async_cluster_rebuild_with_extra_nodes() {
    let _ = env_logger::try_init();
    let name = "rebuild_with_extra_nodes";

    let requests = atomic::AtomicUsize::new(0);
    let started = atomic::AtomicBool::new(false);
    let MockEnv {
        runtime,
        mut connection,
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
        eprintln!("{} => {}", i, String::from_utf8_lossy(cmd));

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
