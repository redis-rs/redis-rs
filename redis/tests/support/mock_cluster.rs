use redis::{
    cluster::{self, ClusterClient, ClusterClientBuilder},
    ErrorKind, FromRedisValue, PushInfo, RedisError,
};

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use {
    once_cell::sync::Lazy,
    redis::{IntoConnectionInfo, RedisResult, Value},
};

use tokio::sync::mpsc;

#[cfg(feature = "cluster-async")]
use redis::{aio, cluster_async, RedisFuture};

#[cfg(feature = "cluster-async")]
use futures::future;

#[cfg(feature = "cluster-async")]
use tokio::runtime::Runtime;

type Handler = Arc<dyn Fn(&[u8], u16) -> Result<(), RedisResult<Value>> + Send + Sync>;

pub struct MockConnectionBehavior {
    pub id: String,
    pub handler: Handler,
    pub connection_id_provider: AtomicUsize,
    pub returned_ip_type: ConnectionIPReturnType,
    pub return_connection_err: ShouldReturnConnectionError,
}

impl MockConnectionBehavior {
    fn new(id: &str, handler: Handler) -> Self {
        Self {
            id: id.to_string(),
            handler,
            connection_id_provider: AtomicUsize::new(0),
            returned_ip_type: ConnectionIPReturnType::default(),
            return_connection_err: ShouldReturnConnectionError::default(),
        }
    }

    #[must_use]
    pub fn register_new(id: &str, handler: Handler) -> RemoveHandler {
        get_behaviors().insert(id.to_string(), Self::new(id, handler));
        RemoveHandler(vec![id.to_string()])
    }

    fn get_handler(&self) -> Handler {
        self.handler.clone()
    }
}

pub fn modify_mock_connection_behavior(name: &str, func: impl FnOnce(&mut MockConnectionBehavior)) {
    func(
        get_behaviors()
            .get_mut(name)
            .expect("Handler `{name}` was not installed"),
    );
}

pub fn get_mock_connection_handler(name: &str) -> Handler {
    MOCK_CONN_BEHAVIORS
        .read()
        .unwrap()
        .get(name)
        .expect("Handler `{name}` was not installed")
        .get_handler()
}

pub fn get_mock_connection(name: &str, id: usize) -> MockConnection {
    get_mock_connection_with_port(name, id, 6379)
}

pub fn get_mock_connection_with_port(name: &str, id: usize, port: u16) -> MockConnection {
    MockConnection {
        id,
        handler: get_mock_connection_handler(name),
        port,
    }
}

static MOCK_CONN_BEHAVIORS: Lazy<RwLock<HashMap<String, MockConnectionBehavior>>> =
    Lazy::new(Default::default);

fn get_behaviors() -> std::sync::RwLockWriteGuard<'static, HashMap<String, MockConnectionBehavior>>
{
    MOCK_CONN_BEHAVIORS.write().unwrap()
}

#[derive(Default)]
pub enum ConnectionIPReturnType {
    /// New connections' IP will be returned as None
    #[default]
    None,
    /// Creates connections with the specified IP
    Specified(IpAddr),
    /// Each new connection will be created with a different IP based on the passed atomic integer
    Different(AtomicUsize),
}

#[derive(Default)]
pub enum ShouldReturnConnectionError {
    /// Don't return a connection error
    #[default]
    No,
    /// Always return a connection error
    Yes,
    /// Return connection error when the internal index is an odd number
    OnOddIdx(AtomicUsize),
}

#[derive(Clone)]
pub struct MockConnection {
    pub id: usize,
    pub handler: Handler,
    pub port: u16,
}

#[cfg(feature = "cluster-async")]
impl cluster_async::Connect for MockConnection {
    fn connect<'a, T>(
        info: T,
        _response_timeout: Duration,
        _connection_timeout: Duration,
        _socket_addr: Option<SocketAddr>,
        _push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
    ) -> RedisFuture<'a, (Self, Option<IpAddr>)>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        let info = info.into_connection_info().unwrap();

        let (name, port) = match &info.addr {
            redis::ConnectionAddr::Tcp(addr, port) => (addr, *port),
            _ => unreachable!(),
        };
        let binding = MOCK_CONN_BEHAVIORS.read().unwrap();
        let conn_utils = binding
            .get(name)
            .unwrap_or_else(|| panic!("MockConnectionUtils for `{name}` were not installed"));
        let conn_err = Box::pin(future::err(RedisError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "mock-io-error",
        ))));
        match &conn_utils.return_connection_err {
            ShouldReturnConnectionError::No => {}
            ShouldReturnConnectionError::Yes => return conn_err,
            ShouldReturnConnectionError::OnOddIdx(curr_idx) => {
                if curr_idx.fetch_add(1, Ordering::SeqCst) % 2 != 0 {
                    // raise an error on each odd number
                    return conn_err;
                }
            }
        }

        let ip = match &conn_utils.returned_ip_type {
            ConnectionIPReturnType::Specified(ip) => Some(*ip),
            ConnectionIPReturnType::Different(ip_getter) => {
                let first_ip_num = ip_getter.fetch_add(1, Ordering::SeqCst) as u8;
                Some(IpAddr::V4(Ipv4Addr::new(first_ip_num, 0, 0, 0)))
            }
            ConnectionIPReturnType::None => None,
        };

        Box::pin(future::ok((
            MockConnection {
                id: conn_utils
                    .connection_id_provider
                    .fetch_add(1, Ordering::SeqCst),
                handler: conn_utils.get_handler(),
                port,
            },
            ip,
        )))
    }
}

impl cluster::Connect for MockConnection {
    fn connect<'a, T>(info: T, _timeout: Option<Duration>) -> RedisResult<Self>
    where
        T: IntoConnectionInfo,
    {
        let info = info.into_connection_info().unwrap();

        let (name, port) = match &info.addr {
            redis::ConnectionAddr::Tcp(addr, port) => (addr, *port),
            _ => unreachable!(),
        };
        let binding = MOCK_CONN_BEHAVIORS.read().unwrap();
        let conn_utils = binding
            .get(name)
            .unwrap_or_else(|| panic!("MockConnectionUtils for `{name}` were not installed"));
        Ok(MockConnection {
            id: conn_utils
                .connection_id_provider
                .fetch_add(1, Ordering::SeqCst),
            handler: conn_utils.get_handler(),
            port,
        })
    }

    fn send_packed_command(&mut self, _cmd: &[u8]) -> RedisResult<()> {
        Ok(())
    }

    fn set_write_timeout(&self, _dur: Option<std::time::Duration>) -> RedisResult<()> {
        Ok(())
    }

    fn set_read_timeout(&self, _dur: Option<std::time::Duration>) -> RedisResult<()> {
        Ok(())
    }

    fn recv_response(&mut self) -> RedisResult<Value> {
        Ok(Value::Nil)
    }
}

pub fn contains_slice(xs: &[u8], ys: &[u8]) -> bool {
    for i in 0..xs.len() {
        if xs[i..].starts_with(ys) {
            return true;
        }
    }
    false
}

pub fn respond_startup(name: &str, cmd: &[u8]) -> Result<(), RedisResult<Value>> {
    if contains_slice(cmd, b"PING") || contains_slice(cmd, b"SETNAME") {
        Err(Ok(Value::SimpleString("OK".into())))
    } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
        Err(Ok(Value::Array(vec![Value::Array(vec![
            Value::Int(0),
            Value::Int(16383),
            Value::Array(vec![
                Value::BulkString(name.as_bytes().to_vec()),
                Value::Int(6379),
            ]),
        ])])))
    } else if contains_slice(cmd, b"READONLY") {
        Err(Ok(Value::SimpleString("OK".into())))
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct MockSlotRange {
    pub primary_port: u16,
    pub replica_ports: Vec<u16>,
    pub slot_range: std::ops::Range<u16>,
}

pub fn respond_startup_with_replica(name: &str, cmd: &[u8]) -> Result<(), RedisResult<Value>> {
    respond_startup_with_replica_using_config(name, cmd, None)
}

pub fn respond_startup_two_nodes(name: &str, cmd: &[u8]) -> Result<(), RedisResult<Value>> {
    respond_startup_with_replica_using_config(
        name,
        cmd,
        Some(vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![],
                slot_range: (0..8191),
            },
            MockSlotRange {
                primary_port: 6380,
                replica_ports: vec![],
                slot_range: (8192..16383),
            },
        ]),
    )
}

pub fn create_topology_from_config(name: &str, slots_config: Vec<MockSlotRange>) -> Value {
    let slots_vec = slots_config
        .into_iter()
        .map(|slot_config| {
            let mut config = vec![
                Value::Int(slot_config.slot_range.start as i64),
                Value::Int(slot_config.slot_range.end as i64),
                Value::Array(vec![
                    Value::BulkString(name.as_bytes().to_vec()),
                    Value::Int(slot_config.primary_port as i64),
                ]),
            ];
            config.extend(slot_config.replica_ports.into_iter().map(|replica_port| {
                Value::Array(vec![
                    Value::BulkString(name.as_bytes().to_vec()),
                    Value::Int(replica_port as i64),
                ])
            }));
            Value::Array(config)
        })
        .collect();
    Value::Array(slots_vec)
}

pub fn respond_startup_with_replica_using_config(
    name: &str,
    cmd: &[u8],
    slots_config: Option<Vec<MockSlotRange>>,
) -> Result<(), RedisResult<Value>> {
    let slots_config = slots_config.unwrap_or(vec![
        MockSlotRange {
            primary_port: 6379,
            replica_ports: vec![6380],
            slot_range: (0..8191),
        },
        MockSlotRange {
            primary_port: 6381,
            replica_ports: vec![6382],
            slot_range: (8192..16383),
        },
    ]);
    if contains_slice(cmd, b"PING") || contains_slice(cmd, b"SETNAME") {
        Err(Ok(Value::SimpleString("OK".into())))
    } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
        let slots = create_topology_from_config(name, slots_config);
        Err(Ok(slots))
    } else if contains_slice(cmd, b"READONLY") {
        Err(Ok(Value::SimpleString("OK".into())))
    } else {
        Ok(())
    }
}

#[cfg(feature = "cluster-async")]
impl aio::ConnectionLike for MockConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a redis::Cmd) -> RedisFuture<'a, Value> {
        Box::pin(future::ready(
            (self.handler)(&cmd.get_packed_command(), self.port)
                .expect_err("Handler did not specify a response"),
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

impl redis::ConnectionLike for MockConnection {
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        (self.handler)(cmd, self.port).expect_err("Handler did not specify a response")
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        _count: usize,
    ) -> RedisResult<Vec<Value>> {
        let res = (self.handler)(cmd, self.port).expect_err("Handler did not specify a response");
        match res {
            Err(err) => Err(err),
            Ok(res) => {
                if let Value::Array(results) = res {
                    match results.into_iter().nth(offset) {
                        Some(Value::Array(res)) => Ok(res),
                        _ => Err((ErrorKind::ResponseError, "non-array response").into()),
                    }
                } else {
                    Err((
                        ErrorKind::ResponseError,
                        "non-array response",
                        String::from_owned_redis_value(res).unwrap(),
                    )
                        .into())
                }
            }
        }
    }

    fn get_db(&self) -> i64 {
        0
    }

    fn check_connection(&mut self) -> bool {
        true
    }

    fn is_open(&self) -> bool {
        true
    }
}

pub struct MockEnv {
    #[cfg(feature = "cluster-async")]
    pub runtime: Runtime,
    pub client: redis::cluster::ClusterClient,
    pub connection: redis::cluster::ClusterConnection<MockConnection>,
    #[cfg(feature = "cluster-async")]
    pub async_connection: redis::cluster_async::ClusterConnection<MockConnection>,
    #[allow(unused)]
    pub handler: RemoveHandler,
}

pub struct RemoveHandler(Vec<String>);

impl Drop for RemoveHandler {
    fn drop(&mut self) {
        for id in &self.0 {
            get_behaviors().remove(id);
        }
    }
}

impl MockEnv {
    pub fn new(
        id: &str,
        handler: impl Fn(&[u8], u16) -> Result<(), RedisResult<Value>> + Send + Sync + 'static,
    ) -> Self {
        Self::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{id}")]),
            id,
            handler,
        )
    }

    pub fn with_client_builder(
        client_builder: ClusterClientBuilder,
        id: &str,
        handler: impl Fn(&[u8], u16) -> Result<(), RedisResult<Value>> + Send + Sync + 'static,
    ) -> Self {
        #[cfg(feature = "cluster-async")]
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        let id = id.to_string();
        let handler = MockConnectionBehavior::register_new(
            &id,
            Arc::new(move |cmd, port| handler(cmd, port)),
        );
        let client = client_builder.build().unwrap();
        let connection = client.get_generic_connection(None).unwrap();
        #[cfg(feature = "cluster-async")]
        let async_connection = runtime
            .block_on(client.get_async_generic_connection())
            .unwrap();
        MockEnv {
            #[cfg(feature = "cluster-async")]
            runtime,
            client,
            connection,
            #[cfg(feature = "cluster-async")]
            async_connection,
            handler,
        }
    }
}
