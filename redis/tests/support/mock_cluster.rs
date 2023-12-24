use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use redis::cluster::{self, ClusterClient, ClusterClientBuilder};

use {
    once_cell::sync::Lazy,
    redis::{IntoConnectionInfo, RedisResult, Value},
};

#[cfg(feature = "cluster-async")]
use redis::{aio, cluster_async, RedisFuture};

#[cfg(feature = "cluster-async")]
use futures::future;

#[cfg(feature = "cluster-async")]
use tokio::runtime::Runtime;

type Handler = Arc<dyn Fn(&[u8], u16) -> Result<(), RedisResult<Value>> + Send + Sync>;

static HANDLERS: Lazy<RwLock<HashMap<String, Handler>>> = Lazy::new(Default::default);

#[derive(Clone)]
pub struct MockConnection {
    pub handler: Handler,
    pub port: u16,
}

#[cfg(feature = "cluster-async")]
impl cluster_async::Connect for MockConnection {
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
                .unwrap_or_else(|| panic!("Handler `{name}` were not installed"))
                .clone(),
            port,
        }))
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
        Ok(MockConnection {
            handler: HANDLERS
                .read()
                .unwrap()
                .get(name)
                .unwrap_or_else(|| panic!("Handler `{name}` were not installed"))
                .clone(),
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
    if contains_slice(cmd, b"PING") {
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

pub fn respond_startup_two_nodes(name: &str, cmd: &[u8]) -> Result<(), RedisResult<Value>> {
    if contains_slice(cmd, b"PING") {
        Err(Ok(Value::SimpleString("OK".into())))
    } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
        Err(Ok(Value::Array(vec![
            Value::Array(vec![
                Value::Int(0),
                Value::Int(8191),
                Value::Array(vec![
                    Value::BulkString(name.as_bytes().to_vec()),
                    Value::Int(6379),
                ]),
            ]),
            Value::Array(vec![
                Value::Int(8192),
                Value::Int(16383),
                Value::Array(vec![
                    Value::BulkString(name.as_bytes().to_vec()),
                    Value::Int(6380),
                ]),
            ]),
        ])))
    } else if contains_slice(cmd, b"READONLY") {
        Err(Ok(Value::SimpleString("OK".into())))
    } else {
        Ok(())
    }
}

pub fn respond_startup_with_replica(name: &str, cmd: &[u8]) -> Result<(), RedisResult<Value>> {
    if contains_slice(cmd, b"PING") {
        Err(Ok(Value::SimpleString("OK".into())))
    } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
        Err(Ok(Value::Array(vec![
            Value::Array(vec![
                Value::Int(0),
                Value::Int(8191),
                Value::Array(vec![
                    Value::BulkString(name.as_bytes().to_vec()),
                    Value::Int(6379),
                ]),
                Value::Array(vec![
                    Value::BulkString(name.as_bytes().to_vec()),
                    Value::Int(6380),
                ]),
            ]),
            Value::Array(vec![
                Value::Int(8192),
                Value::Int(16383),
                Value::Array(vec![
                    Value::BulkString(name.as_bytes().to_vec()),
                    Value::Int(6381),
                ]),
                Value::Array(vec![
                    Value::BulkString(name.as_bytes().to_vec()),
                    Value::Int(6382),
                ]),
            ]),
        ])))
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
        _cmd: &[u8],
        _offset: usize,
        _count: usize,
    ) -> RedisResult<Vec<Value>> {
        Ok(vec![])
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
            HANDLERS.write().unwrap().remove(id);
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
        HANDLERS
            .write()
            .unwrap()
            .insert(id.clone(), Arc::new(move |cmd, port| handler(cmd, port)));

        let client = client_builder.build().unwrap();
        let connection = client.get_generic_connection().unwrap();
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
            handler: RemoveHandler(vec![id]),
        }
    }
}
