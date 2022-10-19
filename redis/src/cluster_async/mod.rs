//! This is a rust implementation for Redis cluster library.
//!
//! This library extends redis-rs library to be able to use cluster.
//! Client impletemts traits of ConnectionLike and Commands.
//! So you can use redis-rs's access methods.
//! If you want more information, read document of redis-rs.
//!
//! Note that this library is currently not have features of Pubsub.
//!
//! # Example
//! ```rust
//! use redis_cluster_async::{Client, redis::{Commands, cmd}};
//!
//! #[tokio::main]
//! async fn main() -> redis::RedisResult<()> {
//! #   let _ = env_logger::try_init();
//!     let nodes = vec!["redis://127.0.0.1:7000/", "redis://127.0.0.1:7001/", "redis://127.0.0.1:7002/"];
//!
//!     let client = Client::open(nodes)?;
//!     let mut connection = client.get_connection().await?;
//!     cmd("SET").arg("test").arg("test_data").query_async(&mut connection).await?;
//!     let res: String = cmd("GET").arg("test").query_async(&mut connection).await?;
//!     assert_eq!(res, "test_data");
//!     Ok(())
//! }
//! ```
//!
//! # Pipelining
//! ```rust
//! use redis_cluster_async::{Client, redis::pipe};
//!
//! #[tokio::main]
//! async fn main() -> redis::RedisResult<()> {
//! #   let _ = env_logger::try_init();
//!     let nodes = vec!["redis://127.0.0.1:7000/", "redis://127.0.0.1:7001/", "redis://127.0.0.1:7002/"];
//!
//!     let client = Client::open(nodes)?;
//!     let mut connection = client.get_connection().await?;
//!     let key = "test2";
//!
//!     let mut pipe = pipe();
//!     pipe.rpush(key, "123").ignore()
//!         .ltrim(key, -10, -1).ignore()
//!         .expire(key, 60).ignore();
//!     pipe.query_async(&mut connection)
//!         .await?;
//!     Ok(())
//! }
//! ```

pub use redis;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt, io,
    iter::Iterator,
    marker::Unpin,
    mem,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};

use crc16::*;
use futures::{
    future::{self, BoxFuture},
    prelude::*,
    ready, stream,
};
use log::trace;
use pin_project_lite::pin_project;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use redis::{
    aio::ConnectionLike, Arg, Cmd, ConnectionAddr, ConnectionInfo, ErrorKind, IntoConnectionInfo,
    RedisError, RedisFuture, RedisResult, Value,
};
use tokio::sync::{mpsc, oneshot};

const SLOT_SIZE: usize = 16384;
const DEFAULT_RETRIES: u32 = 16;

/// This is a Redis cluster client.
pub struct Client {
    initial_nodes: Vec<ConnectionInfo>,
    retries: Option<u32>,
}

impl Client {
    /// Connect to a redis cluster server and return a cluster client.
    /// This does not actually open a connection yet but it performs some basic checks on the URL.
    ///
    /// # Errors
    ///
    /// If it is failed to parse initial_nodes, an error is returned.
    pub fn open<T: IntoConnectionInfo>(initial_nodes: Vec<T>) -> RedisResult<Client> {
        let mut nodes = Vec::with_capacity(initial_nodes.len());

        for info in initial_nodes {
            let info = info.into_connection_info()?;
            if let ConnectionAddr::Unix(_) = info.addr {
                return Err(RedisError::from((ErrorKind::InvalidClientConfig,
                                             "This library cannot use unix socket because Redis's cluster command returns only cluster's IP and port.")));
            }
            nodes.push(info);
        }

        Ok(Client {
            initial_nodes: nodes,
            retries: Some(DEFAULT_RETRIES),
        })
    }

    /// Set how many times we should retry a query. Set `None` to retry forever.
    /// Default: 16
    pub fn set_retries(&mut self, retries: Option<u32>) -> &mut Self {
        self.retries = retries;
        self
    }

    /// Open and get a Redis cluster connection.
    ///
    /// # Errors
    ///
    /// If it is failed to open connections and to create slots, an error is returned.
    pub async fn get_connection(&self) -> RedisResult<Connection> {
        Connection::new(&self.initial_nodes, self.retries).await
    }

    #[doc(hidden)]
    pub async fn get_generic_connection<C>(&self) -> RedisResult<Connection<C>>
    where
        C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
    {
        Connection::new(&self.initial_nodes, self.retries).await
    }
}

/// This is a connection of Redis cluster.
#[derive(Clone)]
pub struct Connection<C = redis::aio::MultiplexedConnection>(mpsc::Sender<Message<C>>);

impl<C> Connection<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    async fn new(
        initial_nodes: &[ConnectionInfo],
        retries: Option<u32>,
    ) -> RedisResult<Connection<C>> {
        Pipeline::new(initial_nodes, retries).await.map(|pipeline| {
            let (tx, mut rx) = mpsc::channel::<Message<_>>(100);

            tokio::spawn(async move {
                let _ = stream::poll_fn(move |cx| rx.poll_recv(cx))
                    .map(Ok)
                    .forward(pipeline)
                    .await;
            });

            Connection(tx)
        })
    }
}

type SlotMap = BTreeMap<u16, String>;
type ConnectionFuture<C> = future::Shared<BoxFuture<'static, C>>;
type ConnectionMap<C> = HashMap<String, ConnectionFuture<C>>;

struct Pipeline<C> {
    connections: ConnectionMap<C>,
    slots: SlotMap,
    state: ConnectionState<C>,
    in_flight_requests: stream::FuturesUnordered<
        Pin<Box<Request<BoxFuture<'static, (String, RedisResult<Response>)>, Response, C>>>,
    >,
    refresh_error: Option<RedisError>,
    pending_requests: Vec<PendingRequest<Response, C>>,
    retries: Option<u32>,
    tls: bool,
}

#[derive(Clone)]
enum CmdArg<C> {
    Cmd {
        cmd: Arc<redis::Cmd>,
        func: fn(C, Arc<redis::Cmd>) -> RedisFuture<'static, Response>,
    },
    Pipeline {
        pipeline: Arc<redis::Pipeline>,
        offset: usize,
        count: usize,
        func: fn(C, Arc<redis::Pipeline>, usize, usize) -> RedisFuture<'static, Response>,
    },
}

impl<C> CmdArg<C> {
    fn exec(&self, con: C) -> RedisFuture<'static, Response> {
        match self {
            Self::Cmd { cmd, func } => func(con, cmd.clone()),
            Self::Pipeline {
                pipeline,
                offset,
                count,
                func,
            } => func(con, pipeline.clone(), *offset, *count),
        }
    }

    fn slot(&self) -> Option<u16> {
        fn get_cmd_arg(cmd: &Cmd, arg_num: usize) -> Option<&[u8]> {
            cmd.args_iter().nth(arg_num).and_then(|arg| match arg {
                redis::Arg::Simple(arg) => Some(arg),
                redis::Arg::Cursor => None,
            })
        }

        fn position(cmd: &Cmd, candidate: &[u8]) -> Option<usize> {
            cmd.args_iter().position(|arg| match arg {
                Arg::Simple(arg) => arg.eq_ignore_ascii_case(candidate),
                _ => false,
            })
        }

        fn slot_for_command(cmd: &Cmd) -> Option<u16> {
            match get_cmd_arg(cmd, 0) {
                Some(b"EVAL") | Some(b"EVALSHA") => {
                    get_cmd_arg(cmd, 2).and_then(|key_count_bytes| {
                        let key_count_res = std::str::from_utf8(key_count_bytes)
                            .ok()
                            .and_then(|key_count_str| key_count_str.parse::<usize>().ok());
                        key_count_res.and_then(|key_count| {
                            if key_count > 0 {
                                get_cmd_arg(cmd, 3).map(|key| slot_for_key(key))
                            } else {
                                // TODO need to handle sending to all masters
                                None
                            }
                        })
                    })
                }
                Some(b"XGROUP") => get_cmd_arg(cmd, 2).map(|key| slot_for_key(key)),
                Some(b"XREAD") | Some(b"XREADGROUP") => {
                    let pos = position(cmd, b"STREAMS")?;
                    get_cmd_arg(cmd, pos + 1).map(slot_for_key)
                }
                Some(b"SCRIPT") => {
                    // TODO need to handle sending to all masters
                    None
                }
                _ => get_cmd_arg(cmd, 1).map(|key| slot_for_key(key)),
            }
        }
        match self {
            Self::Cmd { cmd, .. } => slot_for_command(cmd),
            Self::Pipeline { pipeline, .. } => {
                let mut iter = pipeline.cmd_iter();
                let slot = iter.next().map(slot_for_command)?;
                for cmd in iter {
                    if slot != slot_for_command(cmd) {
                        return None;
                    }
                }
                slot
            }
        }
    }
}

enum Response {
    Single(Value),
    Multiple(Vec<Value>),
}

struct Message<C> {
    cmd: CmdArg<C>,
    sender: oneshot::Sender<RedisResult<Response>>,
}

type RecoverFuture<C> =
    BoxFuture<'static, Result<(SlotMap, ConnectionMap<C>), (RedisError, ConnectionMap<C>)>>;

enum ConnectionState<C> {
    PollComplete,
    Recover(RecoverFuture<C>),
}

impl<C> fmt::Debug for ConnectionState<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ConnectionState::PollComplete => "PollComplete",
                ConnectionState::Recover(_) => "Recover",
            }
        )
    }
}

struct RequestInfo<C> {
    cmd: CmdArg<C>,
    slot: Option<u16>,
    excludes: HashSet<String>,
}

pin_project! {
    #[project = RequestStateProj]
    enum RequestState<F> {
        None,
        Future {
            #[pin]
            future: F,
        },
        Sleep {
            #[pin]
            sleep: tokio::time::Sleep,
        },
    }
}

struct PendingRequest<I, C> {
    retry: u32,
    sender: oneshot::Sender<RedisResult<I>>,
    info: RequestInfo<C>,
}

pin_project! {
    struct Request<F, I, C> {
        max_retries: Option<u32>,
        request: Option<PendingRequest<I, C>>,
        #[pin]
        future: RequestState<F>,
    }
}

#[must_use]
enum Next<I, C> {
    TryNewConnection {
        request: PendingRequest<I, C>,
        error: Option<RedisError>,
    },
    Err {
        request: PendingRequest<I, C>,
        error: RedisError,
    },
    Done,
}

impl<F, I, C> Future for Request<F, I, C>
where
    F: Future<Output = (String, RedisResult<I>)>,
    C: ConnectionLike,
{
    type Output = Next<I, C>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        if this.request.is_none() {
            return Poll::Ready(Next::Done);
        }
        let future = match this.future.as_mut().project() {
            RequestStateProj::Future { future } => future,
            RequestStateProj::Sleep { sleep } => {
                return match ready!(sleep.poll(cx)) {
                    () => Next::TryNewConnection {
                        request: self.project().request.take().unwrap(),
                        error: None,
                    },
                }
                .into();
            }
            _ => panic!("Request future must be Some"),
        };
        match ready!(future.poll(cx)) {
            (_, Ok(item)) => {
                trace!("Ok");
                self.respond(Ok(item));
                Next::Done.into()
            }
            (addr, Err(err)) => {
                trace!("Request error {}", err);

                let request = this.request.as_mut().unwrap();

                match *this.max_retries {
                    Some(max_retries) if request.retry >= max_retries => {
                        self.respond(Err(err));
                        return Next::Done.into();
                    }
                    _ => (),
                }
                request.retry = request.retry.saturating_add(1);

                if let Some(error_code) = err.code() {
                    if error_code == "MOVED" || error_code == "ASK" {
                        // Refresh slots and request again.
                        request.info.excludes.clear();
                        return Next::Err {
                            request: this.request.take().unwrap(),
                            error: err,
                        }
                        .into();
                    } else if error_code == "TRYAGAIN" || error_code == "CLUSTERDOWN" {
                        // Sleep and retry.
                        let sleep_duration =
                            Duration::from_millis(2u64.pow(request.retry.max(7).min(16)) * 10);
                        request.info.excludes.clear();
                        this.future.set(RequestState::Sleep {
                            sleep: tokio::time::sleep(sleep_duration),
                        });
                        return self.poll(cx);
                    }
                }

                request.info.excludes.insert(addr);

                Next::TryNewConnection {
                    request: this.request.take().unwrap(),
                    error: Some(err),
                }
                .into()
            }
        }
    }
}

impl<F, I, C> Request<F, I, C>
where
    F: Future<Output = (String, RedisResult<I>)>,
    C: ConnectionLike,
{
    fn respond(self: Pin<&mut Self>, msg: RedisResult<I>) {
        // If `send` errors the receiver has dropped and thus does not care about the message
        let _ = self
            .project()
            .request
            .take()
            .expect("Result should only be sent once")
            .sender
            .send(msg);
    }
}

impl<C> Pipeline<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    async fn new(initial_nodes: &[ConnectionInfo], retries: Option<u32>) -> RedisResult<Self> {
        let tls = initial_nodes.iter().all(|c| match c.addr {
            ConnectionAddr::TcpTls { .. } => true,
            _ => false,
        });
        let connections = Self::create_initial_connections(initial_nodes).await?;
        let mut connection = Pipeline {
            connections,
            slots: Default::default(),
            in_flight_requests: Default::default(),
            refresh_error: None,
            pending_requests: Vec::new(),
            state: ConnectionState::PollComplete,
            retries,
            tls,
        };
        let (slots, connections) = connection.refresh_slots().await.map_err(|(err, _)| err)?;
        connection.slots = slots;
        connection.connections = connections;
        Ok(connection)
    }

    async fn create_initial_connections(
        initial_nodes: &[ConnectionInfo],
    ) -> RedisResult<ConnectionMap<C>> {
        let connections = stream::iter(initial_nodes.iter().cloned())
            .map(|info| async move {
                let addr = match info.addr {
                    ConnectionAddr::Tcp(ref host, port) => match &info.redis.password {
                        Some(pw) => format!("redis://:{}@{}:{}", pw, host, port),
                        None => format!("redis://{}:{}", host, port),
                    },
                    ConnectionAddr::TcpTls { ref host, port, insecure } => match &info.redis.password {
                        Some(pw) if insecure => format!("rediss://:{}@{}:{}/#insecure", pw, host, port),
                        Some(pw) => format!("rediss://:{}@{}:{}", pw, host, port),
                        None if insecure => format!("rediss://{}:{}/#insecure", host, port),
                        None => format!("rediss://{}:{}", host, port),
                    },
                    _ => panic!("No reach."),
                };

                let result = connect_and_check(info).await;
                match result {
                    Ok(conn) => Some((addr, async { conn }.boxed().shared())),
                    Err(e) => {
                        trace!("Failed to connect to initial node: {:?}", e);
                        None
                    },
                }
            })
            .buffer_unordered(initial_nodes.len())
            .fold(
                HashMap::with_capacity(initial_nodes.len()),
                |mut connections: ConnectionMap<C>, conn| async move {
                    connections.extend(conn);
                    connections
                },
            )
            .await;
        if connections.len() == 0 {
            return Err(RedisError::from((
                ErrorKind::IoError,
                "Failed to create initial connections",
            )));
        }
        Ok(connections)
    }

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(
        &mut self,
    ) -> impl Future<Output = Result<(SlotMap, ConnectionMap<C>), (RedisError, ConnectionMap<C>)>>
    {
        let mut connections = mem::replace(&mut self.connections, Default::default());
        let use_tls = self.tls;

        async move {
            let mut result = Ok(SlotMap::new());
            for (addr, conn) in connections.iter_mut() {
                let mut conn = conn.clone().await;
                match get_slots(addr, &mut conn, use_tls)
                    .await
                    .and_then(|v| Self::build_slot_map(v))
                {
                    Ok(s) => {
                        result = Ok(s);
                        break;
                    }
                    Err(err) => result = Err(err),
                }
            }
            let slots = match result {
                Ok(slots) => slots,
                Err(err) => return Err((err, connections)),
            };

            // Remove dead connections and connect to new nodes if necessary
            let new_connections = HashMap::with_capacity(connections.len());

            let (_, connections) = stream::iter(slots.values())
                .fold(
                    (connections, new_connections),
                    move |(mut connections, mut new_connections), addr| async move {
                        if !new_connections.contains_key(addr) {
                            let new_connection = if let Some(conn) = connections.remove(addr) {
                                let mut conn = conn.await;
                                match check_connection(&mut conn).await {
                                    Ok(_) => Some((addr.to_string(), conn)),
                                    Err(_) => match connect_and_check(addr.as_ref()).await {
                                        Ok(conn) => Some((addr.to_string(), conn)),
                                        Err(_) => None,
                                    },
                                }
                            } else {
                                match connect_and_check(addr.as_ref()).await {
                                    Ok(conn) => Some((addr.to_string(), conn)),
                                    Err(_) => None,
                                }
                            };
                            if let Some((addr, new_connection)) = new_connection {
                                new_connections
                                    .insert(addr, async { new_connection }.boxed().shared());
                            }
                        }
                        (connections, new_connections)
                    },
                )
                .await;
            Ok((slots, connections))
        }
    }

    fn build_slot_map(mut slots_data: Vec<Slot>) -> RedisResult<SlotMap> {
        slots_data.sort_by_key(|slot_data| slot_data.start);
        let last_slot = slots_data.iter().try_fold(0, |prev_end, slot_data| {
            if prev_end != slot_data.start() {
                return Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "Slot refresh error.",
                    format!(
                        "Received overlapping slots {} and {}..{}",
                        prev_end, slot_data.start, slot_data.end
                    ),
                )));
            }
            Ok(slot_data.end() + 1)
        })?;

        if usize::from(last_slot) != SLOT_SIZE {
            return Err(RedisError::from((
                ErrorKind::ResponseError,
                "Slot refresh error.",
                format!("Lacks the slots >= {}", last_slot),
            )));
        }
        let slot_map = slots_data
            .iter()
            .map(|slot_data| (slot_data.end(), slot_data.master().to_string()))
            .collect();
        trace!("{:?}", slot_map);
        Ok(slot_map)
    }

    fn get_connection(&mut self, slot: u16) -> (String, ConnectionFuture<C>) {
        if let Some((_, addr)) = self.slots.range(&slot..).next() {
            if let Some(conn) = self.connections.get(addr) {
                return (addr.clone(), conn.clone());
            }

            // Create new connection.
            //
            let (_, random_conn) = get_random_connection(&self.connections, None); // TODO Only do this lookup if the first check fails
            let connection_future = {
                let addr = addr.clone();
                async move {
                    match connect_and_check(addr.as_ref()).await {
                        Ok(conn) => conn,
                        Err(_) => random_conn.await,
                    }
                }
            }
            .boxed()
            .shared();
            self.connections
                .insert(addr.clone(), connection_future.clone());
            (addr.clone(), connection_future)
        } else {
            // Return a random connection
            get_random_connection(&self.connections, None)
        }
    }

    fn try_request(
        &mut self,
        info: &RequestInfo<C>,
    ) -> impl Future<Output = (String, RedisResult<Response>)> {
        // TODO remove clone by changing the ConnectionLike trait
        let cmd = info.cmd.clone();
        let (addr, conn) = if info.excludes.len() > 0 || info.slot.is_none() {
            get_random_connection(&self.connections, Some(&info.excludes))
        } else {
            self.get_connection(info.slot.unwrap())
        };
        async move {
            let conn = conn.await;
            let result = cmd.exec(conn).await;
            (addr, result)
        }
    }

    fn poll_recover(
        &mut self,
        cx: &mut task::Context<'_>,
        mut future: RecoverFuture<C>,
    ) -> Poll<Result<(), RedisError>> {
        match future.as_mut().poll(cx) {
            Poll::Ready(Ok((slots, connections))) => {
                trace!("Recovered with {} connections!", connections.len());
                self.slots = slots;
                self.connections = connections;
                self.state = ConnectionState::PollComplete;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                self.state = ConnectionState::Recover(future);
                trace!("Recover not ready");
                Poll::Pending
            }
            Poll::Ready(Err((err, connections))) => {
                self.connections = connections;
                self.state = ConnectionState::Recover(Box::pin(self.refresh_slots()));
                Poll::Ready(Err(err))
            }
        }
    }

    fn poll_complete(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), RedisError>> {
        let mut connection_error = None;

        if !self.pending_requests.is_empty() {
            let mut pending_requests = mem::take(&mut self.pending_requests);
            for request in pending_requests.drain(..) {
                // Drop the request if noone is waiting for a response to free up resources for
                // requests callers care about (load shedding). It will be ambigous whether the
                // request actually goes through regardless.
                if request.sender.is_closed() {
                    continue;
                }

                let future = self.try_request(&request.info);
                self.in_flight_requests.push(Box::pin(Request {
                    max_retries: self.retries,
                    request: Some(request),
                    future: RequestState::Future {
                        future: future.boxed(),
                    },
                }));
            }
            self.pending_requests = pending_requests;
        }

        loop {
            let result = match Pin::new(&mut self.in_flight_requests).poll_next(cx) {
                Poll::Ready(Some(result)) => result,
                Poll::Ready(None) | Poll::Pending => break,
            };
            let self_ = &mut *self;
            match result {
                Next::Done => {}
                Next::TryNewConnection { request, error } => {
                    if let Some(error) = error {
                        if request.info.excludes.len() >= self_.connections.len() {
                            let _ = request.sender.send(Err(error));
                            continue;
                        }
                    }
                    let future = self.try_request(&request.info);
                    self.in_flight_requests.push(Box::pin(Request {
                        max_retries: self.retries,
                        request: Some(request),
                        future: RequestState::Future {
                            future: Box::pin(future),
                        },
                    }));
                }
                Next::Err { request, error } => {
                    connection_error = Some(error);
                    self.pending_requests.push(request);
                }
            }
        }

        if let Some(err) = connection_error {
            Poll::Ready(Err(err))
        } else if self.in_flight_requests.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn send_refresh_error(&mut self) {
        if self.refresh_error.is_some() {
            if let Some(mut request) = Pin::new(&mut self.in_flight_requests)
                .iter_pin_mut()
                .find(|request| request.request.is_some())
            {
                (*request)
                    .as_mut()
                    .respond(Err(self.refresh_error.take().unwrap()));
            } else if let Some(request) = self.pending_requests.pop() {
                let _ = request.sender.send(Err(self.refresh_error.take().unwrap()));
            }
        }
    }
}

impl<C> Sink<Message<C>> for Pipeline<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    type Error = ();

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        match mem::replace(&mut self.state, ConnectionState::PollComplete) {
            ConnectionState::PollComplete => Poll::Ready(Ok(())),
            ConnectionState::Recover(future) => {
                match ready!(self.as_mut().poll_recover(cx, future)) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(err) => {
                        // We failed to reconnect, while we will try again we will report the
                        // error if we can to avoid getting trapped in an infinite loop of
                        // trying to reconnect
                        if let Some(mut request) = Pin::new(&mut self.in_flight_requests)
                            .iter_pin_mut()
                            .find(|request| request.request.is_some())
                        {
                            (*request).as_mut().respond(Err(err));
                        } else {
                            self.refresh_error = Some(err);
                        }
                        Poll::Ready(Ok(()))
                    }
                }
            }
        }
    }

    fn start_send(mut self: Pin<&mut Self>, msg: Message<C>) -> Result<(), Self::Error> {
        trace!("start_send");
        let Message { cmd, sender } = msg;

        let excludes = HashSet::new();
        let slot = cmd.slot();

        let info = RequestInfo {
            cmd,
            slot,
            excludes,
        };

        self.pending_requests.push(PendingRequest {
            retry: 0,
            sender,
            info,
        });
        Ok(()).into()
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        trace!("poll_complete: {:?}", self.state);
        loop {
            self.send_refresh_error();

            match mem::replace(&mut self.state, ConnectionState::PollComplete) {
                ConnectionState::Recover(future) => {
                    match ready!(self.as_mut().poll_recover(cx, future)) {
                        Ok(()) => (),
                        Err(err) => {
                            // We failed to reconnect, while we will try again we will report the
                            // error if we can to avoid getting trapped in an infinite loop of
                            // trying to reconnect
                            self.refresh_error = Some(err);

                            // Give other tasks a chance to progress before we try to recover
                            // again. Since the future may not have registered a wake up we do so
                            // now so the task is not forgotten
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                }
                ConnectionState::PollComplete => match ready!(self.poll_complete(cx)) {
                    Ok(()) => return Poll::Ready(Ok(())),
                    Err(err) => {
                        trace!("Recovering {}", err);
                        self.state = ConnectionState::Recover(Box::pin(self.refresh_slots()));
                    }
                },
            }
        }
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        // Try to drive any in flight requests to completion
        match self.poll_complete(cx) {
            Poll::Ready(result) => {
                result.map_err(|_| ())?;
            }
            Poll::Pending => (),
        };
        // If we no longer have any requests in flight we are done (skips any reconnection
        // attempts)
        if self.in_flight_requests.is_empty() {
            return Poll::Ready(Ok(()));
        }

        self.poll_flush(cx)
    }
}

impl<C> ConnectionLike for Connection<C>
where
    C: ConnectionLike + Send + 'static,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        trace!("req_packed_command");
        let (sender, receiver) = oneshot::channel();
        Box::pin(async move {
            self.0
                .send(Message {
                    cmd: CmdArg::Cmd {
                        cmd: Arc::new(cmd.clone()), // TODO Remove this clone?
                        func: |mut conn, cmd| {
                            Box::pin(async move {
                                conn.req_packed_command(&cmd).await.map(Response::Single)
                            })
                        },
                    },
                    sender,
                })
                .await
                .map_err(|_| {
                    RedisError::from(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "redis_cluster: Unable to send command",
                    ))
                })?;
            receiver
                .await
                .unwrap_or_else(|_| {
                    Err(RedisError::from(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "redis_cluster: Unable to receive command",
                    )))
                })
                .map(|response| match response {
                    Response::Single(value) => value,
                    Response::Multiple(_) => unreachable!(),
                })
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        let (sender, receiver) = oneshot::channel();
        Box::pin(async move {
            self.0
                .send(Message {
                    cmd: CmdArg::Pipeline {
                        pipeline: Arc::new(pipeline.clone()), // TODO Remove this clone?
                        offset,
                        count,
                        func: |mut conn, pipeline, offset, count| {
                            Box::pin(async move {
                                conn.req_packed_commands(&pipeline, offset, count)
                                    .await
                                    .map(Response::Multiple)
                            })
                        },
                    },
                    sender,
                })
                .await
                .map_err(|_| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))?;

            receiver
                .await
                .unwrap_or_else(|_| {
                    Err(RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
                })
                .map(|response| match response {
                    Response::Multiple(values) => values,
                    Response::Single(_) => unreachable!(),
                })
        })
    }

    fn get_db(&self) -> i64 {
        0
    }
}

impl Clone for Client {
    fn clone(&self) -> Client {
        Client::open(self.initial_nodes.clone()).unwrap()
    }
}

pub trait Connect: Sized {
    fn connect<'a, T>(info: T) -> RedisFuture<'a, Self>
    where
        T: IntoConnectionInfo + Send + 'a;
}

impl Connect for redis::aio::MultiplexedConnection {
    fn connect<'a, T>(info: T) -> RedisFuture<'a, redis::aio::MultiplexedConnection>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        async move {
            let connection_info = info.into_connection_info()?;
            let client = redis::Client::open(connection_info)?;
            client.get_multiplexed_tokio_connection().await
        }
        .boxed()
    }
}

async fn connect_and_check<T, C>(info: T) -> RedisResult<C>
where
    T: IntoConnectionInfo + Send,
    C: ConnectionLike + Connect + Send + 'static,
{
    let mut conn = C::connect(info).await?;
    check_connection(&mut conn).await?;
    Ok(conn)
}

async fn check_connection<C>(conn: &mut C) -> RedisResult<()>
where
    C: ConnectionLike + Send + 'static,
{
    let mut cmd = Cmd::new();
    cmd.arg("PING");
    cmd.query_async::<_, String>(conn).await?;
    Ok(())
}

fn get_random_connection<'a, C>(
    connections: &'a ConnectionMap<C>,
    excludes: Option<&'a HashSet<String>>,
) -> (String, ConnectionFuture<C>)
where
    C: Clone,
{
    debug_assert!(!connections.is_empty());

    let mut rng = thread_rng();
    let sample = match excludes {
        Some(excludes) if excludes.len() < connections.len() => {
            let target_keys = connections.keys().filter(|key| !excludes.contains(*key));
            target_keys.choose(&mut rng)
        }
        _ => connections.keys().choose(&mut rng),
    };

    let addr = sample.expect("No targets to choose from");
    (addr.to_string(), connections.get(addr).unwrap().clone())
}

fn slot_for_key(key: &[u8]) -> u16 {
    let key = sub_key(&key);
    State::<XMODEM>::calculate(&key) % SLOT_SIZE as u16
}

// If a key contains `{` and `}`, everything between the first occurence is the only thing that
// determines the hash slot
fn sub_key(key: &[u8]) -> &[u8] {
    key.iter()
        .position(|b| *b == b'{')
        .and_then(|open| {
            let after_open = open + 1;
            key[after_open..]
                .iter()
                .position(|b| *b == b'}')
                .and_then(|close_offset| {
                    if close_offset != 0 {
                        Some(&key[after_open..after_open + close_offset])
                    } else {
                        None
                    }
                })
        })
        .unwrap_or(key)
}

#[derive(Debug)]
struct Slot {
    start: u16,
    end: u16,
    master: String,
    replicas: Vec<String>,
}

impl Slot {
    pub fn start(&self) -> u16 {
        self.start
    }
    pub fn end(&self) -> u16 {
        self.end
    }
    pub fn master(&self) -> &str {
        &self.master
    }
    #[allow(dead_code)]
    pub fn replicas(&self) -> &Vec<String> {
        &self.replicas
    }
}

// Get slot data from connection.
async fn get_slots<C>(addr: &str, connection: &mut C, use_tls: bool) -> RedisResult<Vec<Slot>>
where
    C: ConnectionLike,
{
    trace!("get_slots");
    let mut cmd = Cmd::new();
    cmd.arg("CLUSTER").arg("SLOTS");
    let value = connection.req_packed_command(&cmd).await.map_err(|err| {
        trace!("get_slots error: {}", err);
        err
    })?;
    trace!("get_slots -> {:#?}", value);
    // Parse response.
    let mut result = Vec::with_capacity(2);

    if let Value::Bulk(items) = value {
        let password = get_password(addr);
        let mut iter = items.into_iter();
        while let Some(Value::Bulk(item)) = iter.next() {
            if item.len() < 3 {
                continue;
            }

            let start = if let Value::Int(start) = item[0] {
                start as u16
            } else {
                continue;
            };

            let end = if let Value::Int(end) = item[1] {
                end as u16
            } else {
                continue;
            };

            let mut nodes: Vec<String> = item
                .into_iter()
                .skip(2)
                .filter_map(|node| {
                    if let Value::Bulk(node) = node {
                        if node.len() < 2 {
                            return None;
                        }

                        let ip = if let Value::Data(ref ip) = node[0] {
                            String::from_utf8_lossy(ip)
                        } else {
                            return None;
                        };

                        let port = if let Value::Int(port) = node[1] {
                            port
                        } else {
                            return None;
                        };
                        let scheme = if use_tls { "rediss" } else { "redis" };
                        match &password {
                            Some(pw) => Some(format!("{}://:{}@{}:{}", scheme, pw, ip, port)),
                            None => Some(format!("{}://{}:{}", scheme, ip, port)),
                        }
                    } else {
                        None
                    }
                })
                .collect();

            if nodes.len() < 1 {
                continue;
            }

            let replicas = nodes.split_off(1);
            result.push(Slot {
                start,
                end,
                master: nodes.pop().unwrap(),
                replicas,
            });
        }
    }

    Ok(result)
}

fn get_password(addr: &str) -> Option<String> {
    redis::parse_redis_url(addr).and_then(|url| url.password().map(|s| s.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot_for_packed_command(cmd: &[u8]) -> Option<u16> {
        command_key(cmd).map(|key| {
            let key = sub_key(&key);
            State::<XMODEM>::calculate(&key) % SLOT_SIZE as u16
        })
    }

    fn command_key(cmd: &[u8]) -> Option<Vec<u8>> {
        redis::parse_redis_value(cmd)
            .ok()
            .and_then(|value| match value {
                Value::Bulk(mut args) => {
                    if args.len() >= 2 {
                        match args.swap_remove(1) {
                            Value::Data(key) => Some(key),
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            })
    }

    #[test]
    fn slot() {
        assert_eq!(
            slot_for_packed_command(&[
                42, 50, 13, 10, 36, 54, 13, 10, 69, 88, 73, 83, 84, 83, 13, 10, 36, 49, 54, 13, 10,
                244, 93, 23, 40, 126, 127, 253, 33, 89, 47, 185, 204, 171, 249, 96, 139, 13, 10
            ]),
            Some(964)
        );
        assert_eq!(
            slot_for_packed_command(&[
                42, 54, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10, 36, 49, 54, 13, 10, 36, 241,
                197, 111, 180, 254, 5, 175, 143, 146, 171, 39, 172, 23, 164, 145, 13, 10, 36, 52,
                13, 10, 116, 114, 117, 101, 13, 10, 36, 50, 13, 10, 78, 88, 13, 10, 36, 50, 13, 10,
                80, 88, 13, 10, 36, 55, 13, 10, 49, 56, 48, 48, 48, 48, 48, 13, 10
            ]),
            Some(8352)
        );

        assert_eq!(
            slot_for_packed_command(&[
                42, 54, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10, 36, 49, 54, 13, 10, 169, 233,
                247, 59, 50, 247, 100, 232, 123, 140, 2, 101, 125, 221, 66, 170, 13, 10, 36, 52,
                13, 10, 116, 114, 117, 101, 13, 10, 36, 50, 13, 10, 78, 88, 13, 10, 36, 50, 13, 10,
                80, 88, 13, 10, 36, 55, 13, 10, 49, 56, 48, 48, 48, 48, 48, 13, 10
            ]),
            Some(5210),
        );
    }
}
