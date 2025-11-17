//! This module provides async functionality for connecting to Redis / Valkey Clusters.
//!
//! The cluster connection is meant to abstract the fact that a cluster is composed of multiple nodes,
//! and to provide an API which is as close as possible to that of a single node connection. In order to do that,
//! the cluster connection maintains connections to each node in the Redis/ Valkey cluster, and can route
//! requests automatically to the relevant nodes. In cases that the cluster connection receives indications
//! that the cluster topology has changed, it will query nodes in order to find the current cluster topology.
//! If it disconnects from some nodes, it will automatically reconnect to those nodes.
//!
//! By default, [`ClusterConnection`] makes use of [`MultiplexedConnection`] and maintains a pool
//! of connections to each node in the cluster.
//!
//! # Example
//! ```rust,no_run
//! use redis::cluster::ClusterClient;
//! use redis::AsyncTypedCommands;
//!
//! async fn fetch_an_integer() -> String {
//!     let nodes = vec!["redis://127.0.0.1/"];
//!     let client = ClusterClient::new(nodes).unwrap();
//!     let mut connection = client.get_async_connection().await.unwrap();
//!     connection.set("test", "test_data").await.unwrap();
//!     let rv = connection.get("test").await.unwrap().unwrap();
//!     return rv;
//! }
//! ```
//!
//! # Pipelining
//! ```rust,no_run
//! use redis::cluster::ClusterClient;
//! use redis::{Value, AsyncCommands};
//!
//! async fn fetch_an_integer() -> redis::RedisResult<()> {
//!     let nodes = vec!["redis://127.0.0.1/"];
//!     let client = ClusterClient::new(nodes).unwrap();
//!     let mut connection = client.get_async_connection().await.unwrap();
//!     let key = "test";
//!
//!     redis::pipe()
//!         .rpush(key, "123").ignore()
//!         .ltrim(key, -10, -1).ignore()
//!         .expire(key, 60).ignore()
//!         .exec_async(&mut connection).await
//! }
//! ```
//!
//! # Pubsub
//!
//! Pubsub, and generally receiving push messages from the cluster nodes, is now supported
//! when defining a connection with [crate::ProtocolVersion::RESP3] and some
//! [crate::aio::AsyncPushSender] to receive the messages on.
//!
//! ```rust,no_run
//! use redis::cluster::ClusterClientBuilder;
//! use redis::{Value, AsyncCommands};
//!
//! async fn fetch_an_integer() -> redis::RedisResult<()> {
//!     let nodes = vec!["redis://127.0.0.1/?protocol=3"];
//!     let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
//!     let client = ClusterClientBuilder::new(nodes)
//!         .use_protocol(redis::ProtocolVersion::RESP3)
//!         .push_sender(tx).build()?;
//!     let mut connection = client.get_async_connection().await?;
//!     connection.subscribe("channel").await?;
//!     while let Some(msg) = rx.recv().await {
//!         println!("Got: {:?}", msg);
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Sending request to specific node
//! In some cases you'd want to send a request to a specific node in the cluster, instead of
//! letting the cluster connection decide by itself to which node it should send the request.
//! This can happen, for example, if you want to send SCAN commands to each node in the cluster.
//!
//! ```rust,no_run
//! use redis::cluster::ClusterClient;
//! use redis::{Value, AsyncCommands};
//! use redis::cluster_routing::{ RoutingInfo, SingleNodeRoutingInfo };
//!
//! async fn fetch_an_integer() -> redis::RedisResult<Value> {
//!     let nodes = vec!["redis://127.0.0.1/"];
//!     let client = ClusterClient::new(nodes)?;
//!     let mut connection = client.get_async_connection().await?;
//!     let routing_info = RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress{
//!         host: "redis://127.0.0.1".to_string(),
//!         port: 6378
//!     });
//!     connection.route_command(redis::cmd("PING"), routing_info).await
//! }
//! ```
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    io, mem,
    ops::Deref,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{self, Poll},
    time::Duration,
};

mod request;
mod routing;
use crate::{
    aio::{ConnectionLike, HandleContainer, MultiplexedConnection, Runtime},
    check_resp3,
    cluster_handling::{
        client::ClusterParams,
        get_connection_info,
        routing::{
            MultipleNodeRoutingInfo, Redirect, ResponsePolicy, RoutingInfo, SingleNodeRoutingInfo,
        },
        slot_cmd,
        slot_map::{Slot, SlotMap},
        topology::parse_slots,
    },
    cmd,
    errors::closed_connection_error,
    subscription_tracker::SubscriptionTracker,
    AsyncConnectionConfig, Cmd, ConnectionInfo, ErrorKind, IntoConnectionInfo, RedisError,
    RedisFuture, RedisResult, ToRedisArgs, Value,
};

#[cfg(feature = "cache-aio")]
use crate::caching::{CacheManager, CacheStatistics};
use crate::ProtocolVersion;
use arcstr::ArcStr;
use futures_util::{
    future::{self, BoxFuture, FutureExt},
    ready,
    sink::Sink,
    stream::{self, Stream, StreamExt},
};
use log::{debug, trace, warn};
use rand::{rng, seq::IteratorRandom};
use request::{CmdArg, PendingRequest, Request, RequestState, Retry};
use routing::{route_for_pipeline, InternalRoutingInfo, InternalSingleNodeRouting};
use tokio::sync::{mpsc, oneshot, RwLock};

struct ClientSideState {
    protocol: ProtocolVersion,
    _task_handle: HandleContainer,
    response_timeout: Option<Duration>,
    runtime: Runtime,
    #[cfg(feature = "cache-aio")]
    cache_manager: Option<CacheManager>,
}

/// This represents an async Redis Cluster connection.
///
/// It stores the underlying connections maintained for each node in the cluster,
/// as well as common parameters for connecting to nodes and executing commands.
#[derive(Clone)]
pub struct ClusterConnection<C = MultiplexedConnection> {
    state: Arc<ClientSideState>,
    sender: mpsc::Sender<Message<C>>,
}

impl<C> ClusterConnection<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    pub(crate) async fn new(
        initial_nodes: &[ConnectionInfo],
        cluster_params: ClusterParams,
    ) -> RedisResult<ClusterConnection<C>> {
        let (connection, connect_receiver) = Self::new_inner(initial_nodes, cluster_params);
        dbg!(connect_receiver.await).map_err(|_| {
            RedisError::from((ErrorKind::Io, "Cluster connection task were dropped"))
        })??;
        Ok(connection)
    }

    pub(crate) fn new_pending(
        initial_nodes: &[ConnectionInfo],
        cluster_params: ClusterParams,
    ) -> ClusterConnection<C> {
        let (connection, _connect_receiver) = Self::new_inner(initial_nodes, cluster_params);
        connection
    }

    pub(crate) fn new_inner(
        initial_nodes: &[ConnectionInfo],
        cluster_params: ClusterParams,
    ) -> (ClusterConnection<C>, oneshot::Receiver<RedisResult<()>>) {
        let protocol = cluster_params.protocol.unwrap_or_default();
        let response_timeout = cluster_params.response_timeout;
        #[cfg(feature = "cache-aio")]
        let cache_manager = cluster_params.cache_manager.clone();
        let runtime = Runtime::locate();
        let mut inner = ClusterConnInner::new(initial_nodes, cluster_params);

        let (connect_sender, connect_receiver) = oneshot::channel::<RedisResult<()>>();
        let (sender, mut receiver) = mpsc::channel::<Message<_>>(100);
        let stream = async move {
            let connect_result = inner.wait_for_initial_connection().await;
            let _ = connect_sender.send(connect_result);

            let _ = stream::poll_fn(move |cx| receiver.poll_recv(cx))
                .map(Ok)
                .forward(inner)
                .await;
        };
        let _task_handle = HandleContainer::new(runtime.spawn(stream));

        (
            ClusterConnection {
                sender,
                state: Arc::new(ClientSideState {
                    protocol,
                    _task_handle,
                    response_timeout,
                    runtime,
                    #[cfg(feature = "cache-aio")]
                    cache_manager,
                }),
            },
            connect_receiver,
        )
    }

    /// Send a command to the given `routing`, and aggregate the response according to `response_policy`.
    pub async fn route_command(&mut self, cmd: Cmd, routing: RoutingInfo) -> RedisResult<Value> {
        trace!("send_packed_command");
        let (sender, receiver) = oneshot::channel();
        let request = async {
            self.sender
                .send(Message {
                    cmd: CmdArg::Cmd {
                        cmd: Arc::new(cmd),
                        routing: routing.into(),
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
        };

        match self.state.response_timeout {
            Some(duration) => self.state.runtime.timeout(duration, request).await?,
            None => request.await,
        }
    }

    /// Send commands in `pipeline` to the given `route`. If `route` is [None], it will be sent to a random node.
    pub async fn route_pipeline(
        &mut self,
        pipeline: crate::Pipeline,
        offset: usize,
        count: usize,
        route: SingleNodeRoutingInfo,
    ) -> RedisResult<Vec<Value>> {
        let (sender, receiver) = oneshot::channel();

        let request = async {
            self.sender
                .send(Message {
                    cmd: CmdArg::Pipeline {
                        pipeline: Arc::new(pipeline),
                        offset,
                        count,
                        route: route.into(),
                    },
                    sender,
                })
                .await
                .map_err(|_| closed_connection_error())?;
            receiver
                .await
                .unwrap_or_else(|_| Err(closed_connection_error()))
                .map(|response| match response {
                    Response::Multiple(values) => values,
                    Response::Single(_) => unreachable!(),
                })
        };

        match self.state.response_timeout {
            Some(duration) => self.state.runtime.timeout(duration, request).await?,
            None => request.await,
        }
    }

    /// Subscribes to a new channel(s).
    ///
    /// Updates from the sender will be sent on the push sender that was passed to the manager.
    /// If the manager was configured without a push sender, the connection won't be able to pass messages back to the user.
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    /// It should be noted that the subscription will be automatically resubscribed after disconnections, so the user might
    /// receive additional pushes with [crate::PushKind::Subscribe], later after the subscription completed.
    ///
    /// ```rust,no_run
    /// # async fn func() -> redis::RedisResult<()> {
    /// let nodes = vec!["redis://127.0.0.1/protocol=3"];
    /// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    /// let client = redis::cluster::ClusterClientBuilder::new(nodes)
    ///   .use_protocol(redis::ProtocolVersion::RESP3)
    ///   .push_sender(tx).build()?;
    /// let mut con = client.get_async_connection().await?;
    /// con.subscribe(&["channel_1", "channel_2"]).await?;
    /// con.unsubscribe(&["channel_1", "channel_2"]).await?;
    /// # Ok(()) }
    /// ```
    pub async fn subscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.state.protocol);
        let mut cmd = cmd("SUBSCRIBE");
        cmd.arg(channel_name);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Unsubscribes from channel(s).
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    pub async fn unsubscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.state.protocol);
        let mut cmd = cmd("UNSUBSCRIBE");
        cmd.arg(channel_name);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Subscribes to new channel(s) with pattern(s).
    ///
    /// Updates from the sender will be sent on the push sender that was passed to the manager.
    /// If the manager was configured without a push sender, the manager won't be able to pass messages back to the user.
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    /// It should be noted that the subscription will be automatically resubscribed after disconnections, so the user might
    /// receive additional pushes with [crate::PushKind::PSubscribe], later after the subscription completed.
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "aio")]
    /// # async fn do_something() -> redis::RedisResult<()> {
    /// let nodes = vec!["redis://127.0.0.1/protocol=3"];
    /// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    /// let client = redis::cluster::ClusterClientBuilder::new(nodes)
    ///   .use_protocol(redis::ProtocolVersion::RESP3)
    ///   .push_sender(tx).build()?;
    /// let mut connection = client.get_async_connection().await?;
    /// connection.psubscribe("channel*_1").await?;
    /// connection.psubscribe(&["channel*_2", "channel*_3"]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn psubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.state.protocol);
        let mut cmd = cmd("PSUBSCRIBE");
        cmd.arg(channel_pattern);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Unsubscribes from channel pattern(s).
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    pub async fn punsubscribe(&mut self, channel_pattern: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.state.protocol);
        let mut cmd = cmd("PUNSUBSCRIBE");
        cmd.arg(channel_pattern);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Subscribes to a new sharded channel(s).
    ///
    /// Updates from the sender will be sent on the push sender that was passed to the manager.
    /// If the manager was configured without a push sender, the manager won't be able to pass messages back to the user.
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    /// It should be noted that the subscription will be automatically resubscribed after disconnections, so the user might
    /// receive additional pushes with [crate::PushKind::SSubscribe], later after the subscription completed.
    ///
    /// ```rust,no_run
    /// # async fn func() -> redis::RedisResult<()> {
    /// let nodes = vec!["redis://127.0.0.1/protocol=3"];
    /// let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    /// let client = redis::cluster::ClusterClientBuilder::new(nodes)
    ///   .use_protocol(redis::ProtocolVersion::RESP3)
    ///   .push_sender(tx).build()?;
    /// let mut connection = client.get_async_connection().await?;
    /// connection.ssubscribe(&["channel_1", "channel_2"]).await?;
    /// connection.sunsubscribe(&["channel_1", "channel_2"]).await?;
    /// # Ok(()) }
    /// ```
    pub async fn ssubscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.state.protocol);
        let mut cmd = cmd("SSUBSCRIBE");
        cmd.arg(channel_name);
        cmd.exec_async(self).await?;
        Ok(())
    }

    /// Unsubscribes from sharded channel(s).
    ///
    /// This method is only available when the connection is using RESP3 protocol, and will return an error otherwise.
    pub async fn sunsubscribe(&mut self, channel_name: impl ToRedisArgs) -> RedisResult<()> {
        check_resp3!(self.state.protocol);
        let mut cmd = cmd("SUNSUBSCRIBE");
        cmd.arg(channel_name);
        cmd.exec_async(self).await?;
        Ok(())
    }
    /// Gets [`CacheStatistics`] for cluster connection if caching is enabled.
    #[cfg(feature = "cache-aio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cache-aio")))]
    pub fn get_cache_statistics(&self) -> Option<CacheStatistics> {
        self.state.cache_manager.as_ref().map(|cm| cm.statistics())
    }
}

type ConnectionMap<C> = HashMap<ArcStr, C>;

/// This is the internal representation of an async Redis Cluster connection. It stores the
/// underlying connections maintained for each node in the cluster, as well
/// as common parameters for connecting to nodes and executing commands.
struct InnerCore<C> {
    conn_lock: RwLock<(ConnectionMap<C>, SlotMap)>,
    cluster_params: ClusterParams,
    pending_requests: Mutex<Vec<PendingRequest<C>>>,
    initial_nodes: Vec<ConnectionInfo>,
    subscription_tracker: Option<Mutex<SubscriptionTracker>>,
}

/// This is a clonable wrapper.
#[derive(Clone)]
struct Core<C>(Arc<InnerCore<C>>);

impl<C> Deref for Core<C> {
    type Target = InnerCore<C>;

    fn deref(&self) -> &InnerCore<C> {
        &self.0
    }
}

impl<C> Core<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    async fn execute_on_multiple_nodes<'a>(
        &self,
        cmd: &'a Arc<Cmd>,
        routing: &'a MultipleNodeRoutingInfo,
        response_policy: Option<ResponsePolicy>,
    ) -> OperationResult {
        let read_guard = self.conn_lock.read().await;
        if read_guard.0.is_empty() {
            return (
                OperationTarget::FanOut,
                Result::Err(
                    (
                        ErrorKind::ClusterConnectionNotFound,
                        "No connections found for multi-node operation",
                    )
                        .into(),
                ),
            );
        }
        let (receivers, requests): (Vec<_>, Vec<_>) = {
            let to_request = |(addr, cmd): (&ArcStr, Arc<Cmd>)| {
                read_guard.0.get(addr).cloned().map(|conn| {
                    let (sender, receiver) = oneshot::channel();
                    let addr = addr.clone();
                    (
                        (addr.clone(), receiver),
                        PendingRequest {
                            retry: 0,
                            sender: request::ResultExpectation::External(sender),
                            cmd: CmdArg::Cmd {
                                cmd,
                                routing: InternalSingleNodeRouting::Connection {
                                    identifier: addr,
                                    conn,
                                }
                                .into(),
                            },
                        },
                    )
                })
            };
            let slot_map = &read_guard.1;

            // TODO - these filter_map calls mean that we ignore nodes that are missing. Should we report an error in such cases?
            // since some of the operators drop other requests, mapping to errors here might mean that no request is sent.
            match routing {
                MultipleNodeRoutingInfo::AllNodes => slot_map
                    .addresses_for_all_nodes()
                    .into_iter()
                    .filter_map(|addr| to_request((addr, cmd.clone())))
                    .unzip(),
                MultipleNodeRoutingInfo::AllMasters => slot_map
                    .addresses_for_all_primaries()
                    .into_iter()
                    .filter_map(|addr| to_request((addr, cmd.clone())))
                    .unzip(),
                MultipleNodeRoutingInfo::MultiSlot((routes, _)) => slot_map
                    .addresses_for_multi_slot(routes)
                    .enumerate()
                    .filter_map(|(index, addr_opt)| {
                        addr_opt.and_then(|addr| {
                            let (_, indices) = routes.get(index).unwrap();
                            let cmd =
                                Arc::new(crate::cluster_routing::command_for_multi_slot_indices(
                                    cmd.as_ref(),
                                    indices.iter(),
                                ));
                            to_request((addr, cmd))
                        })
                    })
                    .unzip(),
            }
        };
        drop(read_guard);
        self.pending_requests.lock().unwrap().extend(requests);

        (
            OperationTarget::FanOut,
            Self::aggregate_results(receivers, routing, response_policy)
                .await
                .map(Response::Single),
        )
    }

    async fn aggregate_results(
        receivers: Vec<(ArcStr, oneshot::Receiver<RedisResult<Response>>)>,
        routing: &MultipleNodeRoutingInfo,
        response_policy: Option<ResponsePolicy>,
    ) -> RedisResult<Value> {
        if receivers.is_empty() {
            return Err((
                ErrorKind::ClusterConnectionNotFound,
                "No nodes found for multi-node operation",
            )
                .into());
        }

        let extract_result = |response| match response {
            Response::Single(value) => value,
            Response::Multiple(_) => unreachable!(),
        };

        let convert_result = |res: Result<RedisResult<Response>, _>| {
            res.map_err(|_| RedisError::from((ErrorKind::Client, "request wasn't handled due to internal failure"))) // this happens only if the result sender is dropped before usage.
               .and_then(|res| res.map(extract_result))
        };

        let get_receiver = |(_, receiver): (_, oneshot::Receiver<RedisResult<Response>>)| async {
            convert_result(receiver.await)
        };

        match response_policy {
            Some(ResponsePolicy::AllSucceeded) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .and_then(|mut results| {
                        results.pop().ok_or(
                            (
                                ErrorKind::ClusterConnectionNotFound,
                                "No results received for multi-node operation",
                            )
                                .into(),
                        )
                    })
            }
            Some(ResponsePolicy::OneSucceeded) => future::select_ok(
                receivers
                    .into_iter()
                    .map(|tuple| Box::pin(get_receiver(tuple))),
            )
            .await
            .map(|(result, _)| result),
            Some(ResponsePolicy::FirstSucceededNonEmptyOrAllEmpty) => {
                // We want to see each response as it arrives, and:
                //  • If we see `Value::Nil`, increment a counter.
                //  • If we see `Value::ServerError`, remember it as `last_err`.
                //  • If we see `other_value`, return it immediately.
                //
                // Once the stream is exhausted:
                //  – if all successes were Nil → return Value::Nil (indicates that all shards are empty).
                //  – else → return the last error we saw (or a generic “all‐unavailable” error).
                //
                // If we received a mix of errors and `Nil`s, we can't determine if all shards are empty, thus we return the last received error instead of `Nil`.
                let mut nil_counter = 0;
                let mut last_err = None;
                let resolved = future::join_all(receivers.into_iter().map(get_receiver)).await;
                let num_results = resolved.len();

                for val in resolved {
                    match val {
                        Ok(Value::Nil) => nil_counter += 1,
                        Ok(Value::ServerError(err)) => {
                            last_err = Some(err.into());
                        }
                        Ok(val) => return Ok(val),
                        Err(err) => {
                            last_err = Some(err);
                        }
                    }
                }

                if nil_counter == num_results {
                    Ok(Value::Nil)
                } else {
                    Err(last_err.unwrap_or_else(|| {
                        (
                            ErrorKind::ClusterConnectionNotFound,
                            "Couldn't find any connection",
                        )
                            .into()
                    }))
                }
            }
            Some(ResponsePolicy::Aggregate(op)) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .and_then(|results| crate::cluster_routing::aggregate(results, op))
            }
            Some(ResponsePolicy::AggregateLogical(op)) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .and_then(|results| crate::cluster_routing::logical_aggregate(results, op))
            }
            Some(ResponsePolicy::CombineArrays) => {
                future::try_join_all(receivers.into_iter().map(get_receiver))
                    .await
                    .and_then(|results| match routing {
                        MultipleNodeRoutingInfo::MultiSlot((vec, pattern)) => {
                            crate::cluster_routing::combine_and_sort_array_results(
                                results, vec, pattern,
                            )
                        }
                        _ => crate::cluster_routing::combine_array_results(results),
                    })
            }
            Some(ResponsePolicy::CombineMaps) => {
                let resolved =
                    future::try_join_all(receivers.into_iter().map(get_receiver)).await?;
                crate::cluster_routing::combine_map_results(resolved)
            }
            Some(ResponsePolicy::Special) | None => {
                // This is our assumption - if there's no coherent way to aggregate the responses, we just map each response to the sender, and pass it to the user.
                let results =
                    future::join_all(receivers.into_iter().map(|(addr, receiver)| async move {
                        let result =
                            convert_result(receiver.await).or_else(|err| match err.try_into() {
                                Ok(server_error) => Ok(Value::ServerError(server_error)),
                                Err(err) => Err(err),
                            })?;
                        Ok::<_, RedisError>((Value::BulkString(addr.as_bytes().to_vec()), result))
                    }))
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Value::Map(results))
            }
        }
    }

    async fn try_cmd_request(
        &self,
        cmd: Arc<Cmd>,
        routing: InternalRoutingInfo<C>,
    ) -> OperationResult {
        let route = match routing {
            InternalRoutingInfo::SingleNode(single_node_routing) => single_node_routing,
            InternalRoutingInfo::MultiNode((multi_node_routing, response_policy)) => {
                return self
                    .execute_on_multiple_nodes(&cmd, &multi_node_routing, response_policy)
                    .await;
            }
        };

        match self.get_connection(route).await {
            Ok((addr, mut conn)) => (
                addr.into(),
                conn.req_packed_command(&cmd)
                    .await
                    .inspect(|res| {
                        if !matches!(res, Value::ServerError(_)) {
                            if let Some(tracker) = &self.subscription_tracker {
                                let mut tracker = tracker.lock().unwrap();
                                tracker.update_with_cmd(cmd.as_ref());
                            }
                        }
                    })
                    .map(Response::Single),
            ),
            Err(err) => (OperationTarget::NotFound, Err(err)),
        }
    }

    async fn try_pipeline_request(
        &self,
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        route: InternalSingleNodeRouting<C>,
    ) -> OperationResult {
        let conn = self.get_connection(route);
        match conn.await {
            Ok((addr, mut conn)) => (
                OperationTarget::Node { address: addr },
                conn.req_packed_commands(&pipeline, offset, count)
                    .await
                    .inspect(|res| {
                        for (index, cmd) in pipeline.cmd_iter().enumerate() {
                            if !matches!(res[index], Value::ServerError(_)) {
                                if let Some(tracker) = &self.subscription_tracker {
                                    let mut tracker = tracker.lock().unwrap();
                                    tracker.update_with_cmd(cmd);
                                }
                            }
                        }
                    })
                    .map(Response::Multiple),
            ),
            Err(err) => (OperationTarget::NotFound, Err(err)),
        }
    }

    async fn try_request(self, cmd: CmdArg<C>) -> OperationResult {
        match cmd {
            CmdArg::Cmd { cmd, routing } => self.try_cmd_request(cmd, routing).await,
            CmdArg::Pipeline {
                pipeline,
                offset,
                count,
                route,
            } => {
                self.try_pipeline_request(pipeline, offset, count, route)
                    .await
            }
        }
    }

    async fn get_connection(
        &self,
        route: InternalSingleNodeRouting<C>,
    ) -> RedisResult<(ArcStr, C)> {
        let read_guard = self.conn_lock.read().await;

        let conn = match route {
            InternalSingleNodeRouting::Random => None,
            InternalSingleNodeRouting::SpecificNode(route) => {
                read_guard.1.slot_addr_for_route(&route).cloned()
            }
            InternalSingleNodeRouting::Connection { identifier, conn } => {
                return Ok((identifier, conn));
            }
            InternalSingleNodeRouting::Redirect { redirect, .. } => {
                drop(read_guard);
                // redirected requests shouldn't use a random connection, so they have a separate codepath.
                return self.get_redirected_connection(redirect).await;
            }
            InternalSingleNodeRouting::ByAddress(address) => {
                if let Some(conn) = read_guard.0.get(&address).cloned() {
                    return Ok((address, conn));
                } else {
                    return Err((
                        ErrorKind::Client,
                        "Requested connection not found",
                        address.to_string(),
                    )
                        .into());
                }
            }
        }
        .map(|addr| {
            let conn = read_guard.0.get(&addr).cloned();
            (addr, conn)
        });
        drop(read_guard);

        let addr_conn_option = match conn {
            Some((addr, Some(conn))) => Some((addr, conn)),
            Some((addr, None)) => self
                .connect_check_and_add(&addr)
                .await
                .ok()
                .map(|conn| (addr, conn)),
            None => None,
        };

        let (addr, conn) = match addr_conn_option {
            Some(tuple) => tuple,
            None => {
                let read_guard = self.conn_lock.read().await;
                if let Some((random_addr, random_conn)) = get_random_connection(&read_guard.0) {
                    drop(read_guard);
                    (random_addr, random_conn)
                } else {
                    return Err(
                        (ErrorKind::ClusterConnectionNotFound, "No connections found").into(),
                    );
                }
            }
        };

        Ok((addr, conn))
    }

    async fn get_redirected_connection(&self, redirect: Redirect) -> RedisResult<(ArcStr, C)> {
        let asking = matches!(redirect, Redirect::Ask(_));
        let addr = match redirect {
            Redirect::Moved(addr) => addr,
            Redirect::Ask(addr) => addr,
        };
        let read_guard = self.conn_lock.read().await;
        let conn = read_guard.0.get(&addr).cloned();
        drop(read_guard);
        let mut conn = match conn {
            Some(conn) => conn,
            None => self.connect_check_and_add(&addr).await?,
        };
        if asking {
            let _ = conn
                .req_packed_command(&crate::cmd::cmd("ASKING"))
                .await
                .and_then(|value| value.extract_error());
        }

        Ok((addr, conn))
    }

    async fn connect_check_and_add(&self, addr: &ArcStr) -> RedisResult<C> {
        match connect_and_check::<C>(addr, &self.cluster_params).await {
            Ok(conn) => {
                self.conn_lock
                    .write()
                    .await
                    .0
                    .insert(addr.clone(), conn.clone());
                Ok(conn)
            }
            Err(err) => Err(err),
        }
    }

    // Query a node to discover slot-> master mappings.
    async fn refresh_slots(self) -> RedisResult<()> {
        let mut write_guard = self.conn_lock.write().await;
        let (connections, slots) = &mut *write_guard;

        let mut result = Ok(());
        for (addr, conn) in &mut *connections {
            result = async {
                let value = conn
                    .req_packed_command(&slot_cmd())
                    .await
                    .and_then(|value| value.extract_error())?;
                let v: Vec<Slot> = parse_slots(value, addr.rsplit_once(':').unwrap().0)?;
                build_slot_map(slots, v)
            }
            .await;
            if result.is_ok() {
                break;
            }
        }
        result?;

        let mut nodes = slots.values().flatten().cloned().collect::<Vec<_>>();
        nodes.sort_unstable();
        nodes.dedup();
        self.refresh_connections_locked(connections, nodes).await;

        Ok(())
    }

    async fn refresh_connections_locked(
        &self,
        connections: &mut ConnectionMap<C>,
        nodes: Vec<ArcStr>,
    ) {
        let nodes_len = nodes.len();

        let addresses_and_connections_iter = nodes.into_iter().map(|addr| {
            let value = connections.remove(&addr);
            (addr, value)
        });

        *connections = stream::iter(addresses_and_connections_iter)
            .map(|(addr, connection)| async move {
                let res = get_or_create_conn(&addr, connection, &self.cluster_params).await;
                (addr, res)
            })
            .buffer_unordered(nodes_len.max(8))
            .fold(
                HashMap::with_capacity(nodes_len),
                |mut connections, (addr, result)| async move {
                    if let Ok(conn) = result {
                        connections.insert(addr, conn);
                    }
                    connections
                },
            )
            .await;
    }

    fn resubscribe(&self) {
        let Some(subscription_tracker) = self.subscription_tracker.as_ref() else {
            return;
        };

        let subscription_pipe = subscription_tracker
            .lock()
            .unwrap()
            .get_subscription_pipeline();

        // we send request per cmd, instead of sending the pipe together, in order to send each command to the relevant node, instead of all together to a single node.
        let requests = subscription_pipe.into_cmd_iter().map(|cmd| {
            let routing = RoutingInfo::for_routable(&cmd)
                .unwrap_or(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random))
                .into();
            PendingRequest {
                retry: 0,
                sender: request::ResultExpectation::Internal,
                cmd: CmdArg::Cmd {
                    cmd: Arc::new(cmd),
                    routing,
                },
            }
        });
        self.pending_requests.lock().unwrap().extend(requests);
    }
}

/// This is the sink for requests sent by the user.
/// It holds the stream of requests which are "in flight", E.G. on their way to the server,
/// and the inner representation of the connection.
struct ClusterConnInner<C> {
    inner: Core<C>,
    state: ConnectionState,
    #[allow(clippy::complexity)]
    in_flight_requests: stream::FuturesUnordered<Pin<Box<Request<C>>>>,
    refresh_error: Option<RedisError>,
}

fn boxed_sleep(duration: Duration) -> BoxFuture<'static, ()> {
    Box::pin(Runtime::locate_and_sleep(duration))
}

#[derive(Debug, PartialEq)]
pub(crate) enum Response {
    Single(Value),
    Multiple(Vec<Value>),
}

enum OperationTarget {
    Node { address: ArcStr },
    NotFound,
    FanOut,
}
type OperationResult = (OperationTarget, Result<Response, RedisError>);

impl From<ArcStr> for OperationTarget {
    fn from(address: ArcStr) -> Self {
        OperationTarget::Node { address }
    }
}

struct Message<C> {
    cmd: CmdArg<C>,
    sender: oneshot::Sender<RedisResult<Response>>,
}

enum RecoverFuture {
    RecoverSlots(BoxFuture<'static, RedisResult<()>>),
    Reconnect(BoxFuture<'static, RedisResult<()>>),
}

enum ConnectionState {
    PollComplete,
    Recover(RecoverFuture),
}

impl fmt::Debug for ConnectionState {
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

fn build_slot_map(slot_map: &mut SlotMap, slots_data: Vec<Slot>) -> RedisResult<()> {
    slot_map.clear();
    slot_map.fill_slots(slots_data);
    trace!("{slot_map:?}");
    Ok(())
}

impl<C> ClusterConnInner<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    fn new(initial_nodes: &[ConnectionInfo], cluster_params: ClusterParams) -> Self {
        let subscription_tracker = if cluster_params.async_push_sender.is_some() {
            Some(Mutex::new(SubscriptionTracker::default()))
        } else {
            None
        };
        let inner = Arc::new(InnerCore {
            conn_lock: RwLock::new((
                Default::default(),
                SlotMap::new(cluster_params.read_from_replicas),
            )),
            cluster_params,
            pending_requests: Mutex::new(Vec::new()),
            initial_nodes: initial_nodes.to_vec(),
            subscription_tracker,
        });
        let core = Core(inner);
        let mut inner = ClusterConnInner {
            inner: core.clone(),
            in_flight_requests: Default::default(),
            refresh_error: None,
            state: ConnectionState::PollComplete,
        };
        inner.state = ConnectionState::Recover(RecoverFuture::Reconnect(Box::pin(
            inner.reconnect_to_initial_nodes(),
        )));
        inner
    }

    async fn create_initial_connections(
        initial_nodes: &[ConnectionInfo],
        params: &ClusterParams,
    ) -> RedisResult<ConnectionMap<C>> {
        let (connections, error) = stream::iter(initial_nodes.iter().cloned())
            .map(async move |info| {
                let addr = info.addr.to_string();
                let result = connect_and_check(&addr, params).await;
                match result {
                    Ok(conn) => Ok((addr, conn)),
                    Err(e) => {
                        debug!("Failed to connect to initial node: {e:?}");
                        Err(e)
                    }
                }
            })
            .buffer_unordered(initial_nodes.len())
            .fold(
                (ConnectionMap::<C>::with_capacity(initial_nodes.len()), None),
                |(mut connections, mut error), result| async move {
                    match result {
                        Ok((addr, conn)) => {
                            connections.insert(addr.into(), conn);
                        }
                        Err(err) => {
                            // Store at least one error to use as detail in the connection error if
                            // all connections fail.
                            error = Some(err);
                        }
                    }
                    (connections, error)
                },
            )
            .await;
        if connections.is_empty() {
            if let Some(err) = error {
                return Err(RedisError::from((
                    ErrorKind::Io,
                    "Failed to create initial connections",
                    err.to_string(),
                )));
            } else {
                return Err(RedisError::from((
                    ErrorKind::Io,
                    "Failed to create initial connections",
                )));
            }
        }
        Ok(connections)
    }

    fn reconnect_to_initial_nodes(&mut self) -> impl Future<Output = RedisResult<()>> {
        debug!("Received request to reconnect to initial nodes");
        let inner = self.inner.clone();
        async move {
            let connection_map =
                Self::create_initial_connections(&inner.initial_nodes, &inner.cluster_params)
                    .await?;
            *inner.conn_lock.write().await = (
                connection_map,
                SlotMap::new(inner.cluster_params.read_from_replicas),
            );
            inner.refresh_slots().await?;
            Ok(())
        }
    }

    fn refresh_connections(&mut self, addrs: Vec<ArcStr>) -> impl Future<Output = ()> {
        let inner = self.inner.clone();
        async move {
            let mut write_guard = inner.conn_lock.write().await;

            inner
                .refresh_connections_locked(&mut write_guard.0, addrs)
                .await;
        }
    }

    async fn wait_for_initial_connection(&mut self) -> RedisResult<()> {
        if let ConnectionState::Recover(fut) =
            std::mem::replace(&mut self.state, ConnectionState::PollComplete)
        {
            dbg!(1);
            match fut {
                RecoverFuture::RecoverSlots(fut) => dbg!(fut.await)?,
                RecoverFuture::Reconnect(fut) => dbg!(fut.await)?,
            }
        }
        Ok(())
    }

    fn poll_recover(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), RedisError>> {
        let recover_future = match &mut self.state {
            ConnectionState::PollComplete => return Poll::Ready(Ok(())),
            ConnectionState::Recover(future) => future,
        };
        let res = match recover_future {
            RecoverFuture::RecoverSlots(ref mut future) => match ready!(future.as_mut().poll(cx)) {
                Ok(_) => {
                    trace!("Recovered!");
                    self.state = ConnectionState::PollComplete;
                    Ok(())
                }
                Err(err) => {
                    trace!("Recover slots failed!");
                    *future = Box::pin(self.inner.clone().refresh_slots());
                    Err(err)
                }
            },
            RecoverFuture::Reconnect(ref mut future) => {
                match ready!(future.as_mut().poll(cx)) {
                    Err(err) => warn!("Can't reconnect to initial nodes: `{err}`"),
                    Ok(()) => trace!("Reconnected connections"),
                }
                self.state = ConnectionState::PollComplete;
                Ok(())
            }
        };
        if res.is_ok() {
            self.inner.resubscribe();
        }
        Poll::Ready(res)
    }

    fn poll_complete(&mut self, cx: &mut task::Context<'_>) -> Poll<PollFlushAction> {
        let mut poll_flush_action = PollFlushAction::None;

        let mut pending_requests_guard = self.inner.pending_requests.lock().unwrap();
        if !pending_requests_guard.is_empty() {
            let mut pending_requests = mem::take(&mut *pending_requests_guard);
            for request in pending_requests.drain(..) {
                // Drop the request if noone is waiting for a response to free up resources for
                // requests callers care about (load shedding). It will be ambiguous whether the
                // request actually goes through regardless.
                if request.sender.is_closed() {
                    continue;
                }

                let future = self.inner.clone().try_request(request.cmd.clone()).boxed();
                self.in_flight_requests.push(Box::pin(Request {
                    retry_params: self.inner.cluster_params.retry_params.clone(),
                    request: Some(request),
                    future: RequestState::Future { future },
                }));
            }
            *pending_requests_guard = pending_requests;
        }
        drop(pending_requests_guard);

        loop {
            let (request_handling, next) =
                match Pin::new(&mut self.in_flight_requests).poll_next(cx) {
                    Poll::Ready(Some(result)) => result,
                    Poll::Ready(None) | Poll::Pending => break,
                };
            match request_handling {
                Some(Retry::MoveToPending { request }) => {
                    self.inner.pending_requests.lock().unwrap().push(request);
                }
                Some(Retry::Immediately { request }) => {
                    let future = self.inner.clone().try_request(request.cmd.clone());
                    self.in_flight_requests.push(Box::pin(Request {
                        retry_params: self.inner.cluster_params.retry_params.clone(),
                        request: Some(request),
                        future: RequestState::Future {
                            future: Box::pin(future),
                        },
                    }));
                }
                Some(Retry::AfterSleep {
                    request,
                    sleep_duration,
                }) => {
                    let future = RequestState::Sleep {
                        sleep: boxed_sleep(sleep_duration),
                    };
                    self.in_flight_requests.push(Box::pin(Request {
                        retry_params: self.inner.cluster_params.retry_params.clone(),
                        request: Some(request),
                        future,
                    }));
                }
                None => {}
            };
            poll_flush_action = poll_flush_action.change_state(next);
        }

        if !matches!(poll_flush_action, PollFlushAction::None) || self.in_flight_requests.is_empty()
        {
            Poll::Ready(poll_flush_action)
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
            } else {
                // Use a separate binding for this to release the lock guard before calling send.
                let maybe_request = self.inner.pending_requests.lock().unwrap().pop();
                if let Some(request) = maybe_request {
                    request.sender.send(Err(self.refresh_error.take().unwrap()));
                }
            }
        }
    }
}

async fn get_or_create_conn<C>(
    addr: &str,
    conn_option: Option<C>,
    params: &ClusterParams,
) -> RedisResult<C>
where
    C: Connect + ConnectionLike + Clone + Send + Sync + 'static,
{
    if let Some(mut conn) = conn_option {
        match check_connection(&mut conn).await {
            Ok(_) => Ok(conn),
            Err(_) => connect_and_check(addr, params).await,
        }
    } else {
        connect_and_check(addr, params).await
    }
}

#[derive(Debug, PartialEq)]
enum PollFlushAction {
    None,
    RebuildSlots,
    Reconnect(Vec<ArcStr>),
    ReconnectFromInitialConnections,
}

impl PollFlushAction {
    fn change_state(self, next_state: PollFlushAction) -> PollFlushAction {
        match (self, next_state) {
            (PollFlushAction::None, next_state) => next_state,
            (next_state, PollFlushAction::None) => next_state,
            (PollFlushAction::ReconnectFromInitialConnections, _)
            | (_, PollFlushAction::ReconnectFromInitialConnections) => {
                PollFlushAction::ReconnectFromInitialConnections
            }

            (PollFlushAction::RebuildSlots, _) | (_, PollFlushAction::RebuildSlots) => {
                PollFlushAction::RebuildSlots
            }

            (PollFlushAction::Reconnect(mut addrs), PollFlushAction::Reconnect(new_addrs)) => {
                addrs.extend(new_addrs);
                Self::Reconnect(addrs)
            }
        }
    }
}

impl<C> Sink<Message<C>> for ClusterConnInner<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut task::Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, msg: Message<C>) -> Result<(), Self::Error> {
        trace!("start_send");
        let Message { cmd, sender } = msg;

        self.inner
            .pending_requests
            .lock()
            .unwrap()
            .push(PendingRequest {
                retry: 0,
                sender: request::ResultExpectation::External(sender),
                cmd,
            });
        Ok(())
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        trace!("poll_flush: {:?}", self.state);
        loop {
            self.send_refresh_error();

            if let Err(err) = ready!(self.as_mut().poll_recover(cx)) {
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

            match ready!(self.poll_complete(cx)) {
                PollFlushAction::None => return Poll::Ready(Ok(())),
                PollFlushAction::RebuildSlots => {
                    self.state = ConnectionState::Recover(RecoverFuture::RecoverSlots(Box::pin(
                        self.inner.clone().refresh_slots(),
                    )));
                }
                PollFlushAction::Reconnect(addrs) => {
                    self.state = ConnectionState::Recover(RecoverFuture::Reconnect(Box::pin(
                        self.refresh_connections(addrs).map(Ok),
                    )));
                }
                PollFlushAction::ReconnectFromInitialConnections => {
                    self.state = ConnectionState::Recover(RecoverFuture::Reconnect(Box::pin(
                        self.reconnect_to_initial_nodes(),
                    )));
                }
            }
        }
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        // Try to drive any in flight requests to completion
        match self.poll_complete(cx) {
            Poll::Ready(PollFlushAction::None) => (),
            Poll::Ready(_) => Err(())?,
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

impl<C> ConnectionLike for ClusterConnection<C>
where
    C: ConnectionLike + Send + Clone + Unpin + Sync + Connect + 'static,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        let routing = RoutingInfo::for_routable(cmd)
            .unwrap_or(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random));
        self.route_command(cmd.clone(), routing).boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        async move {
            let route = route_for_pipeline(pipeline)?;
            self.route_pipeline(pipeline.clone(), offset, count, route.into())
                .await
        }
        .boxed()
    }

    fn get_db(&self) -> i64 {
        0
    }
}
/// Implements the process of connecting to a Redis server
/// and obtaining a connection handle.
pub trait Connect: Sized {
    /// Connect to a node, returning handle for command execution.
    fn connect_with_config<'a, T>(info: T, config: AsyncConnectionConfig) -> RedisFuture<'a, Self>
    where
        T: IntoConnectionInfo + Send + 'a;
}

impl Connect for MultiplexedConnection {
    fn connect_with_config<'a, T>(info: T, config: AsyncConnectionConfig) -> RedisFuture<'a, Self>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        async move {
            let connection_info = info.into_connection_info()?;
            let client = crate::Client::open(connection_info)?;
            client
                .get_multiplexed_async_connection_with_config(&config)
                .await
        }
        .boxed()
    }
}

async fn connect_and_check<C>(node: &str, params: &ClusterParams) -> RedisResult<C>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let info = get_connection_info(node, params)?;
    let mut config =
        AsyncConnectionConfig::default().set_connection_timeout(Some(params.connection_timeout));
    config = config.set_response_timeout(params.response_timeout);

    if let Some(push_sender) = &params.async_push_sender {
        config = config.set_push_sender_internal(push_sender.clone());
    }
    if let Some(resolver) = &params.async_dns_resolver {
        config = config.set_dns_resolver_internal(resolver.clone());
    }
    #[cfg(feature = "cache-aio")]
    if let Some(cache_manager) = &params.cache_manager {
        config = config.set_cache_manager(cache_manager.clone_and_increase_epoch());
    }
    let mut conn = match C::connect_with_config(info, config).await {
        Ok(conn) => conn,
        Err(err) => {
            warn!("Failed to connect to node: {node:?}, due to: {err:?}");
            return Err(err);
        }
    };

    let check = if params.read_from_replicas {
        // If READONLY is sent to primary nodes, it will have no effect
        cmd("READONLY")
    } else {
        cmd("PING")
    };

    conn.req_packed_command(&check).await?;
    Ok(conn)
}

async fn check_connection<C>(conn: &mut C) -> RedisResult<()>
where
    C: ConnectionLike + Send + 'static,
{
    let mut cmd = Cmd::new();
    cmd.arg("PING");
    cmd.query_async::<()>(conn).await?;
    Ok(())
}

fn get_random_connection<C>(connections: &ConnectionMap<C>) -> Option<(ArcStr, C)>
where
    C: Clone,
{
    connections
        .iter()
        .choose(&mut rng())
        .map(|(addr, conn)| (addr.clone(), conn.clone()))
}
