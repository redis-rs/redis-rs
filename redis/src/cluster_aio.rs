use std::io;
use std::iter::Iterator;
use std::sync::Arc;

use futures_util::future::{self, FutureExt};
use futures_util::stream::{FuturesUnordered, StreamExt};
use rand::seq::IteratorRandom;
use rand::thread_rng;
use tokio::sync::oneshot;

use crate::aio::{ConnectionLike, MultiplexedConnection, PipelineMessage, Runtime};
use crate::cluster::{get_connection_info, parse_slots_response, NodeCmd, SlotMap};
use crate::cluster_client::ClusterParams;
use crate::cluster_routing::{is_illegal_cluster_pipeline_cmd, Routable, UNROUTABLE_ERROR};
use crate::cmd::{cmd, Cmd};
use crate::connection::ConnectionInfo;
use crate::pipeline::Pipeline;
use crate::types::{ErrorKind, HashMap, RedisError, RedisFuture, RedisResult, Value};

// Cluster related logic.
#[derive(Default)]
struct NodesManager {
    runtime: Runtime,
    cluster_params: ClusterParams,
    initial_nodes: Vec<ConnectionInfo>,

    connections: HashMap<String, MultiplexedConnection>,
    slots: SlotMap,
}

impl NodesManager {
    pub(crate) async fn new(
        runtime: Runtime,
        cluster_params: &ClusterParams,
        initial_nodes: &[ConnectionInfo],
    ) -> RedisResult<NodesManager> {
        let mut nodes_manager = NodesManager {
            runtime,
            cluster_params: cluster_params.clone(),
            initial_nodes: initial_nodes.to_vec(),
            ..Default::default()
        };
        nodes_manager.create_initial_connections().await?;

        Ok(nodes_manager)
    }

    async fn create_initial_connections(&mut self) -> RedisResult<()> {
        let nodes = self
            .initial_nodes
            .iter()
            .map(|info| info.addr.to_string())
            .collect::<Vec<_>>();

        self.refresh_connections(nodes).await;

        if self.connections.is_empty() {
            return Err(RedisError::from((
                ErrorKind::IoError,
                "It failed to check startup nodes.",
            )));
        }

        self.refresh_slots().await?;
        Ok(())
    }

    async fn refresh_connections(&mut self, nodes: Vec<String>) {
        for node in nodes {
            let conn = if let Some(conn) = self.connections.get_mut(&node) {
                conn
            } else if self.connect(&node).await.is_ok() {
                self.connections.get_mut(&node).unwrap()
            } else {
                continue;
            };

            if !conn.check_connection().await {
                self.connections.remove(&node);
            }
        }
    }

    // Query a node to discover slot -> master mappings.
    async fn refresh_slots(&mut self) -> RedisResult<()> {
        let mut cmd = Cmd::new();
        cmd.arg("CLUSTER").arg("SLOTS");

        let len = self.connections.len();
        let samples = self
            .connections
            .values_mut()
            .choose_multiple(&mut thread_rng(), len);
        for conn in samples {
            if let Ok(Value::Bulk(response)) = cmd.query_async(conn).await {
                let slots = parse_slots_response(response, &self.cluster_params)?;

                let mut nodes = slots.values().flatten().cloned().collect::<Vec<_>>();
                nodes.sort_unstable();
                nodes.dedup();
                self.refresh_connections(nodes).await;

                self.slots = slots;
                return Ok(());
            }
        }

        Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error.",
            "didn't get any slots from server".to_string(),
        )))
    }

    async fn connect(&mut self, node: &str) -> RedisResult<()> {
        let cluster_params = &self.cluster_params;
        let info = get_connection_info(node, cluster_params);

        let mut conn = self.runtime.get_multiplexed_connection(&info).await?;

        if cluster_params.read_from_replicas {
            // If READONLY is sent to primary nodes, it will have no effect
            cmd("READONLY").query_async(&mut conn).await?;
        }
        self.connections.insert(node.to_string(), conn);
        Ok(())
    }

    fn get_connection_for_node(&self, node: &str) -> RedisResult<&MultiplexedConnection> {
        self.connections
            .get(node)
            .ok_or_else(|| RedisError::from(UNROUTABLE_ERROR))
    }

    #[allow(dead_code)]
    fn get_connection_for_route(&self, route: (u16, usize)) -> RedisResult<&MultiplexedConnection> {
        let node = self.get_node_for_route(route);
        self.get_connection_for_node(node)
    }

    fn get_node_for_route(&self, route: (u16, usize)) -> &str {
        let (slot, idx) = route;
        &self.slots.range(&slot..).next().unwrap().1[idx]
    }

    fn map_cmds_to_nodes(&self, cmds: &[Cmd]) -> RedisResult<Vec<NodeCmd>> {
        let mut cmd_map: HashMap<String, NodeCmd> = HashMap::with_capacity(self.slots.len());

        for (idx, cmd) in cmds.iter().enumerate() {
            let node = match cmd.route()? {
                (Some(slot), idx) => self.get_node_for_route((slot, idx)),
                (None, _idx) => unreachable!(),
            };

            let nc = if let Some(nc) = cmd_map.get_mut(node) {
                nc
            } else {
                cmd_map
                    .entry(node.to_string())
                    .or_insert_with(|| NodeCmd::new(node))
            };

            nc.indexes.push(idx);
            cmd.write_packed_command(&mut nc.pipe);
        }

        // Workaround for https://github.com/tkaitchuck/aHash/issues/118
        Ok(cmd_map.into_iter().map(|(_k, v)| v).collect())
    }
}

/// A multiplexed Redis cluster connection, which can be cloned, allowing multiple requests to be
/// sent concurrently over the same underlying [connections](MultiplexedConnection).
#[derive(Clone)]
pub struct MultiplexedClusterConnection {
    nodes_manager: Arc<NodesManager>,
}

impl MultiplexedClusterConnection {
    pub(crate) async fn new(
        runtime: Runtime,
        cluster_params: &ClusterParams,
        initial_nodes: &[ConnectionInfo],
    ) -> RedisResult<MultiplexedClusterConnection> {
        let nodes_manager = NodesManager::new(runtime, cluster_params, initial_nodes).await?;

        Ok(MultiplexedClusterConnection {
            nodes_manager: Arc::new(nodes_manager),
        })
    }

    async fn execute_pipeline(&self, nc: NodeCmd) -> RedisResult<Vec<(usize, Value)>> {
        let conn = self.nodes_manager.get_connection_for_node(&nc.node)?;

        let (sender, receiver) = oneshot::channel();

        conn.pipeline
            .0
            .send(PipelineMessage {
                input: nc.pipe,
                output: sender,
                response_count: nc.indexes.len(),
            })
            .await
            .map_err(|_| broken_pipe())?;

        Ok(nc
            .indexes
            .into_iter()
            .zip(receiver.await.map_err(|_| broken_pipe())??.into_iter())
            .collect())
    }
}

// ConnectionLike impl for MultiplexedClusterConnection.
impl ConnectionLike for MultiplexedClusterConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        let route = match cmd.route() {
            Ok(route) => match route {
                (Some(slot), idx) => (slot, idx),
                (None, _idx) => return future::err(RedisError::from(UNROUTABLE_ERROR)).boxed(),
            },
            Err(err) => return future::err(err).boxed(),
        };

        let node = self.nodes_manager.get_node_for_route(route);
        let mut nc = NodeCmd::new(node);
        nc.indexes.push(0);
        cmd.write_packed_command(&mut nc.pipe);

        self.execute_pipeline(nc)
            .map(|res| res.map(|mut val| val.pop().unwrap().1))
            .boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        _offset: usize,
        _count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        for cmd in pipeline.commands() {
            let cmd_name = std::str::from_utf8(cmd.arg_idx(0).unwrap_or(b""))
                .unwrap_or("")
                .trim()
                .to_ascii_uppercase();

            if is_illegal_cluster_pipeline_cmd(&cmd_name) {
                return future::err(RedisError::from((
                    UNROUTABLE_ERROR.0,
                    UNROUTABLE_ERROR.1,
                    format!(
                        "Command '{}' can't be executed in a cluster pipeline.",
                        cmd_name
                    ),
                )))
                .boxed();
            }
        }

        if let Ok(ncs) = self.nodes_manager.map_cmds_to_nodes(pipeline.commands()) {
            (async move {
                let mut futs = ncs
                    .into_iter()
                    .map(|nc| self.execute_pipeline(nc))
                    .collect::<FuturesUnordered<_>>();

                let mut ret = vec![Value::Nil; pipeline.commands().len()];
                while let Some(res) = futs.next().await {
                    if let Ok(items) = res {
                        for (idx, val) in items {
                            ret[idx] = val;
                        }
                    }
                }
                Ok(ret)
            })
            .boxed()
        } else {
            future::err(broken_pipe()).boxed()
        }
    }

    fn get_db(&self) -> i64 {
        0
    }

    fn supports_transactions(&self) -> bool {
        false
    }
}

fn broken_pipe() -> RedisError {
    RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe))
}
