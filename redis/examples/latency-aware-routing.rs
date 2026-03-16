//! Latency-aware read routing for Redis Cluster.
//!
//! This example demonstrates a custom [`ReadRoutingStrategyFactory`] that
//! measures PING round-trip times to every cluster node in a single persistent
//! background task and publishes the latest latency snapshot via a
//! [`tokio::sync::watch`] channel. Per-connection strategies read from the
//! watch and route each read command to the lowest-latency eligible node.
//!
//! **Warning:** This example is illustrative — it shows what a more complex
//! routing strategy might look like, but it has not undergone extensive testing.
//! Due diligence should be used before adopting it as a strategy for actual
//! use.
//!
//! Run with a Redis Cluster available:
//!
//! ```sh
//! cargo run --example latency-aware-routing --features cluster-async,tokio-comp
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

use redis::cluster::ClusterClientBuilder;
use redis::cluster_read_routing::{
    ClusterTopology, ReadCandidates, ReadRoutingStrategy, ReadRoutingStrategyFactory,
};
use redis::{AsyncCommands, RedisResult, cluster::NodeAddress};

/// A snapshot of measured latencies, keyed by node address string.
type LatencySnapshot = HashMap<String, Duration>;

// ---------------------------------------------------------------------------
// Factory — one per client, owns the background probe task
// ---------------------------------------------------------------------------

/// Shared handle that keeps the background probe task alive as long as any
/// factory or strategy references it.
struct ProbeHandle(tokio::task::JoinHandle<()>);

impl Drop for ProbeHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct LatencyAwareFactory {
    topology_tx: mpsc::UnboundedSender<ClusterTopology>,
    latency_rx: watch::Receiver<LatencySnapshot>,
    _probe_handle: Arc<ProbeHandle>,
}

impl LatencyAwareFactory {
    fn new(probe_interval: Duration) -> Self {
        let (topology_tx, topology_rx) = mpsc::unbounded_channel();
        let (latency_tx, latency_rx) = watch::channel(HashMap::new());

        let handle = Arc::new(ProbeHandle(tokio::spawn(probe_task(
            topology_rx,
            latency_tx,
            probe_interval,
        ))));

        Self {
            topology_tx,
            latency_rx,
            _probe_handle: handle,
        }
    }
}

impl ReadRoutingStrategyFactory for LatencyAwareFactory {
    fn create_strategy(&self) -> Box<dyn ReadRoutingStrategy> {
        Box::new(LatencyAwareStrategy {
            topology_tx: self.topology_tx.clone(),
            latency_rx: self.latency_rx.clone(),
            _probe_handle: Arc::clone(&self._probe_handle),
        })
    }
}

// ---------------------------------------------------------------------------
// Per-connection strategy
// ---------------------------------------------------------------------------

struct LatencyAwareStrategy {
    topology_tx: mpsc::UnboundedSender<ClusterTopology>,
    latency_rx: watch::Receiver<LatencySnapshot>,
    /// Shared ownership of the probe task — keeps it alive even if the factory
    /// is dropped while connections still exist.
    _probe_handle: Arc<ProbeHandle>,
}

impl ReadRoutingStrategy for LatencyAwareStrategy {
    fn on_topology_changed(&self, topology: ClusterTopology) {
        // Forward the new topology to the probe task. If the channel is closed
        // the factory has been dropped and there's nothing to do.
        let _ = self.topology_tx.send(topology);
    }

    fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
        let snapshot = self.latency_rx.borrow();
        let latency_of = |node: &NodeAddress| {
            snapshot
                .get(&node.to_string())
                .copied()
                .unwrap_or(Duration::MAX)
        };
        match candidates {
            ReadCandidates::AnyNode {
                primary, replicas, ..
            } => match replicas {
                Some(replicas) => std::iter::once(*primary)
                    .chain(replicas.iter())
                    .min_by_key(|node| latency_of(node))
                    // expect is safe because `once` guarantees at least one element
                    .expect("iter is not empty"),
                None => primary,
            },
            ReadCandidates::ReplicasOnly { replicas, .. } => replicas
                .iter()
                .min_by_key(|node| latency_of(node))
                // expect is safe because Replicas is guaranteed non-empty
                .expect("replicas is non-empty"),
        }
    }
}

// ---------------------------------------------------------------------------
// Background probe task
// ---------------------------------------------------------------------------

/// Single long-lived task that:
/// 1. Receives topology updates via `topology_rx`.
/// 2. Maintains persistent PING connections to every known node.
/// 3. Periodically measures RTT and publishes a snapshot via `latency_tx`.
async fn probe_task(
    mut topology_rx: mpsc::UnboundedReceiver<ClusterTopology>,
    latency_tx: watch::Sender<LatencySnapshot>,
    probe_interval: Duration,
) {
    let mut nodes: HashSet<String> = HashSet::new();
    // Keep a persistent connection per node to avoid reconnect overhead.
    let mut connections: HashMap<String, redis::aio::MultiplexedConnection> = HashMap::new();

    loop {
        // Drain any pending topology updates, keeping only the latest.
        let mut latest_topology = None;
        loop {
            match topology_rx.try_recv() {
                Ok(topology) => latest_topology = Some(topology),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return,
            }
        }
        if let Some(topology) = latest_topology {
            nodes = unique_addresses(&topology);
            println!(
                "[probe] topology update — {} unique nodes: {:?}",
                nodes.len(),
                nodes
            );
        }

        if nodes.is_empty() {
            // No topology yet — wait for the first one.
            match topology_rx.recv().await {
                Some(topology) => {
                    nodes = unique_addresses(&topology);
                    println!(
                        "[probe] initial topology — {} unique nodes: {:?}",
                        nodes.len(),
                        nodes
                    );
                }
                None => return, // Factory dropped, shut down.
            }
        }

        // Ensure all nodes have connections before probing.
        for addr in &nodes {
            if !connections.contains_key(addr.as_str()) {
                match redis::Client::open(format!("redis://{addr}")) {
                    Ok(client) => match client.get_multiplexed_async_connection().await {
                        Ok(con) => {
                            connections.insert(addr.clone(), con);
                        }
                        Err(e) => {
                            eprintln!("[probe] {addr}: connect failed: {e}");
                        }
                    },
                    Err(e) => {
                        eprintln!("[probe] {addr}: {e}");
                    }
                }
            }
        }

        // Probe all nodes concurrently.
        let futures: Vec<_> = nodes
            .iter()
            .filter_map(|addr| {
                connections.get(addr.as_str()).map(|con| {
                    let mut con = con.clone();
                    let addr = addr.clone();
                    async move {
                        let start = Instant::now();
                        let result = redis::cmd("PING").exec_async(&mut con).await;
                        (addr, result.map(|()| start.elapsed()))
                    }
                })
            })
            .collect();
        let results = futures::future::join_all(futures).await;

        let mut snapshot = HashMap::with_capacity(results.len());
        for (addr, result) in results {
            match result {
                Ok(d) => {
                    snapshot.insert(addr, d);
                }
                Err(e) => {
                    eprintln!("[probe] {addr}: {e}");
                    // Drop the broken connection so we reconnect next round.
                    connections.remove(addr.as_str());
                }
            }
        }

        // Log the results.
        let mut entries: Vec<_> = snapshot.iter().collect();
        entries.sort_by_key(|(_, d)| *d);
        println!("[probe] latencies:");
        for (addr, d) in &entries {
            println!("  {addr}: {d:.1?}");
        }

        // Publish.
        let _ = latency_tx.send(snapshot);

        tokio::time::sleep(probe_interval).await;
    }
}

/// Extract unique node addresses from a topology snapshot.
fn unique_addresses(topology: &ClusterTopology) -> HashSet<String> {
    let mut seen = HashSet::new();
    for shard in topology.shards() {
        seen.insert(shard.primary.to_string());
        for replica in &shard.replicas {
            seen.insert(replica.to_string());
        }
    }
    seen
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> RedisResult<()> {
    let nodes = std::env::args().skip(1).collect::<Vec<_>>();

    let nodes = if nodes.is_empty() {
        vec![
            "redis://127.0.0.1:6379/".to_string(),
            "redis://127.0.0.1:6380/".to_string(),
            "redis://127.0.0.1:6381/".to_string(),
        ]
    } else {
        nodes
    };

    println!("Connecting to cluster: {nodes:?}");

    let factory = LatencyAwareFactory::new(Duration::from_secs(5));

    let client = ClusterClientBuilder::new(nodes)
        .read_routing_strategy(factory)
        .build()?;

    let mut con = client.get_async_connection().await?;

    // Give the probe task time to collect initial measurements.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Seed keys that hash to different slots so reads might hit different shards.
    let keys = ["key:1", "key:2", "key:3"];
    for key in &keys {
        let _: () = con.set(*key, format!("value-of-{key}")).await?;
    }

    // Read from all shards in a loop.
    for i in 0..10 {
        for key in &keys {
            let val: String = con.get(*key).await?;
            println!("[{i}] GET {key} = {val}");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    Ok(())
}
