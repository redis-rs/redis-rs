//! Latency-aware read routing for Redis Cluster.
//!
//! This example demonstrates a custom [`ReadRoutingStrategyFactory`] that
//! measures PING round-trip times to every cluster node in a single persistent
//! background task and publishes the latest latency snapshot via a
//! [`tokio::sync::watch`] channel. Per-connection strategies read from the
//! watch and route each read command to the lowest-latency eligible node.
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
use redis::cluster_routing::{
    AnyNodeRoutingInfo, NodeAddress, ReadRoutingStrategy, ReadRoutingStrategyFactory,
    ReplicaRoutingInfo, SlotTopology,
};
use redis::{AsyncCommands, RedisResult};

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
    topology_tx: mpsc::UnboundedSender<Vec<SlotTopology>>,
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
    topology_tx: mpsc::UnboundedSender<Vec<SlotTopology>>,
    latency_rx: watch::Receiver<LatencySnapshot>,
    /// Shared ownership of the probe task — keeps it alive even if the factory
    /// is dropped while connections still exist.
    _probe_handle: Arc<ProbeHandle>,
}

impl ReadRoutingStrategy for LatencyAwareStrategy {
    fn on_topology_changed(&self, topology: &[SlotTopology]) {
        // Forward the new topology to the probe task. If the channel is closed
        // the factory has been dropped and there's nothing to do.
        let _ = self.topology_tx.send(topology.to_vec());
    }

    fn choose_any<'a>(&self, info: &AnyNodeRoutingInfo<'a>) -> &'a NodeAddress {
        let snapshot = self.latency_rx.borrow();
        lowest_latency_any(info, &snapshot)
    }

    fn choose_replica<'a>(&self, info: &ReplicaRoutingInfo<'a>) -> &'a NodeAddress {
        let snapshot = self.latency_rx.borrow();
        lowest_latency_replica(info, &snapshot)
    }
}

fn lowest_latency_any<'a>(
    info: &AnyNodeRoutingInfo<'a>,
    snapshot: &LatencySnapshot,
) -> &'a NodeAddress {
    let mut best = info.primary;
    let mut best_latency = snapshot
        .get(info.primary.address())
        .copied()
        .unwrap_or(Duration::MAX);

    if let Some(ref replicas) = info.replicas {
        for replica in replicas.iter() {
            let latency = snapshot
                .get(replica.address())
                .copied()
                .unwrap_or(Duration::MAX);
            if latency < best_latency {
                best = replica;
                best_latency = latency;
            }
        }
    }

    best
}

fn lowest_latency_replica<'a>(
    info: &ReplicaRoutingInfo<'a>,
    snapshot: &LatencySnapshot,
) -> &'a NodeAddress {
    let mut best = info.replicas.first();
    let mut best_latency = snapshot
        .get(best.address())
        .copied()
        .unwrap_or(Duration::MAX);

    for replica in info.replicas.iter().skip(1) {
        let latency = snapshot
            .get(replica.address())
            .copied()
            .unwrap_or(Duration::MAX);
        if latency < best_latency {
            best = replica;
            best_latency = latency;
        }
    }

    best
}

// ---------------------------------------------------------------------------
// Background probe task
// ---------------------------------------------------------------------------

/// Single long-lived task that:
/// 1. Receives topology updates via `topology_rx`.
/// 2. Maintains persistent PING connections to every known node.
/// 3. Periodically measures RTT and publishes a snapshot via `latency_tx`.
async fn probe_task(
    mut topology_rx: mpsc::UnboundedReceiver<Vec<SlotTopology>>,
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

        // Probe every node.
        let mut snapshot = HashMap::with_capacity(nodes.len());
        for addr in &nodes {
            let rtt = ping_node(addr, &mut connections).await;
            match rtt {
                Ok(d) => {
                    snapshot.insert(addr.clone(), d);
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
fn unique_addresses(topology: &[SlotTopology]) -> HashSet<String> {
    let mut seen = HashSet::new();
    for slot in topology {
        seen.insert(slot.primary.address().to_owned());
        for replica in &slot.replicas {
            seen.insert(replica.address().to_owned());
        }
    }
    seen
}

/// PING a node using a persistent connection, reconnecting if necessary.
async fn ping_node(
    addr: &str,
    connections: &mut HashMap<String, redis::aio::MultiplexedConnection>,
) -> RedisResult<Duration> {
    if !connections.contains_key(addr) {
        let client = redis::Client::open(format!("redis://{addr}"))?;
        let con = client.get_multiplexed_async_connection().await?;
        connections.insert(addr.to_owned(), con);
    }

    let con = connections.get_mut(addr).unwrap();
    let start = Instant::now();
    redis::cmd("PING").exec_async(con).await?;
    Ok(start.elapsed())
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
