use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;

use super::traits::{ClusterTopology, ReadCandidates, ReadRoutingStrategy};
use crate::cluster_handling::NodeAddress;

/// Per-shard state: a counter keyed by the shard's primary address.
struct ShardCounters {
    topology: ClusterTopology,
    counters: HashMap<NodeAddress, AtomicUsize>,
}

/// Routes reads to replica nodes in round-robin order, falling back to the
/// primary when no replicas exist.
///
/// Each instance maintains an independent counter **per shard** that increments
/// on every call to [`route_read`](ReadRoutingStrategy::route_read). This
/// ensures that reads to different shards rotate independently — a hot shard
/// won't skew the rotation for other shards.
///
/// Cloning a `RoundRobinReplicaStrategy` preserves the shard structure but
/// resets all counters to zero, so each cluster connection gets its own
/// independent rotation.
pub struct RoundRobinReplicaStrategy {
    state: Arc<ArcSwap<ShardCounters>>,
}

impl RoundRobinReplicaStrategy {
    /// Creates a new `RoundRobinReplicaStrategy` strategy with all counters starting at 0.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for RoundRobinReplicaStrategy {
    fn default() -> Self {
        Self {
            state: Arc::new(ArcSwap::from_pointee(ShardCounters {
                topology: ClusterTopology::from_shards(Vec::new()),
                counters: HashMap::new(),
            })),
        }
    }
}

impl Clone for RoundRobinReplicaStrategy {
    fn clone(&self) -> Self {
        // Preserve the shard structure but create fresh zero counters.
        let old = self.state.load();
        let counters = old
            .counters
            .keys()
            .map(|k| (k.clone(), AtomicUsize::new(0)))
            .collect();
        Self {
            state: Arc::new(ArcSwap::from_pointee(ShardCounters {
                topology: old.topology.clone(),
                counters,
            })),
        }
    }
}

impl ReadRoutingStrategy for RoundRobinReplicaStrategy {
    fn on_topology_changed(&self, topology: ClusterTopology) {
        let counters = topology
            .shards()
            .map(|shard| (shard.primary.clone(), AtomicUsize::new(0)))
            .collect();
        self.state
            .store(Arc::new(ShardCounters { topology, counters }));
    }

    fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
        let state = self.state.load();
        let idx = state
            .topology
            .shard_for_slot(candidates.slot())
            .and_then(|shard| state.counters.get(&shard.primary))
            .map(|counter| counter.fetch_add(1, Ordering::Relaxed))
            .unwrap_or(0);

        match candidates {
            ReadCandidates::AnyNode {
                replicas, primary, ..
            } => match replicas {
                Some(replicas) => replicas.get(idx % replicas.len()).expect("non-empty"),
                None => primary,
            },
            ReadCandidates::ReplicasOnly { replicas, .. } => {
                replicas.get(idx % replicas.len()).expect("non-empty")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_handling::read_routing::{Replicas, Shard};

    fn node(host: &str, port: u16) -> NodeAddress {
        NodeAddress::from_parts(host.into(), port)
    }

    fn setup_strategy() -> RoundRobinReplicaStrategy {
        let strategy = RoundRobinReplicaStrategy::new();
        // Two shards: shard 1 owns slots 0-1000, shard 2 owns slots 1001-2000.
        strategy.on_topology_changed(ClusterTopology::from_shards(vec![
            Shard {
                slot_ranges: vec![(0, 1000)],
                primary: node("primary1", 6379),
                replicas: vec![node("replica1a", 6379), node("replica1b", 6379)],
            },
            Shard {
                slot_ranges: vec![(1001, 2000)],
                primary: node("primary2", 6379),
                replicas: vec![node("replica2a", 6379), node("replica2b", 6379)],
            },
        ]));
        strategy
    }

    #[test]
    fn round_robins_within_a_shard() {
        let strategy = setup_strategy();
        let replica_a = node("replica1a", 6379);
        let replica_b = node("replica1b", 6379);
        let replicas = [replica_a.clone(), replica_b.clone()];

        let candidates = ReadCandidates::ReplicasOnly {
            slot: 1,
            replicas: Replicas::new(&replicas).unwrap(),
        };

        // Two full cycles: a, b, a, b
        assert_eq!(strategy.route_read(&candidates), &replica_a);
        assert_eq!(strategy.route_read(&candidates), &replica_b);
        assert_eq!(strategy.route_read(&candidates), &replica_a);
        assert_eq!(strategy.route_read(&candidates), &replica_b);
    }

    #[test]
    fn different_shards_rotate_independently() {
        let strategy = setup_strategy();

        let replicas1 = [node("replica1a", 6379), node("replica1b", 6379)];
        let replicas2 = [node("replica2a", 6379), node("replica2b", 6379)];

        let candidates1 = ReadCandidates::ReplicasOnly {
            slot: 1,
            replicas: Replicas::new(&replicas1).unwrap(),
        };
        let candidates2 = ReadCandidates::ReplicasOnly {
            slot: 1001,
            replicas: Replicas::new(&replicas2).unwrap(),
        };

        // interleave — each shard keeps its own position.
        assert_eq!(strategy.route_read(&candidates1), &replicas1[0]);
        assert_eq!(strategy.route_read(&candidates2), &replicas2[0]); // shard 2 starts fresh
        assert_eq!(strategy.route_read(&candidates1), &replicas1[1]);
        assert_eq!(strategy.route_read(&candidates2), &replicas2[1]);
        // Second cycle
        assert_eq!(strategy.route_read(&candidates1), &replicas1[0]);
        assert_eq!(strategy.route_read(&candidates2), &replicas2[0]);
    }

    #[test]
    fn round_robins_any_node_with_replicas() {
        let strategy = setup_strategy();
        let primary = node("primary1", 6379);
        let replica_a = node("replica1a", 6379);
        let replica_b = node("replica1b", 6379);
        let replicas = [replica_a.clone(), replica_b.clone()];

        let candidates = ReadCandidates::AnyNode {
            slot: 1,
            primary: &primary,
            replicas: Replicas::new(&replicas),
        };

        // Two full cycles through replicas: a, b, a, b
        assert_eq!(strategy.route_read(&candidates), &replica_a);
        assert_eq!(strategy.route_read(&candidates), &replica_b);
        assert_eq!(strategy.route_read(&candidates), &replica_a);
        assert_eq!(strategy.route_read(&candidates), &replica_b);
    }

    #[test]
    fn falls_back_to_primary_when_no_replicas() {
        let strategy = setup_strategy();
        let primary = node("primary1", 6379);

        let candidates = ReadCandidates::AnyNode {
            slot: 1,
            primary: &primary,
            replicas: None,
        };

        assert_eq!(strategy.route_read(&candidates), &primary);
    }

    #[test]
    fn multiple_ranges_on_same_shard_share_counter() {
        let strategy = RoundRobinReplicaStrategy::new();
        // One shard (same primary) owns two disjoint ranges: 0-1000 and 2001-3000.
        strategy.on_topology_changed(ClusterTopology::from_shards(vec![
            Shard {
                slot_ranges: vec![(0, 1000), (2001, 3000)],
                primary: node("primary1", 6379),
                replicas: vec![node("replica1a", 6379), node("replica1b", 6379)],
            },
            Shard {
                slot_ranges: vec![(1001, 2000)],
                primary: node("primary2", 6379),
                replicas: vec![node("replica2a", 6379), node("replica2b", 6379)],
            },
        ]));

        let replicas = [node("replica1a", 6379), node("replica1b", 6379)];

        let candidates_low = ReadCandidates::ReplicasOnly {
            slot: 500,
            replicas: Replicas::new(&replicas).unwrap(),
        };
        let candidates_high = ReadCandidates::ReplicasOnly {
            slot: 2500,
            replicas: Replicas::new(&replicas).unwrap(),
        };

        // Reading from slot 500 advances the shared counter.
        assert_eq!(strategy.route_read(&candidates_low), &replicas[0]);
        // Reading from slot 2500 sees the same counter (same shard), so it continues.
        assert_eq!(strategy.route_read(&candidates_high), &replicas[1]);
        // Back to low range — counter keeps advancing.
        assert_eq!(strategy.route_read(&candidates_low), &replicas[0]);
        assert_eq!(strategy.route_read(&candidates_high), &replicas[1]);
    }

    #[test]
    fn clone_resets_counters() {
        let strategy = setup_strategy();
        let replicas = [node("replica1a", 6379), node("replica1b", 6379)];
        let candidates = ReadCandidates::ReplicasOnly {
            slot: 1,
            replicas: Replicas::new(&replicas).unwrap(),
        };

        // Advance the original's counter.
        let first = strategy.route_read(&candidates);
        strategy.route_read(&candidates);

        // Clone should start fresh.
        let cloned = strategy.clone();
        let cloned_first = cloned.route_read(&candidates);
        assert_eq!(
            first, cloned_first,
            "cloned strategy should start from counter 0"
        );
    }
}
