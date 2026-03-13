use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;

use super::traits::{ReadCandidates, ReadRoutingStrategy, SlotTopology};
use crate::cluster_handling::NodeAddress;

/// Routes reads to replica nodes in round-robin order, falling back to the
/// primary when no replicas exist.
///
/// Each instance maintains an independent counter **per shard** that increments
/// on every call to [`route_read`](ReadRoutingStrategy::route_read). This
/// ensures that reads to different shards rotate independently — a hot shard
/// won't skew the rotation for other shards. Multiple slot ranges belonging
/// to the same shard share a single counter.
///
/// Cloning a `RoundRobinReplicaStrategy` preserves the shard structure but
/// resets all counters to zero, so each cluster connection gets its own
/// independent rotation.
pub struct RoundRobinReplicaStrategy {
    /// Keyed by slot range end. Multiple entries may share the same
    /// `Arc<AtomicUsize>` when they belong to the same shard.
    shards: Arc<ArcSwap<BTreeMap<u16, Arc<AtomicUsize>>>>,
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
            shards: Arc::new(ArcSwap::new(Arc::new(BTreeMap::new()))),
        }
    }
}

impl Clone for RoundRobinReplicaStrategy {
    fn clone(&self) -> Self {
        // Preserve the shard structure (which ranges share counters) but
        // create fresh zero counters for the new connection.
        let old_map = self.shards.load();
        let mut seen: std::collections::HashMap<*const AtomicUsize, Arc<AtomicUsize>> =
            std::collections::HashMap::new();
        let new_map: BTreeMap<u16, Arc<AtomicUsize>> = old_map
            .iter()
            .map(|(&range_end, old_arc)| {
                let ptr = Arc::as_ptr(old_arc);
                let new_arc = seen
                    .entry(ptr)
                    .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
                    .clone();
                (range_end, new_arc)
            })
            .collect();
        Self {
            shards: Arc::new(ArcSwap::from_pointee(new_map)),
        }
    }
}

impl ReadRoutingStrategy for RoundRobinReplicaStrategy {
    fn on_topology_changed(&self, topology: Vec<SlotTopology>) {
        // Group by primary address — same primary means same shard.
        let mut shard_counters: std::collections::HashMap<NodeAddress, Arc<AtomicUsize>> =
            std::collections::HashMap::new();
        let mut new_map = BTreeMap::new();

        for entry in topology {
            let counter = shard_counters
                .entry(entry.primary)
                .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
                .clone();
            new_map.insert(entry.slot_range_end, counter);
        }

        self.shards.store(Arc::new(new_map));
    }

    fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
        let shards = self.shards.load();
        let idx = shards
            .range(candidates.slot()..)
            .next()
            .map(|(_, counter)| counter.fetch_add(1, Ordering::Relaxed))
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
    use crate::cluster_handling::read_routing::Replicas;

    fn node(host: &str, port: u16) -> NodeAddress {
        NodeAddress::from_parts(host.into(), port)
    }

    fn setup_strategy() -> RoundRobinReplicaStrategy {
        let strategy = RoundRobinReplicaStrategy::new();
        // Two shards: shard 1 owns slots 0-1000, shard 2 owns slots 1001-2000.
        strategy.on_topology_changed(vec![
            SlotTopology {
                slot_range_start: 0,
                slot_range_end: 1000,
                primary: node("primary1", 6379),
                replicas: vec![node("replica1a", 6379), node("replica1b", 6379)],
            },
            SlotTopology {
                slot_range_start: 1001,
                slot_range_end: 2000,
                primary: node("primary2", 6379),
                replicas: vec![node("replica2a", 6379), node("replica2b", 6379)],
            },
        ]);
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
