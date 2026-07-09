use std::collections::{HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use rand::Rng;

use super::interface::{ClusterTopology, ReadCandidates, ReadRoutingStrategy};
use crate::cluster_handling::NodeAddress;
use crate::cluster_handling::slot_range_map::SlotRangeMap;

struct ShardSelection {
    selected_replica: Option<NodeAddress>,
    replicas: Arc<[NodeAddress]>,
    // One-based index into `replicas`; zero means no replica is connected.
    active_replica: AtomicUsize,
}

impl ShardSelection {
    fn new(selected_replica: Option<NodeAddress>, replicas: &[NodeAddress]) -> Self {
        let active_replica = selected_replica
            .as_ref()
            .and_then(|selected| replicas.iter().position(|replica| replica == selected))
            .map_or(0, |index| index + 1);
        Self {
            selected_replica,
            replicas: replicas.into(),
            active_replica: AtomicUsize::new(active_replica),
        }
    }

    fn update_connected_nodes(&self, connected_nodes: &HashSet<NodeAddress>) {
        let active_replica = self
            .selected_replica
            .as_ref()
            .filter(|selected| connected_nodes.contains(*selected))
            .and_then(|selected| self.replicas.iter().position(|replica| replica == selected))
            .or_else(|| {
                self.replicas
                    .iter()
                    .position(|replica| connected_nodes.contains(replica))
            })
            .map_or(0, |index| index + 1);
        self.active_replica.store(active_replica, Ordering::Relaxed);
    }

    fn mark_connected(&self, connected_node: &NodeAddress) {
        let current = self.active_replica.load(Ordering::Relaxed);
        let connected_index = self
            .replicas
            .iter()
            .position(|replica| replica == connected_node)
            .map(|index| index + 1);
        let selected_index = self
            .selected_replica
            .as_ref()
            .and_then(|selected| self.replicas.iter().position(|replica| replica == selected))
            .map(|index| index + 1);

        if let Some(connected_index) = connected_index {
            if current == 0 || Some(connected_index) == selected_index {
                self.active_replica
                    .store(connected_index, Ordering::Relaxed);
            }
        }
    }

    fn active_replica(&self) -> Option<&NodeAddress> {
        self.active_replica
            .load(Ordering::Relaxed)
            .checked_sub(1)
            .and_then(|index| self.replicas.get(index))
    }
}

/// Gives each cluster connection affinity to one uniformly selected replica per shard.
///
/// A connection-specific random seed and rendezvous hashing choose the replica. This
/// keeps the choice stable across unchanged topology refreshes, avoids depending on
/// replica ordering, and only remaps the connections affected by replica membership
/// changes.
///
/// For normal keyed commands, the strategy eagerly maintains every primary plus one
/// replica per shard. Commands routed to all nodes use transient connections for omitted
/// replicas. Explicit-address routing, redirects, and failure recovery can still open a
/// retained connection on demand; a later topology refresh restores the eager set.
///
/// If the selected replica is not connected, optional reads use the primary instead of
/// opening a new connection on every request. Replica-required reads keep trying a
/// replica, and any already-connected replica is preferred. The original affinity is
/// restored when its connection becomes available again.
pub struct UniformRandom {
    seed: u64,
    slots: Arc<RwLock<SlotRangeMap<Arc<ShardSelection>>>>,
}

impl UniformRandom {
    /// Creates a new `UniformRandom` read-routing strategy factory.
    ///
    /// Each cluster connection created from this factory receives an independent
    /// random seed and therefore makes independent per-shard replica selections.
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    fn with_seed(seed: u64) -> Self {
        Self {
            seed,
            slots: Arc::new(RwLock::new(SlotRangeMap::new())),
        }
    }

    #[cfg(test)]
    fn selected_replica_for_slot(&self, slot: u16) -> Option<NodeAddress> {
        self.slots
            .read()
            .expect("Lock poisoned")
            .get(slot)
            .and_then(|selection| selection.selected_replica.clone())
    }
}

impl Default for UniformRandom {
    fn default() -> Self {
        Self {
            seed: rand::rng().random(),
            slots: Arc::new(RwLock::new(SlotRangeMap::new())),
        }
    }
}

fn replica_score(seed: u64, primary: &NodeAddress, replica: &NodeAddress) -> u64 {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    primary.hash(&mut hasher);
    replica.hash(&mut hasher);
    hasher.finish()
}

fn choose_uniform_replica(
    seed: u64,
    primary: &NodeAddress,
    replicas: &[NodeAddress],
) -> Option<NodeAddress> {
    replicas
        .iter()
        .max_by(|left, right| {
            replica_score(seed, primary, left)
                .cmp(&replica_score(seed, primary, right))
                .then_with(|| left.cmp(right))
        })
        .cloned()
}

impl ReadRoutingStrategy for UniformRandom {
    fn on_topology_changed(&self, topology: ClusterTopology) {
        let mut slots = SlotRangeMap::new();
        for shard in topology.shards() {
            let selection = Arc::new(ShardSelection::new(
                choose_uniform_replica(self.seed, shard.primary(), shard.replicas()),
                shard.replicas(),
            ));
            for &(start, end) in shard.slot_ranges() {
                slots.insert(start, end, Arc::clone(&selection));
            }
        }

        *self.slots.write().expect("Lock poisoned") = slots;
    }

    fn eager_connection_nodes(&self, topology: &ClusterTopology) -> Option<HashSet<NodeAddress>> {
        let mut nodes = HashSet::new();
        for shard in topology.shards() {
            nodes.insert(shard.primary().clone());
            nodes.extend(choose_uniform_replica(
                self.seed,
                shard.primary(),
                shard.replicas(),
            ));
        }
        Some(nodes)
    }

    fn on_connections_changed(&self, connected_nodes: &HashSet<NodeAddress>) {
        for selection in self.slots.read().expect("Lock poisoned").values() {
            selection.update_connected_nodes(connected_nodes);
        }
    }

    fn on_connection_added(&self, connected_node: &NodeAddress) {
        for selection in self.slots.read().expect("Lock poisoned").values() {
            selection.mark_connected(connected_node);
        }
    }

    fn supports_read_fallback(&self) -> bool {
        true
    }

    fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
        let slots = self.slots.read().expect("Lock poisoned");
        let selection = slots.get(candidates.slot());
        let active_replica = selection.and_then(|selection| selection.active_replica());
        let selected_replica = selection.and_then(|selection| selection.selected_replica.as_ref());

        match candidates {
            ReadCandidates::AnyNode(c) => active_replica
                .and_then(|active| c.replicas().iter().find(|replica| *replica == active))
                .unwrap_or_else(|| c.primary()),
            ReadCandidates::ReplicasOnly(c) => active_replica
                .or(selected_replica)
                .and_then(|selected| c.replicas().iter().find(|replica| *replica == selected))
                .unwrap_or_else(|| c.replicas().first()),
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

    fn topology() -> ClusterTopology {
        ClusterTopology::from_shards(vec![
            Shard::new(
                vec![(0, 1000)],
                node("primary1", 6379),
                vec![
                    node("replica1a", 6379),
                    node("replica1b", 6379),
                    node("replica1c", 6379),
                ],
            ),
            Shard::new(
                vec![(1001, 2000)],
                node("primary2", 6379),
                vec![node("replica2a", 6379), node("replica2b", 6379)],
            ),
        ])
    }

    #[test]
    fn eagerly_connects_to_all_primaries_and_one_replica_per_shard() {
        let strategy = UniformRandom::with_seed(42);
        let topology = topology();
        strategy.on_topology_changed(topology.clone());

        let eager_nodes = strategy.eager_connection_nodes(&topology).unwrap();
        assert_eq!(eager_nodes.len(), 4);
        for shard in topology.shards() {
            assert!(eager_nodes.contains(shard.primary()));
            assert_eq!(
                shard
                    .replicas()
                    .iter()
                    .filter(|replica| eager_nodes.contains(*replica))
                    .count(),
                1
            );
            let selected = strategy
                .selected_replica_for_slot(shard.slot_ranges()[0].0)
                .unwrap();
            assert!(eager_nodes.contains(&selected));
        }
    }

    #[test]
    fn routes_to_the_eagerly_selected_replica() {
        let strategy = UniformRandom::with_seed(42);
        strategy.on_topology_changed(topology());

        let selected = strategy.selected_replica_for_slot(1).unwrap();
        let primary = node("primary1", 6379);
        let replicas = [
            node("replica1a", 6379),
            node("replica1b", 6379),
            node("replica1c", 6379),
        ];
        let candidates = ReadCandidates::any_node(1, &primary, Replicas::new(&replicas).unwrap());

        assert_eq!(strategy.route_read(&candidates), &selected);
    }

    #[test]
    fn routes_around_an_unavailable_selected_replica_without_losing_affinity() {
        let strategy = UniformRandom::with_seed(42);
        strategy.on_topology_changed(topology());

        let primary = node("primary1", 6379);
        let replicas = [
            node("replica1a", 6379),
            node("replica1b", 6379),
            node("replica1c", 6379),
        ];
        let selected = strategy.selected_replica_for_slot(1).unwrap();
        let fallback = replicas
            .iter()
            .find(|replica| *replica != &selected)
            .unwrap()
            .clone();

        strategy.on_connections_changed(&HashSet::from([primary.clone()]));
        let any_node = ReadCandidates::any_node(
            1,
            &primary,
            Replicas::new(&replicas).expect("replicas are non-empty"),
        );
        let replicas_only = ReadCandidates::replicas_only(
            1,
            Replicas::new(&replicas).expect("replicas are non-empty"),
        );
        assert_eq!(strategy.route_read(&any_node), &primary);
        assert_eq!(strategy.route_read(&replicas_only), &selected);

        strategy.on_connections_changed(&HashSet::from([primary.clone(), fallback.clone()]));
        assert_eq!(strategy.route_read(&any_node), &fallback);
        assert_eq!(strategy.route_read(&replicas_only), &fallback);

        strategy.on_connections_changed(&HashSet::from([primary.clone(), selected.clone()]));
        assert_eq!(strategy.route_read(&any_node), &selected);
    }

    #[test]
    fn selection_is_stable_across_refresh_and_replica_order() {
        let strategy = UniformRandom::with_seed(42);
        strategy.on_topology_changed(topology());
        let selected = strategy.selected_replica_for_slot(1);

        strategy.on_topology_changed(ClusterTopology::from_shards(vec![Shard::new(
            vec![(0, 500), (1000, 1500)],
            node("primary1", 6379),
            vec![
                node("replica1c", 6379),
                node("replica1a", 6379),
                node("replica1b", 6379),
            ],
        )]));

        assert_eq!(strategy.selected_replica_for_slot(1), selected);
        assert_eq!(strategy.selected_replica_for_slot(1200), selected);
    }

    #[test]
    fn adding_a_replica_only_remaps_seeds_that_select_the_new_replica() {
        let primary = node("primary", 6379);
        let old_replicas = [node("replica-a", 6379), node("replica-b", 6379)];
        let new_replica = node("replica-c", 6379);
        let new_replicas = [
            old_replicas[0].clone(),
            old_replicas[1].clone(),
            new_replica.clone(),
        ];

        for seed in 0..1024 {
            let old = choose_uniform_replica(seed, &primary, &old_replicas).unwrap();
            let new = choose_uniform_replica(seed, &primary, &new_replicas).unwrap();
            if new != new_replica {
                assert_eq!(new, old);
            }
        }
    }

    #[test]
    fn selection_is_uniform_across_connection_seeds() {
        let primary = node("primary", 6379);
        let replicas = [
            node("replica-a", 6379),
            node("replica-b", 6379),
            node("replica-c", 6379),
        ];
        let mut counts = [0usize; 3];

        for seed in 0..4096 {
            let selected = choose_uniform_replica(seed, &primary, &replicas).unwrap();
            let index = replicas
                .iter()
                .position(|replica| replica == &selected)
                .unwrap();
            counts[index] += 1;
        }

        let expected = 4096 / replicas.len();
        for count in counts {
            assert!(count.abs_diff(expected) < expected / 10, "{counts:?}");
        }
    }
}
