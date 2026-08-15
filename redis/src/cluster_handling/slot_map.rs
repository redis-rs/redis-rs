use std::collections::{HashMap, HashSet};

use arcstr::ArcStr;

use super::NodeAddress;
use super::read_routing::{ClusterTopology, ReadCandidates, ReadRoutingStrategy, Replicas, Shard};
use super::slot_range_map::SlotRangeMap;
use crate::cluster_routing::{Route, SlotAddr};

pub(crate) const SLOT_SIZE: u16 = 16384;

#[derive(Debug, Default)]
pub(crate) struct SlotMap {
    slots: SlotRangeMap<SlotAddrs>,
    node_availability_zones: HashMap<NodeAddress, ArcStr>,
}

impl SlotMap {
    pub fn new() -> Self {
        Self {
            slots: Default::default(),
            node_availability_zones: Default::default(),
        }
    }

    pub fn from_slots(slots: Vec<SlotRange>) -> Self {
        let mut map = SlotRangeMap::new();
        for slot in slots {
            map.insert(slot.start, slot.end, SlotAddrs::from_slot(slot));
        }
        Self {
            slots: map,
            node_availability_zones: Default::default(),
        }
    }

    #[cfg(feature = "cluster-async")]
    pub fn fill_slots(&mut self, slots: Vec<SlotRange>) {
        for slot in slots {
            self.slots
                .insert(slot.start, slot.end, SlotAddrs::from_slot(slot));
        }
    }

    /// Point a single slot at `primary`, splitting whatever range covers it.
    ///
    /// A `MOVED <slot> <addr>` reply carries exactly the information a full
    /// `CLUSTER SLOTS` would have supplied *for that one slot*, so this lets the
    /// caller repair its map from the redirect it already received instead of
    /// refetching all 16384 mappings.
    ///
    /// Splitting rather than overwriting is load-bearing. [`SlotRangeMap`] is
    /// keyed on range *end*, so inserting `[slot, slot]` into the middle of
    /// `[a, b]` without re-inserting `[a, slot - 1]` would leave every slot
    /// below `slot` resolving to `None` — silently unrouted, not merely stale.
    ///
    /// Only ever call this for MOVED. An ASK redirect is a per-key, per-request
    /// instruction for a slot that is still mid-migration and still owned by the
    /// source node; recording it here would send that slot's not-yet-migrated
    /// keys to a node that does not serve them.
    pub(crate) fn update_slot(&mut self, slot: u16, primary: NodeAddress) {
        // Replicas are a property of the shard, not of the individual slot: every
        // range a given primary owns reports the same replica set. Carrying that
        // set over keeps replica reads working for this slot instead of pinning
        // them to the primary until the next full refresh. The scan is O(ranges)
        // and only runs on a redirect; a denormalised primary→replicas cache
        // would be faster but would have to be kept consistent in every place
        // that builds a map, which is more room for error than it is worth.
        let replicas = self
            .slots
            .values()
            .find(|addrs| addrs.primary == primary)
            .map(|addrs| addrs.replicas.clone())
            .unwrap_or_default();

        let Some((start, end)) = self.slots.range_containing(slot) else {
            self.slots
                .insert(slot, slot, SlotAddrs::new(primary, replicas));
            return;
        };
        let Some((_, old)) = self.slots.remove_range(end) else {
            return;
        };
        if start < slot {
            self.slots.insert(
                start,
                slot - 1,
                SlotAddrs::new(old.primary.clone(), old.replicas.clone()),
            );
        }
        if slot < end {
            self.slots
                .insert(slot + 1, end, SlotAddrs::new(old.primary, old.replicas));
        }
        self.slots
            .insert(slot, slot, SlotAddrs::new(primary, replicas));
    }

    pub fn slot_addr_for_route(
        &self,
        route: &Route,
        strategy: Option<&dyn ReadRoutingStrategy>,
    ) -> Option<&NodeAddress> {
        let slot = route.slot();
        self.slots
            .get(slot)
            .map(|addrs| addrs.slot_addr(slot, &route.slot_addr(), strategy))
    }

    pub(crate) fn slot_addr_for_route_excluding(
        &self,
        route: &Route,
        strategy: Option<&dyn ReadRoutingStrategy>,
        excluded_nodes: &[NodeAddress],
    ) -> Option<&NodeAddress> {
        let slot = route.slot();
        self.slots.get(slot).and_then(|addrs| {
            addrs.slot_addr_excluding(slot, &route.slot_addr(), strategy, excluded_nodes)
        })
    }

    #[cfg(feature = "cluster-async")]
    pub fn clear(&mut self) {
        self.slots.clear();
    }

    pub fn values(&self) -> impl Iterator<Item = &SlotAddrs> {
        self.slots.values()
    }

    pub(crate) fn set_node_availability_zones(
        &mut self,
        node_availability_zones: HashMap<NodeAddress, ArcStr>,
    ) {
        self.node_availability_zones = node_availability_zones;
    }

    fn all_unique_addresses(&self, only_primaries: bool) -> HashSet<&NodeAddress> {
        let mut addresses: HashSet<_> = HashSet::new();
        if only_primaries {
            addresses.extend(self.values().map(|slot_addrs| &slot_addrs.primary));
        } else {
            addresses.extend(self.values().flat_map(|slot_addrs| slot_addrs.into_iter()));
        }

        addresses
    }

    pub fn addresses_for_all_primaries(&self) -> HashSet<&NodeAddress> {
        self.all_unique_addresses(true)
    }

    pub fn addresses_for_all_nodes(&self) -> HashSet<&NodeAddress> {
        self.all_unique_addresses(false)
    }

    #[cfg(any(feature = "cluster-async", test))]
    pub fn addresses_for_multi_slot<'a, 'b>(
        &'a self,
        routes: &'b [(Route, Vec<usize>)],
        strategy: Option<&'a dyn ReadRoutingStrategy>,
    ) -> impl Iterator<Item = Option<&'a NodeAddress>> + 'a
    where
        'b: 'a,
    {
        routes
            .iter()
            .map(move |(route, _)| self.slot_addr_for_route(route, strategy))
    }

    /// Produces a [`ClusterTopology`] snapshot by grouping slot ranges by
    /// primary node into shards.
    pub fn topology(&self) -> ClusterTopology {
        struct ShardBuilder {
            primary: NodeAddress,
            slot_ranges: Vec<(u16, u16)>,
            replicas: Vec<NodeAddress>,
        }

        let mut builders: Vec<ShardBuilder> = Vec::new();
        for (start, end, addrs) in self.slots.iter() {
            if let Some(b) = builders.iter_mut().find(|b| b.primary == addrs.primary) {
                b.slot_ranges.push((start, end));
            } else {
                builders.push(ShardBuilder {
                    primary: addrs.primary.clone(),
                    slot_ranges: vec![(start, end)],
                    replicas: addrs.replicas.clone(),
                });
            }
        }

        ClusterTopology::from_shards_with_node_availability_zones(
            builders
                .into_iter()
                .map(|b| Shard::new(b.slot_ranges, b.primary, b.replicas))
                .collect(),
            self.node_availability_zones.clone(),
        )
    }
}

/// This is just a simplified version of [`SlotRange`],
/// which stores only the master and optional replica
/// to avoid the need to choose a replica each time
/// a command is executed
#[derive(Debug)]
pub(crate) struct SlotAddrs {
    primary: NodeAddress,
    replicas: Vec<NodeAddress>,
}

impl SlotAddrs {
    pub(crate) fn new(primary: NodeAddress, replicas: Vec<NodeAddress>) -> Self {
        Self { primary, replicas }
    }

    pub(crate) fn slot_addr_excluding(
        &self,
        slot: u16,
        slot_addr: &SlotAddr,
        strategy: Option<&dyn ReadRoutingStrategy>,
        excluded_nodes: &[NodeAddress],
    ) -> Option<&NodeAddress> {
        if excluded_nodes.is_empty() {
            return Some(self.slot_addr(slot, slot_addr, strategy));
        }

        match slot_addr {
            SlotAddr::Master => (!excluded_nodes.contains(&self.primary)).then_some(&self.primary),
            SlotAddr::ReplicaOptional => {
                let selected = match (strategy, Replicas::new(&self.replicas)) {
                    (Some(strategy), Some(replicas)) => strategy
                        .route_read(&ReadCandidates::any_node(slot, &self.primary, replicas)),
                    _ => &self.primary,
                };
                if !excluded_nodes.contains(selected) {
                    return Some(selected);
                }

                (!excluded_nodes.contains(&self.primary))
                    .then_some(&self.primary)
                    .or_else(|| {
                        strategy.and_then(|_| {
                            self.replicas
                                .iter()
                                .find(|replica| !excluded_nodes.contains(*replica))
                        })
                    })
            }
            SlotAddr::ReplicaRequired => {
                let Some(replicas) = Replicas::new(&self.replicas) else {
                    return (!excluded_nodes.contains(&self.primary)).then_some(&self.primary);
                };
                let selected = match strategy {
                    Some(strategy) => strategy.route_read(&ReadCandidates::replicas_only(
                        slot,
                        Replicas::new(&self.replicas).expect("replicas are non-empty"),
                    )),
                    None => replicas.choose_random(),
                };
                if !excluded_nodes.contains(selected) {
                    return Some(selected);
                }

                self.replicas
                    .iter()
                    .find(|replica| !excluded_nodes.contains(*replica))
            }
        }
    }

    pub(crate) fn slot_addr(
        &self,
        slot: u16,
        slot_addr: &SlotAddr,
        strategy: Option<&dyn ReadRoutingStrategy>,
    ) -> &NodeAddress {
        let Some(strategy) = strategy else {
            return match slot_addr {
                SlotAddr::Master | SlotAddr::ReplicaOptional => &self.primary,
                SlotAddr::ReplicaRequired => match Replicas::new(&self.replicas) {
                    Some(replicas) => replicas.choose_random(),
                    None => &self.primary,
                },
            };
        };
        match Replicas::new(&self.replicas) {
            Some(replicas) => match slot_addr {
                SlotAddr::Master => &self.primary,
                SlotAddr::ReplicaOptional => {
                    strategy.route_read(&ReadCandidates::any_node(slot, &self.primary, replicas))
                }
                SlotAddr::ReplicaRequired => {
                    strategy.route_read(&ReadCandidates::replicas_only(slot, replicas))
                }
            },
            None => &self.primary,
        }
    }

    pub(crate) fn from_slot(slot: SlotRange) -> Self {
        SlotAddrs::new(slot.master, slot.replicas)
    }
}

impl<'a> IntoIterator for &'a SlotAddrs {
    type Item = &'a NodeAddress;
    type IntoIter =
        std::iter::Chain<std::iter::Once<&'a NodeAddress>, std::slice::Iter<'a, NodeAddress>>;

    fn into_iter(
        self,
    ) -> std::iter::Chain<std::iter::Once<&'a NodeAddress>, std::slice::Iter<'a, NodeAddress>> {
        std::iter::once(&self.primary).chain(self.replicas.iter())
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct SlotRange {
    pub(crate) start: u16,
    pub(crate) end: u16,
    pub(crate) master: NodeAddress,
    pub(crate) replicas: Vec<NodeAddress>,
}

impl SlotRange {
    pub fn new(s: u16, e: u16, m: NodeAddress, r: Vec<NodeAddress>) -> Self {
        Self {
            start: s,
            end: e,
            master: m,
            replicas: r,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_handling::read_routing::ReadRoutingStrategy;
    use crate::cluster_routing::Slot;

    fn addr(s: &str) -> NodeAddress {
        NodeAddress::try_from(s).unwrap()
    }

    fn primary_for(map: &SlotMap, slot: u16) -> Option<String> {
        map.slot_addr_for_route(&Route::new(slot, SlotAddr::Master), None)
            .map(|a| a.to_string())
    }

    fn replica_for(map: &SlotMap, slot: u16) -> Option<String> {
        map.slot_addr_for_route(&Route::new(slot, SlotAddr::ReplicaRequired), None)
            .map(|a| a.to_string())
    }

    /// n1 owns 0-999, n2 owns 1000-1999, n3 owns 2000-2999, each with one replica.
    fn three_shards() -> SlotMap {
        SlotMap::from_slots(vec![
            SlotRange::new(0, 999, addr("n1:6379"), vec![addr("r1:6379")]),
            SlotRange::new(1000, 1999, addr("n2:6379"), vec![addr("r2:6379")]),
            SlotRange::new(2000, 2999, addr("n3:6379"), vec![addr("r3:6379")]),
        ])
    }

    #[test]
    fn update_slot_splits_a_range_without_orphaning_its_neighbours() {
        let mut map = three_shards();
        map.update_slot(1500, addr("n3:6379"));

        assert_eq!(primary_for(&map, 1500).as_deref(), Some("n3:6379"));
        // Both halves of the split range must still resolve to the old owner. This
        // is the regression the range-end keying invites.
        for slot in [1000, 1234, 1499, 1501, 1800, 1999] {
            assert_eq!(
                primary_for(&map, slot).as_deref(),
                Some("n2:6379"),
                "slot {slot} lost its mapping"
            );
        }
        assert_eq!(primary_for(&map, 999).as_deref(), Some("n1:6379"));
        assert_eq!(primary_for(&map, 2000).as_deref(), Some("n3:6379"));
    }

    #[test]
    fn update_slot_handles_both_range_edges() {
        let mut map = three_shards();
        map.update_slot(1000, addr("n1:6379"));
        assert_eq!(primary_for(&map, 1000).as_deref(), Some("n1:6379"));
        assert_eq!(primary_for(&map, 1001).as_deref(), Some("n2:6379"));
        assert_eq!(primary_for(&map, 999).as_deref(), Some("n1:6379"));

        let mut map = three_shards();
        map.update_slot(1999, addr("n1:6379"));
        assert_eq!(primary_for(&map, 1999).as_deref(), Some("n1:6379"));
        assert_eq!(primary_for(&map, 1998).as_deref(), Some("n2:6379"));
        assert_eq!(primary_for(&map, 2000).as_deref(), Some("n3:6379"));
    }

    #[test]
    fn update_slot_inherits_the_new_primarys_replicas() {
        let mut map = three_shards();
        map.update_slot(1500, addr("n3:6379"));
        // n3's replica is known from the range it already owns, so replica reads
        // for the moved slot keep working instead of falling back to the primary.
        assert_eq!(replica_for(&map, 1500).as_deref(), Some("r3:6379"));
        assert_eq!(replica_for(&map, 1501).as_deref(), Some("r2:6379"));
    }

    #[test]
    fn update_slot_to_an_unknown_node_falls_back_to_the_primary() {
        let mut map = three_shards();
        map.update_slot(1500, addr("new:6379"));
        assert_eq!(primary_for(&map, 1500).as_deref(), Some("new:6379"));
        // No replica set is knowable for a node that owns nothing else yet;
        // reads must degrade to the primary rather than to a stale replica.
        assert_eq!(replica_for(&map, 1500).as_deref(), Some("new:6379"));
    }

    #[test]
    fn repeated_updates_keep_every_slot_correctly_mapped() {
        let mut map = three_shards();
        let mut expected: Vec<&str> = (0..3000u16)
            .map(|slot| {
                if slot < 1000 {
                    "n1:6379"
                } else if slot < 2000 {
                    "n2:6379"
                } else {
                    "n3:6379"
                }
            })
            .collect();

        // Walk a pseudo-random sequence of slots to new owners, then assert the
        // whole keyspace. A split that drops a neighbour would show up here as a
        // silently misrouted slot rather than as an error.
        let mut state: u64 = 7;
        for i in 0..1000 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let slot = ((state >> 33) % 3000) as u16;
            let owner = if i % 2 == 0 { "n1:6379" } else { "nX:6379" };
            map.update_slot(slot, addr(owner));
            expected[slot as usize] = owner;
        }
        for (slot, want) in expected.iter().enumerate() {
            assert_eq!(
                primary_for(&map, slot as u16).as_deref(),
                Some(*want),
                "slot {slot} misrouted"
            );
        }
    }

    #[test]
    fn update_slot_on_an_empty_map_inserts_a_single_slot() {
        let mut map = SlotMap::new();
        map.update_slot(42, addr("n1:6379"));
        assert_eq!(primary_for(&map, 42).as_deref(), Some("n1:6379"));
        assert_eq!(primary_for(&map, 43), None);
    }

    #[test]
    fn a_full_refresh_discards_incremental_updates() {
        let mut map = three_shards();
        map.update_slot(1500, addr("n3:6379"));
        // A later CLUSTER SLOTS is authoritative and must win, including undoing
        // an update that has since become wrong.
        map = SlotMap::from_slots(vec![
            SlotRange::new(0, 999, addr("n1:6379"), vec![addr("r1:6379")]),
            SlotRange::new(1000, 1999, addr("n2:6379"), vec![addr("r2:6379")]),
            SlotRange::new(2000, 2999, addr("n3:6379"), vec![addr("r3:6379")]),
        ]);
        assert_eq!(primary_for(&map, 1500).as_deref(), Some("n2:6379"));
    }

    /// Always picks the first replica.
    #[derive(Default)]
    struct FirstReplicaStrategy;

    impl ReadRoutingStrategy for FirstReplicaStrategy {
        fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
            match candidates {
                ReadCandidates::AnyNode(c) => c.replicas().first(),
                ReadCandidates::ReplicasOnly(c) => c.replicas().first(),
            }
        }
    }

    #[test]
    fn test_slot_map_with_strategy() {
        let strategy = FirstReplicaStrategy;
        let slot_map = SlotMap::from_slots(vec![
            SlotRange {
                start: 1,
                end: 1000,
                master: addr("node1:6379"),
                replicas: vec![addr("replica1:6379")],
            },
            SlotRange {
                start: 1001,
                end: 2000,
                master: addr("node2:6379"),
                replicas: vec![addr("replica2:6379")],
            },
        ]);

        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(1).unwrap(), SlotAddr::Master),
                    Some(&strategy)
                )
                .unwrap(),
            "node1:6379"
        );
        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(500).unwrap(), SlotAddr::Master),
                    Some(&strategy)
                )
                .unwrap(),
            "node1:6379"
        );
        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(1000).unwrap(), SlotAddr::Master),
                    Some(&strategy)
                )
                .unwrap(),
            "node1:6379"
        );
        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(1000).unwrap(), SlotAddr::ReplicaOptional),
                    Some(&strategy)
                )
                .unwrap(),
            "replica1:6379"
        );
        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(1001).unwrap(), SlotAddr::Master),
                    Some(&strategy)
                )
                .unwrap(),
            "node2:6379"
        );
        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(1500).unwrap(), SlotAddr::Master),
                    Some(&strategy)
                )
                .unwrap(),
            "node2:6379"
        );
        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(2000).unwrap(), SlotAddr::Master),
                    Some(&strategy)
                )
                .unwrap(),
            "node2:6379"
        );
        assert!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(2001).unwrap(), SlotAddr::Master),
                    Some(&strategy)
                )
                .is_none()
        );
    }

    #[test]
    fn test_slot_map_when_no_strategy_is_set() {
        let slot_map = SlotMap::from_slots(vec![SlotRange {
            start: 1,
            end: 1000,
            master: addr("node1:6379"),
            replicas: vec![addr("replica1:6379")],
        }]);

        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(1000).unwrap(), SlotAddr::ReplicaOptional),
                    None
                )
                .unwrap(),
            "node1:6379"
        );
        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(1000).unwrap(), SlotAddr::ReplicaRequired),
                    None
                )
                .unwrap(),
            "replica1:6379"
        );
    }

    #[test]
    fn test_slot_map_read_fallback_respects_route_semantics() {
        let strategy = FirstReplicaStrategy;
        let slot_map = SlotMap::from_slots(vec![SlotRange::new(
            1,
            1000,
            addr("primary:6379"),
            vec![addr("replica1:6379"), addr("replica2:6379")],
        )]);
        let optional = Route::with_slot(Slot::new(500).unwrap(), SlotAddr::ReplicaOptional);
        let required = Route::with_slot(Slot::new(500).unwrap(), SlotAddr::ReplicaRequired);

        assert_eq!(
            slot_map.slot_addr_for_route_excluding(
                &optional,
                Some(&strategy),
                &[addr("replica1:6379")],
            ),
            Some(&addr("primary:6379"))
        );
        assert_eq!(
            slot_map.slot_addr_for_route_excluding(
                &optional,
                Some(&strategy),
                &[addr("replica1:6379"), addr("primary:6379")],
            ),
            Some(&addr("replica2:6379"))
        );
        assert_eq!(
            slot_map.slot_addr_for_route_excluding(
                &required,
                Some(&strategy),
                &[addr("replica1:6379")],
            ),
            Some(&addr("replica2:6379"))
        );
        assert_eq!(
            slot_map.slot_addr_for_route_excluding(
                &required,
                Some(&strategy),
                &[addr("replica1:6379"), addr("replica2:6379")],
            ),
            None
        );
    }

    #[test]
    fn test_slot_map_without_strategy_does_not_fall_back_to_unprepared_replica() {
        let slot_map = SlotMap::from_slots(vec![SlotRange::new(
            1,
            1000,
            addr("primary:6379"),
            vec![addr("replica:6379")],
        )]);
        let optional = Route::with_slot(Slot::new(500).unwrap(), SlotAddr::ReplicaOptional);

        assert_eq!(
            slot_map.slot_addr_for_route_excluding(&optional, None, &[addr("primary:6379")],),
            None
        );
    }

    fn get_slot_map() -> SlotMap {
        SlotMap::from_slots(vec![
            SlotRange::new(1, 1000, addr("node1:6379"), vec![addr("replica1:6379")]),
            SlotRange::new(
                1002,
                2000,
                addr("node2:6379"),
                vec![addr("replica2:6379"), addr("replica3:6379")],
            ),
            SlotRange::new(
                2001,
                3000,
                addr("node3:6379"),
                vec![
                    addr("replica4:6379"),
                    addr("replica5:6379"),
                    addr("replica6:6379"),
                ],
            ),
            SlotRange::new(
                3001,
                4000,
                addr("node2:6379"),
                vec![addr("replica2:6379"), addr("replica3:6379")],
            ),
        ])
    }

    #[test]
    fn test_slot_map_get_all_primaries() {
        let slot_map = get_slot_map();
        let addresses = slot_map.addresses_for_all_primaries();
        assert_eq!(
            addresses,
            HashSet::from_iter([
                &addr("node1:6379"),
                &addr("node2:6379"),
                &addr("node3:6379")
            ])
        );
    }

    #[test]
    fn test_slot_map_get_all_nodes() {
        let slot_map = get_slot_map();
        let addresses = slot_map.addresses_for_all_nodes();
        assert_eq!(
            addresses,
            HashSet::from_iter([
                &addr("node1:6379"),
                &addr("node2:6379"),
                &addr("node3:6379"),
                &addr("replica1:6379"),
                &addr("replica2:6379"),
                &addr("replica3:6379"),
                &addr("replica4:6379"),
                &addr("replica5:6379"),
                &addr("replica6:6379")
            ])
        );
    }

    #[test]
    fn test_slot_map_get_multi_node() {
        let strategy = FirstReplicaStrategy;
        let slot_map = get_slot_map();
        let routes = vec![
            (
                Route::with_slot(Slot::new(1).unwrap(), SlotAddr::Master),
                vec![],
            ),
            (
                Route::with_slot(Slot::new(2001).unwrap(), SlotAddr::ReplicaOptional),
                vec![],
            ),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes, Some(&strategy))
            .collect::<Vec<_>>();
        assert_eq!(
            addresses,
            vec![Some(&addr("node1:6379")), Some(&addr("replica4:6379"))]
        );
    }

    #[test]
    fn test_slot_map_should_ignore_replicas_in_multi_slot_if_no_strategy_is_set() {
        let slot_map = get_slot_map();
        let routes = vec![
            (
                Route::with_slot(Slot::new(1).unwrap(), SlotAddr::Master),
                vec![],
            ),
            (
                Route::with_slot(Slot::new(2001).unwrap(), SlotAddr::ReplicaOptional),
                vec![],
            ),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes, None)
            .collect::<Vec<_>>();
        assert_eq!(
            addresses,
            vec![Some(&addr("node1:6379")), Some(&addr("node3:6379"))]
        );
    }

    /// This test is needed in order to verify that if the MultiSlot route finds the same node for more than a single route,
    /// that node's address will appear multiple times, in the same order.
    #[test]
    fn test_slot_map_get_repeating_addresses_when_the_same_node_is_found_in_multi_slot() {
        let strategy = FirstReplicaStrategy;
        let slot_map = get_slot_map();
        let routes = vec![
            (
                Route::with_slot(Slot::new(1).unwrap(), SlotAddr::ReplicaOptional),
                vec![],
            ),
            (
                Route::with_slot(Slot::new(2001).unwrap(), SlotAddr::Master),
                vec![],
            ),
            (
                Route::with_slot(Slot::new(2).unwrap(), SlotAddr::ReplicaOptional),
                vec![],
            ),
            (
                Route::with_slot(Slot::new(2002).unwrap(), SlotAddr::Master),
                vec![],
            ),
            (
                Route::with_slot(Slot::new(3).unwrap(), SlotAddr::ReplicaOptional),
                vec![],
            ),
            (
                Route::with_slot(Slot::new(2003).unwrap(), SlotAddr::Master),
                vec![],
            ),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes, Some(&strategy))
            .collect::<Vec<_>>();
        assert_eq!(
            addresses,
            vec![
                Some(&addr("replica1:6379")),
                Some(&addr("node3:6379")),
                Some(&addr("replica1:6379")),
                Some(&addr("node3:6379")),
                Some(&addr("replica1:6379")),
                Some(&addr("node3:6379"))
            ]
        );
    }

    #[test]
    fn test_slot_map_get_none_when_slot_is_missing_from_multi_slot() {
        let strategy = FirstReplicaStrategy;
        let slot_map = get_slot_map();
        let routes = vec![
            (
                Route::with_slot(Slot::new(1).unwrap(), SlotAddr::ReplicaOptional),
                vec![],
            ),
            (
                Route::with_slot(Slot::new(5000).unwrap(), SlotAddr::Master),
                vec![],
            ),
            (
                Route::with_slot(Slot::new(6000).unwrap(), SlotAddr::ReplicaOptional),
                vec![],
            ),
            (
                Route::with_slot(Slot::new(2002).unwrap(), SlotAddr::Master),
                vec![],
            ),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes, Some(&strategy))
            .collect::<Vec<_>>();
        assert_eq!(
            addresses,
            vec![
                Some(&addr("replica1:6379")),
                None,
                None,
                Some(&addr("node3:6379"))
            ]
        );
    }

    #[test]
    fn test_slot_map_topology() {
        let slot_map = SlotMap::from_slots(vec![
            SlotRange::new(0, 5000, addr("node1:6379"), vec![addr("replica1:6379")]),
            SlotRange::new(5001, 10000, addr("node2:6379"), vec![]),
        ]);
        let topo = slot_map.topology();
        assert_eq!(topo.shards().count(), 2);

        let node1 = topo
            .shards()
            .find(|s| s.primary() == &addr("node1:6379"))
            .unwrap();
        assert_eq!(node1.slot_ranges(), &[(0, 5000)]);
        assert_eq!(node1.replicas(), &[addr("replica1:6379")]);

        let node2 = topo
            .shards()
            .find(|s| s.primary() == &addr("node2:6379"))
            .unwrap();
        assert_eq!(node2.slot_ranges(), &[(5001, 10000)]);
        assert!(node2.replicas().is_empty());
    }

    #[test]
    fn test_slot_map_topology_groups_by_primary() {
        let slot_map = get_slot_map();
        let topo = slot_map.topology();
        // node2 has two ranges (1002-2000 and 3001-4000), should be one shard
        assert_eq!(topo.shards().count(), 3);
        let node2_shard = topo
            .shards()
            .find(|s| s.primary() == &addr("node2:6379"))
            .unwrap();
        assert_eq!(node2_shard.slot_ranges(), &[(1002, 2000), (3001, 4000)]);
    }

    #[test]
    fn test_slot_map_topology_shard_lookup() {
        let slot_map = get_slot_map();
        let topo = slot_map.topology();

        let shard = topo.shard_for_slot(500).unwrap();
        assert_eq!(shard.primary(), &addr("node1:6379"));

        let shard = topo.shard_for_slot(1500).unwrap();
        assert_eq!(shard.primary(), &addr("node2:6379"));

        // Slot 3500 is in the second range of node2's shard
        let shard = topo.shard_for_slot(3500).unwrap();
        assert_eq!(shard.primary(), &addr("node2:6379"));

        assert!(topo.shard_for_slot(5000).is_none());
    }

    #[test]
    fn test_slot_map_topology_carries_node_availability_zones() {
        let mut slot_map = SlotMap::from_slots(vec![SlotRange::new(
            0,
            100,
            addr("primary:6379"),
            vec![addr("replica:6379")],
        )]);
        slot_map.set_node_availability_zones(
            [(addr("replica:6379"), ArcStr::from("us-east-1b"))]
                .into_iter()
                .collect(),
        );

        let topo = slot_map.topology();

        assert_eq!(
            topo.availability_zone_for_node(&addr("replica:6379")),
            Some("us-east-1b")
        );
    }

    #[test]
    fn test_custom_strategy() {
        /// Always picks the first replica.
        #[derive(Default)]
        struct AlwaysFirstReplica;

        impl ReadRoutingStrategy for AlwaysFirstReplica {
            fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
                match candidates {
                    ReadCandidates::AnyNode(c) => c.replicas().first(),
                    ReadCandidates::ReplicasOnly(c) => c.replicas().first(),
                }
            }
        }

        let strategy = AlwaysFirstReplica;
        let slot_map = SlotMap::from_slots(vec![SlotRange::new(
            1,
            1000,
            addr("node1:6379"),
            vec![addr("replica1:6379"), addr("replica2:6379")],
        )]);

        // ReplicaOptional with AlwaysFirstReplica should always return replica1
        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(500).unwrap(), SlotAddr::ReplicaOptional),
                    Some(&strategy)
                )
                .unwrap(),
            "replica1:6379"
        );

        // ReplicaRequired with AlwaysFirstReplica should also return replica1
        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(500).unwrap(), SlotAddr::ReplicaRequired),
                    Some(&strategy)
                )
                .unwrap(),
            "replica1:6379"
        );

        // Master always returns primary regardless of strategy
        assert_eq!(
            slot_map
                .slot_addr_for_route(
                    &Route::with_slot(Slot::new(500).unwrap(), SlotAddr::Master),
                    Some(&strategy)
                )
                .unwrap(),
            "node1:6379"
        );
    }
}
