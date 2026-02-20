use std::collections::{BTreeMap, HashSet};

use arcstr::ArcStr;

use crate::cluster_routing::{
    AnyNodeRoutingInfo, NodeAddress, ReadRoutingStrategy, ReplicaRoutingInfo, Replicas, Route,
    SlotAddr, SlotTopology,
};

pub(crate) const SLOT_SIZE: u16 = 16384;

#[derive(Debug)]
struct SlotMapValue {
    start: u16,
    addrs: SlotAddrs,
}

impl SlotMapValue {
    fn from_slot(slot: Slot) -> Self {
        Self {
            start: slot.start,
            addrs: SlotAddrs::from_slot(slot),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct SlotMap {
    slots: BTreeMap<u16, SlotMapValue>,
}

impl SlotMap {
    pub fn new() -> Self {
        Self {
            slots: Default::default(),
        }
    }

    pub fn from_slots(slots: Vec<Slot>) -> Self {
        Self {
            slots: slots
                .into_iter()
                .map(|slot| (slot.end, SlotMapValue::from_slot(slot)))
                .collect(),
        }
    }

    #[cfg(feature = "cluster-async")]
    pub fn fill_slots(&mut self, slots: Vec<Slot>) {
        for slot in slots {
            self.slots.insert(slot.end, SlotMapValue::from_slot(slot));
        }
    }

    pub fn slot_addr_for_route(
        &self,
        route: &Route,
        strategy: Option<&dyn ReadRoutingStrategy>,
    ) -> Option<&ArcStr> {
        let slot = route.slot();
        self.slots
            .range(slot..)
            .next()
            .and_then(|(end, slot_value)| {
                if slot <= *end && slot_value.start <= slot {
                    Some(slot_value.addrs.slot_addr(
                        slot,
                        slot_value.start,
                        *end,
                        &route.slot_addr(),
                        strategy,
                    ))
                } else {
                    None
                }
            })
    }

    #[cfg(feature = "cluster-async")]
    pub fn clear(&mut self) {
        self.slots.clear();
    }

    pub fn values(&self) -> impl Iterator<Item = &SlotAddrs> {
        self.slots.values().map(|slot_value| &slot_value.addrs)
    }

    fn all_unique_addresses(&self, only_primaries: bool) -> HashSet<&ArcStr> {
        let mut addresses: HashSet<_> = HashSet::new();
        if only_primaries {
            addresses.extend(self.values().map(|slot_addrs| &slot_addrs.primary.addr));
        } else {
            addresses.extend(self.values().flat_map(|slot_addrs| slot_addrs.into_iter()));
        }

        addresses
    }

    pub fn addresses_for_all_primaries(&self) -> HashSet<&ArcStr> {
        self.all_unique_addresses(true)
    }

    pub fn addresses_for_all_nodes(&self) -> HashSet<&ArcStr> {
        self.all_unique_addresses(false)
    }

    pub(crate) fn topology(&self) -> Vec<SlotTopology> {
        self.slots
            .iter()
            .map(|(&end, value)| SlotTopology {
                slot_range_start: value.start,
                slot_range_end: end,
                primary: value.addrs.primary.clone(),
                replicas: value.addrs.replicas.clone(),
            })
            .collect()
    }

    pub fn addresses_for_multi_slot<'a, 'b>(
        &'a self,
        routes: &'b [(Route, Vec<usize>)],
        strategy: Option<&'a dyn ReadRoutingStrategy>,
    ) -> impl Iterator<Item = Option<&'a ArcStr>> + 'a
    where
        'b: 'a,
    {
        routes
            .iter()
            .map(move |(route, _)| self.slot_addr_for_route(route, strategy))
    }
}

/// This is just a simplified version of [`Slot`],
/// which stores only the master and optional replica
/// to avoid the need to choose a replica each time
/// a command is executed
#[derive(Debug)]
pub(crate) struct SlotAddrs {
    primary: NodeAddress,
    replicas: Vec<NodeAddress>,
}

impl SlotAddrs {
    pub(crate) fn new(primary: ArcStr, replicas: Vec<ArcStr>) -> Self {
        Self {
            primary: NodeAddress { addr: primary },
            replicas: replicas
                .into_iter()
                .map(|addr| NodeAddress { addr })
                .collect(),
        }
    }

    pub(crate) fn slot_addr(
        &self,
        slot: u16,
        slot_range_start: u16,
        slot_range_end: u16,
        slot_addr: &SlotAddr,
        strategy: Option<&dyn ReadRoutingStrategy>,
    ) -> &ArcStr {
        match slot_addr {
            SlotAddr::Master => &self.primary.addr,
            SlotAddr::ReplicaOptional => match strategy {
                Some(s) => {
                    let info = AnyNodeRoutingInfo {
                        slot,
                        slot_range_start,
                        slot_range_end,
                        primary: &self.primary,
                        replicas: Replicas::new(&self.replicas),
                    };
                    &s.choose_any(&info).addr
                }
                None => &self.primary.addr,
            },
            SlotAddr::ReplicaRequired => match (strategy, Replicas::new(&self.replicas)) {
                (Some(s), Some(replicas)) => {
                    let info = ReplicaRoutingInfo {
                        slot,
                        slot_range_start,
                        slot_range_end,
                        replicas,
                    };
                    &s.choose_replica(&info).addr
                }
                (None, Some(replicas)) => &replicas.choose_random().addr,
                (_, None) => &self.primary.addr,
            },
        }
    }

    pub(crate) fn from_slot(slot: Slot) -> Self {
        SlotAddrs::new(slot.master, slot.replicas)
    }
}

impl<'a> IntoIterator for &'a SlotAddrs {
    type Item = &'a ArcStr;
    type IntoIter = std::iter::Chain<
        std::iter::Once<&'a ArcStr>,
        std::iter::Map<std::slice::Iter<'a, NodeAddress>, fn(&NodeAddress) -> &ArcStr>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        fn addr(node: &NodeAddress) -> &ArcStr {
            &node.addr
        }
        std::iter::once(&self.primary.addr).chain(
            self.replicas
                .iter()
                .map(addr as fn(&NodeAddress) -> &ArcStr),
        )
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Slot {
    pub(crate) start: u16,
    pub(crate) end: u16,
    pub(crate) master: ArcStr,
    pub(crate) replicas: Vec<ArcStr>,
}

impl Slot {
    pub fn new(s: u16, e: u16, m: ArcStr, r: Vec<ArcStr>) -> Self {
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
    use std::collections::HashSet;
    use std::sync::Arc;

    use super::*;
    use crate::cluster_routing::RandomReplica;

    fn random_strategy() -> Option<Arc<RandomReplica>> {
        Some(Arc::new(RandomReplica))
    }

    #[test]
    fn test_slot_map() {
        let strategy = random_strategy();
        let strategy_ref = strategy.as_deref().map(|s| s as &dyn ReadRoutingStrategy);
        let slot_map = SlotMap::from_slots(vec![
            Slot {
                start: 1,
                end: 1000,
                master: "node1:6379".into(),
                replicas: vec!["replica1:6379".into()],
            },
            Slot {
                start: 1001,
                end: 2000,
                master: "node2:6379".into(),
                replicas: vec!["replica2:6379".into()],
            },
        ]);

        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1, SlotAddr::Master), strategy_ref)
                .unwrap()
        );
        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(500, SlotAddr::Master), strategy_ref)
                .unwrap()
        );
        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::Master), strategy_ref)
                .unwrap()
        );
        assert_eq!(
            "replica1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::ReplicaOptional), strategy_ref)
                .unwrap()
        );
        assert_eq!(
            "node2:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1001, SlotAddr::Master), strategy_ref)
                .unwrap()
        );
        assert_eq!(
            "node2:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1500, SlotAddr::Master), strategy_ref)
                .unwrap()
        );
        assert_eq!(
            "node2:6379",
            slot_map
                .slot_addr_for_route(&Route::new(2000, SlotAddr::Master), strategy_ref)
                .unwrap()
        );
        assert!(
            slot_map
                .slot_addr_for_route(&Route::new(2001, SlotAddr::Master), strategy_ref)
                .is_none()
        );
    }

    #[test]
    fn test_slot_map_when_no_strategy_is_set() {
        let slot_map = SlotMap::from_slots(vec![Slot {
            start: 1,
            end: 1000,
            master: "node1:6379".into(),
            replicas: vec!["replica1:6379".into()],
        }]);

        // ReplicaOptional without strategy routes to primary
        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::ReplicaOptional), None)
                .unwrap()
        );
        // ReplicaRequired without strategy still routes to replica (random fallback)
        assert_eq!(
            "replica1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::ReplicaRequired), None)
                .unwrap()
        );
    }

    #[test]
    fn test_slot_map_with_custom_strategy() {
        // Strategy that always picks the primary when allowed, first replica otherwise
        struct AlwaysPrimary;
        impl ReadRoutingStrategy for AlwaysPrimary {
            fn choose_any<'a>(&self, info: &AnyNodeRoutingInfo<'a>) -> &'a NodeAddress {
                info.primary
            }

            fn choose_replica<'a>(&self, info: &ReplicaRoutingInfo<'a>) -> &'a NodeAddress {
                info.replicas.first()
            }
        }

        let strategy: Option<&dyn ReadRoutingStrategy> = Some(&AlwaysPrimary);
        let slot_map = SlotMap::from_slots(vec![Slot {
            start: 1,
            end: 1000,
            master: "node1:6379".into(),
            replicas: vec!["replica1:6379".into()],
        }]);

        // ReplicaOptional with this strategy routes to primary
        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::ReplicaOptional), strategy)
                .unwrap()
        );
        // ReplicaRequired cannot route to primary — gets first replica instead
        assert_eq!(
            "replica1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::ReplicaRequired), strategy)
                .unwrap()
        );
    }

    #[test]
    fn test_slot_map_strategy_receives_correct_info() {
        use std::sync::atomic::{AtomicU16, Ordering};

        struct InfoCapture {
            slot: AtomicU16,
            range_start: AtomicU16,
            range_end: AtomicU16,
        }
        impl ReadRoutingStrategy for InfoCapture {
            fn choose_any<'a>(&self, info: &AnyNodeRoutingInfo<'a>) -> &'a NodeAddress {
                self.slot.store(info.slot, Ordering::Relaxed);
                self.range_start
                    .store(info.slot_range_start, Ordering::Relaxed);
                self.range_end.store(info.slot_range_end, Ordering::Relaxed);
                info.primary
            }

            fn choose_replica<'a>(&self, info: &ReplicaRoutingInfo<'a>) -> &'a NodeAddress {
                self.slot.store(info.slot, Ordering::Relaxed);
                self.range_start
                    .store(info.slot_range_start, Ordering::Relaxed);
                self.range_end.store(info.slot_range_end, Ordering::Relaxed);
                info.replicas.first()
            }
        }

        let capture = InfoCapture {
            slot: AtomicU16::new(0),
            range_start: AtomicU16::new(0),
            range_end: AtomicU16::new(0),
        };
        let strategy: Option<&dyn ReadRoutingStrategy> = Some(&capture);
        let slot_map = SlotMap::from_slots(vec![Slot {
            start: 1,
            end: 1000,
            master: "node1:6379".into(),
            replicas: vec!["replica1:6379".into()],
        }]);

        slot_map.slot_addr_for_route(&Route::new(500, SlotAddr::ReplicaOptional), strategy);
        assert_eq!(500, capture.slot.load(Ordering::Relaxed));
        assert_eq!(1, capture.range_start.load(Ordering::Relaxed));
        assert_eq!(1000, capture.range_end.load(Ordering::Relaxed));
    }

    fn get_slot_map() -> SlotMap {
        SlotMap::from_slots(vec![
            Slot::new(1, 1000, "node1:6379".into(), vec!["replica1:6379".into()]),
            Slot::new(
                1002,
                2000,
                "node2:6379".into(),
                vec!["replica2:6379".into(), "replica3:6379".into()],
            ),
            Slot::new(
                2001,
                3000,
                "node3:6379".into(),
                vec![
                    "replica4:6379".into(),
                    "replica5:6379".into(),
                    "replica6:6379".into(),
                ],
            ),
            Slot::new(
                3001,
                4000,
                "node2:6379".into(),
                vec!["replica2:6379".into(), "replica3:6379".into()],
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
                &"node1:6379".into(),
                &"node2:6379".into(),
                &"node3:6379".into()
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
                &"node1:6379".into(),
                &"node2:6379".into(),
                &"node3:6379".into(),
                &"replica1:6379".into(),
                &"replica2:6379".into(),
                &"replica3:6379".into(),
                &"replica4:6379".into(),
                &"replica5:6379".into(),
                &"replica6:6379".into()
            ])
        );
    }

    #[test]
    fn test_slot_map_get_multi_node() {
        let strategy = random_strategy();
        let strategy_ref = strategy.as_deref().map(|s| s as &dyn ReadRoutingStrategy);
        let slot_map = get_slot_map();
        let routes = vec![
            (Route::new(1, SlotAddr::Master), vec![]),
            (Route::new(2001, SlotAddr::ReplicaOptional), vec![]),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes, strategy_ref)
            .collect::<Vec<_>>();
        assert!(addresses.contains(&Some(&"node1:6379".into())));
        assert!(
            addresses.contains(&Some(&"replica4:6379".into()))
                || addresses.contains(&Some(&"replica5:6379".into()))
                || addresses.contains(&Some(&"replica6:6379".into()))
        );
    }

    #[test]
    fn test_slot_map_should_ignore_replicas_in_multi_slot_if_no_strategy() {
        let slot_map = get_slot_map();
        let routes = vec![
            (Route::new(1, SlotAddr::Master), vec![]),
            (Route::new(2001, SlotAddr::ReplicaOptional), vec![]),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes, None)
            .collect::<Vec<_>>();
        assert_eq!(
            addresses,
            vec![Some(&"node1:6379".into()), Some(&"node3:6379".into())]
        );
    }

    /// This test is needed in order to verify that if the MultiSlot route finds the same node for more than a single route,
    /// that node's address will appear multiple times, in the same order.
    #[test]
    fn test_slot_map_get_repeating_addresses_when_the_same_node_is_found_in_multi_slot() {
        let strategy = random_strategy();
        let strategy_ref = strategy.as_deref().map(|s| s as &dyn ReadRoutingStrategy);
        let slot_map = get_slot_map();
        let routes = vec![
            (Route::new(1, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2001, SlotAddr::Master), vec![]),
            (Route::new(2, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2002, SlotAddr::Master), vec![]),
            (Route::new(3, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2003, SlotAddr::Master), vec![]),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes, strategy_ref)
            .collect::<Vec<_>>();
        assert_eq!(
            addresses,
            vec![
                Some(&"replica1:6379".into()),
                Some(&"node3:6379".into()),
                Some(&"replica1:6379".into()),
                Some(&"node3:6379".into()),
                Some(&"replica1:6379".into()),
                Some(&"node3:6379".into())
            ]
        );
    }

    #[test]
    fn test_slot_map_get_none_when_slot_is_missing_from_multi_slot() {
        let strategy = random_strategy();
        let strategy_ref = strategy.as_deref().map(|s| s as &dyn ReadRoutingStrategy);
        let slot_map = get_slot_map();
        let routes = vec![
            (Route::new(1, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(5000, SlotAddr::Master), vec![]),
            (Route::new(6000, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2002, SlotAddr::Master), vec![]),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes, strategy_ref)
            .collect::<Vec<_>>();
        assert_eq!(
            addresses,
            vec![
                Some(&"replica1:6379".into()),
                None,
                None,
                Some(&"node3:6379".into())
            ]
        );
    }
}
