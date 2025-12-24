use std::collections::{BTreeMap, HashSet};

use arcstr::ArcStr;
use rand::{rng, seq::IndexedRandom};

use crate::cluster_routing::{Route, SlotAddr};

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
    read_from_replica: bool,
}

impl SlotMap {
    pub fn new(read_from_replica: bool) -> Self {
        Self {
            slots: Default::default(),
            read_from_replica,
        }
    }

    pub fn from_slots(slots: Vec<Slot>, read_from_replica: bool) -> Self {
        Self {
            slots: slots
                .into_iter()
                .map(|slot| (slot.end, SlotMapValue::from_slot(slot)))
                .collect(),
            read_from_replica,
        }
    }

    #[cfg(feature = "cluster-async")]
    pub fn fill_slots(&mut self, slots: Vec<Slot>) {
        for slot in slots {
            self.slots.insert(slot.end, SlotMapValue::from_slot(slot));
        }
    }

    pub fn slot_addr_for_route(&self, route: &Route) -> Option<&ArcStr> {
        let slot = route.slot();
        self.slots
            .range(slot..)
            .next()
            .and_then(|(end, slot_value)| {
                if slot <= *end && slot_value.start <= slot {
                    Some(
                        slot_value
                            .addrs
                            .slot_addr(&route.slot_addr(), self.read_from_replica),
                    )
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
            addresses.extend(
                self.values().map(|slot_addrs| {
                    slot_addrs.slot_addr(&SlotAddr::Master, self.read_from_replica)
                }),
            );
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

    pub fn addresses_for_multi_slot<'a, 'b>(
        &'a self,
        routes: &'b [(Route, Vec<usize>)],
    ) -> impl Iterator<Item = Option<&'a ArcStr>> + 'a
    where
        'b: 'a,
    {
        routes
            .iter()
            .map(|(route, _)| self.slot_addr_for_route(route))
    }
}

/// This is just a simplified version of [`Slot`],
/// which stores only the master and optional replica
/// to avoid the need to choose a replica each time
/// a command is executed
#[derive(Debug)]
pub(crate) struct SlotAddrs {
    primary: ArcStr,
    replicas: Vec<ArcStr>,
}

impl SlotAddrs {
    pub(crate) fn new(primary: ArcStr, replicas: Vec<ArcStr>) -> Self {
        Self { primary, replicas }
    }

    fn get_replica_node(&self) -> &ArcStr {
        self.replicas.choose(&mut rng()).unwrap_or(&self.primary)
    }

    pub(crate) fn slot_addr(&self, slot_addr: &SlotAddr, read_from_replica: bool) -> &ArcStr {
        match slot_addr {
            SlotAddr::Master => &self.primary,
            SlotAddr::ReplicaOptional => {
                if read_from_replica {
                    self.get_replica_node()
                } else {
                    &self.primary
                }
            }
            SlotAddr::ReplicaRequired => self.get_replica_node(),
        }
    }

    pub(crate) fn from_slot(slot: Slot) -> Self {
        SlotAddrs::new(slot.master, slot.replicas)
    }
}

impl<'a> IntoIterator for &'a SlotAddrs {
    type Item = &'a ArcStr;
    type IntoIter = std::iter::Chain<std::iter::Once<&'a ArcStr>, std::slice::Iter<'a, ArcStr>>;

    fn into_iter(
        self,
    ) -> std::iter::Chain<std::iter::Once<&'a ArcStr>, std::slice::Iter<'a, ArcStr>> {
        std::iter::once(&self.primary).chain(self.replicas.iter())
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

    use super::*;

    #[test]
    fn test_slot_map() {
        let slot_map = SlotMap::from_slots(
            vec![
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
            ],
            true,
        );

        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(500, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "replica1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::ReplicaOptional))
                .unwrap()
        );
        assert_eq!(
            "node2:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1001, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node2:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1500, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node2:6379",
            slot_map
                .slot_addr_for_route(&Route::new(2000, SlotAddr::Master))
                .unwrap()
        );
        assert!(
            slot_map
                .slot_addr_for_route(&Route::new(2001, SlotAddr::Master))
                .is_none()
        );
    }

    #[test]
    fn test_slot_map_when_read_from_replica_is_false() {
        let slot_map = SlotMap::from_slots(
            vec![Slot {
                start: 1,
                end: 1000,
                master: "node1:6379".into(),
                replicas: vec!["replica1:6379".into()],
            }],
            false,
        );

        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::ReplicaOptional))
                .unwrap()
        );
        assert_eq!(
            "replica1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::ReplicaRequired))
                .unwrap()
        );
    }

    fn get_slot_map(read_from_replica: bool) -> SlotMap {
        SlotMap::from_slots(
            vec![
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
            ],
            read_from_replica,
        )
    }

    #[test]
    fn test_slot_map_get_all_primaries() {
        let slot_map = get_slot_map(false);
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
        let slot_map = get_slot_map(false);
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
        let slot_map = get_slot_map(true);
        let routes = vec![
            (Route::new(1, SlotAddr::Master), vec![]),
            (Route::new(2001, SlotAddr::ReplicaOptional), vec![]),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes)
            .collect::<Vec<_>>();
        assert!(addresses.contains(&Some(&"node1:6379".into())));
        assert!(
            addresses.contains(&Some(&"replica4:6379".into()))
                || addresses.contains(&Some(&"replica5:6379".into()))
                || addresses.contains(&Some(&"replica6:6379".into()))
        );
    }

    #[test]
    fn test_slot_map_should_ignore_replicas_in_multi_slot_if_read_from_replica_is_false() {
        let slot_map = get_slot_map(false);
        let routes = vec![
            (Route::new(1, SlotAddr::Master), vec![]),
            (Route::new(2001, SlotAddr::ReplicaOptional), vec![]),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes)
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
        let slot_map = get_slot_map(true);
        let routes = vec![
            (Route::new(1, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2001, SlotAddr::Master), vec![]),
            (Route::new(2, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2002, SlotAddr::Master), vec![]),
            (Route::new(3, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2003, SlotAddr::Master), vec![]),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes)
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
        let slot_map = get_slot_map(true);
        let routes = vec![
            (Route::new(1, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(5000, SlotAddr::Master), vec![]),
            (Route::new(6000, SlotAddr::ReplicaOptional), vec![]),
            (Route::new(2002, SlotAddr::Master), vec![]),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes)
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
