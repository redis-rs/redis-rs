use std::{
    collections::{BTreeMap, HashSet},
    fmt::Display,
    sync::atomic::AtomicUsize,
};

use crate::cluster_routing::{Route, Slot, SlotAddr, SlotAddrs};

#[derive(Debug)]
pub(crate) struct SlotMapValue {
    pub(crate) start: u16,
    pub(crate) addrs: SlotAddrs,
    pub(crate) latest_used_replica: AtomicUsize,
}

impl SlotMapValue {
    fn from_slot(slot: Slot) -> Self {
        Self {
            start: slot.start(),
            addrs: SlotAddrs::from_slot(slot),
            latest_used_replica: AtomicUsize::new(0),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Copy)]
pub(crate) enum ReadFromReplicaStrategy {
    #[default]
    AlwaysFromPrimary,
    RoundRobin,
}

#[derive(Debug, Default)]
pub(crate) struct SlotMap {
    pub(crate) slots: BTreeMap<u16, SlotMapValue>,
    read_from_replica: ReadFromReplicaStrategy,
}

fn get_address_from_slot(
    slot: &SlotMapValue,
    read_from_replica: ReadFromReplicaStrategy,
    slot_addr: SlotAddr,
) -> &str {
    if slot_addr == SlotAddr::Master || slot.addrs.replicas.is_empty() {
        return slot.addrs.primary.as_str();
    }
    match read_from_replica {
        ReadFromReplicaStrategy::AlwaysFromPrimary => slot.addrs.primary.as_str(),
        ReadFromReplicaStrategy::RoundRobin => {
            let index = slot
                .latest_used_replica
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                % slot.addrs.replicas.len();
            slot.addrs.replicas[index].as_str()
        }
    }
}

impl SlotMap {
    pub(crate) fn new(slots: Vec<Slot>, read_from_replica: ReadFromReplicaStrategy) -> Self {
        let mut this = Self {
            slots: BTreeMap::new(),
            read_from_replica,
        };
        this.slots.extend(
            slots
                .into_iter()
                .map(|slot| (slot.end(), SlotMapValue::from_slot(slot))),
        );
        this
    }

    pub fn slot_value_for_route(&self, route: &Route) -> Option<&SlotMapValue> {
        let slot = route.slot();
        self.slots
            .range(slot..)
            .next()
            .and_then(|(end, slot_value)| {
                if slot <= *end && slot_value.start <= slot {
                    Some(slot_value)
                } else {
                    None
                }
            })
    }

    pub fn slot_addr_for_route(&self, route: &Route) -> Option<&str> {
        self.slot_value_for_route(route).map(|slot_value| {
            get_address_from_slot(slot_value, self.read_from_replica, route.slot_addr())
        })
    }

    pub fn values(&self) -> impl Iterator<Item = &SlotAddrs> {
        self.slots.values().map(|slot_value| &slot_value.addrs)
    }

    fn all_unique_addresses(&self, only_primaries: bool) -> HashSet<&str> {
        let mut addresses = HashSet::new();
        for slot in self.values() {
            addresses.insert(slot.primary.as_str());
            if !only_primaries {
                addresses.extend(slot.replicas.iter().map(|str| str.as_str()));
            }
        }

        addresses
    }

    pub fn addresses_for_all_primaries(&self) -> HashSet<&str> {
        self.all_unique_addresses(true)
    }

    pub fn addresses_for_all_nodes(&self) -> HashSet<&str> {
        self.all_unique_addresses(false)
    }

    pub fn addresses_for_multi_slot<'a, 'b>(
        &'a self,
        routes: &'b [(Route, Vec<usize>)],
    ) -> impl Iterator<Item = Option<&'a str>> + 'a
    where
        'b: 'a,
    {
        routes
            .iter()
            .map(|(route, _)| self.slot_addr_for_route(route))
    }

    // Returns the slots that are assigned to the given address.
    pub(crate) fn get_slots_of_node(&self, node_address: &str) -> Vec<u16> {
        let node_address = node_address.to_string();
        self.slots
            .iter()
            .filter_map(|(end, slot_value)| {
                if slot_value.addrs.primary == node_address
                    || slot_value.addrs.replicas.contains(&node_address)
                {
                    Some(slot_value.start..(*end + 1))
                } else {
                    None
                }
            })
            .flatten()
            .collect()
    }

    pub(crate) fn get_node_address_for_slot(
        &self,
        slot: u16,
        slot_addr: SlotAddr,
    ) -> Option<String> {
        self.slots.range(slot..).next().and_then(|(_, slot_value)| {
            if slot_value.start <= slot {
                Some(
                    get_address_from_slot(slot_value, self.read_from_replica, slot_addr)
                        .to_string(),
                )
            } else {
                None
            }
        })
    }
}

impl Display for SlotMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Strategy: {:?}. Slot mapping:", self.read_from_replica)?;
        for (end, slot_map_value) in self.slots.iter() {
            writeln!(
                f,
                "({}-{}): primary: {}, replicas: {:?}",
                slot_map_value.start,
                end,
                slot_map_value.addrs.primary,
                slot_map_value.addrs.replicas
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_map_retrieve_routes() {
        let slot_map = SlotMap::new(
            vec![
                Slot::new(
                    1,
                    1000,
                    "node1:6379".to_owned(),
                    vec!["replica1:6379".to_owned()],
                ),
                Slot::new(
                    1002,
                    2000,
                    "node2:6379".to_owned(),
                    vec!["replica2:6379".to_owned()],
                ),
            ],
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        );

        assert!(slot_map
            .slot_addr_for_route(&Route::new(0, SlotAddr::Master))
            .is_none());
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
        assert!(slot_map
            .slot_addr_for_route(&Route::new(1001, SlotAddr::Master))
            .is_none());

        assert_eq!(
            "node2:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1002, SlotAddr::Master))
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
        assert!(slot_map
            .slot_addr_for_route(&Route::new(2001, SlotAddr::Master))
            .is_none());
    }

    fn get_slot_map(read_from_replica: ReadFromReplicaStrategy) -> SlotMap {
        SlotMap::new(
            vec![
                Slot::new(
                    1,
                    1000,
                    "node1:6379".to_owned(),
                    vec!["replica1:6379".to_owned()],
                ),
                Slot::new(
                    1002,
                    2000,
                    "node2:6379".to_owned(),
                    vec!["replica2:6379".to_owned(), "replica3:6379".to_owned()],
                ),
                Slot::new(
                    2001,
                    3000,
                    "node3:6379".to_owned(),
                    vec![
                        "replica4:6379".to_owned(),
                        "replica5:6379".to_owned(),
                        "replica6:6379".to_owned(),
                    ],
                ),
                Slot::new(
                    3001,
                    4000,
                    "node2:6379".to_owned(),
                    vec!["replica2:6379".to_owned(), "replica3:6379".to_owned()],
                ),
            ],
            read_from_replica,
        )
    }

    #[test]
    fn test_slot_map_get_all_primaries() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::AlwaysFromPrimary);
        let addresses = slot_map.addresses_for_all_primaries();
        assert_eq!(
            addresses,
            HashSet::from_iter(["node1:6379", "node2:6379", "node3:6379"])
        );
    }

    #[test]
    fn test_slot_map_get_all_nodes() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::AlwaysFromPrimary);
        let addresses = slot_map.addresses_for_all_nodes();
        assert_eq!(
            addresses,
            HashSet::from_iter([
                "node1:6379",
                "node2:6379",
                "node3:6379",
                "replica1:6379",
                "replica2:6379",
                "replica3:6379",
                "replica4:6379",
                "replica5:6379",
                "replica6:6379"
            ])
        );
    }

    #[test]
    fn test_slot_map_get_multi_node() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::RoundRobin);
        let routes = vec![
            (Route::new(1, SlotAddr::Master), vec![]),
            (Route::new(2001, SlotAddr::ReplicaOptional), vec![]),
        ];
        let addresses = slot_map
            .addresses_for_multi_slot(&routes)
            .collect::<Vec<_>>();
        assert!(addresses.contains(&Some("node1:6379")));
        assert!(
            addresses.contains(&Some("replica4:6379"))
                || addresses.contains(&Some("replica5:6379"))
                || addresses.contains(&Some("replica6:6379"))
        );
    }

    /// This test is needed in order to verify that if the MultiSlot route finds the same node for more than a single route,
    /// that node's address will appear multiple times, in the same order.
    #[test]
    fn test_slot_map_get_repeating_addresses_when_the_same_node_is_found_in_multi_slot() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::RoundRobin);
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
                Some("replica1:6379"),
                Some("node3:6379"),
                Some("replica1:6379"),
                Some("node3:6379"),
                Some("replica1:6379"),
                Some("node3:6379")
            ]
        );
    }

    #[test]
    fn test_slot_map_get_none_when_slot_is_missing_from_multi_slot() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::RoundRobin);
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
            vec![Some("replica1:6379"), None, None, Some("node3:6379")]
        );
    }

    #[test]
    fn test_slot_map_rotate_read_replicas() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::RoundRobin);
        let route = Route::new(2001, SlotAddr::ReplicaOptional);
        let mut addresses = vec![
            slot_map.slot_addr_for_route(&route).unwrap(),
            slot_map.slot_addr_for_route(&route).unwrap(),
            slot_map.slot_addr_for_route(&route).unwrap(),
        ];
        addresses.sort();
        assert_eq!(
            addresses,
            vec!["replica4:6379", "replica5:6379", "replica6:6379"]
        );
    }

    #[test]
    fn test_get_slots_of_node() {
        let slot_map = get_slot_map(ReadFromReplicaStrategy::AlwaysFromPrimary);
        assert_eq!(
            slot_map.get_slots_of_node("node1:6379"),
            (1..1001).collect::<Vec<u16>>()
        );
        assert_eq!(
            slot_map.get_slots_of_node("node2:6379"),
            vec![1002..2001, 3001..4001]
                .into_iter()
                .flatten()
                .collect::<Vec<u16>>()
        );
        assert_eq!(
            slot_map.get_slots_of_node("replica3:6379"),
            vec![1002..2001, 3001..4001]
                .into_iter()
                .flatten()
                .collect::<Vec<u16>>()
        );
        assert_eq!(
            slot_map.get_slots_of_node("replica4:6379"),
            (2001..3001).collect::<Vec<u16>>()
        );
        assert_eq!(
            slot_map.get_slots_of_node("replica5:6379"),
            (2001..3001).collect::<Vec<u16>>()
        );
        assert_eq!(
            slot_map.get_slots_of_node("replica6:6379"),
            (2001..3001).collect::<Vec<u16>>()
        );
    }
}
