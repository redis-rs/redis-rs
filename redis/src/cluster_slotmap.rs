use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::{BTreeMap, HashSet};

/// Defines the slot and the [`SlotAddr`] to which
/// a command should be sent
#[derive(Eq, PartialEq, Clone, Copy, Debug, Hash)]
pub struct Route(pub u16, pub SlotAddr);

impl Route {
    /// Returns a new Route.
    pub fn new(slot: u16, slot_addr: SlotAddr) -> Self {
        Self(slot, slot_addr)
    }

    pub(crate) fn slot(&self) -> u16 {
        self.0
    }

    pub(crate) fn slot_addr(&self) -> &SlotAddr {
        &self.1
    }
}

#[derive(Debug)]
pub(crate) struct Slot {
    pub(crate) start: u16,
    pub(crate) end: u16,
    pub(crate) master: String,
    pub(crate) replicas: Vec<String>,
}

impl Slot {
    pub fn new(s: u16, e: u16, m: String, r: Vec<String>) -> Self {
        Self {
            start: s,
            end: e,
            master: m,
            replicas: r,
        }
    }
}

/// What type of node should a request be routed to.
#[derive(Eq, PartialEq, Clone, Copy, Debug, Hash)]
pub enum SlotAddr {
    /// The request must be routed to primary node
    Master,
    /// The request may be routed to a replica node.
    /// For example, a GET command can be routed either to replica or primary.
    ReplicaOptional,
    /// The request must be routed to replica node, if one exists.
    /// For example, by user requested routing.
    ReplicaRequired,
}

/// This is just a simplified version of [`Slot`],
/// which stores only the master and [optional] replica
/// to avoid the need to choose a replica each time
/// a command is executed
#[derive(Debug)]
pub(crate) struct SlotAddrs {
    primary: String,
    replicas: Vec<String>,
}

impl SlotAddrs {
    pub(crate) fn new(primary: String, replicas: Vec<String>) -> Self {
        Self { primary, replicas }
    }

    fn get_replica_node(&self) -> &str {
        self.replicas
            .choose(&mut thread_rng())
            .unwrap_or(&self.primary)
    }

    pub(crate) fn slot_addr(&self, slot_addr: &SlotAddr, read_from_replica: bool) -> &str {
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
    type Item = &'a String;
    type IntoIter = std::iter::Chain<std::iter::Once<&'a String>, std::slice::Iter<'a, String>>;

    fn into_iter(
        self,
    ) -> std::iter::Chain<std::iter::Once<&'a String>, std::slice::Iter<'a, String>> {
        std::iter::once(&self.primary).chain(self.replicas.iter())
    }
}

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

    pub fn fill_slots(&mut self, slots: Vec<Slot>) {
        for slot in slots {
            self.slots.insert(slot.end, SlotMapValue::from_slot(slot));
        }
    }

    pub fn slot_addr_for_route(&self, route: &Route) -> Option<&str> {
        let slot = route.slot();
        self.slots
            .range(slot..)
            .next()
            .and_then(|(end, slot_value)| {
                if slot <= *end && slot_value.start <= slot {
                    Some(
                        slot_value
                            .addrs
                            .slot_addr(route.slot_addr(), self.read_from_replica),
                    )
                } else {
                    None
                }
            })
    }

    pub fn clear(&mut self) {
        self.slots.clear();
    }

    pub fn values(&self) -> impl Iterator<Item = &SlotAddrs> {
        self.slots.values().map(|slot_value| &slot_value.addrs)
    }

    fn all_unique_addresses(&self, only_primaries: bool) -> HashSet<&str> {
        let mut addresses: HashSet<&str> = HashSet::new();
        if only_primaries {
            addresses.extend(
                self.values().map(|slot_addrs| {
                    slot_addrs.slot_addr(&SlotAddr::Master, self.read_from_replica)
                }),
            );
        } else {
            addresses.extend(
                self.values()
                    .flat_map(|slot_addrs| slot_addrs.into_iter())
                    .map(|str| str.as_str()),
            );
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
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_map_retrieve_routes() {
        let slot_map = SlotMap::from_slots(
            vec![
                Slot {
                    start: 1,
                    end: 1000,
                    master: "node1:6379".to_owned(),
                    replicas: vec!["replica1:6379".to_owned()],
                },
                Slot {
                    start: 1002,
                    end: 2000,
                    master: "node2:6379".to_owned(),
                    replicas: vec!["replica2:6379".to_owned()],
                },
            ],
            true,
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

    fn get_slot_map(read_from_replica: bool) -> SlotMap {
        SlotMap::from_slots(
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
        let slot_map = get_slot_map(false);
        let addresses = slot_map.addresses_for_all_primaries();
        assert_eq!(
            addresses,
            HashSet::from_iter(["node1:6379", "node2:6379", "node3:6379"])
        );
    }

    #[test]
    fn test_slot_map_get_all_nodes() {
        let slot_map = get_slot_map(false);
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
        let slot_map = get_slot_map(true);
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
        assert_eq!(addresses, vec![Some("node1:6379"), Some("node3:6379")]);
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
            vec![Some("replica1:6379"), None, None, Some("node3:6379")]
        );
    }
}
