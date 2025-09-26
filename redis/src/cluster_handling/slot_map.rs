use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::str::FromStr;

use arcstr::ArcStr;
use rand::{rng, seq::IndexedRandom};

use crate::ConnectionAddr;
use crate::{
    cluster_routing::{Route, SlotAddr},
    ErrorKind, RedisError, RedisResult,
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

    pub fn slot_addr_for_route(&self, route: &Route) -> Option<&Node> {
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

    fn all_unique_nodes(&self, only_primaries: bool) -> HashSet<&Node> {
        let mut nodes = HashSet::new();
        if only_primaries {
            nodes.extend(
                self.values().map(|slot_addrs| {
                    slot_addrs.slot_addr(&SlotAddr::Master, self.read_from_replica)
                }),
            );
        } else {
            nodes.extend(self.values().flatten());
        }
        nodes
    }

    pub fn nodes_for_all_primaries(&self) -> HashSet<&Node> {
        self.all_unique_nodes(true)
    }

    pub fn nodes_for_all_nodes(&self) -> HashSet<&Node> {
        self.all_unique_nodes(false)
    }

    pub fn nodes_for_multi_slot<'a, 'b>(
        &'a self,
        routes: &'b [(Route, Vec<usize>)],
    ) -> impl Iterator<Item = Option<&'a Node>> + 'a
    where
        'b: 'a,
    {
        routes
            .iter()
            .map(|(route, _)| self.slot_addr_for_route(route))
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Node {
    pub(crate) host: ArcStr,
    pub(crate) port: u16,
}

impl Node {
    pub(crate) fn from_addr(addr: &str) -> RedisResult<Self> {
        // Handle IPv6 addresses with brackets
        let invalid_error =
            || RedisError::from((ErrorKind::InvalidClientConfig, "Invalid node string"));
        addr.rsplit_once(':')
            .and_then(|(host, port)| {
                Some(host.trim_start_matches('[').trim_end_matches(']'))
                    .filter(|host| !host.is_empty())
                    .zip(u16::from_str(port).ok())
                    .map(|(host, port)| Node {
                        host: host.into(),
                        port,
                    })
            })
            .ok_or_else(invalid_error)
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl From<&ConnectionAddr> for Node {
    fn from(value: &ConnectionAddr) -> Self {
        match value {
            ConnectionAddr::Tcp(host, port) => Node {
                host: host.into(),
                port: *port,
            },
            ConnectionAddr::TcpTls { host, port, .. } => Node {
                host: host.into(),
                port: *port,
            },
            ConnectionAddr::Unix(_) => unreachable!(),
        }
    }
}

/// This is just a simplified version of [`Slot`],
/// which stores only the master and optional replica
/// to avoid the need to choose a replica each time
/// a command is executed
#[derive(Debug)]
pub(crate) struct SlotAddrs {
    primary: Node,
    replicas: Box<[Node]>,
}

impl SlotAddrs {
    pub(crate) fn new(primary: Node, replicas: Vec<Node>) -> Self {
        Self {
            primary,
            replicas: replicas.into_boxed_slice(),
        }
    }

    fn get_replica_node(&self) -> &Node {
        self.replicas.choose(&mut rng()).unwrap_or(&self.primary)
    }

    pub(crate) fn slot_addr(&self, slot_addr: &SlotAddr, read_from_replica: bool) -> &Node {
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
        SlotAddrs::new(slot.primary, slot.replicas)
    }
}

impl<'a> IntoIterator for &'a SlotAddrs {
    type Item = &'a Node;
    type IntoIter = std::iter::Chain<std::iter::Once<&'a Node>, std::slice::Iter<'a, Node>>;

    fn into_iter(self) -> std::iter::Chain<std::iter::Once<&'a Node>, std::slice::Iter<'a, Node>> {
        std::iter::once(&self.primary).chain(self.replicas.iter())
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Slot {
    pub(crate) start: u16,
    pub(crate) end: u16,
    pub(crate) primary: Node,
    pub(crate) replicas: Vec<Node>,
}

impl Slot {
    pub fn new(start: u16, end: u16, primary: Node, replicas: Vec<Node>) -> RedisResult<Self> {
        // Validate slot range
        if start > end {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "Slot start cannot be greater than slot end",
            )));
        }

        Ok(Self {
            start,
            end,
            primary,
            replicas,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a Node for testing
    fn node(host: &str, port: u16) -> Node {
        Node {
            host: ArcStr::from(host),
            port,
        }
    }

    #[test]
    fn test_slot_map() {
        let slot_map = SlotMap::from_slots(
            vec![
                Slot {
                    start: 1,
                    end: 1000,
                    primary: node("node1", 6379),
                    replicas: vec![node("replica1", 6379)],
                },
                Slot {
                    start: 1001,
                    end: 2000,
                    primary: node("node2", 6379),
                    replicas: vec![node("replica2", 6379)],
                },
            ],
            true,
        );

        assert_eq!(
            &node("node1", 6379),
            slot_map
                .slot_addr_for_route(&Route::new(1, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            &node("node1", 6379),
            slot_map
                .slot_addr_for_route(&Route::new(500, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            &node("node1", 6379),
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            &node("replica1", 6379),
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::ReplicaOptional))
                .unwrap()
        );
        assert_eq!(
            &node("node2", 6379),
            slot_map
                .slot_addr_for_route(&Route::new(1001, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            &node("node2", 6379),
            slot_map
                .slot_addr_for_route(&Route::new(1500, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            &node("node2", 6379),
            slot_map
                .slot_addr_for_route(&Route::new(2000, SlotAddr::Master))
                .unwrap()
        );
        assert!(slot_map
            .slot_addr_for_route(&Route::new(2001, SlotAddr::Master))
            .is_none());
    }

    #[test]
    fn test_slot_map_when_read_from_replica_is_false() {
        let slot_map = SlotMap::from_slots(
            vec![Slot {
                start: 1,
                end: 1000,
                primary: node("node1", 6379),
                replicas: vec![node("replica1", 6379)],
            }],
            false,
        );

        assert_eq!(
            &node("node1", 6379),
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::ReplicaOptional))
                .unwrap()
        );
        assert_eq!(
            &node("replica1", 6379),
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::ReplicaRequired))
                .unwrap()
        );
    }

    fn get_slot_map(read_from_replica: bool) -> SlotMap {
        SlotMap::from_slots(
            vec![
                Slot {
                    start: 1,
                    end: 1000,
                    primary: node("node1", 6379),
                    replicas: vec![node("replica1", 6379)],
                },
                Slot {
                    start: 1002,
                    end: 2000,
                    primary: node("node2", 6379),
                    replicas: vec![node("replica2", 6379), node("replica3", 6379)],
                },
                Slot {
                    start: 2001,
                    end: 3000,
                    primary: node("node3", 6379),
                    replicas: vec![
                        node("replica4", 6379),
                        node("replica5", 6379),
                        node("replica6", 6379),
                    ],
                },
                Slot {
                    start: 3001,
                    end: 4000,
                    primary: node("node2", 6379),
                    replicas: vec![node("replica2", 6379), node("replica3", 6379)],
                },
            ],
            read_from_replica,
        )
    }

    #[test]
    fn test_slot_map_get_all_primaries() {
        let slot_map = get_slot_map(false);
        let nodes = slot_map.nodes_for_all_primaries();
        assert_eq!(nodes.len(), 3);
        assert!(nodes.contains(&&node("node1", 6379)));
        assert!(nodes.contains(&&node("node2", 6379)));
        assert!(nodes.contains(&&node("node3", 6379)));
    }

    #[test]
    fn test_slot_map_get_all_nodes() {
        let slot_map = get_slot_map(false);
        let nodes = slot_map.nodes_for_all_nodes();
        assert_eq!(nodes.len(), 9);
        assert!(nodes.contains(&&node("node1", 6379)));
        assert!(nodes.contains(&&node("node2", 6379)));
        assert!(nodes.contains(&&node("node3", 6379)));
        assert!(nodes.contains(&&node("replica1", 6379)));
        assert!(nodes.contains(&&node("replica2", 6379)));
        assert!(nodes.contains(&&node("replica3", 6379)));
        assert!(nodes.contains(&&node("replica4", 6379)));
        assert!(nodes.contains(&&node("replica5", 6379)));
        assert!(nodes.contains(&&node("replica6", 6379)));
    }

    #[test]
    fn test_slot_map_get_multi_node() {
        let slot_map = get_slot_map(true);
        let routes = vec![
            (Route::new(1, SlotAddr::Master), vec![]),
            (Route::new(2001, SlotAddr::ReplicaOptional), vec![]),
        ];
        let nodes: Vec<_> = slot_map.nodes_for_multi_slot(&routes).collect();
        assert_eq!(nodes[0].unwrap(), &node("node1", 6379));
        let replica_node = nodes[1].unwrap();
        assert!(
            replica_node == &node("replica4", 6379)
                || replica_node == &node("replica5", 6379)
                || replica_node == &node("replica6", 6379)
        );
    }

    #[test]
    fn test_slot_map_should_ignore_replicas_in_multi_slot_if_read_from_replica_is_false() {
        let slot_map = get_slot_map(false);
        let routes = vec![
            (Route::new(1, SlotAddr::Master), vec![]),
            (Route::new(2001, SlotAddr::ReplicaOptional), vec![]),
        ];
        let nodes: Vec<_> = slot_map.nodes_for_multi_slot(&routes).collect();
        assert_eq!(nodes[0].unwrap(), &node("node1", 6379));
        assert_eq!(nodes[1].unwrap(), &node("node3", 6379));
    }

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
        let nodes: Vec<_> = slot_map.nodes_for_multi_slot(&routes).collect();
        assert_eq!(nodes[0].unwrap(), &node("replica1", 6379));
        assert_eq!(nodes[1].unwrap(), &node("node3", 6379));
        assert_eq!(nodes[2].unwrap(), &node("replica1", 6379));
        assert_eq!(nodes[3].unwrap(), &node("node3", 6379));
        assert_eq!(nodes[4].unwrap(), &node("replica1", 6379));
        assert_eq!(nodes[5].unwrap(), &node("node3", 6379));
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
        let nodes: Vec<_> = slot_map.nodes_for_multi_slot(&routes).collect();
        assert_eq!(nodes[0].unwrap(), &node("replica1", 6379));
        assert!(nodes[1].is_none());
        assert!(nodes[2].is_none());
        assert_eq!(nodes[3].unwrap(), &node("node3", 6379));
    }

    #[test]
    fn test_node_address_parsing() {
        let cases = vec![
            (
                "127.0.0.1:6379",
                ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379u16),
            ),
            (
                "localhost.localdomain:6379",
                ConnectionAddr::Tcp("localhost.localdomain".to_string(), 6379u16),
            ),
            (
                "dead::cafe:beef:30001",
                ConnectionAddr::Tcp("dead::cafe:beef".to_string(), 30001u16),
            ),
            (
                "[fe80::cafe:beef%en1]:30001",
                ConnectionAddr::Tcp("fe80::cafe:beef%en1".to_string(), 30001u16),
            ),
        ];

        for (input, expected) in cases {
            assert_eq!(Node::from_addr(input), Ok((&expected).into()));
        }

        let cases = vec![":0", "[]:6379"];
        for input in cases {
            let res = Node::from_addr(input);
            assert_eq!(
                res.err(),
                Some(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "Invalid node string",
                ))),
            );
        }
    }
}
