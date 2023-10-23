//! This module provides the functionality to refresh and calculate the cluster topology for Redis Cluster.

use crate::cluster::get_connection_addr;
use crate::cluster_routing::{Route, Slot, SlotAddr, SlotAddrs};
use crate::{cluster::TlsMode, ErrorKind, RedisError, RedisResult, Value};
use derivative::Derivative;
use std::collections::{hash_map::DefaultHasher, BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

/// The default number of refersh topology retries
pub const DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES: usize = 3;
/// The default timeout for retrying topology refresh
pub const DEFAULT_REFRESH_SLOTS_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
/// The default initial interval for retrying topology refresh
pub const DEFAULT_REFRESH_SLOTS_RETRY_INITIAL_INTERVAL: Duration = Duration::from_millis(500);

pub(crate) const SLOT_SIZE: u16 = 16384;
pub(crate) type TopologyHash = u64;

#[derive(Derivative)]
#[derivative(PartialEq, Eq)]
#[derive(Debug)]
pub(crate) struct TopologyView {
    pub(crate) hash_value: TopologyHash,
    #[derivative(PartialEq = "ignore")]
    pub(crate) topology_value: Value,
    #[derivative(PartialEq = "ignore")]
    pub(crate) nodes_count: u16,
    #[derivative(PartialEq = "ignore")]
    slots_and_count: Option<(u16, Vec<Slot>)>,
}

impl TopologyView {
    // Tries to parse the `topology_value` field, and sets the parsed value, and the number of covered slots, in `slots_and_count`.
    // If `slots_and_count` is already not `None`, the function will return early. This means that resetting
    // `topology_value` after calling this brings the object into an inconsistent state.
    fn parse_and_count_slots(&mut self, tls_mode: Option<TlsMode>) {
        if self.slots_and_count.is_some() {
            return;
        }
        self.slots_and_count =
            parse_slots(&self.topology_value, tls_mode)
                .ok()
                .map(|parsed_slots| {
                    let slot_count = parsed_slots
                        .iter()
                        .fold(0, |acc, slot| acc + slot.end() - slot.start());
                    (slot_count, parsed_slots)
                })
    }
}

#[derive(Debug)]
pub(crate) struct SlotMapValue {
    start: u16,
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
    slots: BTreeMap<u16, SlotMapValue>,
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

pub(crate) fn slot(key: &[u8]) -> u16 {
    crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE
}

fn get_hashtag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|v| *v == b'{');
    let open = match open {
        Some(open) => open,
        None => return None,
    };

    let close = key[open..].iter().position(|v| *v == b'}');
    let close = match close {
        Some(close) => close,
        None => return None,
    };

    let rv = &key[open + 1..open + close];
    if rv.is_empty() {
        None
    } else {
        Some(rv)
    }
}

/// Returns the slot that matches `key`.
pub fn get_slot(key: &[u8]) -> u16 {
    let key = match get_hashtag(key) {
        Some(tag) => tag,
        None => key,
    };

    slot(key)
}

// Parse slot data from raw redis value.
pub(crate) fn parse_slots(raw_slot_resp: &Value, tls: Option<TlsMode>) -> RedisResult<Vec<Slot>> {
    // Parse response.
    let mut result = Vec::with_capacity(2);

    if let Value::Array(items) = raw_slot_resp {
        let mut iter = items.iter();
        while let Some(Value::Array(item)) = iter.next() {
            if item.len() < 3 {
                continue;
            }

            let start = if let Value::Int(start) = item[0] {
                start as u16
            } else {
                continue;
            };

            let end = if let Value::Int(end) = item[1] {
                end as u16
            } else {
                continue;
            };

            let mut nodes: Vec<String> = item
                .iter()
                .skip(2)
                .filter_map(|node| {
                    if let Value::Array(node) = node {
                        if node.len() < 2 {
                            return None;
                        }

                        let ip = if let Value::BulkString(ref ip) = node[0] {
                            String::from_utf8_lossy(ip)
                        } else {
                            return None;
                        };
                        if ip.is_empty() {
                            return None;
                        }

                        let port = if let Value::Int(port) = node[1] {
                            port as u16
                        } else {
                            return None;
                        };
                        Some(get_connection_addr(ip.into_owned(), port, tls, None).to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if nodes.is_empty() {
                continue;
            }

            let replicas = nodes.split_off(1);
            result.push(Slot::new(start, end, nodes.pop().unwrap(), replicas));
        }
    }

    Ok(result)
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub(crate) fn calculate_topology(
    topology_views: Vec<Value>,
    curr_retry: usize,
    tls_mode: Option<TlsMode>,
    num_of_queried_nodes: usize,
    read_from_replica: ReadFromReplicaStrategy,
) -> Result<(SlotMap, TopologyHash), RedisError> {
    if topology_views.is_empty() {
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: All CLUSTER SLOTS results are errors",
        )));
    }
    let mut hash_view_map = HashMap::new();
    for view in topology_views {
        let hash_value = calculate_hash(&view);
        let topology_entry = hash_view_map.entry(hash_value).or_insert(TopologyView {
            hash_value,
            topology_value: view,
            nodes_count: 0,
            slots_and_count: None,
        });
        topology_entry.nodes_count += 1;
    }
    let mut non_unique_max_node_count = false;
    let mut vec_iter = hash_view_map.into_values();
    let mut most_frequent_topology = match vec_iter.next() {
        Some(view) => view,
        None => {
            return Err(RedisError::from((
                ErrorKind::ResponseError,
                "No topology views found",
            )));
        }
    };
    // Find the most frequent topology view
    for mut curr_view in vec_iter {
        match most_frequent_topology
            .nodes_count
            .cmp(&curr_view.nodes_count)
        {
            std::cmp::Ordering::Less => {
                most_frequent_topology = curr_view;
                non_unique_max_node_count = false;
            }
            std::cmp::Ordering::Greater => continue,
            std::cmp::Ordering::Equal => {
                non_unique_max_node_count = true;

                // We choose as the greater view the one with higher slot coverage.
                most_frequent_topology.parse_and_count_slots(tls_mode);
                if let Some((slot_count, _)) = most_frequent_topology.slots_and_count {
                    curr_view.parse_and_count_slots(tls_mode);
                    let curr_slot_count = curr_view
                        .slots_and_count
                        .as_ref()
                        .map(|(slot_count, _)| *slot_count)
                        .unwrap_or(0);

                    if let std::cmp::Ordering::Less = slot_count.cmp(&curr_slot_count) {
                        most_frequent_topology = curr_view;
                    }
                } else {
                    most_frequent_topology = curr_view;
                }
            }
        }
    }

    let parse_and_built_result = |mut most_frequent_topology: TopologyView| {
        most_frequent_topology.parse_and_count_slots(tls_mode);
        let slots_data = most_frequent_topology
            .slots_and_count
            .map(|(_, slots)| slots)
            .ok_or(RedisError::from((
                ErrorKind::ResponseError,
                "Failed to parse the slots on the majority view",
            )))?;

        Ok((
            SlotMap::new(slots_data, read_from_replica),
            most_frequent_topology.hash_value,
        ))
    };

    if non_unique_max_node_count {
        // More than a single most frequent view was found
        // If we reached the last retry, or if we it's a 2-nodes cluster, we'll return a view with the highest slot coverage, and that is one of most agreed on views.
        if curr_retry >= DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES || num_of_queried_nodes < 3 {
            return parse_and_built_result(most_frequent_topology);
        }
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: Failed to obtain a majority in topology views",
        )));
    }

    // The rate of agreement of the topology view is determined by assessing the number of nodes that share this view out of the total number queried
    let agreement_rate = most_frequent_topology.nodes_count as f32 / num_of_queried_nodes as f32;
    const MIN_AGREEMENT_RATE: f32 = 0.2;
    if agreement_rate >= MIN_AGREEMENT_RATE {
        parse_and_built_result(most_frequent_topology)
    } else {
        Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: The accuracy of the topology view is too low",
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_routing::SlotAddrs;

    #[test]
    fn test_get_hashtag() {
        assert_eq!(get_hashtag(&b"foo{bar}baz"[..]), Some(&b"bar"[..]));
        assert_eq!(get_hashtag(&b"foo{}{baz}"[..]), None);
        assert_eq!(get_hashtag(&b"foo{{bar}}zap"[..]), Some(&b"{bar"[..]));
    }

    enum ViewType {
        SingleNodeViewFullCoverage,
        SingleNodeViewMissingSlots,
        TwoNodesViewFullCoverage,
        TwoNodesViewMissingSlots,
    }
    fn get_view(view_type: &ViewType) -> Value {
        match view_type {
            ViewType::SingleNodeViewFullCoverage => Value::Array(vec![Value::Array(vec![
                Value::Int(0_i64),
                Value::Int(16383_i64),
                Value::Array(vec![
                    Value::BulkString("node1".as_bytes().to_vec()),
                    Value::Int(6379_i64),
                ]),
            ])]),
            ViewType::SingleNodeViewMissingSlots => Value::Array(vec![Value::Array(vec![
                Value::Int(0_i64),
                Value::Int(4000_i64),
                Value::Array(vec![
                    Value::BulkString("node1".as_bytes().to_vec()),
                    Value::Int(6379_i64),
                ]),
            ])]),
            ViewType::TwoNodesViewFullCoverage => Value::Array(vec![
                Value::Array(vec![
                    Value::Int(0_i64),
                    Value::Int(4000_i64),
                    Value::Array(vec![
                        Value::BulkString("node1".as_bytes().to_vec()),
                        Value::Int(6379_i64),
                    ]),
                ]),
                Value::Array(vec![
                    Value::Int(4001_i64),
                    Value::Int(16383_i64),
                    Value::Array(vec![
                        Value::BulkString("node2".as_bytes().to_vec()),
                        Value::Int(6380_i64),
                    ]),
                ]),
            ]),
            ViewType::TwoNodesViewMissingSlots => Value::Array(vec![
                Value::Array(vec![
                    Value::Int(0_i64),
                    Value::Int(3000_i64),
                    Value::Array(vec![
                        Value::BulkString("node3".as_bytes().to_vec()),
                        Value::Int(6381_i64),
                    ]),
                ]),
                Value::Array(vec![
                    Value::Int(4001_i64),
                    Value::Int(16383_i64),
                    Value::Array(vec![
                        Value::BulkString("node4".as_bytes().to_vec()),
                        Value::Int(6382_i64),
                    ]),
                ]),
            ]),
        }
    }

    fn get_node_addr(name: &str, port: u16) -> SlotAddrs {
        SlotAddrs::new(format!("{name}:{port}"), Vec::new())
    }

    #[test]
    fn test_topology_calculator_4_nodes_queried_has_a_majority_success() {
        // 4 nodes queried (1 error): Has a majority, single_node_view should be chosen
        let queried_nodes: usize = 4;
        let topology_results = vec![
            get_view(&ViewType::SingleNodeViewFullCoverage),
            get_view(&ViewType::SingleNodeViewFullCoverage),
            get_view(&ViewType::TwoNodesViewFullCoverage),
        ];
        let (topology_view, _) = calculate_topology(
            topology_results,
            1,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let node_1 = get_node_addr("node1", 6379);
        let expected: Vec<&SlotAddrs> = vec![&node_1];
        assert_eq!(res, expected);
    }

    #[test]
    fn test_topology_calculator_3_nodes_queried_no_majority_has_more_retries_raise_error() {
        // 3 nodes queried: No majority, should return an error
        let queried_nodes = 3;
        let topology_results = vec![
            get_view(&ViewType::SingleNodeViewFullCoverage),
            get_view(&ViewType::TwoNodesViewFullCoverage),
            get_view(&ViewType::TwoNodesViewMissingSlots),
        ];
        let topology_view = calculate_topology(
            topology_results,
            1,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        );
        assert!(topology_view.is_err());
    }

    #[test]
    fn test_topology_calculator_3_nodes_queried_no_majority_last_retry_success() {
        // 3 nodes queried:: No majority, last retry, should get the view that has a full slot coverage
        let queried_nodes = 3;
        let topology_results = vec![
            get_view(&ViewType::SingleNodeViewMissingSlots),
            get_view(&ViewType::TwoNodesViewFullCoverage),
            get_view(&ViewType::TwoNodesViewMissingSlots),
        ];
        let (topology_view, _) = calculate_topology(
            topology_results,
            3,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let node_1 = get_node_addr("node1", 6379);
        let node_2 = get_node_addr("node2", 6380);
        let expected: Vec<&SlotAddrs> = vec![&node_1, &node_2];
        assert_eq!(res, expected);
    }

    #[test]
    fn test_topology_calculator_2_nodes_queried_no_majority_return_full_slot_coverage_view() {
        // 2 nodes queried: No majority, should get the view that has a full slot coverage
        let queried_nodes = 2;
        let topology_results = vec![
            get_view(&ViewType::TwoNodesViewFullCoverage),
            get_view(&ViewType::TwoNodesViewMissingSlots),
        ];
        let (topology_view, _) = calculate_topology(
            topology_results,
            1,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let node_1 = get_node_addr("node1", 6379);
        let node_2 = get_node_addr("node2", 6380);
        let expected: Vec<&SlotAddrs> = vec![&node_1, &node_2];
        assert_eq!(res, expected);
    }

    #[test]
    fn test_topology_calculator_2_nodes_queried_no_majority_no_full_coverage_prefer_fuller_coverage(
    ) {
        //  2 nodes queried: No majority, no full slot coverage, should return error
        let queried_nodes = 2;
        let topology_results = vec![
            get_view(&ViewType::SingleNodeViewMissingSlots),
            get_view(&ViewType::TwoNodesViewMissingSlots),
        ];
        let (topology_view, _) = calculate_topology(
            topology_results,
            1,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let node_1 = get_node_addr("node3", 6381);
        let node_2 = get_node_addr("node4", 6382);
        let expected: Vec<&SlotAddrs> = vec![&node_1, &node_2];
        assert_eq!(res, expected);
    }

    #[test]
    fn test_topology_calculator_3_nodes_queried_no_full_coverage_prefer_majority() {
        //  2 nodes queried: No majority, no full slot coverage, should return error
        let queried_nodes = 2;
        let topology_results = vec![
            get_view(&ViewType::SingleNodeViewMissingSlots),
            get_view(&ViewType::TwoNodesViewMissingSlots),
            get_view(&ViewType::SingleNodeViewMissingSlots),
        ];
        let (topology_view, _) = calculate_topology(
            topology_results,
            1,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let node_1 = get_node_addr("node1", 6379);
        let expected: Vec<&SlotAddrs> = vec![&node_1];
        assert_eq!(res, expected);
    }

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
}
