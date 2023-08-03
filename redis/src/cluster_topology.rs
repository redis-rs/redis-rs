//! This module provides the functionality to refresh and calculate the cluster topology for Redis Cluster.

use crate::cluster::get_connection_addr;
use crate::cluster_routing::MultipleNodeRoutingInfo;
use crate::cluster_routing::Route;
use crate::cluster_routing::SlotAddr;
use crate::cluster_routing::SlotAddrs;
use crate::{cluster::TlsMode, cluster_routing::Slot, ErrorKind, RedisError, RedisResult, Value};
use derivative::Derivative;
use log::trace;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// The default number of refersh topology retries
pub const DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES: usize = 3;
/// The default timeout for retrying topology refresh
pub const DEFAULT_REFRESH_SLOTS_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
/// The default initial interval for retrying topology refresh
pub const DEFAULT_REFRESH_SLOTS_RETRY_INITIAL_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) const SLOT_SIZE: u16 = 16384;

#[derive(Derivative)]
#[derivative(PartialEq, PartialOrd, Ord)]
#[derive(Debug, Eq)]
pub(crate) struct TopologyView {
    #[derivative(PartialOrd = "ignore", Ord = "ignore")]
    pub(crate) hash_value: u64,
    #[derivative(PartialEq = "ignore", PartialOrd = "ignore", Ord = "ignore")]
    pub(crate) topology_value: Value,
    #[derivative(PartialEq = "ignore")]
    pub(crate) nodes_count: u16,
}

#[derive(Debug, Default)]
pub(crate) struct SlotMap(pub(crate) BTreeMap<u16, SlotAddrs>);

impl SlotMap {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn fill_slots(&mut self, slots: Vec<Slot>) {
        for slot in slots {
            self.0.insert(slot.end(), SlotAddrs::from_slot(slot));
        }
    }

    pub fn slot_addr_for_route(&self, route: &Route, allow_replica: bool) -> Option<&str> {
        self.0
            .range(route.slot()..)
            .next()
            .map(|(_, slot_addrs)| slot_addrs.slot_addr(route.slot_addr(), allow_replica))
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn values(&self) -> std::collections::btree_map::Values<u16, SlotAddrs> {
        self.0.values()
    }

    fn all_unique_addresses(&self, only_primaries: bool) -> HashSet<&str> {
        let mut addresses = HashSet::new();
        for slot in self.values() {
            addresses.insert(slot.slot_addr(SlotAddr::Master, false));

            if !only_primaries {
                addresses.insert(slot.slot_addr(SlotAddr::Replica, true));
            }
        }
        addresses
    }

    pub fn addresses_for_multi_routing(
        &self,
        routing: &MultipleNodeRoutingInfo,
        allow_replica: bool,
    ) -> Vec<&str> {
        match routing {
            MultipleNodeRoutingInfo::AllNodes => self
                .all_unique_addresses(!allow_replica)
                .into_iter()
                .collect(),
            MultipleNodeRoutingInfo::AllMasters => {
                self.all_unique_addresses(true).into_iter().collect()
            }
            MultipleNodeRoutingInfo::MultiSlot(routes) => routes
                .iter()
                .flat_map(|(route, _)| self.slot_addr_for_route(route, allow_replica))
                .collect(),
        }
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

    if let Value::Bulk(items) = raw_slot_resp {
        let mut iter = items.iter();
        while let Some(Value::Bulk(item)) = iter.next() {
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
                    if let Value::Bulk(node) = node {
                        if node.len() < 2 {
                            return None;
                        }

                        let ip = if let Value::Data(ref ip) = node[0] {
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
                        Some(get_connection_addr(ip.into_owned(), port, tls).to_string())
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

pub(crate) fn build_slot_map(slot_map: &mut SlotMap, mut slots_data: Vec<Slot>) -> RedisResult<()> {
    slots_data.sort_by_key(|slot_data| slot_data.start());
    let last_slot = slots_data.iter().try_fold(0, |prev_end, slot_data| {
        if prev_end != slot_data.start() {
            return Err(RedisError::from((
                ErrorKind::ResponseError,
                "Slot refresh error.",
                format!(
                    "Received overlapping slots {} and {}..{}",
                    prev_end,
                    slot_data.start(),
                    slot_data.end()
                ),
            )));
        }
        Ok(slot_data.end() + 1)
    })?;

    if last_slot != SLOT_SIZE {
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error.",
            format!("Lacks the slots >= {last_slot}"),
        )));
    }
    slot_map.clear();
    slot_map.fill_slots(slots_data);
    trace!("{:?}", slot_map);
    Ok(())
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
) -> Result<SlotMap, RedisError> {
    if topology_views.is_empty() {
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: All CLUSTER SLOTS results are errors",
        )));
    }
    let mut hash_view_map = HashMap::new();
    let mut new_slots = SlotMap::new();
    for view in topology_views {
        let hash_value = calculate_hash(&view);
        let topology_entry = hash_view_map.entry(hash_value).or_insert(TopologyView {
            hash_value,
            topology_value: view,
            nodes_count: 0,
        });
        topology_entry.nodes_count += 1;
    }
    let mut most_frequent_topology: Option<&TopologyView> = None;
    let mut has_more_than_a_single_max = false;
    let vec_iter = hash_view_map.values();
    // Find the most frequent topology view
    for curr_view in vec_iter {
        let max_view = match most_frequent_topology {
            Some(view) => view,
            None => {
                most_frequent_topology = Some(curr_view);
                continue;
            }
        };
        match max_view.cmp(curr_view) {
            std::cmp::Ordering::Less => {
                most_frequent_topology = Some(curr_view);
                has_more_than_a_single_max = false;
            }
            std::cmp::Ordering::Equal => has_more_than_a_single_max = true,
            std::cmp::Ordering::Greater => continue,
        }
    }
    let most_frequent_topology = match most_frequent_topology {
        Some(view) => view,
        None => unreachable!(),
    };
    if has_more_than_a_single_max {
        // More than a single most frequent view was found
        // If we reached the last retry, or if we it's a 2-nodes cluster, we'll return all found topologies to be checked by the caller
        if curr_retry >= DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES || num_of_queried_nodes < 3 {
            for (idx, topology_view) in hash_view_map.values().enumerate() {
                match parse_slots(&topology_view.topology_value, tls_mode)
                    .and_then(|v| build_slot_map(&mut new_slots, v))
                {
                    Ok(_) => {
                        return Ok(new_slots);
                    }
                    Err(e) => {
                        // If it's the last view, raise the error
                        if idx == hash_view_map.len() - 1 {
                            return Err(RedisError::from((
                                ErrorKind::ResponseError,
                                "Failed to obtain a majority in topology views and couldn't find complete slot coverage in any of the views",
                                e.to_string()
                            )));
                        } else {
                            continue;
                        }
                    }
                }
            }
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
        parse_slots(&most_frequent_topology.topology_value, tls_mode)
            .and_then(|v| build_slot_map(&mut new_slots, v))?;
        Ok(new_slots)
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
            ViewType::SingleNodeViewFullCoverage => Value::Bulk(vec![Value::Bulk(vec![
                Value::Int(0_i64),
                Value::Int(16383_i64),
                Value::Bulk(vec![
                    Value::Data("node1".as_bytes().to_vec()),
                    Value::Int(6379_i64),
                ]),
            ])]),
            ViewType::SingleNodeViewMissingSlots => Value::Bulk(vec![Value::Bulk(vec![
                Value::Int(0_i64),
                Value::Int(4000_i64),
                Value::Bulk(vec![
                    Value::Data("node1".as_bytes().to_vec()),
                    Value::Int(6379_i64),
                ]),
            ])]),
            ViewType::TwoNodesViewFullCoverage => Value::Bulk(vec![
                Value::Bulk(vec![
                    Value::Int(0_i64),
                    Value::Int(4000_i64),
                    Value::Bulk(vec![
                        Value::Data("node1".as_bytes().to_vec()),
                        Value::Int(6379_i64),
                    ]),
                ]),
                Value::Bulk(vec![
                    Value::Int(4001_i64),
                    Value::Int(16383_i64),
                    Value::Bulk(vec![
                        Value::Data("node2".as_bytes().to_vec()),
                        Value::Int(6380_i64),
                    ]),
                ]),
            ]),
            ViewType::TwoNodesViewMissingSlots => Value::Bulk(vec![
                Value::Bulk(vec![
                    Value::Int(0_i64),
                    Value::Int(3000_i64),
                    Value::Bulk(vec![
                        Value::Data("node3".as_bytes().to_vec()),
                        Value::Int(6381_i64),
                    ]),
                ]),
                Value::Bulk(vec![
                    Value::Int(4001_i64),
                    Value::Int(16383_i64),
                    Value::Bulk(vec![
                        Value::Data("node4".as_bytes().to_vec()),
                        Value::Int(6382_i64),
                    ]),
                ]),
            ]),
        }
    }

    fn get_node_addr(name: &str, port: u16) -> SlotAddrs {
        SlotAddrs::new(format!("{name}:{port}"), None)
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
        let topology_view = calculate_topology(topology_results, 1, None, queried_nodes).unwrap();
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
        let topology_view = calculate_topology(topology_results, 1, None, queried_nodes);
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
        let topology_view = calculate_topology(topology_results, 3, None, queried_nodes).unwrap();
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
        let topology_view = calculate_topology(topology_results, 1, None, queried_nodes).unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let node_1 = get_node_addr("node1", 6379);
        let node_2 = get_node_addr("node2", 6380);
        let expected: Vec<&SlotAddrs> = vec![&node_1, &node_2];
        assert_eq!(res, expected);
    }

    #[test]
    fn test_topology_calculator_2_nodes_queried_no_majority_no_full_coverage_return_error() {
        //  2 nodes queried: No majority, no full slot coverage, should return error
        let queried_nodes = 2;
        let topology_results = vec![
            get_view(&ViewType::SingleNodeViewMissingSlots),
            get_view(&ViewType::TwoNodesViewMissingSlots),
        ];
        let topology_view_res = calculate_topology(topology_results, 1, None, queried_nodes);
        assert!(topology_view_res.is_err());
    }
}
