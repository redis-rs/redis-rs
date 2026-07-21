use std::{collections::HashSet, sync::Arc};

use crate::{
    ErrorKind, RedisError, RedisResult,
    cluster_handling::{
        NodeAddress,
        read_routing::{ReadCandidates, ReadRoutingStrategy, Replicas},
        slot_map::{SLOT_SIZE, SlotMap, SlotRange},
    },
    cluster_routing::{Route, SlotAddr},
};

const UNASSIGNED_ROUTE: u16 = u16::MAX;

struct SlotRoute {
    primary: NodeAddress,
    replicas: Arc<[NodeAddress]>,
}

impl SlotRoute {
    fn address_for_route(
        &self,
        slot: u16,
        slot_addr: SlotAddr,
        strategy: Option<&dyn ReadRoutingStrategy>,
    ) -> Option<&NodeAddress> {
        let selected =
            match strategy {
                None => match slot_addr {
                    SlotAddr::Master | SlotAddr::ReplicaOptional => return Some(&self.primary),
                    SlotAddr::ReplicaRequired => {
                        return match Replicas::new(&self.replicas) {
                            Some(replicas) => Some(replicas.choose_random()),
                            None => Some(&self.primary),
                        };
                    }
                },
                Some(strategy) => match Replicas::new(&self.replicas) {
                    Some(replicas) => match slot_addr {
                        SlotAddr::Master => return Some(&self.primary),
                        SlotAddr::ReplicaOptional => strategy
                            .route_read(&ReadCandidates::any_node(slot, &self.primary, replicas)),
                        SlotAddr::ReplicaRequired => {
                            strategy.route_read(&ReadCandidates::replicas_only(slot, replicas))
                        }
                    },
                    None => return Some(&self.primary),
                },
            };

        self.replicas
            .iter()
            .find(|address| *address == selected)
            .or_else(|| (&self.primary == selected).then_some(&self.primary))
    }

    fn fallback_addresses(&self, slot_addr: SlotAddr) -> Vec<&NodeAddress> {
        match slot_addr {
            SlotAddr::Master => Vec::new(),
            SlotAddr::ReplicaOptional => self
                .replicas
                .iter()
                .chain(std::iter::once(&self.primary))
                .collect(),
            SlotAddr::ReplicaRequired => self.replicas.iter().collect(),
        }
    }
}

/// Immutable slot-to-address topology. Connection state deliberately remains
/// in the original locked map so this variant isolates dense lookup and atomic
/// topology publication from the `NodeHandle` reconnect experiment.
pub(super) struct RoutingTable {
    generation: u64,
    slot_to_route: Arc<[u16]>,
    routes: Arc<[SlotRoute]>,
    topology_addresses: Arc<HashSet<NodeAddress>>,
    slot_map: Arc<SlotMap>,
}

impl RoutingTable {
    pub(super) fn empty(generation: u64) -> Self {
        Self {
            generation,
            slot_to_route: vec![UNASSIGNED_ROUTE; SLOT_SIZE as usize].into(),
            routes: Vec::new().into(),
            topology_addresses: Arc::new(HashSet::new()),
            slot_map: Arc::new(SlotMap::new()),
        }
    }

    pub(super) fn from_slots(generation: u64, slots: Vec<SlotRange>) -> RedisResult<Self> {
        let mut slot_to_route = vec![UNASSIGNED_ROUTE; SLOT_SIZE as usize];
        let mut routes = Vec::with_capacity(slots.len());
        let mut topology_addresses = HashSet::new();

        for slot_range in &slots {
            if slot_range.start > slot_range.end || slot_range.start >= SLOT_SIZE {
                return Err(RedisError::from((
                    ErrorKind::Client,
                    "Cluster topology contains an invalid slot range",
                )));
            }

            topology_addresses.insert(slot_range.master.clone());
            topology_addresses.extend(slot_range.replicas.iter().cloned());
            let route_index = if routes.len() < UNASSIGNED_ROUTE as usize {
                routes.len() as u16
            } else {
                return Err(RedisError::from((
                    ErrorKind::Client,
                    "Cluster topology contains too many slot ranges",
                )));
            };
            let last_slot = slot_range.end.min(SLOT_SIZE - 1);
            for slot in slot_range.start..=last_slot {
                slot_to_route[slot as usize] = route_index;
            }
            routes.push(SlotRoute {
                primary: slot_range.master.clone(),
                replicas: slot_range.replicas.clone().into(),
            });
        }

        Ok(Self {
            generation,
            slot_to_route: slot_to_route.into(),
            routes: routes.into(),
            topology_addresses: Arc::new(topology_addresses),
            slot_map: Arc::new(SlotMap::from_slots(slots)),
        })
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn is_empty(&self) -> bool {
        self.topology_addresses.is_empty()
    }

    fn route(&self, slot: u16) -> Option<&SlotRoute> {
        let route = *self.slot_to_route.get(slot as usize)?;
        (route != UNASSIGNED_ROUTE)
            .then(|| self.routes.get(route as usize))
            .flatten()
    }

    pub(super) fn preferred_address(
        &self,
        route: &Route,
        strategy: Option<&dyn ReadRoutingStrategy>,
    ) -> Option<&NodeAddress> {
        self.route(route.slot())?
            .address_for_route(route.slot(), route.slot_addr(), strategy)
    }

    pub(super) fn fallback_addresses(&self, route: &Route) -> Vec<&NodeAddress> {
        self.route(route.slot())
            .map(|slot_route| slot_route.fallback_addresses(route.slot_addr()))
            .unwrap_or_default()
    }

    pub(super) fn topology_addresses(&self, only_primaries: bool) -> Vec<&NodeAddress> {
        if only_primaries {
            self.slot_map
                .addresses_for_all_primaries()
                .into_iter()
                .collect()
        } else {
            self.slot_map
                .addresses_for_all_nodes()
                .into_iter()
                .collect()
        }
    }

    pub(super) fn addresses_for_multi_slot<'a>(
        &'a self,
        routes: &'a [(Route, Vec<usize>)],
        strategy: Option<&dyn ReadRoutingStrategy>,
    ) -> Vec<Option<&'a NodeAddress>> {
        routes
            .iter()
            .map(|(route, _)| self.preferred_address(route, strategy))
            .collect()
    }

    pub(super) fn contains_topology_address(&self, address: &NodeAddress) -> bool {
        self.topology_addresses.contains(address)
    }

    pub(super) fn slot_map(&self) -> &SlotMap {
        &self.slot_map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_routing::Slot;

    fn addr(value: &str) -> NodeAddress {
        NodeAddress::try_from(value).unwrap()
    }

    fn route(slot: u16) -> Route {
        Route::with_slot(Slot::new(slot).unwrap(), SlotAddr::Master)
    }

    #[test]
    fn dense_lookup_covers_boundaries_and_holes() {
        let table = RoutingTable::from_slots(
            1,
            vec![
                SlotRange::new(0, 100, addr("node1:6379"), vec![]),
                SlotRange::new(102, 200, addr("node2:6379"), vec![]),
            ],
        )
        .unwrap();

        assert_eq!(
            table.preferred_address(&route(0), None),
            Some(&addr("node1:6379"))
        );
        assert_eq!(
            table.preferred_address(&route(100), None),
            Some(&addr("node1:6379"))
        );
        assert!(table.preferred_address(&route(101), None).is_none());
        assert_eq!(
            table.preferred_address(&route(102), None),
            Some(&addr("node2:6379"))
        );
    }
}
