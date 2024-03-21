use std::sync::Arc;

use rand::seq::IteratorRandom;

use crate::cluster_slotmap::{Route, SlotMap};
use crate::types::HashMap;

// TODO - Do we want a more memory efficient identifier, such as `arcstr`?
pub(crate) type ConnectionsHashMap<Connection> = HashMap<Arc<str>, Connection>;

pub(crate) struct ConnectionsMap<Connection> {
    connection_map: ConnectionsHashMap<Connection>,
    slot_map: SlotMap,
}

impl<Connection> Default for ConnectionsMap<Connection> {
    fn default() -> Self {
        Self {
            connection_map: Default::default(),
            slot_map: Default::default(),
        }
    }
}

impl<Connection> ConnectionsMap<Connection> {
    pub(crate) fn new(slot_map: SlotMap, connection_map: ConnectionsHashMap<Connection>) -> Self {
        Self {
            connection_map,
            slot_map,
        }
    }

    pub(crate) fn connection_for_route(&self, route: &Route) -> Option<(Arc<str>, &Connection)> {
        self.address_for_route(route)
            .and_then(|addr| self.connection_for_address(&addr).map(|conn| (addr, conn)))
    }

    pub(crate) fn address_for_route(&self, route: &Route) -> Option<Arc<str>> {
        self.slot_map.slot_addr_for_route(route)
    }

    pub(crate) fn all_nodes(&self) -> impl Iterator<Item = (&Arc<str>, Option<&Connection>)> + '_ {
        let mut addresses = self.slot_map.addresses_for_all_nodes();

        // In this situation, some additional connection was added after the slotmap was updated.
        if addresses.len() != self.connection_map.len() {
            for address in self.connection_map.keys() {
                addresses.insert(address);
            }
        }

        addresses.into_iter().map(|addr| {
            let conn = self.connection_for_address(addr);
            (addr, conn)
        })
    }

    pub(crate) fn all_primaries(
        &self,
    ) -> impl Iterator<Item = (&Arc<str>, Option<&Connection>)> + '_ {
        self.slot_map
            .addresses_for_all_primaries()
            .into_iter()
            .map(|addr| (addr, self.connection_for_address(addr)))
    }

    pub(crate) fn connection_for_address(&self, address: &str) -> Option<&Connection> {
        self.connection_map.get(address)
    }

    pub(crate) fn random_connections(
        &self,
        amount: usize,
    ) -> impl Iterator<Item = (Arc<str>, &Connection)> + '_ {
        self.connection_map
            .iter()
            .choose_multiple(&mut rand::thread_rng(), amount)
            .into_iter()
            .map(|(addr, conn)| (addr.clone(), conn))
    }

    pub(crate) fn replace_or_add_connection_for_address(
        &mut self,
        address: Arc<str>,
        conn: Connection,
    ) {
        self.connection_map.insert(address, conn);
    }

    pub(crate) fn remove_connection(&mut self, address: &str) -> Option<Connection> {
        self.connection_map.remove(address)
    }

    /// Returns true if the connections container contains no connections.
    pub(crate) fn is_empty(&self) -> bool {
        self.connection_map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    // For the sake of testing, we use `usize` as a connection type, because
    // `ConnectionsMap` doesn't contain logic that assumes that its `Connection`
    // type is an actual connection - the map is just a container.
    use std::collections::HashSet;

    use crate::cluster_slotmap::{Slot, SlotAddr};

    use super::*;

    fn remove_nodes(container: &mut ConnectionsMap<usize>, identifiers: &[&str]) {
        for identifier in identifiers {
            container.remove_connection(identifier);
        }
    }

    fn remove_all_connections(container: &mut ConnectionsMap<usize>) {
        remove_nodes(
            container,
            &[
                "primary1",
                "primary2",
                "primary3",
                "replica2-1",
                "replica3-1",
                "replica3-2",
            ],
        );
    }

    fn create_container_with_strategy(strategy: bool) -> ConnectionsMap<usize> {
        let slot_map = SlotMap::from_slots(
            vec![
                Slot::new(1, 1000, Arc::from("primary1"), Vec::new()),
                Slot::new(
                    1002,
                    2000,
                    Arc::from("primary2"),
                    vec![Arc::from("replica2-1")],
                ),
                Slot::new(
                    2001,
                    3000,
                    Arc::from("primary3"),
                    vec![Arc::from("replica3-1"), Arc::from("replica3-2")],
                ),
            ],
            strategy,
        );
        let mut connection_map = HashMap::new();
        connection_map.insert(Arc::from("primary1"), 1);
        connection_map.insert(Arc::from("primary2"), 2);
        connection_map.insert(Arc::from("primary3"), 3);
        connection_map.insert(Arc::from("replica2-1"), 21);
        connection_map.insert(Arc::from("replica3-1"), 31);
        connection_map.insert(Arc::from("replica3-2"), 32);

        ConnectionsMap {
            slot_map,
            connection_map,
        }
    }

    fn create_container() -> ConnectionsMap<usize> {
        create_container_with_strategy(true)
    }

    #[test]
    fn get_connection_for_primary_route() {
        let container = create_container();

        assert!(container
            .address_for_route(&Route::new(0, SlotAddr::Master))
            .is_none());

        assert_eq!(
            Arc::from("primary1"),
            container
                .address_for_route(&Route::new(500, SlotAddr::Master))
                .unwrap()
        );

        assert_eq!(
            Arc::from("primary1"),
            container
                .address_for_route(&Route::new(1000, SlotAddr::Master))
                .unwrap()
        );

        assert!(container
            .address_for_route(&Route::new(1001, SlotAddr::Master))
            .is_none());

        assert_eq!(
            Arc::from("primary2"),
            container
                .address_for_route(&Route::new(1002, SlotAddr::Master))
                .unwrap()
        );

        assert_eq!(
            Arc::from("primary2"),
            container
                .address_for_route(&Route::new(1500, SlotAddr::Master))
                .unwrap()
        );

        assert_eq!(
            Arc::from("primary3"),
            container
                .address_for_route(&Route::new(2001, SlotAddr::Master))
                .unwrap()
        );
    }

    #[test]
    fn get_connection_for_replica_route() {
        let container = create_container();

        assert!(container
            .address_for_route(&Route::new(1001, SlotAddr::ReplicaOptional))
            .is_none());

        assert_eq!(
            Arc::from("replica2-1"),
            container
                .address_for_route(&Route::new(1002, SlotAddr::ReplicaOptional))
                .unwrap()
        );

        assert_eq!(
            Arc::from("replica2-1"),
            container
                .address_for_route(&Route::new(1500, SlotAddr::ReplicaOptional))
                .unwrap()
        );

        assert!([Arc::from("replica3-1"), Arc::from("replica3-2")].contains(
            &container
                .address_for_route(&Route::new(2001, SlotAddr::ReplicaOptional))
                .unwrap()
        ));
    }

    #[test]
    fn get_primary_connection_for_replica_route_if_no_replicas_were_added() {
        let container = create_container();

        assert!(container
            .address_for_route(&Route::new(0, SlotAddr::ReplicaOptional))
            .is_none());

        assert_eq!(
            Arc::from("primary1"),
            container
                .address_for_route(&Route::new(500, SlotAddr::ReplicaOptional))
                .unwrap()
        );

        assert_eq!(
            Arc::from("primary1"),
            container
                .address_for_route(&Route::new(1000, SlotAddr::ReplicaOptional))
                .unwrap()
        );
    }

    #[test]
    #[ignore] //TODO - enable once round robin read from replica is enabled.
    fn get_replica_connection_for_replica_route_if_some_but_not_all_replicas_were_removed() {
        let mut container = create_container();
        container.remove_connection("replica3-2");

        assert_eq!(
            Arc::from("replica3-1"),
            container
                .address_for_route(&Route::new(2001, SlotAddr::ReplicaRequired))
                .unwrap()
        );
    }

    #[test]
    fn get_replica_connection_for_replica_route_if_replica_is_required_even_if_strategy_is_always_from_primary(
    ) {
        let container = create_container_with_strategy(false);

        assert!([Arc::from("replica3-1"), Arc::from("replica3-2")].contains(
            &container
                .address_for_route(&Route::new(2001, SlotAddr::ReplicaRequired))
                .unwrap()
        ));
    }

    #[test]
    #[ignore] //TODO - enable once round robin read from replica is enabled.
    fn get_primary_connection_for_replica_route_if_all_replicas_were_removed() {
        let mut container = create_container();
        remove_nodes(&mut container, &["replica2-1", "replica3-1", "replica3-2"]);

        assert_eq!(
            Arc::from("primary2"),
            container
                .address_for_route(&Route::new(1002, SlotAddr::ReplicaOptional))
                .unwrap()
        );

        assert_eq!(
            Arc::from("primary2"),
            container
                .address_for_route(&Route::new(1500, SlotAddr::ReplicaOptional))
                .unwrap()
        );

        assert_eq!(
            Arc::from("primary3"),
            container
                .address_for_route(&Route::new(2001, SlotAddr::ReplicaOptional))
                .unwrap()
        );
    }

    #[test]
    fn get_connection_by_address() {
        let container = create_container();

        assert!(container.connection_for_address("foobar").is_none());

        assert_eq!(1, *container.connection_for_address("primary1").unwrap());
        assert_eq!(2, *container.connection_for_address("primary2").unwrap());
        assert_eq!(3, *container.connection_for_address("primary3").unwrap());
        assert_eq!(21, *container.connection_for_address("replica2-1").unwrap());
        assert_eq!(31, *container.connection_for_address("replica3-1").unwrap());
        assert_eq!(32, *container.connection_for_address("replica3-2").unwrap());
    }

    #[test]
    fn get_connection_by_address_returns_none_if_connection_was_removed() {
        let mut container = create_container();
        container.remove_connection("primary1");

        assert!(container.connection_for_address("primary1").is_none());
    }

    #[test]
    fn get_connection_by_address_returns_added_connection() {
        let mut container = create_container();
        container.replace_or_add_connection_for_address(Arc::from("foobar"), 4);

        assert_eq!(4, *container.connection_for_address("foobar").unwrap());
    }

    #[test]
    fn get_random_connections_without_repetitions() {
        let container = create_container();

        let random_connections: HashSet<_> = container
            .random_connections(3)
            .map(|pair| *pair.1)
            .collect();

        assert_eq!(random_connections.len(), 3);
        assert!(random_connections
            .iter()
            .all(|connection| [1, 2, 3, 21, 31, 32].contains(connection)));
    }

    #[test]
    fn get_random_connections_returns_none_if_all_connections_were_removed() {
        let mut container = create_container();
        remove_all_connections(&mut container);

        assert_eq!(0, container.random_connections(1).count());
    }

    #[test]
    fn get_random_connections_returns_added_connection() {
        let mut container = create_container();
        remove_all_connections(&mut container);
        container.replace_or_add_connection_for_address(Arc::from("foobar"), 4);
        let random_connections: Vec<_> = container
            .random_connections(1)
            .map(|pair| *pair.1)
            .collect();

        assert_eq!(vec![4], random_connections);
    }

    #[test]
    fn random_connections_is_bound_by_the_number_of_connections_in_the_map() {
        let container = create_container();
        let mut random_connections: Vec<_> = container
            .random_connections(1000)
            .map(|pair| *pair.1)
            .collect();
        random_connections.sort();

        assert_eq!(random_connections, vec![1, 2, 3, 21, 31, 32]);
    }

    #[test]
    fn all_nodes_returns_all_nodes() {
        let container = create_container();
        let mut connections: Vec<_> = container.all_nodes().map(|conn| *conn.1.unwrap()).collect();
        connections.sort();

        assert_eq!(vec![1, 2, 3, 21, 31, 32], connections);
    }

    #[test]
    fn all_nodes_returns_added_connection() {
        let mut container = create_container();
        container.replace_or_add_connection_for_address(Arc::from("foobar"), 4);

        let mut connections: Vec<_> = container.all_nodes().map(|conn| *conn.1.unwrap()).collect();
        connections.sort();

        assert_eq!(vec![1, 2, 3, 4, 21, 31, 32], connections);
    }

    #[test]
    fn all_nodes_does_returns_none_for_removed_connection() {
        let mut container = create_container();
        container.remove_connection("primary1");

        let mut connections: Vec<_> = container.all_nodes().map(|pair| pair.1.copied()).collect();
        connections.sort();

        assert_eq!(
            vec![None, Some(2), Some(3), Some(21), Some(31), Some(32)],
            connections
        );
    }

    #[test]
    fn all_primaries() {
        let container = create_container();

        let mut connections: Vec<_> = container
            .all_primaries()
            .map(|pair| *pair.1.unwrap())
            .collect();
        connections.sort();

        assert_eq!(vec![1, 2, 3], connections);
    }

    #[test]
    fn all_primaries_returns_removed_connection() {
        let mut container = create_container();
        container.remove_connection("primary1");

        let mut connections: Vec<_> = container
            .all_primaries()
            .map(|pair| pair.1.copied())
            .collect();
        connections.sort();

        assert_eq!(vec![None, Some(2), Some(3)], connections);
    }

    #[test]
    fn remove_node_returns_connection_if_it_exists() {
        let mut container = create_container();

        let connection = container.remove_connection("primary1");
        assert_eq!(connection, Some(1));

        let non_connection = container.remove_connection("foobar");
        assert_eq!(non_connection, None);
    }

    #[test]
    fn test_is_empty() {
        let mut container = create_container();

        assert!(!container.is_empty());
        container.remove_connection("primary1");
        assert!(!container.is_empty());
        container.remove_connection("primary2");
        container.remove_connection("primary3");
        assert!(!container.is_empty());

        container.remove_connection("replica2-1");
        container.remove_connection("replica3-1");
        assert!(!container.is_empty());

        container.remove_connection("replica3-2");
        assert!(container.is_empty());
    }
}
