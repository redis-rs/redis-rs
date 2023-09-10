use std::collections::HashMap;

use rand::seq::IteratorRandom;

use crate::cluster_routing::{MultipleNodeRoutingInfo, Route, SlotAddr};
use crate::cluster_topology::SlotMap;

type IdentifierType = String;

/// This opaque type allows us to change the way that the connections are organized
/// internally without refactoring the calling code.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) struct Identifier(IdentifierType);

pub(crate) struct ConnectionsContainer<Connection> {
    connection_map: HashMap<Identifier, Connection>,
    slot_map: SlotMap,
}

impl<Connection> Default for ConnectionsContainer<Connection> {
    fn default() -> Self {
        Self {
            connection_map: Default::default(),
            slot_map: Default::default(),
        }
    }
}

pub(crate) type ConnectionAndIdentifier<Connection> = (Identifier, Connection);

impl<Connection> ConnectionsContainer<Connection>
where
    Connection: Clone,
{
    pub(crate) fn new(slot_map: SlotMap, connection_map: HashMap<String, Connection>) -> Self {
        Self {
            connection_map: connection_map
                .into_iter()
                .map(|(address, connection)| (Identifier(address), connection))
                .collect(),
            slot_map,
        }
    }

    fn lookup(&self, route: &Route) -> Option<ConnectionAndIdentifier<Connection>> {
        let address = self.slot_map.slot_addr_for_route(route)?;
        self.connection_for_address(address)
    }

    pub(crate) fn connection_for_route(
        &self,
        route: &Route,
    ) -> Option<ConnectionAndIdentifier<Connection>> {
        self.lookup(route).or_else(|| {
            if route.slot_addr() == SlotAddr::Replica {
                self.lookup(&Route::new(route.slot(), SlotAddr::Master))
            } else {
                None
            }
        })
    }

    pub(crate) fn all_node_connections(
        &self,
    ) -> impl Iterator<Item = ConnectionAndIdentifier<Connection>> + '_ {
        self.connection_map
            .iter()
            .map(|(identifier, connection)| (identifier.clone(), connection.clone()))
    }

    pub(crate) fn all_primary_connections(
        &self,
    ) -> impl Iterator<Item = ConnectionAndIdentifier<Connection>> + '_ {
        self.slot_map
            .addresses_for_multi_routing(&MultipleNodeRoutingInfo::AllMasters) // TODO - this involves allocating a hash set and a vec. can this be avoided?
            .into_iter()
            .flat_map(|addr| self.connection_for_address(addr))
    }

    pub(crate) fn connection_for_identifier(&self, identifier: &Identifier) -> Option<Connection> {
        let conn = self.connection_map.get(identifier)?;
        Some(conn.clone())
    }

    pub(crate) fn connection_for_address(
        &self,
        address: &str,
    ) -> Option<ConnectionAndIdentifier<Connection>> {
        let identifier = Identifier(address.into());
        let connection = self.connection_for_identifier(&identifier)?;
        Some((identifier, connection))
    }

    pub(crate) fn address_for_identifier<'a, 'b: 'a>(
        &'a self,
        identifier: &'a Identifier,
    ) -> Option<String> {
        if self.connection_map.contains_key(identifier) {
            Some(identifier.0.clone())
        } else {
            None
        }
    }

    pub(crate) fn random_connections(
        &self,
        amount: usize,
    ) -> impl Iterator<Item = ConnectionAndIdentifier<Connection>> + '_ {
        self.connection_map
            .iter()
            .choose_multiple(&mut rand::thread_rng(), amount)
            .into_iter()
            .map(|(identifier, connection)| (identifier.clone(), connection.clone()))
    }

    pub(crate) fn replace_or_add_connection_for_address(
        &mut self,
        address: impl Into<String>,
        connection: Connection,
    ) -> Identifier {
        let identifier = Identifier(address.into());
        self.connection_map.insert(identifier.clone(), connection);
        identifier
    }

    pub(crate) fn remove_connection(&mut self, identifier: &Identifier) -> Option<Connection> {
        self.connection_map.remove(identifier)
    }

    pub(crate) fn len(&self) -> usize {
        self.connection_map.len()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::cluster_routing::{Slot, SlotAddr};

    use super::*;

    fn remove_connections(container: &mut ConnectionsContainer<usize>, identifiers: &[&str]) {
        for identifier in identifiers {
            container.remove_connection(&Identifier((*identifier).into()));
        }
    }

    fn remove_all_connections(container: &mut ConnectionsContainer<usize>) {
        remove_connections(
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

    fn one_of(
        connection: Option<ConnectionAndIdentifier<usize>>,
        expected_connections: &[usize],
    ) -> bool {
        let found = connection.unwrap().1;
        expected_connections.contains(&found)
    }

    fn create_container() -> ConnectionsContainer<usize> {
        let slot_map = SlotMap::new(vec![
            Slot::new(1, 1000, "primary1".to_owned(), Vec::new()),
            Slot::new(
                1002,
                2000,
                "primary2".to_owned(),
                vec!["replica2-1".to_owned()],
            ),
            Slot::new(
                2001,
                3000,
                "primary3".to_owned(),
                vec!["replica3-1".to_owned(), "replica3-2".to_owned()],
            ),
        ]);
        let mut connection_map = HashMap::new();
        connection_map.insert(Identifier("primary1".into()), 1);
        connection_map.insert(Identifier("primary2".into()), 2);
        connection_map.insert(Identifier("primary3".into()), 3);
        connection_map.insert(Identifier("replica2-1".into()), 21);
        connection_map.insert(Identifier("replica3-1".into()), 31);
        connection_map.insert(Identifier("replica3-2".into()), 32);

        ConnectionsContainer {
            slot_map,
            connection_map,
        }
    }

    #[test]
    fn get_connection_for_primary_route() {
        let container = create_container();

        assert!(container
            .connection_for_route(&Route::new(0, SlotAddr::Master))
            .is_none());

        assert_eq!(
            1,
            container
                .connection_for_route(&Route::new(500, SlotAddr::Master))
                .unwrap()
                .1
        );

        assert_eq!(
            1,
            container
                .connection_for_route(&Route::new(1000, SlotAddr::Master))
                .unwrap()
                .1
        );

        assert!(container
            .connection_for_route(&Route::new(1001, SlotAddr::Master))
            .is_none());

        assert_eq!(
            2,
            container
                .connection_for_route(&Route::new(1002, SlotAddr::Master))
                .unwrap()
                .1
        );

        assert_eq!(
            2,
            container
                .connection_for_route(&Route::new(1500, SlotAddr::Master))
                .unwrap()
                .1
        );

        assert_eq!(
            3,
            container
                .connection_for_route(&Route::new(2001, SlotAddr::Master))
                .unwrap()
                .1
        );
    }

    #[test]
    fn get_connection_for_replica_route() {
        let container = create_container();

        assert!(container
            .connection_for_route(&Route::new(1001, SlotAddr::Replica))
            .is_none());

        assert_eq!(
            21,
            container
                .connection_for_route(&Route::new(1002, SlotAddr::Replica))
                .unwrap()
                .1
        );

        assert_eq!(
            21,
            container
                .connection_for_route(&Route::new(1500, SlotAddr::Replica))
                .unwrap()
                .1
        );

        assert!(one_of(
            container.connection_for_route(&Route::new(2001, SlotAddr::Replica)),
            &[31, 32],
        ));
    }

    #[test]
    fn get_primary_connection_for_replica_route_if_no_replicas_were_added() {
        let container = create_container();

        assert!(container
            .connection_for_route(&Route::new(0, SlotAddr::Replica))
            .is_none());

        assert_eq!(
            1,
            container
                .connection_for_route(&Route::new(500, SlotAddr::Replica))
                .unwrap()
                .1
        );

        assert_eq!(
            1,
            container
                .connection_for_route(&Route::new(1000, SlotAddr::Replica))
                .unwrap()
                .1
        );
    }

    // TODO - this is waiting for
    // #[test]
    // fn get_replica_connection_for_replica_route_if_some_but_not_all_replicas_were_removed() {
    //     let mut container = create_container();
    //     container.remove_connection(&Identifier("replica3-2".into()));

    //     assert_eq!(
    //         31,
    //         container
    //             .connection_for_route(&Route::new(2001, SlotAddr::Replica))
    //             .unwrap()
    //             .1
    //     );
    // }

    #[test]
    fn get_primary_connection_for_replica_route_if_all_replicas_were_removed() {
        let mut container = create_container();
        remove_connections(&mut container, &["replica2-1", "replica3-1", "replica3-2"]);

        assert_eq!(
            2,
            container
                .connection_for_route(&Route::new(1002, SlotAddr::Replica))
                .unwrap()
                .1
        );

        assert_eq!(
            2,
            container
                .connection_for_route(&Route::new(1500, SlotAddr::Replica))
                .unwrap()
                .1
        );

        assert_eq!(
            3,
            container
                .connection_for_route(&Route::new(2001, SlotAddr::Replica))
                .unwrap()
                .1
        );
    }

    #[test]
    fn get_connection_by_address() {
        let container = create_container();

        assert!(container.connection_for_address("foobar").is_none());

        assert_eq!(1, container.connection_for_address("primary1").unwrap().1);
        assert_eq!(2, container.connection_for_address("primary2").unwrap().1);
        assert_eq!(3, container.connection_for_address("primary3").unwrap().1);
        assert_eq!(
            21,
            container.connection_for_address("replica2-1").unwrap().1
        );
        assert_eq!(
            31,
            container.connection_for_address("replica3-1").unwrap().1
        );
        assert_eq!(
            32,
            container.connection_for_address("replica3-2").unwrap().1
        );
    }

    #[test]
    fn get_connection_by_address_returns_none_if_connection_was_removed() {
        let mut container = create_container();
        container.remove_connection(&Identifier("primary1".into()));

        assert!(container.connection_for_address("primary1").is_none());
    }

    #[test]
    fn get_connection_by_identifier_returns_none_if_connection_was_removed() {
        let mut container = create_container();
        let identifier = Identifier("primary1".into());
        container.remove_connection(&identifier.clone());

        assert!(container.connection_for_identifier(&identifier).is_none());
    }

    #[test]
    fn get_connection_by_address_returns_added_connection() {
        let mut container = create_container();
        let identifier = container.replace_or_add_connection_for_address("foobar", 4);

        assert_eq!(4, container.connection_for_identifier(&identifier).unwrap());
        assert_eq!(
            (identifier, 4),
            container.connection_for_address("foobar").unwrap()
        );
    }

    #[test]
    fn get_random_connections_without_repetitions() {
        let container = create_container();

        let random_connections: HashSet<_> =
            container.random_connections(3).map(|pair| pair.1).collect();

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
        let identifier = container.replace_or_add_connection_for_address("foobar", 4);
        let random_connections: Vec<_> = container.random_connections(1).collect();

        assert_eq!(vec![(identifier, 4)], random_connections);
    }

    #[test]
    fn get_random_connections_is_bound_by_the_number_of_connections_in_the_map() {
        let container = create_container();
        let mut random_connections: Vec<_> = container
            .random_connections(1000)
            .map(|pair| pair.1)
            .collect();
        random_connections.sort();

        assert_eq!(random_connections, vec![1, 2, 3, 21, 31, 32]);
    }

    #[test]
    fn get_all_nodes() {
        let container = create_container();
        let mut connections: Vec<_> = container
            .all_node_connections()
            .map(|conn| conn.1)
            .collect();
        connections.sort();

        assert_eq!(vec![1, 2, 3, 21, 31, 32], connections);
    }

    #[test]
    fn get_all_nodes_returns_added_connection() {
        let mut container = create_container();
        container.replace_or_add_connection_for_address("foobar", 4);

        let mut connections: Vec<_> = container
            .all_node_connections()
            .map(|conn| conn.1)
            .collect();
        connections.sort();

        assert_eq!(vec![1, 2, 3, 4, 21, 31, 32], connections);
    }

    #[test]
    fn get_all_nodes_does_not_return_removed_connection() {
        let mut container = create_container();
        container.remove_connection(&Identifier("primary1".into()));

        let mut connections: Vec<_> = container
            .all_node_connections()
            .map(|conn| conn.1)
            .collect();
        connections.sort();

        assert_eq!(vec![2, 3, 21, 31, 32], connections);
    }

    #[test]
    fn get_all_primaries() {
        let container = create_container();

        let mut connections: Vec<_> = container
            .all_primary_connections()
            .map(|conn| conn.1)
            .collect();
        connections.sort();

        assert_eq!(vec![1, 2, 3], connections);
    }

    #[test]
    fn get_all_primaries_does_not_return_removed_connection() {
        let mut container = create_container();
        container.remove_connection(&Identifier("primary1".into()));

        let mut connections: Vec<_> = container
            .all_primary_connections()
            .map(|conn| conn.1)
            .collect();
        connections.sort();

        assert_eq!(vec![2, 3], connections);
    }

    #[test]
    fn len_is_adjusted_on_removals_and_additions() {
        let mut container = create_container();

        assert_eq!(container.len(), 6);

        container.remove_connection(&Identifier("primary1".into()));
        assert_eq!(container.len(), 5);

        container.replace_or_add_connection_for_address("foobar", 4);
        assert_eq!(container.len(), 6);
    }

    #[test]
    fn len_is_not_adjusted_on_removals_of_nonexisting_connections_or_additions_of_existing_connections(
    ) {
        let mut container = create_container();

        assert_eq!(container.len(), 6);

        container.remove_connection(&Identifier("foobar".into()));
        assert_eq!(container.len(), 6);

        container.replace_or_add_connection_for_address("primary1", 4);
        assert_eq!(container.len(), 6);
    }

    #[test]
    fn remove_connection_returns_connection_if_it_exists() {
        let mut container = create_container();

        let connection = container.remove_connection(&Identifier("primary1".into()));
        assert_eq!(connection, Some(1));

        let non_connection = container.remove_connection(&Identifier("foobar".into()));
        assert_eq!(non_connection, None);
    }

    #[test]
    fn address_for_identifier_returns_address_if_it_exists() {
        let container = create_container();

        let address = container.address_for_identifier(&Identifier("primary1".into()));
        assert_eq!(address, Some("primary1".to_string()));

        let no_address = container.address_for_identifier(&Identifier("foobar".into()));
        assert_eq!(no_address, None);
    }
}
