use std::collections::HashMap;
use std::net::IpAddr;

use arcstr::ArcStr;
use rand::seq::IteratorRandom;

use crate::cluster_routing::{Route, SlotAddr};
use crate::cluster_topology::{ReadFromReplicaStrategy, SlotMap, SlotMapValue, TopologyHash};

type IdentifierType = ArcStr;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct ClusterNode<Connection> {
    pub user_connection: Connection,
    pub management_connection: Option<Connection>,
    pub ip: Option<IpAddr>,
}

impl<Connection> ClusterNode<Connection>
where
    Connection: Clone,
{
    pub(crate) fn new(
        user_connection: Connection,
        management_connection: Option<Connection>,
        ip: Option<IpAddr>,
    ) -> Self {
        Self {
            user_connection,
            management_connection,
            ip,
        }
    }

    pub(crate) fn get_connection(&self, conn_type: &ConnectionType) -> Connection {
        match conn_type {
            ConnectionType::User => self.user_connection.clone(),
            ConnectionType::_Management => self
                .management_connection
                .clone()
                .unwrap_or_else(|| self.user_connection.clone()),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]

pub(crate) enum ConnectionType {
    User,
    _Management,
}

/// This opaque type allows us to change the way that the connections are organized
/// internally without refactoring the calling code.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) struct Identifier(IdentifierType);

pub(crate) struct ConnectionsMap<Connection>(pub(crate) HashMap<ArcStr, ClusterNode<Connection>>);

impl<Connection> std::fmt::Display for ConnectionsMap<Connection> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (address, node) in self.0.iter() {
            match node.ip {
                Some(ip) => writeln!(f, "{address} - {ip}")?,
                None => writeln!(f, "{address}")?,
            };
        }
        Ok(())
    }
}

pub(crate) struct ConnectionsContainer<Connection> {
    connection_map: HashMap<Identifier, Option<ClusterNode<Connection>>>,
    slot_map: SlotMap,
    read_from_replica_strategy: ReadFromReplicaStrategy,
    topology_hash: TopologyHash,
}

impl<Connection> Default for ConnectionsContainer<Connection> {
    fn default() -> Self {
        Self {
            connection_map: Default::default(),
            slot_map: Default::default(),
            read_from_replica_strategy: ReadFromReplicaStrategy::AlwaysFromPrimary,
            topology_hash: 0,
        }
    }
}

pub(crate) type ConnectionAndIdentifier<Connection> = (Identifier, Connection);

impl<Connection> ConnectionsContainer<Connection>
where
    Connection: Clone,
{
    pub(crate) fn new(
        slot_map: SlotMap,
        connection_map: ConnectionsMap<Connection>,
        read_from_replica_strategy: ReadFromReplicaStrategy,
        topology_hash: TopologyHash,
    ) -> Self {
        Self {
            connection_map: connection_map
                .0
                .into_iter()
                .map(|(address, node)| (Identifier(address), Some(node)))
                .collect(),
            slot_map,
            read_from_replica_strategy,
            topology_hash,
        }
    }

    fn round_robin_read_from_replica(
        &self,
        slot_map_value: &SlotMapValue,
    ) -> Option<ConnectionAndIdentifier<Connection>> {
        let addrs = &slot_map_value.addrs;
        let initial_index = slot_map_value
            .latest_used_replica
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut check_count = 0;
        loop {
            check_count += 1;

            // Looped through all replicas, no connected replica was found.
            if check_count > addrs.replicas.len() {
                return self.connection_for_address(addrs.primary.as_str());
            }
            let index = (initial_index + check_count) % addrs.replicas.len();
            if let Some(connection) = self.connection_for_address(addrs.replicas[index].as_str()) {
                let _ = slot_map_value.latest_used_replica.compare_exchange_weak(
                    initial_index,
                    index,
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                );
                return Some(connection);
            }
        }
    }

    fn lookup_route(&self, route: &Route) -> Option<ConnectionAndIdentifier<Connection>> {
        let slot_map_value = self.slot_map.slot_value_for_route(route)?;
        let addrs = &slot_map_value.addrs;
        if addrs.replicas.is_empty() {
            return self.connection_for_address(addrs.primary.as_str());
        }

        match route.slot_addr() {
            SlotAddr::Master => self.connection_for_address(addrs.primary.as_str()),
            SlotAddr::ReplicaOptional => match self.read_from_replica_strategy {
                ReadFromReplicaStrategy::AlwaysFromPrimary => {
                    self.connection_for_address(addrs.primary.as_str())
                }
                ReadFromReplicaStrategy::RoundRobin => {
                    self.round_robin_read_from_replica(slot_map_value)
                }
            },
            SlotAddr::ReplicaRequired => self.round_robin_read_from_replica(slot_map_value),
        }
    }

    pub(crate) fn connection_for_route(
        &self,
        route: &Route,
    ) -> Option<ConnectionAndIdentifier<Connection>> {
        self.lookup_route(route).or_else(|| {
            if route.slot_addr() != SlotAddr::Master {
                self.lookup_route(&Route::new(route.slot(), SlotAddr::Master))
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
            .filter_map(move |(identifier, node)| {
                node.as_ref()
                    .map(|node| (identifier.clone(), node.user_connection.clone()))
            })
    }

    pub(crate) fn all_primary_connections(
        &self,
    ) -> impl Iterator<Item = ConnectionAndIdentifier<Connection>> + '_ {
        self.slot_map
            .addresses_for_all_primaries()
            .into_iter()
            .flat_map(|addr| self.connection_for_address(addr))
    }

    fn node_for_identifier(&self, identifier: &Identifier) -> Option<ClusterNode<Connection>> {
        let node = self.connection_map.get(identifier)?.as_ref()?;
        Some(node.clone())
    }

    pub(crate) fn node_for_address(&self, address: &str) -> Option<ClusterNode<Connection>> {
        let identifier = Identifier(address.into());
        let node = self.node_for_identifier(&identifier)?;
        Some(node.clone())
    }

    pub(crate) fn connection_for_identifier(&self, identifier: &Identifier) -> Option<Connection> {
        let node = self.connection_map.get(identifier)?.as_ref()?;
        Some(node.user_connection.clone())
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
    ) -> Option<ArcStr> {
        if self.connection_map.contains_key(identifier) {
            Some(identifier.0.clone())
        } else {
            None
        }
    }

    pub(crate) fn random_connections(
        &self,
        amount: usize,
        conn_type: ConnectionType,
    ) -> impl Iterator<Item = ConnectionAndIdentifier<Connection>> + '_ {
        self.connection_map
            .iter()
            .filter_map(|(identifier, connection)| {
                connection
                    .as_ref()
                    .map(|connection| (identifier, connection))
            })
            .choose_multiple(&mut rand::thread_rng(), amount)
            .into_iter()
            .map(move |(identifier, node)| {
                let conn = node.get_connection(&conn_type);
                (identifier.clone(), conn)
            })
    }

    pub(crate) fn replace_or_add_connection_for_address(
        &mut self,
        address: impl Into<ArcStr>,
        node: ClusterNode<Connection>,
    ) -> Identifier {
        let identifier = Identifier(address.into());
        self.connection_map.insert(identifier.clone(), Some(node));
        identifier
    }

    pub(crate) fn remove_node(
        &mut self,
        identifier: &Identifier,
    ) -> Option<ClusterNode<Connection>> {
        self.connection_map.get_mut(identifier)?.take()
    }

    pub(crate) fn len(&self) -> usize {
        self.connection_map
            .iter()
            .filter(|(_, node)| node.is_some())
            .count()
    }

    pub(crate) fn get_current_topology_hash(&self) -> TopologyHash {
        self.topology_hash
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::cluster_routing::{Slot, SlotAddr};

    use super::*;
    impl<Connection> ClusterNode<Connection>
    where
        Connection: Clone,
    {
        pub(crate) fn new_only_with_user_conn(user_connection: Connection) -> Self {
            Self {
                user_connection,
                management_connection: None,
                ip: None,
            }
        }
    }
    fn remove_nodes(container: &mut ConnectionsContainer<usize>, identifiers: &[&str]) {
        for identifier in identifiers {
            container.remove_node(&Identifier((*identifier).into()));
        }
    }

    fn remove_all_connections(container: &mut ConnectionsContainer<usize>) {
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

    fn one_of(
        connection: Option<ConnectionAndIdentifier<usize>>,
        expected_connections: &[usize],
    ) -> bool {
        let found = connection.unwrap().1;
        expected_connections.contains(&found)
    }
    fn create_cluster_node(
        connection: usize,
        use_management_connections: bool,
    ) -> Option<ClusterNode<usize>> {
        Some(ClusterNode::new(
            connection,
            if use_management_connections {
                Some(connection * 10)
            } else {
                None
            },
            None,
        ))
    }

    fn create_container_with_strategy(
        stragey: ReadFromReplicaStrategy,
        use_management_connections: bool,
    ) -> ConnectionsContainer<usize> {
        let slot_map = SlotMap::new(
            vec![
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
            ],
            ReadFromReplicaStrategy::AlwaysFromPrimary, // this argument shouldn't matter, since we overload the RFR strategy.
        );
        let mut connection_map = HashMap::new();
        connection_map.insert(
            Identifier("primary1".into()),
            create_cluster_node(1, use_management_connections),
        );
        connection_map.insert(
            Identifier("primary2".into()),
            create_cluster_node(2, use_management_connections),
        );
        connection_map.insert(
            Identifier("primary3".into()),
            create_cluster_node(3, use_management_connections),
        );
        connection_map.insert(
            Identifier("replica2-1".into()),
            create_cluster_node(21, use_management_connections),
        );
        connection_map.insert(
            Identifier("replica3-1".into()),
            create_cluster_node(31, use_management_connections),
        );
        connection_map.insert(
            Identifier("replica3-2".into()),
            create_cluster_node(32, use_management_connections),
        );

        ConnectionsContainer {
            slot_map,
            connection_map,
            read_from_replica_strategy: stragey,
            topology_hash: 0,
        }
    }

    fn create_container() -> ConnectionsContainer<usize> {
        create_container_with_strategy(ReadFromReplicaStrategy::RoundRobin, false)
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
            .connection_for_route(&Route::new(1001, SlotAddr::ReplicaOptional))
            .is_none());

        assert_eq!(
            21,
            container
                .connection_for_route(&Route::new(1002, SlotAddr::ReplicaOptional))
                .unwrap()
                .1
        );

        assert_eq!(
            21,
            container
                .connection_for_route(&Route::new(1500, SlotAddr::ReplicaOptional))
                .unwrap()
                .1
        );

        assert!(one_of(
            container.connection_for_route(&Route::new(2001, SlotAddr::ReplicaOptional)),
            &[31, 32],
        ));
    }

    #[test]
    fn get_primary_connection_for_replica_route_if_no_replicas_were_added() {
        let container = create_container();

        assert!(container
            .connection_for_route(&Route::new(0, SlotAddr::ReplicaOptional))
            .is_none());

        assert_eq!(
            1,
            container
                .connection_for_route(&Route::new(500, SlotAddr::ReplicaOptional))
                .unwrap()
                .1
        );

        assert_eq!(
            1,
            container
                .connection_for_route(&Route::new(1000, SlotAddr::ReplicaOptional))
                .unwrap()
                .1
        );
    }

    #[test]
    fn get_replica_connection_for_replica_route_if_some_but_not_all_replicas_were_removed() {
        let mut container = create_container();
        container.remove_node(&Identifier("replica3-2".into()));

        assert_eq!(
            31,
            container
                .connection_for_route(&Route::new(2001, SlotAddr::ReplicaRequired))
                .unwrap()
                .1
        );
    }

    #[test]
    fn get_replica_connection_for_replica_route_if_replica_is_required_even_if_strategy_is_always_from_primary(
    ) {
        let container =
            create_container_with_strategy(ReadFromReplicaStrategy::AlwaysFromPrimary, false);

        assert!(one_of(
            container.connection_for_route(&Route::new(2001, SlotAddr::ReplicaRequired)),
            &[31, 32],
        ));
    }

    #[test]
    fn get_primary_connection_for_replica_route_if_all_replicas_were_removed() {
        let mut container = create_container();
        remove_nodes(&mut container, &["replica2-1", "replica3-1", "replica3-2"]);

        assert_eq!(
            2,
            container
                .connection_for_route(&Route::new(1002, SlotAddr::ReplicaOptional))
                .unwrap()
                .1
        );

        assert_eq!(
            2,
            container
                .connection_for_route(&Route::new(1500, SlotAddr::ReplicaOptional))
                .unwrap()
                .1
        );

        assert_eq!(
            3,
            container
                .connection_for_route(&Route::new(2001, SlotAddr::ReplicaOptional))
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
        container.remove_node(&Identifier("primary1".into()));

        assert!(container.connection_for_address("primary1").is_none());
    }

    #[test]
    fn get_connection_by_identifier_returns_none_if_connection_was_removed() {
        let mut container = create_container();
        let identifier = Identifier("primary1".into());
        container.remove_node(&identifier.clone());

        assert!(container.connection_for_identifier(&identifier).is_none());
    }

    #[test]
    fn get_connection_by_address_returns_added_connection() {
        let mut container = create_container();
        let identifier = container.replace_or_add_connection_for_address(
            "foobar",
            ClusterNode::new_only_with_user_conn(4),
        );

        assert_eq!(4, container.connection_for_identifier(&identifier).unwrap());
        assert_eq!(
            (identifier, 4),
            container.connection_for_address("foobar").unwrap()
        );
    }

    #[test]
    fn get_random_connections_without_repetitions() {
        let container = create_container();

        let random_connections: HashSet<_> = container
            .random_connections(3, ConnectionType::User)
            .map(|pair| pair.1)
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

        assert_eq!(
            0,
            container
                .random_connections(1, ConnectionType::User)
                .count()
        );
    }

    #[test]
    fn get_random_connections_returns_added_connection() {
        let mut container = create_container();
        remove_all_connections(&mut container);
        let identifier = container.replace_or_add_connection_for_address(
            "foobar",
            ClusterNode::new_only_with_user_conn(4),
        );
        let random_connections: Vec<_> = container
            .random_connections(1, ConnectionType::User)
            .collect();

        assert_eq!(vec![(identifier, 4)], random_connections);
    }

    #[test]
    fn get_random_connections_is_bound_by_the_number_of_connections_in_the_map() {
        let container = create_container();
        let mut random_connections: Vec<_> = container
            .random_connections(1000, ConnectionType::User)
            .map(|pair| pair.1)
            .collect();
        random_connections.sort();

        assert_eq!(random_connections, vec![1, 2, 3, 21, 31, 32]);
    }

    #[test]
    fn get_random_management_connections() {
        let container = create_container_with_strategy(ReadFromReplicaStrategy::RoundRobin, true);
        let mut random_connections: Vec<_> = container
            .random_connections(1000, ConnectionType::_Management)
            .map(|pair| pair.1)
            .collect();
        random_connections.sort();

        assert_eq!(random_connections, vec![10, 20, 30, 210, 310, 320]);
    }

    #[test]
    fn get_all_user_connections() {
        let container = create_container();
        let mut connections: Vec<_> = container
            .all_node_connections()
            .map(|conn| conn.1)
            .collect();
        connections.sort();

        assert_eq!(vec![1, 2, 3, 21, 31, 32], connections);
    }

    #[test]
    fn get_all_user_connections_returns_added_connection() {
        let mut container = create_container();
        container.replace_or_add_connection_for_address(
            "foobar",
            ClusterNode::new_only_with_user_conn(4),
        );

        let mut connections: Vec<_> = container
            .all_node_connections()
            .map(|conn| conn.1)
            .collect();
        connections.sort();

        assert_eq!(vec![1, 2, 3, 4, 21, 31, 32], connections);
    }

    #[test]
    fn get_all_user_connections_does_not_return_removed_connection() {
        let mut container = create_container();
        container.remove_node(&Identifier("primary1".into()));

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
        container.remove_node(&Identifier("primary1".into()));

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

        container.remove_node(&Identifier("primary1".into()));
        assert_eq!(container.len(), 5);

        container.replace_or_add_connection_for_address(
            "foobar",
            ClusterNode::new_only_with_user_conn(4),
        );
        assert_eq!(container.len(), 6);
    }

    #[test]
    fn len_is_not_adjusted_on_removals_of_nonexisting_connections_or_additions_of_existing_connections(
    ) {
        let mut container = create_container();

        assert_eq!(container.len(), 6);

        container.remove_node(&Identifier("foobar".into()));
        assert_eq!(container.len(), 6);

        container.replace_or_add_connection_for_address(
            "primary1",
            ClusterNode::new_only_with_user_conn(4),
        );
        assert_eq!(container.len(), 6);
    }

    #[test]
    fn remove_node_returns_connection_if_it_exists() {
        let mut container = create_container();

        let connection = container.remove_node(&Identifier("primary1".into()));
        assert_eq!(connection, Some(ClusterNode::new_only_with_user_conn(1)));

        let non_connection = container.remove_node(&Identifier("foobar".into()));
        assert_eq!(non_connection, None);
    }

    #[test]
    fn address_for_identifier_returns_address_if_it_existed_in_the_past() {
        let mut container = create_container();

        let address = container.address_for_identifier(&Identifier("primary1".into()));
        assert_eq!(address, Some("primary1".into()));

        container.remove_node(&Identifier("primary1".into()));

        let address = container.address_for_identifier(&Identifier("primary1".into()));
        assert_eq!(address, Some("primary1".into()));
    }

    #[test]
    fn address_for_identifier_returns_none_for_unknown_identifier() {
        let container = create_container();

        let address = container.address_for_identifier(&Identifier("foobar".into()));
        assert_eq!(address, None);
    }
}
