use crate::cluster_async::ConnectionFuture;
use arcstr::ArcStr;
use futures::FutureExt;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::net::IpAddr;

use crate::cluster_routing::{Route, SlotAddr};
use crate::cluster_slotmap::{ReadFromReplicaStrategy, SlotMap, SlotMapValue};
use crate::cluster_topology::TopologyHash;

/// A struct that encapsulates a network connection along with its associated IP address.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ConnectionWithIp<Connection> {
    /// The actual connection
    pub conn: Connection,
    /// The IP associated with the connection
    pub ip: Option<IpAddr>,
}

impl<Connection> ConnectionWithIp<Connection>
where
    Connection: Clone + Send + 'static,
{
    /// Consumes the current instance and returns a new `ConnectionWithIp`
    /// where the connection is wrapped in a future.
    #[doc(hidden)]
    pub fn into_future(self) -> ConnectionWithIp<ConnectionFuture<Connection>> {
        ConnectionWithIp {
            conn: async { self.conn }.boxed().shared(),
            ip: self.ip,
        }
    }
}

impl<Connection> From<(Connection, Option<IpAddr>)> for ConnectionWithIp<Connection> {
    fn from(val: (Connection, Option<IpAddr>)) -> Self {
        ConnectionWithIp {
            conn: val.0,
            ip: val.1,
        }
    }
}

impl<Connection> From<ConnectionWithIp<Connection>> for (Connection, Option<IpAddr>) {
    fn from(val: ConnectionWithIp<Connection>) -> Self {
        (val.conn, val.ip)
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ClusterNode<Connection> {
    pub user_connection: ConnectionWithIp<Connection>,
    pub management_connection: Option<ConnectionWithIp<Connection>>,
}

impl<Connection> ClusterNode<Connection>
where
    Connection: Clone,
{
    pub fn new(
        user_connection: ConnectionWithIp<Connection>,
        management_connection: Option<ConnectionWithIp<Connection>>,
    ) -> Self {
        Self {
            user_connection,
            management_connection,
        }
    }

    pub(crate) fn get_connection(&self, conn_type: &ConnectionType) -> Connection {
        match conn_type {
            ConnectionType::User => self.user_connection.conn.clone(),
            ConnectionType::PreferManagement => self.management_connection.as_ref().map_or_else(
                || self.user_connection.conn.clone(),
                |management_conn| management_conn.conn.clone(),
            ),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]

pub(crate) enum ConnectionType {
    User,
    PreferManagement,
}

pub(crate) struct ConnectionsMap<Connection>(pub(crate) HashMap<ArcStr, ClusterNode<Connection>>);

impl<Connection> std::fmt::Display for ConnectionsMap<Connection> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (address, node) in self.0.iter() {
            match node.user_connection.ip {
                Some(ip) => writeln!(f, "{address} - {ip}")?,
                None => writeln!(f, "{address}")?,
            };
        }
        Ok(())
    }
}

pub(crate) struct ConnectionsContainer<Connection> {
    connection_map: HashMap<ArcStr, ClusterNode<Connection>>,
    pub(crate) slot_map: SlotMap,
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

pub(crate) type ConnectionAndAddress<Connection> = (ArcStr, Connection);

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
            connection_map: connection_map.0,
            slot_map,
            read_from_replica_strategy,
            topology_hash,
        }
    }

    /// Returns true if the address represents a known primary node.
    pub(crate) fn is_primary(&self, address: &ArcStr) -> bool {
        self.connection_for_address(address).is_some()
            && self
                .slot_map
                .values()
                .any(|slot_addrs| slot_addrs.primary.as_str() == address)
    }

    fn round_robin_read_from_replica(
        &self,
        slot_map_value: &SlotMapValue,
    ) -> Option<ConnectionAndAddress<Connection>> {
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

    fn lookup_route(&self, route: &Route) -> Option<ConnectionAndAddress<Connection>> {
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
    ) -> Option<ConnectionAndAddress<Connection>> {
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
    ) -> impl Iterator<Item = ConnectionAndAddress<Connection>> + '_ {
        self.connection_map
            .iter()
            .map(move |(address, node)| (address.clone(), node.user_connection.conn.clone()))
    }

    pub(crate) fn all_primary_connections(
        &self,
    ) -> impl Iterator<Item = ConnectionAndAddress<Connection>> + '_ {
        self.slot_map
            .addresses_for_all_primaries()
            .into_iter()
            .flat_map(|addr| self.connection_for_address(addr))
    }

    pub(crate) fn node_for_address(&self, address: &str) -> Option<ClusterNode<Connection>> {
        self.connection_map.get(address).cloned()
    }

    pub(crate) fn connection_for_address(
        &self,
        address: &str,
    ) -> Option<ConnectionAndAddress<Connection>> {
        self.connection_map
            .get_key_value(address)
            .map(|(address, conn)| (address.clone(), conn.user_connection.conn.clone()))
    }

    pub(crate) fn random_connections(
        &self,
        amount: usize,
        conn_type: ConnectionType,
    ) -> impl Iterator<Item = ConnectionAndAddress<Connection>> + '_ {
        self.connection_map
            .iter()
            .choose_multiple(&mut rand::thread_rng(), amount)
            .into_iter()
            .map(move |(address, node)| {
                let conn = node.get_connection(&conn_type);
                (address.clone(), conn)
            })
    }

    pub(crate) fn replace_or_add_connection_for_address(
        &mut self,
        address: impl Into<ArcStr>,
        node: ClusterNode<Connection>,
    ) -> ArcStr {
        let address = address.into();
        self.connection_map.insert(address.clone(), node);
        address
    }

    pub(crate) fn remove_node(&mut self, address: &ArcStr) -> Option<ClusterNode<Connection>> {
        self.connection_map.remove(address)
    }

    pub(crate) fn len(&self) -> usize {
        self.connection_map.len()
    }

    pub(crate) fn get_current_topology_hash(&self) -> TopologyHash {
        self.topology_hash
    }

    /// Returns true if the connections container contains no connections.
    pub(crate) fn is_empty(&self) -> bool {
        self.connection_map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::cluster_routing::Slot;

    use super::*;
    impl<Connection> ClusterNode<Connection>
    where
        Connection: Clone,
    {
        pub(crate) fn new_only_with_user_conn(user_connection: Connection) -> Self {
            let ip = None;
            Self {
                user_connection: (user_connection, ip).into(),
                management_connection: None,
            }
        }
    }
    fn remove_nodes(container: &mut ConnectionsContainer<usize>, addresss: &[&str]) {
        for address in addresss {
            container.remove_node(&(*address).into());
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
        connection: Option<ConnectionAndAddress<usize>>,
        expected_connections: &[usize],
    ) -> bool {
        let found = connection.unwrap().1;
        expected_connections.contains(&found)
    }
    fn create_cluster_node(
        connection: usize,
        use_management_connections: bool,
    ) -> ClusterNode<usize> {
        let ip = None;
        ClusterNode::new(
            (connection, ip).into(),
            if use_management_connections {
                Some((connection * 10, ip).into())
            } else {
                None
            },
        )
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
            "primary1".into(),
            create_cluster_node(1, use_management_connections),
        );
        connection_map.insert(
            "primary2".into(),
            create_cluster_node(2, use_management_connections),
        );
        connection_map.insert(
            "primary3".into(),
            create_cluster_node(3, use_management_connections),
        );
        connection_map.insert(
            "replica2-1".into(),
            create_cluster_node(21, use_management_connections),
        );
        connection_map.insert(
            "replica3-1".into(),
            create_cluster_node(31, use_management_connections),
        );
        connection_map.insert(
            "replica3-2".into(),
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
        container.remove_node(&"replica3-2".into());

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
        container.remove_node(&"primary1".into());

        assert!(container.connection_for_address("primary1").is_none());
    }

    #[test]
    fn get_connection_by_address_returns_added_connection() {
        let mut container = create_container();
        let address = container.replace_or_add_connection_for_address(
            "foobar",
            ClusterNode::new_only_with_user_conn(4),
        );

        assert_eq!(
            (address, 4),
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
        let address = container.replace_or_add_connection_for_address(
            "foobar",
            ClusterNode::new_only_with_user_conn(4),
        );
        let random_connections: Vec<_> = container
            .random_connections(1, ConnectionType::User)
            .collect();

        assert_eq!(vec![(address, 4)], random_connections);
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
            .random_connections(1000, ConnectionType::PreferManagement)
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
        container.remove_node(&"primary1".into());

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
        container.remove_node(&"primary1".into());

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

        container.remove_node(&"primary1".into());
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

        container.remove_node(&"foobar".into());
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

        let connection = container.remove_node(&"primary1".into());
        assert_eq!(connection, Some(ClusterNode::new_only_with_user_conn(1)));

        let non_connection = container.remove_node(&"foobar".into());
        assert_eq!(non_connection, None);
    }

    #[test]
    fn test_is_empty() {
        let mut container = create_container();

        assert!(!container.is_empty());
        container.remove_node(&"primary1".into());
        assert!(!container.is_empty());
        container.remove_node(&"primary2".into());
        container.remove_node(&"primary3".into());
        assert!(!container.is_empty());

        container.remove_node(&"replica2-1".into());
        container.remove_node(&"replica3-1".into());
        assert!(!container.is_empty());

        container.remove_node(&"replica3-2".into());
        assert!(container.is_empty());
    }

    #[test]
    fn is_primary_returns_true_for_known_primary() {
        let container = create_container();

        assert!(container.is_primary(&"primary1".into()));
    }

    #[test]
    fn is_primary_returns_false_for_known_replica() {
        let container = create_container();

        assert!(!container.is_primary(&"replica2-1".into()));
    }

    #[test]
    fn is_primary_returns_false_for_removed_node() {
        let mut container = create_container();
        let address = "primary1".into();
        container.remove_node(&address);

        assert!(!container.is_primary(&address));
    }
}
