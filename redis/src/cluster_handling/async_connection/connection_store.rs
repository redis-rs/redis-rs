use std::collections::{HashMap, HashSet};

#[cfg(feature = "cluster-async-atomic-node-connections")]
use std::sync::Arc;

#[cfg(feature = "cluster-async-atomic-node-connections")]
use arc_swap::ArcSwap;
use rand::{rng, seq::IteratorRandom};
#[cfg(not(feature = "cluster-async-atomic-node-connections"))]
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::cluster_handling::{NodeAddress, client::ClusterParams};

#[cfg(feature = "cluster-async-atomic-node-connections")]
enum NodeConnectionState<C> {
    Connected(C),
    Reconnecting(Option<C>),
    Connecting,
}

#[cfg(feature = "cluster-async-atomic-node-connections")]
struct NodeHandle<C> {
    state: ArcSwap<NodeConnectionState<C>>,
}

#[cfg(feature = "cluster-async-atomic-node-connections")]
impl<C> NodeHandle<C> {
    fn connected(connection: C) -> Self {
        Self {
            state: ArcSwap::from_pointee(NodeConnectionState::Connected(connection)),
        }
    }

    fn connecting() -> Self {
        Self {
            state: ArcSwap::from_pointee(NodeConnectionState::Connecting),
        }
    }

    fn connected_connection(&self) -> Option<C>
    where
        C: Clone,
    {
        match &**self.state.load() {
            NodeConnectionState::Connected(connection) => Some(connection.clone()),
            NodeConnectionState::Reconnecting(_) | NodeConnectionState::Connecting => None,
        }
    }

    fn connection_for_repair(&self) -> Option<C>
    where
        C: Clone,
    {
        match &**self.state.load() {
            NodeConnectionState::Connected(connection) => Some(connection.clone()),
            NodeConnectionState::Reconnecting(connection) => connection.clone(),
            NodeConnectionState::Connecting => None,
        }
    }

    fn is_connected(&self) -> bool {
        matches!(&**self.state.load(), NodeConnectionState::Connected(_))
    }

    fn begin_reconnect(&self) -> bool
    where
        C: Clone,
    {
        loop {
            let current = self.state.load();
            let replacement = match &**current {
                NodeConnectionState::Connected(connection) => {
                    NodeConnectionState::Reconnecting(Some(connection.clone()))
                }
                NodeConnectionState::Connecting => NodeConnectionState::Reconnecting(None),
                NodeConnectionState::Reconnecting(_) => return false,
            };
            let previous = self.state.compare_and_swap(&current, Arc::new(replacement));
            if Arc::ptr_eq(&previous, &current) {
                return true;
            }
        }
    }

    fn install_connected(&self, connection: C) {
        self.state
            .store(Arc::new(NodeConnectionState::Connected(connection)));
    }

    fn install_repaired(&self, connection: C) -> bool {
        let current = self.state.load();
        if matches!(&**current, NodeConnectionState::Connected(_)) {
            return false;
        }
        let previous = self.state.compare_and_swap(
            &current,
            Arc::new(NodeConnectionState::Connected(connection)),
        );
        Arc::ptr_eq(&previous, &current)
    }
}

#[cfg(not(feature = "cluster-async-atomic-node-connections"))]
enum ConnState<C> {
    Connected(C),
    Reconnecting(Option<C>),
    Connecting,
}

#[cfg(not(feature = "cluster-async-atomic-node-connections"))]
impl<C: Clone> ConnState<C> {
    fn connected(&self) -> Option<C> {
        match self {
            Self::Connected(connection) => Some(connection.clone()),
            Self::Reconnecting(_) | Self::Connecting => None,
        }
    }

    fn connection_for_repair(&self) -> Option<C> {
        match self {
            Self::Connected(connection) => Some(connection.clone()),
            Self::Reconnecting(connection) => connection.clone(),
            Self::Connecting => None,
        }
    }
}

/// Connection state is deliberately independent of the immutable routing
/// table. The default backend preserves a locked connection map; the optional
/// experimental backend gives every address a stable atomically-swapped
/// handle. Keeping the API identical makes the reconnect-state A/B precise.
pub(super) struct ConnectionStore<C> {
    #[cfg(feature = "cluster-async-atomic-node-connections")]
    nodes: ArcSwap<HashMap<NodeAddress, Arc<NodeHandle<C>>>>,
    #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
    connections: RwLock<HashMap<NodeAddress, ConnState<C>>>,
}

impl<C: Clone> ConnectionStore<C> {
    pub(super) fn new() -> Self {
        Self {
            #[cfg(feature = "cluster-async-atomic-node-connections")]
            nodes: ArcSwap::from_pointee(HashMap::new()),
            #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
            connections: RwLock::new(HashMap::new()),
        }
    }

    #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
    async fn read<'a>(
        &'a self,
        params: &ClusterParams,
    ) -> RwLockReadGuard<'a, HashMap<NodeAddress, ConnState<C>>> {
        #[cfg(not(feature = "cluster-async-profiling"))]
        let _ = params;
        match self.connections.try_read() {
            Ok(guard) => {
                #[cfg(feature = "cluster-async-profiling")]
                params.diagnostics.connection_lock_acquired(None);
                guard
            }
            Err(_) => {
                #[cfg(feature = "cluster-async-profiling")]
                let started = std::time::Instant::now();
                let guard = self.connections.read().await;
                #[cfg(feature = "cluster-async-profiling")]
                params.diagnostics.connection_lock_acquired(Some(started));
                guard
            }
        }
    }

    #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
    async fn write<'a>(
        &'a self,
        params: &ClusterParams,
    ) -> RwLockWriteGuard<'a, HashMap<NodeAddress, ConnState<C>>> {
        #[cfg(not(feature = "cluster-async-profiling"))]
        let _ = params;
        match self.connections.try_write() {
            Ok(guard) => {
                #[cfg(feature = "cluster-async-profiling")]
                params.diagnostics.connection_lock_acquired(None);
                guard
            }
            Err(_) => {
                #[cfg(feature = "cluster-async-profiling")]
                let started = std::time::Instant::now();
                let guard = self.connections.write().await;
                #[cfg(feature = "cluster-async-profiling")]
                params.diagnostics.connection_lock_acquired(Some(started));
                guard
            }
        }
    }

    #[cfg(feature = "cluster-async-atomic-node-connections")]
    fn node(&self, address: &NodeAddress) -> Option<Arc<NodeHandle<C>>> {
        self.nodes.load().get(address).cloned()
    }

    #[cfg(feature = "cluster-async-atomic-node-connections")]
    fn ensure_node(&self, address: NodeAddress) -> Arc<NodeHandle<C>> {
        loop {
            let current = self.nodes.load();
            if let Some(node) = current.get(&address) {
                return Arc::clone(node);
            }
            let node = Arc::new(NodeHandle::connecting());
            let mut replacement = (**current).clone();
            replacement.insert(address.clone(), Arc::clone(&node));
            let previous = self.nodes.compare_and_swap(&current, Arc::new(replacement));
            if Arc::ptr_eq(&previous, &current) {
                return node;
            }
        }
    }

    pub(super) async fn replace_connected(
        &self,
        connections: HashMap<NodeAddress, C>,
        params: &ClusterParams,
    ) {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            let nodes = connections
                .into_iter()
                .map(|(address, connection)| {
                    let node = Arc::new(NodeHandle::connected(connection));
                    (address, node)
                })
                .collect();
            self.nodes.store(Arc::new(nodes));
            let _ = params;
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            let mut guard = self.write(params).await;
            *guard = connections
                .into_iter()
                .map(|(address, connection)| (address, ConnState::Connected(connection)))
                .collect();
        }
    }

    pub(super) async fn connected(
        &self,
        address: &NodeAddress,
        params: &ClusterParams,
    ) -> Option<C> {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            let _ = params;
            self.node(address)
                .and_then(|node| node.connected_connection())
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            self.read(params)
                .await
                .get(address)
                .and_then(ConnState::connected)
        }
    }

    pub(super) async fn connection_for_repair(
        &self,
        address: &NodeAddress,
        params: &ClusterParams,
    ) -> Option<C> {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            let _ = params;
            self.node(address)
                .and_then(|node| node.connection_for_repair())
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            self.read(params)
                .await
                .get(address)
                .and_then(ConnState::connection_for_repair)
        }
    }

    pub(super) async fn connected_for_addresses(
        &self,
        addresses: &[NodeAddress],
        include_reconnecting: bool,
        params: &ClusterParams,
    ) -> Vec<(NodeAddress, C)> {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            let _ = params;
            let nodes = self.nodes.load();
            addresses
                .iter()
                .filter_map(|address| {
                    let node = nodes.get(address)?;
                    let connection = if include_reconnecting {
                        node.connection_for_repair()
                    } else {
                        node.connected_connection()
                    }?;
                    Some((address.clone(), connection))
                })
                .collect()
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            let guard = self.read(params).await;
            addresses
                .iter()
                .filter_map(|address| {
                    let state = guard.get(address)?;
                    let connection = if include_reconnecting {
                        state.connection_for_repair()
                    } else {
                        state.connected()
                    }?;
                    Some((address.clone(), connection))
                })
                .collect()
        }
    }

    pub(super) async fn random_connected(
        &self,
        params: &ClusterParams,
    ) -> Option<(NodeAddress, C)> {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            let _ = params;
            self.nodes
                .load()
                .iter()
                .filter_map(|(address, node)| {
                    node.connected_connection()
                        .map(|connection| (address.clone(), connection))
                })
                .choose(&mut rng())
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            self.read(params)
                .await
                .iter()
                .filter_map(|(address, state)| {
                    state
                        .connected()
                        .map(|connection| (address.clone(), connection))
                })
                .choose(&mut rng())
        }
    }

    pub(super) async fn known_connections(
        &self,
        params: &ClusterParams,
    ) -> Vec<(NodeAddress, Option<C>)> {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            let _ = params;
            self.nodes
                .load()
                .iter()
                .map(|(address, node)| (address.clone(), node.connection_for_repair()))
                .collect()
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            self.read(params)
                .await
                .iter()
                .map(|(address, state)| (address.clone(), state.connection_for_repair()))
                .collect()
        }
    }

    pub(super) async fn install_connected(
        &self,
        address: NodeAddress,
        connection: C,
        params: &ClusterParams,
    ) {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            let node = self.ensure_node(address);
            node.install_connected(connection);
            let _ = params;
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            self.write(params)
                .await
                .insert(address, ConnState::Connected(connection));
        }
    }

    pub(super) async fn begin_reconnect(
        &self,
        address: NodeAddress,
        params: &ClusterParams,
    ) -> bool {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            let _ = params;
            self.ensure_node(address).begin_reconnect()
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            let mut guard = self.write(params).await;
            match guard.get_mut(&address) {
                Some(ConnState::Reconnecting(_) | ConnState::Connecting) => false,
                Some(state @ ConnState::Connected(_)) => {
                    let old = match std::mem::replace(state, ConnState::Connecting) {
                        ConnState::Connected(connection) => Some(connection),
                        _ => unreachable!(),
                    };
                    *state = ConnState::Reconnecting(old);
                    true
                }
                None => {
                    guard.insert(address, ConnState::Connecting);
                    true
                }
            }
        }
    }

    pub(super) async fn is_connected(&self, address: &NodeAddress, params: &ClusterParams) -> bool {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            let _ = params;
            self.node(address).is_some_and(|node| node.is_connected())
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            matches!(
                self.read(params).await.get(address),
                Some(ConnState::Connected(_))
            )
        }
    }

    pub(super) async fn install_repaired(
        &self,
        address: NodeAddress,
        connection: C,
        params: &ClusterParams,
    ) -> bool {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            let _ = params;
            self.ensure_node(address).install_repaired(connection)
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            let mut guard = self.write(params).await;
            if matches!(guard.get(&address), Some(ConnState::Connected(_))) {
                false
            } else {
                guard.insert(address, ConnState::Connected(connection));
                true
            }
        }
    }

    pub(super) async fn retain(&self, addresses: &HashSet<NodeAddress>, params: &ClusterParams) {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            loop {
                let current = self.nodes.load();
                let mut replacement = (**current).clone();
                replacement.retain(|address, _| addresses.contains(address));
                let previous = self.nodes.compare_and_swap(&current, Arc::new(replacement));
                if Arc::ptr_eq(&previous, &current) {
                    break;
                }
            }
            let _ = params;
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            self.write(params)
                .await
                .retain(|address, _| addresses.contains(address));
        }
    }

    pub(super) async fn remove(&self, address: &NodeAddress, params: &ClusterParams) {
        #[cfg(feature = "cluster-async-atomic-node-connections")]
        {
            loop {
                let current = self.nodes.load();
                if !current.contains_key(address) {
                    break;
                }
                let mut replacement = (**current).clone();
                replacement.remove(address);
                let previous = self.nodes.compare_and_swap(&current, Arc::new(replacement));
                if Arc::ptr_eq(&previous, &current) {
                    break;
                }
            }
            let _ = params;
        }
        #[cfg(not(feature = "cluster-async-atomic-node-connections"))]
        {
            self.write(params).await.remove(address);
        }
    }
}
