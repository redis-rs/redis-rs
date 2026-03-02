use super::super::NodeAddress;

/// A snapshot of the topology for a single slot range in the cluster.
///
/// Passed to [`ReadRoutingStrategy::on_topology_changed`] so that strategies
/// can maintain internal state (e.g. latency tables, connection counts).
#[derive(Debug, Clone)]
pub struct SlotTopology {
    /// First slot in the range (inclusive).
    pub slot_range_start: u16,
    /// Last slot in the range (inclusive).
    pub slot_range_end: u16,
    /// The primary node for this range.
    pub primary: NodeAddress,
    /// The replica nodes for this range (may be empty).
    pub replicas: Vec<NodeAddress>,
}

/// A non-empty slice of replica [`NodeAddress`]es.
///
/// This wrapper guarantees that the underlying slice contains at least one
/// element, so callers never need to handle the empty case.
#[derive(Debug)]
pub struct Replicas<'a> {
    inner: &'a [NodeAddress],
}

impl<'a> Replicas<'a> {
    /// Wraps a slice, returning `None` if it is empty.
    pub fn new(slice: &'a [NodeAddress]) -> Option<Self> {
        if slice.is_empty() {
            None
        } else {
            Some(Self { inner: slice })
        }
    }

    /// Returns the number of replicas (always >= 1).
    #[allow(clippy::len_without_is_empty)] // Replicas is guaranteed non-empty
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns the first replica (always present).
    pub fn first(&self) -> &'a NodeAddress {
        &self.inner[0]
    }

    /// Returns the replica at the given index, or `None` if out of bounds.
    pub fn get(&self, idx: usize) -> Option<&'a NodeAddress> {
        self.inner.get(idx)
    }

    /// Picks a uniformly random replica.
    pub fn choose_random(&self) -> &'a NodeAddress {
        use rand::seq::IndexedRandom;
        self.inner.choose(&mut rand::rng()).expect("non-empty")
    }

    /// Iterates over all replicas.
    pub fn iter(&self) -> std::slice::Iter<'a, NodeAddress> {
        self.inner.iter()
    }
}

/// The candidate nodes passed to [`ReadRoutingStrategy::route_read`].
#[derive(Debug)]
pub enum ReadCandidates<'a> {
    /// Any node (primary or replica) is acceptable for this read.
    AnyNode {
        /// The exact slot being read.
        slot: u16,
        /// The primary node for this slot range.
        primary: &'a NodeAddress,
        /// The replicas for this slot range, if any exist.
        replicas: Option<Replicas<'a>>,
    },
    /// A replica is required for this read.
    ReplicasOnly {
        /// The exact slot being read.
        slot: u16,
        /// The replicas for this slot range (guaranteed non-empty).
        replicas: Replicas<'a>,
    },
}

/// A strategy for choosing which node to route read commands to in a Redis Cluster.
///
/// [`route_read`](ReadRoutingStrategy::route_read) is called for each read command with
/// the [`ReadCandidates`] for the target slot. Return a reference to the chosen node.
///
/// Optionally, implement [`on_topology_changed`](ReadRoutingStrategy::on_topology_changed)
/// to receive notifications when the cluster topology is discovered or refreshed.
///
/// Set the strategy via [`ClusterClientBuilder::read_routing_strategy`] or use the
/// convenience method [`ClusterClientBuilder::read_from_replicas`] which installs a
/// [`RandomReplicaStrategy`](super::RandomReplicaStrategy) strategy.
///
/// [`ClusterClientBuilder::read_routing_strategy`]: crate::cluster::ClusterClientBuilder::read_routing_strategy
/// [`ClusterClientBuilder::read_from_replicas`]: crate::cluster::ClusterClientBuilder::read_from_replicas
///
/// # Examples
///
/// Route reads to the first replica:
///
/// ```rust
/// use redis::cluster_read_routing::{ReadRoutingStrategy, ReadCandidates};
/// use redis::cluster::NodeAddress;
///
/// #[derive(Clone)]
/// struct FirstReplica;
///
/// impl ReadRoutingStrategy for FirstReplica {
///     fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
///         match candidates {
///             ReadCandidates::AnyNode {
///                 primary, replicas, ..
///             } => match replicas {
///                 Some(replicas) => replicas.first(),
///                 None => primary,
///             },
///             ReadCandidates::ReplicasOnly { replicas, .. } => replicas.first(),
///         }
///     }
/// }
/// ```
pub trait ReadRoutingStrategy: Send + Sync {
    /// Called when the connection discovers or refreshes the cluster topology.
    ///
    /// This is called on every slot map refresh, including the initial topology
    /// discovery when a connection is first created. The default implementation
    /// does nothing.
    fn on_topology_changed(&self, _topology: &[SlotTopology]) {}

    /// Choose which node to route a read command to.
    ///
    /// The returned reference must point to one of the addresses provided in
    /// the [`ReadCandidates`] (either the primary or one of the replicas).
    fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress;
}

/// A factory for creating per-connection [`ReadRoutingStrategy`] instances.
///
/// This trait is stored in the cluster client and used to create a fresh strategy
/// instance for each connection. This gives each connection its own strategy state,
/// which is important for strategies that track per-connection data like latency
/// measurements.
///
/// A blanket implementation is provided for any `T: ReadRoutingStrategy + Clone + 'static`,
/// so simple stateless strategies (like [`RandomReplicaStrategy`](super::RandomReplicaStrategy)) work
/// automatically without implementing this trait explicitly.
///
/// # Examples
///
/// ```rust,no_run
/// use redis::cluster_read_routing::{
///     ReadRoutingStrategy, ReadRoutingStrategyFactory, ReadCandidates,
/// };
/// use redis::cluster::NodeAddress;
///
/// struct MyStrategyFactory;
///
/// impl ReadRoutingStrategyFactory for MyStrategyFactory {
///     fn create_strategy(&self) -> Box<dyn ReadRoutingStrategy> {
///         Box::new(MyStrategy::new())
///     }
/// }
///
/// struct MyStrategy;
///
/// impl MyStrategy {
///     fn new() -> Self { MyStrategy }
/// }
///
/// impl ReadRoutingStrategy for MyStrategy {
///     fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
///         match candidates {
///             ReadCandidates::AnyNode { primary, .. } => primary,
///             ReadCandidates::ReplicasOnly { replicas, .. } => replicas.first(),
///         }
///     }
/// }
/// ```
pub trait ReadRoutingStrategyFactory: Send + Sync {
    /// Create a new strategy instance.
    fn create_strategy(&self) -> Box<dyn ReadRoutingStrategy>;
}

impl<T: ReadRoutingStrategy + Clone + 'static> ReadRoutingStrategyFactory for T {
    fn create_strategy(&self) -> Box<dyn ReadRoutingStrategy> {
        Box::new(self.clone())
    }
}
