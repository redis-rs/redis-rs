//! Pluggable read routing strategies for Redis Cluster connections.
//!
//! By default, all reads go to primary nodes. Setting a strategy via
//! [`ClusterClientBuilder::read_from_replicas`](crate::cluster::ClusterClientBuilder::read_from_replicas)
//! or [`ClusterClientBuilder::read_routing_strategy`](crate::cluster::ClusterClientBuilder::read_routing_strategy)
//! enables routing reads to replica nodes.
//!
//! # Built-in strategies
//!
//! - [`RandomReplicaStrategy`] — picks a random replica for each read, falling back to
//!   the primary when no replicas exist. This is the default when
//!   `read_from_replicas()` is called.
//! - [`RoundRobinReplicaStrategy`] — cycles through replicas in order, falling back to
//!   the primary when no replicas exist. Each clone (i.e. each cluster connection)
//!   starts a fresh counter.
//!
//! # Custom strategies
//!
//! Implement [`ReadRoutingStrategy`] to create your own routing logic:
//!
//! ```rust
//! use redis::cluster_read_routing::{
//!     ReadRoutingStrategy, ReadCandidates,
//! };
//! use redis::cluster::NodeAddress;
//!
//! #[derive(Clone)]
//! struct FirstReplica;
//!
//! impl ReadRoutingStrategy for FirstReplica {
//!     fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
//!         match candidates {
//!             ReadCandidates::AnyNode {
//!                 primary, replicas, ..
//!             } => match replicas {
//!                 Some(replicas) => replicas.first(),
//!                 None => primary,
//!             },
//!             ReadCandidates::ReplicasOnly { replicas, .. } => replicas.first(),
//!         }
//!     }
//! }
//! ```

mod random_replica;
mod round_robin_replica;
mod traits;

pub use random_replica::RandomReplicaStrategy;
pub use round_robin_replica::RoundRobinReplicaStrategy;
pub use traits::{
    ReadCandidates, ReadRoutingStrategy, ReadRoutingStrategyFactory, Replicas, SlotTopology,
};
