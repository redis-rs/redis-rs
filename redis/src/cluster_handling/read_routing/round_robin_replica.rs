use std::sync::atomic::{AtomicUsize, Ordering};

use super::traits::{ReadCandidates, ReadRoutingStrategy};
use crate::cluster_handling::NodeAddress;

/// Routes reads to replica nodes in round-robin order, falling back to the
/// primary when no replicas exist.
///
/// Each instance maintains an independent counter that increments on every
/// call to [`route_read`](ReadRoutingStrategy::route_read). Cloning a
/// `RoundRobinReplicaStrategy` resets the counter to zero, so each cluster connection
/// gets its own independent rotation.
#[derive(Debug)]
pub struct RoundRobinReplicaStrategy {
    counter: AtomicUsize,
}

impl RoundRobinReplicaStrategy {
    /// Creates a new `RoundRobinReplicaStrategy` strategy with the counter starting at 0.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for RoundRobinReplicaStrategy {
    fn default() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Clone for RoundRobinReplicaStrategy {
    fn clone(&self) -> Self {
        // Each clone starts a fresh counter — the factory creates one per
        // connection, giving each connection independent round-robin rotation.
        Self::default()
    }
}

impl ReadRoutingStrategy for RoundRobinReplicaStrategy {
    fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
        let idx = self.counter.fetch_add(1, Ordering::Relaxed);
        match candidates {
            ReadCandidates::AnyNode {
                replicas, primary, ..
            } => match replicas {
                Some(replicas) => replicas.get(idx % replicas.len()).expect("non-empty"),
                None => primary,
            },
            ReadCandidates::ReplicasOnly { replicas, .. } => {
                replicas.get(idx % replicas.len()).expect("non-empty")
            }
        }
    }
}
