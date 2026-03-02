use super::traits::{ReadCandidates, ReadRoutingStrategy};
use crate::cluster_handling::NodeAddress;

/// Routes reads to a random replica node, falling back to the primary when no
/// replicas exist.
///
/// This is the default strategy and reproduces the behavior of the old
/// `read_from_replicas = true` flag.
#[derive(Debug, Clone, Default)]
pub struct RandomReplicaStrategy;

impl ReadRoutingStrategy for RandomReplicaStrategy {
    fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
        match candidates {
            ReadCandidates::AnyNode {
                replicas, primary, ..
            } => match replicas {
                Some(replicas) => replicas.choose_random(),
                None => primary,
            },
            ReadCandidates::ReplicasOnly { replicas, .. } => replicas.choose_random(),
        }
    }
}
