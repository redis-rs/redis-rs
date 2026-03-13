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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_handling::read_routing::Replicas;

    fn node(host: &str, port: u16) -> NodeAddress {
        NodeAddress::from_parts(host.into(), port)
    }

    #[test]
    fn chooses_replica_when_replicas_exist() {
        let primary = node("primary", 6379);
        let replicas = [node("replica1", 6379), node("replica2", 6379)];
        let candidates = ReadCandidates::AnyNode {
            slot: 0,
            primary: &primary,
            replicas: Replicas::new(&replicas),
        };

        let strategy = RandomReplicaStrategy;
        let chosen = strategy.route_read(&candidates);
        assert!(replicas.contains(chosen));
    }

    #[test]
    fn chooses_replica_for_replicas_only() {
        let replicas = [node("replica1", 6379), node("replica2", 6379)];
        let candidates = ReadCandidates::ReplicasOnly {
            slot: 0,
            replicas: Replicas::new(&replicas).unwrap(),
        };

        let strategy = RandomReplicaStrategy;
        let chosen = strategy.route_read(&candidates);
        assert!(replicas.contains(chosen));
    }

    #[test]
    fn falls_back_to_primary_when_no_replicas() {
        let primary = node("primary", 6379);
        let candidates = ReadCandidates::AnyNode {
            slot: 0,
            primary: &primary,
            replicas: None,
        };

        let strategy = RandomReplicaStrategy;
        let chosen = strategy.route_read(&candidates);
        assert_eq!(chosen, &primary);
    }
}
