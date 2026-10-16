//! Helpers for exercising the Search module against a cluster.
use super::{TestClusterContext, TestContextVersioning, VALKEY_SEARCH_ANY};
use redis::Value;
use redis_test::cluster::RedisClusterConfiguration;
use redis_test::server::Module;

/// Per-peer shard connection states from `_FT.DEBUG SHARD_CONNECTION_STATES`: `(peer, states)` pairs.
type ShardConnectionStates = Vec<(String, Vec<String>)>;

/// Build a 3-primary cluster with the search module loaded on every node.
///
/// Check the server version before calling [`PrimaryConnections::from_cluster`],
/// because it expects RediSearch or valkey-search to be loaded.
pub fn setup_cluster_with_search_module() -> TestClusterContext {
    let ctx = TestClusterContext::new_with_config(RedisClusterConfiguration {
        num_nodes: 3,
        modules: vec![Module::Search],
        ..Default::default()
    });
    ctx.wait_for_cluster_up();
    ctx
}

/// Maintains one direct connection per primary, allowing each node to be polled
/// without incurring the cost of reconnecting for every index check.
pub struct PrimaryConnections {
    connections: Vec<(redis::ConnectionAddr, redis::Connection)>,
}

impl PrimaryConnections {
    /// Connect to every primary and wait until the Search module on each one can reach its peers.
    /// Replicas are skipped, so the cluster may have any number of them.
    pub fn from_cluster(ctx: &TestClusterContext) -> Self {
        let connections = ctx
            .cluster
            .iter_servers()
            .filter_map(|server| {
                let addr = server.client_addr().clone();
                let mut connection = redis::Client::open(server.connection_info())
                    .unwrap()
                    .get_connection()
                    .unwrap();
                let role: redis::Role = redis::cmd("ROLE").query(&mut connection).unwrap();
                matches!(role, redis::Role::Primary { .. }).then_some((addr, connection))
            })
            .collect();
        let mut primaries = Self { connections };
        if ctx.supports(VALKEY_SEARCH_ANY) {
            primaries.wait_for_cluster_slots();
        } else {
            primaries.wait_for_search_shard_connections();
        }
        primaries
    }

    /// Wait until every primary's `CLUSTER SLOTS` covers all slots and lists every primary.
    /// valkey-search finds its peers from the cluster topology, and in cluster mode `FT.CREATE`
    /// checks with every primary before it replies. valkey-search has no `_FT.DEBUG`, so wait on
    /// the topology instead.
    fn wait_for_cluster_slots(&mut self) {
        const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
        const SLOT_COUNT: i64 = 16384;

        let expected_primaries = self.connections.len();
        let deadline = std::time::Instant::now() + TIMEOUT;
        loop {
            let mut pending: Option<(redis::ConnectionAddr, Vec<Vec<Value>>)> = None;
            for (addr, connection) in &mut self.connections {
                let ranges: Vec<Vec<Value>> = redis::cmd("CLUSTER")
                    .arg("SLOTS")
                    .query(connection)
                    .unwrap();
                let mut covered_slots = 0;
                let mut primary_ports = std::collections::HashSet::new();
                for range in &ranges {
                    let start: i64 = redis::from_redis_value_ref(&range[0]).unwrap();
                    let end: i64 = redis::from_redis_value_ref(&range[1]).unwrap();
                    let primary: Vec<Value> = redis::from_redis_value_ref(&range[2]).unwrap();
                    let port: u16 = redis::from_redis_value_ref(&primary[1]).unwrap();
                    covered_slots += end - start + 1;
                    primary_ports.insert(port);
                }
                if covered_slots != SLOT_COUNT || primary_ports.len() != expected_primaries {
                    pending = Some((addr.clone(), ranges));
                    break;
                }
            }
            if pending.is_none() {
                return;
            }
            if std::time::Instant::now() >= deadline {
                let (addr, ranges) = pending.unwrap();
                panic!(
                    "cluster slots never converged on {addr:?} within {TIMEOUT:?}; current slots: {ranges:?}",
                );
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    /// Wait until every primary's outbound Search shard connection pool is fully established.
    /// Otherwise, FT.CREATE commands issued during the brief post-bootstrap window may succeed on the originating primary
    /// while replication to one or more peers is silently dropped.
    fn wait_for_search_shard_connections(&mut self) {
        const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

        let expected_peers = self.connections.len();
        let deadline = std::time::Instant::now() + TIMEOUT;
        loop {
            let mut pending: Option<(redis::ConnectionAddr, ShardConnectionStates)> = None;
            for (addr, connection) in &mut self.connections {
                let states: ShardConnectionStates = redis::cmd("_FT.DEBUG")
                    .arg("SHARD_CONNECTION_STATES")
                    .query(connection)
                    .unwrap();
                let ready = states.len() == expected_peers
                    && states.iter().all(|(_, conns)| {
                        !conns.is_empty() && conns.iter().all(|s| s == "Connected")
                    });
                if !ready {
                    pending = Some((addr.clone(), states));
                    break;
                }
            }
            if pending.is_none() {
                return;
            }
            if std::time::Instant::now() >= deadline {
                let (addr, states) = pending.unwrap();
                panic!(
                    "shard connections never converged on {addr:?} within {TIMEOUT:?}; current states: {states:?}",
                );
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    /// Poll each primary directly with `FT._LIST` until it reports `index_name`.
    /// The Search module propagates a successful `FT.CREATE` to peer primaries.
    /// A successful `FT.CREATE` only confirms that the command was accepted by the originating primary.
    /// This check waits until every primary reports the index locally, ensuring that index creation has propagated cluster-wide.
    pub fn assert_index_propagated(&mut self, index_name: &str) {
        const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(25);
        const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);
        for (addr, connection) in &mut self.connections {
            let deadline = std::time::Instant::now() + TIMEOUT;
            loop {
                let indexes: Vec<String> = redis::cmd("FT._LIST").query(connection).unwrap();
                if indexes.iter().any(|name| name == index_name) {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    panic!(
                        "index {index_name:?} never propagated to {addr:?} within {TIMEOUT:?}; got {indexes:?}",
                    );
                }
                std::thread::sleep(POLL_INTERVAL);
            }
        }
    }
}
