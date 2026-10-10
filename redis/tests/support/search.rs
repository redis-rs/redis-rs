//! Helpers for exercising the Search module against a cluster.
use super::TestClusterContext;
use redis_test::cluster::RedisClusterConfiguration;
use redis_test::server::Module;

/// Per-peer shard connection states from `_FT.DEBUG SHARD_CONNECTION_STATES`: `(peer, states)` pairs.
type ShardConnectionStates = Vec<(String, Vec<String>)>;

/// Build a 3-primary cluster with the search module loaded on every node.
///
/// Check the server version before calling [`PrimaryConnections::from_cluster`],
/// because it sends commands that only RediSearch supports.
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
    cons: Vec<(redis::ConnectionAddr, redis::Connection)>,
}

impl PrimaryConnections {
    /// Connect to every primary and wait until their Search shard connections are established.
    pub fn from_cluster(ctx: &TestClusterContext) -> Self {
        let cons = ctx
            .cluster
            .iter_servers()
            .map(|server| {
                let addr = server.client_addr().clone();
                let con = redis::Client::open(server.connection_info())
                    .unwrap()
                    .get_connection()
                    .unwrap();
                (addr, con)
            })
            .collect();
        let mut primaries = Self { cons };
        primaries.wait_for_search_shard_connections();
        primaries
    }

    /// Wait until every primary's outbound Search shard connection pool is fully established.
    /// Otherwise, FT.CREATE commands issued during the brief post-bootstrap window may succeed on the originating primary
    /// while replication to one or more peers is silently dropped.
    fn wait_for_search_shard_connections(&mut self) {
        const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

        let expected_peers = self.cons.len();
        let deadline = std::time::Instant::now() + TIMEOUT;
        loop {
            let mut pending: Option<(redis::ConnectionAddr, ShardConnectionStates)> = None;
            for (addr, con) in &mut self.cons {
                let states: ShardConnectionStates = redis::cmd("_FT.DEBUG")
                    .arg("SHARD_CONNECTION_STATES")
                    .query(con)
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
    /// The Search module propagates a successful `FT.CREATE` to peer primaries via internal `_FT.CREATE` calls.
    /// A successful `FT.CREATE` only confirms that the command was accepted by the originating primary.
    /// This check waits until every primary reports the index locally, ensuring that index creation has propagated cluster-wide.
    pub fn assert_index_propagated(&mut self, index_name: &str) {
        const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(25);
        const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);
        for (addr, con) in &mut self.cons {
            let deadline = std::time::Instant::now() + TIMEOUT;
            loop {
                let indexes: Vec<String> = redis::cmd("FT._LIST").query(con).unwrap();
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
