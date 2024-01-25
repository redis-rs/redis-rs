use crate::cluster::ClusterConnection;
use crate::cmd::{cmd, Cmd};
use crate::types::{
    from_owned_redis_value, ErrorKind, FromRedisValue, HashSet, RedisResult, ToRedisArgs, Value,
};

pub(crate) const UNROUTABLE_ERROR: (ErrorKind, &str) = (
    ErrorKind::ClientError,
    "This command cannot be safely routed in cluster mode",
);

fn is_illegal_cmd(cmd: &str) -> bool {
    matches!(
        cmd,
        "BGREWRITEAOF" | "BGSAVE" | "BITOP" | "BRPOPLPUSH" |
        // All commands that start with "CLIENT"
        "CLIENT" | "CLIENT GETNAME" | "CLIENT KILL" | "CLIENT LIST" | "CLIENT SETNAME" |
        // All commands that start with "CONFIG"
        "CONFIG" | "CONFIG GET" | "CONFIG RESETSTAT" | "CONFIG REWRITE" | "CONFIG SET" |
        "DBSIZE" |
        "ECHO" | "EVALSHA" |
        "FLUSHALL" | "FLUSHDB" |
        "INFO" |
        "KEYS" |
        "LASTSAVE" |
        "MGET" | "MOVE" | "MSET" | "MSETNX" |
        "PFMERGE" | "PFCOUNT" | "PING" | "PUBLISH" |
        "RANDOMKEY" | "RENAME" | "RENAMENX" | "RPOPLPUSH" |
        "SAVE" | "SCAN" |
        // All commands that start with "SCRIPT"
        "SCRIPT" | "SCRIPT EXISTS" | "SCRIPT FLUSH" | "SCRIPT KILL" | "SCRIPT LOAD" |
        "SDIFF" | "SDIFFSTORE" |
        // All commands that start with "SENTINEL"
        "SENTINEL" | "SENTINEL GET MASTER ADDR BY NAME" | "SENTINEL MASTER" | "SENTINEL MASTERS" |
        "SENTINEL MONITOR" | "SENTINEL REMOVE" | "SENTINEL SENTINELS" | "SENTINEL SET" |
        "SENTINEL SLAVES" | "SHUTDOWN" | "SINTER" | "SINTERSTORE" | "SLAVEOF" |
        // All commands that start with "SLOWLOG"
        "SLOWLOG" | "SLOWLOG GET" | "SLOWLOG LEN" | "SLOWLOG RESET" |
        "SMOVE" | "SORT" | "SUNION" | "SUNIONSTORE" |
        "TIME"
    )
}

/// Represents a Redis Cluster command pipeline.
#[derive(Clone)]
pub struct ClusterPipeline {
    commands: Vec<Cmd>,
    ignored_commands: HashSet<usize>,
}

/// A cluster pipeline is almost identical to a normal [Pipeline](crate::pipeline::Pipeline), with two exceptions:
/// * It does not support transactions
/// * The following commands can not be used in a cluster pipeline:
/// ```text
/// BGREWRITEAOF, BGSAVE, BITOP, BRPOPLPUSH
/// CLIENT GETNAME, CLIENT KILL, CLIENT LIST, CLIENT SETNAME, CONFIG GET,
/// CONFIG RESETSTAT, CONFIG REWRITE, CONFIG SET
/// DBSIZE
/// ECHO, EVALSHA
/// FLUSHALL, FLUSHDB
/// INFO
/// KEYS
/// LASTSAVE
/// MGET, MOVE, MSET, MSETNX
/// PFMERGE, PFCOUNT, PING, PUBLISH
/// RANDOMKEY, RENAME, RENAMENX, RPOPLPUSH
/// SAVE, SCAN, SCRIPT EXISTS, SCRIPT FLUSH, SCRIPT KILL, SCRIPT LOAD, SDIFF, SDIFFSTORE,
/// SENTINEL GET MASTER ADDR BY NAME, SENTINEL MASTER, SENTINEL MASTERS, SENTINEL MONITOR,
/// SENTINEL REMOVE, SENTINEL SENTINELS, SENTINEL SET, SENTINEL SLAVES, SHUTDOWN, SINTER,
/// SINTERSTORE, SLAVEOF, SLOWLOG GET, SLOWLOG LEN, SLOWLOG RESET, SMOVE, SORT, SUNION, SUNIONSTORE
/// TIME
/// ```
impl ClusterPipeline {
    /// Create an empty pipeline.
    pub fn new() -> ClusterPipeline {
        Self::with_capacity(0)
    }

    /// Creates an empty pipeline with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> ClusterPipeline {
        ClusterPipeline {
            commands: Vec::with_capacity(capacity),
            ignored_commands: HashSet::new(),
        }
    }

    pub(crate) fn commands(&self) -> &Vec<Cmd> {
        &self.commands
    }

    /// Executes the pipeline and fetches the return values:
    ///
    /// ```rust,no_run
    /// # let nodes = vec!["redis://127.0.0.1:6379/"];
    /// # let client = redis::cluster::ClusterClient::new(nodes).unwrap();
    /// # let mut con = client.get_connection().unwrap();
    /// let mut pipe = redis::cluster::cluster_pipe();
    /// let (k1, k2) : (i32, i32) = pipe
    ///     .cmd("SET").arg("key_1").arg(42).ignore()
    ///     .cmd("SET").arg("key_2").arg(43).ignore()
    ///     .cmd("GET").arg("key_1")
    ///     .cmd("GET").arg("key_2").query(&mut con).unwrap();
    /// ```
    #[inline]
    pub fn query<T: FromRedisValue>(&self, con: &mut ClusterConnection) -> RedisResult<T> {
        for cmd in &self.commands {
            let cmd_name = std::str::from_utf8(cmd.arg_idx(0).unwrap_or(b""))
                .unwrap_or("")
                .trim()
                .to_ascii_uppercase();

            if is_illegal_cmd(&cmd_name) {
                fail!((
                    UNROUTABLE_ERROR.0,
                    UNROUTABLE_ERROR.1,
                    format!("Command '{cmd_name}' can't be executed in a cluster pipeline.")
                ))
            }
        }

        from_owned_redis_value(if self.commands.is_empty() {
            Value::Bulk(vec![])
        } else {
            self.make_pipeline_results(con.execute_pipeline(self)?)
        })
    }

    /// This is a shortcut to `query()` that does not return a value and
    /// will fail the task if the query of the pipeline fails.
    ///
    /// This is equivalent to a call to query like this:
    ///
    /// ```rust,no_run
    /// # let nodes = vec!["redis://127.0.0.1:6379/"];
    /// # let client = redis::cluster::ClusterClient::new(nodes).unwrap();
    /// # let mut con = client.get_connection().unwrap();
    /// let mut pipe = redis::cluster::cluster_pipe();
    /// let _ : () = pipe.cmd("SET").arg("key_1").arg(42).ignore().query(&mut con).unwrap();
    /// ```
    #[inline]
    pub fn execute(&self, con: &mut ClusterConnection) {
        self.query::<()>(con).unwrap();
    }
}

/// Shortcut for creating a new cluster pipeline.
pub fn cluster_pipe() -> ClusterPipeline {
    ClusterPipeline::new()
}

implement_pipeline_commands!(ClusterPipeline);
