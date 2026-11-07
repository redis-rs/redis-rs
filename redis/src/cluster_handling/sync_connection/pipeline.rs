use super::ClusterConnection;
use crate::RedisError;
use crate::cmd::{Arg, Cmd, CmdRef};
use crate::errors::ErrorKind;
use crate::known_commands::KnownCommand;
use crate::pipeline::CommandRecord;
use crate::types::{FromRedisValue, RedisResult, ToRedisArgs, Value, from_redis_value};

/// Returns `true` if `cmd` can't be sent in a cluster pipeline.
fn is_illegal_cmd(cmd: &CmdRef<'_>) -> bool {
    // An unknown sub-command of a command whose sub-commands are all illegal (`CLIENT ...`) is
    // illegal too, so fall back to the command itself.
    cmd.command()
        .or_else(|| KnownCommand::from_parts(cmd.arg_idx(0)?, None))
        .is_some_and(KnownCommand::is_illegal_in_cluster_pipeline)
}

pub(crate) const UNROUTABLE_ERROR: (ErrorKind, &str) = (
    ErrorKind::Client,
    "This command cannot be safely routed in cluster mode",
);

/// Represents a Redis Cluster command pipeline.
#[derive(Clone, Debug)]
pub struct ClusterPipeline {
    data: Vec<u8>,
    args: Vec<Arg<usize>>,
    commands: Vec<CommandRecord>,
    ignore_errors: bool,
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
/// MGET, MOVE, MSET, MSETNX, MSETEX
/// PING, PUBLISH
/// RANDOMKEY, RENAME, RENAMENX, RPOPLPUSH
/// SAVE, SCAN, SCRIPT EXISTS, SCRIPT FLUSH, SCRIPT KILL, SCRIPT LOAD, SDIFF, SDIFFSTORE,
/// SENTINEL GET MASTER ADDR BY NAME, SENTINEL MASTER, SENTINEL MASTERS, SENTINEL MONITOR,
/// SENTINEL REMOVE, SENTINEL SENTINELS, SENTINEL SET, SENTINEL SLAVES, SHUTDOWN, SINTER,
/// SINTERSTORE, SLAVEOF, SLOWLOG GET, SLOWLOG LEN, SLOWLOG RESET, SMOVE, SORT, SUNION, SUNIONSTORE
/// TIME
/// ```
impl ClusterPipeline {
    /// Create an empty pipeline.
    ///
    /// To pre-allocate the internal buffers when you have a size estimate, chain the
    /// [`reserve_for_commands`](Self::reserve_for_commands),
    /// [`reserve_for_args`](Self::reserve_for_args), and
    /// [`reserve_for_data`](Self::reserve_for_data) methods.
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            args: Vec::new(),
            commands: Vec::new(),
            ignore_errors: false,
        }
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
        for cmd in self.cmd_iter() {
            if is_illegal_cmd(&cmd) {
                let cmd_name = std::str::from_utf8(cmd.arg_idx(0).unwrap_or(b""))
                    .unwrap_or("")
                    .trim()
                    .to_ascii_uppercase();
                fail!((
                    UNROUTABLE_ERROR.0,
                    UNROUTABLE_ERROR.1,
                    format!("Command '{cmd_name}' can't be executed in a cluster pipeline.")
                ))
            }
        }

        if self.commands.is_empty() {
            Err(RedisError::make_empty_command())
        } else {
            self.compose_response(con.execute_pipeline(self)?)
        }
    }

    /// This is an alternative to `query`` that can be used if you want to be able to handle a
    /// command's success or failure but don't care about the command's response. For example,
    /// this is useful for "SET" commands for which the response's content is not important.
    /// It avoids the need to define generic bounds for ().
    #[inline]
    pub fn exec(&self, con: &mut ClusterConnection) -> RedisResult<()> {
        self.query::<()>(con)
    }
}

/// Shortcut for creating a new cluster pipeline.
pub fn cluster_pipe() -> ClusterPipeline {
    ClusterPipeline::new()
}

implement_pipeline_commands!(ClusterPipeline);

#[cfg(test)]
mod tests {
    use super::is_illegal_cmd;
    use crate::{Cmd, cmd, cmd::CmdRef};

    fn is_illegal(cmd: &Cmd) -> bool {
        is_illegal_cmd(&CmdRef::from_cmd(cmd))
    }

    #[test]
    fn illegal_commands_are_detected() {
        assert!(is_illegal(&Cmd::mget(&["a", "b"])));
        assert!(is_illegal(cmd("mget").arg("a")));
        assert!(is_illegal(&Cmd::client_getname()));
        assert!(is_illegal(cmd("CLIENT").arg("GETNAME")));
        assert!(is_illegal(&cmd(" client getname ")));
        // Unknown sub-commands fall back to the primary command.
        assert!(is_illegal(cmd("CLIENT").arg("NOT-A-COMMAND")));
        assert!(is_illegal(cmd("SCRIPT").arg("NOT-A-COMMAND")));
    }

    #[test]
    fn legal_commands_are_allowed() {
        assert!(!is_illegal(&Cmd::get("a")));
        assert!(!is_illegal(cmd("get").arg("a")));
        assert!(!is_illegal(cmd("NOT-A-COMMAND").arg("a")));
        assert!(!is_illegal(cmd("OBJECT").arg("NOT-A-COMMAND")));
        assert!(!is_illegal(&Cmd::new()));
    }
}
