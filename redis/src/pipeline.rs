#![macro_use]

use crate::cmd::{cmd, cmd_len, Cmd};
use crate::connection::ConnectionLike;
use crate::types::{
    from_owned_redis_value, ErrorKind, FromRedisValue, HashSet, RedisResult, ToRedisArgs, Value,
};

/// Represents a redis command pipeline.
#[derive(Clone)]
pub struct Pipeline {
    commands: Vec<Cmd>,
    transaction_mode: bool,
    ignored_commands: HashSet<usize>,
}

/// A pipeline allows you to send multiple commands in one go to the
/// redis server.  API wise it's very similar to just using a command
/// but it allows multiple commands to be chained and some features such
/// as iteration are not available.
///
/// Basic example:
///
/// ```rust,no_run
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let mut con = client.get_connection().unwrap();
/// let ((k1, k2),) : ((i32, i32),) = redis::pipe()
///     .cmd("SET").arg("key_1").arg(42).ignore()
///     .cmd("SET").arg("key_2").arg(43).ignore()
///     .cmd("MGET").arg(&["key_1", "key_2"]).query(&mut con).unwrap();
/// ```
///
/// As you can see with `cmd` you can start a new command.  By default
/// each command produces a value but for some you can ignore them by
/// calling `ignore` on the command.  That way it will be skipped in the
/// return value which is useful for `SET` commands and others, which
/// do not have a useful return value.
impl Pipeline {
    /// Creates an empty pipeline.  For consistency with the `cmd`
    /// api a `pipe` function is provided as alias.
    pub fn new() -> Pipeline {
        Self::with_capacity(0)
    }

    /// Creates an empty pipeline with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Pipeline {
        Pipeline {
            commands: Vec::with_capacity(capacity),
            transaction_mode: false,
            ignored_commands: HashSet::new(),
        }
    }

    /// This enables atomic mode.  In atomic mode the whole pipeline is
    /// enclosed in `MULTI`/`EXEC`.  From the user's point of view nothing
    /// changes however.  This is easier than using `MULTI`/`EXEC` yourself
    /// as the format does not change.
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let mut con = client.get_connection().unwrap();
    /// let (k1, k2) : (i32, i32) = redis::pipe()
    ///     .atomic()
    ///     .cmd("GET").arg("key_1")
    ///     .cmd("GET").arg("key_2").query(&mut con).unwrap();
    /// ```
    #[inline]
    pub fn atomic(&mut self) -> &mut Pipeline {
        self.transaction_mode = true;
        self
    }

    /// Returns the encoded pipeline commands.
    pub fn get_packed_pipeline(&self) -> Vec<u8> {
        encode_pipeline(&self.commands, self.transaction_mode)
    }

    #[cfg(feature = "aio")]
    pub(crate) fn write_packed_pipeline(&self, out: &mut Vec<u8>) {
        write_pipeline(out, &self.commands, self.transaction_mode)
    }

    fn execute_pipelined(&self, con: &mut dyn ConnectionLike) -> RedisResult<Value> {
        Ok(self.make_pipeline_results(con.req_packed_commands(
            &encode_pipeline(&self.commands, false),
            0,
            self.commands.len(),
        )?))
    }

    fn execute_transaction(&self, con: &mut dyn ConnectionLike) -> RedisResult<Value> {
        let mut resp = con.req_packed_commands(
            &encode_pipeline(&self.commands, true),
            self.commands.len() + 1,
            1,
        )?;
        match resp.pop() {
            Some(Value::Nil) => Ok(Value::Nil),
            Some(Value::Bulk(items)) => Ok(self.make_pipeline_results(items)),
            _ => fail!((
                ErrorKind::ResponseError,
                "Invalid response when parsing multi response"
            )),
        }
    }

    /// Executes the pipeline and fetches the return values.  Since most
    /// pipelines return different types it's recommended to use tuple
    /// matching to process the results:
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let mut con = client.get_connection().unwrap();
    /// let (k1, k2) : (i32, i32) = redis::pipe()
    ///     .cmd("SET").arg("key_1").arg(42).ignore()
    ///     .cmd("SET").arg("key_2").arg(43).ignore()
    ///     .cmd("GET").arg("key_1")
    ///     .cmd("GET").arg("key_2").query(&mut con).unwrap();
    /// ```
    ///
    /// NOTE: A Pipeline object may be reused after `query()` with all the commands as were inserted
    ///       to them. In order to clear a Pipeline object with minimal memory released/allocated,
    ///       it is necessary to call the `clear()` before inserting new commands.
    #[inline]
    pub fn query<T: FromRedisValue>(&self, con: &mut dyn ConnectionLike) -> RedisResult<T> {
        if !con.supports_pipelining() {
            fail!((
                ErrorKind::ResponseError,
                "This connection does not support pipelining."
            ));
        }
        from_owned_redis_value(if self.commands.is_empty() {
            Value::Bulk(vec![])
        } else if self.transaction_mode {
            self.execute_transaction(con)?
        } else {
            self.execute_pipelined(con)?
        })
    }

    #[cfg(feature = "aio")]
    async fn execute_pipelined_async<C>(&self, con: &mut C) -> RedisResult<Value>
    where
        C: crate::aio::ConnectionLike,
    {
        let value = con
            .req_packed_commands(self, 0, self.commands.len())
            .await?;
        Ok(self.make_pipeline_results(value))
    }

    #[cfg(feature = "aio")]
    async fn execute_transaction_async<C>(&self, con: &mut C) -> RedisResult<Value>
    where
        C: crate::aio::ConnectionLike,
    {
        let mut resp = con
            .req_packed_commands(self, self.commands.len() + 1, 1)
            .await?;
        match resp.pop() {
            Some(Value::Nil) => Ok(Value::Nil),
            Some(Value::Bulk(items)) => Ok(self.make_pipeline_results(items)),
            _ => Err((
                ErrorKind::ResponseError,
                "Invalid response when parsing multi response",
            )
                .into()),
        }
    }

    /// Async version of `query`.
    #[inline]
    #[cfg(feature = "aio")]
    pub async fn query_async<C, T: FromRedisValue>(&self, con: &mut C) -> RedisResult<T>
    where
        C: crate::aio::ConnectionLike,
    {
        let v = if self.commands.is_empty() {
            return from_owned_redis_value(Value::Bulk(vec![]));
        } else if self.transaction_mode {
            self.execute_transaction_async(con).await?
        } else {
            self.execute_pipelined_async(con).await?
        };
        from_owned_redis_value(v)
    }

    /// This is a shortcut to `query()` that does not return a value and
    /// will fail the task if the query of the pipeline fails.
    ///
    /// This is equivalent to a call of query like this:
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let mut con = client.get_connection().unwrap();
    /// let _ : () = redis::pipe().cmd("PING").query(&mut con).unwrap();
    /// ```
    ///
    /// NOTE: A Pipeline object may be reused after `query()` with all the commands as were inserted
    ///       to them. In order to clear a Pipeline object with minimal memory released/allocated,
    ///       it is necessary to call the `clear()` before inserting new commands.
    #[inline]
    pub fn execute(&self, con: &mut dyn ConnectionLike) {
        self.query::<()>(con).unwrap();
    }
}

fn encode_pipeline(cmds: &[Cmd], atomic: bool) -> Vec<u8> {
    let mut rv = vec![];
    write_pipeline(&mut rv, cmds, atomic);
    rv
}

fn write_pipeline(rv: &mut Vec<u8>, cmds: &[Cmd], atomic: bool) {
    let cmds_len = cmds.iter().map(cmd_len).sum();

    if atomic {
        let multi = cmd("MULTI");
        let exec = cmd("EXEC");
        rv.reserve(cmd_len(&multi) + cmd_len(&exec) + cmds_len);

        multi.write_packed_command_preallocated(rv);
        for cmd in cmds {
            cmd.write_packed_command_preallocated(rv);
        }
        exec.write_packed_command_preallocated(rv);
    } else {
        rv.reserve(cmds_len);

        for cmd in cmds {
            cmd.write_packed_command_preallocated(rv);
        }
    }
}

// Macro to implement shared methods between Pipeline and ClusterPipeline
macro_rules! implement_pipeline_commands {
    ($struct_name:ident) => {
        impl $struct_name {
            /// Adds a command to the cluster pipeline.
            #[inline]
            pub fn add_command(&mut self, cmd: Cmd) -> &mut Self {
                self.commands.push(cmd);
                self
            }

            /// Starts a new command. Functions such as `arg` then become
            /// available to add more arguments to that command.
            #[inline]
            pub fn cmd(&mut self, name: &str) -> &mut Self {
                self.add_command(cmd(name))
            }

            /// Returns an iterator over all the commands currently in this pipeline
            pub fn cmd_iter(&self) -> impl Iterator<Item = &Cmd> {
                self.commands.iter()
            }

            /// Instructs the pipeline to ignore the return value of this command.
            /// It will still be ensured that it is not an error, but any successful
            /// result is just thrown away.  This makes result processing through
            /// tuples much easier because you do not need to handle all the items
            /// you do not care about.
            #[inline]
            pub fn ignore(&mut self) -> &mut Self {
                match self.commands.len() {
                    0 => true,
                    x => self.ignored_commands.insert(x - 1),
                };
                self
            }

            /// Adds an argument to the last started command. This works similar
            /// to the `arg` method of the `Cmd` object.
            ///
            /// Note that this function fails the task if executed on an empty pipeline.
            #[inline]
            pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Self {
                {
                    let cmd = self.get_last_command();
                    cmd.arg(arg);
                }
                self
            }

            /// Clear a pipeline object's internal data structure.
            ///
            /// This allows reusing a pipeline object as a clear object while performing a minimal
            /// amount of memory released/reallocated.
            #[inline]
            pub fn clear(&mut self) {
                self.commands.clear();
                self.ignored_commands.clear();
            }

            #[inline]
            fn get_last_command(&mut self) -> &mut Cmd {
                let idx = match self.commands.len() {
                    0 => panic!("No command on stack"),
                    x => x - 1,
                };
                &mut self.commands[idx]
            }

            fn make_pipeline_results(&self, resp: Vec<Value>) -> Value {
                let mut rv = Vec::with_capacity(resp.len() - self.ignored_commands.len());
                for (idx, result) in resp.into_iter().enumerate() {
                    if !self.ignored_commands.contains(&idx) {
                        rv.push(result);
                    }
                }
                Value::Bulk(rv)
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

implement_pipeline_commands!(Pipeline);
