#![macro_use]

#[cfg(feature = "cache-aio")]
use crate::cmd::CommandCacheConfig;
use crate::cmd::{cmd, cmd_len, Cmd};
use crate::connection::ConnectionLike;
use crate::errors::ErrorKind;
use crate::types::{
    from_owned_redis_value, FromRedisValue, HashSet, RedisResult, ToRedisArgs, Value,
};
use crate::{RedisError, ServerErrorKind};

/// Represents a redis command pipeline.
#[derive(Clone)]
pub struct Pipeline {
    pub(crate) commands: Vec<Cmd>,
    pub(crate) transaction_mode: bool,
    pub(crate) ignored_commands: HashSet<usize>,
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

    /// Returns `true` if the pipeline is in transaction mode (aka atomic mode).
    pub fn is_transaction(&self) -> bool {
        self.transaction_mode
    }

    /// Returns the encoded pipeline commands.
    pub fn get_packed_pipeline(&self) -> Vec<u8> {
        encode_pipeline(&self.commands, self.transaction_mode)
    }

    /// Returns the number of commands currently queued by the usr in the pipeline.
    ///
    /// Depending on its configuration (e.g. `atomic`), the pipeline may send more commands to the server than the returned length
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Returns `true` is the pipeline contains no elements.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
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
                ErrorKind::Client,
                "This connection does not support pipelining."
            ));
        }

        let response = if self.commands.is_empty() {
            vec![]
        } else if self.transaction_mode {
            con.req_packed_commands(
                &encode_pipeline(&self.commands, true),
                self.commands.len() + 1,
                1,
            )?
        } else {
            con.req_packed_commands(
                &encode_pipeline(&self.commands, false),
                0,
                self.commands.len(),
            )?
        };

        self.complete_request(response)
    }

    /// Async version of [Self::query].
    #[inline]
    #[cfg(feature = "aio")]
    pub async fn query_async<T: FromRedisValue>(
        &self,
        con: &mut impl crate::aio::ConnectionLike,
    ) -> RedisResult<T> {
        let response = if self.commands.is_empty() {
            vec![]
        } else if self.transaction_mode {
            con.req_packed_commands(self, self.commands.len() + 1, 1)
                .await?
        } else {
            con.req_packed_commands(self, 0, self.commands.len())
                .await?
        };

        self.complete_request(response)
    }

    /// This is an alternative to [Self::query] that can be used if you want to be able to handle a
    /// command's success or failure but don't care about the command's response. For example,
    /// this is useful for "SET" commands for which the response's content is not important.
    /// It avoids the need to define generic bounds for ().
    #[inline]
    pub fn exec(&self, con: &mut dyn ConnectionLike) -> RedisResult<()> {
        self.query::<()>(con)
    }

    /// This is an alternative to [Self::query_async] that can be used if you want to be able to handle a
    /// command's success or failure but don't care about the command's response. For example,
    /// this is useful for "SET" commands for which the response's content is not important.
    /// It avoids the need to define generic bounds for ().
    #[cfg(feature = "aio")]
    pub async fn exec_async(&self, con: &mut impl crate::aio::ConnectionLike) -> RedisResult<()> {
        self.query_async::<()>(con).await
    }

    fn collect_server_errors_deep(value: &Value, out: &mut Vec<crate::errors::ServerError>) {
        match value {
            Value::ServerError(e) => out.push(e.clone()),
            Value::Array(items) | Value::Set(items) => {
                for v in items {
                    Self::collect_server_errors_deep(v, out);
                }
            }
            Value::Map(entries) => {
                for (k, v) in entries {
                    Self::collect_server_errors_deep(k, out);
                    Self::collect_server_errors_deep(v, out);
                }
            }
            Value::Attribute { data, attributes } => {
                Self::collect_server_errors_deep(data, out);
                for (k, v) in attributes {
                    Self::collect_server_errors_deep(k, out);
                    Self::collect_server_errors_deep(v, out);
                }
            }
            Value::Push { data, .. } => {
                for v in data {
                    Self::collect_server_errors_deep(v, out);
                }
            }
            _ => {}
        }
    }

    fn complete_request<T: FromRedisValue>(&self, mut response: Vec<Value>) -> RedisResult<T> {
        let response = if self.is_transaction() {
            match response.pop() {
                // EXEC returned some items. This is the common case.
                Some(Value::Array(response_items)) => {
                    // If EXECABORT is returned as a top-level server error within the results,
                    // surface it as-is.
                    if let Some(Value::ServerError(err)) = response_items.first() {
                        if err.kind() == Some(ServerErrorKind::ExecAbort) {
                            return Err(err.clone().into());
                        }
                    }

                    // Scan each item for nested server errors (RESP3 may wrap errors)
                    let mut txn_errors: Vec<(usize, crate::errors::ServerError)> = Vec::new();
                    for (idx, v) in response_items.iter().enumerate() {
                        let mut errs = Vec::new();
                        Self::collect_server_errors_deep(v, &mut errs);
                        if let Some(first_err) = errs.into_iter().next() {
                            // Only record a single error per command index
                            txn_errors.push((idx, first_err));
                        }
                    }

                    if !txn_errors.is_empty() {
                        return Err(RedisError::make_aborted_transaction(txn_errors));
                    }

                    response_items
                }

                // EXEC returned nil. Treat as an aborted transaction.
                Some(Value::Nil) => {
                    return Err(RedisError::make_aborted_transaction(Vec::new()));
                }

                // EXEC returned a top-level error (RESP2 style or certain server versions).
                Some(Value::ServerError(err)) => {
                    return Err(RedisError::make_aborted_transaction(vec![(0, err)]));
                }

                // Anything else is unexpected.
                _ => {
                    return Err(RedisError::from((
                        ErrorKind::UnexpectedReturnType,
                        "Received unexpected response from EXEC",
                    )));
                }
            }
        } else {
            // Non-transactional pipeline: if any command (even ignored) returned a server error,
            // return a pipeline error that lists all failing command indices.
            let mut pl_errors: Vec<(usize, crate::errors::ServerError)> = Vec::new();
            for (index, value) in response.iter().enumerate() {
                let mut errs = Vec::new();
                Self::collect_server_errors_deep(value, &mut errs);
                for e in errs {
                    pl_errors.push((index, e));
                }
            }
            if !pl_errors.is_empty() {
                return Err(RedisError::pipeline(pl_errors));
            }

            response
        };

        self.compose_response(response)
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
            ///
            /// On any successful result the value from this command is thrown away.
            /// This makes result processing through tuples much easier because you
            /// do not need to handle all the items you do not care about.
            /// If any command received an error from the server, no result will be ignored,
            /// so that the user could retrace which command failed.
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

            fn filter_ignored_results(&self, resp: Vec<Value>) -> Vec<Value> {
                resp.into_iter()
                    .enumerate()
                    .filter_map(|(index, result)| {
                        (!self.ignored_commands.contains(&index)).then(|| result)
                    })
                    .collect()
            }

            fn compose_response<T: FromRedisValue>(&self, response: Vec<Value>) -> RedisResult<T> {
                // Build a list of all server errors, per index, by attempting to extract
                // errors from each top-level item. This also catches RESP3 nested errors
                // since extract_error() recurses.
                let mut server_errors: Vec<(usize, crate::errors::ServerError)> = Vec::new();
                for (index, value) in response.iter().cloned().enumerate() {
                    if let Err(err) = value.extract_error() {
                        if let Some(items) = err.into_server_errors() {
                            for (_inner_idx, server_err) in items.iter() {
                                server_errors.push((index, server_err.clone()));
                            }
                        }
                    }
                }

                if server_errors.is_empty() {
                    let filtered = self.filter_ignored_results(response);
                    Ok(from_owned_redis_value(
                        Value::Array(filtered).extract_error()?,
                    )?)
                } else {
                    Err(crate::RedisError::pipeline(server_errors))
                }
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

// Defines caching related functions for Pipeline, ClusterPipeline isn't supported yet.
impl Pipeline {
    /// Changes caching behaviour for latest command in the pipeline.
    #[cfg(feature = "cache-aio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cache-aio")))]
    pub fn set_cache_config(&mut self, command_cache_config: CommandCacheConfig) -> &mut Self {
        let cmd = self.get_last_command();
        cmd.set_cache_config(command_cache_config);
        self
    }

    #[cfg(feature = "cluster-async")]
    pub(crate) fn into_cmd_iter(self) -> impl Iterator<Item = Cmd> {
        self.commands.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        errors::{Repr, ServerError},
        pipe, ServerErrorKind,
    };

    use super::*;

    fn test_pipe() -> Pipeline {
        let mut pipeline = pipe();
        pipeline
            .cmd("FOO")
            .cmd("BAR")
            .ignore()
            .cmd("baz")
            .ignore()
            .cmd("barvaz");
        pipeline
    }

    fn server_error() -> Value {
        Value::ServerError(ServerError(Repr::Known {
            kind: ServerErrorKind::CrossSlot,
            detail: None,
        }))
    }

    #[test]
    fn test_pipeline_passes_values_only_from_non_ignored_commands() {
        let pipeline = test_pipe();
        let inputs = vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Okay];
        let result = pipeline.complete_request(inputs);

        let expected = vec!["1".to_string(), "OK".to_string()];
        assert_eq!(result, Ok(expected));
    }

    #[test]
    fn test_pipeline_passes_errors_from_ignored_commands() {
        let pipeline = test_pipe();
        let inputs = vec![Value::Okay, server_error(), Value::Okay, server_error()];
        let error = pipeline.compose_response::<Vec<Value>>(inputs).unwrap_err();
        let error_message = error.to_string();

        assert!(error_message.contains("Index 1"), "{error_message}");
        assert!(error_message.contains("Index 3"), "{error_message}");
    }
}
