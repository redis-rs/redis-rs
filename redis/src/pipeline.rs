#![macro_use]

#[cfg(feature = "cache-aio")]
use crate::cmd::CommandCacheConfig;
use crate::cmd::{Arg, Cmd, CmdRef, args_len, write_command};
use crate::connection::ConnectionLike;
use crate::errors::ErrorKind;
use crate::types::{FromRedisValue, RedisResult, ToRedisArgs, Value, from_redis_value};

/// Per-command metadata in a flattened pipeline.
///
/// The command's arguments are `args[args_start..next.args_start]` and its argument bytes are
/// `data[data_start..next.data_start]`, where `next` is the following command's record (or the
/// buffer ends for the last command). Keeping a small record per command — rather than the
/// boundary in the hot `args` buffer — allows O(1) jumps between commands while keeping each
/// `args` entry as small as a standalone `Cmd`'s.
#[derive(Clone, Debug)]
pub(crate) struct CommandRecord {
    pub(crate) args_start: usize,
    pub(crate) data_start: usize,
    pub(crate) cursor: Option<u64>,
    pub(crate) ignored: bool,
    pub(crate) no_response: bool,
    #[cfg(feature = "cache-aio")]
    pub(crate) cache: Option<CommandCacheConfig>,
}

/// Represents a redis command pipeline.
#[derive(Clone, Debug)]
pub struct Pipeline {
    pub(crate) data: Vec<u8>,
    pub(crate) args: Vec<Arg<usize>>,
    pub(crate) commands: Vec<CommandRecord>,
    pub(crate) transaction_mode: bool,
    pub(crate) ignore_errors: bool,
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
    ///
    /// To pre-allocate the internal buffers when you have a size estimate, chain the
    /// [`reserve_for_commands`](Self::reserve_for_commands),
    /// [`reserve_for_args`](Self::reserve_for_args), and
    /// [`reserve_for_data`](Self::reserve_for_data) methods.
    pub fn new() -> Pipeline {
        Pipeline {
            data: Vec::new(),
            args: Vec::new(),
            commands: Vec::new(),
            transaction_mode: false,
            ignore_errors: false,
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

    /// Configures the pipeline to return partial results even if some commands fail.
    ///
    /// By default, if any command within the pipeline returns an error from the Redis server,
    /// the `query()` will only return errors, and not the results of the successful calls.
    ///
    /// When set:
    /// 1. The pipeline executes all commands.
    /// 2. `query()` returns `Ok` (unless there is a connection/IO error).
    /// 3. The results of individual commands are returned in the collection. If a command failed,
    ///    its corresponding element in the collection will be an `Err`.
    ///
    /// This allows handling partial successes where some commands succeed and others fail.
    ///
    /// **Note on ignored commands:** If you use `.ignore()` on a command, its result is discarded
    /// regardless of success or failure. If `ignore_errors()` is set, errors from ignored
    /// commands are also silently discarded and do not affect the returned result.
    ///
    /// **Note on Transactions (`atomic()`):** When used with `atomic()`, this method allows you to inspect
    /// the results of individual commands within the transaction. Redis transactions do not rollback on
    /// response errors (like `Path .path not exists`), so some commands may succeed while others fail. Using `ignore_errors()`
    /// enables you to see which commands in the transaction succeeded and which failed, rather than receiving
    /// a single aggregate error for the entire transaction.
    ///
    /// **Return Type:** When enabling this, you must use a collection of `RedisResult<T>`
    /// (e.g., `Vec<RedisResult<T>>`) or `Value` to capture both successes and failures.
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let mut con = client.get_connection().unwrap();
    /// let mut pipe = redis::pipe();
    /// pipe.set("key_a", 1)
    ///     .hset("key_a", "field", "val") // This will fail (WRONGTYPE)
    ///     .get("key_a");
    ///
    /// // Note the return type: Vec<RedisResult<i32>>
    /// let results: Vec<redis::RedisResult<i32>> = pipe
    ///     .ignore_errors()
    ///     .query(&mut con)
    ///     .unwrap(); // The query itself succeeds
    ///
    /// assert!(results[0].is_ok()); // set succeeded
    /// assert!(results[1].is_err()); // hset failed
    /// assert!(results[2].is_ok()); // get succeeded
    /// ```
    #[inline]
    pub fn ignore_errors(&mut self) -> &mut Pipeline {
        self.ignore_errors = true;
        self
    }

    /// Returns `true` if the pipeline is in transaction mode (aka atomic mode).
    pub fn is_transaction(&self) -> bool {
        self.transaction_mode
    }

    /// Returns the encoded pipeline commands.
    pub fn get_packed_pipeline(&self) -> Vec<u8> {
        encode_pipeline(self, self.transaction_mode)
    }

    /// Returns the `(args, data_start, cursor)` for the command at `idx`, used by the encoder to
    /// walk the flat buffers directly.
    fn command_parts(&self, idx: usize) -> (&[Arg<usize>], usize, u64) {
        let record = &self.commands[idx];
        let args_end = self
            .commands
            .get(idx + 1)
            .map_or(self.args.len(), |r| r.args_start);
        (
            &self.args[record.args_start..args_end],
            record.data_start,
            record.cursor.unwrap_or(0),
        )
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
    ///
    /// NOTE: This method returns a collection of results, even when there's only a single response.
    ///       Make sure to wrap single-result values in a collection. For example:
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let mut con = client.get_connection().unwrap();
    /// let (k1,): (i32,) = redis::pipe()
    ///     .cmd("SET").arg("key_1").arg(42).ignore()
    ///     .cmd("GET").arg("key_1").query(&mut con).unwrap();
    /// ```
    #[inline]
    pub fn query<T: FromRedisValue>(&self, con: &mut dyn ConnectionLike) -> RedisResult<T> {
        if !con.supports_pipelining() {
            fail!((
                ErrorKind::Client,
                "This connection does not support pipelining."
            ));
        }

        let response = if self.transaction_mode {
            con.req_packed_commands(&encode_pipeline(self, true), self.len() + 1, 1)?
        } else {
            con.req_packed_commands(&encode_pipeline(self, false), 0, self.len())?
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
        let response = if self.transaction_mode {
            con.req_packed_commands(self, self.len() + 1, 1).await?
        } else {
            con.req_packed_commands(self, 0, self.len()).await?
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

    fn complete_request<T: FromRedisValue>(&self, mut response: Vec<Value>) -> RedisResult<T> {
        let response = if self.is_transaction() {
            match response.pop() {
                Some(Value::Nil) => {
                    return Ok(from_redis_value(Value::Nil)?);
                }
                Some(Value::Array(items)) => items,
                _ => {
                    return Err((
                        ErrorKind::UnexpectedReturnType,
                        "Invalid response when parsing multi response",
                    )
                        .into());
                }
            }
        } else {
            response
        };

        self.compose_response(response)
    }
}

fn encode_pipeline(pipe: &Pipeline, atomic: bool) -> Vec<u8> {
    let mut rv = vec![];
    write_pipeline(&mut rv, pipe, atomic);
    rv
}

// Pre-encoded RESP frames for the transaction wrapper. These are constant, so there's no need
// to build (and allocate) `Cmd`s just to encode them.
const PACKED_MULTI: &[u8] = b"*1\r\n$5\r\nMULTI\r\n";
const PACKED_EXEC: &[u8] = b"*1\r\n$4\r\nEXEC\r\n";

/// Builds an iterator over a single command's arguments, slicing directly into the pipeline's
/// shared `data` buffer using absolute offsets. Unlike [`CmdRef::args_iter`], this needs no
/// per-argument rebasing, which keeps the hot encode path tight.
fn command_args_iter<'a>(
    data: &'a [u8],
    args: &'a [Arg<usize>],
    data_start: usize,
) -> impl Clone + ExactSizeIterator<Item = Arg<&'a [u8]>> {
    let mut prev = data_start;
    args.iter().map(move |arg| match *arg {
        Arg::Simple(end) => {
            let arg = Arg::Simple(&data[prev..end]);
            prev = end;
            arg
        }
        Arg::Cursor => Arg::Cursor,
    })
}

fn write_pipeline(rv: &mut Vec<u8>, pipe: &Pipeline, atomic: bool) {
    let cmds_len: usize = (0..pipe.commands.len())
        .map(|idx| {
            let (args, data_start, cursor) = pipe.command_parts(idx);
            args_len(command_args_iter(&pipe.data, args, data_start), cursor)
        })
        .sum();

    let write_commands = |rv: &mut Vec<u8>| {
        for idx in 0..pipe.commands.len() {
            let (args, data_start, cursor) = pipe.command_parts(idx);
            write_command(rv, command_args_iter(&pipe.data, args, data_start), cursor).unwrap();
        }
    };

    if atomic {
        rv.reserve(PACKED_MULTI.len() + PACKED_EXEC.len() + cmds_len);
        rv.extend_from_slice(PACKED_MULTI);
        write_commands(rv);
        rv.extend_from_slice(PACKED_EXEC);
    } else {
        rv.reserve(cmds_len);
        write_commands(rv);
    }
}

// Macro to implement shared methods between Pipeline and ClusterPipeline
macro_rules! implement_pipeline_commands {
    ($struct_name:ident) => {
        impl $struct_name {
            /// Pushes a new, empty command record. Subsequent arguments written through
            /// [`RedisWrite`](crate::RedisWrite) belong to it until the next command starts.
            #[inline]
            fn start_command(&mut self) {
                self.commands.push($crate::pipeline::CommandRecord {
                    args_start: self.args.len(),
                    data_start: self.data.len(),
                    cursor: None,
                    ignored: false,
                    no_response: false,
                    #[cfg(feature = "cache-aio")]
                    cache: None,
                });
            }

            /// Adds a command to the pipeline.
            #[inline]
            pub fn add_command(&mut self, cmd: Cmd) -> &mut Self {
                self.start_command();
                for arg in cmd.args_iter() {
                    match arg {
                        Arg::Simple(bytes) => $crate::types::RedisWrite::write_arg(self, bytes),
                        Arg::Cursor => self.args.push(Arg::Cursor),
                    }
                }
                if let Some(record) = self.commands.last_mut() {
                    record.cursor = cmd.cursor_value();
                    record.no_response = cmd.is_no_response();
                    #[cfg(feature = "cache-aio")]
                    {
                        record.cache = cmd.get_cache_config().clone();
                    }
                }
                self
            }

            /// Starts a new command. Functions such as `arg` then become
            /// available to add more arguments to that command.
            #[inline]
            pub fn cmd(&mut self, name: &str) -> &mut Self {
                self.start_command();
                $crate::types::RedisWrite::write_arg(self, name.as_bytes());
                self
            }

            /// Returns an iterator over all the commands currently in this pipeline
            pub fn cmd_iter(&self) -> impl Iterator<Item = CmdRef<'_>> {
                (0..self.commands.len()).map(move |idx| self.command_ref(idx))
            }

            /// Reserves capacity for at least `additional` more commands.
            ///
            /// This is a hint for pre-allocation; the pipeline grows automatically as needed.
            #[inline]
            pub fn reserve_for_commands(&mut self, additional: usize) -> &mut Self {
                self.commands.reserve(additional);
                self
            }

            /// Reserves capacity for at least `additional` more arguments, counted across all
            /// commands.
            ///
            /// This is a hint for pre-allocation; the pipeline grows automatically as needed.
            #[inline]
            pub fn reserve_for_args(&mut self, additional: usize) -> &mut Self {
                self.args.reserve(additional);
                self
            }

            /// Reserves capacity for at least `additional` more argument bytes, counted across all
            /// commands.
            ///
            /// This is a hint for pre-allocation; the pipeline grows automatically as needed.
            #[inline]
            pub fn reserve_for_data(&mut self, additional: usize) -> &mut Self {
                self.data.reserve(additional);
                self
            }

            /// Builds a borrowed view of the command at `idx`.
            #[inline]
            fn command_ref(&self, idx: usize) -> CmdRef<'_> {
                let record = &self.commands[idx];
                let next = self.commands.get(idx + 1);
                let args_end = next.map_or(self.args.len(), |r| r.args_start);
                let data_end = next.map_or(self.data.len(), |r| r.data_start);
                CmdRef::new(
                    &self.data[record.data_start..data_end],
                    &self.args[record.args_start..args_end],
                    record.data_start,
                    record.cursor,
                    record.no_response,
                    record.ignored,
                    #[cfg(feature = "cache-aio")]
                    &record.cache,
                )
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
                if let Some(record) = self.commands.last_mut() {
                    record.ignored = true;
                }
                self
            }

            /// Adds an argument to the last started command. This works similar
            /// to the `arg` method of the `Cmd` object.
            ///
            /// Note that this function fails the task if executed on an empty pipeline.
            #[inline]
            pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Self {
                assert!(!self.commands.is_empty(), "No command on stack");
                arg.write_redis_args(self);
                self
            }

            /// Clear a pipeline object's internal data structure.
            ///
            /// This allows reusing a pipeline object as a clear object while performing a minimal
            /// amount of memory released/reallocated.
            #[inline]
            pub fn clear(&mut self) {
                self.data.clear();
                self.args.clear();
                self.commands.clear();
            }

            fn filter_ignored_results(&self, resp: Vec<Value>) -> Vec<Value> {
                resp.into_iter()
                    .enumerate()
                    .filter_map(|(index, result)| {
                        let ignored = self
                            .commands
                            .get(index)
                            .is_some_and(|record| record.ignored);
                        (!ignored).then_some(result)
                    })
                    .collect()
            }

            fn compose_response<T: FromRedisValue>(&self, response: Vec<Value>) -> RedisResult<T> {
                if self.ignore_errors {
                    return Ok(from_redis_value(Value::Array(
                        self.filter_ignored_results(response),
                    ))?);
                }

                let server_errors: Vec<_> = response
                    .iter()
                    .enumerate()
                    .filter_map(|(index, value)| match value {
                        Value::ServerError(error) => Some((index, error.clone())),
                        _ => None,
                    })
                    .collect();

                if server_errors.is_empty() {
                    Ok(from_redis_value(
                        Value::Array(self.filter_ignored_results(response)).extract_error()?,
                    )?)
                } else {
                    Err(crate::RedisError::pipeline(server_errors))
                }
            }
        }

        impl $crate::types::RedisWrite for $struct_name {
            fn write_arg(&mut self, arg: &[u8]) {
                self.data.extend_from_slice(arg);
                self.args.push(Arg::Simple(self.data.len()));
            }

            fn write_arg_fmt(&mut self, arg: impl std::fmt::Display) {
                use std::io::Write as _;
                write!(self.data, "{arg}").unwrap();
                self.args.push(Arg::Simple(self.data.len()));
            }

            fn writer_for_next_arg(&mut self) -> impl std::io::Write + '_ {
                struct PipelineArgGuard<'a>(&'a mut $struct_name);
                impl Drop for PipelineArgGuard<'_> {
                    fn drop(&mut self) {
                        self.0.args.push(Arg::Simple(self.0.data.len()));
                    }
                }
                impl std::io::Write for PipelineArgGuard<'_> {
                    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                        self.0.data.extend_from_slice(buf);
                        Ok(buf.len())
                    }

                    fn flush(&mut self) -> std::io::Result<()> {
                        Ok(())
                    }
                }

                PipelineArgGuard(self)
            }

            fn reserve_space_for_args(&mut self, additional: impl IntoIterator<Item = usize>) {
                let mut capacity = 0;
                let mut args = 0;
                for add in additional {
                    capacity += add;
                    args += 1;
                }
                self.data.reserve(capacity);
                self.args.reserve(args);
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
        if let Some(record) = self.commands.last_mut() {
            record.cache = Some(command_cache_config);
        }
        self
    }

    /// Appends a command from a borrowed [`CmdRef`] without allocating an intermediate [`Cmd`].
    ///
    /// This is the borrowing counterpart of [`add_command`](Self::add_command), used by the
    /// caching layer to repack commands it inspected as `CmdRef`s.
    #[cfg(feature = "cache-aio")]
    pub(crate) fn add_command_ref(&mut self, cmd: CmdRef<'_>) -> &mut Self {
        self.start_command();
        for arg in cmd.args_iter() {
            match arg {
                Arg::Simple(bytes) => crate::types::RedisWrite::write_arg(self, bytes),
                Arg::Cursor => self.args.push(Arg::Cursor),
            }
        }
        if let Some(record) = self.commands.last_mut() {
            record.cursor = cmd.cursor();
            record.no_response = cmd.is_no_response();
            record.cache = cmd.get_cache_config().clone();
        }
        self
    }

    #[cfg(feature = "cluster-async")]
    pub(crate) fn into_cmd_iter(self) -> impl Iterator<Item = Cmd> {
        self.cmd_iter()
            .map(|cmd| cmd.to_cmd())
            .collect::<Vec<_>>()
            .into_iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ServerErrorKind,
        errors::{RedisError, Repr, ServerError},
        pipe,
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

    #[test]
    fn test_pipeline_passes_response_from_each_commands_despite_errors() {
        let mut pipeline = test_pipe();
        let inputs = vec![Value::Okay, server_error(), Value::Okay, server_error()];
        let result = pipeline
            .ignore_errors()
            .compose_response::<Vec<RedisResult<Value>>>(inputs)
            .unwrap();

        assert_eq!(result[0], Ok::<Value, RedisError>(Value::Okay));
        assert!(
            result[1]
                .clone()
                .is_err_and(|e| e.to_string().contains("CrossSlot"))
        )
    }
}
