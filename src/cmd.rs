use std::{io, pin::Pin};

use crate::connection::ConnectionLike;
use crate::types::{
    from_redis_value, ErrorKind, FromRedisValue, RedisResult, RedisWrite, ToRedisArgs, Value,
};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use futures::prelude::*;

#[derive(Clone)]
enum Arg<D> {
    Simple(D),
    Cursor,
}

/// Represents redis commands.
#[derive(Clone)]
pub struct Cmd {
    data: Vec<u8>,
    // Arg::Simple contains the offset that marks the end of the argument
    args: Vec<Arg<usize>>,
    cursor: Option<u64>,
    is_ignored: bool,
}

/// Represents a redis command pipeline.
#[derive(Clone)]
pub struct Pipeline {
    commands: Vec<Cmd>,
    transaction_mode: bool,
}

/// Represents a redis iterator.
pub struct Iter<'a, T: FromRedisValue> {
    batch: Vec<T>,
    cursor: u64,
    con: &'a mut (dyn ConnectionLike + 'a),
    cmd: Cmd,
}

impl<'a, T: FromRedisValue> Iterator for Iter<'a, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        // we need to do this in a loop until we produce at least one item
        // or we find the actual end of the iteration.  This is necessary
        // because with filtering an iterator it is possible that a whole
        // chunk is not matching the pattern and thus yielding empty results.
        loop {
            if let Some(v) = self.batch.pop() {
                return Some(v);
            };
            if self.cursor == 0 {
                return None;
            }

            let pcmd = unwrap_or!(
                self.cmd.get_packed_command_with_cursor(self.cursor),
                return None
            );
            let rv = unwrap_or!(self.con.req_packed_command(&pcmd).ok(), return None);
            let (cur, mut batch): (u64, Vec<T>) =
                unwrap_or!(from_redis_value(&rv).ok(), return None);
            batch.reverse();

            self.cursor = cur;
            self.batch = batch;
        }
    }
}

fn countdigits(mut v: usize) -> usize {
    let mut result = 1;
    loop {
        if v < 10 {
            return result;
        }
        if v < 100 {
            return result + 1;
        }
        if v < 1000 {
            return result + 2;
        }
        if v < 10000 {
            return result + 3;
        }

        v /= 10000;
        result += 4;
    }
}

#[inline]
fn bulklen(len: usize) -> usize {
    1 + countdigits(len) + 2 + len + 2
}

fn args_len<'a, I>(args: I, cursor: u64) -> usize
where
    I: IntoIterator<Item = Arg<&'a [u8]>> + ExactSizeIterator,
{
    let mut totlen = 1 + countdigits(args.len()) + 2;
    for item in args {
        totlen += bulklen(match item {
            Arg::Cursor => countdigits(cursor as usize),
            Arg::Simple(val) => val.len(),
        });
    }
    totlen
}

fn cmd_len(cmd: &Cmd) -> usize {
    args_len(cmd.args_iter(), cmd.cursor.unwrap_or(0))
}

fn encode_command<'a, I>(args: I, cursor: u64) -> Vec<u8>
where
    I: IntoIterator<Item = Arg<&'a [u8]>> + Clone + ExactSizeIterator,
{
    let mut cmd = Vec::new();
    write_command_to_vec(&mut cmd, args, cursor);
    cmd
}

fn write_command_to_vec<'a, I>(cmd: &mut Vec<u8>, args: I, cursor: u64)
where
    I: IntoIterator<Item = Arg<&'a [u8]>> + Clone + ExactSizeIterator,
{
    let totlen = args_len(args.clone(), cursor);

    cmd.reserve(totlen);

    write_command(cmd, args, cursor).unwrap()
}

fn write_command<'a, I>(cmd: &mut (impl ?Sized + io::Write), args: I, cursor: u64) -> io::Result<()>
where
    I: IntoIterator<Item = Arg<&'a [u8]>> + Clone + ExactSizeIterator,
{
    cmd.write_all(b"*")?;
    ::itoa::write(&mut *cmd, args.len())?;
    cmd.write_all(b"\r\n")?;

    let mut cursor_bytes = itoa::Buffer::new();
    for item in args {
        let bytes = match item {
            Arg::Cursor => cursor_bytes.format(cursor).as_bytes(),
            Arg::Simple(val) => val,
        };

        cmd.write_all(b"$")?;
        ::itoa::write(&mut *cmd, bytes.len())?;
        cmd.write_all(b"\r\n")?;

        cmd.write_all(bytes)?;
        cmd.write_all(b"\r\n")?;
    }
    Ok(())
}

async fn write_command_async<'a, I>(
    mut cmd: Pin<&mut (impl ?Sized + AsyncWrite)>,
    args: I,
    cursor: u64,
) -> io::Result<()>
where
    I: IntoIterator<Item = Arg<&'a [u8]>> + Clone + ExactSizeIterator,
{
    cmd.write_all(b"*").await?;
    cmd.write_all(itoa::Buffer::new().format(args.len()).as_bytes())
        .await?;
    cmd.write_all(b"\r\n").await?;

    let mut cursor_bytes = itoa::Buffer::new();
    for item in args {
        let bytes = match item {
            Arg::Cursor => cursor_bytes.format(cursor).as_bytes(),
            Arg::Simple(val) => val,
        };

        cmd.write_all(b"$").await?;
        cmd.write_all(itoa::Buffer::new().format(bytes.len()).as_bytes())
            .await?;
        cmd.write_all(b"\r\n").await?;

        cmd.write_all(bytes).await?;
        cmd.write_all(b"\r\n").await?;
    }
    Ok(())
}

fn encode_pipeline(cmds: &[Cmd], atomic: bool) -> Vec<u8> {
    let mut rv = vec![];
    let cmds_len = cmds.iter().map(cmd_len).sum();

    if atomic {
        let multi = cmd("MULTI");
        let exec = cmd("EXEC");
        rv.reserve(cmd_len(&multi) + cmd_len(&exec) + cmds_len);

        multi.write_packed_command_preallocated(&mut rv);
        for cmd in cmds {
            cmd.write_packed_command_preallocated(&mut rv);
        }
        exec.write_packed_command_preallocated(&mut rv);
    } else {
        rv.reserve(cmds_len);

        for cmd in cmds {
            cmd.write_packed_command_preallocated(&mut rv);
        }
    }
    rv
}

async fn write_pipeline_async(
    mut out: Pin<&mut (impl ?Sized + AsyncWrite)>,
    cmds: &[Cmd],
    atomic: bool,
) -> io::Result<()> {
    if atomic {
        cmd("MULTI").write_command_async(out.as_mut()).await?;
        for cmd in cmds {
            cmd.write_command_async(out.as_mut()).await?;
        }
        cmd("EXEC").write_command_async(out.as_mut()).await?;
    } else {
        for cmd in cmds {
            cmd.write_command_async(out.as_mut()).await?;
        }
    }
    Ok(())
}

impl RedisWrite for Cmd {
    fn write_arg(&mut self, arg: &[u8]) {
        let prev = self.data.len();
        self.args.push(Arg::Simple(prev + arg.len()));
        self.data.extend_from_slice(arg);
    }
}

impl Default for Cmd {
    fn default() -> Cmd {
        Cmd::new()
    }
}

/// A command acts as a builder interface to creating encoded redis
/// requests.  This allows you to easiy assemble a packed command
/// by chaining arguments together.
///
/// Basic example:
///
/// ```rust
/// redis::Cmd::new().arg("SET").arg("my_key").arg(42);
/// ```
///
/// There is also a helper function called `cmd` which makes it a
/// tiny bit shorter:
///
/// ```rust
/// redis::cmd("SET").arg("my_key").arg(42);
/// ```
///
/// Because currently rust's currently does not have an ideal system
/// for lifetimes of temporaries, sometimes you need to hold on to
/// the initially generated command:
///
/// ```rust,no_run
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let mut con = client.get_connection().unwrap();
/// let mut cmd = redis::cmd("SMEMBERS");
/// let mut iter : redis::Iter<i32> = cmd.arg("my_set").clone().iter(&mut con).unwrap();
/// ```
impl Cmd {
    /// Creates a new empty command.
    pub fn new() -> Cmd {
        Cmd {
            data: vec![],
            args: vec![],
            cursor: None,
            is_ignored: false,
        }
    }

    /// Appends an argument to the command.  The argument passed must
    /// be a type that implements `ToRedisArgs`.  Most primitive types as
    /// well as vectors of primitive types implement it.
    ///
    /// For instance all of the following are valid:
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let mut con = client.get_connection().unwrap();
    /// redis::cmd("SET").arg(&["my_key", "my_value"]);
    /// redis::cmd("SET").arg("my_key").arg(42);
    /// redis::cmd("SET").arg("my_key").arg(b"my_value");
    /// ```
    #[inline]
    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Cmd {
        arg.write_redis_args(self);
        self
    }

    /// Works similar to `arg` but adds a cursor argument.  This is always
    /// an integer and also flips the command implementation to support a
    /// different mode for the iterators where the iterator will ask for
    /// another batch of items when the local data is exhausted.
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let mut con = client.get_connection().unwrap();
    /// let mut cmd = redis::cmd("SSCAN");
    /// let mut iter : redis::Iter<isize> =
    ///     cmd.arg("my_set").cursor_arg(0).clone().iter(&mut con).unwrap();
    /// for x in iter {
    ///     // do something with the item
    /// }
    /// ```
    #[inline]
    pub fn cursor_arg(&mut self, cursor: u64) -> &mut Cmd {
        assert!(!self.in_scan_mode());
        self.cursor = Some(cursor);
        self.args.push(Arg::Cursor);
        self
    }

    /// Returns the packed command as a byte vector.
    #[inline]
    pub fn get_packed_command(&self) -> Vec<u8> {
        let mut cmd = Vec::new();
        self.write_packed_command(&mut cmd);
        cmd
    }

    pub(crate) async fn write_command_async(
        &self,
        out: Pin<&mut (impl ?Sized + AsyncWrite)>,
    ) -> io::Result<()> {
        write_command_async(out, self.args_iter(), self.cursor.unwrap_or(0)).await
    }

    fn write_packed_command(&self, cmd: &mut Vec<u8>) {
        write_command_to_vec(cmd, self.args_iter(), self.cursor.unwrap_or(0))
    }

    fn write_packed_command_preallocated(&self, cmd: &mut Vec<u8>) {
        write_command(cmd, self.args_iter(), self.cursor.unwrap_or(0)).unwrap()
    }

    /// Like `get_packed_command` but replaces the cursor with the
    /// provided value.  If the command is not in scan mode, `None`
    /// is returned.
    #[inline]
    fn get_packed_command_with_cursor(&self, cursor: u64) -> Option<Vec<u8>> {
        if !self.in_scan_mode() {
            None
        } else {
            Some(encode_command(self.args_iter(), cursor))
        }
    }

    /// Returns true if the command is in scan mode.
    #[inline]
    pub fn in_scan_mode(&self) -> bool {
        self.cursor.is_some()
    }

    /// Sends the command as query to the connection and converts the
    /// result to the target redis value.  This is the general way how
    /// you can retrieve data.
    #[inline]
    pub fn query<T: FromRedisValue>(&self, con: &mut dyn ConnectionLike) -> RedisResult<T> {
        let pcmd = self.get_packed_command();
        match con.req_packed_command(&pcmd) {
            Ok(val) => from_redis_value(&val),
            Err(e) => Err(e),
        }
    }

    /// Async version of `query`.
    #[inline]
    pub fn query_async<'c, C, T: FromRedisValue>(
        &'c self,
        con: &'c mut C,
    ) -> impl Future<Output = RedisResult<T>> + 'c
    where
        C: crate::aio::ConnectionLike + Send + 'static,
        T: Send + 'static,
    {
        async move {
            let val = con.req_packed_command(self).await?;
            from_redis_value(&val)
        }
    }

    /// Similar to `query()` but returns an iterator over the items of the
    /// bulk result or iterator.  In normal mode this is not in any way more
    /// efficient than just querying into a `Vec<T>` as it's internally
    /// implemented as buffering into a vector.  This however is useful when
    /// `cursor_arg` was used in which case the iterator will query for more
    /// items until the server side cursor is exhausted.
    ///
    /// This is useful for commands such as `SSCAN`, `SCAN` and others.
    ///
    /// One speciality of this function is that it will check if the response
    /// looks like a cursor or not and always just looks at the payload.
    /// This way you can use the function the same for responses in the
    /// format of `KEYS` (just a list) as well as `SSCAN` (which returns a
    /// tuple of cursor and list).
    #[inline]
    pub fn iter<'a, T: FromRedisValue>(
        self,
        con: &'a mut dyn ConnectionLike,
    ) -> RedisResult<Iter<'a, T>> {
        let pcmd = self.get_packed_command();
        let rv = con.req_packed_command(&pcmd)?;

        let (cursor, mut batch) = if rv.looks_like_cursor() {
            from_redis_value::<(u64, Vec<T>)>(&rv)?
        } else {
            (0, from_redis_value(&rv)?)
        };

        batch.reverse();
        Ok(Iter {
            batch,
            cursor,
            con,
            cmd: self,
        })
    }

    /// This is a shortcut to `query()` that does not return a value and
    /// will fail the task if the query fails because of an error.  This is
    /// mainly useful in examples and for simple commands like setting
    /// keys.
    ///
    /// This is equivalent to a call of query like this:
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let mut con = client.get_connection().unwrap();
    /// let _ : () = redis::cmd("PING").query(&mut con).unwrap();
    /// ```
    #[inline]
    pub fn execute(&self, con: &mut dyn ConnectionLike) {
        self.query::<()>(con).unwrap();
    }

    fn args_iter(&self) -> impl Iterator<Item = Arg<&[u8]>> + Clone + ExactSizeIterator {
        let mut prev = 0;
        self.args.iter().map(move |arg| match *arg {
            Arg::Simple(i) => {
                let arg = Arg::Simple(&self.data[prev..i]);
                prev = i;
                arg
            }

            Arg::Cursor => Arg::Cursor,
        })
    }
}

impl Default for Pipeline {
    fn default() -> Pipeline {
        Pipeline::new()
    }
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
        }
    }

    /// Starts a new command.  Functions such as `arg` then become
    /// available to add more arguments to that command.
    #[inline]
    pub fn cmd(&mut self, name: &str) -> &mut Pipeline {
        self.commands.push(cmd(name));
        self
    }

    /// Adds a command to the pipeline.
    #[inline]
    pub fn add_command(&mut self, cmd: Cmd) -> &mut Pipeline {
        self.commands.push(cmd);
        self
    }

    #[inline]
    fn get_last_command(&mut self) -> &mut Cmd {
        let idx = match self.commands.len() {
            0 => panic!("No command on stack"),
            x => x - 1,
        };
        &mut self.commands[idx]
    }

    /// Adds an argument to the last started command.  This works similar
    /// to the `arg` method of the `Cmd` object.
    ///
    /// Note that this function fails the task if executed on an empty pipeline.
    #[inline]
    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Pipeline {
        {
            let cmd = self.get_last_command();
            cmd.arg(arg);
        }
        self
    }

    /// Instructs the pipeline to ignore the return value of this command.
    /// It will still be ensured that it is not an error, but any successful
    /// result is just thrown away.  This makes result processing through
    /// tuples much easier because you do not need to handle all the items
    /// you do not care about.
    ///
    /// Note that this function fails the task if executed on an empty pipeline.
    #[inline]
    pub fn ignore(&mut self) -> &mut Pipeline {
        {
            let cmd = self.get_last_command();
            cmd.is_ignored = true;
        }
        self
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

    fn make_pipeline_results(&self, resp: Vec<Value>) -> Value {
        let mut rv = vec![];
        for (idx, result) in resp.into_iter().enumerate() {
            if !self.commands[idx].is_ignored {
                rv.push(result);
            }
        }
        Value::Bulk(rv)
    }

    /// Returns the encoded pipeline commands.
    pub fn get_packed_pipeline(&self) -> Vec<u8> {
        encode_pipeline(&self.commands, self.transaction_mode)
    }

    pub(crate) async fn write_pipeline_async(
        &self,
        out: Pin<&mut (impl ?Sized + AsyncWrite)>,
    ) -> io::Result<()> {
        write_pipeline_async(out, &self.commands, self.transaction_mode).await
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
        from_redis_value(
            &(if self.commands.is_empty() {
                Value::Bulk(vec![])
            } else if self.transaction_mode {
                self.execute_transaction(con)?
            } else {
                self.execute_pipelined(con)?
            }),
        )
    }

    /// Clear a Pipeline object internal data structure.
    ///
    /// This allows reusing a Pipeline object as a clear object while performing a minimal amount of
    /// memory released/reallocated.
    #[inline]
    pub fn clear(&mut self) {
        self.commands.clear();
    }

    async fn execute_pipelined_async<C>(&self, con: &mut C) -> RedisResult<Value>
    where
        C: crate::aio::ConnectionLike + Send + 'static,
    {
        let value = con
            .req_packed_commands(self, 0, self.commands.len())
            .await?;
        Ok(self.make_pipeline_results(value))
    }

    async fn execute_transaction_async<C>(&self, con: &mut C) -> RedisResult<Value>
    where
        C: crate::aio::ConnectionLike + Send + 'static,
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
    pub async fn query_async<C, T: FromRedisValue>(self, con: &mut C) -> RedisResult<T>
    where
        C: crate::aio::ConnectionLike + Send + 'static,
        T: Send + 'static,
    {
        let v = if self.commands.is_empty() {
            return from_redis_value(&Value::Bulk(vec![]));
        } else if self.transaction_mode {
            self.execute_transaction_async(con).await?
        } else {
            self.execute_pipelined_async(con).await?
        };
        from_redis_value(&v)
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

/// Shortcut function to creating a command with a single argument.
///
/// The first argument of a redis command is always the name of the command
/// which needs to be a string.  This is the recommended way to start a
/// command pipe.
///
/// ```rust
/// redis::cmd("PING");
/// ```
pub fn cmd(name: &str) -> Cmd {
    let mut rv = Cmd::new();
    rv.arg(name);
    rv
}

/// Packs a bunch of commands into a request.  This is generally a quite
/// useless function as this functionality is nicely wrapped through the
/// `Cmd` object, but in some cases it can be useful.  The return value
/// of this can then be send to the low level `ConnectionLike` methods.
///
/// Example:
///
/// ```rust
/// # use redis::ToRedisArgs;
/// let mut args = vec![];
/// args.extend("SET".to_redis_args());
/// args.extend("my_key".to_redis_args());
/// args.extend(42.to_redis_args());
/// let cmd = redis::pack_command(&args);
/// assert_eq!(cmd, b"*3\r\n$3\r\nSET\r\n$6\r\nmy_key\r\n$2\r\n42\r\n".to_vec());
/// ```
pub fn pack_command(args: &[Vec<u8>]) -> Vec<u8> {
    encode_command(args.iter().map(|x| Arg::Simple(&x[..])), 0)
}

/// Shortcut for creating a new pipeline.
pub fn pipe() -> Pipeline {
    Pipeline::new()
}
