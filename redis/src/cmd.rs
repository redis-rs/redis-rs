#[cfg(feature = "aio")]
use futures_util::{
    future::BoxFuture,
    task::{Context, Poll},
    Stream, StreamExt,
};
#[cfg(feature = "aio")]
use std::pin::Pin;
use std::{fmt, io};

use crate::connection::ConnectionLike;
use crate::pipeline::Pipeline;
use crate::types::{from_owned_redis_value, FromRedisValue, RedisResult, RedisWrite, ToRedisArgs};

/// An argument to a redis command
#[derive(Clone)]
pub enum Arg<D> {
    /// A normal argument
    Simple(D),
    /// A cursor argument created from `cursor_arg()`
    Cursor,
}

/// Represents redis commands.
#[derive(Clone)]
pub struct Cmd {
    data: Vec<u8>,
    // Arg::Simple contains the offset that marks the end of the argument
    args: Vec<Arg<usize>>,
    cursor: Option<u64>,
}

/// Represents a redis iterator.
pub struct Iter<'a, T: FromRedisValue> {
    batch: std::vec::IntoIter<T>,
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
            if let Some(v) = self.batch.next() {
                return Some(v);
            };
            if self.cursor == 0 {
                return None;
            }

            let pcmd = self.cmd.get_packed_command_with_cursor(self.cursor)?;
            let rv = self.con.req_packed_command(&pcmd).ok()?;
            let (cur, batch): (u64, Vec<T>) = from_owned_redis_value(rv).ok()?;

            self.cursor = cur;
            self.batch = batch.into_iter();
        }
    }
}

#[cfg(feature = "aio")]
use crate::aio::ConnectionLike as AsyncConnection;

/// The inner future of AsyncIter
#[cfg(feature = "aio")]
struct AsyncIterInner<'a, T: FromRedisValue + 'a> {
    batch: std::vec::IntoIter<T>,
    con: &'a mut (dyn AsyncConnection + Send + 'a),
    cmd: Cmd,
}

/// Represents the state of AsyncIter
#[cfg(feature = "aio")]
enum IterOrFuture<'a, T: FromRedisValue + 'a> {
    Iter(AsyncIterInner<'a, T>),
    Future(BoxFuture<'a, (AsyncIterInner<'a, T>, Option<T>)>),
    Empty,
}

/// Represents a redis iterator that can be used with async connections.
#[cfg(feature = "aio")]
pub struct AsyncIter<'a, T: FromRedisValue + 'a> {
    inner: IterOrFuture<'a, T>,
}

#[cfg(feature = "aio")]
impl<'a, T: FromRedisValue + 'a> AsyncIterInner<'a, T> {
    #[inline]
    pub async fn next_item(&mut self) -> Option<T> {
        // we need to do this in a loop until we produce at least one item
        // or we find the actual end of the iteration.  This is necessary
        // because with filtering an iterator it is possible that a whole
        // chunk is not matching the pattern and thus yielding empty results.
        loop {
            if let Some(v) = self.batch.next() {
                return Some(v);
            };
            if let Some(cursor) = self.cmd.cursor {
                if cursor == 0 {
                    return None;
                }
            } else {
                return None;
            }

            let rv = self.con.req_packed_command(&self.cmd).await.ok()?;
            let (cur, batch): (u64, Vec<T>) = from_owned_redis_value(rv).ok()?;

            self.cmd.cursor = Some(cur);
            self.batch = batch.into_iter();
        }
    }
}

#[cfg(feature = "aio")]
impl<'a, T: FromRedisValue + 'a + Unpin + Send> AsyncIter<'a, T> {
    /// ```rust,no_run
    /// # use redis::AsyncCommands;
    /// # async fn scan_set() -> redis::RedisResult<()> {
    /// # let client = redis::Client::open("redis://127.0.0.1/")?;
    /// # let mut con = client.get_async_connection().await?;
    /// con.sadd("my_set", 42i32).await?;
    /// con.sadd("my_set", 43i32).await?;
    /// let mut iter: redis::AsyncIter<i32> = con.sscan("my_set").await?;
    /// while let Some(element) = iter.next_item().await {
    ///     assert!(element == 42 || element == 43);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub async fn next_item(&mut self) -> Option<T> {
        StreamExt::next(self).await
    }
}

#[cfg(feature = "aio")]
impl<'a, T: FromRedisValue + Unpin + Send + 'a> Stream for AsyncIter<'a, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let this = self.get_mut();
        let inner = std::mem::replace(&mut this.inner, IterOrFuture::Empty);
        match inner {
            IterOrFuture::Iter(mut iter) => {
                let fut = async move {
                    let next_item = iter.next_item().await;
                    (iter, next_item)
                };
                this.inner = IterOrFuture::Future(Box::pin(fut));
                Pin::new(this).poll_next(cx)
            }
            IterOrFuture::Future(mut fut) => match fut.as_mut().poll(cx) {
                Poll::Pending => {
                    this.inner = IterOrFuture::Future(fut);
                    Poll::Pending
                }
                Poll::Ready((iter, value)) => {
                    this.inner = IterOrFuture::Iter(iter);
                    Poll::Ready(value)
                }
            },
            IterOrFuture::Empty => unreachable!(),
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

pub(crate) fn cmd_len(cmd: &Cmd) -> usize {
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
    let mut buf = ::itoa::Buffer::new();

    cmd.write_all(b"*")?;
    let s = buf.format(args.len());
    cmd.write_all(s.as_bytes())?;
    cmd.write_all(b"\r\n")?;

    let mut cursor_bytes = itoa::Buffer::new();
    for item in args {
        let bytes = match item {
            Arg::Cursor => cursor_bytes.format(cursor).as_bytes(),
            Arg::Simple(val) => val,
        };

        cmd.write_all(b"$")?;
        let s = buf.format(bytes.len());
        cmd.write_all(s.as_bytes())?;
        cmd.write_all(b"\r\n")?;

        cmd.write_all(bytes)?;
        cmd.write_all(b"\r\n")?;
    }
    Ok(())
}

impl RedisWrite for Cmd {
    fn write_arg(&mut self, arg: &[u8]) {
        self.data.extend_from_slice(arg);
        self.args.push(Arg::Simple(self.data.len()));
    }

    fn write_arg_fmt(&mut self, arg: impl fmt::Display) {
        use std::io::Write;
        write!(self.data, "{arg}").unwrap();
        self.args.push(Arg::Simple(self.data.len()));
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
/// Because Rust currently does not have an ideal system
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
        }
    }

    /// Creates a new empty command, with at least the requested capcity.
    pub fn with_capacity(arg_count: usize, size_of_data: usize) -> Cmd {
        Cmd {
            data: Vec::with_capacity(size_of_data),
            args: Vec::with_capacity(arg_count),
            cursor: None,
        }
    }

    /// Get the capacities for the internal buffers.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> (usize, usize) {
        (self.args.capacity(), self.data.capacity())
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

    pub(crate) fn write_packed_command(&self, cmd: &mut Vec<u8>) {
        write_command_to_vec(cmd, self.args_iter(), self.cursor.unwrap_or(0))
    }

    pub(crate) fn write_packed_command_preallocated(&self, cmd: &mut Vec<u8>) {
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
        match con.req_command(self) {
            Ok(val) => from_owned_redis_value(val),
            Err(e) => Err(e),
        }
    }

    /// Async version of `query`.
    #[inline]
    #[cfg(feature = "aio")]
    pub async fn query_async<C, T: FromRedisValue>(&self, con: &mut C) -> RedisResult<T>
    where
        C: crate::aio::ConnectionLike,
    {
        let val = con.req_packed_command(self).await?;
        from_owned_redis_value(val)
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
    pub fn iter<T: FromRedisValue>(self, con: &mut dyn ConnectionLike) -> RedisResult<Iter<'_, T>> {
        let rv = con.req_command(&self)?;

        let (cursor, batch) = if rv.looks_like_cursor() {
            from_owned_redis_value::<(u64, Vec<T>)>(rv)?
        } else {
            (0, from_owned_redis_value(rv)?)
        };

        Ok(Iter {
            batch: batch.into_iter(),
            cursor,
            con,
            cmd: self,
        })
    }

    /// Similar to `iter()` but returns an AsyncIter over the items of the
    /// bulk result or iterator.  A [futures::Stream](https://docs.rs/futures/0.3.3/futures/stream/trait.Stream.html)
    /// is implemented on AsyncIter. In normal mode this is not in any way more
    /// efficient than just querying into a `Vec<T>` as it's internally
    /// implemented as buffering into a vector.  This however is useful when
    /// `cursor_arg` was used in which case the stream will query for more
    /// items until the server side cursor is exhausted.
    ///
    /// This is useful for commands such as `SSCAN`, `SCAN` and others in async contexts.
    ///
    /// One speciality of this function is that it will check if the response
    /// looks like a cursor or not and always just looks at the payload.
    /// This way you can use the function the same for responses in the
    /// format of `KEYS` (just a list) as well as `SSCAN` (which returns a
    /// tuple of cursor and list).
    #[cfg(feature = "aio")]
    #[inline]
    pub async fn iter_async<'a, T: FromRedisValue + 'a>(
        mut self,
        con: &'a mut (dyn AsyncConnection + Send),
    ) -> RedisResult<AsyncIter<'a, T>> {
        let rv = con.req_packed_command(&self).await?;

        let (cursor, batch) = if rv.looks_like_cursor() {
            from_owned_redis_value::<(u64, Vec<T>)>(rv)?
        } else {
            (0, from_owned_redis_value(rv)?)
        };
        if cursor == 0 {
            self.cursor = None;
        } else {
            self.cursor = Some(cursor);
        }

        Ok(AsyncIter {
            inner: IterOrFuture::Iter(AsyncIterInner {
                batch: batch.into_iter(),
                con,
                cmd: self,
            }),
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

    /// Returns an iterator over the arguments in this command (including the command name itself)
    pub fn args_iter(&self) -> impl Clone + ExactSizeIterator<Item = Arg<&[u8]>> {
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

    // Get a reference to the argument at `idx`
    #[cfg(feature = "cluster")]
    pub(crate) fn arg_idx(&self, idx: usize) -> Option<&[u8]> {
        if idx >= self.args.len() {
            return None;
        }

        let start = if idx == 0 {
            0
        } else {
            match self.args[idx - 1] {
                Arg::Simple(n) => n,
                _ => 0,
            }
        };
        let end = match self.args[idx] {
            Arg::Simple(n) => n,
            _ => 0,
        };
        if start == 0 && end == 0 {
            return None;
        }
        Some(&self.data[start..end])
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

#[cfg(test)]
#[cfg(feature = "cluster")]
mod tests {
    use super::Cmd;

    #[test]
    fn test_cmd_arg_idx() {
        let mut c = Cmd::new();
        assert_eq!(c.arg_idx(0), None);

        c.arg("SET");
        assert_eq!(c.arg_idx(0), Some(&b"SET"[..]));
        assert_eq!(c.arg_idx(1), None);

        c.arg("foo").arg("42");
        assert_eq!(c.arg_idx(1), Some(&b"foo"[..]));
        assert_eq!(c.arg_idx(2), Some(&b"42"[..]));
        assert_eq!(c.arg_idx(3), None);
        assert_eq!(c.arg_idx(4), None);
    }
}
