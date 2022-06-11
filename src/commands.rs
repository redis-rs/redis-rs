// can't use rustfmt here because it screws up the file.
#![cfg_attr(rustfmt, rustfmt_skip)]
use crate::cmd::{cmd, Cmd, Iter};
use crate::connection::{Connection, ConnectionLike, Msg};
use crate::pipeline::Pipeline;
use crate::types::{FromRedisValue, NumericBehavior, RedisResult, ToRedisArgs, RedisWrite, Expiry};

#[cfg(feature = "cluster")]
use crate::cluster_pipeline::ClusterPipeline;

#[cfg(feature = "geospatial")]
use crate::geo;

#[cfg(feature = "streams")]
use crate::streams;

#[cfg(feature = "acl")]
use crate::acl;

macro_rules! implement_commands {
    (
        $lifetime: lifetime
        $(
            $(#[$attr:meta])+
            fn $name:ident<$($tyargs:ident : $ty:ident),*>(
                $($argname:ident: $argty:ty),*) $body:block
        )*
    ) =>
    (
        /// Implements common redis commands for connection like objects.  This
        /// allows you to send commands straight to a connection or client.  It
        /// is also implemented for redis results of clients which makes for
        /// very convenient access in some basic cases.
        ///
        /// This allows you to use nicer syntax for some common operations.
        /// For instance this code:
        ///
        /// ```rust,no_run
        /// # fn do_something() -> redis::RedisResult<()> {
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_connection()?;
        /// redis::cmd("SET").arg("my_key").arg(42).execute(&mut con);
        /// assert_eq!(redis::cmd("GET").arg("my_key").query(&mut con), Ok(42));
        /// # Ok(()) }
        /// ```
        ///
        /// Will become this:
        ///
        /// ```rust,no_run
        /// # fn do_something() -> redis::RedisResult<()> {
        /// use redis::Commands;
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_connection()?;
        /// con.set("my_key", 42)?;
        /// assert_eq!(con.get("my_key"), Ok(42));
        /// # Ok(()) }
        /// ```
        pub trait Commands : ConnectionLike+Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty, )* RV: FromRedisValue>(
                    &mut self $(, $argname: $argty)*) -> RedisResult<RV>
                    { Cmd::$name($($argname),*).query(self) }
            )*

            /// Incrementally iterate the keys space.
            #[inline]
            fn scan<RV: FromRedisValue>(&mut self) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("SCAN");
                c.cursor_arg(0);
                c.iter(self)
            }

            /// Incrementally iterate the keys space for keys matching a pattern.
            #[inline]
            fn scan_match<P: ToRedisArgs, RV: FromRedisValue>(&mut self, pattern: P) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("SCAN");
                c.cursor_arg(0).arg("MATCH").arg(pattern);
                c.iter(self)
            }

            /// Incrementally iterate hash fields and associated values.
            #[inline]
            fn hscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("HSCAN");
                c.arg(key).cursor_arg(0);
                c.iter(self)
            }

            /// Incrementally iterate hash fields and associated values for
            /// field names matching a pattern.
            #[inline]
            fn hscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("HSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                c.iter(self)
            }

            /// Incrementally iterate set elements.
            #[inline]
            fn sscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("SSCAN");
                c.arg(key).cursor_arg(0);
                c.iter(self)
            }

            /// Incrementally iterate set elements for elements matching a pattern.
            #[inline]
            fn sscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("SSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                c.iter(self)
            }

            /// Incrementally iterate sorted set elements.
            #[inline]
            fn zscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("ZSCAN");
                c.arg(key).cursor_arg(0);
                c.iter(self)
            }

            /// Incrementally iterate sorted set elements for elements matching a pattern.
            #[inline]
            fn zscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("ZSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                c.iter(self)
            }
        }

        impl Cmd {
            $(
                $(#[$attr])*
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                pub fn $name<$lifetime, $($tyargs: $ty),*>($($argname: $argty),*) -> Self {
                    ::std::mem::replace($body, Cmd::new())
                }
            )*
        }

        /// Implements common redis commands over asynchronous connections. This
        /// allows you to send commands straight to a connection or client.
        ///
        /// This allows you to use nicer syntax for some common operations.
        /// For instance this code:
        ///
        /// ```rust,no_run
        /// use redis::AsyncCommands;
        /// # async fn do_something() -> redis::RedisResult<()> {
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_async_connection().await?;
        /// redis::cmd("SET").arg("my_key").arg(42i32).query_async(&mut con).await?;
        /// assert_eq!(redis::cmd("GET").arg("my_key").query_async(&mut con).await, Ok(42i32));
        /// # Ok(()) }
        /// ```
        ///
        /// Will become this:
        ///
        /// ```rust,no_run
        /// use redis::AsyncCommands;
        /// # async fn do_something() -> redis::RedisResult<()> {
        /// use redis::Commands;
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_async_connection().await?;
        /// con.set("my_key", 42i32).await?;
        /// assert_eq!(con.get("my_key").await, Ok(42i32));
        /// # Ok(()) }
        /// ```
        #[cfg(feature = "aio")]
        pub trait AsyncCommands : crate::aio::ConnectionLike + Send + Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty + Send + Sync + $lifetime,)* RV>(
                    & $lifetime mut self
                    $(, $argname: $argty)*
                ) -> crate::types::RedisFuture<'a, RV>
                where
                    RV: FromRedisValue,
                {
                    Box::pin(async move { ($body).query_async(self).await })
                }
            )*

            /// Incrementally iterate the keys space.
            #[inline]
            fn scan<RV: FromRedisValue>(&mut self) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("SCAN");
                c.cursor_arg(0);
                Box::pin(async move { c.iter_async(self).await })
            }

            /// Incrementally iterate set elements for elements matching a pattern.
            #[inline]
            fn scan_match<P: ToRedisArgs, RV: FromRedisValue>(&mut self, pattern: P) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("SCAN");
                c.cursor_arg(0).arg("MATCH").arg(pattern);
                Box::pin(async move { c.iter_async(self).await })
            }

            /// Incrementally iterate hash fields and associated values.
            #[inline]
            fn hscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("HSCAN");
                c.arg(key).cursor_arg(0);
                Box::pin(async move {c.iter_async(self).await })
            }

            /// Incrementally iterate hash fields and associated values for
            /// field names matching a pattern.
            #[inline]
            fn hscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("HSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                Box::pin(async move {c.iter_async(self).await })
            }

            /// Incrementally iterate set elements.
            #[inline]
            fn sscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("SSCAN");
                c.arg(key).cursor_arg(0);
                Box::pin(async move {c.iter_async(self).await })
            }

            /// Incrementally iterate set elements for elements matching a pattern.
            #[inline]
            fn sscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("SSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                Box::pin(async move {c.iter_async(self).await })
            }

            /// Incrementally iterate sorted set elements.
            #[inline]
            fn zscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("ZSCAN");
                c.arg(key).cursor_arg(0);
                Box::pin(async move {c.iter_async(self).await })
            }

            /// Incrementally iterate sorted set elements for elements matching a pattern.
            #[inline]
            fn zscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("ZSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                Box::pin(async move {c.iter_async(self).await })
            }
        }

        /// Implements common redis commands for pipelines.  Unlike the regular
        /// commands trait, this returns the pipeline rather than a result
        /// directly.  Other than that it works the same however.
        impl Pipeline {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                pub fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*
                ) -> &mut Self {
                    self.add_command(::std::mem::replace($body, Cmd::new()))
                }
            )*
        }

        // Implements common redis commands for cluster pipelines.  Unlike the regular
        // commands trait, this returns the cluster pipeline rather than a result
        // directly.  Other than that it works the same however.
        #[cfg(feature = "cluster")]
        impl ClusterPipeline {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                pub fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*
                ) -> &mut Self {
                    self.add_command(::std::mem::replace($body, Cmd::new()))
                }
            )*
        }
    )
}



include!("generated/commands.rs");



/// Allows pubsub callbacks to stop receiving messages.
///
/// Arbitrary data may be returned from `Break`.
pub enum ControlFlow<U> {
    /// Continues.
    Continue,
    /// Breaks with a value.
    Break(U),
}

/// The PubSub trait allows subscribing to one or more channels
/// and receiving a callback whenever a message arrives.
///
/// Each method handles subscribing to the list of keys, waiting for
/// messages, and unsubscribing from the same list of channels once
/// a ControlFlow::Break is encountered.
///
/// Once (p)subscribe returns Ok(U), the connection is again safe to use
/// for calling other methods.
///
/// # Examples
///
/// ```rust,no_run
/// # fn do_something() -> redis::RedisResult<()> {
/// use redis::{PubSubCommands, ControlFlow};
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let mut con = client.get_connection()?;
/// let mut count = 0;
/// con.subscribe(&["foo"], |msg| {
///     // do something with message
///     assert_eq!(msg.get_channel(), Ok(String::from("foo")));
///
///     // increment messages seen counter
///     count += 1;
///     match count {
///         // stop after receiving 10 messages
///         10 => ControlFlow::Break(()),
///         _ => ControlFlow::Continue,
///     }
/// });
/// # Ok(()) }
/// ```
// TODO In the future, it would be nice to implement Try such that `?` will work
//      within the closure.
pub trait PubSubCommands: Sized {
    /// Subscribe to a list of channels using SUBSCRIBE and run the provided
    /// closure for each message received.
    ///
    /// For every `Msg` passed to the provided closure, either
    /// `ControlFlow::Break` or `ControlFlow::Continue` must be returned. This
    /// method will not return until `ControlFlow::Break` is observed.
    fn subscribe<C, F, U>(&mut self, _: C, _: F) -> RedisResult<U>
    where
        F: FnMut(Msg) -> ControlFlow<U>,
        C: ToRedisArgs;

    /// Subscribe to a list of channels using PSUBSCRIBE and run the provided
    /// closure for each message received.
    ///
    /// For every `Msg` passed to the provided closure, either
    /// `ControlFlow::Break` or `ControlFlow::Continue` must be returned. This
    /// method will not return until `ControlFlow::Break` is observed.
    fn psubscribe<P, F, U>(&mut self, _: P, _: F) -> RedisResult<U>
    where
        F: FnMut(Msg) -> ControlFlow<U>,
        P: ToRedisArgs;
}

impl<T> Commands for T where T: ConnectionLike {}

#[cfg(feature = "aio")]
impl<T> AsyncCommands for T where T: crate::aio::ConnectionLike + Send + Sized {}

impl PubSubCommands for Connection {
    fn subscribe<C, F, U>(&mut self, channels: C, mut func: F) -> RedisResult<U>
    where
        F: FnMut(Msg) -> ControlFlow<U>,
        C: ToRedisArgs,
    {
        let mut pubsub = self.as_pubsub();
        pubsub.subscribe(channels)?;

        loop {
            let msg = pubsub.get_message()?;
            match func(msg) {
                ControlFlow::Continue => continue,
                ControlFlow::Break(value) => return Ok(value),
            }
        }
    }

    fn psubscribe<P, F, U>(&mut self, patterns: P, mut func: F) -> RedisResult<U>
    where
        F: FnMut(Msg) -> ControlFlow<U>,
        P: ToRedisArgs,
    {
        let mut pubsub = self.as_pubsub();
        pubsub.psubscribe(patterns)?;

        loop {
            let msg = pubsub.get_message()?;
            match func(msg) {
                ControlFlow::Continue => continue,
                ControlFlow::Break(value) => return Ok(value),
            }
        }
    }
}

/// Options for the [LPOS](https://redis.io/commands/lpos) command
///
/// # Example
///
/// ```rust,no_run
/// use redis::{Commands, RedisResult, LposOptions};
/// fn fetch_list_position(
///     con: &mut redis::Connection,
///     key: &str,
///     value: &str,
///     count: usize,
///     rank: isize,
///     maxlen: usize,
/// ) -> RedisResult<Vec<usize>> {
///     let opts = LposOptions::default()
///         .count(count)
///         .rank(rank)
///         .maxlen(maxlen);
///     con.lpos(key, value, opts)
/// }
/// ```
#[derive(Default)]
pub struct LposOptions {
    count: Option<usize>,
    maxlen: Option<usize>,
    rank: Option<isize>,
}

impl LposOptions {
    /// Limit the results to the first N matching items.
    pub fn count(mut self, n: usize) -> Self {
        self.count = Some(n);
        self
    }

    /// Return the value of N from the matching items.
    pub fn rank(mut self, n: isize) -> Self {
        self.rank = Some(n);
        self
    }

    /// Limit the search to N items in the list.
    pub fn maxlen(mut self, n: usize) -> Self {
        self.maxlen = Some(n);
        self
    }
}

impl ToRedisArgs for LposOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(n) = self.count {
            out.write_arg(b"COUNT");
            out.write_arg_fmt(n);
        }

        if let Some(n) = self.rank {
            out.write_arg(b"RANK");
            out.write_arg_fmt(n);
        }

        if let Some(n) = self.maxlen {
            out.write_arg(b"MAXLEN");
            out.write_arg_fmt(n);
        }
    }

    fn is_single_arg(&self) -> bool {
        false
    }
}

/// Enum for the LEFT | RIGHT args used by some commands
pub enum Direction {
    Left,
    Right,
}

impl ToRedisArgs for Direction {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let s: &[u8] = match self {
            Direction::Left => b"LEFT",
            Direction::Right => b"RIGHT",
        };
        out.write_arg(s);
    }
}
