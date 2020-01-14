// can't use rustfmt here because it screws up the file.
#![cfg_attr(rustfmt, rustfmt_skip)]
use crate::types::{FromRedisValue, ToRedisArgs, RedisResult, RedisFuture, NumericBehavior};
use crate::connection::{ConnectionLike, Msg, Connection};
use crate::cmd::{cmd, Cmd, Pipeline, Iter};

#[cfg(feature = "geospatial")]
use crate::geo;

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
        /// let () = con.set("my_key", 42)?;
        /// assert_eq!(con.get("my_key"), Ok(42));
        /// # Ok(()) }
        /// ```
        pub trait Commands : ConnectionLike+Sized {
            $(
                $(#[$attr])*
                #[inline]
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
        /// let () = redis::cmd("SET").arg("my_key").arg(42i32).query_async(&mut con).await?;
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
        /// let () = con.set("my_key", 42i32).await?;
        /// assert_eq!(con.get("my_key").await, Ok(42i32));
        /// # Ok(()) }
        /// ```
        pub trait AsyncCommands : crate::aio::ConnectionLike + Send + Sized {
            $(
                $(#[$attr])*
                #[inline]
                fn $name<$lifetime, $($tyargs: $ty + Send + Sync + $lifetime,)* RV>(
                    & $lifetime mut self
                    $(, $argname: $argty)*
                    ) -> RedisFuture<'a, RV>
                where RV: FromRedisValue,
                {
                    Box::pin(async move { ($body).query_async(self).await })
                }
            )*
        }

        /// Implements common redis commands for pipelines.  Unlike the regular
        /// commands trait, this returns the pipeline rather than a result
        /// directly.  Other than that it works the same however.
        impl Pipeline {
            $(
                $(#[$attr])*
                #[inline]
                pub fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*) -> &mut Self
                    { self.add_command(::std::mem::replace($body, Cmd::new())) }
            )*
        }
    )
}

implement_commands! {
    'a
    // most common operations

    /// Get the value of a key.  If key is a vec this becomes an `MGET`.
    fn get<K: ToRedisArgs>(key: K) {
        cmd(if key.is_single_arg() { "GET" } else { "MGET" }).arg(key)
    }

    /// Gets all keys matching pattern
    fn keys<K: ToRedisArgs>(key: K) {
        cmd("KEYS").arg(key)
    }

    /// Set the string value of a key.
    fn set<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("SET").arg(key).arg(value)
    }

    /// Sets multiple keys to their values.
    fn set_multiple<K: ToRedisArgs, V: ToRedisArgs>(items: &'a [(K, V)]) {
        cmd("MSET").arg(items)
    }

    /// Set the value and expiration of a key.
    fn set_ex<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V, seconds: usize) {
        cmd("SETEX").arg(key).arg(seconds).arg(value)
    }

    /// Set the value and expiration in milliseconds of a key.
    fn pset_ex<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V, milliseconds: usize) {
        cmd("PSETEX").arg(key).arg(milliseconds).arg(value)
    }

    /// Set the value of a key, only if the key does not exist
    fn set_nx<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("SETNX").arg(key).arg(value)
    }

    /// Sets multiple keys to their values failing if at least one already exists.
    fn mset_nx<K: ToRedisArgs, V: ToRedisArgs>(items: &'a [(K, V)]) {
        cmd("MSETNX").arg(items)
    }

    /// Set the string value of a key and return its old value.
    fn getset<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("GETSET").arg(key).arg(value)
    }

    /// Get a range of bytes/substring from the value of a key. Negative values provide an offset from the end of the value.
    fn getrange<K: ToRedisArgs>(key: K, from: isize, to: isize) {
        cmd("GETRANGE").arg(key).arg(from).arg(to)
    }

    /// Overwrite the part of the value stored in key at the specified offset.
    fn setrange<K: ToRedisArgs, V: ToRedisArgs>(key: K, offset: isize, value: V) {
        cmd("SETRANGE").arg(key).arg(offset).arg(value)
    }

    /// Delete one or more keys.
    fn del<K: ToRedisArgs>(key: K) {
        cmd("DEL").arg(key)
    }

    /// Determine if a key exists.
    fn exists<K: ToRedisArgs>(key: K) {
        cmd("EXISTS").arg(key)
    }

    /// Set a key's time to live in seconds.
    fn expire<K: ToRedisArgs>(key: K, seconds: usize) {
        cmd("EXPIRE").arg(key).arg(seconds)
    }

    /// Set the expiration for a key as a UNIX timestamp.
    fn expire_at<K: ToRedisArgs>(key: K, ts: usize) {
        cmd("EXPIREAT").arg(key).arg(ts)
    }

    /// Set a key's time to live in milliseconds.
    fn pexpire<K: ToRedisArgs>(key: K, ms: usize) {
        cmd("PEXPIRE").arg(key).arg(ms)
    }

    /// Set the expiration for a key as a UNIX timestamp in milliseconds.
    fn pexpire_at<K: ToRedisArgs>(key: K, ts: usize) {
        cmd("PEXPIREAT").arg(key).arg(ts)
    }

    /// Remove the expiration from a key.
    fn persist<K: ToRedisArgs>(key: K) {
        cmd("PERSIST").arg(key)
    }

    /// Get the expiration time of a key.
    fn ttl<K: ToRedisArgs>(key: K) {
        cmd("TTL").arg(key)
    }

    /// Get the expiration time of a key in milliseconds.
    fn pttl<K: ToRedisArgs>(key: K) {
        cmd("PTTL").arg(key)
    }

    /// Rename a key.
    fn rename<K: ToRedisArgs>(key: K, new_key: K) {
        cmd("RENAME").arg(key).arg(new_key)
    }

    /// Rename a key, only if the new key does not exist.
    fn rename_nx<K: ToRedisArgs>(key: K, new_key: K) {
        cmd("RENAMENX").arg(key).arg(new_key)
    }

    // common string operations

    /// Append a value to a key.
    fn append<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("APPEND").arg(key).arg(value)
    }

    /// Increment the numeric value of a key by the given amount.  This
    /// issues a `INCRBY` or `INCRBYFLOAT` depending on the type.
    fn incr<K: ToRedisArgs, V: ToRedisArgs>(key: K, delta: V) {
        cmd(if delta.describe_numeric_behavior() == NumericBehavior::NumberIsFloat {
            "INCRBYFLOAT"
        } else {
            "INCRBY"
        }).arg(key).arg(delta)
    }

    /// Sets or clears the bit at offset in the string value stored at key.
    fn setbit<K: ToRedisArgs>(key: K, offset: usize, value: bool) {
        cmd("SETBIT").arg(key).arg(offset).arg(if value {1} else {0})
    }

    /// Returns the bit value at offset in the string value stored at key.
    fn getbit<K: ToRedisArgs>(key: K, offset: usize) {
        cmd("GETBIT").arg(key).arg(offset)
    }

    /// Count set bits in a string.
    fn bitcount<K: ToRedisArgs>(key: K) {
        cmd("BITCOUNT").arg(key)
    }

    /// Count set bits in a string in a range.
    fn bitcount_range<K: ToRedisArgs>(key: K, start: usize, end: usize) {
        cmd("BITCOUNT").arg(key).arg(start).arg(end)
    }

    /// Perform a bitwise AND between multiple keys (containing string values)
    /// and store the result in the destination key.
    fn bit_and<K: ToRedisArgs>(dstkey: K, srckeys: K) {
        cmd("BITOP").arg("AND").arg(dstkey).arg(srckeys)
    }

    /// Perform a bitwise OR between multiple keys (containing string values)
    /// and store the result in the destination key.
    fn bit_or<K: ToRedisArgs>(dstkey: K, srckeys: K) {
        cmd("BITOP").arg("OR").arg(dstkey).arg(srckeys)
    }

    /// Perform a bitwise XOR between multiple keys (containing string values)
    /// and store the result in the destination key.
    fn bit_xor<K: ToRedisArgs>(dstkey: K, srckeys: K) {
        cmd("BITOP").arg("XOR").arg(dstkey).arg(srckeys)
    }

    /// Perform a bitwise NOT of the key (containing string values)
    /// and store the result in the destination key.
    fn bit_not<K: ToRedisArgs>(dstkey: K, srckey: K) {
        cmd("BITOP").arg("NOT").arg(dstkey).arg(srckey)
    }

    /// Get the length of the value stored in a key.
    fn strlen<K: ToRedisArgs>(key: K) {
        cmd("STRLEN").arg(key)
    }

    // hash operations

    /// Gets a single (or multiple) fields from a hash.
    fn hget<K: ToRedisArgs, F: ToRedisArgs>(key: K, field: F) {
        cmd(if field.is_single_arg() { "HGET" } else { "HMGET" }).arg(key).arg(field)
    }

    /// Deletes a single (or multiple) fields from a hash.
    fn hdel<K: ToRedisArgs, F: ToRedisArgs>(key: K, field: F) {
        cmd("HDEL").arg(key).arg(field)
    }

    /// Sets a single field in a hash.
    fn hset<K: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs>(key: K, field: F, value: V) {
        cmd("HSET").arg(key).arg(field).arg(value)
    }

    /// Sets a single field in a hash if it does not exist.
    fn hset_nx<K: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs>(key: K, field: F, value: V) {
        cmd("HSETNX").arg(key).arg(field).arg(value)
    }

    /// Sets a multiple fields in a hash.
    fn hset_multiple<K: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs>(key: K, items: &'a [(F, V)]) {
        cmd("HMSET").arg(key).arg(items)
    }

    /// Increments a value.
    fn hincr<K: ToRedisArgs, F: ToRedisArgs, D: ToRedisArgs>(key: K, field: F, delta: D) {
        cmd(if delta.describe_numeric_behavior() == NumericBehavior::NumberIsFloat {
            "HINCRBYFLOAT"
        } else {
            "HINCRBY"
        }).arg(key).arg(field).arg(delta)
    }

    /// Checks if a field in a hash exists.
    fn hexists<K: ToRedisArgs, F: ToRedisArgs>(key: K, field: F) {
        cmd("HEXISTS").arg(key).arg(field)
    }

    /// Gets all the keys in a hash.
    fn hkeys<K: ToRedisArgs>(key: K) {
        cmd("HKEYS").arg(key)
    }

    /// Gets all the values in a hash.
    fn hvals<K: ToRedisArgs>(key: K) {
        cmd("HVALS").arg(key)
    }

    /// Gets all the fields and values in a hash.
    fn hgetall<K: ToRedisArgs>(key: K) {
        cmd("HGETALL").arg(key)
    }

    /// Gets the length of a hash.
    fn hlen<K: ToRedisArgs>(key: K) {
        cmd("HLEN").arg(key)
    }

    // list operations

    /// Remove and get the first element in a list, or block until one is available.
    fn blpop<K: ToRedisArgs>(key: K, timeout: usize) {
        cmd("BLPOP").arg(key).arg(timeout)
    }

    /// Remove and get the last element in a list, or block until one is available.
    fn brpop<K: ToRedisArgs>(key: K, timeout: usize) {
        cmd("BRPOP").arg(key).arg(timeout)
    }

    /// Pop a value from a list, push it to another list and return it;
    /// or block until one is available.
    fn brpoplpush<K: ToRedisArgs>(srckey: K, dstkey: K, timeout: usize) {
        cmd("BRPOPLPUSH").arg(srckey).arg(dstkey).arg(timeout)
    }

    /// Get an element from a list by its index.
    fn lindex<K: ToRedisArgs>(key: K, index: isize) {
        cmd("LINDEX").arg(key).arg(index)
    }

    /// Insert an element before another element in a list.
    fn linsert_before<K: ToRedisArgs, P: ToRedisArgs, V: ToRedisArgs>(
            key: K, pivot: P, value: V) {
        cmd("LINSERT").arg(key).arg("BEFORE").arg(pivot).arg(value)
    }

    /// Insert an element after another element in a list.
    fn linsert_after<K: ToRedisArgs, P: ToRedisArgs, V: ToRedisArgs>(
            key: K, pivot: P, value: V) {
        cmd("LINSERT").arg(key).arg("AFTER").arg(pivot).arg(value)
    }

    /// Returns the length of the list stored at key.
    fn llen<K: ToRedisArgs>(key: K) {
        cmd("LLEN").arg(key)
    }

    /// Removes and returns the first element of the list stored at key.
    fn lpop<K: ToRedisArgs>(key: K) {
        cmd("LPOP").arg(key)
    }

    /// Insert all the specified values at the head of the list stored at key.
    fn lpush<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("LPUSH").arg(key).arg(value)
    }

    /// Inserts a value at the head of the list stored at key, only if key
    /// already exists and holds a list.
    fn lpush_exists<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("LPUSHX").arg(key).arg(value)
    }

    /// Returns the specified elements of the list stored at key.
    fn lrange<K: ToRedisArgs>(key: K, start: isize, stop: isize) {
        cmd("LRANGE").arg(key).arg(start).arg(stop)
    }

    /// Removes the first count occurrences of elements equal to value
    /// from the list stored at key.
    fn lrem<K: ToRedisArgs, V: ToRedisArgs>(key: K, count: isize, value: V) {
        cmd("LREM").arg(key).arg(count).arg(value)
    }

    /// Trim an existing list so that it will contain only the specified
    /// range of elements specified.
    fn ltrim<K: ToRedisArgs>(key: K, start: isize, stop: isize) {
        cmd("LTRIM").arg(key).arg(start).arg(stop)
    }

    /// Sets the list element at index to value
    fn lset<K: ToRedisArgs, V: ToRedisArgs>(key: K, index: isize, value: V) {
        cmd("LSET").arg(key).arg(index).arg(value)
    }

    /// Removes and returns the last element of the list stored at key.
    fn rpop<K: ToRedisArgs>(key: K) {
        cmd("RPOP").arg(key)
    }

    /// Pop a value from a list, push it to another list and return it.
    fn rpoplpush<K: ToRedisArgs>(key: K, dstkey: K) {
        cmd("RPOPLPUSH").arg(key).arg(dstkey)
    }

    /// Insert all the specified values at the tail of the list stored at key.
    fn rpush<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("RPUSH").arg(key).arg(value)
    }

    /// Inserts value at the tail of the list stored at key, only if key
    /// already exists and holds a list.
    fn rpush_exists<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("RPUSHX").arg(key).arg(value)
    }

    // set commands

    /// Add one or more members to a set.
    fn sadd<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("SADD").arg(key).arg(member)
    }

    /// Get the number of members in a set.
    fn scard<K: ToRedisArgs>(key: K) {
        cmd("SCARD").arg(key)
    }

    /// Subtract multiple sets.
    fn sdiff<K: ToRedisArgs>(keys: K) {
        cmd("SDIFF").arg(keys)
    }

    /// Subtract multiple sets and store the resulting set in a key.
    fn sdiffstore<K: ToRedisArgs>(dstkey: K, keys: K) {
        cmd("SDIFFSTORE").arg(dstkey).arg(keys)
    }

    /// Intersect multiple sets.
    fn sinter<K: ToRedisArgs>(keys: K) {
        cmd("SINTER").arg(keys)
    }

    /// Intersect multiple sets and store the resulting set in a key.
    fn sinterstore<K: ToRedisArgs>(dstkey: K, keys: K) {
        cmd("SINTERSTORE").arg(dstkey).arg(keys)
    }

    /// Determine if a given value is a member of a set.
    fn sismember<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("SISMEMBER").arg(key).arg(member)
    }

    /// Get all the members in a set.
    fn smembers<K: ToRedisArgs>(key: K) {
        cmd("SMEMBERS").arg(key)
    }

    /// Move a member from one set to another.
    fn smove<K: ToRedisArgs, M: ToRedisArgs>(srckey: K, dstkey: K, member: M) {
        cmd("SMOVE").arg(srckey).arg(dstkey).arg(member)
    }

    /// Remove and return a random member from a set.
    fn spop<K: ToRedisArgs>(key: K) {
        cmd("SPOP").arg(key)
    }

    /// Get one random member from a set.
    fn srandmember<K: ToRedisArgs>(key: K) {
        cmd("SRANDMEMBER").arg(key)
    }

    /// Get multiple random members from a set.
    fn srandmember_multiple<K: ToRedisArgs>(key: K, count: usize) {
        cmd("SRANDMEMBER").arg(key).arg(count)
    }

    /// Remove one or more members from a set.
    fn srem<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("SREM").arg(key).arg(member)
    }

    /// Add multiple sets.
    fn sunion<K: ToRedisArgs>(keys: K) {
        cmd("SUNION").arg(keys)
    }

    /// Add multiple sets and store the resulting set in a key.
    fn sunionstore<K: ToRedisArgs>(dstkey: K, keys: K) {
        cmd("SUNIONSTORE").arg(dstkey).arg(keys)
    }

    // sorted set commands

    /// Add one member to a sorted set, or update its score if it already exists.
    fn zadd<K: ToRedisArgs, S: ToRedisArgs, M: ToRedisArgs>(key: K, member: M, score: S) {
        cmd("ZADD").arg(key).arg(score).arg(member)
    }

    /// Add multiple members to a sorted set, or update its score if it already exists.
    fn zadd_multiple<K: ToRedisArgs, S: ToRedisArgs, M: ToRedisArgs>(key: K, items: &'a [(S, M)]) {
        cmd("ZADD").arg(key).arg(items)
    }

    /// Get the number of members in a sorted set.
    fn zcard<K: ToRedisArgs>(key: K) {
        cmd("ZCARD").arg(key)
    }

    /// Count the members in a sorted set with scores within the given values.
    fn zcount<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZCOUNT").arg(key).arg(min).arg(max)
    }

    /// Increments the member in a sorted set at key by delta.
    /// If the member does not exist, it is added with delta as its score.
    fn zincr<K: ToRedisArgs, M: ToRedisArgs, D: ToRedisArgs>(key: K, member: M, delta: D) {
        cmd("ZINCRBY").arg(key).arg(delta).arg(member)
    }

    /// Intersect multiple sorted sets and store the resulting sorted set in
    /// a new key using SUM as aggregation function.
    fn zinterstore<K: ToRedisArgs>(dstkey: K, keys: &'a [K]) {
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.len()).arg(keys)
    }

    /// Intersect multiple sorted sets and store the resulting sorted set in
    /// a new key using MIN as aggregation function.
    fn zinterstore_min<K: ToRedisArgs>(dstkey: K, keys: &'a [K]) {
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.len()).arg(keys).arg("AGGREGATE").arg("MIN")
    }

    /// Intersect multiple sorted sets and store the resulting sorted set in
    /// a new key using MAX as aggregation function.
    fn zinterstore_max<K: ToRedisArgs>(dstkey: K, keys: &'a [K]) {
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.len()).arg(keys).arg("AGGREGATE").arg("MAX")
    }

    /// Count the number of members in a sorted set between a given lexicographical range.
    fn zlexcount<K: ToRedisArgs, L: ToRedisArgs>(key: K, min: L, max: L) {
        cmd("ZLEXCOUNT").arg(key).arg(min).arg(max)
    }

    /// Return a range of members in a sorted set, by index
    fn zrange<K: ToRedisArgs>(key: K, start: isize, stop: isize) {
        cmd("ZRANGE").arg(key).arg(start).arg(stop)
    }

    /// Return a range of members in a sorted set, by index with scores.
    fn zrange_withscores<K: ToRedisArgs>(key: K, start: isize, stop: isize) {
        cmd("ZRANGE").arg(key).arg(start).arg(stop).arg("WITHSCORES")
    }

    /// Return a range of members in a sorted set, by lexicographical range.
    fn zrangebylex<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZRANGEBYLEX").arg(key).arg(min).arg(max)
    }

    /// Return a range of members in a sorted set, by lexicographical
    /// range with offset and limit.
    fn zrangebylex_limit<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(
            key: K, min: M, max: MM, offset: isize, count: isize) {
        cmd("ZRANGEBYLEX").arg(key).arg(min).arg(max).arg("LIMIT").arg(offset).arg(count)
    }

    /// Return a range of members in a sorted set, by lexicographical range.
    fn zrevrangebylex<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(key: K, max: MM, min: M) {
        cmd("ZREVRANGEBYLEX").arg(key).arg(max).arg(min)
    }

    /// Return a range of members in a sorted set, by lexicographical
    /// range with offset and limit.
    fn zrevrangebylex_limit<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(
            key: K, max: MM, min: M, offset: isize, count: isize) {
        cmd("ZREVRANGEBYLEX").arg(key).arg(max).arg(min).arg("LIMIT").arg(offset).arg(count)
    }

    /// Return a range of members in a sorted set, by score.
    fn zrangebyscore<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max)
    }

    /// Return a range of members in a sorted set, by score with scores.
    fn zrangebyscore_withscores<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max).arg("WITHSCORES")
    }

    /// Return a range of members in a sorted set, by score with limit.
    fn zrangebyscore_limit<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>
            (key: K, min: M, max: MM, offset: isize, count: isize) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max).arg("LIMIT").arg(offset).arg(count)
    }

    /// Return a range of members in a sorted set, by score with limit with scores.
    fn zrangebyscore_limit_withscores<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>
            (key: K, min: M, max: MM, offset: isize, count: isize) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max).arg("WITHSCORES")
            .arg("LIMIT").arg(offset).arg(count)
    }

    /// Determine the index of a member in a sorted set.
    fn zrank<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("ZRANK").arg(key).arg(member)
    }

    /// Remove one or more members from a sorted set.
    fn zrem<K: ToRedisArgs, M: ToRedisArgs>(key: K, members: M) {
        cmd("ZREM").arg(key).arg(members)
    }

    /// Remove all members in a sorted set between the given lexicographical range.
    fn zrembylex<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZREMBYLEX").arg(key).arg(min).arg(max)
    }

    /// Remove all members in a sorted set within the given indexes.
    fn zremrangebyrank<K: ToRedisArgs>(key: K, start: isize, stop: isize) {
        cmd("ZREMRANGEBYRANK").arg(key).arg(start).arg(stop)
    }

    /// Remove all members in a sorted set within the given scores.
    fn zrembyscore<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZREMRANGEBYSCORE").arg(key).arg(min).arg(max)
    }

    /// Return a range of members in a sorted set, by index, with scores
    /// ordered from high to low.
    fn zrevrange<K: ToRedisArgs>(key: K, start: isize, stop: isize) {
        cmd("ZREVRANGE").arg(key).arg(start).arg(stop)
    }

    /// Return a range of members in a sorted set, by index, with scores
    /// ordered from high to low.
    fn zrevrange_withscores<K: ToRedisArgs>(key: K, start: isize, stop: isize) {
        cmd("ZREVRANGE").arg(key).arg(start).arg(stop).arg("WITHSCORES")
    }

    /// Return a range of members in a sorted set, by score.
    fn zrevrangebyscore<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(key: K, max: MM, min: M) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min)
    }

    /// Return a range of members in a sorted set, by score with scores.
    fn zrevrangebyscore_withscores<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(key: K, max: MM, min: M) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min).arg("WITHSCORES")
    }

    /// Return a range of members in a sorted set, by score with limit.
    fn zrevrangebyscore_limit<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>
            (key: K, max: MM, min: M, offset: isize, count: isize) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min).arg("LIMIT").arg(offset).arg(count)
    }

    /// Return a range of members in a sorted set, by score with limit with scores.
    fn zrevrangebyscore_limit_withscores<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>
            (key: K, max: MM, min: M, offset: isize, count: isize) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min).arg("WITHSCORES")
            .arg("LIMIT").arg(offset).arg(count)
    }

    /// Determine the index of a member in a sorted set, with scores ordered from high to low.
    fn zrevrank<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("ZREVRANK").arg(key).arg(member)
    }

    /// Get the score associated with the given member in a sorted set.
    fn zscore<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("ZSCORE").arg(key).arg(member)
    }

    /// Unions multiple sorted sets and store the resulting sorted set in
    /// a new key using SUM as aggregation function.
    fn zunionstore<K: ToRedisArgs>(dstkey: K, keys: &'a [K]) {
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.len()).arg(keys)
    }

    /// Unions multiple sorted sets and store the resulting sorted set in
    /// a new key using MIN as aggregation function.
    fn zunionstore_min<K: ToRedisArgs>(dstkey: K, keys: &'a [K]) {
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.len()).arg(keys).arg("AGGREGATE").arg("MIN")
    }

    /// Unions multiple sorted sets and store the resulting sorted set in
    /// a new key using MAX as aggregation function.
    fn zunionstore_max<K: ToRedisArgs>(dstkey: K, keys: &'a [K]) {
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.len()).arg(keys).arg("AGGREGATE").arg("MAX")
    }

    // hyperloglog commands

    /// Adds the specified elements to the specified HyperLogLog.
    fn pfadd<K: ToRedisArgs, E: ToRedisArgs>(key: K, element: E) {
        cmd("PFADD").arg(key).arg(element)
    }

    /// Return the approximated cardinality of the set(s) observed by the
    /// HyperLogLog at key(s).
    fn pfcount<K: ToRedisArgs>(key: K) {
        cmd("PFCOUNT").arg(key)
    }

    /// Merge N different HyperLogLogs into a single one.
    fn pfmerge<K: ToRedisArgs>(dstkey: K, srckeys: K) {
        cmd("PFMERGE").arg(dstkey).arg(srckeys)
    }

    /// Posts a message to the given channel.
    fn publish<K: ToRedisArgs, E: ToRedisArgs>(channel: K, message: E) {
        cmd("PUBLISH").arg(channel).arg(message)
    }

    // geospatial commands

    /// Adds the specified geospatial items to the specified key.
    ///
    /// Every member has to be written as a tuple of `(longitude, latitude,
    /// member_name)`. It can be a single tuple, or a vector of tuples.
    ///
    /// `longitude, latitude` can be set using [`redis::geo::Coord`][1].
    ///
    /// [1]: ./geo/struct.Coord.html
    ///
    /// Returns the number of elements added to the sorted set, not including
    /// elements already existing for which the score was updated.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use redis::{Commands, Connection, RedisResult};
    /// use redis::geo::Coord;
    ///
    /// fn add_point(con: &mut Connection) -> RedisResult<isize> {
    ///     con.geo_add("my_gis", (Coord::lon_lat(13.361389, 38.115556), "Palermo"))
    /// }
    ///
    /// fn add_point_with_tuples(con: &mut Connection) -> RedisResult<isize> {
    ///     con.geo_add("my_gis", ("13.361389", "38.115556", "Palermo"))
    /// }
    ///
    /// fn add_many_points(con: &mut Connection) -> RedisResult<isize> {
    ///     con.geo_add("my_gis", &[
    ///         ("13.361389", "38.115556", "Palermo"),
    ///         ("15.087269", "37.502669", "Catania")
    ///     ])
    /// }
    /// ```
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_add<K: ToRedisArgs, M: ToRedisArgs>(key: K, members: M) {
        cmd("GEOADD").arg(key).arg(members)
    }

    /// Return the distance between two members in the geospatial index
    /// represented by the sorted set.
    ///
    /// If one or both the members are missing, the command returns NULL, so
    /// it may be convenient to parse its response as either `Option<f64>` or
    /// `Option<String>`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use redis::{Commands, RedisResult};
    /// use redis::geo::Unit;
    ///
    /// fn get_dists(con: &mut redis::Connection) {
    ///     let x: RedisResult<f64> = con.geo_dist(
    ///         "my_gis",
    ///         "Palermo",
    ///         "Catania",
    ///         Unit::Kilometers
    ///     );
    ///     // x is Ok(166.2742)
    ///
    ///     let x: RedisResult<Option<f64>> = con.geo_dist(
    ///         "my_gis",
    ///         "Palermo",
    ///         "Atlantis",
    ///         Unit::Meters
    ///     );
    ///     // x is Ok(None)
    /// }
    /// ```
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_dist<K: ToRedisArgs, M1: ToRedisArgs, M2: ToRedisArgs>(
        key: K,
        member1: M1,
        member2: M2,
        unit: geo::Unit
    ) {
        cmd("GEODIST")
            .arg(key)
            .arg(member1)
            .arg(member2)
            .arg(unit)
    }

    /// Return valid [Geohash][1] strings representing the position of one or
    /// more members of the geospatial index represented by the sorted set at
    /// key.
    ///
    /// [1]: https://en.wikipedia.org/wiki/Geohash
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use redis::{Commands, RedisResult};
    ///
    /// fn get_hash(con: &mut redis::Connection) {
    ///     let x: RedisResult<Vec<String>> = con.geo_hash("my_gis", "Palermo");
    ///     // x is vec!["sqc8b49rny0"]
    ///
    ///     let x: RedisResult<Vec<String>> = con.geo_hash("my_gis", &["Palermo", "Catania"]);
    ///     // x is vec!["sqc8b49rny0", "sqdtr74hyu0"]
    /// }
    /// ```
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_hash<K: ToRedisArgs, M: ToRedisArgs>(key: K, members: M) {
        cmd("GEOHASH").arg(key).arg(members)
    }

    /// Return the positions of all the specified members of the geospatial
    /// index represented by the sorted set at key.
    ///
    /// Every position is a pair of `(longitude, latitude)`. [`redis::geo::Coord`][1]
    /// can be used to convert these value in a struct.
    ///
    /// [1]: ./geo/struct.Coord.html
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use redis::{Commands, RedisResult};
    /// use redis::geo::Coord;
    ///
    /// fn get_position(con: &mut redis::Connection) {
    ///     let x: RedisResult<Vec<Vec<f64>>> = con.geo_pos("my_gis", &["Palermo", "Catania"]);
    ///     // x is [ [ 13.361389, 38.115556 ], [ 15.087269, 37.502669 ] ];
    ///
    ///     let x: Vec<Coord<f64>> = con.geo_pos("my_gis", "Palermo").unwrap();
    ///     // x[0].longitude is 13.361389
    ///     // x[0].latitude is 38.115556
    /// }
    /// ```
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_pos<K: ToRedisArgs, M: ToRedisArgs>(key: K, members: M) {
        cmd("GEOPOS").arg(key).arg(members)
    }

    /// Return the members of a sorted set populated with geospatial information
    /// using [`geo_add`](#method.geo_add), which are within the borders of the area
    /// specified with the center location and the maximum distance from the center
    /// (the radius).
    ///
    /// Every item in the result can be read with [`redis::geo::RadiusSearchResult`][1],
    /// which support the multiple formats returned by `GEORADIUS`.
    ///
    /// [1]: ./geo/struct.RadiusSearchResult.html
    ///
    /// ```rust,no_run
    /// use redis::{Commands, RedisResult};
    /// use redis::geo::{RadiusOptions, RadiusSearchResult, RadiusOrder, Unit};
    ///
    /// fn radius(con: &mut redis::Connection) -> Vec<RadiusSearchResult> {
    ///     let opts = RadiusOptions::default().with_dist().order(RadiusOrder::Asc);
    ///     con.geo_radius("my_gis", 15.90, 37.21, 51.39, Unit::Kilometers, opts).unwrap()
    /// }
    /// ```
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_radius<K: ToRedisArgs>(
        key: K,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: geo::Unit,
        options: geo::RadiusOptions
    ) {
        cmd("GEORADIUS")
            .arg(key)
            .arg(longitude)
            .arg(latitude)
            .arg(radius)
            .arg(unit)
            .arg(options)
    }

    /// Retrieve members selected by distance with the center of `member`. The
    /// member itself is always contained in the results.
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_radius_by_member<K: ToRedisArgs, M: ToRedisArgs>(
        key: K,
        member: M,
        radius: f64,
        unit: geo::Unit,
        options: geo::RadiusOptions
    ) {
        cmd("GEORADIUSBYMEMBER")
            .arg(key)
            .arg(member)
            .arg(radius)
            .arg(unit)
            .arg(options)
    }

}

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
        where F: FnMut(Msg) -> ControlFlow<U>,
              C: ToRedisArgs;

    /// Subscribe to a list of channels using PSUBSCRIBE and run the provided
    /// closure for each message received.
    ///
    /// For every `Msg` passed to the provided closure, either
    /// `ControlFlow::Break` or `ControlFlow::Continue` must be returned. This
    /// method will not return until `ControlFlow::Break` is observed.
    fn psubscribe<P, F, U>(&mut self, _: P, _: F) -> RedisResult<U>
        where F: FnMut(Msg) -> ControlFlow<U>,
              P: ToRedisArgs;
}

impl<T> Commands for T where T: ConnectionLike {}

impl<T> AsyncCommands for T where T: crate::aio::ConnectionLike + Send + ?Sized {}

impl PubSubCommands for Connection {
    fn subscribe<C, F, U>(&mut self, channels: C, mut func: F) -> RedisResult<U>
        where F: FnMut(Msg) -> ControlFlow<U>,
              C: ToRedisArgs
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
        where F: FnMut(Msg) -> ControlFlow<U>,
              P: ToRedisArgs
    {
        let mut pubsub = self.as_pubsub();
        pubsub.psubscribe(patterns)?;

        loop {
            let msg = pubsub.get_message()?;
            match func(msg) {
                ControlFlow::Continue => continue,
                ControlFlow::Break(value) => return Ok(value),
            }
        };
    }
}
