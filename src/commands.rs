use types::{FromRedisValue, ToRedisArgs, RedisResult, NumericBehavior};
use client::Client;
use connection::{Connection, ConnectionLike};
use cmd::{cmd, Cmd, Pipeline, Iter};


macro_rules! implement_commands {
    (
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
        /// let client = try!(redis::Client::open("redis://127.0.0.1/"));
        /// let con = try!(client.get_connection());
        /// redis::cmd("SET").arg("my_key").arg(42i).execute(&con);
        /// assert_eq!(redis::cmd("GET").arg("my_key").query(&con), Ok(42i));
        /// # Ok(()) }
        /// ```
        ///
        /// Will become this:
        ///
        /// ```rust,no_run
        /// # fn do_something() -> redis::RedisResult<()> {
        /// use redis::Commands;
        /// let client = try!(redis::Client::open("redis://127.0.0.1/"));
        /// let con = try!(client.get_connection());
        /// assert_eq!(con.get("my_key"), Ok(42i));
        /// # Ok(()) }
        /// ```
        pub trait Commands : ConnectionLike {
            $(
                $(#[$attr])*
                #[inline]
                fn $name<$($tyargs: $ty,)* RV: FromRedisValue>(
                    &self $(, $argname: $argty)*) -> RedisResult<RV>
                    { ($body).query(self) }
            )*

            /// Incrementally iterate the keys space.
            #[inline]
            fn scan<RV: FromRedisValue>(&self) -> RedisResult<Iter<RV>> {
                cmd("SCAN").cursor_arg(0).iter(self)
            }

            /// Incrementally iterate the keys space for keys matching a pattern.
            #[inline]
            fn scan_match<P: ToRedisArgs, RV: FromRedisValue>(&self, pattern: P) -> RedisResult<Iter<RV>> {
                cmd("SCAN").cursor_arg(0).arg("MATCH").arg(pattern).iter(self)
            }

            /// Incrementally iterate hash fields and associated values.
            #[inline]
            fn hscan<K: ToRedisArgs, RV: FromRedisValue>(&self, key: K) -> RedisResult<Iter<RV>> {
                cmd("HSCAN").arg(key).cursor_arg(0).iter(self)
            }

            /// Incrementally iterate hash fields and associated values for
            /// field names matching a pattern.
            #[inline]
            fn hscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&self, key: K, pattern: P) -> RedisResult<Iter<RV>> {
                cmd("HSCAN").arg(key).cursor_arg(0).arg("MATCH").arg(pattern).iter(self)
            }

            /// Incrementally iterate set elements.
            #[inline]
            fn sscan<K: ToRedisArgs, RV: FromRedisValue>(&self, key: K) -> RedisResult<Iter<RV>> {
                cmd("SSCAN").arg(key).cursor_arg(0).iter(self)
            }

            /// Incrementally iterate set elements for elements matching a pattern.
            #[inline]
            fn sscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&self, key: K, pattern: P) -> RedisResult<Iter<RV>> {
                cmd("SSCAN").arg(key).cursor_arg(0).arg("MATCH").arg(pattern).iter(self)
            }

            /// Incrementally iterate sorted set elements.
            #[inline]
            fn zscan<K: ToRedisArgs, RV: FromRedisValue>(&self, key: K) -> RedisResult<Iter<RV>> {
                cmd("ZSCAN").arg(key).cursor_arg(0).iter(self)
            }

            /// Incrementally iterate sorted set elements for elements matching a pattern.
            #[inline]
            fn zscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&self, key: K, pattern: P) -> RedisResult<Iter<RV>> {
                cmd("ZSCAN").arg(key).cursor_arg(0).arg("MATCH").arg(pattern).iter(self)
            }
        }

        /// Implements common redis commands for pipelines.  Unlike the regular
        /// commands trait, this returns the pipeline rather than a result
        /// directly.  Other than that it works the same however.
        pub trait PipelineCommands {
            #[doc(hidden)]
            #[inline]
            fn perform(&mut self, con: &Cmd) -> &mut Self;

            $(
                $(#[$attr])*
                #[inline]
                fn $name<'a $(, $tyargs: $ty)*>(
                    &mut self $(, $argname: $argty)*) -> &mut Self
                    { self.perform($body) }
            )*
        }
    )
}

implement_commands! {
    // most common operations

    #[doc="Get the value of a key.  If key is a vec this becomes an `MGET`."]
    fn get<K: ToRedisArgs>(key: K) {
        cmd(if key.is_single_arg() { "GET" } else { "MGET" }).arg(key)
    }

    #[doc="Set the string value of a key."]
    fn set<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("SET").arg(key).arg(value)
    }

    #[doc="Sets multiple keys to their values."]
    fn set_multiple<K: ToRedisArgs, V: ToRedisArgs>(items: &[(K, V)]) {
        cmd("MSET").arg(items)
    }

    #[doc="Set the value and expiration of a key."]
    fn set_ex<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V, seconds: uint) {
        cmd("SETEX").arg(key).arg(value).arg(seconds)
    }

    #[doc="Set the value of a key, only if the key does not exist"]
    fn set_nx<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("SETNX").arg(key).arg(value)
    }

    #[doc="Sets multiple keys to their values failing if at least one already exists."]
    fn mset_nx<K: ToRedisArgs, V: ToRedisArgs>(items: &[(K, V)]) {
        cmd("MSETNX").arg(items)
    }

    #[doc="Set the string value of a key and return its old value."]
    fn getset<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("GETSET").arg(key).arg(value)
    }

    #[doc="Delete one or more keys."]
    fn del<K: ToRedisArgs>(key: K) {
        cmd("DEL").arg(key)
    }

    #[doc="Determine if a key exists."]
    fn exists<K: ToRedisArgs>(key: K) {
        cmd("EXISTS").arg(key)
    }

    #[doc="Set a key's time to live in seconds."]
    fn expire<K: ToRedisArgs>(key: K, seconds: uint) {
        cmd("EXPIRE").arg(key).arg(seconds)
    }

    #[doc="Set the expiration for a key as a UNIX timestamp."]
    fn expire_at<K: ToRedisArgs>(key: K, ts: uint) {
        cmd("EXPIREAT").arg(key).arg(ts)
    }

    #[doc="Set a key's time to live in milliseconds."]
    fn pexpire<K: ToRedisArgs>(key: K, ms: uint) {
        cmd("PEXPIRE").arg(key).arg(ms)
    }

    #[doc="Set the expiration for a key as a UNIX timestamp in milliseconds."]
    fn pexpire_at<K: ToRedisArgs>(key: K, ts: uint) {
        cmd("PEXPIREAT").arg(key).arg(ts)
    }

    #[doc="Remove the expiration from a key."]
    fn persist<K: ToRedisArgs>(key: K) {
        cmd("PERSIST").arg(key)
    }

    #[doc="Rename a key."]
    fn rename<K: ToRedisArgs>(key: K, new_key: K) {
        cmd("RENAME").arg(key).arg(new_key)
    }

    #[doc="Rename a key, only if the new key does not exist."]
    fn rename_nx<K: ToRedisArgs>(key: K, new_key: K) {
        cmd("RENAMENX").arg(key).arg(new_key)
    }

    // common string operations

    #[doc="Append a value to a key."]
    fn append<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("APPEND").arg(key).arg(value)
    }

    #[doc="Increment the numeric value of a key by the given amount.  This 
          issues a `INCRBY` or `INCRBYFLOAT` depending on the type."]
    fn incr<K: ToRedisArgs, V: ToRedisArgs>(key: K, delta: V) {
        cmd(if delta.describe_numeric_behavior() == NumericBehavior::NumberIsFloat {
            "INCRBYFLOAT"
        } else {
            "INCRBY"
        }).arg(key).arg(delta)
    }

    #[doc="Sets or clears the bit at offset in the string value stored at key."]
    fn setbit<K: ToRedisArgs>(key: K, offset: uint, value: bool) {
        cmd("SETBIT").arg(key).arg(offset).arg(value)
    }

    #[doc="Returns the bit value at offset in the string value stored at key."]
    fn getbit<K: ToRedisArgs>(key: K, offset: uint) {
        cmd("GETBIT").arg(key).arg(offset)
    }

    #[doc="Count set bits in a string."]
    fn bitcount<K: ToRedisArgs>(key: K) {
        cmd("BITCOUNT").arg(key)
    }

    #[doc="Count set bits in a string in a range."]
    fn bitcount_range<K: ToRedisArgs>(key: K, start: uint, end: uint) {
        cmd("BITCOUNT").arg(key).arg(start).arg(end)
    }

    #[doc="Perform a bitwise AND between multiple keys (containing string values)
        and store the result in the destination key."]
    fn bit_and<K: ToRedisArgs>(dstkey: K, srckeys: K) {
        cmd("BITOP").arg("AND").arg(dstkey).arg(srckeys)
    }

    #[doc="Perform a bitwise OR between multiple keys (containing string values)
        and store the result in the destination key."]
    fn bit_or<K: ToRedisArgs>(dstkey: K, srckeys: K) {
        cmd("BITOP").arg("OR").arg(dstkey).arg(srckeys)
    }

    #[doc="Perform a bitwise XOR between multiple keys (containing string values)
        and store the result in the destination key."]
    fn bit_xor<K: ToRedisArgs>(dstkey: K, srckeys: K) {
        cmd("BITOP").arg("XOR").arg(dstkey).arg(srckeys)
    }

    #[doc="Perform a bitwise NOT of the key (containing string values)
        and store the result in the destination key."]
    fn bit_not<K: ToRedisArgs>(dstkey: K, srckey: K) {
        cmd("BITOP").arg("NOT").arg(dstkey).arg(srckey)
    }

    #[doc="Get the length of the value stored in a key."]
    fn strlen<K: ToRedisArgs>(key: K) {
        cmd("STRLEN").arg(key)
    }

    // hash operations

    #[doc="Gets a single (or multiple) fields from a hash."]
    fn hget<K: ToRedisArgs, F: ToRedisArgs>(key: K, field: F) {
        cmd(if field.is_single_arg() { "HGET" } else { "HMGET" }).arg(key).arg(field)
    }

    #[doc="Deletes a single (or multiple) fields from a hash."]
    fn hdel<K: ToRedisArgs, F: ToRedisArgs>(key: K, field: F) {
        cmd("HDEL").arg(key).arg(field)
    }

    #[doc="Sets a single field in a hash."]
    fn hset<K: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs>(key: K, field: F, value: V) {
        cmd("HSET").arg(key).arg(field).arg(value)
    }

    #[doc="Sets a single field in a hash if it does not exist."]
    fn hset_nx<K: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs>(key: K, field: F, value: V) {
        cmd("HSETNX").arg(key).arg(field).arg(value)
    }

    #[doc="Sets a multiple fields in a hash."]
    fn hset_multiple<K: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs>(key: K, items: &[(F, V)]) {
        cmd("HMSET").arg(key).arg(items)
    }

    #[doc="Increments a value."]
    fn hincr<K: ToRedisArgs, F: ToRedisArgs, D: ToRedisArgs>(key: K, field: F, delta: D) {
        cmd(if delta.describe_numeric_behavior() == NumericBehavior::NumberIsFloat {
            "HINCRBYFLOAT"
        } else {
            "HINCRBY"
        }).arg(key).arg(field).arg(delta)
    }

    #[doc="Checks if a field in a hash exists."]
    fn hexists<K: ToRedisArgs, F: ToRedisArgs>(key: K, field: F) {
        cmd("HEXISTS").arg(key).arg(field)
    }

    #[doc="Gets all the keys in a hash."]
    fn hkeys<K: ToRedisArgs>(key: K) {
        cmd("HKEYS").arg(key)
    }

    #[doc="Gets all the values in a hash."]
    fn hvals<K: ToRedisArgs>(key: K) {
        cmd("HVALS").arg(key)
    }

    #[doc="Gets all the fields and values in a hash."]
    fn hgetall<K: ToRedisArgs>(key: K) {
        cmd("HGETALL").arg(key)
    }

    #[doc="Gets the length of a hash."]
    fn hlen<K: ToRedisArgs>(key: K) {
        cmd("HLEN").arg(key)
    }

    // list operations

    #[doc="Remove and get the first element in a list, or block until one is available."]
    fn blpop<K: ToRedisArgs>(key: K, timeout: uint) {
        cmd("BLPOP").arg(key).arg(timeout)
    }

    #[doc="Remove and get the last element in a list, or block until one is available."]
    fn brpop<K: ToRedisArgs>(key: K, timeout: uint) {
        cmd("BRPOP").arg(key).arg(timeout)
    }

    #[doc="Pop a value from a list, push it to another list and return it;
        or block until one is available."]
    fn brpoplpush<K: ToRedisArgs>(srckey: K, dstkey: K, timeout: uint) {
        cmd("BRPOPLPUSH").arg(srckey).arg(dstkey).arg(timeout)
    }

    #[doc="Get an element from a list by its index."]
    fn lindex<K: ToRedisArgs>(key: K, index: int) {
        cmd("LINDEX").arg(key).arg(index)
    }

    #[doc="Insert an element before another element in a list."]
    fn linsert_before<K: ToRedisArgs, P: ToRedisArgs, V: ToRedisArgs>(
            key: K, pivot: P, value: V) {
        cmd("LINSERT").arg(key).arg("BEFORE").arg(pivot).arg(value)
    }

    #[doc="Insert an element after another element in a list."]
    fn linsert_after<K: ToRedisArgs, P: ToRedisArgs, V: ToRedisArgs>(
            key: K, pivot: P, value: V) {
        cmd("LINSERT").arg(key).arg("AFTER").arg(pivot).arg(value)
    }

    #[doc="Returns the length of the list stored at key."]
    fn llen<K: ToRedisArgs>(key: K) {
        cmd("LLEN").arg(key)
    }

    #[doc="Removes and returns the first element of the list stored at key."]
    fn lpop<K: ToRedisArgs>(key: K) {
        cmd("LPOP").arg(key)
    }

    #[doc="Insert all the specified values at the head of the list stored at key."]
    fn lpush<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("LPUSH").arg(key).arg(value)
    }

    #[doc="Inserts a value at the head of the list stored at key, only if key already exists and
        holds a list."]
    fn lpush_exists<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("LPUSHX").arg(key).arg(value)
    }

    #[doc="Returns the specified elements of the list stored at key."]
    fn lrange<K: ToRedisArgs>(key: K, start: int, stop: int) {
        cmd("LRANGE").arg(key).arg(start).arg(stop)
    }

    #[doc="Removes the first count occurrences of elements equal to value 
        from the list stored at key."]
    fn lrem<K: ToRedisArgs, V: ToRedisArgs>(key: K, count: int, value: V) {
        cmd("LREM").arg(key).arg(count).arg(value)
    }

    #[doc="Trim an existing list so that it will contain only the specified
        range of elements specified."]
    fn ltrim<K: ToRedisArgs>(key: K, start: int, stop: int) {
        cmd("LTRIM").arg(key).arg(start).arg(stop)
    }

    #[doc="Removes and returns the last element of the list stored at key."]
    fn rpop<K: ToRedisArgs>(key: K) {
        cmd("RPOP").arg(key)
    }

    #[doc="Pop a value from a list, push it to another list and return it."]
    fn rpoplpush<K: ToRedisArgs>(key: K, dstkey: K) {
        cmd("RPOPLPUSH").arg(key).arg(dstkey)
    }

    #[doc="Insert all the specified values at the tail of the list stored at key."]
    fn rpush<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("RPUSH").arg(key).arg(value)
    }

    #[doc="Inserts value at the tail of the list stored at key, only if key already exists and
        holds a list."]
    fn rpush_exists<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) {
        cmd("RPUSHX").arg(key).arg(value)
    }

    // set commands

    #[doc="Add one or more members to a set."]
    fn sadd<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("SADD").arg(key).arg(member)
    }

    #[doc="Get the number of members in a set."]
    fn scard<K: ToRedisArgs>(key: K) {
        cmd("SCARD").arg(key)
    }

    #[doc="Subtract multiple sets."]
    fn sdiff<K: ToRedisArgs>(keys: K) {
        cmd("SDIFF").arg(keys)
    }

    #[doc="Subtract multiple sets and store the resulting set in a key."]
    fn sdiffstore<K: ToRedisArgs>(dstkey: K, keys: K) {
        cmd("SDIFFSTORE").arg(dstkey).arg(keys)
    }

    #[doc="Intersect multiple sets."]
    fn sinter<K: ToRedisArgs>(keys: K) {
        cmd("SINTER").arg(keys)
    }

    #[doc="Intersect multiple sets and store the resulting set in a key."]
    fn sdinterstore<K: ToRedisArgs>(dstkey: K, keys: K) {
        cmd("SINTERSTORE").arg(dstkey).arg(keys)
    }

    #[doc="Determine if a given value is a member of a set."]
    fn sismember<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("SISMEMBER").arg(key).arg(member)
    }

    #[doc="Get all the members in a set."]
    fn smembers<K: ToRedisArgs>(key: K) {
        cmd("SMEMBERS").arg(key)
    }

    #[doc="Move a member from one set to another."]
    fn smove<K: ToRedisArgs, M: ToRedisArgs>(srckey: K, dstkey: K, member: M) {
        cmd("SMOVE").arg(srckey).arg(dstkey).arg(member)
    }

    #[doc="Remove and return a random member from a set."]
    fn spop<K: ToRedisArgs>(key: K) {
        cmd("SPOP").arg(key)
    }

    #[doc="Get one random member from a set."]
    fn srandmember<K: ToRedisArgs>(key: K) {
        cmd("SRANDMEMBER").arg(key)
    }

    #[doc="Get multiple random members from a set."]
    fn srandmember_multiple<K: ToRedisArgs>(key: K, count: uint) {
        cmd("SRANDMEMBER").arg(key).arg(count)
    }

    #[doc="Remove one or more members from a set."]
    fn srem<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("SREM").arg(key).arg(member)
    }

    #[doc="Add multiple sets."]
    fn sunion<K: ToRedisArgs>(keys: K) {
        cmd("SUNION").arg(keys)
    }

    #[doc="Add multiple sets and store the resulting set in a key."]
    fn sunionstore<K: ToRedisArgs>(dstkey: K, keys: K) {
        cmd("SUNIONSTORE").arg(dstkey).arg(keys)
    }

    // sorted set commands

    #[doc="Add one member to a sorted set, or update its score 
        if it already exists."]
    fn zadd<K: ToRedisArgs, S: ToRedisArgs, M: ToRedisArgs>(key: K, member: M, score: S) {
        cmd("ZADD").arg(key).arg(score).arg(member)
    }

    #[doc="Add multiple members to a sorted set, or update its score 
        if it already exists."]
    fn zadd_multiple<K: ToRedisArgs, S: ToRedisArgs, M: ToRedisArgs>(key: K, items: &[(S, M)]) {
        cmd("ZADD").arg(key).arg(items)
    }

    #[doc="Get the number of members in a sorted set."]
    fn zcard<K: ToRedisArgs>(key: K) {
        cmd("ZCARD").arg(key)
    }

    #[doc="Count the members in a sorted set with scores within the given values."]
    fn zcount<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZCOUNT").arg(key).arg(min).arg(max)
    }

    #[doc="Count the members in a sorted set with scores within the given values."]
    fn zincr<K: ToRedisArgs, M: ToRedisArgs, D: ToRedisArgs>(key: K, member: M, delta: D) {
        cmd("ZINCRBY").arg(key).arg(delta).arg(member)
    }

    #[doc="Intersect multiple sorted sets and store the resulting sorted set in
        a new key using SUM as aggregation function."]
    fn zinterstore<K: ToRedisArgs>(dstkey: K, keys: &[K]) {
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.len()).arg(keys)
    }

    #[doc="Intersect multiple sorted sets and store the resulting sorted set in
        a new key using MIN as aggregation function."]
    fn zinterstore_min<K: ToRedisArgs>(dstkey: K, keys: &[K]) {
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.len()).arg(keys).arg("AGGREGATE").arg("MIN")
    }

    #[doc="Intersect multiple sorted sets and store the resulting sorted set in
        a new key using MAX as aggregation function."]
    fn zinterstore_max<K: ToRedisArgs>(dstkey: K, keys: &[K]) {
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.len()).arg(keys).arg("AGGREGATE").arg("MAX")
    }

    #[doc="Count the number of members in a sorted set between a given lexicographical range."]
    fn zlexcount<K: ToRedisArgs, L: ToRedisArgs>(key: K, min: L, max: L) {
        cmd("ZLEXCOUNT").arg(key).arg(min).arg(max)
    }

    #[doc="Return a range of members in a sorted set, by index"]
    fn zrange<K: ToRedisArgs>(key: K, start: int, stop: int) {
        cmd("ZRANGE").arg(key).arg(start).arg(stop)
    }

    #[doc="Return a range of members in a sorted set, by index with scores."]
    fn zrange_withscores<K: ToRedisArgs>(key: K, start: int, stop: int) {
        cmd("ZRANGE").arg(key).arg(start).arg(stop).arg("WITHSCORES")
    }

    #[doc="Return a range of members in a sorted set, by lexicographical range."]
    fn zrangebylex<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZRANGEBYLEX").arg(key).arg(min).arg(max)
    }

    #[doc="Return a range of members in a sorted set, by lexicographical
        range with offset and limit."]
    fn zrangebylex_limit<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(
            key: K, min: M, max: MM, offset: int, count: int) {
        cmd("ZRANGEBYLEX").arg(key).arg(min).arg(max).arg("LIMIT").arg(offset).arg(count)
    }

    #[doc="Return a range of members in a sorted set, by lexicographical range."]
    fn zrevrangebylex<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(key: K, max: MM, min: M) {
        cmd("ZREVRANGEBYLEX").arg(key).arg(max).arg(min)
    }

    #[doc="Return a range of members in a sorted set, by lexicographical
        range with offset and limit."]
    fn zrevrangebylex_limit<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(
            key: K, max: MM, min: M, offset: int, count: int) {
        cmd("ZREVRANGEBYLEX").arg(key).arg(max).arg(min).arg("LIMIT").arg(offset).arg(count)
    }

    #[doc="Return a range of members in a sorted set, by score."]
    fn zrangebyscore<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max)
    }

    #[doc="Return a range of members in a sorted set, by score with scores."]
    fn zrangebyscore_withscores<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max).arg("WITHSCORES")
    }

    #[doc="Return a range of members in a sorted set, by score with limit."]
    fn zrangebyscore_limit<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>
            (key: K, min: M, max: MM, offset: int, count: int) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max).arg("LIMIT").arg(offset).arg(count)
    }

    #[doc="Return a range of members in a sorted set, by score with limit with scores."]
    fn zrangebyscore_limit_withscores<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>
            (key: K, min: M, max: MM, offset: int, count: int) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max).arg("WITHSCORES")
            .arg("LIMIT").arg(offset).arg(count)
    }

    #[doc="Determine the index of a member in a sorted set."]
    fn zrank<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("ZRANK").arg(key).arg(member)
    }

    #[doc="Remove one or more members from a sorted set."]
    fn zrem<K: ToRedisArgs, M: ToRedisArgs>(key: K, members: M) {
        cmd("ZREM").arg(key).arg(members)
    }

    #[doc="Remove all members in a sorted set between the given lexicographical range."]
    fn zrembylex<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZREMBYLEX").arg(key).arg(min).arg(max)
    }

    #[doc="Remove all members in a sorted set within the given indexes."]
    fn zrembyrank<K: ToRedisArgs>(key: K, start: int, stop: int) {
        cmd("ZREMBYRANK").arg(key).arg(start).arg(stop)
    }

    #[doc="Remove all members in a sorted set within the given scores."]
    fn zrembyscore<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) {
        cmd("ZREMBYSCORE").arg(key).arg(min).arg(max)
    }

    #[doc="Return a range of members in a sorted set, by index, with scores
        ordered from high to low."]
    fn zrevrange<K: ToRedisArgs>(key: K, start: int, stop: int) {
        cmd("ZREVRANGE").arg(key).arg(start).arg(stop)
    }

    #[doc="Return a range of members in a sorted set, by index, with scores
        ordered from high to low."]
    fn zrevrange_withscores<K: ToRedisArgs>(key: K, start: int, stop: int) {
        cmd("ZREVRANGE").arg(key).arg(start).arg(stop).arg("WITHSCORES")
    }

    #[doc="Return a range of members in a sorted set, by score."]
    fn zrevrangebyscore<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(key: K, max: MM, min: M) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min)
    }

    #[doc="Return a range of members in a sorted set, by score with scores."]
    fn zrevrangebyscore_withscores<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(key: K, max: MM, min: M) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min).arg("WITHSCORES")
    }

    #[doc="Return a range of members in a sorted set, by score with limit."]
    fn zrevrangebyscore_limit<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>
            (key: K, max: MM, min: M, offset: int, count: int) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min).arg("LIMIT").arg(offset).arg(count)
    }

    #[doc="Return a range of members in a sorted set, by score with limit with scores."]
    fn zrevrangebyscore_limit_withscores<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>
            (key: K, max: MM, min: M, offset: int, count: int) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min).arg("WITHSCORES")
            .arg("LIMIT").arg(offset).arg(count)
    }

    #[doc="Determine the index of a member in a sorted set, with scores ordered from high to low."]
    fn zrevrank<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("ZREVRANK").arg(key).arg(member)
    }

    #[doc="Get the score associated with the given member in a sorted set."]
    fn zscore<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) {
        cmd("ZSCORE").arg(key).arg(member)
    }

    #[doc="Unions multiple sorted sets and store the resulting sorted set in
        a new key using SUM as aggregation function."]
    fn zunionstore<K: ToRedisArgs>(dstkey: K, keys: &[K]) {
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.len()).arg(keys)
    }

    #[doc="Unions multiple sorted sets and store the resulting sorted set in
        a new key using MIN as aggregation function."]
    fn zunionstore_min<K: ToRedisArgs>(dstkey: K, keys: &[K]) {
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.len()).arg(keys).arg("AGGREGATE").arg("MIN")
    }

    #[doc="Unions multiple sorted sets and store the resulting sorted set in
        a new key using MAX as aggregation function."]
    fn zunionstore_max<K: ToRedisArgs>(dstkey: K, keys: &[K]) {
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.len()).arg(keys).arg("AGGREGATE").arg("MAX")
    }

    // hyperloglog commands

    #[doc="Adds the specified elements to the specified HyperLogLog."]
    fn pfadd<K: ToRedisArgs, E: ToRedisArgs>(key: K, element: E) {
        cmd("PFADD").arg(key).arg(element)
    }

    #[doc="Return the approximated cardinality of the set(s) observed by the
        HyperLogLog at key(s)."]
    fn pfcount<K: ToRedisArgs>(key: K) {
        cmd("PFCOUNT").arg(key)
    }

    #[doc="Merge N different HyperLogLogs into a single one."]
    fn pfmerge<K: ToRedisArgs>(dstkey: K, srckeys: K) {
        cmd("PFMERGE").arg(dstkey).arg(srckeys)
    }
}

impl Commands for Connection {}
impl Commands for Client {}
impl<T: Commands+ConnectionLike> Commands for RedisResult<T> {}

impl PipelineCommands for Pipeline {
    fn perform(&mut self, cmd: &Cmd) -> &mut Pipeline {
        self.add_command(cmd)
    }
}
