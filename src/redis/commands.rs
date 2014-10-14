use types::{FromRedisValue, ToRedisArgs, RedisResult, NumberIsFloat};
use client::Client;
use connection::{Connection, ConnectionLike};
use cmd::{cmd, Cmd, Pipeline};


/// Mode for the bit operation on commands.
#[deriving(PartialEq, Eq, Clone, Show)]
pub enum BitOp {
    BitAnd,
    BitOr,
    BitXor,
    BitNot,
}

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
        /// let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        /// let con = client.get_connection().unwrap();
        /// redis::cmd("SET").arg("my_key").arg(42i).execute(&con);
        /// assert_eq!(redis::cmd("GET").arg("my_key").query(&con), Ok(42i));
        /// ```
        ///
        /// Will become this:
        ///
        /// ```rust,no_run
        /// use redis::Commands;
        /// let client = redis::Client::open("redis://127.0.0.1/");
        /// assert_eq!(client.get("my_key"), Ok(42i));
        /// ```
        pub trait Commands {
            #[doc(hidden)]
            fn perform<T: FromRedisValue>(&self, con: &Cmd) -> RedisResult<T>;

            $(
                $(#[$attr])*
                fn $name<$($tyargs: $ty,)* RV: FromRedisValue>(
                    &self $(, $argname: $argty)*) -> RedisResult<RV>
                    { self.perform($body) }
            )*
        }

        /// Implements common redis commands for pipelines.  Unlike the regular
        /// commands trait, this returns the pipeline rather than a result
        /// directly.  Other than that it works the same however.
        pub trait PipelineCommands {
            #[doc(hidden)]
            fn perform<'a>(&'a mut self, con: &Cmd) -> &'a mut Self;

            $(
                $(#[$attr])*
                fn $name<'a $(, $tyargs: $ty)*>(
                    &'a mut self $(, $argname: $argty)*) -> &'a mut Self
                    { self.perform($body) }
            )*
        }
    )
}

implement_commands!(
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
          issues a `INCR` or `INCRBYFLOAT` depending on the type."]
    fn incr<K: ToRedisArgs, V: ToRedisArgs>(key: K, delta: V) {
        cmd(if delta.describe_numeric_behavior() == NumberIsFloat {
            "INCRBYFLOAT"
        } else {
            "INCR"
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

    #[doc="Perform a bitwise operation between multiple keys (containing string values)
        and store the result in the destination key."]
    fn bitop<K: ToRedisArgs>(op: BitOp, dstkey: K, srckeys: K) {
        cmd("BITOP").arg(match op {
            BitAnd => "AND",
            BitOr => "OR",
            BitXor => "XOR",
            BitNot => "NOT"
        }).arg(dstkey).arg(srckeys)
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
        cmd(if delta.describe_numeric_behavior() == NumberIsFloat {
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
)

macro_rules! forward_perform(
    () => (
        fn perform<T: FromRedisValue>(&self, cmd: &Cmd) -> RedisResult<T> {
            cmd.query(self)
        }
    )
)

impl Commands for Connection {
    forward_perform!()
}

impl Commands for Client {
    forward_perform!()
}

impl<T: Commands+ConnectionLike> Commands for RedisResult<T> {
    forward_perform!()
}

impl PipelineCommands for Pipeline {
    fn perform<'a>(&'a mut self, cmd: &Cmd) -> &'a mut Pipeline {
        self.add_command(cmd)
    }
}
