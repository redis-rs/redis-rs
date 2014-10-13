use types::{FromRedisValue, ToRedisArgs, RedisResult, NumberIsFloat};
use client::Client;
use connection::Connection;
use cmd::{cmd, Cmd, Pipeline};


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
                $self_:ident$(, $argname:ident: $argty:ty)*) $body:block
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
                    &$self_ $(, $argname: $argty)*) -> RedisResult<RV>
                    { $self_.perform($body) }
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
                    &'a mut $self_ $(, $argname: $argty)*) -> &'a mut Self
                    { $self_.perform($body) }
            )*
        }
    )
}

implement_commands!(
    // most common operations

    #[doc="Get the value of a key.  If key is a vec this becomes an `MGET`."]
    fn get<K: ToRedisArgs>(self, key: K) {
        cmd(if key.is_single_arg() { "GET" } else { "MGET" }).arg(key)
    }

    #[doc="Set the string value of a key."]
    fn set<K: ToRedisArgs, V: ToRedisArgs>(self, key: K, value: V) {
        cmd("SET").arg(key).arg(value)
    }

    #[doc="Set the value and expiration of a key."]
    fn set_ex<K: ToRedisArgs, V: ToRedisArgs>(self, key: K, value: V, seconds: uint) {
        cmd("SETEX").arg(key).arg(value).arg(seconds)
    }

    #[doc="Set the value of a key, only if the key does not exist"]
    fn set_nx<K: ToRedisArgs, V: ToRedisArgs>(self, key: K, value: V) {
        cmd("SETNX").arg(key).arg(value)
    }

    #[doc="Set the string value of a key and return its old value."]
    fn getset<K: ToRedisArgs, V: ToRedisArgs>(self, key: K, value: V) {
        cmd("GETSET").arg(key).arg(value)
    }

    #[doc="Delete one or more keys."]
    fn del<K: ToRedisArgs>(self, key: K) {
        cmd("DEL").arg(key)
    }

    #[doc="Determine if a key exists."]
    fn exists<K: ToRedisArgs>(self, key: K) {
        cmd("EXISTS").arg(key)
    }

    #[doc="Set a key's time to live in seconds."]
    fn expire<K: ToRedisArgs>(self, key: K, seconds: uint) {
        cmd("EXPIRE").arg(key).arg(seconds)
    }

    #[doc="Set the expiration for a key as a UNIX timestamp."]
    fn expire_at<K: ToRedisArgs>(self, key: K, ts: uint) {
        cmd("EXPIREAT").arg(key).arg(ts)
    }

    #[doc="Set a key's time to live in milliseconds."]
    fn pexpire<K: ToRedisArgs>(self, key: K, ms: uint) {
        cmd("PEXPIRE").arg(key).arg(ms)
    }

    #[doc="Set the expiration for a key as a UNIX timestamp in milliseconds."]
    fn pexpire_at<K: ToRedisArgs>(self, key: K, ts: uint) {
        cmd("PEXPIREAT").arg(key).arg(ts)
    }

    #[doc="Remove the expiration from a key."]
    fn persist<K: ToRedisArgs>(self, key: K) {
        cmd("PERSIST").arg(key)
    }

    #[doc="Rename a key."]
    fn rename<K: ToRedisArgs>(self, key: K, new_key: K) {
        cmd("RENAME").arg(key).arg(new_key)
    }

    #[doc="Rename a key, only if the new key does not exist."]
    fn rename_nx<K: ToRedisArgs>(self, key: K, new_key: K) {
        cmd("RENAMENX").arg(key).arg(new_key)
    }

    // common string operations

    #[doc="Append a value to a key."]
    fn append<K: ToRedisArgs, V: ToRedisArgs>(self, key: K, value: V) {
        cmd("APPEND").arg(key).arg(value)
    }

    #[doc="Increment the numeric value of a key by the given amount.  This 
          issues a `INCR` or `INCRBYFLOAT` depending on the type."]
    fn incr<K: ToRedisArgs, V: ToRedisArgs>(self, key: K, delta: V) {
        cmd(if delta.describe_numeric_behavior() == NumberIsFloat {
            "INCRBYFLOAT"
        } else {
            "INCR"
        }).arg(key).arg(delta)
    }

    #[doc="Sets or clears the bit at offset in the string value stored at key."]
    fn setbit<K: ToRedisArgs>(self, key: K, offset: uint, value: bool) {
        cmd("SETBIT").arg(key).arg(offset).arg(value)
    }

    #[doc="Returns the bit value at offset in the string value stored at key."]
    fn getbit<K: ToRedisArgs>(self, key: K, offset: uint) {
        cmd("GETBIT").arg(key).arg(offset)
    }

    #[doc="Count set bits in a string."]
    fn bitcount<K: ToRedisArgs>(self, key: K) {
        cmd("BITCOUNT").arg(key)
    }

    #[doc="Count set bits in a string in a range."]
    fn bitcount_range<K: ToRedisArgs>(self, key: K, start: uint, end: uint) {
        cmd("BITCOUNT").arg(key).arg(start).arg(end)
    }

    #[doc="Perform a bitwise operation between multiple keys (containing string values)
        and store the result in the destination key."]
    fn bitop<K: ToRedisArgs>(self, op: BitOp, dstkey: K, srckeys: K) {
        cmd("BITOP").arg(match op {
            BitAnd => "AND",
            BitOr => "OR",
            BitXor => "XOR",
            BitNot => "NOT"
        }).arg(dstkey).arg(srckeys)
    }

    #[doc="Get the length of the value stored in a key."]
    fn strlen<K: ToRedisArgs>(self, key: K) {
        cmd("STRLEN").arg(key)
    }
)

impl Commands for Connection {
    fn perform<T: FromRedisValue>(&self, cmd: &Cmd) -> RedisResult<T> {
        cmd.query(self)
    }
}

impl Commands for Client {
    fn perform<T: FromRedisValue>(&self, cmd: &Cmd) -> RedisResult<T> {
        cmd.query(&try!(self.get_connection()))
    }
}

impl Commands for RedisResult<Client> {
    fn perform<T: FromRedisValue>(&self, cmd: &Cmd) -> RedisResult<T> {
        match self {
            &Ok(ref client) => cmd.query(&try!(client.get_connection())),
            &Err(ref x) => Err(x.clone())
        }
    }
}

impl PipelineCommands for Pipeline {
    fn perform<'a>(&'a mut self, cmd: &Cmd) -> &'a mut Pipeline {
        self.add_command(cmd)
    }
}
