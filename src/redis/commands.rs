use types::{FromRedisValue, ToRedisArgs, RedisResult};
use client::Client;
use connection::Connection;
use cmd::{cmd, Cmd, Pipeline};

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

    #[doc="Get the value of a key."]
    fn get<K: ToRedisArgs>(self, key: K) {
        cmd("GET").arg(key)
    }

    #[doc="Set the string value of a key."]
    fn set<K: ToRedisArgs, V: ToRedisArgs>(self, key: K, value: V) {
        cmd("SET").arg(key).arg(value)
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
    fn expire<K: ToRedisArgs, S: ToRedisArgs>(self, key: K, seconds: S) {
        cmd("EXPIRE").arg(key).arg(seconds)
    }

    #[doc="Set the expiration for a key as a UNIX timestamp."]
    fn expire_at<K: ToRedisArgs, TS: ToRedisArgs>(self, key: K, ts: TS) {
        cmd("EXPIREAT").arg(key).arg(ts)
    }

    #[doc="Set a key's time to live in milliseconds."]
    fn pexpire<K: ToRedisArgs, MS: ToRedisArgs>(self, key: K, ms: MS) {
        cmd("PEXPIRE").arg(key).arg(ms)
    }

    #[doc="Set the expiration for a key as a UNIX timestamp in milliseconds."]
    fn pexpire_at<K: ToRedisArgs, TS: ToRedisArgs>(self, key: K, ts: TS) {
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
