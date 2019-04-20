use sha1::Sha1;

use aio::ConnectionLike as AsyncConnectionLike;
use cmd::cmd;
use connection::ConnectionLike;
use futures::{
    future::{err, Either},
    Future,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use types::{ErrorKind, FromRedisValue, RedisFuture, RedisResult, ToRedisArgs};

/// Represents a lua script.
#[derive(Clone)]
pub struct Script {
    inner: Arc<ScriptInner>,
    // This stores whether the script has already been loaded into Redis
    // by this instance. It is possible that the script could have been loaded
    // by another instance before even if this value is false.
    already_loaded: Arc<AtomicBool>,
}

struct ScriptInner {
    code: String,
    hash: String,
}

/// The script object represents a lua script that can be executed on the
/// redis server.  The object itself takes care of automatic uploading and
/// execution.  The script object itself can be shared and is immutable.
///
/// Example:
///
/// ```rust,no_run
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let mut con = client.get_connection().unwrap();
/// let script = redis::Script::new(r"
///     return tonumber(ARGV[1]) + tonumber(ARGV[2]);
/// ");
/// let result = script.arg(1).arg(2).invoke(&mut con);
/// assert_eq!(result, Ok(3));
/// ```
impl Script {
    /// Creates a new script object.
    pub fn new(code: &str) -> Script {
        let mut hash = Sha1::new();
        hash.update(code.as_bytes());
        Script {
            inner: Arc::new(ScriptInner {
                code: code.to_string(),
                hash: hash.digest().to_string(),
            }),
            already_loaded: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the script's SHA1 hash in hexadecimal format.
    pub fn get_hash(&self) -> &str {
        &self.inner.hash
    }

    /// Creates a script invocation object with a key filled in.
    #[inline]
    pub fn key<T: ToRedisArgs>(&self, key: T) -> ScriptInvocation {
        ScriptInvocation {
            script: self.clone(),
            args: vec![],
            keys: key.to_redis_args(),
        }
    }

    /// Creates a script invocation object with an argument filled in.
    #[inline]
    pub fn arg<T: ToRedisArgs>(&self, arg: T) -> ScriptInvocation {
        ScriptInvocation {
            script: self.clone(),
            args: arg.to_redis_args(),
            keys: vec![],
        }
    }

    /// Returns an empty script invocation object.  This is primarily useful
    /// for programmatically adding arguments and keys because the type will
    /// not change.  Normally you can use `arg` and `key` directly.
    #[inline]
    pub fn prepare_invoke(&self) -> ScriptInvocation {
        ScriptInvocation {
            script: self.clone(),
            args: vec![],
            keys: vec![],
        }
    }

    /// Invokes the script directly without arguments.
    #[inline]
    pub fn invoke<T: FromRedisValue>(&self, con: &mut ConnectionLike) -> RedisResult<T> {
        ScriptInvocation {
            script: self.clone(),
            args: vec![],
            keys: vec![],
        }
        .invoke(con)
    }

    #[inline]
    pub fn invoke_async<T, C>(&self, con: C) -> RedisFuture<(C, T)>
    where
        T: FromRedisValue + Send + 'static,
        C: AsyncConnectionLike + Send + Clone + 'static,
    {
        ScriptInvocation {
            script: self.clone(),
            args: vec![],
            keys: vec![],
        }
        .invoke_async(con)
    }
}

/// Represents a prepared script call.
#[derive(Clone)]
pub struct ScriptInvocation {
    script: Script,
    args: Vec<Vec<u8>>,
    keys: Vec<Vec<u8>>,
}

/// This type collects keys and other arguments for the script so that it
/// can be then invoked.  While the `Script` type itself holds the script,
/// the `ScriptInvocation` holds the arguments that should be invoked until
/// it's sent to the server.
impl ScriptInvocation {
    /// Adds a regular argument to the invocation.  This ends up as `ARGV[i]`
    /// in the script.
    #[inline]
    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut ScriptInvocation {
        arg.write_redis_args(&mut self.args);
        self
    }

    /// Adds a key argument to the invocation.  This ends up as `KEYS[i]`
    /// in the script.
    #[inline]
    pub fn key<T: ToRedisArgs>(&mut self, key: T) -> &mut ScriptInvocation {
        key.write_redis_args(&mut self.keys);
        self
    }

    /// Invokes the script and returns the result.
    #[inline]
    pub fn invoke<T: FromRedisValue>(&self, con: &mut ConnectionLike) -> RedisResult<T> {
        loop {
            match cmd("EVALSHA")
                .arg(self.script.inner.hash.as_bytes())
                .arg(self.keys.len())
                .arg(&*self.keys)
                .arg(&*self.args)
                .query(con)
            {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    if err.kind() == ErrorKind::NoScriptError {
                        let _: () = cmd("SCRIPT")
                            .arg("LOAD")
                            .arg(self.script.inner.code.as_bytes())
                            .query(con)?;
                    } else {
                        fail!(err);
                    }
                }
            }
        }
    }

    /// Invokes the script and returns the result.
    #[inline]
    pub fn invoke_async<T, C>(&self, con: C) -> RedisFuture<(C, T)>
    where
        T: FromRedisValue + Send + 'static,
        C: AsyncConnectionLike + Send + Clone + 'static,
    {
        let code = self.script.inner.code.to_string();

        // Check if the script has already been loaded into Redis by this instance
        // Note that storing this boolean makes it so that we do not need to clone
        // the ScriptInvocation in the normal case when we know that the script has
        // already been loaded.
        let already_loaded = self
            .script
            .already_loaded
            .fetch_and(true, Ordering::Relaxed);
        if already_loaded {
            Box::new(
                cmd("EVALSHA")
                    .arg(self.script.inner.hash.as_bytes())
                    .arg(self.keys.len())
                    .arg(&*self.keys)
                    .arg(&*self.args)
                    .query_async(con.clone()),
            )
        } else {
            let self_clone = self.clone();
            let already_loaded_ref = self.script.already_loaded.clone();
            Box::new(
                cmd("EVALSHA")
                    .arg(self.script.inner.hash.as_bytes())
                    .arg(self.keys.len())
                    .arg(&*self.keys)
                    .arg(&*self.args)
                    .query_async(con.clone())
                    .or_else(move |error| {
                        if error.kind() == ErrorKind::NoScriptError {
                            Either::A(
                                cmd("SCRIPT")
                                    .arg("LOAD")
                                    .arg(code.as_bytes())
                                    .query_async(con)
                                    .and_then(move |(con, _result): (C, String)| {
                                        already_loaded_ref.store(true, Ordering::Relaxed);
                                        self_clone.invoke_async(con)
                                    }),
                            )
                        } else {
                            Either::B(err(error))
                        }
                    })
                    .then(|result| result),
            )
        }
    }
}
