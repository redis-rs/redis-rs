use std::pin::Pin;

use sha1::Sha1;

use futures::{prelude::*, ready, task, Poll};

use crate::aio;
use crate::cmd::{cmd, Cmd};
use crate::connection::ConnectionLike;
use crate::types::{ErrorKind, FromRedisValue, RedisFuture, RedisResult, ToRedisArgs};

/// Represents a lua script.
pub struct Script {
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
            code: code.to_string(),
            hash: hash.digest().to_string(),
        }
    }

    /// Returns the script's SHA1 hash in hexadecimal format.
    pub fn get_hash(&self) -> &str {
        &self.hash
    }

    /// Creates a script invocation object with a key filled in.
    #[inline]
    pub fn key<T: ToRedisArgs>(&self, key: T) -> ScriptInvocation<'_> {
        ScriptInvocation {
            script: self,
            args: vec![],
            keys: key.to_redis_args(),
        }
    }

    /// Creates a script invocation object with an argument filled in.
    #[inline]
    pub fn arg<T: ToRedisArgs>(&self, arg: T) -> ScriptInvocation<'_> {
        ScriptInvocation {
            script: self,
            args: arg.to_redis_args(),
            keys: vec![],
        }
    }

    /// Returns an empty script invocation object.  This is primarily useful
    /// for programmatically adding arguments and keys because the type will
    /// not change.  Normally you can use `arg` and `key` directly.
    #[inline]
    pub fn prepare_invoke(&self) -> ScriptInvocation<'_> {
        ScriptInvocation {
            script: self,
            args: vec![],
            keys: vec![],
        }
    }

    /// Invokes the script directly without arguments.
    #[inline]
    pub fn invoke<T: FromRedisValue>(&self, con: &mut dyn ConnectionLike) -> RedisResult<T> {
        ScriptInvocation {
            script: self,
            args: vec![],
            keys: vec![],
        }
        .invoke(con)
    }
}

/// Represents a prepared script call.
pub struct ScriptInvocation<'a> {
    script: &'a Script,
    args: Vec<Vec<u8>>,
    keys: Vec<Vec<u8>>,
}

/// This type collects keys and other arguments for the script so that it
/// can be then invoked.  While the `Script` type itself holds the script,
/// the `ScriptInvocation` holds the arguments that should be invoked until
/// it's sent to the server.
impl<'a> ScriptInvocation<'a> {
    /// Adds a regular argument to the invocation.  This ends up as `ARGV[i]`
    /// in the script.
    #[inline]
    pub fn arg<'b, T: ToRedisArgs>(&'b mut self, arg: T) -> &'b mut ScriptInvocation<'a>
    where
        'a: 'b,
    {
        arg.write_redis_args(&mut self.args);
        self
    }

    /// Adds a key argument to the invocation.  This ends up as `KEYS[i]`
    /// in the script.
    #[inline]
    pub fn key<'b, T: ToRedisArgs>(&'b mut self, key: T) -> &'b mut ScriptInvocation<'a>
    where
        'a: 'b,
    {
        key.write_redis_args(&mut self.keys);
        self
    }

    /// Invokes the script and returns the result.
    #[inline]
    pub fn invoke<T: FromRedisValue>(&self, con: &mut dyn ConnectionLike) -> RedisResult<T> {
        loop {
            match cmd("EVALSHA")
                .arg(self.script.hash.as_bytes())
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
                        cmd("SCRIPT")
                            .arg("LOAD")
                            .arg(self.script.code.as_bytes())
                            .query(con)?;
                    } else {
                        fail!(err);
                    }
                }
            }
        }
    }

    /// Asynchronously invokes the script and returns the result.
    #[inline]
    pub fn invoke_async<'c, C, T: FromRedisValue + Send + 'static>(
        &self,
        con: &'c mut C,
    ) -> impl Future<Output = RedisResult<T>> + 'c
    where
        C: aio::ConnectionLike + Clone + Send + Unpin + 'static,
        T: FromRedisValue + Send + 'static,
    {
        let mut eval_cmd = cmd("EVALSHA");
        eval_cmd
            .arg(self.script.hash.as_bytes())
            .arg(self.keys.len())
            .arg(&*self.keys)
            .arg(&*self.args);

        let mut load_cmd = cmd("SCRIPT");
        load_cmd.arg("LOAD").arg(self.script.code.as_bytes());
        async move {
            let future = {
                let mut con = con.clone();
                let eval_cmd = eval_cmd.clone();
                CmdFuture::Eval((async move { eval_cmd.query_async(&mut con).await }).boxed())
            };
            InvokeAsyncFuture {
                con: con.clone(),
                eval_cmd,
                load_cmd,
                future,
            }
            .await
        }
    }
}

/// A future that runs the given script and loads it into Redis if
/// it has not already been loaded
struct InvokeAsyncFuture<C, T> {
    con: C,
    eval_cmd: Cmd,
    load_cmd: Cmd,
    future: CmdFuture<T>,
}

enum CmdFuture<T> {
    Load(RedisFuture<'static, String>),
    Eval(RedisFuture<'static, T>),
}

impl<C, T> Future for InvokeAsyncFuture<C, T>
where
    C: aio::ConnectionLike + Clone + Send + Unpin + 'static,
    T: FromRedisValue + Send + 'static,
{
    type Output = RedisResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
        let self_ = self.get_mut();
        loop {
            self_.future = match self_.future {
                CmdFuture::Load(ref mut future) => {
                    // When we're done loading the script into Redis, try eval'ing it again
                    let _hash = ready!(future.as_mut().poll(cx))?;

                    let mut con = self_.con.clone();
                    let eval_cmd = self_.eval_cmd.clone();
                    CmdFuture::Eval(async move { eval_cmd.query_async(&mut con).await }.boxed())
                }
                CmdFuture::Eval(ref mut future) => match ready!(future.as_mut().poll(cx)) {
                    Ok(val) => {
                        // Return the value from the script evaluation
                        return Ok(val).into();
                    }
                    Err(err) => {
                        // Load the script into Redis if the script hash wasn't there already
                        if err.kind() == ErrorKind::NoScriptError {
                            let load_cmd = self_.load_cmd.clone();
                            let mut con = self_.con.clone();
                            CmdFuture::Load(
                                async move { load_cmd.query_async(&mut con).await }.boxed(),
                            )
                        } else {
                            return Err(err).into();
                        }
                    }
                },
            };
        }
    }
}
