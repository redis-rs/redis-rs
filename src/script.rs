#![cfg(feature = "script")]
use sha1_smol::Sha1;

use crate::cmd::cmd;
use crate::connection::ConnectionLike;
use crate::types::{ErrorKind, FromRedisValue, RedisResult, ToRedisArgs};

/// Represents a lua script.
#[derive(Debug, Clone)]
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
    #[cfg(feature = "aio")]
    pub async fn invoke_async<C, T>(&self, con: &mut C) -> RedisResult<T>
    where
        C: crate::aio::ConnectionLike,
        T: FromRedisValue,
    {
        let mut eval_cmd = cmd("EVALSHA");
        eval_cmd
            .arg(self.script.hash.as_bytes())
            .arg(self.keys.len())
            .arg(&*self.keys)
            .arg(&*self.args);

        let mut load_cmd = cmd("SCRIPT");
        load_cmd.arg("LOAD").arg(self.script.code.as_bytes());
        match eval_cmd.query_async(con).await {
            Ok(val) => {
                // Return the value from the script evaluation
                Ok(val)
            }
            Err(err) => {
                // Load the script into Redis if the script hash wasn't there already
                if err.kind() == ErrorKind::NoScriptError {
                    load_cmd.query_async(con).await?;
                    eval_cmd.query_async(con).await
                } else {
                    Err(err)
                }
            }
        }
    }
}
