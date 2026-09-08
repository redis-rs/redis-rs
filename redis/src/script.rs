#![cfg(feature = "script")]
use sha1_smol::Sha1;

use crate::{
    Cmd, ErrorKind, RedisWrite, ToSingleRedisArg,
    connection::ConnectionLike,
    types::{FromRedisValue, RedisResult, ToRedisArgs},
};

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
    pub fn new(code: &str) -> Self {
        let mut hash = Sha1::new();
        hash.update(code.as_bytes());
        Self {
            code: code.to_string(),
            hash: hash.digest().to_string(),
        }
    }

    /// Returns the script's SHA1 hash in hexadecimal format.
    pub fn get_hash(&self) -> &str {
        &self.hash
    }

    /// Loads the script and returns the SHA1 of it.
    #[inline]
    pub fn load(&self, con: &mut dyn ConnectionLike) -> RedisResult<String> {
        let hash: String = Cmd::load_script(self).query(con)?;

        debug_assert_eq!(hash, self.hash);

        Ok(hash)
    }

    /// Asynchronously loads the script and returns the SHA1 of it.
    #[inline]
    #[cfg(feature = "aio")]
    pub async fn load_async<C>(&self, con: &mut C) -> RedisResult<String>
    where
        C: crate::aio::ConnectionLike,
    {
        let hash: String = Cmd::load_script(self).query_async(con).await?;

        debug_assert_eq!(hash, self.hash);

        Ok(hash)
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

    /// Asynchronously invokes the script without arguments.
    #[inline]
    #[cfg(feature = "aio")]
    pub async fn invoke_async<C, T>(&self, con: &mut C) -> RedisResult<T>
    where
        C: crate::aio::ConnectionLike,
        T: FromRedisValue,
    {
        ScriptInvocation {
            script: self,
            args: vec![],
            keys: vec![],
        }
        .invoke_async(con)
        .await
    }
}

impl ToRedisArgs for Script {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.code.as_bytes());
    }

    #[inline]
    fn args_size(&self) -> usize {
        self.code.len()
    }
}

impl ToSingleRedisArg for Script {}

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
    pub fn arg<'b, T: ToRedisArgs>(&'b mut self, arg: T) -> &'b mut Self
    where
        'a: 'b,
    {
        arg.write_redis_args(&mut self.args);
        self
    }

    /// Adds a key argument to the invocation.  This ends up as `KEYS[i]`
    /// in the script.
    #[inline]
    pub fn key<'b, T: ToRedisArgs>(&'b mut self, key: T) -> &'b mut Self
    where
        'a: 'b,
    {
        key.write_redis_args(&mut self.keys);
        self
    }

    /// Invokes the script and returns the result.
    #[inline]
    pub fn invoke<T: FromRedisValue>(&self, con: &mut dyn ConnectionLike) -> RedisResult<T> {
        let eval_cmd = Cmd::invoke_script(self);
        match eval_cmd.query(con) {
            Ok(val) => Ok(val),
            Err(err) => {
                if err.kind() == ErrorKind::Server(crate::ServerErrorKind::NoScript) {
                    self.load(con)?;
                    eval_cmd.query(con)
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Asynchronously invokes the script and returns the result.
    #[inline]
    #[cfg(feature = "aio")]
    pub async fn invoke_async<T: FromRedisValue>(
        &self,
        con: &mut impl crate::aio::ConnectionLike,
    ) -> RedisResult<T> {
        let eval_cmd = Cmd::invoke_script(self);
        match eval_cmd.query_async(con).await {
            Ok(val) => {
                // Return the value from the script evaluation
                Ok(val)
            }
            Err(err) => {
                // Load the script into Redis if the script hash wasn't there already
                if err.kind() == ErrorKind::Server(crate::ServerErrorKind::NoScript) {
                    self.load_async(con).await?;
                    eval_cmd.query_async(con).await
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Loads the script and returns the SHA1 of it.
    #[inline]
    pub fn load(&self, con: &mut dyn ConnectionLike) -> RedisResult<String> {
        self.script.load(con)
    }

    /// Asynchronously loads the script and returns the SHA1 of it.
    #[inline]
    #[cfg(feature = "aio")]
    pub async fn load_async<C>(&self, con: &mut C) -> RedisResult<String>
    where
        C: crate::aio::ConnectionLike,
    {
        self.script.load_async(con).await
    }
}

impl ToRedisArgs for ScriptInvocation<'_> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.script.hash.as_bytes());
        self.keys.len().write_redis_args(out);
        self.keys.write_redis_args(out);
        self.args.write_redis_args(out);
    }

    #[inline]
    fn num_of_args(&self) -> usize {
        2 + self.keys.len() + self.args.len()
    }

    #[inline]
    fn args_size(&self) -> usize {
        self.keys
            .iter()
            .chain(self.args.iter())
            .fold(0, |acc, e| acc + e.len())
            + self.script.hash.len()
            + 4 /* Slots reserved for the length of keys. */
    }
}

#[cfg(test)]
mod tests {
    use super::Script;
    use crate::Cmd;
    use crate::types::ToRedisArgs;

    #[test]
    fn script_eval_should_work() {
        let script = Script::new("return KEYS[1]");
        let invocation = script.key("dummy");
        assert_eq!(invocation.args_size(), 49);
        let cmd = Cmd::invoke_script(&invocation);
        let expected = "*4\r\n$7\r\nEVALSHA\r\n$40\r\n4a2267357833227dd98abdedb8cf24b15a986445\r\n$1\r\n1\r\n$5\r\ndummy\r\n";
        assert_eq!(
            expected,
            std::str::from_utf8(cmd.get_packed_command().as_slice()).unwrap()
        );
    }
}
