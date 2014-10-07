use crypto::sha1::Sha1;
use crypto::digest::Digest;

use cmd::cmd;
use types::{ToRedisArgs, FromRedisValue, RedisResult, NoScriptError, Error};
use connection::Connection;

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
/// # let con = client.get_connection().unwrap();
/// let script = redis::Script::new(r"
///     return tonumber(ARGV[1]) + tonumber(ARGV[2]);
/// ");
/// let result = script.arg(1i).arg(2i).invoke(&con);
/// assert_eq!(result, Ok(3i));
/// ```
impl Script {

    /// Creates a new script object.
    pub fn new<'a>(code: &'a str) -> Script {
        let mut hash = Sha1::new();
        hash.input(code.as_bytes());
        Script {
            code: code.to_string(),
            hash: hash.result_str(),
        }
    }

    /// Returns the script's SHA1 hash in hexadecimal format.
    pub fn get_hash<'a>(&'a self) -> &'a str {
        self.hash.as_slice()
    }

    /// Creates a script invocation object with a key filled in.
    #[inline]
    pub fn key<'a, T: ToRedisArgs>(&'a self, key: T) -> ScriptInvocation<'a> {
        ScriptInvocation {
            script: self,
            args: vec![],
            keys: key.to_redis_args(),
        }
    }

    /// Creates a script invocation object with an argument filled in.
    #[inline]
    pub fn arg<'a, T: ToRedisArgs>(&'a self, arg: T) -> ScriptInvocation<'a> {
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
    pub fn prepare_invoke<'a>(&'a self) -> ScriptInvocation<'a> {
        ScriptInvocation { script: self, args: vec![], keys: vec![] }
    }

    /// Invokes the script directly without arguments.
    #[inline]
    pub fn invoke<T: FromRedisValue>(&self, con: &Connection) -> RedisResult<T> {
        ScriptInvocation {
            script: self,
            args: vec![],
            keys: vec![],
        }.invoke(con)
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
    pub fn arg<T: ToRedisArgs>(&'a mut self, arg: T) -> &'a mut ScriptInvocation {
        self.args.push_all(arg.to_redis_args()[]);
        self
    }

    /// Adds a key argument to the invocation.  This ends up as `KEYS[i]`
    /// in the script.
    #[inline]
    pub fn key<T: ToRedisArgs>(&'a mut self, key: T) -> &'a mut ScriptInvocation {
        self.keys.push_all(key.to_redis_args()[]);
        self
    }

    /// Invokes the script and returns the result.
    #[inline]
    pub fn invoke<T: FromRedisValue>(&self, con: &Connection) -> RedisResult<T> {
        loop {
            match cmd("EVALSHA")
                .arg(self.script.hash.as_bytes())
                .arg(self.keys.len())
                .arg(self.keys[])
                .arg(self.args[]).query(con) {
                Ok(val) => { return Ok(val); }
                Err(Error { kind: NoScriptError, .. }) => {
                    let _ : () = try!(cmd("SCRIPT")
                        .arg("LOAD")
                        .arg(self.script.code.as_bytes())
                        .query(con));
                }
                Err(x) => { return Err(x); }
            }
        }
    }
}
