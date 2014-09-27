use types::{ToRedisArg, FromRedisValue, RedisResult};
use connection::Connection;

/// Represents redis commands.
pub struct Cmd {
    args: Vec<Vec<u8>>
}

/// A command acts as a builder interface to creating encoded redis
/// requests.  This allows you to easiy assemble a packed command
/// by chaining arguments together.
///
/// Basic example:
///
/// ```rust
/// redis::Cmd::new().arg("SET").arg("my_key").arg(42i);
/// ```
///
/// There is also a helper function called `cmd` which makes it a
/// tiny bit shorter:
///
/// ```rust
/// redis::cmd("SET").arg("my_key").arg(42i);
/// ```
impl Cmd {
    /// Creates a new empty command.
    pub fn new() -> Cmd {
        Cmd { args: vec![] }
    }

    /// Appends an argument to the command.
    pub fn arg<T: ToRedisArg>(&mut self, arg: T) -> &mut Cmd {
        self.args.push(arg.to_redis_arg());
        self
    }

    /// Returns the packed command as a byte vector.
    pub fn get_packed_command(&self) -> Vec<u8> {
        let mut cmd = vec![];
        cmd.push_all(format!("*{}\r\n", self.args.len()).as_bytes());
        for item in self.args.iter() {
            cmd.push_all(item.as_slice());
        }
        cmd
    }

    /// Sends the command as query to the connection and converts the
    /// result to the target redis value.  This is the general way how
    /// you can retrieve data.
    pub fn query<T: FromRedisValue>(&self, con: &Connection) -> RedisResult<T> {
        let pcmd = self.get_packed_command();
        match con.send_packed_command(pcmd.as_slice()) {
            Ok(val) => FromRedisValue::from_redis_value(&val),
            Err(e) => Err(e),
        }
    }

    /// This is a shortcut to `query()` that does not return a value and
    /// will fail the task if the query fails because of an error.  This is
    /// mainly useful in examples and for simple commands like setting
    /// keys.
    pub fn execute(&self, con: &Connection) {
        let _ : () = self.query(con).unwrap();
    }
}

/// Shortcut function to creating a command with a single argument.
///
/// The first argument of a redis command is always the name of the command
/// which needs to be a string.  This is the recommended way to start a
/// command pipe.
///
/// ```rust
/// redis::cmd("PING");
/// ```
pub fn cmd(name: &'static str) -> Cmd {
    let mut rv = Cmd::new();
    rv.arg(name);
    rv
}
