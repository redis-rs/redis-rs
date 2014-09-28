use types::{ToRedisArg, FromRedisValue, RedisResult};
use connection::Connection;

/// Represents redis commands.
pub struct Cmd {
    args: Vec<Vec<u8>>,
    scan_arg: u16,
}

/// Represents a redis iterator.
pub struct Iter<'a, T: FromRedisValue> {
    batch: Vec<T>,
    cursor: i64,
    con: &'a Connection,
    cmd: &'a Cmd,
}

impl<'a, T: FromRedisValue> Iterator<T> for Iter<'a, T> {

    #[inline]
    fn next(&mut self) -> Option<T> {
        match self.batch.pop() {
            Some(v) => { return Some(v); }
            None => {}
        };
        if self.cursor == 0 {
            return None;
        }

        let pcmd = unwrap_or!(self.cmd.get_packed_command_with_cursor(
            self.cursor), return None);
        let rv = unwrap_or!(self.con.send_packed_command(
            pcmd[]).ok(), return None);
        let (cur, mut batch) : (i64, Vec<T>) = unwrap_or!(
            FromRedisValue::from_redis_value(&rv).ok(), return None);
        batch.reverse();

        self.cursor = cur;
        self.batch = batch;
        self.batch.pop()
    }
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
///
/// Because currently rust's currently does not have an ideal system
/// for lifetimes of temporaries, sometimes you need to hold on to
/// the initially generated command:
///
/// ```rust
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let con = client.get_connection().unwrap();
/// let mut cmd = redis::cmd("SET");
/// let mut iter = cmd.arg("...").arg("...").iter(&con);
/// ```
impl Cmd {
    /// Creates a new empty command.
    pub fn new() -> Cmd {
        Cmd { args: vec![], scan_arg: 0 }
    }

    /// Appends an argument to the command.
    pub fn arg<T: ToRedisArg>(&mut self, arg: T) -> &mut Cmd {
        self.args.push(arg.to_redis_arg());
        self
    }

    /// Works similar to `arg` but adds a cursor argument.  This is always
    /// an integer and also flips the command implementation to support a
    /// different mode for the iterators where the iterator will ask for
    /// another batch of items when the local data is exhausted.
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let con = client.get_connection().unwrap();
    /// let mut cmd = redis::cmd("SSCAN");
    /// let mut iter : redis::Iter<int> = cmd.arg("my_set").cursor_arg(0).iter(&con).unwrap();
    /// for x in iter {
    ///     // do something with the item
    /// }
    /// ```
    pub fn cursor_arg(&mut self, start: u64) -> &mut Cmd {
        assert!(!self.in_scan_mode());
        self.scan_arg = self.args.len() as u16;
        self.arg(start)
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

    /// Like `get_packed_command` but replaces the cursor with the
    /// provided value.  If the command is not in scan mode, `None`
    /// is returned.
    fn get_packed_command_with_cursor(&self, cursor: i64) -> Option<Vec<u8>> {
        if !self.in_scan_mode() {
            return None;
        }
        let mut args = self.args.clone();
        *args.get_mut(self.scan_arg as uint) = cursor.to_redis_arg();
        let mut cmd = vec![];
        cmd.push_all(format!("*{}\r\n", args.len()).as_bytes());
        for item in args.iter() {
            cmd.push_all(item.as_slice());
        }
        Some(cmd)
    }

    /// Returns true if the command is in scan mode.
    pub fn in_scan_mode(&self) -> bool {
        self.scan_arg > 0
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

    /// Similar to `query()` but returns an iterator over the items of the
    /// bulk result or iterator.  In normal mode this is not in any way more
    /// efficient than just querying into a `Vec<T>` as it's internally
    /// implemented as buffering into a vector.  This however is useful when
    /// `cursor_arg` was used in which case the iterator will query for more
    /// items until the server side cursor is exhausted.
    ///
    /// This is useful for commands such as `SSCAN`, `SCAN` and others.
    ///
    /// One speciality of this function is that it will check if the response
    /// looks like a cursor or not and always just looks at the payload.
    /// This way you can use the function the same for responses in the
    /// format of `KEYS` (just a list) as well as `SSCAN` (which returns a
    /// tuple of cursor and list).
    pub fn iter<'a, T: FromRedisValue>(&'a mut self, con: &'a Connection)
            -> RedisResult<Iter<'a, T>> {
        let pcmd = self.get_packed_command();
        let rv = try!(con.send_packed_command(pcmd.as_slice()));
        let mut batch : Vec<T>;
        let mut cursor = 0;

        if rv.looks_like_cursor() {
            let (next, b) : (i64, Vec<T>) = try!(FromRedisValue::from_redis_value(&rv));
            batch = b;
            cursor = next;
        } else {
            batch = try!(FromRedisValue::from_redis_value(&rv));
        }

        batch.reverse();
        Ok(Iter {
            batch: batch,
            cursor: cursor,
            con: con,
            cmd: self
        })
    }

    /// This is a shortcut to `query()` that does not return a value and
    /// will fail the task if the query fails because of an error.  This is
    /// mainly useful in examples and for simple commands like setting
    /// keys.
    ///
    /// This is equivalent to a call of query like this:
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let con = client.get_connection().unwrap();
    /// let _ : () = redis::cmd("PING").execute(&con).unwrap();
    /// ```
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
