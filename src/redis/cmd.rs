use types::{ToRedisArgs, FromRedisValue, RedisResult};
use connection::Connection;

enum Arg {
    SimpleArg(Vec<u8>),
    CursorArg,
}


fn encode_command(args: &Vec<Arg>, cursor: u64) -> Vec<u8> {
    let mut cmd = vec![];
    cmd.push_all(format!("*{}\r\n", args.len()).as_bytes());

    {
        let encode = |item: &[u8]| {
            cmd.push_all(format!("${}\r\n", item.len()).as_bytes());
            cmd.push_all(item);
            cmd.push_all(b"\r\n");
        };

        for item in args.iter() {
            match *item {
                CursorArg => encode(cursor.to_string().as_bytes()),
                SimpleArg(ref val) => encode(val[]),
            }
        }
    }

    cmd
}

/// Represents redis commands.
pub struct Cmd {
    args: Vec<Arg>,
    cursor: Option<u64>,
}

/// Represents a redis iterator.
pub struct Iter<'a, T: FromRedisValue> {
    batch: Vec<T>,
    cursor: u64,
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
        let (cur, mut batch) : (u64, Vec<T>) = unwrap_or!(
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
/// ```rust,no_run
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let con = client.get_connection().unwrap();
/// let mut cmd = redis::cmd("SMEMBERS");
/// let mut iter : redis::Iter<i32> = cmd.arg("my_set").iter(&con).unwrap();
/// ```
impl Cmd {
    /// Creates a new empty command.
    pub fn new() -> Cmd {
        Cmd { args: vec![], cursor: None }
    }

    /// Appends an argument to the command.  The argument passed must
    /// be a type that implements `ToRedisArgs`.  Most primitive types as
    /// well as vectors of primitive types implement it.
    ///
    /// For instance all of the following are valid:
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let con = client.get_connection().unwrap();
    /// redis::cmd("SET").arg(["my_key", "my_value"][]);
    /// redis::cmd("SET").arg("my_key").arg(42i);
    /// redis::cmd("SET").arg("my_key").arg(b"my_value");
    /// ```
    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Cmd {
        for item in arg.to_redis_args().into_iter() {
            self.args.push(SimpleArg(item));
        }
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
    pub fn cursor_arg(&mut self, cursor: u64) -> &mut Cmd {
        assert!(!self.in_scan_mode());
        self.cursor = Some(cursor);
        self.args.push(CursorArg);
        self
    }

    /// Returns the packed command as a byte vector.
    pub fn get_packed_command(&self) -> Vec<u8> {
        encode_command(&self.args, self.cursor.unwrap_or(0))
    }

    /// Like `get_packed_command` but replaces the cursor with the
    /// provided value.  If the command is not in scan mode, `None`
    /// is returned.
    fn get_packed_command_with_cursor(&self, cursor: u64) -> Option<Vec<u8>> {
        if !self.in_scan_mode() {
            None
        } else {
            Some(encode_command(&self.args, cursor))
        }
    }

    /// Returns true if the command is in scan mode.
    pub fn in_scan_mode(&self) -> bool {
        self.cursor.is_some()
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
            let (next, b) : (u64, Vec<T>) = try!(FromRedisValue::from_redis_value(&rv));
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
    /// let _ : () = redis::cmd("PING").query(&con).unwrap();
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
