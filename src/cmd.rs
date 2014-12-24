use types::{ToRedisArgs, FromRedisValue, Value, RedisResult,
            ResponseError, from_redis_value};
use connection::ConnectionLike;

#[deriving(Clone)]
enum Arg<'a> {
    Simple(Vec<u8>),
    Cursor,
    Borrowed(&'a [u8]),
}


/// Represents redis commands.
#[deriving(Clone)]
pub struct Cmd {
    args: Vec<Arg<'static>>,
    cursor: Option<u64>,
    is_ignored: bool,
}

/// Represents a redis command pipeline.
pub struct Pipeline {
    commands: Vec<Cmd>,
    transaction_mode: bool,
}

/// Represents a redis iterator.
pub struct Iter<'a, T: FromRedisValue> {
    batch: Vec<T>,
    cursor: u64,
    con: &'a (ConnectionLike + 'a),
    cmd: Cmd,
}

impl<'a, T: FromRedisValue> Iterator<T> for Iter<'a, T> {

    #[inline]
    fn next(&mut self) -> Option<T> {
        // we need to do this in a loop until we produce at least one item
        // or we find the actual end of the iteration.  This is necessary
        // because with filtering an iterator it is possible that a whole
        // chunk is not matching the pattern and thus yielding empty results.
        loop {
            match self.batch.pop() {
                Some(v) => { return Some(v); }
                None => {}
            };
            if self.cursor == 0 {
                return None;
            }

            let pcmd = unwrap_or!(self.cmd.get_packed_command_with_cursor(
                self.cursor), return None);
            let rv = unwrap_or!(self.con.req_packed_command(
                pcmd[]).ok(), return None);
            let (cur, mut batch) : (u64, Vec<T>) = unwrap_or!(
                from_redis_value(&rv).ok(), return None);
            batch.reverse();

            self.cursor = cur;
            self.batch = batch;
        }
    }
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
                Arg::Cursor => encode(cursor.to_string().as_bytes()),
                Arg::Simple(ref val) => encode(val[]),
                Arg::Borrowed(ptr) => encode(ptr),
            }
        }
    }

    cmd
}

fn encode_pipeline(cmds: &[Cmd], atomic: bool) -> Vec<u8> {
    let mut rv = vec![];
    if atomic {
        rv.push_all(cmd("MULTI").get_packed_command()[]);
    }
    for cmd in cmds.iter() {
        rv.push_all(cmd.get_packed_command()[]);
    }
    if atomic {
        rv.push_all(cmd("EXEC").get_packed_command()[]);
    }
    rv
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
        Cmd { args: vec![], cursor: None, is_ignored: false }
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
    /// redis::cmd("SET").arg(["my_key", "my_value"].as_slice());
    /// redis::cmd("SET").arg("my_key").arg(42i);
    /// redis::cmd("SET").arg("my_key").arg(b"my_value");
    /// ```
    #[inline]
    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Cmd {
        for item in arg.to_redis_args().into_iter() {
            self.args.push(Arg::Simple(item));
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
    #[inline]
    pub fn cursor_arg(&mut self, cursor: u64) -> &mut Cmd {
        assert!(!self.in_scan_mode());
        self.cursor = Some(cursor);
        self.args.push(Arg::Cursor);
        self
    }

    /// Returns the packed command as a byte vector.
    #[inline]
    pub fn get_packed_command(&self) -> Vec<u8> {
        encode_command(&self.args, self.cursor.unwrap_or(0))
    }

    /// Like `get_packed_command` but replaces the cursor with the
    /// provided value.  If the command is not in scan mode, `None`
    /// is returned.
    #[inline]
    fn get_packed_command_with_cursor(&self, cursor: u64) -> Option<Vec<u8>> {
        if !self.in_scan_mode() {
            None
        } else {
            Some(encode_command(&self.args, cursor))
        }
    }

    /// Returns true if the command is in scan mode.
    #[inline]
    pub fn in_scan_mode(&self) -> bool {
        self.cursor.is_some()
    }

    /// Sends the command as query to the connection and converts the
    /// result to the target redis value.  This is the general way how
    /// you can retrieve data.
    #[inline]
    pub fn query<T: FromRedisValue>(&self, con: &ConnectionLike) -> RedisResult<T> {
        let pcmd = self.get_packed_command();
        match con.req_packed_command(pcmd.as_slice()) {
            Ok(val) => from_redis_value(&val),
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
    #[inline]
    pub fn iter<'a, T: FromRedisValue>(&self, con: &'a ConnectionLike)
            -> RedisResult<Iter<'a, T>> {
        let pcmd = self.get_packed_command();
        let rv = try!(con.req_packed_command(pcmd.as_slice()));
        let mut batch : Vec<T>;
        let mut cursor = 0;

        if rv.looks_like_cursor() {
            let (next, b) : (u64, Vec<T>) = try!(from_redis_value(&rv));
            batch = b;
            cursor = next;
        } else {
            batch = try!(from_redis_value(&rv));
        }

        batch.reverse();
        Ok(Iter {
            batch: batch,
            cursor: cursor,
            con: con,
            cmd: self.clone(),
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
    #[inline]
    pub fn execute(&self, con: &ConnectionLike) {
        let _ : () = self.query(con).unwrap();
    }
}


/// A pipeline allows you to send multiple commands in one go to the
/// redis server.  API wise it's very similar to just using a command
/// but it allows multiple commands to be chained and some features such
/// as iteration are not available.
/// 
/// Basic example:
///
/// ```rust,no_run
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let con = client.get_connection().unwrap();
/// let ((k1, k2),) : ((i32, i32),) = redis::pipe()
///     .cmd("SET").arg("key_1").arg(42i).ignore()
///     .cmd("SET").arg("key_2").arg(43i).ignore()
///     .cmd("MGET").arg(["key_1", "key_2"].as_slice()).query(&con).unwrap();
/// ```
///
/// As you can see with `cmd` you can start a new command.  By default
/// each command produces a value but for some you can ignore them by
/// calling `ignore` on the command.  That way it will be skipped in the
/// return value which is useful for `SET` commands and others, which
/// do not have a useful return value.
impl Pipeline {

    /// Creates an empty pipeline.  For consistency with the `cmd`
    /// api a `pipe` function is provided as alias.
    pub fn new() -> Pipeline {
        Pipeline { commands: vec![], transaction_mode: false }
    }

    /// Starts a new command.  Functions such as `arg` then become
    /// available to add more arguments to that command.
    #[inline]
    pub fn cmd(&mut self, name: &str) -> &mut Pipeline {
        self.commands.push(cmd(name));
        self
    }

    /// Adds a command to the pipeline.
    #[inline]
    pub fn add_command(&mut self, cmd: &Cmd) -> &mut Pipeline {
        self.commands.push(cmd.clone());
        self
    }

    #[inline]
    fn get_last_command(&mut self) -> &mut Cmd {
        let idx = match self.commands.len() {
            0 => panic!("No command on stack"),
            x => x - 1,
        };
        &mut self.commands[idx]
    }

    /// Adds an argument to the last started command.  This works similar
    /// to the `arg` method of the `Cmd` object.
    ///
    /// Note that this function fails the task if executed on an empty pipeline.
    #[inline]
    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Pipeline {
        {
            let cmd = self.get_last_command();
            cmd.arg(arg);
        }
        self
    }

    /// Instructs the pipeline to ignore the return value of this command.
    /// It will still be ensured that it is not an error, but any successful
    /// result is just thrown away.  This makes result processing through
    /// tuples much easier because you do not need to handle all the items
    /// you do not care about.
    ///
    /// Note that this function fails the task if executed on an empty pipeline.
    #[inline]
    pub fn ignore(&mut self) -> &mut Pipeline {
        {
            let cmd = self.get_last_command();
            cmd.is_ignored = true;
        }
        self
    }

    /// This enables atomic mode.  In atomic mode the whole pipeline is
    /// enclosed in `MULTI`/`EXEC`.  From the user's point of view nothing
    /// changes however.  This is easier than using `MULTI`/`EXEC` yourself
    /// as the format does not change.
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let con = client.get_connection().unwrap();
    /// let (k1, k2) : (i32, i32) = redis::pipe()
    ///     .atomic()
    ///     .cmd("GET").arg("key_1")
    ///     .cmd("GET").arg("key_2").query(&con).unwrap();
    /// ```
    #[inline]
    pub fn atomic(&mut self) -> &mut Pipeline {
        self.transaction_mode = true;
        self
    }

    fn make_pipeline_results(&self, resp: Vec<Value>) -> Value {
        let mut rv = vec![];
        for (idx, result) in resp.into_iter().enumerate() {
            if !self.commands[idx].is_ignored {
                rv.push(result);
            }
        }
        Value::Bulk(rv)
    }

    fn execute_pipelined(&self, con: &ConnectionLike) -> RedisResult<Value> {
        Ok(self.make_pipeline_results(try!(con.req_packed_commands(
            encode_pipeline(self.commands[], false)[],
            0, self.commands.len()))))
    }

    fn execute_transaction(&self, con: &ConnectionLike) -> RedisResult<Value> {
        let mut resp = try!(con.req_packed_commands(
            encode_pipeline(self.commands[], true)[],
            self.commands.len() + 1, 1));
        match resp.pop() {
            Some(Value::Nil) => Ok(Value::Nil),
            Some(Value::Bulk(items)) => Ok(self.make_pipeline_results(items)),
            _ => fail!((ResponseError, "Invalid response when parsing multi response"))
        }
    }

    /// Executes the pipeline and fetches the return values.  Since most
    /// pipelines return different types it's recommended to use tuple
    /// matching to process the results:
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let con = client.get_connection().unwrap();
    /// let (k1, k2) : (i32, i32) = redis::pipe()
    ///     .cmd("SET").arg("key_1").arg(42i).ignore()
    ///     .cmd("SET").arg("key_2").arg(43i).ignore()
    ///     .cmd("GET").arg("key_1")
    ///     .cmd("GET").arg("key_2").query(&con).unwrap();
    /// ```
    #[inline]
    pub fn query<T: FromRedisValue>(&self, con: &ConnectionLike) -> RedisResult<T> {
        from_redis_value(&(
            if self.commands.len() == 0 {
                Value::Bulk(vec![])
            } else if self.transaction_mode {
                try!(self.execute_transaction(con))
            } else {
                try!(self.execute_pipelined(con))
            }
        ))
    }

    /// This is a shortcut to `query()` that does not return a value and
    /// will fail the task if the query of the pipeline fails.
    ///
    /// This is equivalent to a call of query like this:
    ///
    /// ```rust,no_run
    /// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    /// # let con = client.get_connection().unwrap();
    /// let _ : () = redis::pipe().cmd("PING").query(&con).unwrap();
    /// ```
    #[inline]
    pub fn execute(&self, con: &ConnectionLike) {
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
pub fn cmd<'a>(name: &'a str) -> Cmd {
    let mut rv = Cmd::new();
    rv.arg(name);
    rv
}

/// Packs a bunch of commands into a request.  This is generally a quite
/// useless function as this functionality is nicely wrapped through the
/// `Cmd` object, but in some cases it can be useful.  The return value
/// of this can then be send to the low level `ConnectionLike` methods.
///
/// Example:
///
/// ```rust
/// # use redis::ToRedisArgs;
/// let mut args = vec![];
/// args.push_all("SET".to_redis_args().as_slice());
/// args.push_all("my_key".to_redis_args().as_slice());
/// args.push_all(42i.to_redis_args().as_slice());
/// let cmd = redis::pack_command(args.as_slice());
/// assert_eq!(cmd, b"*3\r\n$3\r\nSET\r\n$6\r\nmy_key\r\n$2\r\n42\r\n".to_vec());
/// ```
pub fn pack_command(args: &[Vec<u8>]) -> Vec<u8> {
    encode_command(&args.iter().map(|x| Arg::Borrowed(x[])).collect(), 0)
}

/// Shortcut for creating a new pipeline.
pub fn pipe() -> Pipeline {
    Pipeline::new()
}
