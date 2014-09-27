use types::{ToRedisArg, FromRedisValue, RedisResult};
use connection::Connection;

pub struct Cmd {
    args: Vec<Vec<u8>>
}

impl Cmd {
    pub fn new() -> Cmd {
        Cmd { args: vec![] }
    }

    pub fn arg<T: ToRedisArg>(&mut self, arg: T) -> &mut Cmd {
        self.args.push(arg.to_redis_arg());
        self
    }

    pub fn execute<T: FromRedisValue>(&self, con: &mut Connection) -> RedisResult<T> {
        let mut cmd = vec![];
        cmd.push_all(format!("*{}\r\n", self.args.len()).as_bytes());
        for item in self.args.iter() {
            cmd.push_all(item.as_slice());
        }
        match con.send_request(cmd.as_slice()) {
            Ok(val) => FromRedisValue::from_redis_value(&val),
            Err(e) => Err(e),
        }
    }
}

pub fn cmd(name: &'static str) -> Cmd {
    let mut rv = Cmd::new();
    rv.arg(name);
    rv
}
