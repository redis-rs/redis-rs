use std::iter::Iterator;

use crate::cmd::{Arg, Cmd};
use crate::types::Value;

pub(crate) const SLOT_SIZE: usize = 16384;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum RoutingInfo {
    AllNodes,
    AllMasters,
    Random,
    Slot(u16),
}

impl RoutingInfo {
    pub(crate) fn for_routable<R>(r: &R) -> Option<RoutingInfo>
    where
        R: Routable + ?Sized,
    {
        match &r.command()?[..] {
            b"FLUSHALL" | b"FLUSHDB" | b"SCRIPT" => Some(RoutingInfo::AllMasters),
            b"ECHO" | b"CONFIG" | b"CLIENT" | b"SLOWLOG" | b"DBSIZE" | b"LASTSAVE" | b"PING"
            | b"INFO" | b"BGREWRITEAOF" | b"BGSAVE" | b"CLIENT LIST" | b"SAVE" | b"TIME"
            | b"KEYS" => Some(RoutingInfo::AllNodes),
            b"SCAN" | b"CLIENT SETNAME" | b"SHUTDOWN" | b"SLAVEOF" | b"REPLICAOF"
            | b"SCRIPT KILL" | b"MOVE" | b"BITOP" => None,
            b"EVALSHA" | b"EVAL" => {
                let key_count = r
                    .arg_idx(2)
                    .and_then(|x| std::str::from_utf8(x).ok())
                    .and_then(|x| x.parse::<u64>().ok())?;
                if key_count == 0 {
                    Some(RoutingInfo::Random)
                } else {
                    r.arg_idx(3).and_then(RoutingInfo::for_key)
                }
            }
            b"XGROUP" | b"XINFO" => r.arg_idx(2).and_then(RoutingInfo::for_key),
            b"XREAD" | b"XREADGROUP" => {
                let streams_position = r.position(b"STREAMS")?;
                r.arg_idx(streams_position + 1)
                    .and_then(RoutingInfo::for_key)
            }
            _ => match r.arg_idx(1) {
                Some(key) => RoutingInfo::for_key(key),
                None => Some(RoutingInfo::Random),
            },
        }
    }

    pub fn for_key(key: &[u8]) -> Option<RoutingInfo> {
        let key = match get_hashtag(key) {
            Some(tag) => tag,
            None => key,
        };
        Some(RoutingInfo::Slot(
            crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE as u16,
        ))
    }
}

pub(crate) trait Routable {
    // Convenience function to return ascii uppercase version of the
    // the first argument (i.e., the command).
    fn command(&self) -> Option<Vec<u8>> {
        self.arg_idx(0).map(|x| x.to_ascii_uppercase())
    }

    // Returns a reference to the data for the argument at `idx`.
    fn arg_idx(&self, idx: usize) -> Option<&[u8]>;

    // Returns index of argument that matches `candidate`, if it exists
    fn position(&self, candidate: &[u8]) -> Option<usize>;
}

impl Routable for Cmd {
    fn arg_idx(&self, idx: usize) -> Option<&[u8]> {
        self.arg_idx(idx)
    }

    fn position(&self, candidate: &[u8]) -> Option<usize> {
        self.args_iter().position(|a| match a {
            Arg::Simple(d) => d.eq_ignore_ascii_case(candidate),
            _ => false,
        })
    }
}

impl Routable for Value {
    fn arg_idx(&self, idx: usize) -> Option<&[u8]> {
        match self {
            Value::Bulk(args) => match args.get(idx) {
                Some(Value::Data(ref data)) => Some(&data[..]),
                _ => None,
            },
            _ => None,
        }
    }

    fn position(&self, candidate: &[u8]) -> Option<usize> {
        match self {
            Value::Bulk(args) => args.iter().position(|a| match a {
                Value::Data(d) => d.eq_ignore_ascii_case(candidate),
                _ => false,
            }),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Slot {
    start: u16,
    end: u16,
    master: String,
    replicas: Vec<String>,
}

impl Slot {
    pub fn new(s: u16, e: u16, m: String, r: Vec<String>) -> Self {
        Self {
            start: s,
            end: e,
            master: m,
            replicas: r,
        }
    }

    pub fn start(&self) -> u16 {
        self.start
    }

    pub fn end(&self) -> u16 {
        self.end
    }

    pub fn master(&self) -> &str {
        &self.master
    }

    #[allow(dead_code)]
    pub fn replicas(&self) -> &Vec<String> {
        &self.replicas
    }
}

fn get_hashtag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|v| *v == b'{');
    let open = match open {
        Some(open) => open,
        None => return None,
    };

    let close = key[open..].iter().position(|v| *v == b'}');
    let close = match close {
        Some(close) => close,
        None => return None,
    };

    let rv = &key[open + 1..open + close];
    if rv.is_empty() {
        None
    } else {
        Some(rv)
    }
}

#[cfg(test)]
mod tests {
    use super::{get_hashtag, RoutingInfo};
    use crate::{cmd, parser::parse_redis_value};

    #[test]
    fn test_get_hashtag() {
        assert_eq!(get_hashtag(&b"foo{bar}baz"[..]), Some(&b"bar"[..]));
        assert_eq!(get_hashtag(&b"foo{}{baz}"[..]), None);
        assert_eq!(get_hashtag(&b"foo{{bar}}zap"[..]), Some(&b"{bar"[..]));
    }

    #[test]
    fn test_routing_info_mixed_capatalization() {
        let mut upper = cmd("XREAD");
        upper.arg("STREAMS").arg("foo").arg(0);

        let mut lower = cmd("xread");
        lower.arg("streams").arg("foo").arg(0);

        assert_eq!(
            RoutingInfo::for_routable(&upper).unwrap(),
            RoutingInfo::for_routable(&lower).unwrap()
        );

        let mut mixed = cmd("xReAd");
        mixed.arg("StReAmS").arg("foo").arg(0);

        assert_eq!(
            RoutingInfo::for_routable(&lower).unwrap(),
            RoutingInfo::for_routable(&mixed).unwrap()
        );
    }

    #[test]
    fn test_routing_info() {
        let mut test_cmds = vec![];

        // RoutingInfo::AllMasters
        let mut test_cmd = cmd("FLUSHALL");
        test_cmd.arg("");
        test_cmds.push(test_cmd);

        // RoutingInfo::AllNodes
        test_cmd = cmd("ECHO");
        test_cmd.arg("");
        test_cmds.push(test_cmd);

        // Routing key is 2nd arg ("42")
        test_cmd = cmd("SET");
        test_cmd.arg("42");
        test_cmds.push(test_cmd);

        // Routing key is 3rd arg ("FOOBAR")
        test_cmd = cmd("XINFO");
        test_cmd.arg("GROUPS").arg("FOOBAR");
        test_cmds.push(test_cmd);

        // Routing key is 3rd or 4th arg (3rd = "0" == RoutingInfo::Random)
        test_cmd = cmd("EVAL");
        test_cmd.arg("FOO").arg("0").arg("BAR");
        test_cmds.push(test_cmd);

        // Routing key is 3rd or 4th arg (3rd != "0" == RoutingInfo::Slot)
        test_cmd = cmd("EVAL");
        test_cmd.arg("FOO").arg("4").arg("BAR");
        test_cmds.push(test_cmd);

        // Routing key position is variable, 3rd arg
        test_cmd = cmd("XREAD");
        test_cmd.arg("STREAMS").arg("4");
        test_cmds.push(test_cmd);

        // Routing key position is variable, 4th arg
        test_cmd = cmd("XREAD");
        test_cmd.arg("FOO").arg("STREAMS").arg("4");
        test_cmds.push(test_cmd);

        for cmd in test_cmds {
            let value = parse_redis_value(&cmd.get_packed_command()).unwrap();
            assert_eq!(
                RoutingInfo::for_routable(&value).unwrap(),
                RoutingInfo::for_routable(&cmd).unwrap(),
            );
        }
    }
}
