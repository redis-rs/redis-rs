use super::{parse_redis_value, Value};

pub(crate) const SLOT_SIZE: usize = 16384;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum RoutingInfo {
    AllNodes,
    AllMasters,
    Random,
    Slot(u16),
}

fn get_arg(values: &[Value], idx: usize) -> Option<&[u8]> {
    match values.get(idx) {
        Some(Value::Data(ref data)) => Some(&data[..]),
        _ => None,
    }
}

fn get_command_arg(values: &[Value], idx: usize) -> Option<Vec<u8>> {
    get_arg(values, idx).map(|x| x.to_ascii_uppercase())
}

fn get_u64_arg(values: &[Value], idx: usize) -> Option<u64> {
    get_arg(values, idx)
        .and_then(|x| std::str::from_utf8(x).ok())
        .and_then(|x| x.parse().ok())
}

impl RoutingInfo {
    pub fn for_packed_command(cmd: &[u8]) -> Option<RoutingInfo> {
        parse_redis_value(cmd).ok().and_then(RoutingInfo::for_value)
    }

    pub fn for_value(value: Value) -> Option<RoutingInfo> {
        let args = match value {
            Value::Bulk(args) => args,
            _ => return None,
        };

        match &get_command_arg(&args, 0)?[..] {
            b"FLUSHALL" | b"FLUSHDB" | b"SCRIPT" => Some(RoutingInfo::AllMasters),
            b"ECHO" | b"CONFIG" | b"CLIENT" | b"SLOWLOG" | b"DBSIZE" | b"LASTSAVE" | b"PING"
            | b"INFO" | b"BGREWRITEAOF" | b"BGSAVE" | b"CLIENT LIST" | b"SAVE" | b"TIME"
            | b"KEYS" => Some(RoutingInfo::AllNodes),
            b"SCAN" | b"CLIENT SETNAME" | b"SHUTDOWN" | b"SLAVEOF" | b"REPLICAOF"
            | b"SCRIPT KILL" | b"MOVE" | b"BITOP" => None,
            b"EVALSHA" | b"EVAL" => {
                let key_count = get_u64_arg(&args, 2)?;
                if key_count == 0 {
                    Some(RoutingInfo::Random)
                } else {
                    get_arg(&args, 3).and_then(RoutingInfo::for_key)
                }
            }
            b"XGROUP" | b"XINFO" => get_arg(&args, 2).and_then(RoutingInfo::for_key),
            b"XREAD" | b"XREADGROUP" => {
                let streams_position = args.iter().position(|a| match a {
                    Value::Data(a) => a.eq_ignore_ascii_case(b"STREAMS"),
                    _ => false,
                })?;
                get_arg(&args, streams_position + 1).and_then(RoutingInfo::for_key)
            }
            _ => match get_arg(&args, 1) {
                Some(key) => RoutingInfo::for_key(key),
                None => Some(RoutingInfo::Random),
            },
        }
    }

    pub fn for_key(key: &[u8]) -> Option<RoutingInfo> {
        let key = match get_hashtag(&key) {
            Some(tag) => tag,
            None => &key,
        };
        Some(RoutingInfo::Slot(
            crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE as u16,
        ))
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
    use crate::cmd;

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
            RoutingInfo::for_packed_command(&upper.get_packed_command()).unwrap(),
            RoutingInfo::for_packed_command(&lower.get_packed_command()).unwrap()
        );

        let mut mixed = cmd("xReAd");
        mixed.arg("StReAmS").arg("foo").arg(0);

        assert_eq!(
            RoutingInfo::for_packed_command(&lower.get_packed_command()).unwrap(),
            RoutingInfo::for_packed_command(&mixed.get_packed_command()).unwrap()
        );
    }
}
