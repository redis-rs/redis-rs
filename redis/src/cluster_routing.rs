use std::collections::{BTreeMap, HashSet};
use std::iter::Iterator;

use rand::seq::SliceRandom;
use rand::thread_rng;

use crate::cmd::{Arg, Cmd};
use crate::commands::is_readonly_cmd;
use crate::types::Value;

pub(crate) const SLOT_SIZE: u16 = 16384;

fn slot(key: &[u8]) -> u16 {
    crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE
}

#[derive(Clone)]
pub(crate) enum Redirect {
    Moved(String),
    Ask(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum RoutingInfo {
    AllNodes,
    AllMasters,
    Random,
    SpecificNode(Route),
}

impl RoutingInfo {
    pub(crate) fn for_routable<R>(r: &R) -> Option<RoutingInfo>
    where
        R: Routable + ?Sized,
    {
        let cmd = &r.command()?[..];
        match cmd {
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
                    r.arg_idx(3).map(|key| RoutingInfo::for_key(cmd, key))
                }
            }
            b"XGROUP" | b"XINFO" => r.arg_idx(2).map(|key| RoutingInfo::for_key(cmd, key)),
            b"XREAD" | b"XREADGROUP" => {
                let streams_position = r.position(b"STREAMS")?;
                r.arg_idx(streams_position + 1)
                    .map(|key| RoutingInfo::for_key(cmd, key))
            }
            _ => match r.arg_idx(1) {
                Some(key) => Some(RoutingInfo::for_key(cmd, key)),
                None => Some(RoutingInfo::Random),
            },
        }
    }

    pub fn for_key(cmd: &[u8], key: &[u8]) -> RoutingInfo {
        let key = match get_hashtag(key) {
            Some(tag) => tag,
            None => key,
        };

        let slot = slot(key);
        if is_readonly_cmd(cmd) {
            RoutingInfo::SpecificNode(Route::new(slot, SlotAddr::Replica))
        } else {
            RoutingInfo::SpecificNode(Route::new(slot, SlotAddr::Master))
        }
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

    pub fn replicas(&self) -> &Vec<String> {
        &self.replicas
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub(crate) enum SlotAddr {
    Master,
    Replica,
}

/// This is just a simplified version of [`Slot`],
/// which stores only the master and [optional] replica
/// to avoid the need to choose a replica each time
/// a command is executed
#[derive(Debug)]
pub(crate) struct SlotAddrs([String; 2]);

impl SlotAddrs {
    pub(crate) fn new(master_node: String, replica_node: Option<String>) -> Self {
        let replica = replica_node.unwrap_or_else(|| master_node.clone());
        Self([master_node, replica])
    }

    pub(crate) fn slot_addr(&self, slot_addr: &SlotAddr) -> &str {
        match slot_addr {
            SlotAddr::Master => &self.0[0],
            SlotAddr::Replica => &self.0[1],
        }
    }

    pub(crate) fn from_slot(slot: &Slot, read_from_replicas: bool) -> Self {
        let replica = if !read_from_replicas || slot.replicas().is_empty() {
            None
        } else {
            Some(
                slot.replicas()
                    .choose(&mut thread_rng())
                    .unwrap()
                    .to_string(),
            )
        };

        SlotAddrs::new(slot.master().to_string(), replica)
    }
}

impl<'a> IntoIterator for &'a SlotAddrs {
    type Item = &'a String;
    type IntoIter = std::slice::Iter<'a, String>;

    fn into_iter(self) -> std::slice::Iter<'a, String> {
        self.0.iter()
    }
}

#[derive(Debug, Default)]
pub(crate) struct SlotMap(BTreeMap<u16, SlotAddrs>);

impl SlotMap {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn from_slots(slots: &[Slot], read_from_replicas: bool) -> Self {
        Self(
            slots
                .iter()
                .map(|slot| (slot.end(), SlotAddrs::from_slot(slot, read_from_replicas)))
                .collect(),
        )
    }

    pub fn fill_slots(&mut self, slots: &[Slot], read_from_replicas: bool) {
        for slot in slots {
            self.0
                .insert(slot.end(), SlotAddrs::from_slot(slot, read_from_replicas));
        }
    }

    pub fn slot_addr_for_route(&self, route: &Route) -> Option<&str> {
        self.0
            .range(route.slot()..)
            .next()
            .map(|(_, slot_addrs)| slot_addrs.slot_addr(route.slot_addr()))
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn values(&self) -> std::collections::btree_map::Values<u16, SlotAddrs> {
        self.0.values()
    }

    pub fn all_unique_addresses(&self, only_primaries: bool) -> HashSet<&str> {
        let mut addresses = HashSet::new();
        for slot in self.values() {
            addresses.insert(slot.slot_addr(&SlotAddr::Master));

            if !only_primaries {
                addresses.insert(slot.slot_addr(&SlotAddr::Replica));
            }
        }
        addresses
    }
}

/// Defines the slot and the [`SlotAddr`] to which
/// a command should be sent
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub(crate) struct Route(u16, SlotAddr);

impl Route {
    pub(crate) fn new(slot: u16, slot_addr: SlotAddr) -> Self {
        Self(slot, slot_addr)
    }

    pub(crate) fn slot(&self) -> u16 {
        self.0
    }

    pub(crate) fn slot_addr(&self) -> &SlotAddr {
        &self.1
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
    use super::{get_hashtag, slot, Route, RoutingInfo, Slot, SlotMap};
    use crate::{cluster_routing::SlotAddr, cmd, parser::parse_redis_value};

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

        // Assert expected RoutingInfo explicitly:

        for cmd in vec![cmd("FLUSHALL"), cmd("FLUSHDB"), cmd("SCRIPT")] {
            assert_eq!(
                RoutingInfo::for_routable(&cmd),
                Some(RoutingInfo::AllMasters)
            );
        }

        for cmd in vec![
            cmd("ECHO"),
            cmd("CONFIG"),
            cmd("CLIENT"),
            cmd("SLOWLOG"),
            cmd("DBSIZE"),
            cmd("LASTSAVE"),
            cmd("PING"),
            cmd("INFO"),
            cmd("BGREWRITEAOF"),
            cmd("BGSAVE"),
            cmd("CLIENT LIST"),
            cmd("SAVE"),
            cmd("TIME"),
            cmd("KEYS"),
        ] {
            assert_eq!(RoutingInfo::for_routable(&cmd), Some(RoutingInfo::AllNodes));
        }

        for cmd in vec![
            cmd("SCAN"),
            cmd("CLIENT SETNAME"),
            cmd("SHUTDOWN"),
            cmd("SLAVEOF"),
            cmd("REPLICAOF"),
            cmd("SCRIPT KILL"),
            cmd("MOVE"),
            cmd("BITOP"),
        ] {
            assert_eq!(RoutingInfo::for_routable(&cmd), None,);
        }

        for cmd in vec![
            cmd("EVAL").arg(r#"redis.call("PING");"#).arg(0),
            cmd("EVALSHA").arg(r#"redis.call("PING");"#).arg(0),
        ] {
            assert_eq!(RoutingInfo::for_routable(cmd), Some(RoutingInfo::Random));
        }

        for (cmd, expected) in vec![
            (
                cmd("EVAL")
                    .arg(r#"redis.call("GET, KEYS[1]");"#)
                    .arg(1)
                    .arg("foo"),
                Some(RoutingInfo::SpecificNode(Route::new(
                    slot(b"foo"),
                    SlotAddr::Master,
                ))),
            ),
            (
                cmd("XGROUP")
                    .arg("CREATE")
                    .arg("mystream")
                    .arg("workers")
                    .arg("$")
                    .arg("MKSTREAM"),
                Some(RoutingInfo::SpecificNode(Route::new(
                    slot(b"mystream"),
                    SlotAddr::Master,
                ))),
            ),
            (
                cmd("XINFO").arg("GROUPS").arg("foo"),
                Some(RoutingInfo::SpecificNode(Route::new(
                    slot(b"foo"),
                    SlotAddr::Replica,
                ))),
            ),
            (
                cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg("wkrs")
                    .arg("consmrs")
                    .arg("STREAMS")
                    .arg("mystream"),
                Some(RoutingInfo::SpecificNode(Route::new(
                    slot(b"mystream"),
                    SlotAddr::Master,
                ))),
            ),
            (
                cmd("XREAD")
                    .arg("COUNT")
                    .arg("2")
                    .arg("STREAMS")
                    .arg("mystream")
                    .arg("writers")
                    .arg("0-0")
                    .arg("0-0"),
                Some(RoutingInfo::SpecificNode(Route::new(
                    slot(b"mystream"),
                    SlotAddr::Replica,
                ))),
            ),
        ] {
            assert_eq!(RoutingInfo::for_routable(cmd), expected,);
        }
    }

    #[test]
    fn test_slot_for_packed_cmd() {
        assert!(matches!(RoutingInfo::for_routable(&parse_redis_value(&[
                42, 50, 13, 10, 36, 54, 13, 10, 69, 88, 73, 83, 84, 83, 13, 10, 36, 49, 54, 13, 10,
                244, 93, 23, 40, 126, 127, 253, 33, 89, 47, 185, 204, 171, 249, 96, 139, 13, 10
            ]).unwrap()), Some(RoutingInfo::SpecificNode(Route(slot, SlotAddr::Replica))) if slot == 964));

        assert!(matches!(RoutingInfo::for_routable(&parse_redis_value(&[
                42, 54, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10, 36, 49, 54, 13, 10, 36, 241,
                197, 111, 180, 254, 5, 175, 143, 146, 171, 39, 172, 23, 164, 145, 13, 10, 36, 52,
                13, 10, 116, 114, 117, 101, 13, 10, 36, 50, 13, 10, 78, 88, 13, 10, 36, 50, 13, 10,
                80, 88, 13, 10, 36, 55, 13, 10, 49, 56, 48, 48, 48, 48, 48, 13, 10
            ]).unwrap()), Some(RoutingInfo::SpecificNode(Route(slot, SlotAddr::Master))) if slot == 8352));

        assert!(matches!(RoutingInfo::for_routable(&parse_redis_value(&[
                42, 54, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10, 36, 49, 54, 13, 10, 169, 233,
                247, 59, 50, 247, 100, 232, 123, 140, 2, 101, 125, 221, 66, 170, 13, 10, 36, 52,
                13, 10, 116, 114, 117, 101, 13, 10, 36, 50, 13, 10, 78, 88, 13, 10, 36, 50, 13, 10,
                80, 88, 13, 10, 36, 55, 13, 10, 49, 56, 48, 48, 48, 48, 48, 13, 10
            ]).unwrap()), Some(RoutingInfo::SpecificNode(Route(slot, SlotAddr::Master))) if slot == 5210));
    }

    #[test]
    fn test_slot_map() {
        let slot_map = SlotMap::from_slots(
            &[
                Slot {
                    start: 1,
                    end: 1000,
                    master: "node1:6379".to_owned(),
                    replicas: vec!["replica1:6379".to_owned()],
                },
                Slot {
                    start: 1001,
                    end: 2000,
                    master: "node2:6379".to_owned(),
                    replicas: vec!["replica2:6379".to_owned()],
                },
            ],
            true,
        );

        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(500, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "replica1:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1000, SlotAddr::Replica))
                .unwrap()
        );
        assert_eq!(
            "node2:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1001, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node2:6379",
            slot_map
                .slot_addr_for_route(&Route::new(1500, SlotAddr::Master))
                .unwrap()
        );
        assert_eq!(
            "node2:6379",
            slot_map
                .slot_addr_for_route(&Route::new(2000, SlotAddr::Master))
                .unwrap()
        );
        assert!(slot_map
            .slot_addr_for_route(&Route::new(2001, SlotAddr::Master))
            .is_none());
    }
}
