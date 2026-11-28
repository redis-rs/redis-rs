//! Checks that routing by [`KnownCommand::base_routing`] and [`KnownCommand::key_positions`]
//! matches the routing tables that preceded them, which were keyed by command name.

use super::*;
use crate::cmd;
use crate::known_commands::Keyless;

enum LegacyRouteBy {
    AllNodes,
    AllPrimaries,
    FirstKey,
    MultiShard(MultiSlotArgPattern),
    Random,
    SecondArg,
    SecondArgAfterKeyCount,
    SecondArgSlot,
    StreamsIndex,
    ThirdArg,
    ThirdArgAfterKeyCount,
    Undefined,
}

fn legacy_base_routing(cmd: &[u8]) -> LegacyRouteBy {
    match cmd {
        b"ACL DELUSER"
        | b"ACL SAVE"
        | b"ACL SETUSER"
        | b"AUTH"
        | b"CLIENT KILL"
        | b"CLIENT SETINFO"
        | b"CLIENT SETNAME"
        | b"CONFIG RESETSTAT"
        | b"CONFIG REWRITE"
        | b"CONFIG SET"
        | b"FUNCTION KILL"
        | b"FUNCTION STATS"
        | b"PUBSUB CHANNELS"
        | b"PUBSUB NUMPAT"
        | b"PUBSUB NUMSUB"
        | b"PUBSUB SHARDCHANNELS"
        | b"PUBSUB SHARDNUMSUB"
        | b"RESET"
        | b"SCRIPT FLUSH"
        | b"SCRIPT KILL"
        | b"SCRIPT LOAD"
        | b"SELECT"
        | b"SLOWLOG GET"
        | b"SLOWLOG LEN"
        | b"SLOWLOG RESET" => LegacyRouteBy::AllNodes,

        b"BGREWRITEAOF"
        | b"BGSAVE"
        | b"CLIENT PAUSE"
        | b"CLIENT UNPAUSE"
        | b"DBSIZE"
        | b"DEBUG"
        | b"FLUSHALL"
        | b"FLUSHDB"
        | b"FT._ALIASLIST"
        | b"FT._LIST"
        | b"FUNCTION DELETE"
        | b"FUNCTION FLUSH"
        | b"FUNCTION LOAD"
        | b"FUNCTION RESTORE"
        | b"INFO"
        | b"KEYS"
        | b"LATENCY DOCTOR"
        | b"LATENCY GRAPH"
        | b"LATENCY HISTOGRAM"
        | b"LATENCY HISTORY"
        | b"LATENCY LATEST"
        | b"LATENCY RESET"
        | b"MEMORY DOCTOR"
        | b"MEMORY MALLOC-STATS"
        | b"MEMORY PURGE"
        | b"MEMORY STATS"
        | b"PING"
        | b"RANDOMKEY"
        | b"SAVE"
        | b"SCRIPT EXISTS"
        | b"UNWATCH"
        | b"WAIT"
        | b"WAITAOF" => LegacyRouteBy::AllPrimaries,

        b"MGET" | b"DEL" | b"EXISTS" | b"UNLINK" | b"TOUCH" | b"WATCH" => {
            LegacyRouteBy::MultiShard(MultiSlotArgPattern::KeysOnly)
        }

        b"MSET" => LegacyRouteBy::MultiShard(MultiSlotArgPattern::KeyValuePairs),
        b"JSON.MGET" => LegacyRouteBy::MultiShard(MultiSlotArgPattern::KeysAndLastArg),
        b"JSON.MSET" => LegacyRouteBy::MultiShard(MultiSlotArgPattern::KeyWithTwoArgTriples),
        // TODO - special handling - b"SCAN"
        b"SCAN" | b"SHUTDOWN" | b"SLAVEOF" | b"REPLICAOF" => LegacyRouteBy::Undefined,

        b"BLMPOP" | b"BZMPOP" | b"EVAL" | b"EVALSHA" | b"EVALSHA_RO" | b"EVAL_RO" | b"FCALL"
        | b"FCALL_RO" => LegacyRouteBy::ThirdArgAfterKeyCount,

        b"BITOP"
        | b"MEMORY USAGE"
        | b"PFDEBUG"
        | b"XGROUP CREATE"
        | b"XGROUP CREATECONSUMER"
        | b"XGROUP DELCONSUMER"
        | b"XGROUP DESTROY"
        | b"XGROUP SETID"
        | b"XINFO CONSUMERS"
        | b"XINFO GROUPS"
        | b"XINFO STREAM"
        | b"OBJECT ENCODING"
        | b"OBJECT FREQ"
        | b"OBJECT IDLETIME"
        | b"OBJECT REFCOUNT"
        | b"JSON.DEBUG" => LegacyRouteBy::SecondArg,

        b"MIGRATE" => LegacyRouteBy::ThirdArg,

        b"LMPOP" | b"MSETEX" | b"SINTERCARD" | b"ZDIFF" | b"ZINTER" | b"ZINTERCARD" | b"ZMPOP"
        | b"ZUNION" => LegacyRouteBy::SecondArgAfterKeyCount,

        b"XREAD" | b"XREADGROUP" => LegacyRouteBy::StreamsIndex,

        // keyless commands with more arguments, whose arguments might be wrongly taken to be keys.
        // TODO - double check these, in order to find better ways to route some of them.
        b"ACL DRYRUN"
        | b"ACL GENPASS"
        | b"ACL GETUSER"
        | b"ACL HELP"
        | b"ACL LIST"
        | b"ACL LOG"
        | b"ACL USERS"
        | b"ACL WHOAMI"
        | b"CLIENT GETNAME"
        | b"CLIENT GETREDIR"
        | b"CLIENT ID"
        | b"CLIENT INFO"
        | b"CLIENT LIST"
        | b"CLIENT REPLY"
        | b"CLIENT TRACKINGINFO"
        | b"CLIENT UNBLOCK"
        | b"CLUSTER COUNT-FAILURE-REPORTS"
        | b"CLUSTER INFO"
        | b"CLUSTER KEYSLOT"
        | b"CLUSTER MEET"
        | b"CLUSTER MYSHARDID"
        | b"CLUSTER NODES"
        | b"CLUSTER REPLICAS"
        | b"CLUSTER RESET"
        | b"CLUSTER SET-CONFIG-EPOCH"
        | b"CLUSTER SHARDS"
        | b"CLUSTER SLOTS"
        | b"COMMAND COUNT"
        | b"COMMAND GETKEYS"
        | b"COMMAND LIST"
        | b"COMMAND"
        | b"CONFIG GET"
        | b"ECHO"
        | b"FT.CREATE"
        | b"FUNCTION LIST"
        | b"LASTSAVE"
        | b"LOLWUT"
        | b"MODULE LIST"
        | b"MODULE LOAD"
        | b"MODULE LOADEX"
        | b"MODULE UNLOAD"
        | b"READONLY"
        | b"READWRITE"
        | b"SCRIPT SHOW"
        | b"TFCALL"
        | b"TFCALLASYNC"
        | b"TFUNCTION DELETE"
        | b"TFUNCTION LIST"
        | b"TFUNCTION LOAD"
        | b"TIME" => LegacyRouteBy::Random,

        b"CLUSTER ADDSLOTS"
        | b"CLUSTER COUNTKEYSINSLOT"
        | b"CLUSTER DELSLOTS"
        | b"CLUSTER DELSLOTSRANGE"
        | b"CLUSTER GETKEYSINSLOT"
        | b"CLUSTER SETSLOT" => LegacyRouteBy::SecondArgSlot,

        _ => LegacyRouteBy::FirstKey,
    }
}

/// The name the routing tables were keyed by: the upper-cased command, followed by its
/// sub-command for the commands that have them.
fn legacy_command_name(r: &Cmd) -> Option<Vec<u8>> {
    let primary_command = r.arg_idx(0).map(|x| x.to_ascii_uppercase())?;
    let mut primary_command = match primary_command.as_slice() {
        b"XGROUP" | b"OBJECT" | b"SLOWLOG" | b"FUNCTION" | b"MODULE" | b"COMMAND" | b"PUBSUB"
        | b"CONFIG" | b"MEMORY" | b"XINFO" | b"CLIENT" | b"ACL" | b"SCRIPT" | b"CLUSTER"
        | b"LATENCY" => primary_command,
        _ => return Some(primary_command),
    };
    Some(match r.arg_idx(1) {
        Some(secondary_command) => {
            let previous_len = primary_command.len();
            primary_command.extend(b" ");
            primary_command.extend(secondary_command);
            let current_len = primary_command.len();
            primary_command[previous_len + 1..current_len].make_ascii_uppercase();
            primary_command
        }
        None => primary_command,
    })
}

fn legacy_for_routable(r: &Cmd) -> Option<RoutingInfo> {
    let name = legacy_command_name(r)?;
    let known = KnownCommand::ALL
        .iter()
        .copied()
        .find(|command| command.name().as_bytes() == name);
    let readonly = known.is_some_and(KnownCommand::is_readonly);
    let for_key = |key: &[u8]| RoutingInfo::for_key(readonly, key);
    let name = name.as_slice();
    // Only known commands have an aggregation policy or are split between shards.
    let command = known.unwrap_or(KnownCommand::Get);
    let policy = known.and_then(KnownCommand::response_policy);
    match legacy_base_routing(name) {
        LegacyRouteBy::AllNodes => Some(RoutingInfo::MultiNode((
            MultipleNodeRoutingInfo::AllNodes,
            policy,
        ))),
        LegacyRouteBy::AllPrimaries => Some(RoutingInfo::MultiNode((
            MultipleNodeRoutingInfo::AllMasters,
            policy,
        ))),
        LegacyRouteBy::MultiShard(pattern) => multi_shard(r, command, 1, pattern),
        LegacyRouteBy::Random => Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random)),
        LegacyRouteBy::ThirdArgAfterKeyCount | LegacyRouteBy::SecondArgAfterKeyCount => {
            let count_index = if matches!(
                legacy_base_routing(name),
                LegacyRouteBy::ThirdArgAfterKeyCount
            ) {
                2
            } else {
                1
            };
            let key_count = r
                .arg_idx(count_index)
                .and_then(|x| std::str::from_utf8(x).ok())
                .and_then(|x| x.parse::<u64>().ok())?;
            if key_count == 0 {
                Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random))
            } else {
                r.arg_idx(count_index + 1).map(for_key)
            }
        }
        LegacyRouteBy::SecondArg => r.arg_idx(2).map(for_key),
        LegacyRouteBy::ThirdArg => r.arg_idx(3).map(for_key),
        LegacyRouteBy::StreamsIndex => {
            let streams_position = r.position(b"STREAMS")?;
            r.arg_idx(streams_position + 1).map(for_key)
        }
        LegacyRouteBy::SecondArgSlot => r
            .arg_idx(2)
            .and_then(|arg| std::str::from_utf8(arg).ok())
            .and_then(|slot| slot.parse::<u16>().ok())
            .and_then(Slot::new)
            .map(|slot| {
                RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(Route::with_slot(
                    slot,
                    SlotAddr::Master,
                )))
            }),
        LegacyRouteBy::FirstKey => match r.arg_idx(1) {
            Some(key) => Some(for_key(key)),
            None => Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random)),
        },
        LegacyRouteBy::Undefined => None,
    }
}

const ARGUMENTS: &[&[&str]] = &[
    &[],
    &["a"],
    &["a", "b"],
    &["a", "b", "c"],
    &["a", "b", "c", "d"],
    &["a", "b", "c", "d", "e", "f"],
    &["0"],
    &["0", "a"],
    &["2", "a", "b"],
    &["100", "a"],
    &["x", "0"],
    &["x", "0", "a"],
    &["x", "2", "a", "b"],
    &["x", "100", "a"],
    &["1", "2", "3"],
    &["16383", "a"],
    &["STREAMS"],
    &["STREAMS", "a", "b"],
    &["COUNT", "1", "STREAMS", "a", "0"],
];

/// Orders the routes of a split command, which are collected in a map.
fn sorted(info: Option<RoutingInfo>) -> Option<RoutingInfo> {
    let mut info = info;
    if let Some(RoutingInfo::MultiNode((MultipleNodeRoutingInfo::MultiSlot((routes, _)), _))) =
        &mut info
    {
        routes.sort_by_key(|(_, indices)| indices.clone());
    }
    info
}

/// Commands without keys that used to be routed by the slot of their first argument, and are now
/// sent to a random node when they have arguments.
const KEYLESS_ROUTED_BY_THEIR_FIRST_ARGUMENT: &[(&str, &[KnownCommand])] = &[
    (
        "Sentinel commands: their arguments are names, addresses and settings",
        &[
            KnownCommand::SentinelGetMasterAddrByName,
            KnownCommand::SentinelMaster,
            KnownCommand::SentinelMasters,
            KnownCommand::SentinelMonitor,
            KnownCommand::SentinelRemove,
            KnownCommand::SentinelSentinels,
            KnownCommand::SentinelSet,
            KnownCommand::SentinelSlaves,
        ],
    ),
    (
        "Help commands, which take no arguments",
        &[
            KnownCommand::ClientHelp,
            KnownCommand::ClusterHelp,
            KnownCommand::CommandHelp,
            KnownCommand::ConfigHelp,
            KnownCommand::FunctionHelp,
            KnownCommand::LatencyHelp,
            KnownCommand::MemoryHelp,
            KnownCommand::ModuleHelp,
            KnownCommand::ObjectHelp,
            KnownCommand::PubsubHelp,
            KnownCommand::ScriptHelp,
            KnownCommand::SlowlogHelp,
            KnownCommand::XgroupHelp,
            KnownCommand::XinfoHelp,
        ],
    ),
    (
        "Transactions, connection and replication commands, whose arguments are not keys",
        &[
            KnownCommand::Asking,
            KnownCommand::ClientCaching,
            KnownCommand::ClientCapa,
            KnownCommand::ClientNoEvict,
            KnownCommand::ClientNoTouch,
            KnownCommand::ClientTracking,
            KnownCommand::Discard,
            KnownCommand::Exec,
            KnownCommand::Failover,
            KnownCommand::Hello,
            KnownCommand::Monitor,
            KnownCommand::Multi,
            KnownCommand::Psync,
            KnownCommand::Quit,
            KnownCommand::Replconf,
            KnownCommand::Role,
            KnownCommand::Sync,
        ],
    ),
    (
        "Administration commands, whose arguments are not keys",
        &[
            KnownCommand::AclCat,
            KnownCommand::AclLoad,
            KnownCommand::ClusterAddslotsrange,
            KnownCommand::ClusterBumpepoch,
            KnownCommand::ClusterFailover,
            KnownCommand::ClusterFlushslots,
            KnownCommand::ClusterForget,
            KnownCommand::ClusterLinks,
            KnownCommand::ClusterMyid,
            KnownCommand::ClusterReplicate,
            KnownCommand::ClusterSaveconfig,
            KnownCommand::ClusterSlaves,
            KnownCommand::CommandDocs,
            KnownCommand::CommandGetkeysandflags,
            KnownCommand::CommandInfo,
            KnownCommand::FunctionDump,
            KnownCommand::Hotkeys,
            KnownCommand::Pfselftest,
            KnownCommand::ScriptDebug,
            KnownCommand::Swapdb,
        ],
    ),
];

/// Commands whose routing changed for another reason.
const OTHER_CHANGES: &[(&str, &[KnownCommand])] = &[(
    "Fixed on purpose: the tables listed the sub-commands of `TFUNCTION` as keyless, but they \
     were never looked up by their full name, so `TFUNCTION LIST` used to be routed to the slot \
     of its sub-command, `LIST`",
    &[
        KnownCommand::TfunctionDelete,
        KnownCommand::TfunctionList,
        KnownCommand::TfunctionLoad,
    ],
)];

fn is_expected_to_differ(command: KnownCommand) -> bool {
    KEYLESS_ROUTED_BY_THEIR_FIRST_ARGUMENT
        .iter()
        .chain(OTHER_CHANGES)
        .any(|(_, commands)| commands.contains(&command))
}

#[test]
fn routing_matches_the_name_keyed_tables_except_for_the_listed_commands() {
    for &command in KnownCommand::ALL {
        let (primary, sub) = command.name_parts();
        let mut differs = false;
        for arguments in ARGUMENTS {
            let mut built = cmd(std::str::from_utf8(primary).unwrap());
            if let Some(sub) = sub {
                built.arg(sub);
            }
            for argument in *arguments {
                built.arg(*argument);
            }
            differs |=
                sorted(RoutingInfo::for_routable(&built)) != sorted(legacy_for_routable(&built));
        }
        assert_eq!(
            differs,
            is_expected_to_differ(command),
            "{}: the routing differs from the name-keyed tables only for the listed commands",
            command.name(),
        );
    }
}

#[test]
fn the_commands_that_are_split_across_slots_are_the_expected_ones() {
    let split: Vec<_> = KnownCommand::ALL
        .iter()
        .copied()
        .filter(|command| command.is_split_across_slots())
        .collect();
    assert_eq!(
        split,
        [
            KnownCommand::Del,
            KnownCommand::Exists,
            KnownCommand::JsonMget,
            KnownCommand::JsonMset,
            KnownCommand::Mget,
            KnownCommand::Mset,
            KnownCommand::Touch,
            KnownCommand::Unlink,
            KnownCommand::Watch,
        ]
    );
    for command in &split {
        assert!(
            command.key_positions().multi_slot_pattern().is_some(),
            "{}",
            command.name()
        );
    }
}

#[test]
fn tfcall_is_routed_to_a_random_node_and_is_not_read_only() {
    for command in [KnownCommand::Tfcall, KnownCommand::Tfcallasync] {
        assert!(!command.is_readonly(), "{}", command.name());
    }
    let routing = RoutingInfo::for_routable(cmd("TFCALL").arg("lib.fn").arg("1").arg("key"));
    assert_eq!(
        routing,
        Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random))
    );
}

#[test]
fn split_commands_without_keys_cant_be_routed() {
    for &command in KnownCommand::ALL {
        if command.is_split_across_slots() {
            let (primary, sub) = command.name_parts();
            let mut built = cmd(std::str::from_utf8(primary).unwrap());
            if let Some(sub) = sub {
                built.arg(sub);
            }
            assert_eq!(
                RoutingInfo::for_routable(&built),
                None,
                "{}",
                command.name()
            );
        }
    }
}

#[test]
fn key_layouts_follow_the_command_specifications() {
    use KeyPositions::{
        AfterKeyCount, AfterStreams, AllExceptLast, AllFrom, At, First, FirstAndAfterKeyCount,
        FirstTwo, Pairs, PairsAfterKeyCount, Triples,
    };
    for (command, expected) in [
        (KnownCommand::Get, First),
        (KnownCommand::Rename, FirstTwo),
        (KnownCommand::Lmove, FirstTwo),
        (KnownCommand::Smove, FirstTwo),
        (KnownCommand::Blpop, AllExceptLast),
        (KnownCommand::Sinterstore, AllFrom(1)),
        (KnownCommand::Pfmerge, AllFrom(1)),
        (KnownCommand::Pfcount, AllFrom(1)),
        (KnownCommand::Bitop, AllFrom(2)),
        (KnownCommand::Msetnx, Pairs),
        (KnownCommand::Zunionstore, FirstAndAfterKeyCount(2)),
        (KnownCommand::Zunion, AfterKeyCount(1)),
        (KnownCommand::Eval, AfterKeyCount(2)),
        (KnownCommand::Blmpop, AfterKeyCount(2)),
        (KnownCommand::Msetex, PairsAfterKeyCount(1)),
        (KnownCommand::Xread, AfterStreams),
        (KnownCommand::ObjectEncoding, At(2)),
        (KnownCommand::Migrate, At(3)),
        (KnownCommand::Publish, First),
        (KnownCommand::Subscribe, AllFrom(1)),
        (KnownCommand::Ssubscribe, AllFrom(1)),
        (KnownCommand::Spublish, First),
        (KnownCommand::Mget, AllFrom(1)),
        (KnownCommand::Mset, Pairs),
        (KnownCommand::JsonMget, AllExceptLast),
        (KnownCommand::JsonMset, Triples),
        (KnownCommand::Hello, KeyPositions::Keyless(Keyless::Random)),
        (KnownCommand::Tfcall, KeyPositions::Keyless(Keyless::Random)),
        (
            KnownCommand::Flushall,
            KeyPositions::Keyless(Keyless::AllPrimaries),
        ),
        (
            KnownCommand::PubsubShardnumsub,
            KeyPositions::Keyless(Keyless::AllNodes),
        ),
        (
            KnownCommand::Psubscribe,
            KeyPositions::Keyless(Keyless::PinnedByFirstArgument),
        ),
        (
            KnownCommand::FtSearch,
            KeyPositions::Keyless(Keyless::PinnedByFirstArgument),
        ),
    ] {
        assert_eq!(command.key_positions(), expected, "{}", command.name());
    }
}

#[test]
fn tfunction_sub_commands_are_routed_to_a_random_node() {
    for sub_command in ["DELETE", "LIST", "LOAD"] {
        assert_eq!(
            RoutingInfo::for_routable(cmd("TFUNCTION").arg(sub_command).arg("lib")),
            Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random)),
            "TFUNCTION {sub_command}"
        );
    }
}

#[test]
fn shard_channel_counts_are_asked_from_every_node_and_combined() {
    let routing = RoutingInfo::for_routable(
        cmd("PUBSUB")
            .arg("SHARDNUMSUB")
            .arg("foo")
            .arg("bar")
            .arg("baz"),
    );
    assert_eq!(
        routing,
        Some(RoutingInfo::MultiNode((
            MultipleNodeRoutingInfo::AllNodes,
            Some(ResponsePolicy::CombineMaps)
        )))
    );
}
