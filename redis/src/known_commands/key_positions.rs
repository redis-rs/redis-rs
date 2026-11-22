//! Where the keys of each command are.

use super::KnownCommand;
#[cfg(feature = "cluster")]
use crate::cluster_handling::routing::MultiSlotArgPattern;

/// Where a command's keys are among its arguments, or how a command without keys is routed in a
/// cluster.
///
/// Indices count from the first word of the command's name, as in the key specifications of
/// `COMMAND INFO`: index `1` is the argument after a one-word name, and for a command with a
/// sub-command (`MEMORY USAGE key`) the sub-command is argument `1` and the first argument after
/// the name is argument `2`.
///
/// This is the one place a command's key layout is declared. Cluster routing derives where a
/// command goes from it, and client-side caching takes its keys from it. Whether a command with
/// keys is split between the slots of its keys is a routing property, see
/// [`KnownCommand::is_split_across_slots`].
///
/// The layouts follow the commands' key specifications. Where a layout can't be described by
/// position, the variant that covers the keys that can is used, and the command is listed on the
/// variant.
///
/// Pub/sub channels are described as keys on purpose, so that they are routed by their slot
/// like keys are, although the server only has key specifications for the sharded commands
/// (`SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE`). The patterns of `PSUBSCRIBE` match channels of all
/// slots, so they aren't.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyPositions {
    /// The command has no keys, and is routed as described.
    Keyless(Keyless),
    /// A key at index 1: `GET key`.
    ///
    /// Also used for the commands whose first key can be followed by keys that are introduced by
    /// a keyword, or that are found only by parsing the options: `SORT key [STORE destination]`,
    /// `GEORADIUS key ... [STORE destination]`. Only the first key is described for them.
    First,
    /// Keys at indices 1 and 2: `RENAME key newkey`.
    FirstTwo,
    /// A key at the given index, which is at least 2: `MEMORY USAGE key`,
    /// `MIGRATE host port key ...`.
    ///
    /// `MIGRATE host port "" db timeout KEYS key [key ...]` has its keys after the `KEYS`
    /// keyword, which isn't described.
    At(usize),
    /// Every argument from the given index on is a key: `DEL key [key ...]`,
    /// `BITOP AND destination key [key ...]`.
    AllFrom(usize),
    /// Every argument after the name is a key except for the last one:
    /// `BLPOP key [key ...] timeout`, `JSON.MGET key [key ...] path`.
    AllExceptLast,
    /// The arguments after the name are key-value pairs: `MSET key value [key value ...]`.
    Pairs,
    /// The arguments after the name are keys, each followed by two arguments:
    /// `JSON.MSET key path value [key path value ...]`.
    Triples,
    /// A key at index 1, followed by the number of keys at the given index, and then by these
    /// keys: `ZUNIONSTORE destination numkeys key [key ...]`.
    FirstAndAfterKeyCount(usize),
    /// The argument at the given index is the number of keys, which follow it:
    /// `ZUNION numkeys key [key ...]`.
    AfterKeyCount(usize),
    /// Like [`Self::AfterKeyCount`], but every key is followed by a value:
    /// `MSETEX numkeys key value [key value ...]`.
    PairsAfterKeyCount(usize),
    /// The keys are the first half of the arguments after the `STREAMS` argument, which are
    /// followed by their IDs: `XREAD STREAMS key [key ...] id [id ...]`.
    AfterStreams,
}

/// How a command without keys is routed in a cluster.
// Without cluster support, only the key positions of the commands are used.
#[cfg_attr(not(feature = "cluster"), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Keyless {
    /// To every node.
    ///
    /// Also for `PUBSUB NUMSUB` and `PUBSUB SHARDNUMSUB`, although they take channels, which are
    /// keys elsewhere: each node counts its own subscribers, and the responses are merged.
    AllNodes,
    /// To every primary.
    AllPrimaries,
    /// To a random node.
    ///
    /// Also for `TFCALL` and `TFCALLASYNC`, which have keys after their number of keys. They are
    /// described as keyless because the keys are only used for routing, which sends these commands
    /// to a random node, and they are neither read-only nor cacheable.
    Random,
    /// To the primary of the slot given in the command's second argument.
    SlotArg,
    /// To the node of the slot of the command's first argument, which isn't a key, so that
    /// requests with the same first argument reach the same node: the pattern subscriptions of a
    /// connection (`PSUBSCRIBE`, `PUNSUBSCRIBE`) have to land on one deterministic node, and the
    /// search commands (`FT.SEARCH`, ...) are sent to the node of their index name.
    PinnedByFirstArgument,
    /// To the node of the slot of the command's first argument, if it has one, which isn't a
    /// key. This is how the command was routed before its key layout was described.
    // TODO: remove, the commands should go to a random node.
    HashFirstArgument,
    /// The command can't be routed automatically.
    Undefined,
}

#[cfg(feature = "cluster")]
impl KeyPositions {
    /// How the arguments of a command that is split between slots are laid out, if it can be.
    pub(crate) fn multi_slot_pattern(self) -> Option<MultiSlotArgPattern> {
        match self {
            Self::AllFrom(1) => Some(MultiSlotArgPattern::KeysOnly),
            Self::Pairs => Some(MultiSlotArgPattern::KeyValuePairs),
            Self::AllExceptLast => Some(MultiSlotArgPattern::KeysAndLastArg),
            Self::Triples => Some(MultiSlotArgPattern::KeyWithTwoArgTriples),
            Self::Keyless(_)
            | Self::First
            | Self::FirstTwo
            | Self::At(_)
            | Self::AllFrom(_)
            | Self::FirstAndAfterKeyCount(_)
            | Self::AfterKeyCount(_)
            | Self::PairsAfterKeyCount(_)
            | Self::AfterStreams => None,
        }
    }
}

impl KnownCommand {
    /// Returns where this command's keys are.
    pub(crate) fn key_positions(self) -> KeyPositions {
        match self {
            Self::AclDeluser
            | Self::AclSave
            | Self::AclSetuser
            | Self::Auth
            | Self::ClientKill
            | Self::ClientSetinfo
            | Self::ClientSetname
            | Self::ConfigResetstat
            | Self::ConfigRewrite
            | Self::ConfigSet
            | Self::FunctionKill
            | Self::FunctionStats
            | Self::PubsubChannels
            | Self::PubsubNumpat
            | Self::PubsubNumsub
            | Self::PubsubShardchannels
            | Self::PubsubShardnumsub
            | Self::Reset
            | Self::ScriptFlush
            | Self::ScriptKill
            | Self::ScriptLoad
            | Self::Select
            | Self::SlowlogGet
            | Self::SlowlogLen
            | Self::SlowlogReset => KeyPositions::Keyless(Keyless::AllNodes),
            Self::Bgrewriteaof
            | Self::Bgsave
            | Self::ClientPause
            | Self::ClientUnpause
            | Self::Dbsize
            | Self::Debug
            | Self::Flushall
            | Self::Flushdb
            | Self::FtAliasList
            | Self::FtList
            | Self::FunctionDelete
            | Self::FunctionFlush
            | Self::FunctionLoad
            | Self::FunctionRestore
            | Self::Info
            | Self::Keys
            | Self::LatencyDoctor
            | Self::LatencyGraph
            | Self::LatencyHistogram
            | Self::LatencyHistory
            | Self::LatencyLatest
            | Self::LatencyReset
            | Self::MemoryDoctor
            | Self::MemoryMallocStats
            | Self::MemoryPurge
            | Self::MemoryStats
            | Self::Ping
            | Self::Randomkey
            | Self::Save
            | Self::ScriptExists
            | Self::Unwatch
            | Self::Wait
            | Self::Waitaof => KeyPositions::Keyless(Keyless::AllPrimaries),
            Self::Acl
            | Self::AclCat
            | Self::AclLoad
            | Self::Asking
            | Self::Client
            | Self::ClientCaching
            | Self::ClientCapa
            | Self::ClientHelp
            | Self::ClientNoEvict
            | Self::ClientNoTouch
            | Self::ClientTracking
            | Self::Cluster
            | Self::ClusterAddslotsrange
            | Self::ClusterBumpepoch
            | Self::ClusterFailover
            | Self::ClusterFlushslots
            | Self::ClusterForget
            | Self::ClusterHelp
            | Self::ClusterLinks
            | Self::ClusterMyid
            | Self::ClusterReplicate
            | Self::ClusterSaveconfig
            | Self::ClusterSlaves
            | Self::CommandDocs
            | Self::CommandGetkeysandflags
            | Self::CommandHelp
            | Self::CommandInfo
            | Self::Config
            | Self::ConfigHelp
            | Self::Discard
            | Self::Exec
            | Self::Failover
            | Self::Function
            | Self::FunctionDump
            | Self::FunctionHelp
            | Self::Hello
            | Self::Hotkeys
            | Self::Latency
            | Self::LatencyHelp
            | Self::Memory
            | Self::MemoryHelp
            | Self::Module
            | Self::ModuleHelp
            | Self::Monitor
            | Self::Multi
            | Self::Object
            | Self::ObjectHelp
            | Self::Pfselftest
            | Self::Psync
            | Self::Pubsub
            | Self::PubsubHelp
            | Self::Quit
            | Self::Replconf
            | Self::Role
            | Self::Script
            | Self::ScriptDebug
            | Self::ScriptHelp
            | Self::Sentinel
            | Self::SentinelGetMasterAddrByName
            | Self::SentinelMaster
            | Self::SentinelMasters
            | Self::SentinelMonitor
            | Self::SentinelRemove
            | Self::SentinelSentinels
            | Self::SentinelSet
            | Self::SentinelSlaves
            | Self::Slowlog
            | Self::SlowlogHelp
            | Self::Swapdb
            | Self::Sync
            | Self::Xgroup
            | Self::XgroupHelp
            | Self::Xinfo
            | Self::XinfoHelp => KeyPositions::Keyless(Keyless::HashFirstArgument),
            Self::FtAggregate
            | Self::FtExplain
            | Self::FtExplaincli
            | Self::FtInfo
            | Self::FtProfile
            | Self::FtSearch
            | Self::Psubscribe
            | Self::Punsubscribe => KeyPositions::Keyless(Keyless::PinnedByFirstArgument),
            Self::AclDryrun
            | Self::AclGenpass
            | Self::AclGetuser
            | Self::AclHelp
            | Self::AclList
            | Self::AclLog
            | Self::AclUsers
            | Self::AclWhoami
            | Self::ClientGetname
            | Self::ClientGetredir
            | Self::ClientId
            | Self::ClientInfo
            | Self::ClientList
            | Self::ClientReply
            | Self::ClientTrackinginfo
            | Self::ClientUnblock
            | Self::ClusterCountFailureReports
            | Self::ClusterInfo
            | Self::ClusterKeyslot
            | Self::ClusterMeet
            | Self::ClusterMyshardid
            | Self::ClusterNodes
            | Self::ClusterReplicas
            | Self::ClusterReset
            | Self::ClusterSetConfigEpoch
            | Self::ClusterShards
            | Self::ClusterSlots
            | Self::Command
            | Self::CommandCount
            | Self::CommandGetkeys
            | Self::CommandList
            | Self::ConfigGet
            | Self::Echo
            | Self::FtCreate
            | Self::FunctionList
            | Self::Lastsave
            | Self::Lolwut
            | Self::ModuleList
            | Self::ModuleLoad
            | Self::ModuleLoadex
            | Self::ModuleUnload
            | Self::Readonly
            | Self::Readwrite
            | Self::ScriptShow
            | Self::Tfcall
            | Self::Tfcallasync
            | Self::TfunctionDelete
            | Self::TfunctionList
            | Self::TfunctionLoad
            | Self::Time => KeyPositions::Keyless(Keyless::Random),
            Self::ClusterAddslots
            | Self::ClusterCountkeysinslot
            | Self::ClusterDelslots
            | Self::ClusterDelslotsrange
            | Self::ClusterGetkeysinslot
            | Self::ClusterSetslot => KeyPositions::Keyless(Keyless::SlotArg),
            Self::Replicaof | Self::Scan | Self::Shutdown | Self::Slaveof => {
                KeyPositions::Keyless(Keyless::Undefined)
            }
            Self::Append
            | Self::BfAdd
            | Self::BfCard
            | Self::BfExists
            | Self::BfInfo
            | Self::BfInsert
            | Self::BfLoadchunk
            | Self::BfMadd
            | Self::BfMexists
            | Self::BfReserve
            | Self::BfScandump
            | Self::Bitcount
            | Self::Bitfield
            | Self::BitfieldRo
            | Self::Bitpos
            | Self::Decr
            | Self::Decrby
            | Self::Delex
            | Self::Digest
            | Self::Dump
            | Self::Expire
            | Self::Expireat
            | Self::Expiretime
            | Self::Geoadd
            | Self::Geodist
            | Self::Geohash
            | Self::Geopos
            | Self::Georadius
            | Self::Georadiusbymember
            | Self::GeoradiusbymemberRo
            | Self::GeoradiusRo
            | Self::Geosearch
            | Self::Get
            | Self::Getbit
            | Self::Getdel
            | Self::Getex
            | Self::Getrange
            | Self::Getset
            | Self::Hdel
            | Self::Hexists
            | Self::Hexpire
            | Self::Hexpireat
            | Self::Hexpiretime
            | Self::Hget
            | Self::Hgetall
            | Self::Hgetdel
            | Self::Hgetex
            | Self::Hincrby
            | Self::Hincrbyfloat
            | Self::Hkeys
            | Self::Hlen
            | Self::Hmget
            | Self::Hmset
            | Self::Hpersist
            | Self::Hpexpire
            | Self::Hpexpireat
            | Self::Hpexpiretime
            | Self::Hpttl
            | Self::Hrandfield
            | Self::Hscan
            | Self::Hset
            | Self::Hsetex
            | Self::Hsetnx
            | Self::Hstrlen
            | Self::Httl
            | Self::Hvals
            | Self::Incr
            | Self::Incrby
            | Self::Incrbyfloat
            | Self::Increx
            | Self::JsonArrappend
            | Self::JsonArrindex
            | Self::JsonArrinsert
            | Self::JsonArrlen
            | Self::JsonArrpop
            | Self::JsonArrtrim
            | Self::JsonClear
            | Self::JsonDel
            | Self::JsonGet
            | Self::JsonNumincrby
            | Self::JsonObjkeys
            | Self::JsonObjlen
            | Self::JsonResp
            | Self::JsonSet
            | Self::JsonStrappend
            | Self::JsonStrlen
            | Self::JsonToggle
            | Self::JsonType
            | Self::Lindex
            | Self::Linsert
            | Self::Llen
            | Self::Lpop
            | Self::Lpos
            | Self::Lpush
            | Self::Lpushx
            | Self::Lrange
            | Self::Lrem
            | Self::Lset
            | Self::Ltrim
            | Self::Move
            | Self::Persist
            | Self::Pexpire
            | Self::Pexpireat
            | Self::Pexpiretime
            | Self::Pfadd
            | Self::Psetex
            | Self::Pttl
            | Self::Publish
            | Self::Restore
            | Self::RestoreAsking
            | Self::Rpop
            | Self::Rpush
            | Self::Rpushx
            | Self::Sadd
            | Self::Scard
            | Self::Set
            | Self::Setbit
            | Self::Setex
            | Self::Setnx
            | Self::Setrange
            | Self::Sismember
            | Self::Smembers
            | Self::Smismember
            | Self::Sort
            | Self::SortRo
            | Self::Spop
            | Self::Spublish
            | Self::Srandmember
            | Self::Srem
            | Self::Sscan
            | Self::Strlen
            | Self::Substr
            | Self::Ttl
            | Self::Type
            | Self::Vadd
            | Self::Vcard
            | Self::Vdim
            | Self::Vemb
            | Self::Vgetattr
            | Self::Vinfo
            | Self::Vlinks
            | Self::Vrandmember
            | Self::Vrem
            | Self::Vsetattr
            | Self::Vsim
            | Self::Xack
            | Self::Xackdel
            | Self::Xadd
            | Self::Xautoclaim
            | Self::Xcfgset
            | Self::Xclaim
            | Self::Xdel
            | Self::Xdelex
            | Self::Xlen
            | Self::Xnack
            | Self::Xpending
            | Self::Xrange
            | Self::Xrevrange
            | Self::Xsetid
            | Self::Xtrim
            | Self::Zadd
            | Self::Zcard
            | Self::Zcount
            | Self::Zincrby
            | Self::Zlexcount
            | Self::Zmscore
            | Self::Zpopmax
            | Self::Zpopmin
            | Self::Zrandmember
            | Self::Zrange
            | Self::Zrangebylex
            | Self::Zrangebyscore
            | Self::Zrank
            | Self::Zrem
            | Self::Zremrangebylex
            | Self::Zremrangebyrank
            | Self::Zremrangebyscore
            | Self::Zrevrange
            | Self::Zrevrangebylex
            | Self::Zrevrangebyscore
            | Self::Zrevrank
            | Self::Zscan
            | Self::Zscore => KeyPositions::First,
            Self::Zdiffstore | Self::Zinterstore | Self::Zunionstore => {
                KeyPositions::FirstAndAfterKeyCount(2)
            }
            Self::Blmove
            | Self::Brpoplpush
            | Self::Copy
            | Self::Geosearchstore
            | Self::Lcs
            | Self::Lmove
            | Self::Rename
            | Self::Renamenx
            | Self::Rpoplpush
            | Self::Smove
            | Self::Zrangestore => KeyPositions::FirstTwo,
            Self::JsonDebug
            | Self::MemoryUsage
            | Self::ObjectEncoding
            | Self::ObjectFreq
            | Self::ObjectIdletime
            | Self::ObjectRefcount
            | Self::Pfdebug
            | Self::XgroupCreate
            | Self::XgroupCreateconsumer
            | Self::XgroupDelconsumer
            | Self::XgroupDestroy
            | Self::XgroupSetid
            | Self::XinfoConsumers
            | Self::XinfoGroups
            | Self::XinfoStream => KeyPositions::At(2),
            Self::Migrate => KeyPositions::At(3),
            Self::Del
            | Self::Exists
            | Self::Mget
            | Self::Pfcount
            | Self::Pfmerge
            | Self::Sdiff
            | Self::Sdiffstore
            | Self::Sinter
            | Self::Sinterstore
            | Self::Ssubscribe
            | Self::Subscribe
            | Self::Sunion
            | Self::Sunionstore
            | Self::Sunsubscribe
            | Self::Touch
            | Self::Unlink
            | Self::Unsubscribe
            | Self::Watch => KeyPositions::AllFrom(1),
            Self::Bitop => KeyPositions::AllFrom(2),
            Self::Blpop | Self::Brpop | Self::Bzpopmax | Self::Bzpopmin | Self::JsonMget => {
                KeyPositions::AllExceptLast
            }
            Self::Mset | Self::Msetnx => KeyPositions::Pairs,
            Self::Msetex => KeyPositions::PairsAfterKeyCount(1),
            Self::JsonMset => KeyPositions::Triples,
            Self::Lmpop
            | Self::Sintercard
            | Self::Zdiff
            | Self::Zinter
            | Self::Zintercard
            | Self::Zmpop
            | Self::Zunion => KeyPositions::AfterKeyCount(1),
            Self::Blmpop
            | Self::Bzmpop
            | Self::Eval
            | Self::Evalsha
            | Self::EvalshaRo
            | Self::EvalRo
            | Self::Fcall
            | Self::FcallRo => KeyPositions::AfterKeyCount(2),
            Self::Xread | Self::Xreadgroup => KeyPositions::AfterStreams,
        }
    }
}
