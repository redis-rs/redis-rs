//! Where the keys of each command are.

use super::KnownCommand;
#[cfg(feature = "cluster")]
use crate::cluster_handling::routing::MultiSlotArgPattern;

/// Where a command's keys are among its arguments.
///
/// Indices count from the first word of the command's name, so index `1` is the argument after
/// a one-word name. For a command with a sub-command (`MEMORY USAGE key`) the sub-command is
/// argument `1` and the first argument after the name is argument `2`.
///
/// Both cluster routing and client-side caching take their keys from here, so a command's key
/// layout is declared once, in [`KnownCommand::key_positions`]. The positions describe the keys
/// those two features rely on: for a command routed or cached by its first key only, that is
/// [`Self::First`], even if the command accepts further keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyPositions {
    /// The command has no keys, or none that can be found by position.
    None,
    /// A key at index 1: `GET key`.
    First,
    /// Keys at indices 1 and 2: `LCS key1 key2`.
    FirstTwo,
    /// A key at index 2: `BITOP AND dest key`, `MEMORY USAGE key`.
    Second,
    /// A key at index 3: `MIGRATE host port key`.
    Third,
    /// Every argument after the name is a key: `DEL key [key ...]`.
    All,
    /// Every argument after the name is a key except the last one: `JSON.MGET key [key ...] path`.
    AllExceptLast,
    /// Arguments after the name are key-value pairs: `MSET key value [key value ...]`.
    Pairs,
    /// Arguments after the name are key and two arguments triples:
    /// `JSON.MSET key path value [key path value ...]`.
    Triples,
    /// The argument at the given index is the number of keys, which follow it:
    /// `ZUNION 2 key1 key2`.
    AfterKeyCount(usize),
    /// The keys follow the `STREAMS` argument: `XREAD STREAMS key id`.
    AfterStreams,
}

impl KeyPositions {
    /// How the arguments of a command that is split between shards by key are laid out.
    #[cfg(feature = "cluster")]
    pub(crate) fn multi_slot_pattern(self) -> Option<MultiSlotArgPattern> {
        match self {
            Self::All => Some(MultiSlotArgPattern::KeysOnly),
            Self::Pairs => Some(MultiSlotArgPattern::KeyValuePairs),
            Self::AllExceptLast => Some(MultiSlotArgPattern::KeysAndLastArg),
            Self::Triples => Some(MultiSlotArgPattern::KeyWithTwoArgTriples),
            Self::None
            | Self::First
            | Self::FirstTwo
            | Self::Second
            | Self::Third
            | Self::AfterKeyCount(_)
            | Self::AfterStreams => None,
        }
    }
}

impl KnownCommand {
    /// Returns where this command's keys are.
    pub(crate) fn key_positions(self) -> KeyPositions {
        match self {
            Self::AclDeluser
            | Self::AclDryrun
            | Self::AclGenpass
            | Self::AclGetuser
            | Self::AclHelp
            | Self::AclList
            | Self::AclLog
            | Self::AclSave
            | Self::AclSetuser
            | Self::AclUsers
            | Self::AclWhoami
            | Self::Auth
            | Self::Bgrewriteaof
            | Self::Bgsave
            | Self::ClientGetname
            | Self::ClientGetredir
            | Self::ClientId
            | Self::ClientInfo
            | Self::ClientKill
            | Self::ClientList
            | Self::ClientPause
            | Self::ClientReply
            | Self::ClientSetinfo
            | Self::ClientSetname
            | Self::ClientTrackinginfo
            | Self::ClientUnblock
            | Self::ClientUnpause
            | Self::ClusterAddslots
            | Self::ClusterCountFailureReports
            | Self::ClusterCountkeysinslot
            | Self::ClusterDelslots
            | Self::ClusterDelslotsrange
            | Self::ClusterGetkeysinslot
            | Self::ClusterInfo
            | Self::ClusterKeyslot
            | Self::ClusterMeet
            | Self::ClusterMyshardid
            | Self::ClusterNodes
            | Self::ClusterReplicas
            | Self::ClusterReset
            | Self::ClusterSetConfigEpoch
            | Self::ClusterSetslot
            | Self::ClusterShards
            | Self::ClusterSlots
            | Self::Command
            | Self::CommandCount
            | Self::CommandGetkeys
            | Self::CommandList
            | Self::ConfigGet
            | Self::ConfigResetstat
            | Self::ConfigRewrite
            | Self::ConfigSet
            | Self::Dbsize
            | Self::Debug
            | Self::Echo
            | Self::Flushall
            | Self::Flushdb
            | Self::FtCreate
            | Self::FtAliasList
            | Self::FtList
            | Self::FunctionDelete
            | Self::FunctionFlush
            | Self::FunctionKill
            | Self::FunctionList
            | Self::FunctionLoad
            | Self::FunctionRestore
            | Self::FunctionStats
            | Self::Info
            | Self::Keys
            | Self::Lastsave
            | Self::LatencyDoctor
            | Self::LatencyGraph
            | Self::LatencyHistogram
            | Self::LatencyHistory
            | Self::LatencyLatest
            | Self::LatencyReset
            | Self::Lolwut
            | Self::MemoryDoctor
            | Self::MemoryMallocStats
            | Self::MemoryPurge
            | Self::MemoryStats
            | Self::ModuleList
            | Self::ModuleLoad
            | Self::ModuleLoadex
            | Self::ModuleUnload
            | Self::Ping
            | Self::PubsubChannels
            | Self::PubsubNumpat
            | Self::PubsubNumsub
            | Self::PubsubShardchannels
            | Self::PubsubShardnumsub
            | Self::Randomkey
            | Self::Readonly
            | Self::Readwrite
            | Self::Replicaof
            | Self::Reset
            | Self::Save
            | Self::Scan
            | Self::ScriptExists
            | Self::ScriptFlush
            | Self::ScriptKill
            | Self::ScriptLoad
            | Self::ScriptShow
            | Self::Select
            | Self::Shutdown
            | Self::Slaveof
            | Self::SlowlogGet
            | Self::SlowlogLen
            | Self::SlowlogReset
            | Self::Tfcall
            | Self::Tfcallasync
            | Self::TfunctionDelete
            | Self::TfunctionList
            | Self::TfunctionLoad
            | Self::Time
            | Self::Unwatch
            | Self::Wait
            | Self::Waitaof => KeyPositions::None,
            Self::Acl
            | Self::AclCat
            | Self::AclLoad
            | Self::Append
            | Self::Asking
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
            | Self::Blmove
            | Self::Blpop
            | Self::Brpop
            | Self::Brpoplpush
            | Self::Bzpopmax
            | Self::Bzpopmin
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
            | Self::Copy
            | Self::Decr
            | Self::Decrby
            | Self::Delex
            | Self::Digest
            | Self::Discard
            | Self::Dump
            | Self::Exec
            | Self::Expire
            | Self::Expireat
            | Self::Expiretime
            | Self::Failover
            | Self::FtAggregate
            | Self::FtExplain
            | Self::FtExplaincli
            | Self::FtInfo
            | Self::FtProfile
            | Self::FtSearch
            | Self::Function
            | Self::FunctionDump
            | Self::FunctionHelp
            | Self::Geoadd
            | Self::Geodist
            | Self::Geohash
            | Self::Geopos
            | Self::Georadius
            | Self::Georadiusbymember
            | Self::GeoradiusbymemberRo
            | Self::GeoradiusRo
            | Self::Geosearch
            | Self::Geosearchstore
            | Self::Get
            | Self::Getbit
            | Self::Getdel
            | Self::Getex
            | Self::Getrange
            | Self::Getset
            | Self::Hdel
            | Self::Hello
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
            | Self::Hotkeys
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
            | Self::Latency
            | Self::LatencyHelp
            | Self::Lindex
            | Self::Linsert
            | Self::Llen
            | Self::Lmove
            | Self::Lpop
            | Self::Lpos
            | Self::Lpush
            | Self::Lpushx
            | Self::Lrange
            | Self::Lrem
            | Self::Lset
            | Self::Ltrim
            | Self::Memory
            | Self::MemoryHelp
            | Self::Module
            | Self::ModuleHelp
            | Self::Monitor
            | Self::Move
            | Self::Msetnx
            | Self::Multi
            | Self::Object
            | Self::ObjectHelp
            | Self::Persist
            | Self::Pexpire
            | Self::Pexpireat
            | Self::Pexpiretime
            | Self::Pfadd
            | Self::Pfcount
            | Self::Pfmerge
            | Self::Pfselftest
            | Self::Psetex
            | Self::Psubscribe
            | Self::Psync
            | Self::Pttl
            | Self::Publish
            | Self::Pubsub
            | Self::PubsubHelp
            | Self::Punsubscribe
            | Self::Quit
            | Self::Rename
            | Self::Renamenx
            | Self::Replconf
            | Self::Restore
            | Self::RestoreAsking
            | Self::Role
            | Self::Rpop
            | Self::Rpoplpush
            | Self::Rpush
            | Self::Rpushx
            | Self::Sadd
            | Self::Scard
            | Self::Script
            | Self::ScriptDebug
            | Self::ScriptHelp
            | Self::Sdiffstore
            | Self::Sentinel
            | Self::SentinelGetMasterAddrByName
            | Self::SentinelMaster
            | Self::SentinelMasters
            | Self::SentinelMonitor
            | Self::SentinelRemove
            | Self::SentinelSentinels
            | Self::SentinelSet
            | Self::SentinelSlaves
            | Self::Set
            | Self::Setbit
            | Self::Setex
            | Self::Setnx
            | Self::Setrange
            | Self::Sinterstore
            | Self::Sismember
            | Self::Slowlog
            | Self::SlowlogHelp
            | Self::Smembers
            | Self::Smismember
            | Self::Smove
            | Self::Sort
            | Self::SortRo
            | Self::Spop
            | Self::Spublish
            | Self::Srandmember
            | Self::Srem
            | Self::Sscan
            | Self::Ssubscribe
            | Self::Strlen
            | Self::Subscribe
            | Self::Substr
            | Self::Sunionstore
            | Self::Sunsubscribe
            | Self::Swapdb
            | Self::Sync
            | Self::Ttl
            | Self::Type
            | Self::Unsubscribe
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
            | Self::Xgroup
            | Self::XgroupHelp
            | Self::Xinfo
            | Self::XinfoHelp
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
            | Self::Zdiffstore
            | Self::Zincrby
            | Self::Zinterstore
            | Self::Zlexcount
            | Self::Zmscore
            | Self::Zpopmax
            | Self::Zpopmin
            | Self::Zrandmember
            | Self::Zrange
            | Self::Zrangebylex
            | Self::Zrangebyscore
            | Self::Zrangestore
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
            | Self::Zscore
            | Self::Zunionstore => KeyPositions::First,
            Self::Lcs => KeyPositions::FirstTwo,
            Self::Bitop
            | Self::JsonDebug
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
            | Self::XinfoStream => KeyPositions::Second,
            Self::Migrate => KeyPositions::Third,
            Self::Del
            | Self::Exists
            | Self::Mget
            | Self::Sdiff
            | Self::Sinter
            | Self::Sunion
            | Self::Touch
            | Self::Unlink
            | Self::Watch => KeyPositions::All,
            Self::JsonMget => KeyPositions::AllExceptLast,
            Self::Mset => KeyPositions::Pairs,
            Self::JsonMset => KeyPositions::Triples,
            Self::Lmpop
            | Self::Msetex
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
