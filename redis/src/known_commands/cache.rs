//! Read-only access and client-side caching properties of each command.

use super::KnownCommand;

/// How the response of a cacheable command is cached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CacheKind {
    /// The response is cached as a whole, under the command's key.
    Whole,
    /// The response holds one value per key, and each value is cached on its own, as the
    /// response of `single`, the single-key command that is equivalent to the command applied
    /// to that key: `MGET a b` is cached as `GET a` and `GET b`.
    PerKey { single: KnownCommand },
}

/// Properties used to decide cluster routing and client-side caching.
#[cfg(any(feature = "cluster", feature = "cache-aio"))]
// How a command is cached only matters to the cache.
#[cfg_attr(not(feature = "cache-aio"), allow(dead_code))]
enum Properties {
    /// The command only reads data, and its response can be cached.
    ReadOnlyCacheable(CacheKind),
    /// The command only reads data.
    ReadOnly,
    /// The command neither only reads data nor is cacheable.
    Neither,
}

impl KnownCommand {
    /// Returns the access and caching properties that determine how this command is routed.
    #[cfg(any(feature = "cluster", feature = "cache-aio"))]
    fn command_properties(self) -> Properties {
        match self {
            Self::Bitcount
            | Self::BitfieldRo
            | Self::Bitpos
            | Self::Dump
            | Self::Exists
            | Self::Geodist
            | Self::Geohash
            | Self::Geopos
            | Self::Get
            | Self::Getbit
            | Self::Getrange
            | Self::Hexists
            | Self::Hget
            | Self::Hgetall
            | Self::Hkeys
            | Self::Hlen
            | Self::Hmget
            | Self::Hstrlen
            | Self::Hvals
            | Self::JsonArrindex
            | Self::JsonArrlen
            | Self::JsonGet
            | Self::JsonObjkeys
            | Self::JsonObjlen
            | Self::JsonResp
            | Self::JsonStrlen
            | Self::JsonType
            | Self::Lcs
            | Self::Lindex
            | Self::Llen
            | Self::Lpos
            | Self::Lrange
            | Self::Scard
            | Self::Sdiff
            | Self::Sinter
            | Self::Sintercard
            | Self::Sismember
            | Self::Smembers
            | Self::Smismember
            | Self::Strlen
            | Self::Substr
            | Self::Sunion
            | Self::Type
            | Self::Zcard
            | Self::Zcount
            | Self::Zdiff
            | Self::Zinter
            | Self::Zintercard
            | Self::Zlexcount
            | Self::Zmscore
            | Self::Zrange
            | Self::Zrangebylex
            | Self::Zrangebyscore
            | Self::Zrank
            | Self::Zrevrange
            | Self::Zrevrangebylex
            | Self::Zrevrangebyscore
            | Self::Zrevrank
            | Self::Zscore
            | Self::Zunion => Properties::ReadOnlyCacheable(CacheKind::Whole),
            Self::JsonMget => Properties::ReadOnlyCacheable(CacheKind::PerKey {
                single: Self::JsonGet,
            }),
            Self::Mget => Properties::ReadOnlyCacheable(CacheKind::PerKey { single: Self::Get }),
            Self::AclCat
            | Self::AclDeluser
            | Self::AclDryrun
            | Self::AclGenpass
            | Self::AclGetuser
            | Self::AclHelp
            | Self::AclList
            | Self::AclLoad
            | Self::AclLog
            | Self::AclSave
            | Self::AclSetuser
            | Self::AclUsers
            | Self::AclWhoami
            | Self::Auth
            | Self::BfCard
            | Self::BfExists
            | Self::BfInfo
            | Self::BfMexists
            | Self::BfScandump
            | Self::Bgrewriteaof
            | Self::Bgsave
            | Self::ClientCaching
            | Self::ClientCapa
            | Self::ClientGetname
            | Self::ClientGetredir
            | Self::ClientHelp
            | Self::ClientId
            | Self::ClientInfo
            | Self::ClientKill
            | Self::ClientList
            | Self::ClientNoEvict
            | Self::ClientNoTouch
            | Self::ClientPause
            | Self::ClientReply
            | Self::ClientSetinfo
            | Self::ClientSetname
            | Self::ClientTracking
            | Self::ClientTrackinginfo
            | Self::ClientUnblock
            | Self::ClientUnpause
            | Self::ClusterCountFailureReports
            | Self::ClusterCountkeysinslot
            | Self::ClusterFailover
            | Self::ClusterGetkeysinslot
            | Self::ClusterHelp
            | Self::ClusterInfo
            | Self::ClusterKeyslot
            | Self::ClusterLinks
            | Self::ClusterMyid
            | Self::ClusterMyshardid
            | Self::ClusterNodes
            | Self::ClusterReplicate
            | Self::ClusterSaveconfig
            | Self::ClusterShards
            | Self::ClusterSlots
            | Self::CommandCount
            | Self::CommandDocs
            | Self::CommandGetkeys
            | Self::CommandGetkeysandflags
            | Self::CommandHelp
            | Self::CommandInfo
            | Self::CommandList
            | Self::ConfigGet
            | Self::ConfigHelp
            | Self::ConfigResetstat
            | Self::ConfigRewrite
            | Self::ConfigSet
            | Self::Dbsize
            | Self::Echo
            | Self::EvalshaRo
            | Self::EvalRo
            | Self::Expiretime
            | Self::FcallRo
            | Self::FtAggregate
            | Self::FtExplain
            | Self::FtExplaincli
            | Self::FtInfo
            | Self::FtProfile
            | Self::FtSearch
            | Self::FtAliasList
            | Self::FtList
            | Self::FunctionDump
            | Self::FunctionHelp
            | Self::FunctionKill
            | Self::FunctionList
            | Self::FunctionStats
            | Self::GeoradiusbymemberRo
            | Self::GeoradiusRo
            | Self::Geosearch
            | Self::Hello
            | Self::Hrandfield
            | Self::Hscan
            | Self::Info
            | Self::JsonDebug
            | Self::Keys
            | Self::Lastsave
            | Self::LatencyDoctor
            | Self::LatencyGraph
            | Self::LatencyHelp
            | Self::LatencyHistogram
            | Self::LatencyHistory
            | Self::LatencyLatest
            | Self::LatencyReset
            | Self::Lolwut
            | Self::MemoryDoctor
            | Self::MemoryHelp
            | Self::MemoryMallocStats
            | Self::MemoryPurge
            | Self::MemoryStats
            | Self::MemoryUsage
            | Self::ModuleHelp
            | Self::ModuleList
            | Self::ModuleLoad
            | Self::ModuleLoadex
            | Self::ModuleUnload
            | Self::ObjectEncoding
            | Self::ObjectFreq
            | Self::ObjectHelp
            | Self::ObjectIdletime
            | Self::ObjectRefcount
            | Self::Pexpiretime
            | Self::Pfcount
            | Self::Ping
            | Self::Pttl
            | Self::Publish
            | Self::PubsubChannels
            | Self::PubsubHelp
            | Self::PubsubNumpat
            | Self::PubsubNumsub
            | Self::PubsubShardchannels
            | Self::PubsubShardnumsub
            | Self::Randomkey
            | Self::Replicaof
            | Self::Reset
            | Self::Role
            | Self::Save
            | Self::Scan
            | Self::ScriptDebug
            | Self::ScriptExists
            | Self::ScriptFlush
            | Self::ScriptKill
            | Self::ScriptLoad
            | Self::ScriptShow
            | Self::Select
            | Self::Shutdown
            | Self::SlowlogGet
            | Self::SlowlogHelp
            | Self::SlowlogLen
            | Self::SlowlogReset
            | Self::SortRo
            | Self::Spublish
            | Self::Srandmember
            | Self::Sscan
            | Self::Ssubscribe
            | Self::Subscribe
            | Self::Sunsubscribe
            | Self::Time
            | Self::Touch
            | Self::Ttl
            | Self::Unsubscribe
            | Self::XinfoConsumers
            | Self::XinfoGroups
            | Self::XinfoHelp
            | Self::XinfoStream
            | Self::Xlen
            | Self::Xpending
            | Self::Xrange
            | Self::Xread
            | Self::Xrevrange
            | Self::Zrandmember
            | Self::Zscan => Properties::ReadOnly,
            Self::Acl
            | Self::Append
            | Self::Asking
            | Self::BfAdd
            | Self::BfInsert
            | Self::BfLoadchunk
            | Self::BfMadd
            | Self::BfReserve
            | Self::Bitfield
            | Self::Bitop
            | Self::Blmove
            | Self::Blmpop
            | Self::Blpop
            | Self::Brpop
            | Self::Brpoplpush
            | Self::Bzmpop
            | Self::Bzpopmax
            | Self::Bzpopmin
            | Self::Client
            | Self::Cluster
            | Self::ClusterAddslots
            | Self::ClusterAddslotsrange
            | Self::ClusterBumpepoch
            | Self::ClusterDelslots
            | Self::ClusterDelslotsrange
            | Self::ClusterFlushslots
            | Self::ClusterForget
            | Self::ClusterMeet
            | Self::ClusterReplicas
            | Self::ClusterReset
            | Self::ClusterSetConfigEpoch
            | Self::ClusterSetslot
            | Self::ClusterSlaves
            | Self::Command
            | Self::Config
            | Self::Copy
            | Self::Debug
            | Self::Decr
            | Self::Decrby
            | Self::Del
            | Self::Delex
            | Self::Digest
            | Self::Discard
            | Self::Eval
            | Self::Evalsha
            | Self::Exec
            | Self::Expire
            | Self::Expireat
            | Self::Failover
            | Self::Fcall
            | Self::Flushall
            | Self::Flushdb
            | Self::FtCreate
            | Self::Function
            | Self::FunctionDelete
            | Self::FunctionFlush
            | Self::FunctionLoad
            | Self::FunctionRestore
            | Self::Geoadd
            | Self::Georadius
            | Self::Georadiusbymember
            | Self::Geosearchstore
            | Self::Getdel
            | Self::Getex
            | Self::Getset
            | Self::Hdel
            | Self::Hexpire
            | Self::Hexpireat
            | Self::Hexpiretime
            | Self::Hgetdel
            | Self::Hgetex
            | Self::Hincrby
            | Self::Hincrbyfloat
            | Self::Hmset
            | Self::Hotkeys
            | Self::Hpersist
            | Self::Hpexpire
            | Self::Hpexpireat
            | Self::Hpexpiretime
            | Self::Hpttl
            | Self::Hset
            | Self::Hsetex
            | Self::Hsetnx
            | Self::Httl
            | Self::Incr
            | Self::Incrby
            | Self::Incrbyfloat
            | Self::Increx
            | Self::JsonArrappend
            | Self::JsonArrinsert
            | Self::JsonArrpop
            | Self::JsonArrtrim
            | Self::JsonClear
            | Self::JsonDel
            | Self::JsonMset
            | Self::JsonNumincrby
            | Self::JsonSet
            | Self::JsonStrappend
            | Self::JsonToggle
            | Self::Latency
            | Self::Linsert
            | Self::Lmove
            | Self::Lmpop
            | Self::Lpop
            | Self::Lpush
            | Self::Lpushx
            | Self::Lrem
            | Self::Lset
            | Self::Ltrim
            | Self::Memory
            | Self::Migrate
            | Self::Module
            | Self::Monitor
            | Self::Move
            | Self::Mset
            | Self::Msetex
            | Self::Msetnx
            | Self::Multi
            | Self::Object
            | Self::Persist
            | Self::Pexpire
            | Self::Pexpireat
            | Self::Pfadd
            | Self::Pfdebug
            | Self::Pfmerge
            | Self::Pfselftest
            | Self::Psetex
            | Self::Psubscribe
            | Self::Psync
            | Self::Pubsub
            | Self::Punsubscribe
            | Self::Quit
            | Self::Readonly
            | Self::Readwrite
            | Self::Rename
            | Self::Renamenx
            | Self::Replconf
            | Self::Restore
            | Self::RestoreAsking
            | Self::Rpop
            | Self::Rpoplpush
            | Self::Rpush
            | Self::Rpushx
            | Self::Sadd
            | Self::Script
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
            | Self::Slaveof
            | Self::Slowlog
            | Self::Smove
            | Self::Sort
            | Self::Spop
            | Self::Srem
            | Self::Sunionstore
            | Self::Swapdb
            | Self::Sync
            | Self::Tfcall
            | Self::Tfcallasync
            | Self::TfunctionDelete
            | Self::TfunctionList
            | Self::TfunctionLoad
            | Self::Unlink
            | Self::Unwatch
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
            | Self::Wait
            | Self::Waitaof
            | Self::Watch
            | Self::Xack
            | Self::Xackdel
            | Self::Xadd
            | Self::Xautoclaim
            | Self::Xcfgset
            | Self::Xclaim
            | Self::Xdel
            | Self::Xdelex
            | Self::Xgroup
            | Self::XgroupCreate
            | Self::XgroupCreateconsumer
            | Self::XgroupDelconsumer
            | Self::XgroupDestroy
            | Self::XgroupHelp
            | Self::XgroupSetid
            | Self::Xinfo
            | Self::Xnack
            | Self::Xreadgroup
            | Self::Xsetid
            | Self::Xtrim
            | Self::Zadd
            | Self::Zdiffstore
            | Self::Zincrby
            | Self::Zinterstore
            | Self::Zmpop
            | Self::Zpopmax
            | Self::Zpopmin
            | Self::Zrangestore
            | Self::Zrem
            | Self::Zremrangebylex
            | Self::Zremrangebyrank
            | Self::Zremrangebyscore
            | Self::Zunionstore => Properties::Neither,
        }
    }

    /// Returns `true` if this command only reads data.
    #[cfg(feature = "cluster")]
    pub(crate) fn is_readonly(self) -> bool {
        matches!(
            self.command_properties(),
            Properties::ReadOnly | Properties::ReadOnlyCacheable(_)
        )
    }

    /// Returns how this command's response is cached client-side, or `None` if it can't be.
    #[cfg(feature = "cache-aio")]
    pub(crate) fn cache_kind(self) -> Option<CacheKind> {
        match self.command_properties() {
            Properties::ReadOnlyCacheable(kind) => Some(kind),
            Properties::ReadOnly | Properties::Neither => None,
        }
    }
}
