//! Commands known to this crate.
//!
//! Every command is declared once, in the `known_commands!` invocation below. The macro
//! generates the [`KnownCommand`] enum, its canonical names and the parser from a command's
//! first arguments. The behaviour of each command (routing, caching, ...) is declared in the
//! exhaustive matches further down, so adding a command forces a decision for each of them.

#[cfg(feature = "cluster")]
use crate::cluster_handling::routing::{
    AggregateOp, ArrayAggregateOp, LogicalAggregateOp, MultiSlotArgPattern, ResponsePolicy, RouteBy,
};

/// The longest command name the parser accepts, in bytes. A test checks that every known
/// name fits.
#[cfg(any(feature = "cluster", feature = "cache-aio", test))]
const MAX_NAME_LEN: usize = 32;

/// Declares the commands known to this crate.
///
/// Each entry is `Variant => "PRIMARY"` or, for a sub-command, `Variant => "PRIMARY" "SUB"`.
macro_rules! known_commands {
    ($($variant:ident => $primary:literal $($sub:literal)?,)*) => {
        /// A command known to this crate.
        ///
        /// This is used internally to map a command to its behaviour (read-only access,
        /// caching, cluster routing, response aggregation, ...) without allocating a string.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        // Without the features that parse commands from their arguments, the variants that
        // no typed command uses are never constructed.
        #[cfg_attr(
            not(any(feature = "cluster", feature = "cache-aio")),
            allow(dead_code)
        )]
        pub(crate) enum KnownCommand {
            $($variant,)*
        }

        impl KnownCommand {
            /// Every known command.
            #[cfg(test)]
            pub(crate) const ALL: &'static [Self] = &[$(Self::$variant,)*];

            /// Returns the canonical name of this command, with a space before its
            /// sub-command, if any.
            #[cfg(any(feature = "cache-aio", test))]
            pub(crate) fn name(self) -> &'static str {
                match self {
                    $(Self::$variant => concat!($primary $(, " ", $sub)?),)*
                }
            }

            /// Returns the primary command name and, if present, its sub-command.
            pub(crate) fn name_parts(self) -> (&'static [u8], Option<&'static [u8]>) {
                match self {
                    $(Self::$variant => ($primary.as_bytes(), known_commands!(@sub $($sub)?)),)*
                }
            }

            /// Parses an uppercase command name, as returned by [`Self::name`].
            #[cfg(any(feature = "cluster", feature = "cache-aio", test))]
            fn from_name(name: &str) -> Option<Self> {
                match name {
                    $(concat!($primary $(, " ", $sub)?) => Some(Self::$variant),)*
                    _ => None,
                }
            }

            /// Returns `true` if the uppercase command `primary` takes a sub-command.
            #[cfg(any(feature = "cluster", feature = "cache-aio", test))]
            fn has_subcommands(primary: &str) -> bool {
                // Each primary is listed once per sub-command, so the later arms are unreachable.
                #[allow(unreachable_patterns)]
                match primary {
                    $($($primary => {
                        // Only expands for entries with a sub-command.
                        let _ = $sub;
                        true
                    })?)*
                    _ => false,
                }
            }
        }
    };
    (@sub) => { None };
    (@sub $sub:literal) => { Some($sub.as_bytes()) };
}

known_commands! {
    Acl => "ACL",
    AclCat => "ACL" "CAT",
    AclDeluser => "ACL" "DELUSER",
    AclDryrun => "ACL" "DRYRUN",
    AclGenpass => "ACL" "GENPASS",
    AclGetuser => "ACL" "GETUSER",
    AclHelp => "ACL" "HELP",
    AclList => "ACL" "LIST",
    AclLoad => "ACL" "LOAD",
    AclLog => "ACL" "LOG",
    AclSave => "ACL" "SAVE",
    AclSetuser => "ACL" "SETUSER",
    AclUsers => "ACL" "USERS",
    AclWhoami => "ACL" "WHOAMI",
    Append => "APPEND",
    Asking => "ASKING",
    Auth => "AUTH",
    BfAdd => "BF.ADD",
    BfCard => "BF.CARD",
    BfExists => "BF.EXISTS",
    BfInfo => "BF.INFO",
    BfInsert => "BF.INSERT",
    BfLoadchunk => "BF.LOADCHUNK",
    BfMadd => "BF.MADD",
    BfMexists => "BF.MEXISTS",
    BfReserve => "BF.RESERVE",
    BfScandump => "BF.SCANDUMP",
    Bgrewriteaof => "BGREWRITEAOF",
    Bgsave => "BGSAVE",
    Bitcount => "BITCOUNT",
    Bitfield => "BITFIELD",
    BitfieldRo => "BITFIELD_RO",
    Bitop => "BITOP",
    Bitpos => "BITPOS",
    Blmove => "BLMOVE",
    Blmpop => "BLMPOP",
    Blpop => "BLPOP",
    Brpop => "BRPOP",
    Brpoplpush => "BRPOPLPUSH",
    Bzmpop => "BZMPOP",
    Bzpopmax => "BZPOPMAX",
    Bzpopmin => "BZPOPMIN",
    Client => "CLIENT",
    ClientCaching => "CLIENT" "CACHING",
    ClientCapa => "CLIENT" "CAPA",
    ClientGetname => "CLIENT" "GETNAME",
    ClientGetredir => "CLIENT" "GETREDIR",
    ClientHelp => "CLIENT" "HELP",
    ClientId => "CLIENT" "ID",
    ClientInfo => "CLIENT" "INFO",
    ClientKill => "CLIENT" "KILL",
    ClientList => "CLIENT" "LIST",
    ClientNoEvict => "CLIENT" "NO-EVICT",
    ClientNoTouch => "CLIENT" "NO-TOUCH",
    ClientPause => "CLIENT" "PAUSE",
    ClientReply => "CLIENT" "REPLY",
    ClientSetinfo => "CLIENT" "SETINFO",
    ClientSetname => "CLIENT" "SETNAME",
    ClientTracking => "CLIENT" "TRACKING",
    ClientTrackinginfo => "CLIENT" "TRACKINGINFO",
    ClientUnblock => "CLIENT" "UNBLOCK",
    ClientUnpause => "CLIENT" "UNPAUSE",
    Cluster => "CLUSTER",
    ClusterAddslots => "CLUSTER" "ADDSLOTS",
    ClusterAddslotsrange => "CLUSTER" "ADDSLOTSRANGE",
    ClusterBumpepoch => "CLUSTER" "BUMPEPOCH",
    ClusterCountFailureReports => "CLUSTER" "COUNT-FAILURE-REPORTS",
    ClusterCountkeysinslot => "CLUSTER" "COUNTKEYSINSLOT",
    ClusterDelslots => "CLUSTER" "DELSLOTS",
    ClusterDelslotsrange => "CLUSTER" "DELSLOTSRANGE",
    ClusterFailover => "CLUSTER" "FAILOVER",
    ClusterFlushslots => "CLUSTER" "FLUSHSLOTS",
    ClusterForget => "CLUSTER" "FORGET",
    ClusterGetkeysinslot => "CLUSTER" "GETKEYSINSLOT",
    ClusterHelp => "CLUSTER" "HELP",
    ClusterInfo => "CLUSTER" "INFO",
    ClusterKeyslot => "CLUSTER" "KEYSLOT",
    ClusterLinks => "CLUSTER" "LINKS",
    ClusterMeet => "CLUSTER" "MEET",
    ClusterMyid => "CLUSTER" "MYID",
    ClusterMyshardid => "CLUSTER" "MYSHARDID",
    ClusterNodes => "CLUSTER" "NODES",
    ClusterReplicas => "CLUSTER" "REPLICAS",
    ClusterReplicate => "CLUSTER" "REPLICATE",
    ClusterReset => "CLUSTER" "RESET",
    ClusterSaveconfig => "CLUSTER" "SAVECONFIG",
    ClusterSetConfigEpoch => "CLUSTER" "SET-CONFIG-EPOCH",
    ClusterSetslot => "CLUSTER" "SETSLOT",
    ClusterShards => "CLUSTER" "SHARDS",
    ClusterSlaves => "CLUSTER" "SLAVES",
    ClusterSlots => "CLUSTER" "SLOTS",
    Command => "COMMAND",
    CommandCount => "COMMAND" "COUNT",
    CommandDocs => "COMMAND" "DOCS",
    CommandGetkeys => "COMMAND" "GETKEYS",
    CommandGetkeysandflags => "COMMAND" "GETKEYSANDFLAGS",
    CommandHelp => "COMMAND" "HELP",
    CommandInfo => "COMMAND" "INFO",
    CommandList => "COMMAND" "LIST",
    Config => "CONFIG",
    ConfigGet => "CONFIG" "GET",
    ConfigHelp => "CONFIG" "HELP",
    ConfigResetstat => "CONFIG" "RESETSTAT",
    ConfigRewrite => "CONFIG" "REWRITE",
    ConfigSet => "CONFIG" "SET",
    Copy => "COPY",
    Dbsize => "DBSIZE",
    Debug => "DEBUG",
    Decr => "DECR",
    Decrby => "DECRBY",
    Del => "DEL",
    Delex => "DELEX",
    Digest => "DIGEST",
    Discard => "DISCARD",
    Dump => "DUMP",
    Echo => "ECHO",
    Eval => "EVAL",
    Evalsha => "EVALSHA",
    EvalshaRo => "EVALSHA_RO",
    EvalRo => "EVAL_RO",
    Exec => "EXEC",
    Exists => "EXISTS",
    Expire => "EXPIRE",
    Expireat => "EXPIREAT",
    Expiretime => "EXPIRETIME",
    Failover => "FAILOVER",
    Fcall => "FCALL",
    FcallRo => "FCALL_RO",
    Flushall => "FLUSHALL",
    Flushdb => "FLUSHDB",
    FtAggregate => "FT.AGGREGATE",
    FtCreate => "FT.CREATE",
    FtExplain => "FT.EXPLAIN",
    FtExplaincli => "FT.EXPLAINCLI",
    FtInfo => "FT.INFO",
    FtProfile => "FT.PROFILE",
    FtSearch => "FT.SEARCH",
    FtAliasList => "FT._ALIASLIST",
    FtList => "FT._LIST",
    Function => "FUNCTION",
    FunctionDelete => "FUNCTION" "DELETE",
    FunctionDump => "FUNCTION" "DUMP",
    FunctionFlush => "FUNCTION" "FLUSH",
    FunctionHelp => "FUNCTION" "HELP",
    FunctionKill => "FUNCTION" "KILL",
    FunctionList => "FUNCTION" "LIST",
    FunctionLoad => "FUNCTION" "LOAD",
    FunctionRestore => "FUNCTION" "RESTORE",
    FunctionStats => "FUNCTION" "STATS",
    Geoadd => "GEOADD",
    Geodist => "GEODIST",
    Geohash => "GEOHASH",
    Geopos => "GEOPOS",
    Georadius => "GEORADIUS",
    Georadiusbymember => "GEORADIUSBYMEMBER",
    GeoradiusbymemberRo => "GEORADIUSBYMEMBER_RO",
    GeoradiusRo => "GEORADIUS_RO",
    Geosearch => "GEOSEARCH",
    Geosearchstore => "GEOSEARCHSTORE",
    Get => "GET",
    Getbit => "GETBIT",
    Getdel => "GETDEL",
    Getex => "GETEX",
    Getrange => "GETRANGE",
    Getset => "GETSET",
    Hdel => "HDEL",
    Hello => "HELLO",
    Hexists => "HEXISTS",
    Hexpire => "HEXPIRE",
    Hexpireat => "HEXPIREAT",
    Hexpiretime => "HEXPIRETIME",
    Hget => "HGET",
    Hgetall => "HGETALL",
    Hgetdel => "HGETDEL",
    Hgetex => "HGETEX",
    Hincrby => "HINCRBY",
    Hincrbyfloat => "HINCRBYFLOAT",
    Hkeys => "HKEYS",
    Hlen => "HLEN",
    Hmget => "HMGET",
    Hmset => "HMSET",
    Hotkeys => "HOTKEYS",
    Hpersist => "HPERSIST",
    Hpexpire => "HPEXPIRE",
    Hpexpireat => "HPEXPIREAT",
    Hpexpiretime => "HPEXPIRETIME",
    Hpttl => "HPTTL",
    Hrandfield => "HRANDFIELD",
    Hscan => "HSCAN",
    Hset => "HSET",
    Hsetex => "HSETEX",
    Hsetnx => "HSETNX",
    Hstrlen => "HSTRLEN",
    Httl => "HTTL",
    Hvals => "HVALS",
    Incr => "INCR",
    Incrby => "INCRBY",
    Incrbyfloat => "INCRBYFLOAT",
    Increx => "INCREX",
    Info => "INFO",
    JsonArrappend => "JSON.ARRAPPEND",
    JsonArrindex => "JSON.ARRINDEX",
    JsonArrinsert => "JSON.ARRINSERT",
    JsonArrlen => "JSON.ARRLEN",
    JsonArrpop => "JSON.ARRPOP",
    JsonArrtrim => "JSON.ARRTRIM",
    JsonClear => "JSON.CLEAR",
    JsonDebug => "JSON.DEBUG",
    JsonDel => "JSON.DEL",
    JsonGet => "JSON.GET",
    JsonMget => "JSON.MGET",
    JsonMset => "JSON.MSET",
    JsonNumincrby => "JSON.NUMINCRBY",
    JsonObjkeys => "JSON.OBJKEYS",
    JsonObjlen => "JSON.OBJLEN",
    JsonResp => "JSON.RESP",
    JsonSet => "JSON.SET",
    JsonStrappend => "JSON.STRAPPEND",
    JsonStrlen => "JSON.STRLEN",
    JsonToggle => "JSON.TOGGLE",
    JsonType => "JSON.TYPE",
    Keys => "KEYS",
    Lastsave => "LASTSAVE",
    Latency => "LATENCY",
    LatencyDoctor => "LATENCY" "DOCTOR",
    LatencyGraph => "LATENCY" "GRAPH",
    LatencyHelp => "LATENCY" "HELP",
    LatencyHistogram => "LATENCY" "HISTOGRAM",
    LatencyHistory => "LATENCY" "HISTORY",
    LatencyLatest => "LATENCY" "LATEST",
    LatencyReset => "LATENCY" "RESET",
    Lcs => "LCS",
    Lindex => "LINDEX",
    Linsert => "LINSERT",
    Llen => "LLEN",
    Lmove => "LMOVE",
    Lmpop => "LMPOP",
    Lolwut => "LOLWUT",
    Lpop => "LPOP",
    Lpos => "LPOS",
    Lpush => "LPUSH",
    Lpushx => "LPUSHX",
    Lrange => "LRANGE",
    Lrem => "LREM",
    Lset => "LSET",
    Ltrim => "LTRIM",
    Memory => "MEMORY",
    MemoryDoctor => "MEMORY" "DOCTOR",
    MemoryHelp => "MEMORY" "HELP",
    MemoryMallocStats => "MEMORY" "MALLOC-STATS",
    MemoryPurge => "MEMORY" "PURGE",
    MemoryStats => "MEMORY" "STATS",
    MemoryUsage => "MEMORY" "USAGE",
    Mget => "MGET",
    Migrate => "MIGRATE",
    Module => "MODULE",
    ModuleHelp => "MODULE" "HELP",
    ModuleList => "MODULE" "LIST",
    ModuleLoad => "MODULE" "LOAD",
    ModuleLoadex => "MODULE" "LOADEX",
    ModuleUnload => "MODULE" "UNLOAD",
    Monitor => "MONITOR",
    Move => "MOVE",
    Mset => "MSET",
    Msetex => "MSETEX",
    Msetnx => "MSETNX",
    Multi => "MULTI",
    Object => "OBJECT",
    ObjectEncoding => "OBJECT" "ENCODING",
    ObjectFreq => "OBJECT" "FREQ",
    ObjectHelp => "OBJECT" "HELP",
    ObjectIdletime => "OBJECT" "IDLETIME",
    ObjectRefcount => "OBJECT" "REFCOUNT",
    Persist => "PERSIST",
    Pexpire => "PEXPIRE",
    Pexpireat => "PEXPIREAT",
    Pexpiretime => "PEXPIRETIME",
    Pfadd => "PFADD",
    Pfcount => "PFCOUNT",
    Pfdebug => "PFDEBUG",
    Pfmerge => "PFMERGE",
    Pfselftest => "PFSELFTEST",
    Ping => "PING",
    Psetex => "PSETEX",
    Psubscribe => "PSUBSCRIBE",
    Psync => "PSYNC",
    Pttl => "PTTL",
    Publish => "PUBLISH",
    Pubsub => "PUBSUB",
    PubsubChannels => "PUBSUB" "CHANNELS",
    PubsubHelp => "PUBSUB" "HELP",
    PubsubNumpat => "PUBSUB" "NUMPAT",
    PubsubNumsub => "PUBSUB" "NUMSUB",
    PubsubShardchannels => "PUBSUB" "SHARDCHANNELS",
    PubsubShardnumsub => "PUBSUB" "SHARDNUMSUB",
    Punsubscribe => "PUNSUBSCRIBE",
    Quit => "QUIT",
    Randomkey => "RANDOMKEY",
    Readonly => "READONLY",
    Readwrite => "READWRITE",
    Rename => "RENAME",
    Renamenx => "RENAMENX",
    Replconf => "REPLCONF",
    Replicaof => "REPLICAOF",
    Reset => "RESET",
    Restore => "RESTORE",
    RestoreAsking => "RESTORE-ASKING",
    Role => "ROLE",
    Rpop => "RPOP",
    Rpoplpush => "RPOPLPUSH",
    Rpush => "RPUSH",
    Rpushx => "RPUSHX",
    Sadd => "SADD",
    Save => "SAVE",
    Scan => "SCAN",
    Scard => "SCARD",
    Script => "SCRIPT",
    ScriptDebug => "SCRIPT" "DEBUG",
    ScriptExists => "SCRIPT" "EXISTS",
    ScriptFlush => "SCRIPT" "FLUSH",
    ScriptHelp => "SCRIPT" "HELP",
    ScriptKill => "SCRIPT" "KILL",
    ScriptLoad => "SCRIPT" "LOAD",
    ScriptShow => "SCRIPT" "SHOW",
    Sdiff => "SDIFF",
    Sdiffstore => "SDIFFSTORE",
    Select => "SELECT",
    Sentinel => "SENTINEL",
    SentinelGetMasterAddrByName => "SENTINEL" "GET-MASTER-ADDR-BY-NAME",
    SentinelMaster => "SENTINEL" "MASTER",
    SentinelMasters => "SENTINEL" "MASTERS",
    SentinelMonitor => "SENTINEL" "MONITOR",
    SentinelRemove => "SENTINEL" "REMOVE",
    SentinelSentinels => "SENTINEL" "SENTINELS",
    SentinelSet => "SENTINEL" "SET",
    SentinelSlaves => "SENTINEL" "SLAVES",
    Set => "SET",
    Setbit => "SETBIT",
    Setex => "SETEX",
    Setnx => "SETNX",
    Setrange => "SETRANGE",
    Shutdown => "SHUTDOWN",
    Sinter => "SINTER",
    Sintercard => "SINTERCARD",
    Sinterstore => "SINTERSTORE",
    Sismember => "SISMEMBER",
    Slaveof => "SLAVEOF",
    Slowlog => "SLOWLOG",
    SlowlogGet => "SLOWLOG" "GET",
    SlowlogHelp => "SLOWLOG" "HELP",
    SlowlogLen => "SLOWLOG" "LEN",
    SlowlogReset => "SLOWLOG" "RESET",
    Smembers => "SMEMBERS",
    Smismember => "SMISMEMBER",
    Smove => "SMOVE",
    Sort => "SORT",
    SortRo => "SORT_RO",
    Spop => "SPOP",
    Spublish => "SPUBLISH",
    Srandmember => "SRANDMEMBER",
    Srem => "SREM",
    Sscan => "SSCAN",
    Ssubscribe => "SSUBSCRIBE",
    Strlen => "STRLEN",
    Subscribe => "SUBSCRIBE",
    Substr => "SUBSTR",
    Sunion => "SUNION",
    Sunionstore => "SUNIONSTORE",
    Sunsubscribe => "SUNSUBSCRIBE",
    Swapdb => "SWAPDB",
    Sync => "SYNC",
    Tfcall => "TFCALL",
    Tfcallasync => "TFCALLASYNC",
    TfunctionDelete => "TFUNCTION" "DELETE",
    TfunctionList => "TFUNCTION" "LIST",
    TfunctionLoad => "TFUNCTION" "LOAD",
    Time => "TIME",
    Touch => "TOUCH",
    Ttl => "TTL",
    Type => "TYPE",
    Unlink => "UNLINK",
    Unsubscribe => "UNSUBSCRIBE",
    Unwatch => "UNWATCH",
    Vadd => "VADD",
    Vcard => "VCARD",
    Vdim => "VDIM",
    Vemb => "VEMB",
    Vgetattr => "VGETATTR",
    Vinfo => "VINFO",
    Vlinks => "VLINKS",
    Vrandmember => "VRANDMEMBER",
    Vrem => "VREM",
    Vsetattr => "VSETATTR",
    Vsim => "VSIM",
    Wait => "WAIT",
    Waitaof => "WAITAOF",
    Watch => "WATCH",
    Xack => "XACK",
    Xackdel => "XACKDEL",
    Xadd => "XADD",
    Xautoclaim => "XAUTOCLAIM",
    Xcfgset => "XCFGSET",
    Xclaim => "XCLAIM",
    Xdel => "XDEL",
    Xdelex => "XDELEX",
    Xgroup => "XGROUP",
    XgroupCreate => "XGROUP" "CREATE",
    XgroupCreateconsumer => "XGROUP" "CREATECONSUMER",
    XgroupDelconsumer => "XGROUP" "DELCONSUMER",
    XgroupDestroy => "XGROUP" "DESTROY",
    XgroupHelp => "XGROUP" "HELP",
    XgroupSetid => "XGROUP" "SETID",
    Xinfo => "XINFO",
    XinfoConsumers => "XINFO" "CONSUMERS",
    XinfoGroups => "XINFO" "GROUPS",
    XinfoHelp => "XINFO" "HELP",
    XinfoStream => "XINFO" "STREAM",
    Xlen => "XLEN",
    Xnack => "XNACK",
    Xpending => "XPENDING",
    Xrange => "XRANGE",
    Xread => "XREAD",
    Xreadgroup => "XREADGROUP",
    Xrevrange => "XREVRANGE",
    Xsetid => "XSETID",
    Xtrim => "XTRIM",
    Zadd => "ZADD",
    Zcard => "ZCARD",
    Zcount => "ZCOUNT",
    Zdiff => "ZDIFF",
    Zdiffstore => "ZDIFFSTORE",
    Zincrby => "ZINCRBY",
    Zinter => "ZINTER",
    Zintercard => "ZINTERCARD",
    Zinterstore => "ZINTERSTORE",
    Zlexcount => "ZLEXCOUNT",
    Zmpop => "ZMPOP",
    Zmscore => "ZMSCORE",
    Zpopmax => "ZPOPMAX",
    Zpopmin => "ZPOPMIN",
    Zrandmember => "ZRANDMEMBER",
    Zrange => "ZRANGE",
    Zrangebylex => "ZRANGEBYLEX",
    Zrangebyscore => "ZRANGEBYSCORE",
    Zrangestore => "ZRANGESTORE",
    Zrank => "ZRANK",
    Zrem => "ZREM",
    Zremrangebylex => "ZREMRANGEBYLEX",
    Zremrangebyrank => "ZREMRANGEBYRANK",
    Zremrangebyscore => "ZREMRANGEBYSCORE",
    Zrevrange => "ZREVRANGE",
    Zrevrangebylex => "ZREVRANGEBYLEX",
    Zrevrangebyscore => "ZREVRANGEBYSCORE",
    Zrevrank => "ZREVRANK",
    Zscan => "ZSCAN",
    Zscore => "ZSCORE",
    Zunion => "ZUNION",
    Zunionstore => "ZUNIONSTORE",
}

/// Properties used to decide cluster routing and client-side caching.
#[cfg(any(feature = "cluster", feature = "cache-aio"))]
enum Properties {
    /// The command only reads data, and its response can be cached.
    ReadOnlyCacheable,
    /// The command only reads data.
    ReadOnly,
    /// The command neither only reads data nor is cacheable.
    Neither,
}

impl KnownCommand {
    /// Returns how many arguments the name of this command takes: one, or two for a sub-command.
    pub(crate) fn name_arg_count(self) -> usize {
        match self.name_parts() {
            (_, None) => 1,
            (_, Some(_)) => 2,
        }
    }

    /// Parses a command from its first one or two arguments, ignoring ASCII case.
    ///
    /// The second argument is only used when the first names a command that takes a
    /// sub-command. If it isn't one of that command's sub-commands, the command is unknown.
    /// A sub-command can also be passed with the command in the first argument, separated by
    /// whitespace (`"CLIENT GETNAME"`), in which case `second` is ignored.
    #[cfg(any(feature = "cluster", test))]
    pub(crate) fn from_parts(first: &[u8], second: Option<&[u8]>) -> Option<Self> {
        Self::parse(first, second).map(|(command, _)| command)
    }

    /// Like [`Self::from_parts`], but also returns how many of the two arguments the command's
    /// name took.
    #[cfg(any(feature = "cluster", feature = "cache-aio", test))]
    pub(crate) fn parse(first: &[u8], second: Option<&[u8]>) -> Option<(Self, usize)> {
        let (first, embedded_second) = split_command_name(first);
        let mut buffer = [0; MAX_NAME_LEN];
        let first_len = first.len();
        buffer.get_mut(..first_len)?.copy_from_slice(first);
        buffer[..first_len].make_ascii_uppercase();
        let primary = std::str::from_utf8(&buffer[..first_len]).ok()?;
        if !Self::has_subcommands(primary) {
            return Self::from_name(primary).map(|command| (command, 1));
        }
        let (second, arg_count) = match (embedded_second, second) {
            (Some(second), _) => (second, 1),
            (None, Some(second)) => (second, 2),
            (None, None) => return Self::from_name(primary).map(|command| (command, 1)),
        };

        let len = first_len + 1 + second.len();
        let name = buffer.get_mut(..len)?;
        name[first_len] = b' ';
        name[first_len + 1..].copy_from_slice(second);
        name[first_len + 1..].make_ascii_uppercase();
        Self::from_name(std::str::from_utf8(name).ok()?).map(|command| (command, arg_count))
    }

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
            | Self::JsonMget
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
            | Self::Mget
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
            | Self::Zunion => Properties::ReadOnlyCacheable,
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
            Properties::ReadOnly | Properties::ReadOnlyCacheable
        )
    }

    /// Returns `true` if this command's response can be cached client-side.
    #[cfg(feature = "cache-aio")]
    pub(crate) fn is_cacheable(self) -> bool {
        matches!(self.command_properties(), Properties::ReadOnlyCacheable)
    }

    /// Returns `true` if this command belongs to the RedisJSON module.
    #[cfg(feature = "cache-aio")]
    pub(crate) fn is_json_command(self) -> bool {
        self.name().starts_with("JSON.")
    }

    /// Returns this command and the single-key command that can serve its cached values,
    /// if this command supports multiple keys.
    #[cfg(feature = "cache-aio")]
    pub(crate) fn is_multi_key(self) -> Option<(Self, Self)> {
        match self {
            Self::Mget => Some((self, Self::Get)),
            Self::JsonMget => Some((self, Self::JsonGet)),
            _ => None,
        }
    }

    /// Returns the response policy for aggregating this command's responses across nodes.
    #[cfg(feature = "cluster")]
    pub(crate) fn response_policy(self) -> Option<ResponsePolicy> {
        match self {
            Self::ScriptExists => Some(ResponsePolicy::AggregateLogical(LogicalAggregateOp::And)),
            Self::ClientKill
            | Self::Dbsize
            | Self::Del
            | Self::Exists
            | Self::LatencyReset
            | Self::PubsubNumpat
            | Self::SlowlogLen
            | Self::Touch
            | Self::Unlink => Some(ResponsePolicy::Aggregate(AggregateOp::Sum)),
            Self::Wait => Some(ResponsePolicy::Aggregate(AggregateOp::Min)),
            Self::Waitaof => Some(ResponsePolicy::AggregateArray(ArrayAggregateOp::Min)),
            Self::AclDeluser
            | Self::AclSave
            | Self::AclSetuser
            | Self::Auth
            | Self::ClientPause
            | Self::ClientReply
            | Self::ClientSetinfo
            | Self::ClientSetname
            | Self::ClientUnpause
            | Self::ConfigResetstat
            | Self::ConfigRewrite
            | Self::ConfigSet
            | Self::Flushall
            | Self::Flushdb
            | Self::FunctionDelete
            | Self::FunctionFlush
            | Self::FunctionLoad
            | Self::FunctionRestore
            | Self::JsonMset
            | Self::MemoryPurge
            | Self::Mset
            | Self::Ping
            | Self::Reset
            | Self::Save
            | Self::ScriptFlush
            | Self::ScriptLoad
            | Self::Select
            | Self::SlowlogReset
            | Self::Unwatch
            | Self::Watch => Some(ResponsePolicy::AllSucceeded),
            Self::FtAliasList
            | Self::FtList
            | Self::JsonMget
            | Self::Keys
            | Self::Mget
            | Self::PubsubChannels
            | Self::PubsubShardchannels
            | Self::SlowlogGet => Some(ResponsePolicy::CombineArrays),
            Self::PubsubNumsub | Self::PubsubShardnumsub => Some(ResponsePolicy::CombineMaps),
            Self::FunctionKill | Self::ScriptKill => Some(ResponsePolicy::OneSucceeded),
            Self::Randomkey => Some(ResponsePolicy::FirstSucceededNonEmptyOrAllEmpty),
            Self::FunctionStats
            | Self::Info
            | Self::LatencyDoctor
            | Self::LatencyGraph
            | Self::LatencyHistogram
            | Self::LatencyHistory
            | Self::LatencyLatest
            | Self::MemoryDoctor
            | Self::MemoryMallocStats
            | Self::MemoryStats => Some(ResponsePolicy::Special),
            Self::Acl
            | Self::AclCat
            | Self::AclDryrun
            | Self::AclGenpass
            | Self::AclGetuser
            | Self::AclHelp
            | Self::AclList
            | Self::AclLoad
            | Self::AclLog
            | Self::AclUsers
            | Self::AclWhoami
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
            | Self::Bgrewriteaof
            | Self::Bgsave
            | Self::Bitcount
            | Self::Bitfield
            | Self::BitfieldRo
            | Self::Bitop
            | Self::Bitpos
            | Self::Blmove
            | Self::Blmpop
            | Self::Blpop
            | Self::Brpop
            | Self::Brpoplpush
            | Self::Bzmpop
            | Self::Bzpopmax
            | Self::Bzpopmin
            | Self::Client
            | Self::ClientCaching
            | Self::ClientCapa
            | Self::ClientGetname
            | Self::ClientGetredir
            | Self::ClientHelp
            | Self::ClientId
            | Self::ClientInfo
            | Self::ClientList
            | Self::ClientNoEvict
            | Self::ClientNoTouch
            | Self::ClientTracking
            | Self::ClientTrackinginfo
            | Self::ClientUnblock
            | Self::Cluster
            | Self::ClusterAddslots
            | Self::ClusterAddslotsrange
            | Self::ClusterBumpepoch
            | Self::ClusterCountFailureReports
            | Self::ClusterCountkeysinslot
            | Self::ClusterDelslots
            | Self::ClusterDelslotsrange
            | Self::ClusterFailover
            | Self::ClusterFlushslots
            | Self::ClusterForget
            | Self::ClusterGetkeysinslot
            | Self::ClusterHelp
            | Self::ClusterInfo
            | Self::ClusterKeyslot
            | Self::ClusterLinks
            | Self::ClusterMeet
            | Self::ClusterMyid
            | Self::ClusterMyshardid
            | Self::ClusterNodes
            | Self::ClusterReplicas
            | Self::ClusterReplicate
            | Self::ClusterReset
            | Self::ClusterSaveconfig
            | Self::ClusterSetConfigEpoch
            | Self::ClusterSetslot
            | Self::ClusterShards
            | Self::ClusterSlaves
            | Self::ClusterSlots
            | Self::Command
            | Self::CommandCount
            | Self::CommandDocs
            | Self::CommandGetkeys
            | Self::CommandGetkeysandflags
            | Self::CommandHelp
            | Self::CommandInfo
            | Self::CommandList
            | Self::Config
            | Self::ConfigGet
            | Self::ConfigHelp
            | Self::Copy
            | Self::Debug
            | Self::Decr
            | Self::Decrby
            | Self::Delex
            | Self::Digest
            | Self::Discard
            | Self::Dump
            | Self::Echo
            | Self::Eval
            | Self::Evalsha
            | Self::EvalshaRo
            | Self::EvalRo
            | Self::Exec
            | Self::Expire
            | Self::Expireat
            | Self::Expiretime
            | Self::Failover
            | Self::Fcall
            | Self::FcallRo
            | Self::FtAggregate
            | Self::FtCreate
            | Self::FtExplain
            | Self::FtExplaincli
            | Self::FtInfo
            | Self::FtProfile
            | Self::FtSearch
            | Self::Function
            | Self::FunctionDump
            | Self::FunctionHelp
            | Self::FunctionList
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
            | Self::JsonDebug
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
            | Self::Lastsave
            | Self::Latency
            | Self::LatencyHelp
            | Self::Lcs
            | Self::Lindex
            | Self::Linsert
            | Self::Llen
            | Self::Lmove
            | Self::Lmpop
            | Self::Lolwut
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
            | Self::MemoryUsage
            | Self::Migrate
            | Self::Module
            | Self::ModuleHelp
            | Self::ModuleList
            | Self::ModuleLoad
            | Self::ModuleLoadex
            | Self::ModuleUnload
            | Self::Monitor
            | Self::Move
            | Self::Msetex
            | Self::Msetnx
            | Self::Multi
            | Self::Object
            | Self::ObjectEncoding
            | Self::ObjectFreq
            | Self::ObjectHelp
            | Self::ObjectIdletime
            | Self::ObjectRefcount
            | Self::Persist
            | Self::Pexpire
            | Self::Pexpireat
            | Self::Pexpiretime
            | Self::Pfadd
            | Self::Pfcount
            | Self::Pfdebug
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
            | Self::Readonly
            | Self::Readwrite
            | Self::Rename
            | Self::Renamenx
            | Self::Replconf
            | Self::Replicaof
            | Self::Restore
            | Self::RestoreAsking
            | Self::Role
            | Self::Rpop
            | Self::Rpoplpush
            | Self::Rpush
            | Self::Rpushx
            | Self::Sadd
            | Self::Scan
            | Self::Scard
            | Self::Script
            | Self::ScriptDebug
            | Self::ScriptHelp
            | Self::ScriptShow
            | Self::Sdiff
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
            | Self::Shutdown
            | Self::Sinter
            | Self::Sintercard
            | Self::Sinterstore
            | Self::Sismember
            | Self::Slaveof
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
            | Self::Sunion
            | Self::Sunionstore
            | Self::Sunsubscribe
            | Self::Swapdb
            | Self::Sync
            | Self::Tfcall
            | Self::Tfcallasync
            | Self::TfunctionDelete
            | Self::TfunctionList
            | Self::TfunctionLoad
            | Self::Time
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
            | Self::XgroupCreate
            | Self::XgroupCreateconsumer
            | Self::XgroupDelconsumer
            | Self::XgroupDestroy
            | Self::XgroupHelp
            | Self::XgroupSetid
            | Self::Xinfo
            | Self::XinfoConsumers
            | Self::XinfoGroups
            | Self::XinfoHelp
            | Self::XinfoStream
            | Self::Xlen
            | Self::Xnack
            | Self::Xpending
            | Self::Xrange
            | Self::Xread
            | Self::Xreadgroup
            | Self::Xrevrange
            | Self::Xsetid
            | Self::Xtrim
            | Self::Zadd
            | Self::Zcard
            | Self::Zcount
            | Self::Zdiff
            | Self::Zdiffstore
            | Self::Zincrby
            | Self::Zinter
            | Self::Zintercard
            | Self::Zinterstore
            | Self::Zlexcount
            | Self::Zmpop
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
            | Self::Zunion
            | Self::Zunionstore => None,
        }
    }

    /// Returns how this command is routed in cluster mode.
    #[cfg(feature = "cluster")]
    pub(crate) fn base_routing(self) -> RouteBy {
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
            | Self::SlowlogReset => RouteBy::AllNodes,
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
            | Self::Waitaof => RouteBy::AllPrimaries,
            Self::Del | Self::Exists | Self::Mget | Self::Touch | Self::Unlink | Self::Watch => {
                RouteBy::MultiShard(MultiSlotArgPattern::KeysOnly)
            }
            Self::Mset => RouteBy::MultiShard(MultiSlotArgPattern::KeyValuePairs),
            Self::JsonMget => RouteBy::MultiShard(MultiSlotArgPattern::KeysAndLastArg),
            Self::JsonMset => RouteBy::MultiShard(MultiSlotArgPattern::KeyWithTwoArgTriples),
            Self::Replicaof | Self::Scan | Self::Shutdown | Self::Slaveof => RouteBy::Undefined,
            Self::Blmpop
            | Self::Bzmpop
            | Self::Eval
            | Self::Evalsha
            | Self::EvalshaRo
            | Self::EvalRo
            | Self::Fcall
            | Self::FcallRo => RouteBy::ThirdArgAfterKeyCount,
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
            | Self::XinfoStream => RouteBy::SecondArg,
            Self::Migrate => RouteBy::ThirdArg,
            Self::Lmpop
            | Self::Msetex
            | Self::Sintercard
            | Self::Zdiff
            | Self::Zinter
            | Self::Zintercard
            | Self::Zmpop
            | Self::Zunion => RouteBy::SecondArgAfterKeyCount,
            Self::Xread | Self::Xreadgroup => RouteBy::StreamsIndex,
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
            | Self::Time => RouteBy::Random,
            Self::ClusterAddslots
            | Self::ClusterCountkeysinslot
            | Self::ClusterDelslots
            | Self::ClusterDelslotsrange
            | Self::ClusterGetkeysinslot
            | Self::ClusterSetslot => RouteBy::SecondArgSlot,
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
            | Self::Lcs
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
            | Self::Sdiff
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
            | Self::Sinter
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
            | Self::Sunion
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
            | Self::Zunionstore => RouteBy::FirstKey,
        }
    }

    /// Returns `true` if this command cannot be used in a cluster pipeline.
    #[cfg(feature = "cluster")]
    pub(crate) fn is_illegal_in_cluster_pipeline(self) -> bool {
        match self {
            Self::Bgrewriteaof
            | Self::Bgsave
            | Self::Bitop
            | Self::Brpoplpush
            // All commands that start with "CLIENT"
            | Self::Client
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
            // All commands that start with "CONFIG"
            | Self::Config
            | Self::ConfigGet
            | Self::ConfigHelp
            | Self::ConfigResetstat
            | Self::ConfigRewrite
            | Self::ConfigSet
            | Self::Dbsize
            | Self::Echo
            | Self::Flushall
            | Self::Flushdb
            | Self::Info
            | Self::Keys
            | Self::Lastsave
            | Self::Mget
            | Self::Move
            | Self::Mset
            | Self::Msetnx
            | Self::Msetex
            | Self::Ping
            | Self::Publish
            | Self::Randomkey
            | Self::Rename
            | Self::Renamenx
            | Self::Rpoplpush
            | Self::Save
            | Self::Scan
            // All commands that start with "SCRIPT"
            | Self::Script
            | Self::ScriptDebug
            | Self::ScriptExists
            | Self::ScriptFlush
            | Self::ScriptHelp
            | Self::ScriptKill
            | Self::ScriptLoad
            | Self::ScriptShow
            | Self::Sdiff
            | Self::Sdiffstore
            // All commands that start with "SENTINEL"
            | Self::Sentinel
            | Self::SentinelGetMasterAddrByName
            | Self::SentinelMaster
            | Self::SentinelMasters
            | Self::SentinelMonitor
            | Self::SentinelRemove
            | Self::SentinelSentinels
            | Self::SentinelSet
            | Self::SentinelSlaves
            | Self::Shutdown
            | Self::Sinter
            | Self::Sinterstore
            | Self::Slaveof
            // All commands that start with "SLOWLOG"
            | Self::Slowlog
            | Self::SlowlogGet
            | Self::SlowlogHelp
            | Self::SlowlogLen
            | Self::SlowlogReset
            | Self::Smove
            | Self::Sort
            | Self::Sunion
            | Self::Sunionstore
            | Self::Time => true,        // All other commands are legal.
            Self::Acl
            | Self::AclCat
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
            | Self::Append
            | Self::Asking
            | Self::Auth
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
            | Self::Blmpop
            | Self::Blpop
            | Self::Brpop
            | Self::Bzmpop
            | Self::Bzpopmax
            | Self::Bzpopmin
            | Self::Cluster
            | Self::ClusterAddslots
            | Self::ClusterAddslotsrange
            | Self::ClusterBumpepoch
            | Self::ClusterCountFailureReports
            | Self::ClusterCountkeysinslot
            | Self::ClusterDelslots
            | Self::ClusterDelslotsrange
            | Self::ClusterFailover
            | Self::ClusterFlushslots
            | Self::ClusterForget
            | Self::ClusterGetkeysinslot
            | Self::ClusterHelp
            | Self::ClusterInfo
            | Self::ClusterKeyslot
            | Self::ClusterLinks
            | Self::ClusterMeet
            | Self::ClusterMyid
            | Self::ClusterMyshardid
            | Self::ClusterNodes
            | Self::ClusterReplicas
            | Self::ClusterReplicate
            | Self::ClusterReset
            | Self::ClusterSaveconfig
            | Self::ClusterSetConfigEpoch
            | Self::ClusterSetslot
            | Self::ClusterShards
            | Self::ClusterSlaves
            | Self::ClusterSlots
            | Self::Command
            | Self::CommandCount
            | Self::CommandDocs
            | Self::CommandGetkeys
            | Self::CommandGetkeysandflags
            | Self::CommandHelp
            | Self::CommandInfo
            | Self::CommandList
            | Self::Copy
            | Self::Debug
            | Self::Decr
            | Self::Decrby
            | Self::Del
            | Self::Delex
            | Self::Digest
            | Self::Discard
            | Self::Dump
            | Self::Eval
            | Self::Evalsha
            | Self::EvalshaRo
            | Self::EvalRo
            | Self::Exec
            | Self::Exists
            | Self::Expire
            | Self::Expireat
            | Self::Expiretime
            | Self::Failover
            | Self::Fcall
            | Self::FcallRo
            | Self::FtAggregate
            | Self::FtCreate
            | Self::FtExplain
            | Self::FtExplaincli
            | Self::FtInfo
            | Self::FtProfile
            | Self::FtSearch
            | Self::FtAliasList
            | Self::FtList
            | Self::Function
            | Self::FunctionDelete
            | Self::FunctionDump
            | Self::FunctionFlush
            | Self::FunctionHelp
            | Self::FunctionKill
            | Self::FunctionList
            | Self::FunctionLoad
            | Self::FunctionRestore
            | Self::FunctionStats
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
            | Self::JsonDebug
            | Self::JsonDel
            | Self::JsonGet
            | Self::JsonMget
            | Self::JsonMset
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
            | Self::LatencyDoctor
            | Self::LatencyGraph
            | Self::LatencyHelp
            | Self::LatencyHistogram
            | Self::LatencyHistory
            | Self::LatencyLatest
            | Self::LatencyReset
            | Self::Lcs
            | Self::Lindex
            | Self::Linsert
            | Self::Llen
            | Self::Lmove
            | Self::Lmpop
            | Self::Lolwut
            | Self::Lpop
            | Self::Lpos
            | Self::Lpush
            | Self::Lpushx
            | Self::Lrange
            | Self::Lrem
            | Self::Lset
            | Self::Ltrim
            | Self::Memory
            | Self::MemoryDoctor
            | Self::MemoryHelp
            | Self::MemoryMallocStats
            | Self::MemoryPurge
            | Self::MemoryStats
            | Self::MemoryUsage
            | Self::Migrate
            | Self::Module
            | Self::ModuleHelp
            | Self::ModuleList
            | Self::ModuleLoad
            | Self::ModuleLoadex
            | Self::ModuleUnload
            | Self::Monitor
            | Self::Multi
            | Self::Object
            | Self::ObjectEncoding
            | Self::ObjectFreq
            | Self::ObjectHelp
            | Self::ObjectIdletime
            | Self::ObjectRefcount
            | Self::Persist
            | Self::Pexpire
            | Self::Pexpireat
            | Self::Pexpiretime
            | Self::Pfadd
            | Self::Pfcount
            | Self::Pfdebug
            | Self::Pfmerge
            | Self::Pfselftest
            | Self::Psetex
            | Self::Psubscribe
            | Self::Psync
            | Self::Pttl
            | Self::Pubsub
            | Self::PubsubChannels
            | Self::PubsubHelp
            | Self::PubsubNumpat
            | Self::PubsubNumsub
            | Self::PubsubShardchannels
            | Self::PubsubShardnumsub
            | Self::Punsubscribe
            | Self::Quit
            | Self::Readonly
            | Self::Readwrite
            | Self::Replconf
            | Self::Replicaof
            | Self::Reset
            | Self::Restore
            | Self::RestoreAsking
            | Self::Role
            | Self::Rpop
            | Self::Rpush
            | Self::Rpushx
            | Self::Sadd
            | Self::Scard
            | Self::Select
            | Self::Set
            | Self::Setbit
            | Self::Setex
            | Self::Setnx
            | Self::Setrange
            | Self::Sintercard
            | Self::Sismember
            | Self::Smembers
            | Self::Smismember
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
            | Self::Sunsubscribe
            | Self::Swapdb
            | Self::Sync
            | Self::Tfcall
            | Self::Tfcallasync
            | Self::TfunctionDelete
            | Self::TfunctionList
            | Self::TfunctionLoad
            | Self::Touch
            | Self::Ttl
            | Self::Type
            | Self::Unlink
            | Self::Unsubscribe
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
            | Self::XinfoConsumers
            | Self::XinfoGroups
            | Self::XinfoHelp
            | Self::XinfoStream
            | Self::Xlen
            | Self::Xnack
            | Self::Xpending
            | Self::Xrange
            | Self::Xread
            | Self::Xreadgroup
            | Self::Xrevrange
            | Self::Xsetid
            | Self::Xtrim
            | Self::Zadd
            | Self::Zcard
            | Self::Zcount
            | Self::Zdiff
            | Self::Zdiffstore
            | Self::Zincrby
            | Self::Zinter
            | Self::Zintercard
            | Self::Zinterstore
            | Self::Zlexcount
            | Self::Zmpop
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
            | Self::Zunion
            | Self::Zunionstore => false,
        }
    }
}

/// Splits a command name that may contain a sub-command from surrounding whitespace.
#[cfg(any(feature = "cluster", feature = "cache-aio", test))]
fn split_command_name(command: &[u8]) -> (&[u8], Option<&[u8]>) {
    let command = command.trim_ascii();
    let Some(separator) = command.iter().position(u8::is_ascii_whitespace) else {
        return (command, None);
    };
    let primary = &command[..separator];
    let secondary = command[separator..].trim_ascii_start();
    (primary, (!secondary.is_empty()).then_some(secondary))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_round_trip(name: &str, expected: KnownCommand) {
        let (first, second) = match name.split_once(' ') {
            Some((first, second)) => (first, Some(second)),
            None => (name, None),
        };
        assert_eq!(
            KnownCommand::from_parts(first.as_bytes(), second.map(str::as_bytes)),
            Some(expected),
            "failed to parse {name:?}"
        );
        assert_eq!(
            KnownCommand::from_parts(name.as_bytes(), None),
            Some(expected),
            "failed to parse {name:?} as a single argument"
        );
    }

    #[test]
    fn command_names_are_unique() {
        let mut names = std::collections::HashSet::new();
        for &command in KnownCommand::ALL {
            assert!(
                names.insert(command.name()),
                "duplicate name for {command:?}"
            );
        }
    }

    #[test]
    fn command_names_fit_the_parse_buffer() {
        for &command in KnownCommand::ALL {
            assert!(
                command.name().len() <= MAX_NAME_LEN,
                "{command:?} is too long"
            );
        }
    }

    #[test]
    fn command_names_round_trip() {
        for &command in KnownCommand::ALL {
            assert_round_trip(command.name(), command);
        }
    }

    #[test]
    fn command_names_round_trip_lowercase() {
        for &command in KnownCommand::ALL {
            assert_round_trip(&command.name().to_ascii_lowercase(), command);
        }
    }

    #[test]
    fn name_parts_match_name() {
        for &command in KnownCommand::ALL {
            let (primary, sub) = command.name_parts();
            let mut name = primary.to_vec();
            if let Some(sub) = sub {
                name.push(b' ');
                name.extend_from_slice(sub);
            }
            assert_eq!(name, command.name().as_bytes());
            assert_eq!(command.name_arg_count(), 1 + usize::from(sub.is_some()));
        }
    }

    #[test]
    fn unknown_sub_command_is_unknown() {
        assert_eq!(KnownCommand::from_parts(b"CLIENT", Some(b"FOO")), None);
        assert_eq!(KnownCommand::from_parts(b"CLIENT FOO", None), None);
        // A command without sub-commands ignores its second argument.
        assert_eq!(
            KnownCommand::from_parts(b"GET", Some(b"FOO")),
            Some(KnownCommand::Get)
        );
        // A command that takes sub-commands can also be sent alone.
        assert_eq!(
            KnownCommand::from_parts(b"CLIENT", None),
            Some(KnownCommand::Client)
        );
    }

    #[test]
    fn parse_counts_the_arguments_the_name_takes() {
        assert_eq!(
            KnownCommand::parse(b"GET", Some(b"key")),
            Some((KnownCommand::Get, 1))
        );
        assert_eq!(
            KnownCommand::parse(b"CLIENT", Some(b"GETNAME")),
            Some((KnownCommand::ClientGetname, 2))
        );
        assert_eq!(
            KnownCommand::parse(b"CLIENT GETNAME", Some(b"other")),
            Some((KnownCommand::ClientGetname, 1))
        );
        assert_eq!(
            KnownCommand::parse(b"CLIENT", None),
            Some((KnownCommand::Client, 1))
        );
    }

    #[test]
    fn surrounding_whitespace_is_ignored() {
        assert_eq!(
            KnownCommand::from_parts(b" get ", None),
            Some(KnownCommand::Get)
        );
        assert_eq!(
            KnownCommand::from_parts(b"  client \t getname ", Some(b"ignored")),
            Some(KnownCommand::ClientGetname)
        );
    }

    #[test]
    fn long_or_non_ascii_names_are_unknown() {
        assert_eq!(KnownCommand::from_parts(&[b'A'; 100], None), None);
        assert_eq!(
            KnownCommand::from_parts(b"CLIENT", Some(&[b'A'; 100])),
            None
        );
        assert_eq!(KnownCommand::from_parts("GÉT".as_bytes(), None), None);
        assert_eq!(KnownCommand::from_parts(b"", None), None);
    }
}
