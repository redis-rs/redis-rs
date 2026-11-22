//! Commands known to this crate.
//!
//! Every command is declared once, in the `known_commands!` invocation below. The macro
//! generates the [`KnownCommand`] enum, its canonical names and the parser from a command's
//! first arguments. The behaviour of each command is declared in exhaustive matches, one per
//! property and without catch-all arms, so adding a command forces a decision for each of them:
//!
//! * `cache`: whether a command only reads data, and how its response is cached.
//! * `key_positions`: where a command's keys are, or how a command without keys is routed.
//!   Cluster routing is derived from it, and client-side caching takes its keys from it.
//! * `routing`: how the responses of a command are aggregated in a cluster, and whether it can
//!   be part of a cluster pipeline.

#[cfg(any(feature = "cluster", feature = "cache-aio"))]
mod cache;
#[cfg(any(feature = "cluster", feature = "cache-aio"))]
mod key_positions;
#[cfg(feature = "cluster")]
mod routing;

#[cfg(feature = "cache-aio")]
pub(crate) use cache::CacheKind;
#[cfg(any(feature = "cluster", feature = "cache-aio"))]
pub(crate) use key_positions::KeyPositions;
#[cfg(feature = "cluster")]
pub(crate) use key_positions::Keyless;

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
            #[cfg(test)]
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
