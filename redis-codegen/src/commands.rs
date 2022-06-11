use serde::Deserialize;
use std::collections::{hash_map, HashMap};
use std::fmt;

#[derive(Debug, Deserialize)]
pub struct CommandSet(HashMap<String, CommandDefinition>);
impl CommandSet {
    pub fn into_iter(self) -> hash_map::IntoIter<String, CommandDefinition> {
        self.0.into_iter()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CommandDefinition {
    pub(crate) summary: String,
    pub(crate) since: Version,
    pub(crate) group: CommandGroup,
    pub(crate) complexity: Option<String>,
    pub(crate) deprecated_since: Option<Version>,
    pub(crate) replaced_by: Option<String>,
    #[serde(default)]
    pub(crate) history: Vec<History>,
    #[serde(default)]
    pub(crate) acl_categories: Vec<AclCategory>,
    pub(crate) arity: Arity,
    #[serde(default)]
    pub(crate) key_specs: Vec<CommandKeySpec>,
    #[serde(default)]
    pub(crate) arguments: Vec<CommandArgument>,
    #[serde(default)]
    pub(crate) command_flags: Vec<CommandFlag>,
    #[serde(default)]
    pub(crate) doc_flags: Vec<DocFlag>,
    #[serde(default)]
    pub(crate) hints: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub(crate) struct Arity(i8);

impl Arity {
    fn is_fixed(&self) -> bool {
        self.0 > 0
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CommandGroup {
    Generic,
    String,
    List,
    Set,
    SortedSet,
    Hash,
    Pubsub,
    Transactions,
    Connection,
    Server,
    Scripting,
    Hyperloglog,
    Cluster,
    Sentinel,
    Geo,
    Stream,
    Bitmap,
}

impl fmt::Display for CommandGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandGroup::Generic => write!(f, "Generic"),
            CommandGroup::String => write!(f, "String"),
            CommandGroup::List => write!(f, "List"),
            CommandGroup::Set => write!(f, "Set"),
            CommandGroup::SortedSet => write!(f, "SortedSet"),
            CommandGroup::Hash => write!(f, "Hash"),
            CommandGroup::Pubsub => write!(f, "Pubsub"),
            CommandGroup::Transactions => write!(f, "Transactions"),
            CommandGroup::Connection => write!(f, "Connection"),
            CommandGroup::Server => write!(f, "Server"),
            CommandGroup::Scripting => write!(f, "Scripting"),
            CommandGroup::Hyperloglog => write!(f, "Hyperloglog"),
            CommandGroup::Cluster => write!(f, "Cluster"),
            CommandGroup::Sentinel => write!(f, "Sentinel"),
            CommandGroup::Geo => write!(f, "Geo"),
            CommandGroup::Stream => write!(f, "Stream"),
            CommandGroup::Bitmap => write!(f, "Bitmap"),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CommandFlag {
    /// the command is an administrative command.
    Admin,
    /// DOCS missing, from https://redis.io/docs/reference/modules/modules-api-ref/: Permit the command while the server is blocked either by a script or by a slow module command, see RM_Yield.
    AllowBusy,
    /// the command is allowed even during hash slot migration. This flag is relevant in Redis Cluster deployments.
    Asking,
    /// the command may block the requesting client.
    Blocking,
    /// the command is rejected if the server's memory usage is too high (see the maxmemory configuration directive).
    Denyoom,
    /// the command operates in constant or log(N) time. This flag is used for monitoring latency with the LATENCY command.
    Fast,
    /// the command is allowed while the database is loading.
    Loading,
    /// the first key, last key, and step values don't determine all key positions. Clients need to use COMMAND GETKEYS or key specifications in this case. See below for more details.
    Movablekeys,
    /// executing the command doesn't require authentication.
    NoAuth,
    /// the command is denied during asynchronous loading (that is when a replica uses disk-less SWAPDB SYNC, and allows access to the old dataset).
    NoAsyncLoading,
    /// the command may accept key name arguments, but these aren't mandatory.
    NoMandatoryKeys,
    /// the command isn't allowed inside the context of a transaction.
    NoMulti,
    /// the command can't be called from scripts or functions.
    Noscript,
    /// the command is related to Redis Pub/Sub.
    Pubsub,
    /// the command returns random results, which is a concern with verbatim script replication. As of Redis 7.0, this flag is a command tip.
    Random,
    /// the command doesn't modify data.
    Readonly,
    /// the command's output is sorted when called from a script.
    SortForScript,
    /// the command is not shown in MONITOR's output.
    SkipMonitor,
    /// the command is not shown in SLOWLOG's output. As of Redis 7.0, this flag is a command tip.
    SkipSlowlog,
    /// the command is allowed while a replica has stale data.
    Stale,
    /// the command may modify data.
    Write,
}

impl fmt::Display for CommandFlag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandFlag::Admin => write!(f, "Admin: This command is an administrative command."),
            CommandFlag::AllowBusy => write!(f, "AllowBusy: From https://redis.io/docs/reference/modules/modules-api-ref/: Permit the command while the server is blocked either by a script or by a slow module command, see RM_Yield."), 
            CommandFlag::Asking => write!(f, "Asking: This command is allowed even during hash slot migration. This flag is relevant in Redis Cluster deployments."), 
            CommandFlag::Blocking => write!(f, "Blocking: This command may block the requesting client."), 
            CommandFlag::Denyoom => write!(f, "Denyoom: This command is rejected if the server's memory usage is too high (see the maxmemory configuration directive)."), 
            CommandFlag::Fast => write!(f, "Fast: This command operates in constant or log(N) time. This flag is used for monitoring latency with the LATENCY command."),
            CommandFlag::Loading => write!(f, "Loading: This command is allowed while the database is loading."), 
            CommandFlag::Movablekeys => write!(f, "Movablekeys: This first key, last key, and step values don't determine all key positions. Clients need to use COMMAND GETKEYS or key specifications in this case. See below for more details."), 
            CommandFlag::NoAuth => write!(f, "NoAuth: Thiscuting the command doesn't require authentication."), 
            CommandFlag::NoAsyncLoading => write!(f, "NoAsyncLoading: This command is denied during asynchronous loading (that is when a replica uses disk-less SWAPDB SYNC, and allows access to the old dataset)."), 
            CommandFlag::NoMandatoryKeys => write!(f, "NoMandatoryKeys: This command may accept key name arguments, but these aren't mandatory."), 
            CommandFlag::NoMulti => write!(f, "NoMulti: This command isn't allowed inside the context of a transaction."), 
            CommandFlag::Noscript => write!(f, "Noscript: This command can't be called from scripts or functions."), 
            CommandFlag::Pubsub => write!(f, "Pubsub: This command is related to Redis Pub/Sub."), 
            CommandFlag::Random => write!(f, "Random: This command returns random results, which is a concern with verbatim script replication. As of Redis 7.0, this flag is a command tip."), 
            CommandFlag::Readonly => write!(f, "Readonly: This command doesn't modify data."), 
            CommandFlag::SortForScript => write!(f, "SortForScript: This command's output is sorted when called from a script."), 
            CommandFlag::SkipMonitor => write!(f, "SkipMonitor: This command is not shown in MONITOR's output."), 
            CommandFlag::SkipSlowlog => write!(f, "SkipSlowlog: This command is not shown in SLOWLOG's output. As of Redis 7.0, this flag is a command tip."), 
            CommandFlag::Stale => write!(f, "Stale: This command is allowed while a replica has stale data."), 
            CommandFlag::Write => write!(f, "Write: This command may modify data."),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum AclCategory {
    /// Administrative commands. Normal applications will never need to use these. Includes REPLICAOF, CONFIG, DEBUG, SAVE, MONITOR, ACL, SHUTDOWN, etc.
    #[serde(rename = "@admin")]
    Admin,
    /// Data type: bitmaps related.
    #[serde(rename = "@bitmap")]
    Bitmap,
    /// Potentially blocking the connection until released by another command.
    #[serde(rename = "@blocking")]
    Blocking,
    /// Commands affecting the connection or other connections. This includes AUTH, SELECT, COMMAND, CLIENT, ECHO, PING, etc.
    #[serde(rename = "@connection")]
    Connection,
    /// Potentially dangerous commands (each should be considered with care for various reasons). This includes FLUSHALL, MIGRATE, RESTORE, SORT, KEYS, CLIENT, DEBUG, INFO, CONFIG, SAVE, REPLICAOF, etc.
    #[serde(rename = "@dangerous")]
    Dangerous,
    /// Data type: geospatial indexes related.
    #[serde(rename = "@geo")]
    Geo,
    /// Data type: hashes related.
    #[serde(rename = "@hash")]
    Hash,
    /// Data type: hyperloglog related.
    #[serde(rename = "@hyperloglog")]
    Hyperloglog,
    /// Fast O(1) commands. May loop on the number of arguments, but not the number of elements in the key.
    #[serde(rename = "@fast")]
    Fast,
    /// Writing or reading from keys, databases, or their metadata in a type agnostic way. Includes DEL, RESTORE, DUMP, RENAME, EXISTS, DBSIZE, KEYS, EXPIRE, TTL, FLUSHALL, etc. Commands that may modify the keyspace, key, or metadata will also have the write category. Commands that only read the keyspace, key, or metadata will have the read category.
    #[serde(rename = "@keyspace")]
    Keyspace,
    /// Data type: lists related.
    #[serde(rename = "@list")]
    List,
    /// PubSub-related commands.
    #[serde(rename = "@pubsub")]
    Pubsub,
    /// Reading from keys (values or metadata). Note that commands that don't interact with keys, will not have either read or write.
    #[serde(rename = "@read")]
    Read,
    /// Scripting related.
    #[serde(rename = "@scripting")]
    Scripting,
    /// Data type: sets related.
    #[serde(rename = "@set")]
    Set,
    /// Data type: sorted sets related.
    #[serde(rename = "@sortedset")]
    Sortedset,
    /// All commands that are not fast.
    #[serde(rename = "@slow")]
    Slow,
    /// Data type: streams related.
    #[serde(rename = "@stream")]
    Stream,
    /// Data type: strings related.
    #[serde(rename = "@string")]
    String,
    /// WATCH / MULTI / EXEC related commands.
    #[serde(rename = "@transaction")]
    Transaction,
    /// Writing to keys (values or metadata).
    #[serde(rename = "@write")]
    Write,
}

impl fmt::Display for AclCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AclCategory::Admin => write!(f, "@admin"),
            AclCategory::Bitmap => write!(f, "@bitmap"),
            AclCategory::Blocking => write!(f, "@blocking"),
            AclCategory::Connection => write!(f, "@connection"),
            AclCategory::Dangerous => write!(f, "@dangerous"),
            AclCategory::Geo => write!(f, "@geo"),
            AclCategory::Hash => write!(f, "@hash"),
            AclCategory::Hyperloglog => write!(f, "@hyperloglog"),
            AclCategory::Fast => write!(f, "@fast"),
            AclCategory::Keyspace => write!(f, "@keyspace"),
            AclCategory::List => write!(f, "@list"),
            AclCategory::Pubsub => write!(f, "@pubsub"),
            AclCategory::Read => write!(f, "@read"),
            AclCategory::Scripting => write!(f, "@scripting"),
            AclCategory::Set => write!(f, "@set"),
            AclCategory::Sortedset => write!(f, "@sortedset"),
            AclCategory::Slow => write!(f, "@slow"),
            AclCategory::Stream => write!(f, "@stream"),
            AclCategory::String => write!(f, "@string"),
            AclCategory::Transaction => write!(f, "@transaction"),
            AclCategory::Write => write!(f, "@write"),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum DocFlag {
    Deprecated,
    Syscmd,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct History(Version, String);

#[derive(Debug, Clone, Deserialize)]
/// TODo split into Major.Minor.Patch
pub(crate) struct Version(String);

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CommandKeySpec {
    // TODO type this
    begin_search: serde_json::Value,
    find_keys: serde_json::Value,
    notes: Option<String>,
    // TODO Flags (RW, RO, OW, RM, access, udpdate, instert, delete, not_key, incomplete, viriable_flags
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CommandArgument {
    pub(crate) name: String,
    #[serde(flatten)]
    pub(crate) r#type: ArgType,
    #[serde(default)]
    pub(crate) token: Option<String>,
    #[serde(default)]
    pub(crate) multiple: bool,
    #[serde(default)]
    pub(crate) optional: bool,
}

/// The Argument Type
///
/// Currently only String, Integer, Double, and Key are used to generate code
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub(crate) enum ArgType {
    String,
    Integer,
    Double,
    Key { key_spec_index: u8 },
    Pattern,
    UnixTime,
    PureToken,
    Oneof { arguments: Vec<CommandArgument> },
    Block { arguments: Vec<CommandArgument> },
}
