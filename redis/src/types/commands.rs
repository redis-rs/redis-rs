//! Types that are needed in commands as types for arguments or return values

use super::to_single_arg;
use super::{HashMap, get_inner_value, get_owned_inner_value};
use crate::{
    FromRedisValue, ParsingError, PushKind, RedisWrite, ToRedisArgs, ToSingleRedisArg, Value,
    from_redis_value, from_redis_value_ref,
};
use std::ops::Deref;

/// Helper enum that is used to define expiry time
#[derive(Clone)]
#[non_exhaustive]
pub enum Expiry {
    /// EX seconds -- Set the specified expire time, in seconds.
    EX(u64),
    /// PX milliseconds -- Set the specified expire time, in milliseconds.
    PX(u64),
    /// EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
    EXAT(u64),
    /// PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds.
    PXAT(u64),
    /// PERSIST -- Remove the time to live associated with the key.
    PERSIST,
}

/// Helper enum that is used to define expiry time for SET command
#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum SetExpiry {
    /// EX seconds -- Set the specified expire time, in seconds.
    EX(u64),
    /// PX milliseconds -- Set the specified expire time, in milliseconds.
    PX(u64),
    /// EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
    EXAT(u64),
    /// PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds.
    PXAT(u64),
    /// KEEPTTL -- Retain the time to live associated with the key.
    KEEPTTL,
}

impl ToRedisArgs for SetExpiry {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let mut buf = ::itoa::Buffer::new();
        match self {
            Self::EX(secs) => {
                out.write_arg(b"EX");
                out.write_arg(buf.format(*secs).as_bytes());
            }
            Self::PX(millis) => {
                out.write_arg(b"PX");
                out.write_arg(buf.format(*millis).as_bytes());
            }
            Self::EXAT(unix_time) => {
                out.write_arg(b"EXAT");
                out.write_arg(buf.format(*unix_time).as_bytes());
            }
            Self::PXAT(unix_time) => {
                out.write_arg(b"PXAT");
                out.write_arg(buf.format(*unix_time).as_bytes());
            }
            Self::KEEPTTL => {
                out.write_arg(b"KEEPTTL");
            }
        }
    }

    #[inline]
    fn num_of_args(&self) -> usize {
        match self {
            Self::EX(_) | Self::PX(_) | Self::EXAT(_) | Self::PXAT(_) => 2,
            Self::KEEPTTL => 1,
        }
    }

    #[inline]
    fn args_size(&self) -> usize {
        let mut buf = ::itoa::Buffer::new();
        match self {
            Self::EX(secs) => b"EX".len() + buf.format(*secs).len(),
            Self::PX(millis) => b"PX".len() + buf.format(*millis).len(),
            Self::EXAT(unix_time) => b"EXAT".len() + buf.format(*unix_time).len(),
            Self::PXAT(unix_time) => b"PXAT".len() + buf.format(*unix_time).len(),
            Self::KEEPTTL => b"KEEPTTL".len(),
        }
    }
}

/// Helper enum that is used to define existence checks
#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum ExistenceCheck {
    /// NX -- Only set the key if it does not already exist.
    NX,
    /// XX -- Only set the key if it already exists.
    XX,
}

impl ToRedisArgs for ExistenceCheck {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Self::NX => {
                out.write_arg(b"NX");
            }
            Self::XX => {
                out.write_arg(b"XX");
            }
        }
    }

    #[inline]
    fn num_of_args(&self) -> usize {
        1
    }

    #[inline]
    fn args_size(&self) -> usize {
        2
    }
}

/// Helper enum that is used to define field existence checks
#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum FieldExistenceCheck {
    /// FNX -- Only set the fields if all do not already exist.
    FNX,
    /// FXX -- Only set the fields if all already exist.
    FXX,
}

impl ToRedisArgs for FieldExistenceCheck {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Self::FNX => out.write_arg(b"FNX"),
            Self::FXX => out.write_arg(b"FXX"),
        }
    }

    #[inline]
    fn num_of_args(&self) -> usize {
        1
    }

    #[inline]
    fn args_size(&self) -> usize {
        3
    }
}

/// Helper enum that is used to define comparisons between values and their digests
///
/// # Example
/// ```rust
/// use redis::ValueComparison;
///
/// // Create comparisons using constructor methods
/// let eq_comparison = ValueComparison::ifeq("my_value");
/// let ne_comparison = ValueComparison::ifne("other_value");
/// let deq_comparison = ValueComparison::ifdeq("digest_hash");
/// let dne_comparison = ValueComparison::ifdne("other_digest");
/// ```
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ValueComparison {
    /// Value is equal
    IFEQ(Vec<u8>),
    /// Value is not equal
    IFNE(Vec<u8>),
    /// Value's digest is equal
    IFDEQ(Vec<u8>),
    /// Value's digest is not equal
    IFDNE(Vec<u8>),
}

impl ValueComparison {
    /// Create a new IFEQ (if equal) comparison
    ///
    /// Performs the operation only if the key's current value is equal to the provided value.
    ///
    /// For SET: Sets the key only if its current value matches. Non-existent keys are not created.
    /// For DEL_EX: Deletes the key only if its current value matches. Non-existent keys are ignored.
    pub fn ifeq(value: impl ToSingleRedisArg) -> Self {
        Self::IFEQ(to_single_arg(value))
    }

    /// Create a new IFNE (if not equal) comparison
    ///
    /// Performs the operation only if the key's current value is not equal to the provided value.
    ///
    /// For SET: Sets the key only if its current value doesn't match. Non-existent keys are created.
    /// For DEL_EX: Deletes the key only if its current value doesn't match. Non-existent keys are ignored.
    pub fn ifne(value: impl ToSingleRedisArg) -> Self {
        Self::IFNE(to_single_arg(value))
    }

    /// Create a new IFDEQ (if digest equal) comparison
    ///
    /// Performs the operation only if the digest of the key's current value is equal to the provided digest.
    ///
    /// For SET: Sets the key only if its current value's digest matches. Non-existent keys are not created.
    /// For DEL_EX: Deletes the key only if its current value's digest matches. Non-existent keys are ignored.
    ///
    /// Use [`calculate_value_digest`](super::calculate_value_digest) to compute the digest of a value.
    pub fn ifdeq(digest: impl ToSingleRedisArg) -> Self {
        Self::IFDEQ(to_single_arg(digest))
    }

    /// Create a new IFDNE (if digest not equal) comparison
    ///
    /// Performs the operation only if the digest of the key's current value is not equal to the provided digest.
    ///
    /// For SET: Sets the key only if its current value's digest doesn't match. Non-existent keys are created.
    /// For DEL_EX: Deletes the key only if its current value's digest doesn't match. Non-existent keys are ignored.
    ///
    /// Use [`calculate_value_digest`](super::calculate_value_digest) to compute the digest of a value.
    pub fn ifdne(digest: impl ToSingleRedisArg) -> Self {
        Self::IFDNE(to_single_arg(digest))
    }
}

impl ToRedisArgs for ValueComparison {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Self::IFEQ(value) => {
                out.write_arg(b"IFEQ");
                out.write_arg(value);
            }
            Self::IFNE(value) => {
                out.write_arg(b"IFNE");
                out.write_arg(value);
            }
            Self::IFDEQ(digest) => {
                out.write_arg(b"IFDEQ");
                out.write_arg(digest);
            }
            Self::IFDNE(digest) => {
                out.write_arg(b"IFDNE");
                out.write_arg(digest);
            }
        }
    }

    #[inline]
    fn num_of_args(&self) -> usize {
        2
    }

    #[inline]
    fn args_size(&self) -> usize {
        match self {
            Self::IFEQ(value) => b"IFEQ".len() + value.len(),
            Self::IFNE(value) => b"IFNE".len() + value.len(),
            Self::IFDEQ(value) => b"IFDEQ".len() + value.len(),
            Self::IFDNE(value) => b"IFDNE".len() + value.len(),
        }
    }
}

/// An info dictionary type for `INFO`s response.
///
/// This type provides convenient access to key/value data returned by
/// the `INFO` command.  It acts like a regular mapping but also has
/// a convenience method `get` which can return data in the appropriate
/// type.
///
/// For instance this can be used to query the server for the role it's
/// in (master, slave) etc:
///
/// # Caveats
///
/// As this struct internally uses a [`HashMap`], it only collects the last value for each key, if
/// they occur multiple times. So if a key occurs multiple times (e.g.: `module`), this struct holds
/// only its last value.
///
/// # Examples
///
/// ```rust,no_run
/// # fn do_something() -> redis::RedisResult<()> {
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let mut con = client.get_connection().unwrap();
/// let info : redis::InfoDict = redis::cmd("INFO").query(&mut con)?;
/// let role : Option<String> = info.get("role");
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct InfoDict {
    map: HashMap<String, Value>,
}

impl InfoDict {
    /// Creates a new info dictionary from a string in the response of
    /// the INFO command.  Each line is a key, value pair with the
    /// key and value separated by a colon (`:`).  Lines starting with a
    /// hash (`#`) are ignored.
    pub fn new(kvpairs: &str) -> Self {
        let mut map = HashMap::new();
        for line in kvpairs.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut p = line.splitn(2, ':');
            let (k, v) = match (p.next(), p.next()) {
                (Some(k), Some(v)) => (k.to_string(), v.to_string()),
                _ => continue,
            };
            map.insert(k, Value::SimpleString(v));
        }
        Self { map }
    }

    /// Fetches a value by key and converts it into the given type.
    /// Typical types are `String`, `bool` and integer types.
    pub fn get<T: FromRedisValue>(&self, key: &str) -> Option<T> {
        match self.find(&key) {
            Some(x) => from_redis_value_ref(x).ok(),
            None => None,
        }
    }

    /// Looks up a key in the info dict.
    pub fn find(&self, key: &&str) -> Option<&Value> {
        self.map.get(*key)
    }

    /// Checks if a key is contained in the info dicf.
    pub fn contains_key(&self, key: &&str) -> bool {
        self.find(key).is_some()
    }

    /// Returns the size of the info dict.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Checks if the dict is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Deref for InfoDict {
    type Target = HashMap<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl FromRedisValue for InfoDict {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        let v = get_inner_value(v);
        let s: String = from_redis_value_ref(v)?;
        Ok(Self::new(&s))
    }
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let v = get_owned_inner_value(v);
        let s: String = from_redis_value(v)?;
        Ok(Self::new(&s))
    }
}

/// High level representation of response to the [`ROLE`][1] command.
///
/// [1]: https://redis.io/docs/latest/commands/role/
#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum Role {
    /// Represents a primary role, which is `master` in legacy Redis terminology.
    Primary {
        /// The current primary replication offset
        replication_offset: u64,
        /// List of replica, each represented by a tuple of IP, port and the last acknowledged replication offset.
        replicas: Vec<ReplicaInfo>,
    },
    /// Represents a replica role, which is `slave` in legacy Redis terminology.
    Replica {
        /// The IP of the primary.
        primary_ip: String,
        /// The port of the primary.
        primary_port: u16,
        /// The state of the replication from the point of view of the primary.
        replication_state: String,
        /// The amount of data received from the replica so far in terms of primary replication offset.
        data_received: u64,
    },
    /// Represents a sentinel role.
    Sentinel {
        /// List of primary names monitored by this Sentinel instance.
        primary_names: Vec<String>,
    },
}

impl Role {
    fn new_primary(values: Vec<Value>) -> Result<Self, ParsingError> {
        if values.len() < 3 {
            crate::errors::invalid_type_error!(
                values,
                "Role primary response too short, expected 3 elements"
            );
        }

        let mut values = values.into_iter();
        _ = values.next();

        let replication_offset = from_redis_value(values.next().expect("len was checked"))?;
        let replicas = from_redis_value(values.next().expect("len was checked"))?;

        Ok(Self::Primary {
            replication_offset,
            replicas,
        })
    }

    fn new_replica(values: Vec<Value>) -> Result<Self, ParsingError> {
        if values.len() < 5 {
            crate::errors::invalid_type_error!(
                values,
                "Role replica response too short, expected 5 elements"
            );
        }

        let mut values = values.into_iter();
        _ = values.next();

        let primary_ip = from_redis_value(values.next().expect("len was checked"))?;
        let primary_port = from_redis_value(values.next().expect("len was checked"))?;
        let replication_state = from_redis_value(values.next().expect("len was checked"))?;
        let data_received = from_redis_value(values.next().expect("len was checked"))?;

        Ok(Self::Replica {
            primary_ip,
            primary_port,
            replication_state,
            data_received,
        })
    }

    fn new_sentinel(values: Vec<Value>) -> Result<Self, ParsingError> {
        if values.len() < 2 {
            crate::errors::invalid_type_error!(
                values,
                "Role sentinel response too short, expected at least 2 elements"
            );
        }
        let second_val = values.into_iter().nth(1).expect("len was checked");
        let primary_names = from_redis_value(second_val)?;
        Ok(Self::Sentinel { primary_names })
    }
}

impl FromRedisValue for Role {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        Self::from_redis_value(v.clone())
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let v = match get_owned_inner_value(v).into_sequence() {
            Ok(v) => v,
            Err(v) => crate::errors::invalid_type_error!(v, "Role response should be an array"),
        };
        if v.len() < 2 {
            crate::errors::invalid_type_error!(
                v,
                "Role array is too short, expected at least 2 elements"
            );
        }
        match &v[0] {
            Value::BulkString(role) => match role.as_slice() {
                b"master" => Self::new_primary(v),
                b"slave" => Self::new_replica(v),
                b"sentinel" => Self::new_sentinel(v),
                _ => crate::errors::invalid_type_error!(
                    v,
                    "Role type is not master, slave or sentinel"
                ),
            },
            _ => crate::errors::invalid_type_error!(v, "Role type is not a bulk string"),
        }
    }
}

/// Replication information for a replica, as returned by the [`ROLE`][1] command.
///
/// [1]: https://redis.io/docs/latest/commands/role/
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReplicaInfo {
    /// The IP of the replica.
    pub ip: String,
    /// The port of the replica.
    pub port: u16,
    /// The last acknowledged replication offset.
    pub replication_offset: i64,
}

impl ReplicaInfo {
    /// Builds a new instance
    pub fn new<S: Into<String>>(ip: S, port: u16, replication_offset: i64) -> Self {
        Self {
            ip: ip.into(),
            port,
            replication_offset,
        }
    }
}

impl FromRedisValue for ReplicaInfo {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        Self::from_redis_value(v.clone())
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let v = match get_owned_inner_value(v).into_sequence() {
            Ok(v) => v,
            Err(v) => crate::errors::invalid_type_error!(v, "Replica response should be an array"),
        };
        if v.len() < 3 {
            crate::errors::invalid_type_error!(
                v,
                "Replica array is too short, expected 3 elements"
            );
        }
        let mut v = v.into_iter();
        let ip = from_redis_value(v.next().expect("len was checked"))?;
        let port = from_redis_value(v.next().expect("len was checked"))?;
        let offset = from_redis_value(v.next().expect("len was checked"))?;
        Ok(Self {
            ip,
            port,
            replication_offset: offset,
        })
    }
}

/// Helper enum that is used to define option for the hash expire commands
#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum ExpireOption {
    /// NONE -- Set expiration regardless of the field's current expiration.
    NONE,
    /// NX -- Only set expiration only when the field has no expiration.
    NX,
    /// XX -- Only set expiration only when the field has an existing expiration.
    XX,
    /// GT -- Only set expiration only when the new expiration is greater than current one.
    GT,
    /// LT -- Only set expiration only when the new expiration is less than current one.
    LT,
}

impl ToRedisArgs for ExpireOption {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Self::NX => out.write_arg(b"NX"),
            Self::XX => out.write_arg(b"XX"),
            Self::GT => out.write_arg(b"GT"),
            Self::LT => out.write_arg(b"LT"),
            _ => {}
        }
    }

    #[inline]
    fn num_of_args(&self) -> usize {
        1
    }

    #[inline]
    fn args_size(&self) -> usize {
        match self {
            Self::NONE => b"NONE".len(),
            Self::NX | Self::XX | Self::GT | Self::LT => 2,
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
/// A push message from the server.
pub struct PushInfo {
    /// Push Kind
    pub kind: PushKind,
    /// Data from push message
    pub data: Vec<Value>,
}

impl PushInfo {
    /// Builds a new instance
    pub fn new(kind: PushKind) -> Self {
        Self { kind, data: vec![] }
    }

    /// Sets the message's data
    pub fn data(mut self, data: Vec<Value>) -> Self {
        self.data = data;
        self
    }

    pub(crate) fn disconnect() -> Self {
        Self {
            kind: crate::PushKind::Disconnection,
            data: vec![],
        }
    }
}

pub(crate) type SyncPushSender = std::sync::mpsc::Sender<PushInfo>;

/// Possible types of value held in Redis: [Redis Docs](https://redis.io/docs/latest/commands/type/)
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ValueType {
    /// Key does not have a value
    None,
    /// Generally returned by anything that returns a single element. [Redis Docs](https://redis.io/docs/latest/develop/data-types/strings/)
    String,
    /// A list of String values. [Redis Docs](https://redis.io/docs/latest/develop/data-types/lists/)
    List,
    /// A set of unique String values. [Redis Docs](https://redis.io/docs/latest/develop/data-types/sets/)
    Set,
    /// A sorted set of String values. [Redis Docs](https://redis.io/docs/latest/develop/data-types/sorted-sets/)
    ZSet,
    /// A collection of field-value pairs. [Redis Docs](https://redis.io/docs/latest/develop/data-types/hashes/)
    Hash,
    /// A Redis Stream. [Redis Docs](https://redis.io/docs/latest/develop/data-types/stream)
    Stream,
    /// A vector set. [Redis Docs](https://redis.io/docs/latest/develop/data-types/vector-sets/)
    VectorSet,
    /// A RedisJSON value. [Redis Docs](https://redis.io/docs/latest/develop/data-types/json/)
    JSON,
    /// A Bloom filter from Redis' module. [Redis Docs](https://redis.io/docs/latest/develop/data-types/probabilistic/bloom-filter/)
    BloomFilterRedis,
    /// A Cuckoo filter. [Redis Docs](https://redis.io/docs/latest/develop/data-types/probabilistic/cuckoo-filter/)
    CuckooFilter,
    /// A Count-min. [Redis Docs](https://redis.io/docs/latest/develop/data-types/probabilistic/count-min-sketch/)
    CountMin,
    /// A t-Digest. [Redis Docs](https://redis.io/docs/latest/develop/data-types/probabilistic/t-digest/)
    TDigest,
    /// A Top-K. [Redis Docs](https://redis.io/docs/latest/develop/data-types/probabilistic/top-k/)
    TopK,
    /// A time series. [Redis Docs](https://redis.io/docs/latest/develop/data-types/timeseries/)
    TimeSeries,
    /// A Trie. [Redis Docs](https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/autocomplete/)
    Trie,
    /// A Bloom filter from Valkey's module. [ValKey Docs](https://valkey.io/topics/bloomfilters/)
    BloomFilterValKey,
    /// Any other value type not explicitly defined in [Redis Docs](https://redis.io/docs/latest/commands/type/)
    Unknown(String),
}

impl<T: AsRef<str>> From<T> for ValueType {
    fn from(s: T) -> Self {
        match s.as_ref() {
            "none" => Self::None,
            "string" => Self::String,
            "list" => Self::List,
            "set" => Self::Set,
            "zset" => Self::ZSet,
            "hash" => Self::Hash,
            "stream" => Self::Stream,
            "vectorset" => Self::VectorSet,
            // JSON module
            "ReJSON-RL" => Self::JSON,
            // Bloom module (Redis)
            "CMSk-TYPE" => Self::CountMin,
            "MBbloom--" => Self::BloomFilterRedis,
            "MBbloomCF" => Self::CuckooFilter,
            "TDIS-TYPE" => Self::TDigest,
            "TopK-TYPE" => Self::TopK,
            // Search module
            "trietype0" => Self::Trie,
            // Timeseries module
            "TSDB-TYPE" => Self::TimeSeries,
            // Bloom module (ValKey)
            "bloomfltr" => Self::BloomFilterValKey,
            // Fallback
            s => Self::Unknown(s.to_string()),
        }
    }
}

impl From<ValueType> for String {
    fn from(v: ValueType) -> Self {
        <&ValueType as Into<&str>>::into(&v).to_string()
    }
}

impl<'a> From<&'a ValueType> for &'a str {
    fn from(v: &'a ValueType) -> &'a str {
        match v {
            ValueType::None => "none",
            ValueType::String => "string",
            ValueType::List => "list",
            ValueType::Set => "set",
            ValueType::ZSet => "zset",
            ValueType::Hash => "hash",
            ValueType::Stream => "stream",
            ValueType::VectorSet => "vectorset",
            // JSON module
            ValueType::JSON => "ReJSON-RL",
            // Bloom module (Redis)
            ValueType::BloomFilterRedis => "MBbloom--",
            ValueType::CuckooFilter => "MBbloomCF",
            ValueType::TDigest => "TDIS-TYPE",
            ValueType::TopK => "TopK-TYPE",
            ValueType::CountMin => "CMSk-TYPE",
            // Search module
            ValueType::Trie => "trietype0",
            // Timeseries module
            ValueType::TimeSeries => "TSDB-TYPE",
            // Bloom module (ValKey)
            ValueType::BloomFilterValKey => "bloomfltr",
            // Fallback
            ValueType::Unknown(s) => s.as_str(),
        }
    }
}

impl FromRedisValue for ValueType {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        match v {
            Value::SimpleString(s) => Ok(s.into()),
            _ => crate::errors::invalid_type_error!(v, "Value type should be a simple string"),
        }
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        match v {
            Value::SimpleString(s) => Ok(s.into()),
            _ => crate::errors::invalid_type_error!(v, "Value type should be a simple string"),
        }
    }
}

impl ToRedisArgs for ValueType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let as_str = <&Self as Into<&str>>::into(self);
        out.write_arg(as_str.as_bytes());
    }

    #[inline]
    fn num_of_args(&self) -> usize {
        1
    }

    #[inline]
    fn args_size(&self) -> usize {
        let as_str = <&Self as Into<&str>>::into(self);
        as_str.len()
    }
}

/// Returned by typed commands which either return a positive integer or some negative integer indicating some kind of no-op.
#[derive(Debug, PartialEq, Clone)]
#[non_exhaustive]
pub enum IntegerReplyOrNoOp {
    /// A positive integer reply indicating success of some kind.
    IntegerReply(usize),
    /// The field/key you are trying to operate on does not exist.
    NotExists,
    /// The field/key you are trying to operate on exists but is not of the correct type or does not have some property you are trying to affect.
    ExistsButNotRelevant,
}

impl IntegerReplyOrNoOp {
    /// Returns the integer value of the reply.
    pub fn raw(&self) -> isize {
        match self {
            Self::IntegerReply(s) => *s as isize,
            Self::NotExists => -2,
            Self::ExistsButNotRelevant => -1,
        }
    }
}

impl FromRedisValue for IntegerReplyOrNoOp {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        match v {
            Value::Int(s) => match s {
                -2 => Ok(Self::NotExists),
                -1 => Ok(Self::ExistsButNotRelevant),
                _ => Ok(Self::IntegerReply(*s as usize)),
            },
            _ => crate::errors::invalid_type_error!(v, "Value should be an integer"),
        }
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        match v {
            Value::Int(s) => match s {
                -2 => Ok(Self::NotExists),
                -1 => Ok(Self::ExistsButNotRelevant),
                _ => Ok(Self::IntegerReply(s as usize)),
            },
            _ => crate::errors::invalid_type_error!(v, "Value should be an integer"),
        }
    }
}

impl PartialEq<isize> for IntegerReplyOrNoOp {
    fn eq(&self, other: &isize) -> bool {
        match self {
            Self::IntegerReply(s) => *s as isize == *other,
            Self::NotExists => *other == -2,
            Self::ExistsButNotRelevant => *other == -1,
        }
    }
}

impl PartialEq<usize> for IntegerReplyOrNoOp {
    fn eq(&self, other: &usize) -> bool {
        match self {
            Self::IntegerReply(s) => *s == *other,
            _ => false,
        }
    }
}

impl PartialEq<i32> for IntegerReplyOrNoOp {
    fn eq(&self, other: &i32) -> bool {
        match self {
            Self::IntegerReply(s) => *s as i32 == *other,
            Self::NotExists => *other == -2,
            Self::ExistsButNotRelevant => *other == -1,
        }
    }
}

impl PartialEq<u32> for IntegerReplyOrNoOp {
    fn eq(&self, other: &u32) -> bool {
        match self {
            Self::IntegerReply(s) => *s as u32 == *other,
            _ => false,
        }
    }
}

/// The two-element reply of the [INCREX](https://redis.io/commands/increx) command.
///
/// Each field holds the raw [`Value`] returned by the server.
/// For `BYINT` operations this is an integer, while for `BYFLOAT` it is a bulk string (RESP2) or double (RESP3).
/// Decode a field into a concrete type with [`value_as`](Self::value_as) / [`actual_increment_as`](Self::actual_increment_as)
/// - e.g. `i64` for `BYINT` or `f64` (or a wider type such as `bigdecimal::BigDecimal`) for `BYFLOAT`.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct IncrexResult {
    /// The key's value after the increment.
    pub value: Value,
    /// The increment that was actually applied.
    ///
    /// This is `0` when the default policy (when `SATURATE` is not set) rejected an out-of-bounds operation.
    /// In that case `value` holds the unchanged current value and the TTL is left untouched.
    /// When `SATURATE` is set, it clamps the result to a bound.
    /// This reflects the clamped delta, which may differ from the requested increment.
    pub actual_increment: Value,
}

impl IncrexResult {
    /// Decode [`value`](Self::value) into the desired type.
    /// - e.g. `i64` for `BYINT` operations and `f64` or a wider type for `BYFLOAT` operations.
    pub fn value_as<T: FromRedisValue>(&self) -> Result<T, ParsingError> {
        T::from_redis_value_ref(&self.value)
    }

    /// Decode [`actual_increment`](Self::actual_increment) into the desired type.
    /// - e.g. `i64` for `BYINT` operations and `f64` or a wider type for `BYFLOAT` operations.
    pub fn actual_increment_as<T: FromRedisValue>(&self) -> Result<T, ParsingError> {
        T::from_redis_value_ref(&self.actual_increment)
    }

    /// Decode both fields as `i64`, the natural type for a `BYINT` result, returning `(value, actual_increment)`.
    pub fn as_i64(&self) -> Result<(i64, i64), ParsingError> {
        Ok((self.value_as()?, self.actual_increment_as()?))
    }

    /// Decode both fields as `f64`, the natural type for a `BYFLOAT` result, returning `(value, actual_increment)`.
    pub fn as_f64(&self) -> Result<(f64, f64), ParsingError> {
        Ok((self.value_as()?, self.actual_increment_as()?))
    }
}

impl FromRedisValue for IncrexResult {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let [value, actual_increment] = <[Value; 2]>::from_redis_value(v)?;
        Ok(Self {
            value,
            actual_increment,
        })
    }
}
