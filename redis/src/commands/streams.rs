//! Defines types to use with the streams commands.

#[cfg(feature = "streams")]
use crate::{
    FromRedisValue, RedisWrite, ToRedisArgs, Value,
    errors::{ParsingError, invalid_type_error},
    types::HashMap,
};
use crate::{from_redis_value, from_redis_value_ref, types::ToSingleRedisArg};

// Stream Maxlen Enum

/// Utility enum for passing `MAXLEN [= or ~] [COUNT]`
/// arguments into `StreamCommands`.
/// The enum value represents the count.
#[derive(PartialEq, Eq, Clone, Debug, Copy)]
#[non_exhaustive]
pub enum StreamMaxlen {
    /// Match an exact count
    Equals(usize),
    /// Match an approximate count
    Approx(usize),
}

impl ToRedisArgs for StreamMaxlen {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let (ch, val) = match *self {
            StreamMaxlen::Equals(v) => ("=", v),
            StreamMaxlen::Approx(v) => ("~", v),
        };
        out.write_arg(b"MAXLEN");
        out.write_arg(ch.as_bytes());
        val.write_redis_args(out);
    }
}

/// Utility enum for passing the trim mode`[=|~]`
/// arguments into `StreamCommands`.
#[derive(Debug)]
#[non_exhaustive]
pub enum StreamTrimmingMode {
    /// Match an exact count
    Exact,
    /// Match an approximate count
    Approx,
}

impl ToRedisArgs for StreamTrimmingMode {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Self::Exact => out.write_arg(b"="),
            Self::Approx => out.write_arg(b"~"),
        };
    }
}

/// Utility enum for passing `<MAXLEN|MINID> [=|~] threshold [LIMIT count]`
/// arguments into `StreamCommands`.
/// The enum values the trimming mode (=|~), the threshold, and the optional limit
#[derive(Debug)]
#[non_exhaustive]
pub enum StreamTrimStrategy {
    /// Evicts entries as long as the streams length exceeds threshold.  With an optional limit.
    MaxLen(StreamTrimmingMode, usize, Option<usize>),
    /// Evicts entries with IDs lower than threshold, where threshold is a stream ID With an optional limit.
    MinId(StreamTrimmingMode, String, Option<usize>),
}

impl StreamTrimStrategy {
    /// Define a MAXLEN trim strategy with the given maximum number of entries
    pub fn maxlen(trim: StreamTrimmingMode, max_entries: usize) -> Self {
        Self::MaxLen(trim, max_entries, None)
    }

    /// Defines a MINID trim strategy with the given minimum stream ID
    pub fn minid(trim: StreamTrimmingMode, stream_id: impl Into<String>) -> Self {
        Self::MinId(trim, stream_id.into(), None)
    }

    /// Set a limit to the number of records to trim in a single operation
    pub fn limit(self, limit: usize) -> Self {
        match self {
            StreamTrimStrategy::MaxLen(m, t, _) => StreamTrimStrategy::MaxLen(m, t, Some(limit)),
            StreamTrimStrategy::MinId(m, t, _) => StreamTrimStrategy::MinId(m, t, Some(limit)),
        }
    }
}

impl ToRedisArgs for StreamTrimStrategy {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let limit = match self {
            StreamTrimStrategy::MaxLen(m, t, limit) => {
                out.write_arg(b"MAXLEN");
                m.write_redis_args(out);
                t.write_redis_args(out);
                limit
            }
            StreamTrimStrategy::MinId(m, t, limit) => {
                out.write_arg(b"MINID");
                m.write_redis_args(out);
                t.write_redis_args(out);
                limit
            }
        };
        if let Some(limit) = limit {
            out.write_arg(b"LIMIT");
            limit.write_redis_args(out);
        }
    }
}

/// Builder options for [`xtrim_options`] command
///
/// [`xtrim_options`]: ../trait.Commands.html#method.xtrim_options
///
#[derive(Debug)]
pub struct StreamTrimOptions {
    strategy: StreamTrimStrategy,
    deletion_policy: Option<StreamDeletionPolicy>,
}

impl StreamTrimOptions {
    /// Define a MAXLEN trim strategy with the given maximum number of entries
    pub fn maxlen(mode: StreamTrimmingMode, max_entries: usize) -> Self {
        Self {
            strategy: StreamTrimStrategy::maxlen(mode, max_entries),
            deletion_policy: None,
        }
    }

    /// Defines a MINID trim strategy with the given minimum stream ID
    pub fn minid(mode: StreamTrimmingMode, stream_id: impl Into<String>) -> Self {
        Self {
            strategy: StreamTrimStrategy::minid(mode, stream_id),
            deletion_policy: None,
        }
    }

    /// Set a limit to the number of records to trim in a single operation
    pub fn limit(mut self, limit: usize) -> Self {
        self.strategy = self.strategy.limit(limit);
        self
    }

    /// Set the deletion policy for the XTRIM operation
    pub fn set_deletion_policy(mut self, deletion_policy: StreamDeletionPolicy) -> Self {
        self.deletion_policy = Some(deletion_policy);
        self
    }
}

impl ToRedisArgs for StreamTrimOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        self.strategy.write_redis_args(out);
        if let Some(deletion_policy) = self.deletion_policy.as_ref() {
            deletion_policy.write_redis_args(out);
        }
    }
}

/// Builder options for [`xadd_options`] command
///
/// [`xadd_options`]: ../trait.Commands.html#method.xadd_options
///
#[derive(Default, Debug)]
pub struct StreamAddOptions {
    nomkstream: bool,
    trim: Option<StreamTrimStrategy>,
    deletion_policy: Option<StreamDeletionPolicy>,
}

impl StreamAddOptions {
    /// Set the NOMKSTREAM flag on which prevents creating a stream for the XADD operation
    pub fn nomkstream(mut self) -> Self {
        self.nomkstream = true;
        self
    }

    /// Enable trimming when adding using the given trim strategy
    pub fn trim(mut self, trim: StreamTrimStrategy) -> Self {
        self.trim = Some(trim);
        self
    }

    /// Set the deletion policy for the XADD operation
    pub fn set_deletion_policy(mut self, deletion_policy: StreamDeletionPolicy) -> Self {
        self.deletion_policy = Some(deletion_policy);
        self
    }
}

impl ToRedisArgs for StreamAddOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if self.nomkstream {
            out.write_arg(b"NOMKSTREAM");
        }
        if let Some(strategy) = self.trim.as_ref() {
            strategy.write_redis_args(out);
        }
        if let Some(deletion_policy) = self.deletion_policy.as_ref() {
            deletion_policy.write_redis_args(out);
        }
    }
}

/// Builder options for [`xautoclaim_options`] command.
///
/// [`xautoclaim_options`]: ../trait.Commands.html#method.xautoclaim_options
///
#[derive(Default, Debug)]
pub struct StreamAutoClaimOptions {
    count: Option<usize>,
    justid: bool,
}

impl StreamAutoClaimOptions {
    /// Sets the maximum number of elements to claim per stream.
    pub fn count(mut self, n: usize) -> Self {
        self.count = Some(n);
        self
    }

    /// Set `JUSTID` cmd arg to true. Be advised: the response
    /// type changes with this option.
    pub fn with_justid(mut self) -> Self {
        self.justid = true;
        self
    }
}

impl ToRedisArgs for StreamAutoClaimOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref count) = self.count {
            out.write_arg(b"COUNT");
            out.write_arg(format!("{count}").as_bytes());
        }
        if self.justid {
            out.write_arg(b"JUSTID");
        }
    }
}

/// Builder options for [`xclaim_options`] command.
///
/// [`xclaim_options`]: ../trait.Commands.html#method.xclaim_options
///
#[derive(Default, Debug)]
pub struct StreamClaimOptions {
    /// Set `IDLE <milliseconds>` cmd arg.
    idle: Option<usize>,
    /// Set `TIME <Unix epoch milliseconds>` cmd arg.
    time: Option<usize>,
    /// Set `RETRYCOUNT <count>` cmd arg.
    retry: Option<usize>,
    /// Set `FORCE` cmd arg.
    force: bool,
    /// Set `JUSTID` cmd arg. Be advised: the response
    /// type changes with this option.
    justid: bool,
    /// Set `LASTID <lastid>` cmd arg.
    lastid: Option<String>,
}

impl StreamClaimOptions {
    /// Set `IDLE <milliseconds>` cmd arg.
    pub fn idle(mut self, ms: usize) -> Self {
        self.idle = Some(ms);
        self
    }

    /// Set `TIME <Unix epoch milliseconds>` cmd arg.
    pub fn time(mut self, ms_time: usize) -> Self {
        self.time = Some(ms_time);
        self
    }

    /// Set `RETRYCOUNT <count>` cmd arg.
    pub fn retry(mut self, count: usize) -> Self {
        self.retry = Some(count);
        self
    }

    /// Set `FORCE` cmd arg to true.
    pub fn with_force(mut self) -> Self {
        self.force = true;
        self
    }

    /// Set `JUSTID` cmd arg to true. Be advised: the response
    /// type changes with this option.
    pub fn with_justid(mut self) -> Self {
        self.justid = true;
        self
    }

    /// Set `LASTID <lastid>` cmd arg.
    pub fn with_lastid(mut self, lastid: impl Into<String>) -> Self {
        self.lastid = Some(lastid.into());
        self
    }
}

impl ToRedisArgs for StreamClaimOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref ms) = self.idle {
            out.write_arg(b"IDLE");
            out.write_arg(format!("{ms}").as_bytes());
        }
        if let Some(ref ms_time) = self.time {
            out.write_arg(b"TIME");
            out.write_arg(format!("{ms_time}").as_bytes());
        }
        if let Some(ref count) = self.retry {
            out.write_arg(b"RETRYCOUNT");
            out.write_arg(format!("{count}").as_bytes());
        }
        if self.force {
            out.write_arg(b"FORCE");
        }
        if self.justid {
            out.write_arg(b"JUSTID");
        }
        if let Some(ref lastid) = self.lastid {
            out.write_arg(b"LASTID");
            lastid.write_redis_args(out);
        }
    }
}

/// Argument to `StreamReadOptions`
/// Represents the Redis `GROUP <groupname> <consumername>` cmd arg.
/// This option will toggle the cmd from `XREAD` to `XREADGROUP`
type SRGroup = Option<(Vec<Vec<u8>>, Vec<Vec<u8>>)>;
/// Builder options for [`xread_options`] command.
///
/// [`xread_options`]: ../trait.Commands.html#method.xread_options
///
#[derive(Default, Debug)]
pub struct StreamReadOptions {
    /// Set the `BLOCK <milliseconds>` cmd arg.
    block: Option<usize>,
    /// Set the `COUNT <count>` cmd arg.
    count: Option<usize>,
    /// Set the `NOACK` cmd arg.
    noack: Option<bool>,
    /// Set the `GROUP <groupname> <consumername>` cmd arg.
    /// This option will toggle the cmd from XREAD to XREADGROUP.
    group: SRGroup,
    /// Set the `CLAIM <min-idle-time>` cmd arg.
    /// The `<min-idle-time>` is specified in milliseconds.
    claim: Option<usize>,
}

impl StreamReadOptions {
    /// Indicates whether the command is participating in a group
    /// and generating ACKs
    pub fn read_only(&self) -> bool {
        self.group.is_none()
    }

    /// Sets the command so that it avoids adding the message
    /// to the PEL in cases where reliability is not a requirement
    /// and the occasional message loss is acceptable.
    pub fn noack(mut self) -> Self {
        self.noack = Some(true);
        self
    }

    /// Sets the block time in milliseconds.
    pub fn block(mut self, ms: usize) -> Self {
        self.block = Some(ms);
        self
    }

    /// Sets the maximum number of elements to return per stream.
    pub fn count(mut self, n: usize) -> Self {
        self.count = Some(n);
        self
    }

    /// Sets the name of a consumer group associated to the stream.
    pub fn group<GN: ToRedisArgs, CN: ToRedisArgs>(
        mut self,
        group_name: GN,
        consumer_name: CN,
    ) -> Self {
        self.group = Some((
            ToRedisArgs::to_redis_args(&group_name),
            ToRedisArgs::to_redis_args(&consumer_name),
        ));
        self
    }

    /// Set the minimum idle time for the CLAIM parameter.
    pub fn claim(mut self, min_idle_time: usize) -> Self {
        self.claim = Some(min_idle_time);
        self
    }
}

impl ToRedisArgs for StreamReadOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref group) = self.group {
            out.write_arg(b"GROUP");
            for i in &group.0 {
                out.write_arg(i);
            }
            for i in &group.1 {
                out.write_arg(i);
            }
        }

        if let Some(ref ms) = self.block {
            out.write_arg(b"BLOCK");
            out.write_arg(format!("{ms}").as_bytes());
        }

        if let Some(ref n) = self.count {
            out.write_arg(b"COUNT");
            out.write_arg(format!("{n}").as_bytes());
        }

        if self.group.is_some() {
            // noack is only available w/ xreadgroup
            if self.noack == Some(true) {
                out.write_arg(b"NOACK");
            }
            // claim is only available w/ xreadgroup
            if let Some(ref min_idle_time) = self.claim {
                out.write_arg(b"CLAIM");
                out.write_arg(format!("{min_idle_time}").as_bytes());
            }
        }
    }
}

/// Reply type used with the [`xautoclaim_options`] command.
///
/// [`xautoclaim_options`]: ../trait.Commands.html#method.xautoclaim_options
///
#[derive(Default, Debug, Clone)]
pub struct StreamAutoClaimReply {
    /// The next stream id to use as the start argument for the next xautoclaim
    pub next_stream_id: String,
    /// The entries claimed for the consumer. When JUSTID is enabled the map in each entry is blank
    pub claimed: Vec<StreamId>,
    /// The list of stream ids that were removed due to no longer being in the stream
    pub deleted_ids: Vec<String>,
    /// If set, this means that the reply contained invalid nil entries, that were skipped during parsing.
    ///
    /// This should only happen when using Redis 6, see <https://github.com/redis-rs/redis-rs/issues/1798>
    pub invalid_entries: bool,
}

/// Reply type used with [`xread`] or [`xread_options`] commands.
///
/// [`xread`]: ../trait.Commands.html#method.xread
/// [`xread_options`]: ../trait.Commands.html#method.xread_options
///
#[derive(Default, Debug, Clone)]
pub struct StreamReadReply {
    /// Complex data structure containing a payload for each key in this array
    pub keys: Vec<StreamKey>,
}

/// Reply type used with [`xrange`], [`xrange_count`], [`xrange_all`], [`xrevrange`], [`xrevrange_count`], [`xrevrange_all`] commands.
///
/// Represents stream entries matching a given range of `id`'s.
///
/// [`xrange`]: ../trait.Commands.html#method.xrange
/// [`xrange_count`]: ../trait.Commands.html#method.xrange_count
/// [`xrange_all`]: ../trait.Commands.html#method.xrange_all
/// [`xrevrange`]: ../trait.Commands.html#method.xrevrange
/// [`xrevrange_count`]: ../trait.Commands.html#method.xrevrange_count
/// [`xrevrange_all`]: ../trait.Commands.html#method.xrevrange_all
///
#[derive(Default, Debug, Clone)]
pub struct StreamRangeReply {
    /// Complex data structure containing a payload for each ID in this array
    pub ids: Vec<StreamId>,
}

/// Reply type used with [`xclaim`] command.
///
/// Represents that ownership of the specified messages was changed.
///
/// [`xclaim`]: ../trait.Commands.html#method.xclaim
///
#[derive(Default, Debug, Clone)]
pub struct StreamClaimReply {
    /// Complex data structure containing a payload for each ID in this array
    pub ids: Vec<StreamId>,
}

/// Reply type used with [`xpending`] command.
///
/// Data returned here were fetched from the stream without
/// having been acknowledged.
///
/// [`xpending`]: ../trait.Commands.html#method.xpending
///
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum StreamPendingReply {
    /// The stream is empty.
    #[default]
    Empty,
    /// Data with payload exists in the stream.
    Data(StreamPendingData),
}

impl StreamPendingReply {
    /// Returns how many records are in the reply.
    pub fn count(&self) -> usize {
        match self {
            StreamPendingReply::Empty => 0,
            StreamPendingReply::Data(x) => x.count,
        }
    }
}

/// Inner reply type when an [`xpending`] command has data.
///
/// [`xpending`]: ../trait.Commands.html#method.xpending
#[derive(Default, Debug, Clone)]
pub struct StreamPendingData {
    /// Limit on the number of messages to return per call.
    pub count: usize,
    /// ID for the first pending record.
    pub start_id: String,
    /// ID for the final pending record.
    pub end_id: String,
    /// Every consumer in the consumer group with at
    /// least one pending message,
    /// and the number of pending messages it has.
    pub consumers: Vec<StreamInfoConsumer>,
}

/// Reply type used with [`xpending_count`] and
/// [`xpending_consumer_count`] commands.
///
/// Data returned here have been fetched from the stream without
/// any acknowledgement.
///
/// [`xpending_count`]: ../trait.Commands.html#method.xpending_count
/// [`xpending_consumer_count`]: ../trait.Commands.html#method.xpending_consumer_count
///
#[derive(Default, Debug, Clone)]
pub struct StreamPendingCountReply {
    /// An array of structs containing information about
    /// message IDs yet to be acknowledged by various consumers,
    /// time since last ack, and total number of acks by that consumer.
    pub ids: Vec<StreamPendingId>,
}

/// Reply type used with [`xinfo_stream`] command, containing
/// general information about the stream stored at the specified key.
///
/// The very first and last IDs in the stream are shown,
/// in order to give some sense about what is the stream content.
///
/// [`xinfo_stream`]: ../trait.Commands.html#method.xinfo_stream
///
#[derive(Default, Debug, Clone)]
pub struct StreamInfoStreamReply {
    /// The last generated ID that may not be the same as the last
    /// entry ID in case some entry was deleted.
    pub last_generated_id: String,
    /// Details about the radix tree representing the stream mostly
    /// useful for optimization and debugging tasks.
    pub radix_tree_keys: usize,
    /// The number of consumer groups associated with the stream.
    pub groups: usize,
    /// Number of elements of the stream.
    pub length: usize,
    /// The very first entry in the stream.
    pub first_entry: StreamId,
    /// The very last entry in the stream.
    pub last_entry: StreamId,
}

/// Reply type used with [`xinfo_consumer`] command, an array of every
/// consumer in a specific consumer group.
///
/// [`xinfo_consumer`]: ../trait.Commands.html#method.xinfo_consumer
///
#[derive(Default, Debug, Clone)]
pub struct StreamInfoConsumersReply {
    /// An array of every consumer in a specific consumer group.
    pub consumers: Vec<StreamInfoConsumer>,
}

/// Reply type used with [`xinfo_groups`] command.
///
/// This output represents all the consumer groups associated with
/// the stream.
///
/// [`xinfo_groups`]: ../trait.Commands.html#method.xinfo_groups
///
#[derive(Default, Debug, Clone)]
pub struct StreamInfoGroupsReply {
    /// All the consumer groups associated with the stream.
    pub groups: Vec<StreamInfoGroup>,
}

/// A consumer parsed from [`xinfo_consumers`] command.
///
/// [`xinfo_consumers`]: ../trait.Commands.html#method.xinfo_consumers
///
#[derive(Default, Debug, Clone)]
pub struct StreamInfoConsumer {
    /// Name of the consumer group.
    pub name: String,
    /// Number of pending messages for this specific consumer.
    pub pending: usize,
    /// This consumer's idle time in milliseconds.
    pub idle: usize,
}

/// A group parsed from [`xinfo_groups`] command.
///
/// [`xinfo_groups`]: ../trait.Commands.html#method.xinfo_groups
///
#[derive(Default, Debug, Clone)]
pub struct StreamInfoGroup {
    /// The group name.
    pub name: String,
    /// Number of consumers known in the group.
    pub consumers: usize,
    /// Number of pending messages (delivered but not yet acknowledged) in the group.
    pub pending: usize,
    /// Last ID delivered to this group.
    pub last_delivered_id: String,
    /// The logical "read counter" of the last entry delivered to group's consumers
    /// (or `None` if the server does not provide the value).
    pub entries_read: Option<usize>,
    /// The number of entries in the stream that are still waiting to be delivered to the
    /// group's consumers, or a `None` when that number can't be determined.
    pub lag: Option<usize>,
}

/// Represents a pending message parsed from [`xpending`] methods.
///
/// [`xpending`]: ../trait.Commands.html#method.xpending
#[derive(Default, Debug, Clone)]
pub struct StreamPendingId {
    /// The ID of the message.
    pub id: String,
    /// The name of the consumer that fetched the message and has
    /// still to acknowledge it. We call it the current owner
    /// of the message.
    pub consumer: String,
    /// The number of milliseconds that elapsed since the
    /// last time this message was delivered to this consumer.
    pub last_delivered_ms: usize,
    /// The number of times this message was delivered.
    pub times_delivered: usize,
}

/// Represents a stream `key` and its `id`'s parsed from `xread` methods.
#[derive(Default, Debug, Clone)]
pub struct StreamKey {
    /// The stream `key`.
    pub key: String,
    /// The parsed stream `id`'s.
    pub ids: Vec<StreamId>,
}

/// Represents a stream `id` and its field/values as a `HashMap`
/// Also contains optional PEL information if the message was fetched with XREADGROUP with a `claim` option
#[derive(Default, Debug, Clone, PartialEq)]
pub struct StreamId {
    /// The stream `id` (entry ID) of this particular message.
    pub id: String,
    /// All fields in this message, associated with their respective values.
    pub map: HashMap<String, Value>,
    /// The number of milliseconds that elapsed since the last time this entry was delivered to a consumer.
    pub milliseconds_elapsed_from_delivery: Option<usize>,
    /// The number of times this entry was delivered.
    pub delivered_count: Option<usize>,
}

impl StreamId {
    /// Converts a `Value::Array` into a `StreamId`.
    fn from_array_value(v: Value) -> Result<Self, ParsingError> {
        let mut stream_id = StreamId::default();
        if let Value::Array(mut values) = v {
            if let Some(v) = values.first_mut() {
                stream_id.id = from_redis_value(std::mem::take(v))?;
            }
            if let Some(v) = values.first_mut() {
                stream_id.map = from_redis_value(std::mem::take(v))?;
            }
        }

        Ok(stream_id)
    }

    /// Fetches value of a given field and converts it to the specified
    /// type.
    pub fn get<T: FromRedisValue>(&self, key: &str) -> Option<T> {
        match self.map.get(key) {
            Some(x) => from_redis_value_ref(x).ok(),
            None => None,
        }
    }

    /// Does the message contain a particular field?
    pub fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    /// Returns how many field/value pairs exist in this message.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns true if there are no field/value pairs in this message.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

type SACRows = Vec<HashMap<String, HashMap<String, Value>>>;

impl FromRedisValue for StreamAutoClaimReply {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let Value::Array(mut items) = v else {
            invalid_type_error!("Not a array response", v);
        };

        if items.len() > 3 || items.len() < 2 {
            invalid_type_error!("Incorrect number of items", &items);
        }

        let deleted_ids = if items.len() == 3 {
            from_redis_value(items.pop().unwrap())?
        } else {
            Vec::new()
        };
        // safe, because we've checked for length beforehand
        let claimed = items.pop().unwrap();
        let next_stream_id = from_redis_value(items.pop().unwrap())?;

        let Value::Array(arr) = &claimed else {
            invalid_type_error!("Incorrect type", claimed)
        };
        let Some(entry) = arr.iter().find(|val| !matches!(val, Value::Nil)) else {
            return Ok(Self {
                next_stream_id,
                claimed: Vec::new(),
                deleted_ids,
                invalid_entries: !arr.is_empty(),
            });
        };
        let (claimed, invalid_entries) = match entry {
            Value::BulkString(_) => {
                // JUSTID response
                let claimed_count = arr.len();
                let ids: Vec<Option<String>> = from_redis_value(claimed)?;

                let claimed: Vec<_> = ids
                    .into_iter()
                    .filter_map(|id| {
                        id.map(|id| StreamId {
                            id,
                            ..Default::default()
                        })
                    })
                    .collect();
                // This means that some nil entries were filtered
                let invalid_entries = claimed.len() < claimed_count;
                (claimed, invalid_entries)
            }
            Value::Array(_) => {
                // full response
                let claimed_count = arr.len();
                let rows: SACRows = from_redis_value(claimed)?;

                let claimed: Vec<_> = rows
                    .into_iter()
                    .flat_map(|row| {
                        row.into_iter().map(|(id, map)| StreamId {
                            id,
                            map,
                            milliseconds_elapsed_from_delivery: None,
                            delivered_count: None,
                        })
                    })
                    .collect();
                // This means that some nil entries were filtered
                let invalid_entries = claimed.len() < claimed_count;
                (claimed, invalid_entries)
            }
            _ => invalid_type_error!("Incorrect type", claimed),
        };

        Ok(Self {
            next_stream_id,
            claimed,
            deleted_ids,
            invalid_entries,
        })
    }
}

type SRRows = Vec<HashMap<String, Vec<HashMap<String, HashMap<String, Value>>>>>;
type SRClaimRows =
    Vec<HashMap<String, Vec<(String, HashMap<String, Value>, Option<usize>, Option<usize>)>>>;

impl FromRedisValue for StreamReadReply {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        // Try to parse as the standard format first
        if let Ok(rows) = from_redis_value::<SRRows>(v.clone()) {
            return Ok(Self::from_standard_rows(rows));
        }

        // If that fails, try to parse as XREADGROUP with CLAIM format
        // Format: [[stream_name, [[id, [field, value, ...], ms_elapsed, delivery_count], ...]]]
        if let Ok(rows) = from_redis_value::<SRClaimRows>(v.clone()) {
            return Ok(Self::from_claim_rows(rows));
        }

        invalid_type_error!("Could not parse StreamReadReply in any known format", v)
    }
}

impl StreamReadReply {
    fn from_standard_rows(rows: SRRows) -> Self {
        let keys = rows
            .into_iter()
            .flat_map(|row| {
                row.into_iter().map(|(key, entries)| StreamKey {
                    key,
                    ids: entries
                        .into_iter()
                        .flat_map(|id_row| {
                            id_row.into_iter().map(|(id, map)| StreamId {
                                id,
                                map,
                                milliseconds_elapsed_from_delivery: None,
                                delivered_count: None,
                            })
                        })
                        .collect(),
                })
            })
            .collect();
        StreamReadReply { keys }
    }

    fn from_claim_rows(rows: SRClaimRows) -> Self {
        let keys = rows
            .into_iter()
            .flat_map(|row| {
                row.into_iter().map(|(key, entries)| StreamKey {
                    key,
                    ids: entries
                        .into_iter()
                        .map(
                            |(id, map, milliseconds_elapsed_from_delivery, delivered_count)| {
                                StreamId {
                                    id,
                                    map,
                                    milliseconds_elapsed_from_delivery,
                                    delivered_count,
                                }
                            },
                        )
                        .collect(),
                })
            })
            .collect();
        StreamReadReply { keys }
    }
}

impl FromRedisValue for StreamRangeReply {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let rows: Vec<HashMap<String, HashMap<String, Value>>> = from_redis_value(v)?;
        let ids: Vec<StreamId> = rows
            .into_iter()
            .flat_map(|row| {
                row.into_iter().map(|(id, map)| StreamId {
                    id,
                    map,
                    milliseconds_elapsed_from_delivery: None,
                    delivered_count: None,
                })
            })
            .collect();
        Ok(StreamRangeReply { ids })
    }
}

impl FromRedisValue for StreamClaimReply {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let rows: Vec<HashMap<String, HashMap<String, Value>>> = from_redis_value(v)?;
        let ids: Vec<StreamId> = rows
            .into_iter()
            .flat_map(|row| {
                row.into_iter().map(|(id, map)| StreamId {
                    id,
                    map,
                    milliseconds_elapsed_from_delivery: None,
                    delivered_count: None,
                })
            })
            .collect();
        Ok(StreamClaimReply { ids })
    }
}

type SPRInner = (
    usize,
    Option<String>,
    Option<String>,
    Vec<Option<(String, String)>>,
);
impl FromRedisValue for StreamPendingReply {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let (count, start, end, consumer_data): SPRInner = from_redis_value(v)?;

        if count == 0 {
            Ok(StreamPendingReply::Empty)
        } else {
            let mut result = StreamPendingData::default();

            let start_id = start.ok_or_else(|| {
                ParsingError::from(arcstr::literal!(
                    "IllegalState: Non-zero pending expects start id"
                ))
            })?;

            let end_id = end.ok_or_else(|| {
                ParsingError::from(arcstr::literal!(
                    "IllegalState: Non-zero pending expects end id"
                ))
            })?;

            result.count = count;
            result.start_id = start_id;
            result.end_id = end_id;

            result.consumers = consumer_data
                .into_iter()
                .flatten()
                .map(|(name, pending)| StreamInfoConsumer {
                    name,
                    pending: pending.parse().unwrap_or_default(),
                    ..Default::default()
                })
                .collect();

            Ok(StreamPendingReply::Data(result))
        }
    }
}

impl FromRedisValue for StreamPendingCountReply {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let mut reply = StreamPendingCountReply::default();
        match v {
            Value::Array(outer_tuple) => {
                for outer in outer_tuple {
                    match outer {
                        Value::Array(inner_tuple) => match &inner_tuple[..] {
                            [
                                Value::BulkString(id_bytes),
                                Value::BulkString(consumer_bytes),
                                Value::Int(last_delivered_ms_u64),
                                Value::Int(times_delivered_u64),
                            ] => {
                                let id = String::from_utf8(id_bytes.to_vec())?;
                                let consumer = String::from_utf8(consumer_bytes.to_vec())?;
                                let last_delivered_ms = *last_delivered_ms_u64 as usize;
                                let times_delivered = *times_delivered_u64 as usize;
                                reply.ids.push(StreamPendingId {
                                    id,
                                    consumer,
                                    last_delivered_ms,
                                    times_delivered,
                                });
                            }
                            _ => fail!(ParsingError::from(arcstr::literal!(
                                "Cannot parse redis data (3)"
                            ))),
                        },
                        _ => fail!(ParsingError::from(arcstr::literal!(
                            "Cannot parse redis data (2)"
                        ))),
                    }
                }
            }
            _ => fail!(ParsingError::from(arcstr::literal!(
                "Cannot parse redis data (1)"
            ))),
        };
        Ok(reply)
    }
}

impl FromRedisValue for StreamInfoStreamReply {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let mut map: HashMap<String, Value> = from_redis_value(v)?;
        let mut reply = StreamInfoStreamReply::default();
        if let Some(v) = map.remove("last-generated-id") {
            reply.last_generated_id = from_redis_value(v)?;
        }
        if let Some(v) = map.remove("radix-tree-nodes") {
            reply.radix_tree_keys = from_redis_value(v)?;
        }
        if let Some(v) = map.remove("groups") {
            reply.groups = from_redis_value(v)?;
        }
        if let Some(v) = map.remove("length") {
            reply.length = from_redis_value(v)?;
        }
        if let Some(v) = map.remove("first-entry") {
            reply.first_entry = StreamId::from_array_value(v)?;
        }
        if let Some(v) = map.remove("last-entry") {
            reply.last_entry = StreamId::from_array_value(v)?;
        }
        Ok(reply)
    }
}

impl FromRedisValue for StreamInfoConsumersReply {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let consumers: Vec<HashMap<String, Value>> = from_redis_value(v)?;
        let mut reply = StreamInfoConsumersReply::default();
        for mut map in consumers {
            let mut c = StreamInfoConsumer::default();
            if let Some(v) = map.remove("name") {
                c.name = from_redis_value(v)?;
            }
            if let Some(v) = map.remove("pending") {
                c.pending = from_redis_value(v)?;
            }
            if let Some(v) = map.remove("idle") {
                c.idle = from_redis_value(v)?;
            }
            reply.consumers.push(c);
        }

        Ok(reply)
    }
}

impl FromRedisValue for StreamInfoGroupsReply {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        let groups: Vec<HashMap<String, Value>> = from_redis_value(v)?;
        let mut reply = StreamInfoGroupsReply::default();
        for mut map in groups {
            let mut g = StreamInfoGroup::default();
            if let Some(v) = map.remove("name") {
                g.name = from_redis_value(v)?;
            }
            if let Some(v) = map.remove("pending") {
                g.pending = from_redis_value(v)?;
            }
            if let Some(v) = map.remove("consumers") {
                g.consumers = from_redis_value(v)?;
            }
            if let Some(v) = map.remove("last-delivered-id") {
                g.last_delivered_id = from_redis_value(v)?;
            }
            if let Some(v) = map.remove("entries-read") {
                g.entries_read = if let Value::Nil = v {
                    None
                } else {
                    Some(from_redis_value(v)?)
                };
            }
            if let Some(v) = map.remove("lag") {
                g.lag = if let Value::Nil = v {
                    None
                } else {
                    Some(from_redis_value(v)?)
                };
            }
            reply.groups.push(g);
        }
        Ok(reply)
    }
}

/// Deletion policy for stream entries.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum StreamDeletionPolicy {
    /// Preserve existing references to the deleted entries in all consumer groups' PEL.
    #[default]
    KeepRef,
    /// Delete the entry from the stream and from all the consumer groups' PELs.
    DelRef,
    /// Delete the entry from the stream and from all the consumer groups' PELs, but only if the entry is acknowledged by all the groups.
    Acked,
}

impl ToRedisArgs for StreamDeletionPolicy {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            StreamDeletionPolicy::KeepRef => out.write_arg(b"KEEPREF"),
            StreamDeletionPolicy::DelRef => out.write_arg(b"DELREF"),
            StreamDeletionPolicy::Acked => out.write_arg(b"ACKED"),
        }
    }
}
impl ToSingleRedisArg for StreamDeletionPolicy {}

/// Status codes returned by the `XDELEX` command
#[cfg(feature = "streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum XDelExStatusCode {
    /// No entry with the given id exists in the stream
    IdNotFound = -1,
    /// The entry was deleted from the stream
    Deleted = 1,
    /// The entry was not deleted because it has either not been delivered to any consumer
    /// or still has references in the consumer groups' Pending Entries List (PEL)
    NotDeletedUnacknowledgedOrStillReferenced = 2,
}

#[cfg(feature = "streams")]
impl FromRedisValue for XDelExStatusCode {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        match v {
            Value::Int(code) => match code {
                -1 => Ok(XDelExStatusCode::IdNotFound),
                1 => Ok(XDelExStatusCode::Deleted),
                2 => Ok(XDelExStatusCode::NotDeletedUnacknowledgedOrStillReferenced),
                _ => Err(format!("Invalid XDelExStatusCode status code: {code}").into()),
            },
            _ => Err(arcstr::literal!("Response type not XAckDelStatusCode compatible").into()),
        }
    }
}

/// Status codes returned by the `XACKDEL` command
#[cfg(feature = "streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum XAckDelStatusCode {
    /// No entry with the given id exists in the stream
    IdNotFound = -1,
    /// The entry was acknowledged and deleted from the stream
    AcknowledgedAndDeleted = 1,
    /// The entry was acknowledged but not deleted because it has references in the consumer groups' Pending Entries List (PEL)
    AcknowledgedNotDeletedStillReferenced = 2,
}

#[cfg(feature = "streams")]
impl FromRedisValue for XAckDelStatusCode {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        match v {
            Value::Int(code) => match code {
                -1 => Ok(XAckDelStatusCode::IdNotFound),
                1 => Ok(XAckDelStatusCode::AcknowledgedAndDeleted),
                2 => Ok(XAckDelStatusCode::AcknowledgedNotDeletedStillReferenced),
                _ => Err(arcstr::literal!("Invalid XAckDelStatusCode status code: {code}").into()),
            },
            _ => Err(arcstr::literal!("Response type not XAckDelStatusCode compatible").into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_command_eq(object: impl ToRedisArgs, expected: &[u8]) {
        let mut out: Vec<Vec<u8>> = Vec::new();

        object.write_redis_args(&mut out);

        let mut cmd: Vec<u8> = Vec::new();

        out.iter_mut().for_each(|item| {
            cmd.append(item);
            cmd.push(b' ');
        });

        cmd.pop();

        assert_eq!(cmd, expected);
    }

    mod stream_auto_claim_reply {
        use super::*;
        use crate::Value;

        #[test]
        fn short_response() {
            let value = Value::Array(vec![Value::BulkString("1713465536578-0".into())]);

            StreamAutoClaimReply::from_redis_value(value).unwrap_err();
        }

        #[test]
        fn parses_none_claimed_response() {
            let value = Value::Array(vec![
                Value::BulkString("0-0".into()),
                Value::Array(vec![]),
                Value::Array(vec![]),
            ]);

            let reply: StreamAutoClaimReply = FromRedisValue::from_redis_value(value).unwrap();

            assert_eq!(reply.next_stream_id.as_str(), "0-0");
            assert_eq!(reply.claimed.len(), 0);
            assert_eq!(reply.deleted_ids.len(), 0);
        }

        #[test]
        fn parses_response() {
            let value = Value::Array(vec![
                Value::BulkString("1713465536578-0".into()),
                Value::Array(vec![
                    Value::Array(vec![
                        Value::BulkString("1713465533411-0".into()),
                        // Both RESP2 and RESP3 expose this map as an array of key/values
                        Value::Array(vec![
                            Value::BulkString("name".into()),
                            Value::BulkString("test".into()),
                            Value::BulkString("other".into()),
                            Value::BulkString("whaterver".into()),
                        ]),
                    ]),
                    Value::Array(vec![
                        Value::BulkString("1713465536069-0".into()),
                        Value::Array(vec![
                            Value::BulkString("name".into()),
                            Value::BulkString("another test".into()),
                            Value::BulkString("other".into()),
                            Value::BulkString("something".into()),
                        ]),
                    ]),
                ]),
                Value::Array(vec![Value::BulkString("123456789-0".into())]),
            ]);

            let reply: StreamAutoClaimReply = FromRedisValue::from_redis_value(value).unwrap();

            assert_eq!(reply.next_stream_id.as_str(), "1713465536578-0");
            assert_eq!(reply.claimed.len(), 2);
            assert_eq!(reply.claimed[0].id.as_str(), "1713465533411-0");
            assert!(
                matches!(reply.claimed[0].map.get("name"), Some(Value::BulkString(v)) if v == "test".as_bytes())
            );
            assert_eq!(reply.claimed[1].id.as_str(), "1713465536069-0");
            assert_eq!(reply.deleted_ids.len(), 1);
            assert!(reply.deleted_ids.contains(&"123456789-0".to_string()))
        }

        #[test]
        fn parses_v6_response() {
            let value = Value::Array(vec![
                Value::BulkString("1713465536578-0".into()),
                Value::Array(vec![
                    Value::Array(vec![
                        Value::BulkString("1713465533411-0".into()),
                        Value::Array(vec![
                            Value::BulkString("name".into()),
                            Value::BulkString("test".into()),
                            Value::BulkString("other".into()),
                            Value::BulkString("whaterver".into()),
                        ]),
                    ]),
                    Value::Array(vec![
                        Value::BulkString("1713465536069-0".into()),
                        Value::Array(vec![
                            Value::BulkString("name".into()),
                            Value::BulkString("another test".into()),
                            Value::BulkString("other".into()),
                            Value::BulkString("something".into()),
                        ]),
                    ]),
                ]),
                // V6 and lower lack the deleted_ids array
            ]);

            let reply: StreamAutoClaimReply = FromRedisValue::from_redis_value(value).unwrap();

            assert_eq!(reply.next_stream_id.as_str(), "1713465536578-0");
            assert_eq!(reply.claimed.len(), 2);
            let ids: Vec<_> = reply.claimed.iter().map(|e| e.id.as_str()).collect();
            assert!(ids.contains(&"1713465533411-0"));
            assert!(ids.contains(&"1713465536069-0"));
            assert_eq!(reply.deleted_ids.len(), 0);
        }

        #[test]
        fn parses_justid_response() {
            let value = Value::Array(vec![
                Value::BulkString("1713465536578-0".into()),
                Value::Array(vec![
                    Value::BulkString("1713465533411-0".into()),
                    Value::BulkString("1713465536069-0".into()),
                ]),
                Value::Array(vec![Value::BulkString("123456789-0".into())]),
            ]);

            let reply: StreamAutoClaimReply = FromRedisValue::from_redis_value(value).unwrap();

            assert_eq!(reply.next_stream_id.as_str(), "1713465536578-0");
            assert_eq!(reply.claimed.len(), 2);
            let ids: Vec<_> = reply.claimed.iter().map(|e| e.id.as_str()).collect();
            assert!(ids.contains(&"1713465533411-0"));
            assert!(ids.contains(&"1713465536069-0"));
            assert_eq!(reply.deleted_ids.len(), 1);
            assert!(reply.deleted_ids.contains(&"123456789-0".to_string()))
        }

        #[test]
        fn parses_v6_justid_response() {
            let value = Value::Array(vec![
                Value::BulkString("1713465536578-0".into()),
                Value::Array(vec![
                    Value::BulkString("1713465533411-0".into()),
                    Value::BulkString("1713465536069-0".into()),
                ]),
                // V6 and lower lack the deleted_ids array
            ]);

            let reply: StreamAutoClaimReply = FromRedisValue::from_redis_value(value).unwrap();

            assert_eq!(reply.next_stream_id.as_str(), "1713465536578-0");
            assert_eq!(reply.claimed.len(), 2);
            let ids: Vec<_> = reply.claimed.iter().map(|e| e.id.as_str()).collect();
            assert!(ids.contains(&"1713465533411-0"));
            assert!(ids.contains(&"1713465536069-0"));
            assert_eq!(reply.deleted_ids.len(), 0);
        }
    }

    mod stream_trim_options {
        use super::*;

        #[test]
        fn maxlen_trim() {
            let options = StreamTrimOptions::maxlen(StreamTrimmingMode::Approx, 10);

            assert_command_eq(options, b"MAXLEN ~ 10");
        }

        #[test]
        fn maxlen_exact_trim() {
            let options = StreamTrimOptions::maxlen(StreamTrimmingMode::Exact, 10);

            assert_command_eq(options, b"MAXLEN = 10");
        }

        #[test]
        fn maxlen_trim_limit() {
            let options = StreamTrimOptions::maxlen(StreamTrimmingMode::Approx, 10).limit(5);

            assert_command_eq(options, b"MAXLEN ~ 10 LIMIT 5");
        }
        #[test]
        fn minid_trim_limit() {
            let options = StreamTrimOptions::minid(StreamTrimmingMode::Exact, "123456-7").limit(5);

            assert_command_eq(options, b"MINID = 123456-7 LIMIT 5");
        }
    }

    mod stream_add_options {
        use super::*;

        #[test]
        fn the_default() {
            let options = StreamAddOptions::default();

            assert_command_eq(options, b"");
        }

        #[test]
        fn with_maxlen_trim() {
            let options = StreamAddOptions::default()
                .trim(StreamTrimStrategy::maxlen(StreamTrimmingMode::Exact, 10));

            assert_command_eq(options, b"MAXLEN = 10");
        }

        #[test]
        fn with_nomkstream() {
            let options = StreamAddOptions::default().nomkstream();

            assert_command_eq(options, b"NOMKSTREAM");
        }

        #[test]
        fn with_nomkstream_and_maxlen_trim() {
            let options = StreamAddOptions::default()
                .nomkstream()
                .trim(StreamTrimStrategy::maxlen(StreamTrimmingMode::Exact, 10));

            assert_command_eq(options, b"NOMKSTREAM MAXLEN = 10");
        }
    }
}
