//! Defines types to use with the streams commands.

use crate::{
    from_redis_value, types::HashMap, FromRedisValue, RedisResult, RedisWrite, ToRedisArgs, Value,
};

use std::io::{Error, ErrorKind};

macro_rules! invalid_type_error {
    ($v:expr, $det:expr) => {{
        fail!((
            $crate::ErrorKind::TypeError,
            "Response was of incompatible type",
            format!("{:?} (response was {:?})", $det, $v)
        ));
    }};
}

// Stream Maxlen Enum

/// Utility enum for passing `MAXLEN [= or ~] [COUNT]`
/// arguments into `StreamCommands`.
/// The enum value represents the count.
#[derive(PartialEq, Eq, Clone, Debug, Copy)]
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
}

impl StreamTrimOptions {
    /// Define a MAXLEN trim strategy with the given maximum number of entries
    pub fn maxlen(mode: StreamTrimmingMode, max_entries: usize) -> Self {
        Self {
            strategy: StreamTrimStrategy::maxlen(mode, max_entries),
        }
    }

    /// Defines a MINID trim strategy with the given minimum stream ID
    pub fn minid(mode: StreamTrimmingMode, stream_id: impl Into<String>) -> Self {
        Self {
            strategy: StreamTrimStrategy::minid(mode, stream_id),
        }
    }

    /// Set a limit to the number of records to trim in a single operation
    pub fn limit(mut self, limit: usize) -> Self {
        self.strategy = self.strategy.limit(limit);
        self
    }
}

impl ToRedisArgs for StreamTrimOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        self.strategy.write_redis_args(out);
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
#[derive(Default, Debug, Clone)]
pub struct StreamId {
    /// The stream `id` (entry ID) of this particular message.
    pub id: String,
    /// All fields in this message, associated with their respective values.
    pub map: HashMap<String, Value>,
}

impl StreamId {
    /// Converts a `Value::Array` into a `StreamId`.
    fn from_array_value(v: &Value) -> RedisResult<Self> {
        let mut stream_id = StreamId::default();
        if let Value::Array(ref values) = *v {
            if let Some(v) = values.first() {
                stream_id.id = from_redis_value(v)?;
            }
            if let Some(v) = values.get(1) {
                stream_id.map = from_redis_value(v)?;
            }
        }

        Ok(stream_id)
    }

    /// Fetches value of a given field and converts it to the specified
    /// type.
    pub fn get<T: FromRedisValue>(&self, key: &str) -> Option<T> {
        match self.map.get(key) {
            Some(x) => from_redis_value(x).ok(),
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
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match *v {
            Value::Array(ref items) => {
                if let 2..=3 = items.len() {
                    let deleted_ids = if let Some(o) = items.get(2) {
                        from_redis_value(o)?
                    } else {
                        Vec::new()
                    };

                    let claimed: Vec<StreamId> = match &items[1] {
                        // JUSTID response
                        Value::Array(x)
                            if matches!(x.first(), None | Some(Value::BulkString(_))) =>
                        {
                            let ids: Vec<String> = from_redis_value(&items[1])?;

                            ids.into_iter()
                                .map(|id| StreamId {
                                    id,
                                    ..Default::default()
                                })
                                .collect()
                        }
                        // full response
                        Value::Array(x) if matches!(x.first(), Some(Value::Array(_))) => {
                            let rows: SACRows = from_redis_value(&items[1])?;

                            rows.into_iter()
                                .flat_map(|id_row| {
                                    id_row.into_iter().map(|(id, map)| StreamId { id, map })
                                })
                                .collect()
                        }
                        _ => invalid_type_error!("Incorrect type", &items[1]),
                    };

                    Ok(Self {
                        next_stream_id: from_redis_value(&items[0])?,
                        claimed,
                        deleted_ids,
                    })
                } else {
                    invalid_type_error!("Wrong number of entries in array response", v)
                }
            }
            _ => invalid_type_error!("Not a array response", v),
        }
    }
}

type SRRows = Vec<HashMap<String, Vec<HashMap<String, HashMap<String, Value>>>>>;
impl FromRedisValue for StreamReadReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let rows: SRRows = from_redis_value(v)?;
        let keys = rows
            .into_iter()
            .flat_map(|row| {
                row.into_iter().map(|(key, entry)| {
                    let ids = entry
                        .into_iter()
                        .flat_map(|id_row| id_row.into_iter().map(|(id, map)| StreamId { id, map }))
                        .collect();
                    StreamKey { key, ids }
                })
            })
            .collect();
        Ok(StreamReadReply { keys })
    }
}

impl FromRedisValue for StreamRangeReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let rows: Vec<HashMap<String, HashMap<String, Value>>> = from_redis_value(v)?;
        let ids: Vec<StreamId> = rows
            .into_iter()
            .flat_map(|row| row.into_iter().map(|(id, map)| StreamId { id, map }))
            .collect();
        Ok(StreamRangeReply { ids })
    }
}

impl FromRedisValue for StreamClaimReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let rows: Vec<HashMap<String, HashMap<String, Value>>> = from_redis_value(v)?;
        let ids: Vec<StreamId> = rows
            .into_iter()
            .flat_map(|row| row.into_iter().map(|(id, map)| StreamId { id, map }))
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
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let (count, start, end, consumer_data): SPRInner = from_redis_value(v)?;

        if count == 0 {
            Ok(StreamPendingReply::Empty)
        } else {
            let mut result = StreamPendingData::default();

            let start_id = start.ok_or_else(|| {
                Error::new(
                    ErrorKind::Other,
                    "IllegalState: Non-zero pending expects start id",
                )
            })?;

            let end_id = end.ok_or_else(|| {
                Error::new(
                    ErrorKind::Other,
                    "IllegalState: Non-zero pending expects end id",
                )
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
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let mut reply = StreamPendingCountReply::default();
        match v {
            Value::Array(outer_tuple) => {
                for outer in outer_tuple {
                    match outer {
                        Value::Array(inner_tuple) => match &inner_tuple[..] {
                            [Value::BulkString(id_bytes), Value::BulkString(consumer_bytes), Value::Int(last_delivered_ms_u64), Value::Int(times_delivered_u64)] =>
                            {
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
                            _ => fail!((
                                crate::types::ErrorKind::TypeError,
                                "Cannot parse redis data (3)"
                            )),
                        },
                        _ => fail!((
                            crate::types::ErrorKind::TypeError,
                            "Cannot parse redis data (2)"
                        )),
                    }
                }
            }
            _ => fail!((
                crate::types::ErrorKind::TypeError,
                "Cannot parse redis data (1)"
            )),
        };
        Ok(reply)
    }
}

impl FromRedisValue for StreamInfoStreamReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let map: HashMap<String, Value> = from_redis_value(v)?;
        let mut reply = StreamInfoStreamReply::default();
        if let Some(v) = &map.get("last-generated-id") {
            reply.last_generated_id = from_redis_value(v)?;
        }
        if let Some(v) = &map.get("radix-tree-nodes") {
            reply.radix_tree_keys = from_redis_value(v)?;
        }
        if let Some(v) = &map.get("groups") {
            reply.groups = from_redis_value(v)?;
        }
        if let Some(v) = &map.get("length") {
            reply.length = from_redis_value(v)?;
        }
        if let Some(v) = &map.get("first-entry") {
            reply.first_entry = StreamId::from_array_value(v)?;
        }
        if let Some(v) = &map.get("last-entry") {
            reply.last_entry = StreamId::from_array_value(v)?;
        }
        Ok(reply)
    }
}

impl FromRedisValue for StreamInfoConsumersReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let consumers: Vec<HashMap<String, Value>> = from_redis_value(v)?;
        let mut reply = StreamInfoConsumersReply::default();
        for map in consumers {
            let mut c = StreamInfoConsumer::default();
            if let Some(v) = &map.get("name") {
                c.name = from_redis_value(v)?;
            }
            if let Some(v) = &map.get("pending") {
                c.pending = from_redis_value(v)?;
            }
            if let Some(v) = &map.get("idle") {
                c.idle = from_redis_value(v)?;
            }
            reply.consumers.push(c);
        }

        Ok(reply)
    }
}

impl FromRedisValue for StreamInfoGroupsReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let groups: Vec<HashMap<String, Value>> = from_redis_value(v)?;
        let mut reply = StreamInfoGroupsReply::default();
        for map in groups {
            let mut g = StreamInfoGroup::default();
            if let Some(v) = &map.get("name") {
                g.name = from_redis_value(v)?;
            }
            if let Some(v) = &map.get("pending") {
                g.pending = from_redis_value(v)?;
            }
            if let Some(v) = &map.get("consumers") {
                g.consumers = from_redis_value(v)?;
            }
            if let Some(v) = &map.get("last-delivered-id") {
                g.last_delivered_id = from_redis_value(v)?;
            }
            if let Some(v) = &map.get("entries-read") {
                g.entries_read = if let Value::Nil = v {
                    None
                } else {
                    Some(from_redis_value(v)?)
                };
            }
            if let Some(v) = &map.get("lag") {
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

            let reply: RedisResult<StreamAutoClaimReply> = FromRedisValue::from_redis_value(&value);

            assert!(reply.is_err());
        }

        #[test]
        fn parses_none_claimed_response() {
            let value = Value::Array(vec![
                Value::BulkString("0-0".into()),
                Value::Array(vec![]),
                Value::Array(vec![]),
            ]);

            let reply: RedisResult<StreamAutoClaimReply> = FromRedisValue::from_redis_value(&value);

            assert!(reply.is_ok());

            let reply = reply.unwrap();

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

            let reply: RedisResult<StreamAutoClaimReply> = FromRedisValue::from_redis_value(&value);

            assert!(reply.is_ok());

            let reply = reply.unwrap();

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

            let reply: RedisResult<StreamAutoClaimReply> = FromRedisValue::from_redis_value(&value);

            assert!(reply.is_ok());

            let reply = reply.unwrap();

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

            let reply: RedisResult<StreamAutoClaimReply> = FromRedisValue::from_redis_value(&value);

            assert!(reply.is_ok());

            let reply = reply.unwrap();

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

            let reply: RedisResult<StreamAutoClaimReply> = FromRedisValue::from_redis_value(&value);

            assert!(reply.is_ok());

            let reply = reply.unwrap();

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
