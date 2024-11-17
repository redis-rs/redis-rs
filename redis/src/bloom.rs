//! Defines types to use with the bloom filter commands.

#[cfg(feature = "bloom")]
use crate::{
    from_redis_value, ErrorKind, FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs,
    Value,
};
use crate::{Commands, Connection};
use std::collections::HashMap;

/// scaling options used in bf_reserve and bf_insert commands.
#[derive(Debug)]
pub enum ScalingOptions {
    /// Specifies the desired expansion rate for the filter to be created.
    ExpansionRate(usize),
    /// Prevents the filter from creating additional sub-filters if initial capacity is reached.
    NonScaling,
}

impl ToRedisArgs for ScalingOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match *self {
            ScalingOptions::ExpansionRate(expansion) => {
                out.write_arg(b"EXPANSION");
                out.write_arg(format!("{expansion}").as_bytes());
            }
            ScalingOptions::NonScaling => {
                out.write_arg(b"NONSCALING");
            }
        }
    }
}

/// Options for the `bf_insert` command.
/// See the [Redis documentation](https://redis.io/docs/latest/commands/bf.insert/) for more information.
#[derive(Default, Debug)]
pub struct InsertOptions {
    capacity: Option<i64>,
    no_create: Option<bool>,
    scaling: Option<ScalingOptions>,
    error_rate: Option<f64>,
}

impl InsertOptions {
    ///Indicates that the filter should not be created if it does not already exist.
    pub fn nocreate(mut self) -> Self {
        self.no_create = Some(true);
        self
    }

    /// Specifies the desired capacity for the filter to be created.
    pub fn scale(mut self, scale_option: ScalingOptions) -> Self {
        self.scaling = Some(scale_option);
        self
    }
    /// Specifies the error ratio of the newly created filter if it does not yet exist.
    pub fn error_rate(mut self, rate: f64) -> Self {
        self.error_rate = Some(rate);
        self
    }
    /// Specifies the desired capacity for the filter to be created.
    pub fn capacity(mut self, capacity: i64) -> Self {
        self.capacity = Some(capacity);
        self
    }
}
impl ToRedisArgs for InsertOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if Some(true) == self.no_create {
            out.write_arg(b"NOCREATE");
        }
        if let Some(ref scaling) = self.scaling {
            scaling.write_redis_args(out);
        }

        if let Some(ref error) = self.error_rate {
            out.write_arg(b"ERROR");
            out.write_arg(format!("{error}").as_bytes());
        }

        if let Some(ref capacity) = self.capacity {
            out.write_arg(b"CAPACITY");
            out.write_arg(format!("{capacity}").as_bytes());
        }
    }
}

/// single info options
/// See the [Redis documentation](https://redis.io/docs/latest/commands/bf.info/) for more information.
#[derive(Debug)]
pub enum InfoType {
    /// Return the number of unique items that can be stored in this Bloom filter before scaling would be required
    Capacity,
    /// Return the memory size: number of bytes allocated for this Bloom filter.
    Size,
    /// Return the number of sub-filters
    Filters,
    /// Return he number of items inserted in the filter
    Items,
    /// Return the expansion rate of the filter
    Expansion,
}

impl ToRedisArgs for InfoType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match *self {
            InfoType::Capacity => {
                out.write_arg(b"CAPACITY");
            }
            InfoType::Size => {
                out.write_arg(b"SIZE");
            }
            InfoType::Filters => {
                out.write_arg(b"FILTERS");
            }
            InfoType::Expansion => {
                out.write_arg(b"EXPANSION");
            }
            InfoType::Items => {
                out.write_arg(b"ITEMS");
            }
        }
    }
}

/// Response returned by the `bf_info_all` command.
/// See the [Redis documentation](https://redis.io/docs/latest/commands/bf.info/) for more information.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AllInfoResponse {
    /// The number of unique items that can be stored in this Bloom filter before scaling would be required
    pub capacity: i64,
    /// The memory size: number of bytes allocated for this Bloom filter.
    pub memory_size: i64,
    /// Return the number of sub-filters
    pub number_of_filters: i64,
    /// The number of items inserted in the filter
    pub number_of_items_inserted: i64,
    /// The expansion rate of the filter (if filter is scaling)
    pub expansion_rate: Option<i64>,
}

impl FromRedisValue for AllInfoResponse {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let hm: RedisResult<HashMap<String, Option<_>>> = from_redis_value(v);
        hm.map(|hm| {
            let resp = &mut AllInfoResponse {
                capacity: 0,
                memory_size: 0,
                number_of_filters: 0,
                number_of_items_inserted: 0,
                expansion_rate: None,
            };

            hm.iter().for_each(|(k, v)| match k.as_str() {
                "Capacity" => resp.capacity = v.map_or(0, |x| x),
                "Size" => resp.memory_size = v.map_or(0, |x| x),
                "Number of filters" => resp.number_of_filters = v.map_or(0, |x| x),
                "Number of items inserted" => resp.number_of_items_inserted = v.map_or(0, |x| x),
                "Expansion rate" => resp.expansion_rate = *v,
                _ => { /*quietly ignore unknown keys*/ }
            });
            *resp
        })
    }
}
/// Response returned by the bf_info command (used to return a specific field).
/// See the [Redis documentation](https://redis.io/docs/latest/commands/bf.info/) for more information.
///
#[derive(Debug)]
pub struct SingleInfoResponse {
    /// The value of the requested field if exists
    pub value: Option<i64>,
}

macro_rules! not_convertible_error {
    ($v:expr, $det:expr) => {
        RedisError::from((
            ErrorKind::TypeError,
            "Response type not convertible",
            format!("{:?} (response was {:?})", $det, $v),
        ))
    };
}

impl FromRedisValue for SingleInfoResponse {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Array(vec) => {
                let resp = SingleInfoResponse {
                    value: vec.first().and_then(|v| match v {
                        Value::Int(i) => Some(*i),
                        _ => None,
                    }),
                };
                Ok(resp)
            }
            Value::Map(map) => {
                let resp = SingleInfoResponse {
                    value: map.first().and_then(|(_, v)| match v {
                        Value::Int(i) => Some(*i),
                        _ => None,
                    }),
                };
                Ok(resp)
            }

            _ => Err(not_convertible_error!(
                v,
                "expected vector (resp2) or map (resp3)"
            )),
        }
    }
}

/// The result of bf_scan_dump
/// See the [Redis documentation](https://redis.io/docs/latest/commands/bf.scandump/) for more information.
#[derive(Debug, Clone)]
pub struct DumpResult {
    /// The iterator id to be used in the next call to `bf_scan_dump`
    pub iterator: i64,
    /// The chunked data
    pub data: Vec<u8>,
}

impl FromRedisValue for DumpResult {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Array(vec) => {
                let iterator = match vec.first() {
                    Some(Value::Int(i)) => *i,
                    _ => return Err(not_convertible_error!(v, "expected int")),
                };
                let data: &Vec<u8> = match vec.get(1) {
                    Some(Value::BulkString(d)) => d,
                    _ => return Err(not_convertible_error!(v, "expected data")),
                };
                Ok(DumpResult {
                    iterator,
                    data: data.to_vec(),
                })
            }
            _ => Err(not_convertible_error!(v, "expected array")),
        }
    }
}

/// An iterator over the results of the `bf_scan_dump` command.
///
/// Examples:
///
/// ```rust,no_run
/// # use std::collections::VecDeque;
/// use redis::bloom::{AllInfoResponse, DumpIterator, DumpResult};
/// use redis::{Commands, ToRedisArgs};
/// # fn do_something() -> redis::RedisResult<()> {
/// use redis::{PubSubCommands, ControlFlow};
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let mut con = client.get_connection()?;
///
/// let mut chunks: VecDeque<DumpResult> = VecDeque::new();
///
/// let dump_iterator = DumpIterator::new(&mut con, "bloomf2");
/// for chunk in dump_iterator {
///
///    chunks.push_back(chunk);
///  }
///  println!("completed dump for bloomf2");
///  Ok(())}
/// #
/// ```

pub struct DumpIterator<'a> {
    con: &'a mut Connection,
    key: &'a str,
    iter_id: i64,
}

impl<'a> DumpIterator<'a> {
    /// Create a new iterator for the given key
    pub fn new(con: &'a mut Connection, key: &'a str) -> Self {
        DumpIterator {
            con,
            key,
            iter_id: 0,
        }
    }
}

impl Iterator for DumpIterator<'_> {
    type Item = DumpResult;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let dump: DumpResult = self.con.bf_scandump(self.key, self.iter_id).unwrap();
        if dump.iterator == 0 {
            return None;
        }
        self.iter_id = dump.iterator;
        Some(dump)
    }
}
