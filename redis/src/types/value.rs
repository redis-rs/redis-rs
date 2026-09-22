//! High-level `Value` type for responses

use super::is_nested_pairs;
use crate::{FromRedisValue, ParsingError, RedisResult, ServerError};
#[cfg(feature = "num-bigint")]
use num_bigint::BigInt;
use std::fmt;
use std::str::from_utf8;

/// Internal low-level redis value enum.
#[derive(PartialEq, Clone, Default)]
#[non_exhaustive]
pub enum Value {
    /// A nil response from the server.
    #[default]
    Nil,
    /// An integer response.  Note that there are a few situations
    /// in which redis actually returns a string for an integer which
    /// is why this library generally treats integers and strings
    /// the same for all numeric responses.
    Int(i64),
    /// An arbitrary binary data, usually represents a binary-safe string.
    BulkString(Vec<u8>),
    /// A response containing an array with more data. This is generally used by redis
    /// to express nested structures.
    Array(Vec<Self>),
    /// A simple string response, without line breaks and not binary safe.
    SimpleString(String),
    /// A status response which represents the string "OK".
    Okay,
    /// Unordered key,value list from the server. Use `as_map_iter` function.
    Map(Vec<(Self, Self)>),
    /// Attribute value from the server. Client will give data instead of whole Attribute type.
    Attribute {
        /// Data that attributes belong to.
        data: Box<Self>,
        /// Key,Value list of attributes.
        attributes: Vec<(Self, Self)>,
    },
    /// Unordered set value from the server.
    Set(Vec<Self>),
    /// A floating number response from the server.
    Double(f64),
    /// A boolean response from the server.
    Boolean(bool),
    /// First String is format and other is the string
    VerbatimString {
        /// Text's format type
        format: VerbatimFormat,
        /// Remaining string check format before using!
        text: String,
    },
    #[cfg(feature = "num-bigint")]
    /// Very large number that out of the range of the signed 64 bit numbers
    BigNumber(BigInt),
    #[cfg(not(feature = "num-bigint"))]
    /// Very large number that out of the range of the signed 64 bit numbers
    BigNumber(Vec<u8>),
    /// Push data from the server.
    Push {
        /// Push Kind
        kind: PushKind,
        /// Remaining data from push message
        data: Vec<Self>,
    },
    /// Represents an error message from the server
    ServerError(ServerError),
}

/// Values are generally not used directly unless you are using the
/// more low level functionality in the library.  For the most part
/// this is hidden with the help of the `FromRedisValue` trait.
///
/// While on the redis protocol there is an error type this is already
/// separated at an early point so the value only holds the remaining
/// types.
impl Value {
    /// Checks if the return value looks like it fulfils the cursor
    /// protocol.  That means the result is an array item of length
    /// two with the first one being a cursor and the second an
    /// array response.
    pub fn looks_like_cursor(&self) -> bool {
        match *self {
            Self::Array(ref items) => {
                if items.len() != 2 {
                    return false;
                }
                matches!(items[0], Self::BulkString(_)) && matches!(items[1], Self::Array(_))
            }
            _ => false,
        }
    }

    /// Returns an `&[Value]` if `self` is compatible with a sequence type
    pub fn as_sequence(&self) -> Option<&[Self]> {
        match self {
            Self::Array(items) | Self::Set(items) => Some(&items[..]),
            Self::Nil => Some(&[]),
            _ => None,
        }
    }

    /// Returns a `Vec<Value>` if `self` is compatible with a sequence type,
    /// otherwise returns `Err(self)`.
    pub fn into_sequence(self) -> Result<Vec<Self>, Self> {
        match self {
            Self::Array(items) | Self::Set(items) => Ok(items),
            Self::Nil => Ok(vec![]),
            _ => Err(self),
        }
    }

    /// Returns an iterator of `(&Value, &Value)` if `self` is compatible with a map type
    pub fn as_map_iter(&self) -> Option<MapIter<'_>> {
        match self {
            Self::Array(items) => {
                if is_nested_pairs(items) {
                    Some(MapIter::NestedPairs(items.iter()))
                } else {
                    (items.len() % 2 == 0).then(|| MapIter::Array(items.iter()))
                }
            }
            Self::Map(items) => Some(MapIter::Map(items.iter())),
            _ => None,
        }
    }

    /// If `self` is a two-element collection, return its two elements.
    fn as_pair(&self) -> Option<(&Self, &Self)> {
        match self {
            Self::Array(items) | Self::Set(items) if items.len() == 2 => {
                Some((&items[0], &items[1]))
            }
            Self::Map(items) if items.len() == 1 => Some((&items[0].0, &items[0].1)),
            _ => None,
        }
    }

    /// Owned counterpart of [`Self::as_pair`].
    fn into_pair(self) -> Result<(Self, Self), Self> {
        match self {
            Self::Array(items) | Self::Set(items) if items.len() == 2 => {
                let mut it = items.into_iter();
                let (a, b) = (it.next().unwrap(), it.next().unwrap());
                Ok((a, b))
            }
            Self::Map(items) if items.len() == 1 => Ok(items.into_iter().next().unwrap()),
            other => Err(other),
        }
    }

    /// Returns an iterator of `(Value, Value)` if `self` is compatible with a map type.
    /// If not, returns `Err(self)`.
    pub fn into_map_iter(self) -> Result<OwnedMapIter, Self> {
        match self {
            Self::Array(items) => {
                if is_nested_pairs(&items) {
                    Ok(OwnedMapIter::NestedPairs(items.into_iter()))
                } else if items.len() % 2 == 0 {
                    Ok(OwnedMapIter::Array(items.into_iter()))
                } else {
                    Err(Self::Array(items))
                }
            }
            Self::Map(items) => Ok(OwnedMapIter::Map(items.into_iter())),
            _ => Err(self),
        }
    }

    /// If value contains a server error, return it as an Err. Otherwise wrap the value in Ok.
    pub fn extract_error(self) -> RedisResult<Self> {
        match self {
            Self::Array(val) => Ok(Self::Array(Self::extract_error_vec(val)?)),
            Self::Map(map) => Ok(Self::Map(Self::extract_error_map(map)?)),
            Self::Attribute { data, attributes } => {
                let data = Box::new((*data).extract_error()?);
                let attributes = Self::extract_error_map(attributes)?;
                Ok(Self::Attribute { data, attributes })
            }
            Self::Set(set) => Ok(Self::Set(Self::extract_error_vec(set)?)),
            Self::Push { kind, data } => Ok(Self::Push {
                kind,
                data: Self::extract_error_vec(data)?,
            }),
            Self::ServerError(err) => Err(err.into()),
            _ => Ok(self),
        }
    }

    pub(crate) fn extract_error_vec(vec: Vec<Self>) -> RedisResult<Vec<Self>> {
        vec.into_iter()
            .map(Self::extract_error)
            .collect::<RedisResult<Vec<_>>>()
    }

    pub(crate) fn extract_error_map(map: Vec<(Self, Self)>) -> RedisResult<Vec<(Self, Self)>> {
        let mut vec = Vec::with_capacity(map.len());
        for (key, value) in map.into_iter() {
            vec.push((key.extract_error()?, value.extract_error()?));
        }
        Ok(vec)
    }

    pub(crate) fn is_collection_of_len(&self, len: usize) -> bool {
        match self {
            Self::Array(values) | Self::Set(values) => values.len() == len,
            Self::Map(items) => items.len() * 2 == len,
            _ => false,
        }
    }

    #[cfg(feature = "cluster-async")]
    pub(crate) fn is_error_that_requires_action(&self) -> bool {
        matches!(self, Self::ServerError(error) if error.requires_action())
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Nil => write!(f, "nil"),
            Self::Int(val) => write!(f, "int({val:?})"),
            Self::BulkString(ref val) => match from_utf8(val) {
                Ok(x) => write!(f, "bulk-string('{x:?}')"),
                Err(_) => write!(f, "binary-data({val:?})"),
            },
            Self::Array(ref values) => write!(f, "array({values:?})"),
            Self::Push { ref kind, ref data } => write!(f, "push({kind:?}, {data:?})"),
            Self::Okay => write!(f, "ok"),
            Self::SimpleString(ref s) => write!(f, "simple-string({s:?})"),
            Self::Map(ref values) => write!(f, "map({values:?})"),
            Self::Attribute {
                ref data,
                attributes: _,
            } => write!(f, "attribute({data:?})"),
            Self::Set(ref values) => write!(f, "set({values:?})"),
            Self::Double(ref d) => write!(f, "double({d:?})"),
            Self::Boolean(ref b) => write!(f, "boolean({b:?})"),
            Self::VerbatimString {
                ref format,
                ref text,
            } => {
                write!(f, "verbatim-string({format:?},{text:?})")
            }
            Self::BigNumber(ref m) => write!(f, "big-number({m:?})"),
            Self::ServerError(ref err) => match err.details() {
                Some(details) => write!(f, "Server error: `{}: {details}`", err.code()),
                None => write!(f, "Server error: `{}`", err.code()),
            },
        }
    }
}

impl FromRedisValue for Value {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        Ok(v.clone())
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        Ok(v)
    }
}

/// `VerbatimString`'s format types defined by spec
#[derive(PartialEq, Clone, Debug)]
#[non_exhaustive]
pub enum VerbatimFormat {
    /// Unknown type to catch future formats.
    Unknown(String),
    /// `mkd` format
    Markdown,
    /// `txt` format
    Text,
}

impl fmt::Display for VerbatimFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Markdown => write!(f, "mkd"),
            Self::Unknown(val) => write!(f, "{val}"),
            Self::Text => write!(f, "txt"),
        }
    }
}

/// `Push` type's currently known kinds.
#[derive(PartialEq, Clone, Debug)]
#[non_exhaustive]
pub enum PushKind {
    /// `Disconnection` is sent from the **library** when connection is closed.
    Disconnection,
    /// Other kind to catch future kinds.
    Other(String),
    /// `invalidate` is received when a key is changed/deleted.
    Invalidate,
    /// `message` is received when pubsub message published by another client.
    Message,
    /// `pmessage` is received when pubsub message published by another client and client subscribed to topic via pattern.
    PMessage,
    /// `smessage` is received when pubsub message published by another client and client subscribed to it with sharding.
    SMessage,
    /// `unsubscribe` is received when client unsubscribed from a channel.
    Unsubscribe,
    /// `punsubscribe` is received when client unsubscribed from a pattern.
    PUnsubscribe,
    /// `sunsubscribe` is received when client unsubscribed from a shard channel.
    SUnsubscribe,
    /// `subscribe` is received when client subscribed to a channel.
    Subscribe,
    /// `psubscribe` is received when client subscribed to a pattern.
    PSubscribe,
    /// `ssubscribe` is received when client subscribed to a shard channel.
    SSubscribe,
}

impl PushKind {
    #[cfg(feature = "aio")]
    pub(crate) fn has_reply(&self) -> bool {
        matches!(
            self,
            &Self::Unsubscribe
                | &Self::PUnsubscribe
                | &Self::SUnsubscribe
                | &Self::Subscribe
                | &Self::PSubscribe
                | &Self::SSubscribe
        )
    }
}

impl fmt::Display for PushKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Other(kind) => write!(f, "{kind}"),
            Self::Invalidate => write!(f, "invalidate"),
            Self::Message => write!(f, "message"),
            Self::PMessage => write!(f, "pmessage"),
            Self::SMessage => write!(f, "smessage"),
            Self::Unsubscribe => write!(f, "unsubscribe"),
            Self::PUnsubscribe => write!(f, "punsubscribe"),
            Self::SUnsubscribe => write!(f, "sunsubscribe"),
            Self::Subscribe => write!(f, "subscribe"),
            Self::PSubscribe => write!(f, "psubscribe"),
            Self::SSubscribe => write!(f, "ssubscribe"),
            Self::Disconnection => write!(f, "disconnection"),
        }
    }
}

#[non_exhaustive]
pub enum MapIter<'a> {
    Array(std::slice::Iter<'a, Value>),
    Map(std::slice::Iter<'a, (Value, Value)>),
    /// An array whose every element is itself a two-element collection. This is
    /// the shape RESP3 uses where RESP2 used a flat key/value array, e.g. for
    /// `ZRANGE ... WITHSCORES`.
    NestedPairs(std::slice::Iter<'a, Value>),
}

impl<'a> Iterator for MapIter<'a> {
    type Item = (&'a Value, &'a Value);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MapIter::Array(iter) => Some((iter.next()?, iter.next()?)),
            MapIter::Map(iter) => {
                let (k, v) = iter.next()?;
                Some((k, v))
            }
            MapIter::NestedPairs(iter) => iter.next()?.as_pair(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            MapIter::Map(iter) => iter.size_hint(),
            MapIter::Array(iter) | MapIter::NestedPairs(iter) => iter.size_hint(),
        }
    }
}

#[non_exhaustive]
pub enum OwnedMapIter {
    Array(std::vec::IntoIter<Value>),
    Map(std::vec::IntoIter<(Value, Value)>),
    /// An array whose every element is itself a two-element collection. This is
    /// the shape RESP3 uses where RESP2 used a flat key/value array, e.g. for
    /// `ZRANGE ... WITHSCORES`.
    NestedPairs(std::vec::IntoIter<Value>),
}

impl Iterator for OwnedMapIter {
    type Item = (Value, Value);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Array(iter) => Some((iter.next()?, iter.next()?)),
            Self::Map(iter) => iter.next(),
            Self::NestedPairs(iter) => iter.next()?.into_pair().ok(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Array(iter) => {
                let (low, high) = iter.size_hint();
                (low / 2, high.map(|h| h / 2))
            }
            Self::Map(iter) => iter.size_hint(),
            Self::NestedPairs(iter) => iter.size_hint(),
        }
    }
}
