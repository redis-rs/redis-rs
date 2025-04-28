#[cfg(feature = "ahash")]
pub(crate) use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use num_bigint::BigInt;
use std::borrow::Cow;
#[cfg(not(feature = "ahash"))]
pub(crate) use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::error;
use std::ffi::{CString, NulError};
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::io;
use std::ops::Deref;
use std::str::{from_utf8, Utf8Error};
use std::string::FromUtf8Error;

macro_rules! invalid_type_error {
    ($v:expr, $det:expr) => {{
        fail!(invalid_type_error_inner!($v, $det))
    }};
}

macro_rules! invalid_type_error_inner {
    ($v:expr, $det:expr) => {
        RedisError::from((
            ErrorKind::TypeError,
            "Response was of incompatible type",
            format!("{:?} (response was {:?})", $det, $v),
        ))
    };
}

/// Helper enum that is used to define expiry time
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

/// Helper enum that is used to define existence checks
#[derive(Clone, Copy)]
pub enum ExistenceCheck {
    /// NX -- Only set the key if it does not already exist.
    NX,
    /// XX -- Only set the key if it already exists.
    XX,
}

/// Helper enum that is used to define field existence checks
#[derive(Clone, Copy)]
pub enum FieldExistenceCheck {
    /// FNX -- Only set the fields if all do not already exist.
    FNX,
    /// FXX -- Only set the fields if all already exist.
    FXX,
}

/// Helper enum that is used in some situations to describe
/// the behavior of arguments in a numeric context.
#[derive(PartialEq, Eq, Clone, Debug, Copy)]
pub enum NumericBehavior {
    /// This argument is not numeric.
    NonNumeric,
    /// This argument is an integer.
    NumberIsInteger,
    /// This argument is a floating point value.
    NumberIsFloat,
}

/// An enum of all error kinds.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The server generated an invalid response.
    ResponseError,
    /// The parser failed to parse the server response.
    ParseError,
    /// The authentication with the server failed.
    AuthenticationFailed,
    /// Operation failed because of a type mismatch.
    TypeError,
    /// A script execution was aborted.
    ExecAbortError,
    /// The server cannot response because it's loading a dump.
    BusyLoadingError,
    /// A script that was requested does not actually exist.
    NoScriptError,
    /// An error that was caused because the parameter to the
    /// client were wrong.
    InvalidClientConfig,
    /// Raised if a key moved to a different node.
    Moved,
    /// Raised if a key moved to a different node but we need to ask.
    Ask,
    /// Raised if a request needs to be retried.
    TryAgain,
    /// Raised if a redis cluster is down.
    ClusterDown,
    /// A request spans multiple slots
    CrossSlot,
    /// A cluster master is unavailable.
    MasterDown,
    /// This kind is returned if the redis error is one that is
    /// not native to the system.  This is usually the case if
    /// the cause is another error.
    IoError,
    /// An error raised that was identified on the client before execution.
    ClientError,
    /// An extension error.  This is an error created by the server
    /// that is not directly understood by the library.
    ExtensionError,
    /// Attempt to write to a read-only server
    ReadOnly,
    /// Requested name not found among masters returned by the sentinels
    MasterNameNotFoundBySentinel,
    /// No valid replicas found in the sentinels, for a given master name
    NoValidReplicasFoundBySentinel,
    /// At least one sentinel connection info is required
    EmptySentinelList,
    /// Attempted to kill a script/function while they werent' executing
    NotBusy,
    /// Used when a cluster connection cannot find a connection to a valid node.
    ClusterConnectionNotFound,
    /// Attempted to unsubscribe on a connection that is not in subscribed mode.
    NoSub,

    #[cfg(feature = "json")]
    /// Error Serializing a struct to JSON form
    Serialize,

    /// Redis Servers prior to v6.0.0 doesn't support RESP3.
    /// Try disabling resp3 option
    RESP3NotSupported,
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum ServerErrorKind {
    ResponseError,
    ExecAbortError,
    BusyLoadingError,
    NoScriptError,
    Moved,
    Ask,
    TryAgain,
    ClusterDown,
    CrossSlot,
    MasterDown,
    ReadOnly,
    NotBusy,
    NoSub,
}

#[derive(PartialEq, Debug, Clone)]
pub enum ServerError {
    ExtensionError {
        code: String,
        detail: Option<String>,
    },
    KnownError {
        kind: ServerErrorKind,
        detail: Option<String>,
    },
}

impl ServerError {
    pub fn kind(&self) -> Option<ServerErrorKind> {
        match self {
            ServerError::ExtensionError { .. } => None,
            ServerError::KnownError { kind, .. } => Some(*kind),
        }
    }

    pub fn code(&self) -> &str {
        match self {
            ServerError::ExtensionError { code, .. } => code,
            ServerError::KnownError { kind, .. } => match kind {
                ServerErrorKind::ResponseError => "ERR",
                ServerErrorKind::ExecAbortError => "EXECABORT",
                ServerErrorKind::BusyLoadingError => "LOADING",
                ServerErrorKind::NoScriptError => "NOSCRIPT",
                ServerErrorKind::Moved => "MOVED",
                ServerErrorKind::Ask => "ASK",
                ServerErrorKind::TryAgain => "TRYAGAIN",
                ServerErrorKind::ClusterDown => "CLUSTERDOWN",
                ServerErrorKind::CrossSlot => "CROSSSLOT",
                ServerErrorKind::MasterDown => "MASTERDOWN",
                ServerErrorKind::ReadOnly => "READONLY",
                ServerErrorKind::NotBusy => "NOTBUSY",
                ServerErrorKind::NoSub => "NOSUB",
            },
        }
    }

    pub fn details(&self) -> Option<&str> {
        match self {
            ServerError::ExtensionError { detail, .. } => detail.as_ref().map(|str| str.as_str()),
            ServerError::KnownError { detail, .. } => detail.as_ref().map(|str| str.as_str()),
        }
    }
}

impl From<ServerError> for RedisError {
    fn from(value: ServerError) -> Self {
        // TODO - Consider changing RedisError to explicitly represent whether an error came from the server or not. Today it is only implied.
        match value {
            ServerError::ExtensionError { code, detail } => make_extension_error(code, detail),
            ServerError::KnownError { kind, detail } => {
                let desc = "An error was signalled by the server";
                let kind = match kind {
                    ServerErrorKind::ResponseError => ErrorKind::ResponseError,
                    ServerErrorKind::ExecAbortError => ErrorKind::ExecAbortError,
                    ServerErrorKind::BusyLoadingError => ErrorKind::BusyLoadingError,
                    ServerErrorKind::NoScriptError => ErrorKind::NoScriptError,
                    ServerErrorKind::Moved => ErrorKind::Moved,
                    ServerErrorKind::Ask => ErrorKind::Ask,
                    ServerErrorKind::TryAgain => ErrorKind::TryAgain,
                    ServerErrorKind::ClusterDown => ErrorKind::ClusterDown,
                    ServerErrorKind::CrossSlot => ErrorKind::CrossSlot,
                    ServerErrorKind::MasterDown => ErrorKind::MasterDown,
                    ServerErrorKind::ReadOnly => ErrorKind::ReadOnly,
                    ServerErrorKind::NotBusy => ErrorKind::NotBusy,
                    ServerErrorKind::NoSub => ErrorKind::NoSub,
                };
                match detail {
                    Some(detail) => RedisError::from((kind, desc, detail)),
                    None => RedisError::from((kind, desc)),
                }
            }
        }
    }
}

/// Internal low-level redis value enum.
#[derive(PartialEq, Clone)]
pub enum Value {
    /// A nil response from the server.
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
    Array(Vec<Value>),
    /// A simple string response, without line breaks and not binary safe.
    SimpleString(String),
    /// A status response which represents the string "OK".
    Okay,
    /// Unordered key,value list from the server. Use `as_map_iter` function.
    Map(Vec<(Value, Value)>),
    /// Attribute value from the server. Client will give data instead of whole Attribute type.
    Attribute {
        /// Data that attributes belong to.
        data: Box<Value>,
        /// Key,Value list of attributes.
        attributes: Vec<(Value, Value)>,
    },
    /// Unordered set value from the server.
    Set(Vec<Value>),
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
    /// Very large number that out of the range of the signed 64 bit numbers
    BigNumber(BigInt),
    /// Push data from the server.
    Push {
        /// Push Kind
        kind: PushKind,
        /// Remaining data from push message
        data: Vec<Value>,
    },
    /// Represents an error message from the server
    ServerError(ServerError),
}

/// `VerbatimString`'s format types defined by spec
#[derive(PartialEq, Clone, Debug)]
pub enum VerbatimFormat {
    /// Unknown type to catch future formats.
    Unknown(String),
    /// `mkd` format
    Markdown,
    /// `txt` format
    Text,
}

/// `Push` type's currently known kinds.
#[derive(PartialEq, Clone, Debug)]
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
            &PushKind::Unsubscribe
                | &PushKind::PUnsubscribe
                | &PushKind::SUnsubscribe
                | &PushKind::Subscribe
                | &PushKind::PSubscribe
                | &PushKind::SSubscribe
        )
    }
}

impl fmt::Display for VerbatimFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VerbatimFormat::Markdown => write!(f, "mkd"),
            VerbatimFormat::Unknown(val) => write!(f, "{val}"),
            VerbatimFormat::Text => write!(f, "txt"),
        }
    }
}

impl fmt::Display for PushKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushKind::Other(kind) => write!(f, "{}", kind),
            PushKind::Invalidate => write!(f, "invalidate"),
            PushKind::Message => write!(f, "message"),
            PushKind::PMessage => write!(f, "pmessage"),
            PushKind::SMessage => write!(f, "smessage"),
            PushKind::Unsubscribe => write!(f, "unsubscribe"),
            PushKind::PUnsubscribe => write!(f, "punsubscribe"),
            PushKind::SUnsubscribe => write!(f, "sunsubscribe"),
            PushKind::Subscribe => write!(f, "subscribe"),
            PushKind::PSubscribe => write!(f, "psubscribe"),
            PushKind::SSubscribe => write!(f, "ssubscribe"),
            PushKind::Disconnection => write!(f, "disconnection"),
        }
    }
}

pub enum MapIter<'a> {
    Array(std::slice::Iter<'a, Value>),
    Map(std::slice::Iter<'a, (Value, Value)>),
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
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            MapIter::Array(iter) => iter.size_hint(),
            MapIter::Map(iter) => iter.size_hint(),
        }
    }
}

pub enum OwnedMapIter {
    Array(std::vec::IntoIter<Value>),
    Map(std::vec::IntoIter<(Value, Value)>),
}

impl Iterator for OwnedMapIter {
    type Item = (Value, Value);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            OwnedMapIter::Array(iter) => Some((iter.next()?, iter.next()?)),
            OwnedMapIter::Map(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            OwnedMapIter::Array(iter) => {
                let (low, high) = iter.size_hint();
                (low / 2, high.map(|h| h / 2))
            }
            OwnedMapIter::Map(iter) => iter.size_hint(),
        }
    }
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
            Value::Array(ref items) => {
                if items.len() != 2 {
                    return false;
                }
                matches!(items[0], Value::BulkString(_)) && matches!(items[1], Value::Array(_))
            }
            _ => false,
        }
    }

    /// Returns an `&[Value]` if `self` is compatible with a sequence type
    pub fn as_sequence(&self) -> Option<&[Value]> {
        match self {
            Value::Array(items) => Some(&items[..]),
            Value::Set(items) => Some(&items[..]),
            Value::Nil => Some(&[]),
            _ => None,
        }
    }

    /// Returns a `Vec<Value>` if `self` is compatible with a sequence type,
    /// otherwise returns `Err(self)`.
    pub fn into_sequence(self) -> Result<Vec<Value>, Value> {
        match self {
            Value::Array(items) => Ok(items),
            Value::Set(items) => Ok(items),
            Value::Nil => Ok(vec![]),
            _ => Err(self),
        }
    }

    /// Returns an iterator of `(&Value, &Value)` if `self` is compatible with a map type
    pub fn as_map_iter(&self) -> Option<MapIter<'_>> {
        match self {
            Value::Array(items) => {
                if items.len() % 2 == 0 {
                    Some(MapIter::Array(items.iter()))
                } else {
                    None
                }
            }
            Value::Map(items) => Some(MapIter::Map(items.iter())),
            _ => None,
        }
    }

    /// Returns an iterator of `(Value, Value)` if `self` is compatible with a map type.
    /// If not, returns `Err(self)`.
    pub fn into_map_iter(self) -> Result<OwnedMapIter, Value> {
        match self {
            Value::Array(items) => {
                if items.len() % 2 == 0 {
                    Ok(OwnedMapIter::Array(items.into_iter()))
                } else {
                    Err(Value::Array(items))
                }
            }
            Value::Map(items) => Ok(OwnedMapIter::Map(items.into_iter())),
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
                Ok(Value::Attribute { data, attributes })
            }
            Self::Set(set) => Ok(Self::Set(Self::extract_error_vec(set)?)),
            Self::Push { kind, data } => Ok(Self::Push {
                kind,
                data: Self::extract_error_vec(data)?,
            }),
            Value::ServerError(err) => Err(err.into()),
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
}

impl fmt::Debug for Value {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Value::Nil => write!(fmt, "nil"),
            Value::Int(val) => write!(fmt, "int({val:?})"),
            Value::BulkString(ref val) => match from_utf8(val) {
                Ok(x) => write!(fmt, "bulk-string('{x:?}')"),
                Err(_) => write!(fmt, "binary-data({val:?})"),
            },
            Value::Array(ref values) => write!(fmt, "array({values:?})"),
            Value::Push { ref kind, ref data } => write!(fmt, "push({kind:?}, {data:?})"),
            Value::Okay => write!(fmt, "ok"),
            Value::SimpleString(ref s) => write!(fmt, "simple-string({s:?})"),
            Value::Map(ref values) => write!(fmt, "map({values:?})"),
            Value::Attribute {
                ref data,
                attributes: _,
            } => write!(fmt, "attribute({data:?})"),
            Value::Set(ref values) => write!(fmt, "set({values:?})"),
            Value::Double(ref d) => write!(fmt, "double({d:?})"),
            Value::Boolean(ref b) => write!(fmt, "boolean({b:?})"),
            Value::VerbatimString {
                ref format,
                ref text,
            } => {
                write!(fmt, "verbatim-string({:?},{:?})", format, text)
            }
            Value::BigNumber(ref m) => write!(fmt, "big-number({:?})", m),
            Value::ServerError(ref err) => match err.details() {
                Some(details) => write!(fmt, "Server error: `{}: {details}`", err.code()),
                None => write!(fmt, "Server error: `{}`", err.code()),
            },
        }
    }
}

/// Represents a redis error.
///
/// For the most part you should be using the Error trait to interact with this
/// rather than the actual struct.
pub struct RedisError {
    repr: ErrorRepr,
}

#[cfg(feature = "json")]
impl From<serde_json::Error> for RedisError {
    fn from(serde_err: serde_json::Error) -> RedisError {
        RedisError::from((
            ErrorKind::Serialize,
            "Serialization Error",
            format!("{serde_err}"),
        ))
    }
}

#[derive(Debug)]
enum ErrorRepr {
    WithDescription(ErrorKind, &'static str),
    WithDescriptionAndDetail(ErrorKind, &'static str, String),
    ExtensionError(String, String),
    IoError(io::Error),
}

impl PartialEq for RedisError {
    fn eq(&self, other: &RedisError) -> bool {
        match (&self.repr, &other.repr) {
            (&ErrorRepr::WithDescription(kind_a, _), &ErrorRepr::WithDescription(kind_b, _)) => {
                kind_a == kind_b
            }
            (
                &ErrorRepr::WithDescriptionAndDetail(kind_a, _, _),
                &ErrorRepr::WithDescriptionAndDetail(kind_b, _, _),
            ) => kind_a == kind_b,
            (ErrorRepr::ExtensionError(a, _), ErrorRepr::ExtensionError(b, _)) => *a == *b,
            _ => false,
        }
    }
}

impl From<io::Error> for RedisError {
    fn from(err: io::Error) -> RedisError {
        RedisError {
            repr: ErrorRepr::IoError(err),
        }
    }
}

impl From<Utf8Error> for RedisError {
    fn from(_: Utf8Error) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescription(ErrorKind::TypeError, "Invalid UTF-8"),
        }
    }
}

impl From<NulError> for RedisError {
    fn from(err: NulError) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::TypeError,
                "Value contains interior nul terminator",
                err.to_string(),
            ),
        }
    }
}

#[cfg(feature = "tls-native-tls")]
impl From<native_tls::Error> for RedisError {
    fn from(err: native_tls::Error) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::IoError,
                "TLS error",
                err.to_string(),
            ),
        }
    }
}

#[cfg(feature = "tls-rustls")]
impl From<rustls::Error> for RedisError {
    fn from(err: rustls::Error) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::IoError,
                "TLS error",
                err.to_string(),
            ),
        }
    }
}

#[cfg(feature = "tls-rustls")]
impl From<rustls::pki_types::InvalidDnsNameError> for RedisError {
    fn from(err: rustls::pki_types::InvalidDnsNameError) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::IoError,
                "TLS Error",
                err.to_string(),
            ),
        }
    }
}

#[cfg(feature = "tls-rustls")]
impl From<rustls_native_certs::Error> for RedisError {
    fn from(err: rustls_native_certs::Error) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::IoError,
                "Fetch certs Error",
                err.to_string(),
            ),
        }
    }
}

#[cfg(feature = "uuid")]
impl From<uuid::Error> for RedisError {
    fn from(err: uuid::Error) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::TypeError,
                "Value is not a valid UUID",
                err.to_string(),
            ),
        }
    }
}

impl From<FromUtf8Error> for RedisError {
    fn from(_: FromUtf8Error) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescription(ErrorKind::TypeError, "Cannot convert from UTF-8"),
        }
    }
}

impl From<(ErrorKind, &'static str)> for RedisError {
    fn from((kind, desc): (ErrorKind, &'static str)) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescription(kind, desc),
        }
    }
}

impl From<(ErrorKind, &'static str, String)> for RedisError {
    fn from((kind, desc, detail): (ErrorKind, &'static str, String)) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescriptionAndDetail(kind, desc, detail),
        }
    }
}

impl error::Error for RedisError {
    #[allow(deprecated)]
    fn description(&self) -> &str {
        match self.repr {
            ErrorRepr::WithDescription(_, desc) => desc,
            ErrorRepr::WithDescriptionAndDetail(_, desc, _) => desc,
            ErrorRepr::ExtensionError(_, _) => "extension error",
            ErrorRepr::IoError(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        match self.repr {
            ErrorRepr::IoError(ref err) => Some(err as &dyn error::Error),
            _ => None,
        }
    }

    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self.repr {
            ErrorRepr::IoError(ref err) => Some(err),
            _ => None,
        }
    }
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self.repr {
            ErrorRepr::WithDescription(kind, desc) => {
                desc.fmt(f)?;
                f.write_str("- ")?;
                fmt::Debug::fmt(&kind, f)
            }
            ErrorRepr::WithDescriptionAndDetail(kind, desc, ref detail) => {
                desc.fmt(f)?;
                f.write_str(" - ")?;
                fmt::Debug::fmt(&kind, f)?;
                f.write_str(": ")?;
                detail.fmt(f)
            }
            ErrorRepr::ExtensionError(ref code, ref detail) => {
                code.fmt(f)?;
                f.write_str(": ")?;
                detail.fmt(f)
            }
            ErrorRepr::IoError(ref err) => err.fmt(f),
        }
    }
}

impl fmt::Debug for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        fmt::Display::fmt(self, f)
    }
}

/// What method should be used if retrying this request.
#[non_exhaustive]
pub enum RetryMethod {
    /// Create a fresh connection, since the current connection is no longer usable.
    Reconnect,
    /// Don't retry, this is a permanent error.
    NoRetry,
    /// Retry immediately, this doesn't require a wait.
    RetryImmediately,
    /// Retry after sleeping to avoid overloading the external service.
    WaitAndRetry,
    /// The key has moved to a different node but we have to ask which node, this is only relevant for clusters.
    AskRedirect,
    /// The key has moved to a different node, this is only relevant for clusters.
    MovedRedirect,
    /// Reconnect the initial connection to the master cluster, this is only relevant for clusters.
    ReconnectFromInitialConnections,
}

/// Indicates a general failure in the library.
impl RedisError {
    /// Returns the kind of the error.
    pub fn kind(&self) -> ErrorKind {
        match self.repr {
            ErrorRepr::WithDescription(kind, _)
            | ErrorRepr::WithDescriptionAndDetail(kind, _, _) => kind,
            ErrorRepr::ExtensionError(_, _) => ErrorKind::ExtensionError,
            ErrorRepr::IoError(_) => ErrorKind::IoError,
        }
    }

    /// Returns the error detail.
    pub fn detail(&self) -> Option<&str> {
        match self.repr {
            ErrorRepr::WithDescriptionAndDetail(_, _, ref detail)
            | ErrorRepr::ExtensionError(_, ref detail) => Some(detail.as_str()),
            _ => None,
        }
    }

    /// Returns the raw error code if available.
    pub fn code(&self) -> Option<&str> {
        match self.kind() {
            ErrorKind::ResponseError => Some("ERR"),
            ErrorKind::ExecAbortError => Some("EXECABORT"),
            ErrorKind::BusyLoadingError => Some("LOADING"),
            ErrorKind::NoScriptError => Some("NOSCRIPT"),
            ErrorKind::Moved => Some("MOVED"),
            ErrorKind::Ask => Some("ASK"),
            ErrorKind::TryAgain => Some("TRYAGAIN"),
            ErrorKind::ClusterDown => Some("CLUSTERDOWN"),
            ErrorKind::CrossSlot => Some("CROSSSLOT"),
            ErrorKind::MasterDown => Some("MASTERDOWN"),
            ErrorKind::ReadOnly => Some("READONLY"),
            ErrorKind::NotBusy => Some("NOTBUSY"),
            _ => match self.repr {
                ErrorRepr::ExtensionError(ref code, _) => Some(code),
                _ => None,
            },
        }
    }

    /// Returns the name of the error category for display purposes.
    pub fn category(&self) -> &str {
        match self.kind() {
            ErrorKind::ResponseError => "response error",
            ErrorKind::AuthenticationFailed => "authentication failed",
            ErrorKind::TypeError => "type error",
            ErrorKind::ExecAbortError => "script execution aborted",
            ErrorKind::BusyLoadingError => "busy loading",
            ErrorKind::NoScriptError => "no script",
            ErrorKind::InvalidClientConfig => "invalid client config",
            ErrorKind::Moved => "key moved",
            ErrorKind::Ask => "key moved (ask)",
            ErrorKind::TryAgain => "try again",
            ErrorKind::ClusterDown => "cluster down",
            ErrorKind::CrossSlot => "cross-slot",
            ErrorKind::MasterDown => "master down",
            ErrorKind::IoError => "I/O error",
            ErrorKind::ExtensionError => "extension error",
            ErrorKind::ClientError => "client error",
            ErrorKind::ReadOnly => "read-only",
            ErrorKind::MasterNameNotFoundBySentinel => "master name not found by sentinel",
            ErrorKind::NoValidReplicasFoundBySentinel => "no valid replicas found by sentinel",
            ErrorKind::EmptySentinelList => "empty sentinel list",
            ErrorKind::NotBusy => "not busy",
            ErrorKind::ClusterConnectionNotFound => "connection to node in cluster not found",
            #[cfg(feature = "json")]
            ErrorKind::Serialize => "serializing",
            ErrorKind::RESP3NotSupported => "resp3 is not supported by server",
            ErrorKind::ParseError => "parse error",
            ErrorKind::NoSub => {
                "Server declined unsubscribe related command in non-subscribed mode"
            }
        }
    }

    /// Indicates that this failure is an IO failure.
    pub fn is_io_error(&self) -> bool {
        self.kind() == ErrorKind::IoError
    }

    pub(crate) fn as_io_error(&self) -> Option<&io::Error> {
        match &self.repr {
            ErrorRepr::IoError(e) => Some(e),
            _ => None,
        }
    }

    /// Indicates that this is a cluster error.
    pub fn is_cluster_error(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::Moved | ErrorKind::Ask | ErrorKind::TryAgain | ErrorKind::ClusterDown
        )
    }

    /// Returns true if this error indicates that the connection was
    /// refused.  You should generally not rely much on this function
    /// unless you are writing unit tests that want to detect if a
    /// local server is available.
    pub fn is_connection_refusal(&self) -> bool {
        match self.repr {
            ErrorRepr::IoError(ref err) => {
                #[allow(clippy::match_like_matches_macro)]
                match err.kind() {
                    io::ErrorKind::ConnectionRefused => true,
                    // if we connect to a unix socket and the file does not
                    // exist yet, then we want to treat this as if it was a
                    // connection refusal.
                    io::ErrorKind::NotFound => cfg!(unix),
                    _ => false,
                }
            }
            _ => false,
        }
    }

    /// Returns true if error was caused by I/O time out.
    /// Note that this may not be accurate depending on platform.
    pub fn is_timeout(&self) -> bool {
        match self.repr {
            ErrorRepr::IoError(ref err) => matches!(
                err.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ),
            _ => false,
        }
    }

    /// Returns true if error was caused by a dropped connection.
    pub fn is_connection_dropped(&self) -> bool {
        match self.repr {
            ErrorRepr::IoError(ref err) => matches!(
                err.kind(),
                io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::UnexpectedEof
            ),
            _ => false,
        }
    }

    /// Returns true if the error is likely to not be recoverable, and the connection must be replaced.
    pub fn is_unrecoverable_error(&self) -> bool {
        match self.retry_method() {
            RetryMethod::Reconnect => true,
            RetryMethod::ReconnectFromInitialConnections => true,

            RetryMethod::NoRetry => false,
            RetryMethod::RetryImmediately => false,
            RetryMethod::WaitAndRetry => false,
            RetryMethod::AskRedirect => false,
            RetryMethod::MovedRedirect => false,
        }
    }

    /// Returns the node the error refers to.
    ///
    /// This returns `(addr, slot_id)`.
    pub fn redirect_node(&self) -> Option<(&str, u16)> {
        match self.kind() {
            ErrorKind::Ask | ErrorKind::Moved => (),
            _ => return None,
        }
        let mut iter = self.detail()?.split_ascii_whitespace();
        let slot_id: u16 = iter.next()?.parse().ok()?;
        let addr = iter.next()?;
        Some((addr, slot_id))
    }

    /// Returns the extension error code.
    ///
    /// This method should not be used because every time the redis library
    /// adds support for a new error code it would disappear form this method.
    /// `code()` always returns the code.
    #[deprecated(note = "use code() instead")]
    pub fn extension_error_code(&self) -> Option<&str> {
        match self.repr {
            ErrorRepr::ExtensionError(ref code, _) => Some(code),
            _ => None,
        }
    }

    /// Clone the `RedisError`, throwing away non-cloneable parts of an `IoError`.
    ///
    /// Deriving `Clone` is not possible because the wrapped `io::Error` is not
    /// cloneable.
    ///
    /// The `ioerror_description` parameter will be prepended to the message in
    /// case an `IoError` is found.
    #[cfg(feature = "connection-manager")] // Used to avoid "unused method" warning
    pub(crate) fn clone_mostly(&self, ioerror_description: &'static str) -> Self {
        let repr = match self.repr {
            ErrorRepr::WithDescription(kind, desc) => ErrorRepr::WithDescription(kind, desc),
            ErrorRepr::WithDescriptionAndDetail(kind, desc, ref detail) => {
                ErrorRepr::WithDescriptionAndDetail(kind, desc, detail.clone())
            }
            ErrorRepr::ExtensionError(ref code, ref detail) => {
                ErrorRepr::ExtensionError(code.clone(), detail.clone())
            }
            ErrorRepr::IoError(ref e) => ErrorRepr::IoError(io::Error::new(
                e.kind(),
                format!("{ioerror_description}: {e}"),
            )),
        };
        Self { repr }
    }

    /// Specifies what method (if any) should be used to retry this request.
    ///
    /// If you are using the cluster api retrying of requests is already handled by the library.
    ///
    /// This isn't precise, and internally the library uses multiple other considerations rather
    /// than just the error kind on when to retry.
    pub fn retry_method(&self) -> RetryMethod {
        match self.kind() {
            ErrorKind::Moved => RetryMethod::MovedRedirect,
            ErrorKind::Ask => RetryMethod::AskRedirect,

            ErrorKind::TryAgain => RetryMethod::WaitAndRetry,
            ErrorKind::MasterDown => RetryMethod::WaitAndRetry,
            ErrorKind::ClusterDown => RetryMethod::WaitAndRetry,
            ErrorKind::BusyLoadingError => RetryMethod::WaitAndRetry,
            ErrorKind::MasterNameNotFoundBySentinel => RetryMethod::WaitAndRetry,
            ErrorKind::NoValidReplicasFoundBySentinel => RetryMethod::WaitAndRetry,

            ErrorKind::ResponseError => RetryMethod::NoRetry,
            ErrorKind::ReadOnly => RetryMethod::NoRetry,
            ErrorKind::ExtensionError => RetryMethod::NoRetry,
            ErrorKind::ExecAbortError => RetryMethod::NoRetry,
            ErrorKind::TypeError => RetryMethod::NoRetry,
            ErrorKind::NoScriptError => RetryMethod::NoRetry,
            ErrorKind::InvalidClientConfig => RetryMethod::NoRetry,
            ErrorKind::CrossSlot => RetryMethod::NoRetry,
            ErrorKind::ClientError => RetryMethod::NoRetry,
            ErrorKind::EmptySentinelList => RetryMethod::NoRetry,
            ErrorKind::NotBusy => RetryMethod::NoRetry,
            #[cfg(feature = "json")]
            ErrorKind::Serialize => RetryMethod::NoRetry,
            ErrorKind::RESP3NotSupported => RetryMethod::NoRetry,
            ErrorKind::NoSub => RetryMethod::NoRetry,

            ErrorKind::ParseError => RetryMethod::Reconnect,
            ErrorKind::AuthenticationFailed => RetryMethod::Reconnect,
            ErrorKind::ClusterConnectionNotFound => RetryMethod::ReconnectFromInitialConnections,

            ErrorKind::IoError => match &self.repr {
                ErrorRepr::IoError(err) => match err.kind() {
                    io::ErrorKind::ConnectionRefused => RetryMethod::Reconnect,
                    io::ErrorKind::NotFound => RetryMethod::Reconnect,
                    io::ErrorKind::ConnectionReset => RetryMethod::Reconnect,
                    io::ErrorKind::ConnectionAborted => RetryMethod::Reconnect,
                    io::ErrorKind::NotConnected => RetryMethod::Reconnect,
                    io::ErrorKind::BrokenPipe => RetryMethod::Reconnect,
                    io::ErrorKind::UnexpectedEof => RetryMethod::Reconnect,

                    io::ErrorKind::PermissionDenied => RetryMethod::NoRetry,
                    io::ErrorKind::Unsupported => RetryMethod::NoRetry,

                    _ => RetryMethod::RetryImmediately,
                },
                _ => RetryMethod::RetryImmediately,
            },
        }
    }
}

/// Creates a new Redis error with the `ExtensionError` kind.
///
/// This function is used to create Redis errors for extension error codes
/// that are not directly understood by the library.
///
/// # Arguments
///
/// * `code` - The error code string returned by the Redis server
/// * `detail` - Optional detailed error message. If None, a default message is used.
///
/// # Returns
///
/// A `RedisError` with the `ExtensionError` kind.
pub fn make_extension_error(code: String, detail: Option<String>) -> RedisError {
    RedisError {
        repr: ErrorRepr::ExtensionError(
            code,
            match detail {
                Some(x) => x,
                None => "Unknown extension error encountered".to_string(),
            },
        ),
    }
}

/// Library generic result type.
pub type RedisResult<T> = Result<T, RedisError>;

/// Library generic future type.
#[cfg(feature = "aio")]
pub type RedisFuture<'a, T> = futures_util::future::BoxFuture<'a, RedisResult<T>>;

/// An info dictionary type.
#[derive(Debug, Clone)]
pub struct InfoDict {
    map: HashMap<String, Value>,
}

/// This type provides convenient access to key/value data returned by
/// the "INFO" command.  It acts like a regular mapping but also has
/// a convenience method `get` which can return data in the appropriate
/// type.
///
/// For instance this can be used to query the server for the role it's
/// in (master, slave) etc:
///
/// ```rust,no_run
/// # fn do_something() -> redis::RedisResult<()> {
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let mut con = client.get_connection().unwrap();
/// let info : redis::InfoDict = redis::cmd("INFO").query(&mut con)?;
/// let role : Option<String> = info.get("role");
/// # Ok(()) }
/// ```
impl InfoDict {
    /// Creates a new info dictionary from a string in the response of
    /// the INFO command.  Each line is a key, value pair with the
    /// key and value separated by a colon (`:`).  Lines starting with a
    /// hash (`#`) are ignored.
    pub fn new(kvpairs: &str) -> InfoDict {
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
        InfoDict { map }
    }

    /// Fetches a value by key and converts it into the given type.
    /// Typical types are `String`, `bool` and integer types.
    pub fn get<T: FromRedisValue>(&self, key: &str) -> Option<T> {
        match self.find(&key) {
            Some(x) => from_redis_value(x).ok(),
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

/// High level representation of response to the [`ROLE`][1] command.
///
/// [1]: https://redis.io/docs/latest/commands/role/
#[derive(Debug, Clone, Eq, PartialEq)]
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

/// Replication information for a replica, as returned by the [`ROLE`][1] command.
///
/// [1]: https://redis.io/docs/latest/commands/role/
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReplicaInfo {
    /// The IP of the replica.
    pub ip: String,
    /// The port of the replica.
    pub port: u16,
    /// The last acknowledged replication offset.
    pub replication_offset: i64,
}

impl FromRedisValue for ReplicaInfo {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        Self::from_owned_redis_value(v.clone())
    }

    fn from_owned_redis_value(v: Value) -> RedisResult<Self> {
        let v = match get_owned_inner_value(v).into_sequence() {
            Ok(v) => v,
            Err(v) => invalid_type_error!(v, "Replica response should be an array"),
        };
        if v.len() < 3 {
            invalid_type_error!(v, "Replica array is too short, expected 3 elements")
        }
        let mut v = v.into_iter();
        let ip = from_owned_redis_value(v.next().expect("len was checked"))?;
        let port = from_owned_redis_value(v.next().expect("len was checked"))?;
        let offset = from_owned_redis_value(v.next().expect("len was checked"))?;
        Ok(ReplicaInfo {
            ip,
            port,
            replication_offset: offset,
        })
    }
}

impl FromRedisValue for Role {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        Self::from_owned_redis_value(v.clone())
    }

    fn from_owned_redis_value(v: Value) -> RedisResult<Self> {
        let v = match get_owned_inner_value(v).into_sequence() {
            Ok(v) => v,
            Err(v) => invalid_type_error!(v, "Role response should be an array"),
        };
        if v.len() < 2 {
            invalid_type_error!(v, "Role array is too short, expected at least 2 elements")
        }
        match &v[0] {
            Value::BulkString(role) => match role.as_slice() {
                b"master" => Role::new_primary(v),
                b"slave" => Role::new_replica(v),
                b"sentinel" => Role::new_sentinel(v),
                _ => invalid_type_error!(v, "Role type is not master, slave or sentinel"),
            },
            _ => invalid_type_error!(v, "Role type is not a bulk string"),
        }
    }
}

impl Role {
    fn new_primary(values: Vec<Value>) -> RedisResult<Self> {
        if values.len() < 3 {
            invalid_type_error!(
                values,
                "Role primary response too short, expected 3 elements"
            )
        }

        let mut values = values.into_iter();
        _ = values.next();

        let replication_offset = from_owned_redis_value(values.next().expect("len was checked"))?;
        let replicas = from_owned_redis_value(values.next().expect("len was checked"))?;

        Ok(Role::Primary {
            replication_offset,
            replicas,
        })
    }

    fn new_replica(values: Vec<Value>) -> RedisResult<Self> {
        if values.len() < 5 {
            invalid_type_error!(
                values,
                "Role replica response too short, expected 5 elements"
            )
        }

        let mut values = values.into_iter();
        _ = values.next();

        let primary_ip = from_owned_redis_value(values.next().expect("len was checked"))?;
        let primary_port = from_owned_redis_value(values.next().expect("len was checked"))?;
        let replication_state = from_owned_redis_value(values.next().expect("len was checked"))?;
        let data_received = from_owned_redis_value(values.next().expect("len was checked"))?;

        Ok(Role::Replica {
            primary_ip,
            primary_port,
            replication_state,
            data_received,
        })
    }

    fn new_sentinel(values: Vec<Value>) -> RedisResult<Self> {
        if values.len() < 2 {
            invalid_type_error!(
                values,
                "Role sentinel response too short, expected at least 2 elements"
            )
        }
        let second_val = values.into_iter().nth(1).expect("len was checked");
        let primary_names = from_owned_redis_value(second_val)?;
        Ok(Role::Sentinel { primary_names })
    }
}

/// Abstraction trait for redis command abstractions.
pub trait RedisWrite {
    /// Accepts a serialized redis command.
    fn write_arg(&mut self, arg: &[u8]);

    /// Accepts a serialized redis command.
    fn write_arg_fmt(&mut self, arg: impl fmt::Display) {
        self.write_arg(arg.to_string().as_bytes())
    }

    /// Appends an empty argument to the command, and returns a
    /// [`std::io::Write`] instance that can write to it.
    ///
    /// Writing multiple arguments into this buffer is unsupported. The resulting
    /// data will be interpreted as one argument by Redis.
    ///
    /// Writing no data is supported and is similar to having an empty bytestring
    /// as an argument.
    fn writer_for_next_arg(&mut self) -> impl std::io::Write + '_;

    /// Reserve space for `additional` arguments in the command
    ///
    /// `additional` is a list of the byte sizes of the arguments.
    ///
    /// # Examples
    /// Sending some Protobufs with `prost` to Redis.
    /// ```rust,ignore
    /// use prost::Message;
    ///
    /// let to_send: Vec<SomeType> = todo!();
    /// let mut cmd = Cmd::new();
    ///
    /// // Calculate and reserve the space for the args
    /// cmd.reserve_space_for_args(to_send.iter().map(Message::encoded_len));
    ///
    /// // Write the args to the buffer
    /// for arg in to_send {
    ///     // Encode the type directly into the Cmd buffer
    ///     // Supplying the required capacity again is not needed for Cmd,
    ///     // but can be useful for other implementers like Vec<Vec<u8>>.
    ///     arg.encode(cmd.bufmut_for_next_arg(arg.encoded_len()));
    /// }
    ///
    /// ```
    ///
    /// # Implementation note
    /// The default implementation provided by this trait is a no-op. It's therefore strongly
    /// recommended to implement this function. Depending on the internal buffer it might only
    /// be possible to use the numbers of arguments (`additional.len()`) or the total expected
    /// capacity (`additional.iter().sum()`). Implementors should assume that the caller will
    /// be wrong and might over or under specify the amount of arguments and space required.
    fn reserve_space_for_args(&mut self, additional: impl IntoIterator<Item = usize>) {
        // _additional would show up in the documentation, so we assign it
        // to make it used.
        let _do_nothing = additional;
    }

    #[cfg(feature = "bytes")]
    /// Appends an empty argument to the command, and returns a
    /// [`bytes::BufMut`] instance that can write to it.
    ///
    /// `capacity` should be equal or greater to the amount of bytes
    /// expected, as some implementations might not be able to resize
    /// the returned buffer.
    ///
    /// Writing multiple arguments into this buffer is unsupported. The resulting
    /// data will be interpreted as one argument by Redis.
    ///
    /// Writing no data is supported and is similar to having an empty bytestring
    /// as an argument.
    fn bufmut_for_next_arg(&mut self, capacity: usize) -> impl bytes::BufMut + '_ {
        // This default implementation is not the most efficient, but does
        // allow for implementers to skip this function. This means that
        // upstream libraries that implement this trait don't suddenly
        // stop working because someone enabled one of the async features.

        /// Has a temporary buffer that is written to [`writer_for_next_arg`]
        /// on drop.
        struct Wrapper<'a> {
            /// The buffer, implements [`bytes::BufMut`] allowing passthrough
            buf: Vec<u8>,
            /// The writer to the command, used on drop
            writer: Box<dyn std::io::Write + 'a>,
        }
        unsafe impl bytes::BufMut for Wrapper<'_> {
            fn remaining_mut(&self) -> usize {
                self.buf.remaining_mut()
            }

            unsafe fn advance_mut(&mut self, cnt: usize) {
                self.buf.advance_mut(cnt);
            }

            fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
                self.buf.chunk_mut()
            }

            // Vec specializes these methods, so we do too
            fn put<T: bytes::buf::Buf>(&mut self, src: T)
            where
                Self: Sized,
            {
                self.buf.put(src);
            }

            fn put_slice(&mut self, src: &[u8]) {
                self.buf.put_slice(src);
            }

            fn put_bytes(&mut self, val: u8, cnt: usize) {
                self.buf.put_bytes(val, cnt);
            }
        }
        impl Drop for Wrapper<'_> {
            fn drop(&mut self) {
                self.writer.write_all(&self.buf).unwrap()
            }
        }

        Wrapper {
            buf: Vec::with_capacity(capacity),
            writer: Box::new(self.writer_for_next_arg()),
        }
    }
}

impl RedisWrite for Vec<Vec<u8>> {
    fn write_arg(&mut self, arg: &[u8]) {
        self.push(arg.to_owned());
    }

    fn write_arg_fmt(&mut self, arg: impl fmt::Display) {
        self.push(arg.to_string().into_bytes())
    }

    fn writer_for_next_arg(&mut self) -> impl std::io::Write + '_ {
        self.push(Vec::new());
        self.last_mut().unwrap()
    }

    fn reserve_space_for_args(&mut self, additional: impl IntoIterator<Item = usize>) {
        // It would be nice to do this, but there's no way to store where we currently are.
        // Checking for the first empty Vec is not possible, as it's valid to write empty args.
        // self.extend(additional.iter().copied().map(Vec::with_capacity));
        // So we just reserve space for the extra args and have to forgo the extra optimisation
        self.reserve(additional.into_iter().count());
    }

    #[cfg(feature = "bytes")]
    fn bufmut_for_next_arg(&mut self, capacity: usize) -> impl bytes::BufMut + '_ {
        self.push(Vec::with_capacity(capacity));
        self.last_mut().unwrap()
    }
}

/// Used to convert a value into one or multiple redis argument
/// strings.  Most values will produce exactly one item but in
/// some cases it might make sense to produce more than one.
pub trait ToRedisArgs: Sized {
    /// This converts the value into a vector of bytes.  Each item
    /// is a single argument.  Most items generate a vector of a
    /// single item.
    ///
    /// The exception to this rule currently are vectors of items.
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        self.write_redis_args(&mut out);
        out
    }

    /// This writes the value into a vector of bytes.  Each item
    /// is a single argument.  Most items generate a single item.
    ///
    /// The exception to this rule currently are vectors of items.
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite;

    /// Returns an information about the contained value with regards
    /// to it's numeric behavior in a redis context.  This is used in
    /// some high level concepts to switch between different implementations
    /// of redis functions (for instance `INCR` vs `INCRBYFLOAT`).
    fn describe_numeric_behavior(&self) -> NumericBehavior {
        NumericBehavior::NonNumeric
    }

    /// Returns the number of arguments this value will generate.
    ///
    /// This is used in some high level functions to intelligently switch
    /// between `GET` and `MGET` variants. Also, for some commands like HEXPIREDAT
    /// which require a specific number of arguments, this method can be used to
    /// know the number of arguments.
    fn num_of_args(&self) -> usize {
        1
    }

    /// This only exists internally as a workaround for the lack of
    /// specialization.
    #[doc(hidden)]
    fn write_args_from_slice<W>(items: &[Self], out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        Self::make_arg_iter_ref(items.iter(), out)
    }

    /// This only exists internally as a workaround for the lack of
    /// specialization.
    #[doc(hidden)]
    fn make_arg_iter_ref<'a, I, W>(items: I, out: &mut W)
    where
        W: ?Sized + RedisWrite,
        I: Iterator<Item = &'a Self>,
        Self: 'a,
    {
        for item in items {
            item.write_redis_args(out);
        }
    }

    #[doc(hidden)]
    fn is_single_vec_arg(items: &[Self]) -> bool {
        items.len() == 1 && items[0].num_of_args() <= 1
    }
}

macro_rules! itoa_based_to_redis_impl {
    ($t:ty, $numeric:expr) => {
        impl ToRedisArgs for $t {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                let mut buf = ::itoa::Buffer::new();
                let s = buf.format(*self);
                out.write_arg(s.as_bytes())
            }

            fn describe_numeric_behavior(&self) -> NumericBehavior {
                $numeric
            }
        }
    };
}

macro_rules! non_zero_itoa_based_to_redis_impl {
    ($t:ty, $numeric:expr) => {
        impl ToRedisArgs for $t {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                let mut buf = ::itoa::Buffer::new();
                let s = buf.format(self.get());
                out.write_arg(s.as_bytes())
            }

            fn describe_numeric_behavior(&self) -> NumericBehavior {
                $numeric
            }
        }
    };
}

macro_rules! ryu_based_to_redis_impl {
    ($t:ty, $numeric:expr) => {
        impl ToRedisArgs for $t {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                let mut buf = ::ryu::Buffer::new();
                let s = buf.format(*self);
                out.write_arg(s.as_bytes())
            }

            fn describe_numeric_behavior(&self) -> NumericBehavior {
                $numeric
            }
        }
    };
}

impl ToRedisArgs for u8 {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let mut buf = ::itoa::Buffer::new();
        let s = buf.format(*self);
        out.write_arg(s.as_bytes())
    }

    fn write_args_from_slice<W>(items: &[u8], out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(items);
    }

    fn is_single_vec_arg(_items: &[u8]) -> bool {
        true
    }
}

itoa_based_to_redis_impl!(i8, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(i16, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(u16, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(i32, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(u32, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(i64, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(u64, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(isize, NumericBehavior::NumberIsInteger);
itoa_based_to_redis_impl!(usize, NumericBehavior::NumberIsInteger);

non_zero_itoa_based_to_redis_impl!(core::num::NonZeroU8, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroI8, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroU16, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroI16, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroU32, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroI32, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroU64, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroI64, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroUsize, NumericBehavior::NumberIsInteger);
non_zero_itoa_based_to_redis_impl!(core::num::NonZeroIsize, NumericBehavior::NumberIsInteger);

ryu_based_to_redis_impl!(f32, NumericBehavior::NumberIsFloat);
ryu_based_to_redis_impl!(f64, NumericBehavior::NumberIsFloat);

#[cfg(any(
    feature = "rust_decimal",
    feature = "bigdecimal",
    feature = "num-bigint"
))]
macro_rules! bignum_to_redis_impl {
    ($t:ty) => {
        impl ToRedisArgs for $t {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                out.write_arg(&self.to_string().into_bytes())
            }
        }
    };
}

#[cfg(feature = "rust_decimal")]
bignum_to_redis_impl!(rust_decimal::Decimal);
#[cfg(feature = "bigdecimal")]
bignum_to_redis_impl!(bigdecimal::BigDecimal);
#[cfg(feature = "num-bigint")]
bignum_to_redis_impl!(num_bigint::BigInt);
#[cfg(feature = "num-bigint")]
bignum_to_redis_impl!(num_bigint::BigUint);

impl ToRedisArgs for bool {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(if *self { b"1" } else { b"0" })
    }
}

impl ToRedisArgs for String {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.as_bytes())
    }
}

impl ToRedisArgs for &str {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.as_bytes())
    }
}

impl<'a, T> ToRedisArgs for Cow<'a, T>
where
    T: ToOwned + ?Sized,
    &'a T: ToRedisArgs,
    for<'b> &'b T::Owned: ToRedisArgs,
{
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Cow::Borrowed(inner) => inner.write_redis_args(out),
            Cow::Owned(inner) => inner.write_redis_args(out),
        }
    }
}

impl<T: ToRedisArgs> ToRedisArgs for Vec<T> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        ToRedisArgs::write_args_from_slice(self, out)
    }

    fn num_of_args(&self) -> usize {
        if ToRedisArgs::is_single_vec_arg(&self[..]) {
            return 1;
        }
        if self.len() == 1 {
            self[0].num_of_args()
        } else {
            self.len()
        }
    }
}

impl<T: ToRedisArgs> ToRedisArgs for &[T] {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        ToRedisArgs::write_args_from_slice(self, out)
    }

    fn num_of_args(&self) -> usize {
        if ToRedisArgs::is_single_vec_arg(&self[..]) {
            return 1;
        }
        if self.len() == 1 {
            self[0].num_of_args()
        } else {
            self.len()
        }
    }
}

impl<T: ToRedisArgs> ToRedisArgs for Option<T> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref x) = *self {
            x.write_redis_args(out);
        }
    }

    fn describe_numeric_behavior(&self) -> NumericBehavior {
        match *self {
            Some(ref x) => x.describe_numeric_behavior(),
            None => NumericBehavior::NonNumeric,
        }
    }

    fn num_of_args(&self) -> usize {
        match *self {
            Some(ref x) => x.num_of_args(),
            None => 0,
        }
    }
}

macro_rules! deref_to_write_redis_args_impl {
    (
        $(#[$attr:meta])*
        <$($desc:tt)+
    ) => {
        $(#[$attr])*
        impl <$($desc)+ {
            #[inline]
            fn write_redis_args<W>(&self, out: &mut W)
                where
                W: ?Sized + RedisWrite,
            {
                (**self).write_redis_args(out)
            }

            fn num_of_args(&self) -> usize {
                (**self).num_of_args()
            }

            fn describe_numeric_behavior(&self) -> NumericBehavior {
                (**self).describe_numeric_behavior()
            }
        }
    };
}

deref_to_write_redis_args_impl! {
    <'a, T> ToRedisArgs for &'a T where T: ToRedisArgs
}

deref_to_write_redis_args_impl! {
    <'a, T> ToRedisArgs for &'a mut T where T: ToRedisArgs
}

deref_to_write_redis_args_impl! {
    <T> ToRedisArgs for Box<T> where T: ToRedisArgs
}

deref_to_write_redis_args_impl! {
    <T> ToRedisArgs for std::sync::Arc<T> where T: ToRedisArgs
}

deref_to_write_redis_args_impl! {
    <T> ToRedisArgs for std::rc::Rc<T> where T: ToRedisArgs
}

/// @note: Redis cannot store empty sets so the application has to
/// check whether the set is empty and if so, not attempt to use that
/// result
macro_rules! impl_to_redis_args_for_set {
    (for <$($TypeParam:ident),+> $SetType:ty, where ($($WhereClause:tt)+) ) => {
        impl< $($TypeParam),+ > ToRedisArgs for $SetType
        where
            $($WhereClause)+
        {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                ToRedisArgs::make_arg_iter_ref(self.iter(), out)
            }

            fn num_of_args(&self) -> usize {
                self.len()
            }
        }
    };
}

impl_to_redis_args_for_set!(
    for <T, S> std::collections::HashSet<T, S>,
    where (T: ToRedisArgs + Hash + Eq, S: BuildHasher + Default)
);

impl_to_redis_args_for_set!(
    for <T> std::collections::BTreeSet<T>,
    where (T: ToRedisArgs + Hash + Eq + Ord)
);

#[cfg(feature = "hashbrown")]
impl_to_redis_args_for_set!(
    for <T, S> hashbrown::HashSet<T, S>,
    where (T: ToRedisArgs + Hash + Eq, S: BuildHasher + Default)
);

#[cfg(feature = "ahash")]
impl_to_redis_args_for_set!(
    for <T, S> ahash::AHashSet<T, S>,
    where (T: ToRedisArgs + Hash + Eq, S: BuildHasher + Default)
);

/// @note: Redis cannot store empty maps so the application has to
/// check whether the set is empty and if so, not attempt to use that
/// result
macro_rules! impl_to_redis_args_for_map {
    (
        $(#[$meta:meta])*
        for <$($TypeParam:ident),+> $MapType:ty,
        where ($($WhereClause:tt)+)
    ) => {
        $(#[$meta])*
        impl< $($TypeParam),+ > ToRedisArgs for $MapType
        where
            $($WhereClause)+
        {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                for (key, value) in self {
                    // Ensure key and value produce a single argument each
                    assert!(key.num_of_args() <= 1 && value.num_of_args() <= 1);
                    key.write_redis_args(out);
                    value.write_redis_args(out);
                }
            }

            fn num_of_args(&self) -> usize {
                self.len()
            }
        }
    };
}
impl_to_redis_args_for_map!(
    for <K, V> std::collections::HashMap<K, V>,
    where (K: ToRedisArgs + Hash + Eq + Ord, V: ToRedisArgs)
);

impl_to_redis_args_for_map!(
    /// this flattens BTreeMap into something that goes well with HMSET
    for <K, V> std::collections::BTreeMap<K, V>,
    where (K: ToRedisArgs + Hash + Eq + Ord, V: ToRedisArgs)
);

#[cfg(feature = "hashbrown")]
impl_to_redis_args_for_map!(
    for <K, V> hashbrown::HashMap<K, V>,
    where (K: ToRedisArgs + Hash + Eq + Ord, V: ToRedisArgs)
);

macro_rules! to_redis_args_for_tuple {
    () => ();
    ($(#[$meta:meta],)*$($name:ident,)+) => (
        $(#[$meta])*
        impl<$($name: ToRedisArgs),*> ToRedisArgs for ($($name,)*) {
            // we have local variables named T1 as dummies and those
            // variables are unused.
            #[allow(non_snake_case, unused_variables)]
            fn write_redis_args<W>(&self, out: &mut W) where W: ?Sized + RedisWrite {
                let ($(ref $name,)*) = *self;
                $($name.write_redis_args(out);)*
            }

            #[allow(non_snake_case, unused_variables)]
            fn num_of_args(&self) -> usize {
                let mut n: usize = 0;
                $(let $name = (); n += 1;)*
                n
            }
        }
    )
}

to_redis_args_for_tuple! { #[cfg_attr(docsrs, doc(fake_variadic))], #[doc = "This trait is implemented for tuples up to 12 items long."], T, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }

impl<T: ToRedisArgs, const N: usize> ToRedisArgs for &[T; N] {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        ToRedisArgs::write_args_from_slice(self.as_slice(), out)
    }

    fn num_of_args(&self) -> usize {
        if ToRedisArgs::is_single_vec_arg(&self[..]) {
            return 1;
        }
        if self.len() == 1 {
            self[0].num_of_args()
        } else {
            self.len()
        }
    }
}

fn vec_to_array<T, const N: usize>(items: Vec<T>, original_value: &Value) -> RedisResult<[T; N]> {
    match items.try_into() {
        Ok(array) => Ok(array),
        Err(items) => {
            let msg = format!(
                "Response has wrong dimension, expected {N}, got {}",
                items.len()
            );
            invalid_type_error!(original_value, msg)
        }
    }
}

impl<T: FromRedisValue, const N: usize> FromRedisValue for [T; N] {
    fn from_redis_value(value: &Value) -> RedisResult<[T; N]> {
        match *value {
            Value::BulkString(ref bytes) => match FromRedisValue::from_byte_vec(bytes) {
                Some(items) => vec_to_array(items, value),
                None => {
                    let msg = format!(
                        "Conversion to Array[{}; {N}] failed",
                        std::any::type_name::<T>()
                    );
                    invalid_type_error!(value, msg)
                }
            },
            Value::Array(ref items) => {
                let items = FromRedisValue::from_redis_values(items)?;
                vec_to_array(items, value)
            }
            Value::Nil => vec_to_array(vec![], value),
            _ => invalid_type_error!(value, "Response type not array compatible"),
        }
    }
}

/// This trait is used to convert a redis value into a more appropriate
/// type.
///
/// While a redis `Value` can represent any response that comes
/// back from the redis server, usually you want to map this into something
/// that works better in rust.  For instance you might want to convert the
/// return value into a `String` or an integer.
///
/// This trait is well supported throughout the library and you can
/// implement it for your own types if you want.
///
/// In addition to what you can see from the docs, this is also implemented
/// for tuples up to size 12 and for `Vec<u8>`.
pub trait FromRedisValue: Sized {
    /// Given a redis `Value` this attempts to convert it into the given
    /// destination type.  If that fails because it's not compatible an
    /// appropriate error is generated.
    fn from_redis_value(v: &Value) -> RedisResult<Self>;

    /// Given a redis `Value` this attempts to convert it into the given
    /// destination type.  If that fails because it's not compatible an
    /// appropriate error is generated.
    fn from_owned_redis_value(v: Value) -> RedisResult<Self> {
        // By default, fall back to `from_redis_value`.
        // This function only needs to be implemented if it can benefit
        // from taking `v` by value.
        Self::from_redis_value(&v)
    }

    /// Similar to `from_redis_value` but constructs a vector of objects
    /// from another vector of values.  This primarily exists internally
    /// to customize the behavior for vectors of tuples.
    fn from_redis_values(items: &[Value]) -> RedisResult<Vec<Self>> {
        items.iter().map(FromRedisValue::from_redis_value).collect()
    }

    /// The same as `from_redis_values`, but takes a `Vec<Value>` instead
    /// of a `&[Value]`.
    fn from_owned_redis_values(items: Vec<Value>) -> RedisResult<Vec<Self>> {
        items
            .into_iter()
            .map(FromRedisValue::from_owned_redis_value)
            .collect()
    }

    /// Convert bytes to a single element vector.
    fn from_byte_vec(_vec: &[u8]) -> Option<Vec<Self>> {
        Self::from_owned_redis_value(Value::BulkString(_vec.into()))
            .map(|rv| vec![rv])
            .ok()
    }

    /// Convert bytes to a single element vector.
    fn from_owned_byte_vec(_vec: Vec<u8>) -> RedisResult<Vec<Self>> {
        Self::from_owned_redis_value(Value::BulkString(_vec)).map(|rv| vec![rv])
    }
}

fn get_inner_value(v: &Value) -> &Value {
    if let Value::Attribute {
        data,
        attributes: _,
    } = v
    {
        data.as_ref()
    } else {
        v
    }
}

fn get_owned_inner_value(v: Value) -> Value {
    if let Value::Attribute {
        data,
        attributes: _,
    } = v
    {
        *data
    } else {
        v
    }
}

macro_rules! from_redis_value_for_num_internal {
    ($t:ty, $v:expr) => {{
        let v = if let Value::Attribute {
            data,
            attributes: _,
        } = $v
        {
            data
        } else {
            $v
        };
        match *v {
            Value::Int(val) => Ok(val as $t),
            Value::SimpleString(ref s) => match s.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!(v, "Could not convert from string."),
            },
            Value::BulkString(ref bytes) => match from_utf8(bytes)?.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!(v, "Could not convert from string."),
            },
            Value::Double(val) => Ok(val as $t),
            _ => invalid_type_error!(v, "Response type not convertible to numeric."),
        }
    }};
}

macro_rules! from_redis_value_for_num {
    ($t:ty) => {
        impl FromRedisValue for $t {
            fn from_redis_value(v: &Value) -> RedisResult<$t> {
                from_redis_value_for_num_internal!($t, v)
            }
        }
    };
}

impl FromRedisValue for u8 {
    fn from_redis_value(v: &Value) -> RedisResult<u8> {
        from_redis_value_for_num_internal!(u8, v)
    }

    // this hack allows us to specialize Vec<u8> to work with binary data.
    fn from_byte_vec(vec: &[u8]) -> Option<Vec<u8>> {
        Some(vec.to_vec())
    }
    fn from_owned_byte_vec(vec: Vec<u8>) -> RedisResult<Vec<u8>> {
        Ok(vec)
    }
}

from_redis_value_for_num!(i8);
from_redis_value_for_num!(i16);
from_redis_value_for_num!(u16);
from_redis_value_for_num!(i32);
from_redis_value_for_num!(u32);
from_redis_value_for_num!(i64);
from_redis_value_for_num!(u64);
from_redis_value_for_num!(i128);
from_redis_value_for_num!(u128);
from_redis_value_for_num!(f32);
from_redis_value_for_num!(f64);
from_redis_value_for_num!(isize);
from_redis_value_for_num!(usize);

#[cfg(any(
    feature = "rust_decimal",
    feature = "bigdecimal",
    feature = "num-bigint"
))]
macro_rules! from_redis_value_for_bignum_internal {
    ($t:ty, $v:expr) => {{
        let v = $v;
        match *v {
            Value::Int(val) => <$t>::try_from(val)
                .map_err(|_| invalid_type_error_inner!(v, "Could not convert from integer.")),
            Value::SimpleString(ref s) => match s.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!(v, "Could not convert from string."),
            },
            Value::BulkString(ref bytes) => match from_utf8(bytes)?.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!(v, "Could not convert from string."),
            },
            _ => invalid_type_error!(v, "Response type not convertible to numeric."),
        }
    }};
}

#[cfg(any(
    feature = "rust_decimal",
    feature = "bigdecimal",
    feature = "num-bigint"
))]
macro_rules! from_redis_value_for_bignum {
    ($t:ty) => {
        impl FromRedisValue for $t {
            fn from_redis_value(v: &Value) -> RedisResult<$t> {
                from_redis_value_for_bignum_internal!($t, v)
            }
        }
    };
}

#[cfg(feature = "rust_decimal")]
from_redis_value_for_bignum!(rust_decimal::Decimal);
#[cfg(feature = "bigdecimal")]
from_redis_value_for_bignum!(bigdecimal::BigDecimal);
#[cfg(feature = "num-bigint")]
from_redis_value_for_bignum!(num_bigint::BigInt);
#[cfg(feature = "num-bigint")]
from_redis_value_for_bignum!(num_bigint::BigUint);

impl FromRedisValue for bool {
    fn from_redis_value(v: &Value) -> RedisResult<bool> {
        let v = get_inner_value(v);
        match *v {
            Value::Nil => Ok(false),
            Value::Int(val) => Ok(val != 0),
            Value::SimpleString(ref s) => {
                if &s[..] == "1" {
                    Ok(true)
                } else if &s[..] == "0" {
                    Ok(false)
                } else {
                    invalid_type_error!(v, "Response status not valid boolean");
                }
            }
            Value::BulkString(ref bytes) => {
                if bytes == b"1" {
                    Ok(true)
                } else if bytes == b"0" {
                    Ok(false)
                } else {
                    invalid_type_error!(v, "Response type not bool compatible.");
                }
            }
            Value::Boolean(b) => Ok(b),
            Value::Okay => Ok(true),
            _ => invalid_type_error!(v, "Response type not bool compatible."),
        }
    }
}

impl FromRedisValue for CString {
    fn from_redis_value(v: &Value) -> RedisResult<CString> {
        let v = get_inner_value(v);
        match *v {
            Value::BulkString(ref bytes) => Ok(CString::new(bytes.as_slice())?),
            Value::Okay => Ok(CString::new("OK")?),
            Value::SimpleString(ref val) => Ok(CString::new(val.as_bytes())?),
            _ => invalid_type_error!(v, "Response type not CString compatible."),
        }
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<CString> {
        let v = get_owned_inner_value(v);
        match v {
            Value::BulkString(bytes) => Ok(CString::new(bytes)?),
            Value::Okay => Ok(CString::new("OK")?),
            Value::SimpleString(val) => Ok(CString::new(val)?),
            _ => invalid_type_error!(v, "Response type not CString compatible."),
        }
    }
}

impl FromRedisValue for String {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let v = get_inner_value(v);
        match *v {
            Value::BulkString(ref bytes) => Ok(from_utf8(bytes)?.to_string()),
            Value::Okay => Ok("OK".to_string()),
            Value::SimpleString(ref val) => Ok(val.to_string()),
            Value::VerbatimString {
                format: _,
                ref text,
            } => Ok(text.to_string()),
            Value::Double(ref val) => Ok(val.to_string()),
            Value::Int(val) => Ok(val.to_string()),
            _ => invalid_type_error!(v, "Response type not string compatible."),
        }
    }

    fn from_owned_redis_value(v: Value) -> RedisResult<Self> {
        let v = get_owned_inner_value(v);
        match v {
            Value::BulkString(bytes) => Ok(Self::from_utf8(bytes)?),
            Value::Okay => Ok("OK".to_string()),
            Value::SimpleString(val) => Ok(val),
            Value::VerbatimString { format: _, text } => Ok(text),
            Value::Double(val) => Ok(val.to_string()),
            Value::Int(val) => Ok(val.to_string()),
            _ => invalid_type_error!(v, "Response type not string compatible."),
        }
    }
}

macro_rules! pointer_from_redis_value_impl {
    (
        $(#[$attr:meta])*
        $id:ident, $ty:ty, $func:expr
    ) => {
        $(#[$attr])*
        impl<$id:  FromRedisValue> FromRedisValue for $ty {
            fn from_redis_value(v: &Value) -> RedisResult<Self>
            {
                FromRedisValue::from_redis_value(v).map($func)
            }

            fn from_owned_redis_value(v: Value) -> RedisResult<Self> {
                FromRedisValue::from_owned_redis_value(v).map($func)
            }
        }
    }
}

pointer_from_redis_value_impl!(T, Box<T>, Box::new);
pointer_from_redis_value_impl!(T, std::sync::Arc<T>, std::sync::Arc::new);
pointer_from_redis_value_impl!(T, std::rc::Rc<T>, std::rc::Rc::new);

/// Implement `FromRedisValue` for `$Type` (which should use the generic parameter `$T`).
///
/// The implementation parses the value into a vec, and then passes the value through `$convert`.
/// If `$convert` is omitted, it defaults to `Into::into`.
macro_rules! from_vec_from_redis_value {
    (<$T:ident> $Type:ty) => {
        from_vec_from_redis_value!(<$T> $Type; Into::into);
    };

    (<$T:ident> $Type:ty; $convert:expr) => {
        impl<$T: FromRedisValue> FromRedisValue for $Type {
            fn from_redis_value(v: &Value) -> RedisResult<$Type> {
                match v {
                    // All binary data except u8 will try to parse into a single element vector.
                    // u8 has its own implementation of from_byte_vec.
                    Value::BulkString(bytes) => match FromRedisValue::from_byte_vec(bytes) {
                        Some(x) => Ok($convert(x)),
                        None => invalid_type_error!(
                            v,
                            format!("Conversion to {} failed.", std::any::type_name::<$Type>())
                        ),
                    },
                    Value::Array(items) => FromRedisValue::from_redis_values(items).map($convert),
                    Value::Set(ref items) => FromRedisValue::from_redis_values(items).map($convert),
                    Value::Map(ref items) => {
                        let mut n: Vec<T> = vec![];
                        for item in items {
                            match FromRedisValue::from_redis_value(&Value::Map(vec![item.clone()])) {
                                Ok(v) => {
                                    n.push(v);
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                        }
                        Ok($convert(n))
                    }
                    Value::Nil => Ok($convert(Vec::new())),
                    _ => invalid_type_error!(v, "Response type not vector compatible."),
                }
            }
            fn from_owned_redis_value(v: Value) -> RedisResult<$Type> {
                match v {
                    // Binary data is parsed into a single-element vector, except
                    // for the element type `u8`, which directly consumes the entire
                    // array of bytes.
                    Value::BulkString(bytes) => FromRedisValue::from_owned_byte_vec(bytes).map($convert),
                    Value::Array(items) => FromRedisValue::from_owned_redis_values(items).map($convert),
                    Value::Set(items) => FromRedisValue::from_owned_redis_values(items).map($convert),
                    Value::Map(items) => {
                        let mut n: Vec<T> = vec![];
                        for item in items {
                            match FromRedisValue::from_owned_redis_value(Value::Map(vec![item])) {
                                Ok(v) => {
                                    n.push(v);
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                        }
                        Ok($convert(n))
                    }
                    Value::Nil => Ok($convert(Vec::new())),
                    _ => invalid_type_error!(v, "Response type not vector compatible."),
                }
            }
        }
    };
}

from_vec_from_redis_value!(<T> Vec<T>);
from_vec_from_redis_value!(<T> std::sync::Arc<[T]>);
from_vec_from_redis_value!(<T> Box<[T]>; Vec::into_boxed_slice);

macro_rules! impl_from_redis_value_for_map {
    (for <$($TypeParam:ident),+> $MapType:ty, where ($($WhereClause:tt)+)) => {
        impl< $($TypeParam),+ > FromRedisValue for $MapType
        where
            $($WhereClause)+
        {
            fn from_redis_value(v: &Value) -> RedisResult<$MapType> {
                let v = get_inner_value(v);
                match *v {
                    Value::Nil => Ok(Default::default()),
                    _ => v
                        .as_map_iter()
                        .ok_or_else(|| invalid_type_error_inner!(v, "Response type not map compatible"))?
                        .map(|(k, v)| {
                            Ok((from_redis_value(k)?, from_redis_value(v)?))
                        })
                        .collect(),
                }
            }

            fn from_owned_redis_value(v: Value) -> RedisResult<$MapType> {
                let v = get_owned_inner_value(v);
                match v {
                    Value::Nil => Ok(Default::default()),
                    _ => v
                        .into_map_iter()
                        .map_err(|v| invalid_type_error_inner!(v, "Response type not map compatible"))?
                        .map(|(k, v)| {
                            Ok((from_owned_redis_value(k)?, from_owned_redis_value(v)?))
                        })
                        .collect(),
                }
            }
        }
    };
}

impl_from_redis_value_for_map!(
    for <K, V, S> std::collections::HashMap<K, V, S>,
    where (K: FromRedisValue + Eq + Hash, V: FromRedisValue, S: BuildHasher + Default)
);

#[cfg(feature = "hashbrown")]
impl_from_redis_value_for_map!(
    for <K, V, S> hashbrown::HashMap<K, V, S>,
    where (K: FromRedisValue + Eq + Hash, V: FromRedisValue, S: BuildHasher + Default)
);

#[cfg(feature = "ahash")]
impl_from_redis_value_for_map!(
    for <K, V> ahash::AHashMap<K, V>,
    where (K: FromRedisValue + Eq + Hash, V: FromRedisValue)
);

impl_from_redis_value_for_map!(
    for <K, V> std::collections::BTreeMap<K, V>,
    where (K: FromRedisValue + Eq + Hash + Ord, V: FromRedisValue)
);

macro_rules! impl_from_redis_value_for_set {
    (for <$($TypeParam:ident),+> $SetType:ty, where ($($WhereClause:tt)+)) => {
        impl< $($TypeParam),+ > FromRedisValue for $SetType
        where
            $($WhereClause)+
        {
            fn from_redis_value(v: &Value) -> RedisResult<$SetType> {
                let v = get_inner_value(v);
                let items = v
                    .as_sequence()
                    .ok_or_else(|| invalid_type_error_inner!(v, "Response type not map compatible"))?;
                items.iter().map(|item| from_redis_value(item)).collect()
            }

            fn from_owned_redis_value(v: Value) -> RedisResult<$SetType> {
                let v = get_owned_inner_value(v);
                let items = v
                    .into_sequence()
                    .map_err(|v| invalid_type_error_inner!(v, "Response type not map compatible"))?;
                items
                    .into_iter()
                    .map(|item| from_owned_redis_value(item))
                    .collect()
            }
        }
    };
}

impl_from_redis_value_for_set!(
    for <T, S> std::collections::HashSet<T, S>,
    where (T: FromRedisValue + Eq + Hash, S: BuildHasher + Default)
);

impl_from_redis_value_for_set!(
    for <T> std::collections::BTreeSet<T>,
    where (T: FromRedisValue + Eq + Ord)
);

#[cfg(feature = "hashbrown")]
impl_from_redis_value_for_set!(
    for <T, S> hashbrown::HashSet<T, S>,
    where (T: FromRedisValue + Eq + Hash, S: BuildHasher + Default)
);

#[cfg(feature = "ahash")]
impl_from_redis_value_for_set!(
    for <T> ahash::AHashSet<T>,
    where (T: FromRedisValue + Eq + Hash)
);

impl FromRedisValue for Value {
    fn from_redis_value(v: &Value) -> RedisResult<Value> {
        Ok(v.clone())
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<Self> {
        Ok(v)
    }
}

impl FromRedisValue for () {
    fn from_redis_value(_v: &Value) -> RedisResult<()> {
        Ok(())
    }
}

macro_rules! from_redis_value_for_tuple {
    () => ();
    ($(#[$meta:meta],)*$($name:ident,)+) => (
        $(#[$meta])*
        impl<$($name: FromRedisValue),*> FromRedisValue for ($($name,)*) {
            // we have local variables named T1 as dummies and those
            // variables are unused.
            #[allow(non_snake_case, unused_variables)]
            fn from_redis_value(v: &Value) -> RedisResult<($($name,)*)> {
                let v = get_inner_value(v);
                match *v {
                    Value::Array(ref items) => {
                        // hacky way to count the tuple size
                        let mut n = 0;
                        $(let $name = (); n += 1;)*
                        if items.len() != n {
                            invalid_type_error!(v, "Array response of wrong dimension")
                        }

                        // this is pretty ugly too.  The { i += 1; i - 1} is rust's
                        // postfix increment :)
                        let mut i = 0;
                        Ok(($({let $name = (); from_redis_value(
                             &items[{ i += 1; i - 1 }])?},)*))
                    }

                    Value::Map(ref items) => {
                        // hacky way to count the tuple size
                        let mut n = 0;
                        $(let $name = (); n += 1;)*
                        if n != 2 {
                            invalid_type_error!(v, "Map response of wrong dimension")
                        }

                        let mut flatten_items = vec![];
                        for (k,v) in items {
                            flatten_items.push(k);
                            flatten_items.push(v);
                        }

                        // this is pretty ugly too.  The { i += 1; i - 1} is rust's
                        // postfix increment :)
                        let mut i = 0;
                        Ok(($({let $name = (); from_redis_value(
                             &flatten_items[{ i += 1; i - 1 }])?},)*))
                    }

                    _ => invalid_type_error!(v, "Not a Array response")
                }
            }

            // we have local variables named T1 as dummies and those
            // variables are unused.
            #[allow(non_snake_case, unused_variables)]
            fn from_owned_redis_value(v: Value) -> RedisResult<($($name,)*)> {
                let v = get_owned_inner_value(v);
                match v {
                    Value::Array(mut items) => {
                        // hacky way to count the tuple size
                        let mut n = 0;
                        $(let $name = (); n += 1;)*
                        if items.len() != n {
                            invalid_type_error!(Value::Array(items), "Array response of wrong dimension")
                        }

                        // this is pretty ugly too.  The { i += 1; i - 1} is rust's
                        // postfix increment :)
                        let mut i = 0;
                        Ok(($({let $name = (); from_owned_redis_value(
                            ::std::mem::replace(&mut items[{ i += 1; i - 1 }], Value::Nil)
                        )?},)*))
                    }

                    Value::Map(items) => {
                        // hacky way to count the tuple size
                        let mut n = 0;
                        $(let $name = (); n += 1;)*
                        if n != 2 {
                            invalid_type_error!(Value::Map(items), "Map response of wrong dimension")
                        }

                        let mut flatten_items = vec![];
                        for (k,v) in items {
                            flatten_items.push(k);
                            flatten_items.push(v);
                        }

                        // this is pretty ugly too.  The { i += 1; i - 1} is rust's
                        // postfix increment :)
                        let mut i = 0;
                        Ok(($({let $name = (); from_redis_value(
                             &flatten_items[{ i += 1; i - 1 }])?},)*))
                    }

                    _ => invalid_type_error!(v, "Not a Array response")
                }
            }

            #[allow(non_snake_case, unused_variables)]
            fn from_redis_values(items: &[Value]) -> RedisResult<Vec<($($name,)*)>> {
                // hacky way to count the tuple size
                let mut n = 0;
                $(let $name = (); n += 1;)*
                let mut rv = vec![];
                if items.len() == 0 {
                    return Ok(rv)
                }
                //It's uglier then before!
                for item in items {
                    match item {
                        Value::Array(ch) => {
                           if  let [$($name),*] = &ch[..] {
                            rv.push(($(from_redis_value(&$name)?),*),)
                           };
                        },
                        _ => {},

                    }
                }
                if !rv.is_empty(){
                    return Ok(rv);
                }

                if let  [$($name),*] = items{
                    rv.push(($(from_redis_value($name)?),*),);
                    return Ok(rv);
                }
                 for chunk in items.chunks_exact(n) {
                    match chunk {
                        [$($name),*] => rv.push(($(from_redis_value($name)?),*),),
                         _ => {},
                    }
                }
                Ok(rv)
            }

            #[allow(non_snake_case, unused_variables)]
            fn from_owned_redis_values(mut items: Vec<Value>) -> RedisResult<Vec<($($name,)*)>> {
                // hacky way to count the tuple size
                let mut n = 0;
                $(let $name = (); n += 1;)*

                let mut rv = vec![];
                if items.len() == 0 {
                    return Ok(rv)
                }
                //It's uglier then before!
                for item in items.iter_mut() {
                    match item {
                        Value::Array(ref mut ch) => {
                        if  let [$($name),*] = &mut ch[..] {
                            rv.push(($(from_owned_redis_value(std::mem::replace($name, Value::Nil))?),*),);
                           };
                        },
                        _ => {},
                    }
                }
                if !rv.is_empty(){
                    return Ok(rv);
                }

                let mut rv = Vec::with_capacity(items.len() / n);
                if items.len() == 0 {
                    return Ok(rv)
                }
                for chunk in items.chunks_mut(n) {
                    match chunk {
                        // Take each element out of the chunk with `std::mem::replace`, leaving a `Value::Nil`
                        // in its place. This allows each `Value` to be parsed without being copied.
                        // Since `items` is consume by this function and not used later, this replacement
                        // is not observable to the rest of the code.
                        [$($name),*] => rv.push(($(from_owned_redis_value(std::mem::replace($name, Value::Nil))?),*),),
                         _ => unreachable!(),
                    }
                }
                Ok(rv)
            }
        }
    )
}

from_redis_value_for_tuple! { #[cfg_attr(docsrs, doc(fake_variadic))], #[doc = "This trait is implemented for tuples up to 12 items long."], T, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }

impl FromRedisValue for InfoDict {
    fn from_redis_value(v: &Value) -> RedisResult<InfoDict> {
        let v = get_inner_value(v);
        let s: String = from_redis_value(v)?;
        Ok(InfoDict::new(&s))
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<InfoDict> {
        let v = get_owned_inner_value(v);
        let s: String = from_owned_redis_value(v)?;
        Ok(InfoDict::new(&s))
    }
}

impl<T: FromRedisValue> FromRedisValue for Option<T> {
    fn from_redis_value(v: &Value) -> RedisResult<Option<T>> {
        let v = get_inner_value(v);
        if *v == Value::Nil {
            return Ok(None);
        }
        Ok(Some(from_redis_value(v)?))
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<Option<T>> {
        let v = get_owned_inner_value(v);
        if v == Value::Nil {
            return Ok(None);
        }
        Ok(Some(from_owned_redis_value(v)?))
    }
}

#[cfg(feature = "bytes")]
impl FromRedisValue for bytes::Bytes {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let v = get_inner_value(v);
        match v {
            Value::BulkString(bytes_vec) => Ok(bytes::Bytes::copy_from_slice(bytes_vec.as_ref())),
            _ => invalid_type_error!(v, "Not a bulk string"),
        }
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<Self> {
        let v = get_owned_inner_value(v);
        match v {
            Value::BulkString(bytes_vec) => Ok(bytes_vec.into()),
            _ => invalid_type_error!(v, "Not a bulk string"),
        }
    }
}

#[cfg(feature = "uuid")]
impl FromRedisValue for uuid::Uuid {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match *v {
            Value::BulkString(ref bytes) => Ok(uuid::Uuid::from_slice(bytes)?),
            _ => invalid_type_error!(v, "Response type not uuid compatible."),
        }
    }
}

#[cfg(feature = "uuid")]
impl ToRedisArgs for uuid::Uuid {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.as_bytes());
    }
}

/// A shortcut function to invoke `FromRedisValue::from_redis_value`
/// to make the API slightly nicer.
pub fn from_redis_value<T: FromRedisValue>(v: &Value) -> RedisResult<T> {
    FromRedisValue::from_redis_value(v)
}

/// A shortcut function to invoke `FromRedisValue::from_owned_redis_value`
/// to make the API slightly nicer.
pub fn from_owned_redis_value<T: FromRedisValue>(v: Value) -> RedisResult<T> {
    FromRedisValue::from_owned_redis_value(v)
}

/// Enum representing the communication protocol with the server.
///
/// This enum represents the types of data that the server can send to the client,
/// and the capabilities that the client can use.
#[derive(Clone, Eq, PartialEq, Default, Debug, Copy)]
pub enum ProtocolVersion {
    /// <https://github.com/redis/redis-specifications/blob/master/protocol/RESP2.md>
    #[default]
    RESP2,
    /// <https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md>
    RESP3,
}

/// Helper enum that is used to define option for the hash expire commands
#[derive(Clone, Copy)]
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
            ExpireOption::NX => out.write_arg(b"NX"),
            ExpireOption::XX => out.write_arg(b"XX"),
            ExpireOption::GT => out.write_arg(b"GT"),
            ExpireOption::LT => out.write_arg(b"LT"),
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A push message from the server.
pub struct PushInfo {
    /// Push Kind
    pub kind: PushKind,
    /// Data from push message
    pub data: Vec<Value>,
}

impl PushInfo {
    pub(crate) fn disconnect() -> Self {
        PushInfo {
            kind: crate::PushKind::Disconnection,
            data: vec![],
        }
    }
}

pub(crate) type SyncPushSender = std::sync::mpsc::Sender<PushInfo>;

// A consistent error value for connections closed without a reason.
#[cfg(any(feature = "aio", feature = "r2d2"))]
pub(crate) fn closed_connection_error() -> RedisError {
    RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe))
}
