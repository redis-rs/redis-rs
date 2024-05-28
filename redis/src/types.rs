use std::collections::{BTreeMap, BTreeSet};
use std::error;
use std::ffi::{CString, NulError};
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::io;
use std::str::{from_utf8, Utf8Error};
use std::string::FromUtf8Error;

#[cfg(feature = "ahash")]
pub(crate) use ahash::{AHashMap as HashMap, AHashSet as HashSet};
#[cfg(not(feature = "ahash"))]
pub(crate) use std::collections::{HashMap, HashSet};
use std::ops::Deref;

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
    EX(usize),
    /// PX milliseconds -- Set the specified expire time, in milliseconds.
    PX(usize),
    /// EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
    EXAT(usize),
    /// PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds.
    PXAT(usize),
    /// PERSIST -- Remove the time to live associated with the key.
    PERSIST,
}

/// Helper enum that is used to define expiry time for SET command
#[derive(Clone, Copy)]
pub enum SetExpiry {
    /// EX seconds -- Set the specified expire time, in seconds.
    EX(usize),
    /// PX milliseconds -- Set the specified expire time, in milliseconds.
    PX(usize),
    /// EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
    EXAT(usize),
    /// PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds.
    PXAT(usize),
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

    #[cfg(feature = "json")]
    /// Error Serializing a struct to JSON form
    Serialize,
}

#[derive(PartialEq, Debug)]
pub(crate) enum ServerErrorKind {
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
}

#[derive(PartialEq, Debug)]
pub(crate) enum ServerError {
    ExtensionError {
        code: String,
        detail: Option<String>,
    },
    KnownError {
        kind: ServerErrorKind,
        detail: Option<String>,
    },
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
#[derive(PartialEq, Debug)]
pub(crate) enum InternalValue {
    /// A nil response from the server.
    Nil,
    /// An integer response.  Note that there are a few situations
    /// in which redis actually returns a string for an integer which
    /// is why this library generally treats integers and strings
    /// the same for all numeric responses.
    Int(i64),
    /// An arbitary binary data.
    Data(Vec<u8>),
    /// A bulk response of more data.  This is generally used by redis
    /// to express nested structures.
    Bulk(Vec<InternalValue>),
    /// A status response.
    Status(String),
    /// A status response which represents the string "OK".
    Okay,
    ServerError(ServerError),
}

impl InternalValue {
    pub(crate) fn into(self) -> RedisResult<Value> {
        match self {
            InternalValue::Nil => Ok(Value::Nil),
            InternalValue::Int(val) => Ok(Value::Int(val)),
            InternalValue::Data(val) => Ok(Value::Data(val)),
            InternalValue::Bulk(val) => Ok(Value::Bulk(
                val.into_iter()
                    .map(InternalValue::into)
                    .collect::<RedisResult<Vec<Value>>>()?,
            )),
            InternalValue::Status(val) => Ok(Value::Status(val)),
            InternalValue::Okay => Ok(Value::Okay),
            InternalValue::ServerError(err) => Err(err.into()),
        }
    }
}

/// Internal low-level redis value enum.
#[derive(PartialEq, Eq, Clone)]
pub enum Value {
    /// A nil response from the server.
    Nil,
    /// An integer response.  Note that there are a few situations
    /// in which redis actually returns a string for an integer which
    /// is why this library generally treats integers and strings
    /// the same for all numeric responses.
    Int(i64),
    /// An arbitary binary data.
    Data(Vec<u8>),
    /// A bulk response of more data.  This is generally used by redis
    /// to express nested structures.
    Bulk(Vec<Value>),
    /// A status response.
    Status(String),
    /// A status response which represents the string "OK".
    Okay,
}

pub struct MapIter<'a>(std::slice::Iter<'a, Value>);

impl<'a> Iterator for MapIter<'a> {
    type Item = (&'a Value, &'a Value);

    fn next(&mut self) -> Option<Self::Item> {
        Some((self.0.next()?, self.0.next()?))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (low, high) = self.0.size_hint();
        (low / 2, high.map(|h| h / 2))
    }
}

pub struct OwnedMapIter(std::vec::IntoIter<Value>);

impl Iterator for OwnedMapIter {
    type Item = (Value, Value);

    fn next(&mut self) -> Option<Self::Item> {
        Some((self.0.next()?, self.0.next()?))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (low, high) = self.0.size_hint();
        (low / 2, high.map(|h| h / 2))
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
    /// protocol.  That means the result is a bulk item of length
    /// two with the first one being a cursor and the second a
    /// bulk response.
    pub fn looks_like_cursor(&self) -> bool {
        match *self {
            Value::Bulk(ref items) => {
                if items.len() != 2 {
                    return false;
                }
                matches!(items[0], Value::Data(_)) && matches!(items[1], Value::Bulk(_))
            }
            _ => false,
        }
    }

    /// Returns an `&[Value]` if `self` is compatible with a sequence type
    pub fn as_sequence(&self) -> Option<&[Value]> {
        match self {
            Value::Bulk(items) => Some(&items[..]),
            Value::Nil => Some(&[]),
            _ => None,
        }
    }

    /// Returns a `Vec<Value>` if `self` is compatible with a sequence type,
    /// otherwise returns `Err(self)`.
    pub fn into_sequence(self) -> Result<Vec<Value>, Value> {
        match self {
            Value::Bulk(items) => Ok(items),
            Value::Nil => Ok(vec![]),
            _ => Err(self),
        }
    }

    /// Returns an iterator of `(&Value, &Value)` if `self` is compatible with a map type
    pub fn as_map_iter(&self) -> Option<MapIter<'_>> {
        match self {
            Value::Bulk(items) => {
                if items.len() % 2 == 0 {
                    Some(MapIter(items.iter()))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Returns an iterator of `(Value, Value)` if `self` is compatible with a map type.
    /// If not, returns `Err(self)`.
    pub fn into_map_iter(self) -> Result<OwnedMapIter, Value> {
        match self {
            Value::Bulk(items) => {
                if items.len() % 2 == 0 {
                    Ok(OwnedMapIter(items.into_iter()))
                } else {
                    Err(Value::Bulk(items))
                }
            }
            _ => Err(self),
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Value::Nil => write!(fmt, "nil"),
            Value::Int(val) => write!(fmt, "int({val:?})"),
            Value::Data(ref val) => match from_utf8(val) {
                Ok(x) => write!(fmt, "string-data('{x:?}')"),
                Err(_) => write!(fmt, "binary-data({val:?})"),
            },
            Value::Bulk(ref values) => {
                write!(fmt, "bulk(")?;
                let mut is_first = true;
                for val in values.iter() {
                    if !is_first {
                        write!(fmt, ", ")?;
                    }
                    write!(fmt, "{val:?}")?;
                    is_first = false;
                }
                write!(fmt, ")")
            }
            Value::Okay => write!(fmt, "ok"),
            Value::Status(ref s) => write!(fmt, "status({s:?})"),
        }
    }
}

/// Represents a redis error.  For the most part you should be using
/// the Error trait to interact with this rather than the actual
/// struct.
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
impl From<rustls_pki_types::InvalidDnsNameError> for RedisError {
    fn from(err: rustls_pki_types::InvalidDnsNameError) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::IoError,
                "TLS Error",
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

pub(crate) enum RetryMethod {
    Reconnect,
    NoRetry,
    RetryImmediately,
    WaitAndRetry,
    AskRedirect,
    MovedRedirect,
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
            ErrorKind::ParseError => "parse error",
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

    pub(crate) fn retry_method(&self) -> RetryMethod {
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

            ErrorKind::ParseError => RetryMethod::Reconnect,
            ErrorKind::AuthenticationFailed => RetryMethod::Reconnect,
            ErrorKind::ClusterConnectionNotFound => RetryMethod::Reconnect,

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
            map.insert(k, Value::Status(v));
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

/// Abstraction trait for redis command abstractions.
pub trait RedisWrite {
    /// Accepts a serialized redis command.
    fn write_arg(&mut self, arg: &[u8]);

    /// Accepts a serialized redis command.
    fn write_arg_fmt(&mut self, arg: impl fmt::Display) {
        self.write_arg(arg.to_string().as_bytes())
    }
}

impl RedisWrite for Vec<Vec<u8>> {
    fn write_arg(&mut self, arg: &[u8]) {
        self.push(arg.to_owned());
    }

    fn write_arg_fmt(&mut self, arg: impl fmt::Display) {
        self.push(arg.to_string().into_bytes())
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

    /// Returns an indiciation if the value contained is exactly one
    /// argument.  It returns false if it's zero or more than one.  This
    /// is used in some high level functions to intelligently switch
    /// between `GET` and `MGET` variants.
    fn is_single_arg(&self) -> bool {
        true
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
        items.len() == 1 && items[0].is_single_arg()
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

impl<'a> ToRedisArgs for &'a str {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.as_bytes())
    }
}

impl<T: ToRedisArgs> ToRedisArgs for Vec<T> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        ToRedisArgs::write_args_from_slice(self, out)
    }

    fn is_single_arg(&self) -> bool {
        ToRedisArgs::is_single_vec_arg(&self[..])
    }
}

impl<'a, T: ToRedisArgs> ToRedisArgs for &'a [T] {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        ToRedisArgs::write_args_from_slice(self, out)
    }

    fn is_single_arg(&self) -> bool {
        ToRedisArgs::is_single_vec_arg(self)
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

    fn is_single_arg(&self) -> bool {
        match *self {
            Some(ref x) => x.is_single_arg(),
            None => false,
        }
    }
}

impl<T: ToRedisArgs> ToRedisArgs for &T {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        (*self).write_redis_args(out)
    }

    fn is_single_arg(&self) -> bool {
        (*self).is_single_arg()
    }
}

/// @note: Redis cannot store empty sets so the application has to
/// check whether the set is empty and if so, not attempt to use that
/// result
impl<T: ToRedisArgs + Hash + Eq, S: BuildHasher + Default> ToRedisArgs
    for std::collections::HashSet<T, S>
{
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        ToRedisArgs::make_arg_iter_ref(self.iter(), out)
    }

    fn is_single_arg(&self) -> bool {
        self.len() <= 1
    }
}

/// @note: Redis cannot store empty sets so the application has to
/// check whether the set is empty and if so, not attempt to use that
/// result
#[cfg(feature = "ahash")]
impl<T: ToRedisArgs + Hash + Eq, S: BuildHasher + Default> ToRedisArgs for ahash::AHashSet<T, S> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        ToRedisArgs::make_arg_iter_ref(self.iter(), out)
    }

    fn is_single_arg(&self) -> bool {
        self.len() <= 1
    }
}

/// @note: Redis cannot store empty sets so the application has to
/// check whether the set is empty and if so, not attempt to use that
/// result
impl<T: ToRedisArgs + Hash + Eq + Ord> ToRedisArgs for BTreeSet<T> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        ToRedisArgs::make_arg_iter_ref(self.iter(), out)
    }

    fn is_single_arg(&self) -> bool {
        self.len() <= 1
    }
}

/// this flattens BTreeMap into something that goes well with HMSET
/// @note: Redis cannot store empty sets so the application has to
/// check whether the set is empty and if so, not attempt to use that
/// result
impl<T: ToRedisArgs + Hash + Eq + Ord, V: ToRedisArgs> ToRedisArgs for BTreeMap<T, V> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        for (key, value) in self {
            // otherwise things like HMSET will simply NOT work
            assert!(key.is_single_arg() && value.is_single_arg());

            key.write_redis_args(out);
            value.write_redis_args(out);
        }
    }

    fn is_single_arg(&self) -> bool {
        self.len() <= 1
    }
}

impl<T: ToRedisArgs + Hash + Eq + Ord, V: ToRedisArgs> ToRedisArgs
    for std::collections::HashMap<T, V>
{
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        for (key, value) in self {
            assert!(key.is_single_arg() && value.is_single_arg());

            key.write_redis_args(out);
            value.write_redis_args(out);
        }
    }

    fn is_single_arg(&self) -> bool {
        self.len() <= 1
    }
}

macro_rules! to_redis_args_for_tuple {
    () => ();
    ($($name:ident,)+) => (
        #[doc(hidden)]
        impl<$($name: ToRedisArgs),*> ToRedisArgs for ($($name,)*) {
            // we have local variables named T1 as dummies and those
            // variables are unused.
            #[allow(non_snake_case, unused_variables)]
            fn write_redis_args<W>(&self, out: &mut W) where W: ?Sized + RedisWrite {
                let ($(ref $name,)*) = *self;
                $($name.write_redis_args(out);)*
            }

            #[allow(non_snake_case, unused_variables)]
            fn is_single_arg(&self) -> bool {
                let mut n = 0u32;
                $(let $name = (); n += 1;)*
                n == 1
            }
        }
        to_redis_args_for_tuple_peel!($($name,)*);
    )
}

/// This chips of the leading one and recurses for the rest.  So if the first
/// iteration was T1, T2, T3 it will recurse to T2, T3.  It stops for tuples
/// of size 1 (does not implement down to unit).
macro_rules! to_redis_args_for_tuple_peel {
    ($name:ident, $($other:ident,)*) => (to_redis_args_for_tuple!($($other,)*);)
}

to_redis_args_for_tuple! { T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }

impl<T: ToRedisArgs, const N: usize> ToRedisArgs for &[T; N] {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        ToRedisArgs::write_args_from_slice(self.as_slice(), out)
    }

    fn is_single_arg(&self) -> bool {
        ToRedisArgs::is_single_vec_arg(self.as_slice())
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
            Value::Data(ref bytes) => match FromRedisValue::from_byte_vec(bytes) {
                Some(items) => vec_to_array(items, value),
                None => {
                    let msg = format!(
                        "Conversion to Array[{}; {N}] failed",
                        std::any::type_name::<T>()
                    );
                    invalid_type_error!(value, msg)
                }
            },
            Value::Bulk(ref items) => {
                let items = FromRedisValue::from_redis_values(items)?;
                vec_to_array(items, value)
            }
            Value::Nil => vec_to_array(vec![], value),
            _ => invalid_type_error!(value, "Response type not array compatible"),
        }
    }
}

/// This trait is used to convert a redis value into a more appropriate
/// type.  While a redis `Value` can represent any response that comes
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
        Self::from_owned_redis_value(Value::Data(_vec.into()))
            .map(|rv| vec![rv])
            .ok()
    }

    /// Convert bytes to a single element vector.
    fn from_owned_byte_vec(_vec: Vec<u8>) -> RedisResult<Vec<Self>> {
        Self::from_owned_redis_value(Value::Data(_vec)).map(|rv| vec![rv])
    }
}

macro_rules! from_redis_value_for_num_internal {
    ($t:ty, $v:expr) => {{
        let v = $v;
        match *v {
            Value::Int(val) => Ok(val as $t),
            Value::Status(ref s) => match s.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!(v, "Could not convert from string."),
            },
            Value::Data(ref bytes) => match from_utf8(bytes)?.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!(v, "Could not convert from string."),
            },
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
            Value::Status(ref s) => match s.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!(v, "Could not convert from string."),
            },
            Value::Data(ref bytes) => match from_utf8(bytes)?.parse::<$t>() {
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
        match *v {
            Value::Nil => Ok(false),
            Value::Int(val) => Ok(val != 0),
            Value::Status(ref s) => {
                if &s[..] == "1" {
                    Ok(true)
                } else if &s[..] == "0" {
                    Ok(false)
                } else {
                    invalid_type_error!(v, "Response status not valid boolean");
                }
            }
            Value::Data(ref bytes) => {
                if bytes == b"1" {
                    Ok(true)
                } else if bytes == b"0" {
                    Ok(false)
                } else {
                    invalid_type_error!(v, "Response type not bool compatible.");
                }
            }
            Value::Okay => Ok(true),
            _ => invalid_type_error!(v, "Response type not bool compatible."),
        }
    }
}

impl FromRedisValue for CString {
    fn from_redis_value(v: &Value) -> RedisResult<CString> {
        match *v {
            Value::Data(ref bytes) => Ok(CString::new(bytes.as_slice())?),
            Value::Okay => Ok(CString::new("OK")?),
            Value::Status(ref val) => Ok(CString::new(val.as_bytes())?),
            _ => invalid_type_error!(v, "Response type not CString compatible."),
        }
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<CString> {
        match v {
            Value::Data(bytes) => Ok(CString::new(bytes)?),
            Value::Okay => Ok(CString::new("OK")?),
            Value::Status(val) => Ok(CString::new(val)?),
            _ => invalid_type_error!(v, "Response type not CString compatible."),
        }
    }
}

impl FromRedisValue for String {
    fn from_redis_value(v: &Value) -> RedisResult<String> {
        match *v {
            Value::Data(ref bytes) => Ok(from_utf8(bytes)?.to_string()),
            Value::Okay => Ok("OK".to_string()),
            Value::Status(ref val) => Ok(val.to_string()),
            _ => invalid_type_error!(v, "Response type not string compatible."),
        }
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<String> {
        match v {
            Value::Data(bytes) => Ok(String::from_utf8(bytes)?),
            Value::Okay => Ok("OK".to_string()),
            Value::Status(val) => Ok(val),
            _ => invalid_type_error!(v, "Response type not string compatible."),
        }
    }
}

/// Implement `FromRedisValue` for `$Type` (which should use the generic parameter `$T`).
///
/// The implementation parses the value into a vec, and then passes the value through `$convert`.
/// If `$convert` is ommited, it defaults to `Into::into`.
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
                    Value::Data(bytes) => match FromRedisValue::from_byte_vec(bytes) {
                        Some(x) => Ok($convert(x)),
                        None => invalid_type_error!(
                            v,
                            format!("Conversion to {} failed.", std::any::type_name::<$Type>())
                        ),
                    },
                    Value::Bulk(items) => FromRedisValue::from_redis_values(items).map($convert),
                    Value::Nil => Ok($convert(Vec::new())),
                    _ => invalid_type_error!(v, "Response type not vector compatible."),
                }
            }
            fn from_owned_redis_value(v: Value) -> RedisResult<$Type> {
                match v {
                    // Binary data is parsed into a single-element vector, except
                    // for the element type `u8`, which directly consumes the entire
                    // array of bytes.
                    Value::Data(bytes) => FromRedisValue::from_owned_byte_vec(bytes).map($convert),
                    Value::Bulk(items) => FromRedisValue::from_owned_redis_values(items).map($convert),
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

impl<K: FromRedisValue + Eq + Hash, V: FromRedisValue, S: BuildHasher + Default> FromRedisValue
    for std::collections::HashMap<K, V, S>
{
    fn from_redis_value(v: &Value) -> RedisResult<std::collections::HashMap<K, V, S>> {
        match *v {
            Value::Nil => Ok(Default::default()),
            _ => v
                .as_map_iter()
                .ok_or_else(|| {
                    invalid_type_error_inner!(v, "Response type not hashmap compatible")
                })?
                .map(|(k, v)| Ok((from_redis_value(k)?, from_redis_value(v)?)))
                .collect(),
        }
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<std::collections::HashMap<K, V, S>> {
        match v {
            Value::Nil => Ok(Default::default()),
            _ => v
                .into_map_iter()
                .map_err(|v| invalid_type_error_inner!(v, "Response type not hashmap compatible"))?
                .map(|(k, v)| Ok((from_owned_redis_value(k)?, from_owned_redis_value(v)?)))
                .collect(),
        }
    }
}

#[cfg(feature = "ahash")]
impl<K: FromRedisValue + Eq + Hash, V: FromRedisValue> FromRedisValue for ahash::AHashMap<K, V> {
    fn from_redis_value(v: &Value) -> RedisResult<ahash::AHashMap<K, V>> {
        match *v {
            Value::Nil => Ok(ahash::AHashMap::with_hasher(Default::default())),
            _ => v
                .as_map_iter()
                .ok_or_else(|| {
                    invalid_type_error_inner!(v, "Response type not hashmap compatible")
                })?
                .map(|(k, v)| Ok((from_redis_value(k)?, from_redis_value(v)?)))
                .collect(),
        }
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<ahash::AHashMap<K, V>> {
        match v {
            Value::Nil => Ok(ahash::AHashMap::with_hasher(Default::default())),
            _ => v
                .into_map_iter()
                .map_err(|v| invalid_type_error_inner!(v, "Response type not hashmap compatible"))?
                .map(|(k, v)| Ok((from_owned_redis_value(k)?, from_owned_redis_value(v)?)))
                .collect(),
        }
    }
}

impl<K: FromRedisValue + Eq + Hash, V: FromRedisValue> FromRedisValue for BTreeMap<K, V>
where
    K: Ord,
{
    fn from_redis_value(v: &Value) -> RedisResult<BTreeMap<K, V>> {
        v.as_map_iter()
            .ok_or_else(|| invalid_type_error_inner!(v, "Response type not btreemap compatible"))?
            .map(|(k, v)| Ok((from_redis_value(k)?, from_redis_value(v)?)))
            .collect()
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<BTreeMap<K, V>> {
        v.into_map_iter()
            .map_err(|v| invalid_type_error_inner!(v, "Response type not btreemap compatible"))?
            .map(|(k, v)| Ok((from_owned_redis_value(k)?, from_owned_redis_value(v)?)))
            .collect()
    }
}

impl<T: FromRedisValue + Eq + Hash, S: BuildHasher + Default> FromRedisValue
    for std::collections::HashSet<T, S>
{
    fn from_redis_value(v: &Value) -> RedisResult<std::collections::HashSet<T, S>> {
        let items = v
            .as_sequence()
            .ok_or_else(|| invalid_type_error_inner!(v, "Response type not hashset compatible"))?;
        items.iter().map(|item| from_redis_value(item)).collect()
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<std::collections::HashSet<T, S>> {
        let items = v
            .into_sequence()
            .map_err(|v| invalid_type_error_inner!(v, "Response type not hashset compatible"))?;
        items
            .into_iter()
            .map(|item| from_owned_redis_value(item))
            .collect()
    }
}

#[cfg(feature = "ahash")]
impl<T: FromRedisValue + Eq + Hash> FromRedisValue for ahash::AHashSet<T> {
    fn from_redis_value(v: &Value) -> RedisResult<ahash::AHashSet<T>> {
        let items = v
            .as_sequence()
            .ok_or_else(|| invalid_type_error_inner!(v, "Response type not hashset compatible"))?;
        items.iter().map(|item| from_redis_value(item)).collect()
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<ahash::AHashSet<T>> {
        let items = v
            .into_sequence()
            .map_err(|v| invalid_type_error_inner!(v, "Response type not hashset compatible"))?;
        items
            .into_iter()
            .map(|item| from_owned_redis_value(item))
            .collect()
    }
}

impl<T: FromRedisValue + Eq + Hash> FromRedisValue for BTreeSet<T>
where
    T: Ord,
{
    fn from_redis_value(v: &Value) -> RedisResult<BTreeSet<T>> {
        let items = v
            .as_sequence()
            .ok_or_else(|| invalid_type_error_inner!(v, "Response type not btreeset compatible"))?;
        items.iter().map(|item| from_redis_value(item)).collect()
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<BTreeSet<T>> {
        let items = v
            .into_sequence()
            .map_err(|v| invalid_type_error_inner!(v, "Response type not btreeset compatible"))?;
        items
            .into_iter()
            .map(|item| from_owned_redis_value(item))
            .collect()
    }
}

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
    ($($name:ident,)+) => (
        #[doc(hidden)]
        impl<$($name: FromRedisValue),*> FromRedisValue for ($($name,)*) {
            // we have local variables named T1 as dummies and those
            // variables are unused.
            #[allow(non_snake_case, unused_variables)]
            fn from_redis_value(v: &Value) -> RedisResult<($($name,)*)> {
                match *v {
                    Value::Bulk(ref items) => {
                        // hacky way to count the tuple size
                        let mut n = 0;
                        $(let $name = (); n += 1;)*
                        if items.len() != n {
                            invalid_type_error!(v, "Bulk response of wrong dimension")
                        }

                        // this is pretty ugly too.  The { i += 1; i - 1} is rust's
                        // postfix increment :)
                        let mut i = 0;
                        Ok(($({let $name = (); from_redis_value(
                             &items[{ i += 1; i - 1 }])?},)*))
                    }
                    _ => invalid_type_error!(v, "Not a bulk response")
                }
            }

            // we have local variables named T1 as dummies and those
            // variables are unused.
            #[allow(non_snake_case, unused_variables)]
            fn from_owned_redis_value(v: Value) -> RedisResult<($($name,)*)> {
                match v {
                    Value::Bulk(mut items) => {
                        // hacky way to count the tuple size
                        let mut n = 0;
                        $(let $name = (); n += 1;)*
                        if items.len() != n {
                            invalid_type_error!(Value::Bulk(items), "Bulk response of wrong dimension")
                        }

                        // this is pretty ugly too.  The { i += 1; i - 1} is rust's
                        // postfix increment :)
                        let mut i = 0;
                        Ok(($({let $name = (); from_owned_redis_value(
                            ::std::mem::replace(&mut items[{ i += 1; i - 1 }], Value::Nil)
                        )?},)*))
                    }
                    _ => invalid_type_error!(v, "Not a bulk response")
                }
            }

            #[allow(non_snake_case, unused_variables)]
            fn from_redis_values(items: &[Value]) -> RedisResult<Vec<($($name,)*)>> {
                // hacky way to count the tuple size
                let mut n = 0;
                $(let $name = (); n += 1;)*
                if items.len() % n != 0 {
                    invalid_type_error!(items, "Bulk response of wrong dimension")
                }

                // this is pretty ugly too.  The { i += 1; i - 1} is rust's
                // postfix increment :)
                let mut rv = vec![];
                if items.len() == 0 {
                    return Ok(rv)
                }
                for chunk in items.chunks_exact(n) {
                    match chunk {
                        [$($name),*] => rv.push(($(from_redis_value($name)?),*),),
                         _ => unreachable!(),
                    }
                }
                Ok(rv)
            }

            #[allow(non_snake_case, unused_variables)]
            fn from_owned_redis_values(mut items: Vec<Value>) -> RedisResult<Vec<($($name,)*)>> {
                // hacky way to count the tuple size
                let mut n = 0;
                $(let $name = (); n += 1;)*
                if items.len() % n != 0 {
                    invalid_type_error!(items, "Bulk response of wrong dimension")
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
        from_redis_value_for_tuple_peel!($($name,)*);
    )
}

/// This chips of the leading one and recurses for the rest.  So if the first
/// iteration was T1, T2, T3 it will recurse to T2, T3.  It stops for tuples
/// of size 1 (does not implement down to unit).
macro_rules! from_redis_value_for_tuple_peel {
    ($name:ident, $($other:ident,)*) => (from_redis_value_for_tuple!($($other,)*);)
}

from_redis_value_for_tuple! { T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }

impl FromRedisValue for InfoDict {
    fn from_redis_value(v: &Value) -> RedisResult<InfoDict> {
        let s: String = from_redis_value(v)?;
        Ok(InfoDict::new(&s))
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<InfoDict> {
        let s: String = from_owned_redis_value(v)?;
        Ok(InfoDict::new(&s))
    }
}

impl<T: FromRedisValue> FromRedisValue for Option<T> {
    fn from_redis_value(v: &Value) -> RedisResult<Option<T>> {
        if *v == Value::Nil {
            return Ok(None);
        }
        Ok(Some(from_redis_value(v)?))
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<Option<T>> {
        if v == Value::Nil {
            return Ok(None);
        }
        Ok(Some(from_owned_redis_value(v)?))
    }
}

#[cfg(feature = "bytes")]
impl FromRedisValue for bytes::Bytes {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Data(bytes_vec) => Ok(bytes::Bytes::copy_from_slice(bytes_vec.as_ref())),
            _ => invalid_type_error!(v, "Not binary data"),
        }
    }
    fn from_owned_redis_value(v: Value) -> RedisResult<Self> {
        match v {
            Value::Data(bytes_vec) => Ok(bytes_vec.into()),
            _ => invalid_type_error!(v, "Not binary data"),
        }
    }
}

#[cfg(feature = "uuid")]
impl FromRedisValue for uuid::Uuid {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match *v {
            Value::Data(ref bytes) => Ok(uuid::Uuid::from_slice(bytes)?),
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

#[cfg(test)]
mod tests {}
