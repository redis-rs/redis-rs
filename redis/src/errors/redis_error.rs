use std::{error, fmt, io};

use arcstr::ArcStr;

use crate::{
    errors::server_error::{ServerError, ServerErrorKind},
    ParsingError,
};

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
    UnexpectedReturnType,
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
    WithDescriptionAndDetail(ErrorKind, &'static str, ArcStr),
    ExtensionError(ArcStr, ArcStr),
    IoError(io::Error),
    ParsingError(ParsingError),
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

#[cfg(feature = "tls-rustls")]
impl From<rustls::pki_types::InvalidDnsNameError> for RedisError {
    fn from(err: rustls::pki_types::InvalidDnsNameError) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::IoError,
                "TLS Error",
                err.to_string().into(),
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
                err.to_string().into(),
            ),
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
            repr: ErrorRepr::WithDescriptionAndDetail(kind, desc, detail.into()),
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
            ErrorRepr::ParsingError(ref err) => err.description(),
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

impl fmt::Debug for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        fmt::Display::fmt(self, f)
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
            ErrorRepr::ParsingError(ref err) => err.fmt(f),
        }
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
            ErrorRepr::ParsingError(_) => ErrorKind::ParseError,
        }
    }

    /// Returns the error detail.
    pub fn detail(&self) -> Option<&str> {
        match self.repr {
            ErrorRepr::WithDescriptionAndDetail(_, _, ref detail)
            | ErrorRepr::ExtensionError(_, ref detail) => Some(detail.as_str()),
            ErrorRepr::ParsingError(ref err) => Some(&err.description),
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
            ErrorKind::UnexpectedReturnType => "type error",
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
            ErrorRepr::ParsingError(ref err) => ErrorRepr::ParsingError(err.clone()),
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
            ErrorKind::UnexpectedReturnType => RetryMethod::NoRetry,
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
            code.into(),
            match detail {
                Some(x) => x.into(),
                None => arcstr::literal!("Unknown extension error encountered"),
            },
        ),
    }
}

#[cfg(feature = "tls-native-tls")]
impl From<native_tls::Error> for RedisError {
    fn from(err: native_tls::Error) -> RedisError {
        RedisError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::IoError,
                "TLS error",
                err.to_string().into(),
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
                err.to_string().into(),
            ),
        }
    }
}

impl From<ServerError> for RedisError {
    fn from(value: ServerError) -> Self {
        // TODO - Consider changing RedisError to explicitly represent whether an error came from the server or not. Today it is only implied.
        match value {
            ServerError::ExtensionError { code, detail } => {
                make_extension_error(code.as_str().into(), detail.map(|str| str.as_str().into()))
            }
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
                    Some(detail) => RedisError::from((kind, desc, detail.as_str().into())),
                    None => RedisError::from((kind, desc)),
                }
            }
        }
    }
}

impl From<ParsingError> for RedisError {
    fn from(err: ParsingError) -> Self {
        RedisError {
            repr: ErrorRepr::ParsingError(err),
        }
    }
}
