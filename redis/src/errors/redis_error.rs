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
    /// The parser failed to parse the server response.
    ParseError,
    /// The authentication with the server failed.
    AuthenticationFailed,
    /// Operation failed because of a type mismatch.
    UnexpectedReturnType,
    /// An error that was caused because the parameter to the
    /// client were wrong.
    InvalidClientConfig,
    /// This kind is returned if the redis error is one that is
    /// not native to the system.  This is usually the case if
    /// the cause is another error.
    IoError,
    /// An error raised that was identified on the client before execution.
    ClientError,
    /// An extension error.  This is an error created by the server
    /// that is not directly understood by the library.
    ExtensionError,
    /// Requested name not found among masters returned by the sentinels
    MasterNameNotFoundBySentinel,
    /// No valid replicas found in the sentinels, for a given master name
    NoValidReplicasFoundBySentinel,
    /// At least one sentinel connection info is required
    EmptySentinelList,
    /// Used when a cluster connection cannot find a connection to a valid node.
    ClusterConnectionNotFound,
    /// An error returned from the server
    ServerError(ServerErrorKind),

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
    ServerError(ServerError),
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
            (ErrorRepr::ParsingError(a), ErrorRepr::ParsingError(b)) => *a == *b,
            (ErrorRepr::ServerError(a), ErrorRepr::ServerError(b)) => *a == *b,
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
        match &self.repr {
            ErrorRepr::WithDescription(_, desc) => desc,
            ErrorRepr::WithDescriptionAndDetail(_, desc, _) => desc,
            ErrorRepr::ExtensionError(_, _) => "extension error",
            ErrorRepr::IoError(err) => err.description(),
            ErrorRepr::ParsingError(err) => err.description(),
            ErrorRepr::ServerError(err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        match self.repr {
            ErrorRepr::IoError(ref err) => Some(err as &dyn error::Error),
            _ => None,
        }
    }

    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self.repr {
            ErrorRepr::IoError(err) => Some(err),
            ErrorRepr::ServerError(err) => Some(err),
            ErrorRepr::ParsingError(err) => Some(err),
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
        match &self.repr {
            ErrorRepr::WithDescription(kind, desc) => {
                desc.fmt(f)?;
                f.write_str("- ")?;
                fmt::Debug::fmt(&kind, f)
            }
            ErrorRepr::WithDescriptionAndDetail(kind, desc, detail) => {
                desc.fmt(f)?;
                f.write_str(" - ")?;
                fmt::Debug::fmt(&kind, f)?;
                f.write_str(": ")?;
                detail.fmt(f)
            }
            ErrorRepr::ExtensionError(code, detail) => {
                code.fmt(f)?;
                f.write_str(": ")?;
                detail.fmt(f)
            }
            ErrorRepr::IoError(err) => err.fmt(f),
            ErrorRepr::ParsingError(err) => err.fmt(f),
            ErrorRepr::ServerError(err) => err.fmt(f),
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
        match &self.repr {
            ErrorRepr::WithDescription(kind, _)
            | ErrorRepr::WithDescriptionAndDetail(kind, _, _) => *kind,
            ErrorRepr::ExtensionError(_, _) => ErrorKind::ExtensionError,
            ErrorRepr::IoError(_) => ErrorKind::IoError,
            ErrorRepr::ParsingError(_) => ErrorKind::ParseError,
            ErrorRepr::ServerError(err) => match err.kind() {
                Some(kind) => ErrorKind::ServerError(kind),
                None => ErrorKind::ExtensionError,
            },
        }
    }

    /// Returns the error detail.
    pub fn detail(&self) -> Option<&str> {
        match &self.repr {
            ErrorRepr::WithDescriptionAndDetail(_, _, detail)
            | ErrorRepr::ExtensionError(_, detail) => Some(detail.as_str()),
            ErrorRepr::ParsingError(err) => Some(&err.description),
            ErrorRepr::ServerError(err) => err.details(),
            _ => None,
        }
    }

    /// Returns the raw error code if available.
    pub fn code(&self) -> Option<&str> {
        match self.kind() {
            ErrorKind::ServerError(kind) => Some(kind.code()),
            _ => match &self.repr {
                ErrorRepr::ExtensionError(code, _) => Some(code),
                ErrorRepr::ServerError(err) => Some(err.code()),
                _ => None,
            },
        }
    }

    /// Returns the name of the error category for display purposes.
    pub fn category(&self) -> &str {
        match self.kind() {
            ErrorKind::ServerError(ServerErrorKind::ResponseError) => "response error",
            ErrorKind::AuthenticationFailed => "authentication failed",
            ErrorKind::UnexpectedReturnType => "type error",
            ErrorKind::ServerError(ServerErrorKind::ExecAbortError) => "script execution aborted",
            ErrorKind::ServerError(ServerErrorKind::BusyLoadingError) => "busy loading",
            ErrorKind::ServerError(ServerErrorKind::NoScriptError) => "no script",
            ErrorKind::InvalidClientConfig => "invalid client config",
            ErrorKind::ServerError(ServerErrorKind::Moved) => "key moved",
            ErrorKind::ServerError(ServerErrorKind::Ask) => "key moved (ask)",
            ErrorKind::ServerError(ServerErrorKind::TryAgain) => "try again",
            ErrorKind::ServerError(ServerErrorKind::ClusterDown) => "cluster down",
            ErrorKind::ServerError(ServerErrorKind::CrossSlot) => "cross-slot",
            ErrorKind::ServerError(ServerErrorKind::MasterDown) => "master down",
            ErrorKind::IoError => "I/O error",
            ErrorKind::ExtensionError => "extension error",
            ErrorKind::ClientError => "client error",
            ErrorKind::ServerError(ServerErrorKind::ReadOnly) => "read-only",
            ErrorKind::MasterNameNotFoundBySentinel => "master name not found by sentinel",
            ErrorKind::NoValidReplicasFoundBySentinel => "no valid replicas found by sentinel",
            ErrorKind::EmptySentinelList => "empty sentinel list",
            ErrorKind::ServerError(ServerErrorKind::NotBusy) => "not busy",
            ErrorKind::ClusterConnectionNotFound => "connection to node in cluster not found",
            #[cfg(feature = "json")]
            ErrorKind::Serialize => "serializing",
            ErrorKind::RESP3NotSupported => "resp3 is not supported by server",
            ErrorKind::ParseError => "parse error",
            ErrorKind::ServerError(ServerErrorKind::NoSub) => {
                "Server declined unsubscribe related command in non-subscribed mode"
            }
            ErrorKind::ServerError(ServerErrorKind::NoPerm) => "",
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
            ErrorKind::ServerError(ServerErrorKind::Moved)
                | ErrorKind::ServerError(ServerErrorKind::Ask)
                | ErrorKind::ServerError(ServerErrorKind::TryAgain)
                | ErrorKind::ServerError(ServerErrorKind::ClusterDown)
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
            ErrorKind::ServerError(ServerErrorKind::Ask)
            | ErrorKind::ServerError(ServerErrorKind::Moved) => (),
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
        let repr = match &self.repr {
            ErrorRepr::WithDescription(kind, desc) => ErrorRepr::WithDescription(*kind, desc),
            ErrorRepr::WithDescriptionAndDetail(kind, desc, ref detail) => {
                ErrorRepr::WithDescriptionAndDetail(*kind, desc, detail.clone())
            }
            ErrorRepr::ExtensionError(ref code, ref detail) => {
                ErrorRepr::ExtensionError(code.clone(), detail.clone())
            }
            ErrorRepr::IoError(ref e) => ErrorRepr::IoError(io::Error::new(
                e.kind(),
                format!("{ioerror_description}: {e}"),
            )),
            ErrorRepr::ParsingError(ref err) => ErrorRepr::ParsingError(err.clone()),
            ErrorRepr::ServerError(server_error) => ErrorRepr::ServerError(server_error.clone()),
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
            ErrorKind::ServerError(ServerErrorKind::Moved) => RetryMethod::MovedRedirect,
            ErrorKind::ServerError(ServerErrorKind::Ask) => RetryMethod::AskRedirect,

            ErrorKind::ServerError(ServerErrorKind::TryAgain) => RetryMethod::WaitAndRetry,
            ErrorKind::ServerError(ServerErrorKind::MasterDown) => RetryMethod::WaitAndRetry,
            ErrorKind::ServerError(ServerErrorKind::ClusterDown) => RetryMethod::WaitAndRetry,
            ErrorKind::ServerError(ServerErrorKind::BusyLoadingError) => RetryMethod::WaitAndRetry,
            ErrorKind::MasterNameNotFoundBySentinel => RetryMethod::WaitAndRetry,
            ErrorKind::NoValidReplicasFoundBySentinel => RetryMethod::WaitAndRetry,

            ErrorKind::ServerError(ServerErrorKind::ResponseError) => RetryMethod::NoRetry,
            ErrorKind::ServerError(ServerErrorKind::ReadOnly) => RetryMethod::NoRetry,
            ErrorKind::ExtensionError => RetryMethod::NoRetry,
            ErrorKind::ServerError(ServerErrorKind::ExecAbortError) => RetryMethod::NoRetry,
            ErrorKind::UnexpectedReturnType => RetryMethod::NoRetry,
            ErrorKind::ServerError(ServerErrorKind::NoScriptError) => RetryMethod::NoRetry,
            ErrorKind::InvalidClientConfig => RetryMethod::NoRetry,
            ErrorKind::ServerError(ServerErrorKind::CrossSlot) => RetryMethod::NoRetry,
            ErrorKind::ClientError => RetryMethod::NoRetry,
            ErrorKind::EmptySentinelList => RetryMethod::NoRetry,
            ErrorKind::ServerError(ServerErrorKind::NotBusy) => RetryMethod::NoRetry,
            #[cfg(feature = "json")]
            ErrorKind::Serialize => RetryMethod::NoRetry,
            ErrorKind::RESP3NotSupported => RetryMethod::NoRetry,
            ErrorKind::ServerError(ServerErrorKind::NoSub) => RetryMethod::NoRetry,
            ErrorKind::ServerError(ServerErrorKind::NoPerm) => RetryMethod::NoRetry,

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
    fn from(err: ServerError) -> Self {
        Self {
            repr: ErrorRepr::ServerError(err),
        }
    }
}

impl From<ServerErrorKind> for ErrorKind {
    fn from(kind: ServerErrorKind) -> Self {
        ErrorKind::ServerError(kind)
    }
}

impl From<ParsingError> for RedisError {
    fn from(err: ParsingError) -> Self {
        RedisError {
            repr: ErrorRepr::ParsingError(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parse_redis_value;

    #[test]
    fn test_redirect_node() {
        let err = parse_redis_value(b"-ASK 123 foobar:6380\r\n")
            .unwrap()
            .extract_error()
            .unwrap_err();
        let node = err.redirect_node();

        assert_eq!(node, Some(("foobar:6380", 123)));
    }
}
