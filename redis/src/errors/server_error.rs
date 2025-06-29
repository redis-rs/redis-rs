use std::fmt;

#[derive(PartialEq, Debug, Clone, Copy, Eq)]
pub enum ServerErrorKind {
    /// The server generated an invalid response.
    ResponseError,
    /// A script execution was aborted.
    ExecAbortError,
    /// The server cannot response because it's loading a dump.
    BusyLoadingError,
    /// A script that was requested does not actually exist.
    NoScriptError,
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
    /// Attempt to write to a read-only server
    ReadOnly,
    /// Attempted to kill a script/function while they werent' executing
    NotBusy,
    /// Attempted to unsubscribe on a connection that is not in subscribed mode.
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

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ServerError::ExtensionError { code, detail } => {
                fmt::Debug::fmt(&code, f)?;
                if let Some(detail) = detail {
                    f.write_str(": ")?;
                    detail.fmt(f)?;
                }
                Ok(())
            }
            ServerError::KnownError { kind, detail } => {
                fmt::Debug::fmt(&kind, f)?;
                if let Some(detail) = detail {
                    f.write_str(": ")?;
                    detail.fmt(f)?;
                }
                Ok(())
            }
        }
    }
}
