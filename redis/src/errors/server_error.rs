use arcstr::ArcStr;
use std::fmt;

/// Kinds of errors returned from the server
#[derive(PartialEq, Debug, Clone, Copy, Eq)]
pub enum ServerErrorKind {
    /// The server generated an invalid response, or returned a general error.
    ResponseError,
    /// A script execution was aborted.
    ExecAbort,
    /// The server cannot response because it's loading a dump.
    BusyLoading,
    /// A script that was requested does not actually exist.
    NoScript,
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
    /// Attempted to use a command without ACL permission.
    NoPerm,
}

impl ServerErrorKind {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            ServerErrorKind::ResponseError => "ERR",
            ServerErrorKind::ExecAbort => "EXECABORT",
            ServerErrorKind::BusyLoading => "LOADING",
            ServerErrorKind::NoScript => "NOSCRIPT",
            ServerErrorKind::Moved => "MOVED",
            ServerErrorKind::Ask => "ASK",
            ServerErrorKind::TryAgain => "TRYAGAIN",
            ServerErrorKind::ClusterDown => "CLUSTERDOWN",
            ServerErrorKind::CrossSlot => "CROSSSLOT",
            ServerErrorKind::MasterDown => "MASTERDOWN",
            ServerErrorKind::ReadOnly => "READONLY",
            ServerErrorKind::NotBusy => "NOTBUSY",
            ServerErrorKind::NoSub => "NOSUB",
            ServerErrorKind::NoPerm => "NOPERM",
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ServerError {
    Extension {
        code: ArcStr,
        detail: Option<ArcStr>,
    },
    Known {
        kind: ServerErrorKind,
        detail: Option<ArcStr>,
    },
}

impl ServerError {
    pub fn kind(&self) -> Option<ServerErrorKind> {
        match self {
            ServerError::Extension { .. } => None,
            ServerError::Known { kind, .. } => Some(*kind),
        }
    }

    pub fn code(&self) -> &str {
        match self {
            ServerError::Extension { code, .. } => code,
            ServerError::Known { kind, .. } => kind.code(),
        }
    }

    pub fn details(&self) -> Option<&str> {
        match self {
            ServerError::Extension { detail, .. } => detail.as_ref().map(|str| str.as_str()),
            ServerError::Known { detail, .. } => detail.as_ref().map(|str| str.as_str()),
        }
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            ServerError::Extension { code, detail } => {
                fmt::Debug::fmt(&code, f)?;
                if let Some(detail) = detail {
                    f.write_str(": ")?;
                    detail.fmt(f)?;
                }
                Ok(())
            }
            ServerError::Known { kind, detail } => {
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

impl std::error::Error for ServerError {}
