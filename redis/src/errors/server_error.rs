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
