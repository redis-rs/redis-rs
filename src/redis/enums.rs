#[deriving(Clone, Eq)]
pub enum Error {
    ResponseError,
    ExecAbortError,
    BusyLoadingError,
    NoScriptError,
    UnknownError,
    ExtensionError(~str),
}

#[deriving(Clone, Eq)]
pub enum Value {
    Invalid,
    Nil,
    Int(i64),
    Data(~[u8]),
    Bulk(~[Value]),
    Error(Error, ~str),
    Success,
    Status(~str),
}

pub enum ConnectFailure {
    InvalidURI,
    HostNotFound,
    ConnectionRefused,
}

#[deriving(Clone, Eq)]
pub enum CmdArg<'a> {
    StrArg(&'a str),
    IntArg(int),
    FloatArg(f32),
    BytesArg(&'a [u8]),
}

pub enum ShutdownMode {
    ShutdownNormal,
    ShutdownSave,
    ShutdownNoSave,
}
