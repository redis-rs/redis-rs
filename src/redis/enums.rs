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

#[deriving(Clone, Eq)]
pub enum ConnectFailure {
    InvalidURI,
    HostNotFound,
    ConnectionRefused,
}

#[deriving(Clone, Eq)]
pub enum CmdArg<'a> {
    StrArg(&'a str),
    IntArg(i64),
    FloatArg(f32),
    BytesArg(&'a [u8]),
}

#[deriving(Clone, Eq)]
pub enum ShutdownMode {
    ShutdownNormal,
    ShutdownSave,
    ShutdownNoSave,
}

#[deriving(Clone, Eq)]
pub enum KeyType {
    StringType,
    ListType,
    SetType,
    ZSetType,
    HashType,
    UnknownType,
    NilType,
}

#[deriving(Clone, Eq)]
pub enum RangeBoundary {
    Open(f32),
    Closed(f32),
    Inf,
    NegInf,
}

impl ToStr for RangeBoundary {
    fn to_str(&self) -> ~str {
        match *self {
            Open(x) => format!("({}", x),
            Closed(x) => x.to_str(),
            Inf => ~"+inf",
            NegInf => ~"-inf",
        }
    }
}
