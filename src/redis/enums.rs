use std::str::from_utf8;

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

impl ToPrimitive for Value {
    fn to_i64(&self) -> Option<i64> {
        match *self {
            Int(x) => Some(x),
            _ => None,
        }
    }

    fn to_u64(&self) -> Option<u64> {
        match *self {
            Int(x) => Some(x as u64),
            _ => None,
        }
    }
}

impl ToStr for Value {

    fn to_str(&self) -> ~str {
        match *self {
            Nil | Invalid => ~"",
            Int(x) => x.to_str(),
            Data(ref x) => from_utf8(*x).to_owned(),
            Bulk(ref items) => {
                items.iter().map(|x| x.to_str()).to_owned_vec().concat()
            },
            Error(ref ty, ref msg) => {
                format!("Error {:?}: {}", ty, *msg)
            },
            Success => { ~"OK" },
            Status(ref msg) => msg.to_owned(),
        }
    }
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
