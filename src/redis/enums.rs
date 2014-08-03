use std::fmt;
use std::str::from_utf8_owned;
use std::from_str::FromStr;

#[deriving(Clone, Eq, Show)]
pub enum Error {
    ResponseError,
    ExecAbortError,
    BusyLoadingError,
    NoScriptError,
    UnknownError,
    ExtensionError(String),
}

#[deriving(Clone, Eq, Show)]
pub enum Value {
    Invalid,
    Nil,
    Int(i64),
    Data(Vec<u8>),
    Bulk(Vec<Value>),
    Error(Error, String),
    Success,
    Status(String),
}

#[deriving(Clone, Eq, Show)]
pub enum ConnectFailure {
    InvalidURI,
    HostNotFound,
    ConnectionRefused,
}

#[deriving(Clone, Eq, Show)]
pub enum CmdArg<'a> {
    StrArg(&'a str),
    IntArg(i64),
    FloatArg(f32),
    BytesArg(&'a [u8]),
}

#[deriving(Clone, Eq, Show)]
pub enum ShutdownMode {
    ShutdownNormal,
    ShutdownSave,
    ShutdownNoSave,
}

#[deriving(Clone, Eq, Show)]
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

impl fmt::Show for RangeBoundary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Open(x) => write!(f, "({}", x),
            Closed(x) => write!(f, "{}", x),
            Inf => write!(f, "+inf"),
            NegInf => write!(f, "-inf"),
        }
    }
}

impl Value {

    pub fn get_bytes(self) -> Option<Vec<u8>> {
        match self {
            Data(payload) => Some(payload),
            _ => None,
        }
    }

    pub fn get_string(self) -> Option<String> {
        match self {
            Status(x) => Some(x),
            Data(payload) => from_utf8_owned(payload).ok(),
            _ => None,
        }
    }

    pub fn get_as<T: FromStr>(self) -> Option<T> {
        match self.get_string() {
            Some(x) => from_str(x.as_slice()),
            None => None,
        }
    }
}
