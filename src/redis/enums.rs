use std::io::IoError;


#[deriving(PartialEq, Eq, Clone, Show)]
pub enum RedisErrorKind {
    ResponseError,
    ExecAbortError,
    BusyLoadingError,
    NoScriptError,
    ExtensionError(String),
    InternalIoError(IoError),
}

#[deriving(PartialEq, Eq, Clone, Show)]
pub enum RedisValue {
    Nil,
    Int(i64),
    Data(Vec<u8>),
    Bulk(Vec<RedisValue>),
    Okay,
    Status(String),
}

#[deriving(PartialEq, Eq, Clone, Show)]
pub struct RedisError {
    pub kind: RedisErrorKind,
    pub desc: &'static str,
    pub detail: Option<String>,
}

#[deriving(Clone)]
pub enum CmdArg<'a> {
    StrArg(&'a str),
    IntArg(i64),
    FloatArg(f32),
    BytesArg(&'a [u8]),
}

pub type RedisResult = Result<RedisValue, RedisError>;


impl RedisError {

    pub fn simple(kind: RedisErrorKind, desc: &'static str) -> RedisError {
        RedisError {
            kind: kind,
            desc: desc,
            detail: None,
        }
    }
}
