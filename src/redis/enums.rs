pub enum Error {
    ResponseError,
    ExecAbortError,
    BusyLoadingError,
    NoScriptError,
    UnknownError,
    ExtensionError(~str),
}

pub enum Value {
    Invalid,
    Nil,
    Int(int),
    Data(~[u8]),
    Bulk(~[Value]),
    Error(Error, ~str),
    Status(~str),
}
