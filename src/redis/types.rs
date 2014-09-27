use std::io::IoError;
use std::from_str::from_str;
use std::str::from_utf8;


#[deriving(PartialEq, Eq, Clone, Show)]
pub enum ErrorKind {
    ResponseError,
    TypeError,
    ExecAbortError,
    BusyLoadingError,
    NoScriptError,
    ExtensionError(String),
    InternalIoError(IoError),
}


#[deriving(PartialEq, Eq, Clone, Show)]
pub enum Value {
    Nil,
    Int(i64),
    Data(Vec<u8>),
    Bulk(Vec<Value>),
    Okay,
    Status(String),
}


#[deriving(PartialEq, Eq, Clone, Show)]
pub struct Error {
    pub kind: ErrorKind,
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

pub type RedisResult<T> = Result<T, Error>;


impl Error {

    pub fn simple(kind: ErrorKind, desc: &'static str) -> Error {
        Error {
            kind: kind,
            desc: desc,
            detail: None,
        }
    }
}


pub trait ToRedisArg {
    fn to_redis_arg(&self) -> Vec<u8>;
}


macro_rules! invalid_type_error(
    ($t:ty, $det:expr) => ({
        return Err(Error {
            kind: TypeError,
            desc: "Response was of incompatible type",
            detail: Some(($det).to_string())
         });
    })
)

macro_rules! format_for_redis(
    ($v:expr) => ({
        let mut rv = vec![];
        let b = $v;
        rv.push_all(format!("${}\r\n", b.len()).as_bytes());
        rv.push_all(b[]);
        rv.push_all(b"\r\n");
        rv
    })
)

macro_rules! string_based_to_redis_impl(
    ($t:ty) => (
        impl ToRedisArg for $t {
            fn to_redis_arg(&self) -> Vec<u8> {
                let s = self.to_string();
                format_for_redis!(s.as_bytes())
            }
        }
    )
)


string_based_to_redis_impl!(bool)
string_based_to_redis_impl!(i32)
string_based_to_redis_impl!(u32)
string_based_to_redis_impl!(i64)
string_based_to_redis_impl!(u64)
string_based_to_redis_impl!(f32)
string_based_to_redis_impl!(f64)
string_based_to_redis_impl!(int)
string_based_to_redis_impl!(uint)


impl ToRedisArg for String {
    fn to_redis_arg(&self) -> Vec<u8> {
        format_for_redis!(self.as_bytes())
    }
}

impl<'a> ToRedisArg for &'a str {
    fn to_redis_arg(&self) -> Vec<u8> {
        format_for_redis!(self.as_bytes())
    }
}

impl ToRedisArg for Vec<u8> {
    fn to_redis_arg(&self) -> Vec<u8> {
        format_for_redis!(self.to_vec())
    }
}

impl<'a> ToRedisArg for &'a [u8] {
    fn to_redis_arg(&self) -> Vec<u8> {
        format_for_redis!(self.to_vec())
    }
}


pub trait FromRedisValue {
    fn from_redis_value(v: &Value) -> RedisResult<Self>;
}

impl FromRedisValue for bool {
    fn from_redis_value(v: &Value) -> RedisResult<bool> {
        Ok(match v {
            &Nil => false,
            &Int(1) => true,
            &Int(_) => false,
            &Okay => true,
            &Data(ref v) => v.len() > 0,
            &Bulk(ref v) => v.len() > 0,
            &Status(_) => true,
        })
    }
}

macro_rules! from_redis_value_for_num(
    ($t:ty) => (
        impl FromRedisValue for $t {
            fn from_redis_value(v: &Value) -> RedisResult<$t> {
                match v {
                    &Int(val) => Ok(val as $t),
                    &Data(ref bytes) => {
                        match from_utf8(bytes[]) {
                            Some(s) => match from_str(s.as_slice()) {
                                Some(rv) => Ok(rv),
                                None => invalid_type_error!($t,
                                    "Could not convert from string.")
                            },
                            None => invalid_type_error!($t,
                                "Invalid UTF-8 string."),
                        }
                    },
                    _ => invalid_type_error!($t,
                        "Response type not convertible to numeric.")
                }
            }
        }
    )
)

from_redis_value_for_num!(i32)
from_redis_value_for_num!(u32)
from_redis_value_for_num!(i64)
from_redis_value_for_num!(u64)
from_redis_value_for_num!(f32)
from_redis_value_for_num!(f64)
from_redis_value_for_num!(int)
from_redis_value_for_num!(uint)

impl FromRedisValue for String {
    fn from_redis_value(v: &Value) -> RedisResult<String> {
        match v {
            &Data(ref bytes) => {
                match from_utf8(bytes[]) {
                    Some(s) => Ok(s.to_string()),
                    None => invalid_type_error!(String,
                        "Invalid UTF-8 string."),
                }
            },
            &Okay => Ok("OK".to_string()),
            &Status(ref val) => Ok(val.to_string()),
            _ => invalid_type_error!(String,
                "Response type not string compatible."),
        }
    }
}

impl<T: FromRedisValue> FromRedisValue for Vec<T> {
    fn from_redis_value(v: &Value) -> RedisResult<Vec<T>> {
        match v {
            &Bulk(ref items) => {
                let mut rv = vec![];
                for item in items.iter() {
                    match FromRedisValue::from_redis_value(item) {
                        Ok(val) => rv.push(val),
                        Err(_) => {},
                    }
                }
                Ok(rv)
            }
            &Nil => {
                Ok(vec![])
            },
            _ => invalid_type_error!(Vec<T>,
                "Response type not vector compatible.")
        }
    }
}

impl FromRedisValue for Value {
    fn from_redis_value(v: &Value) -> RedisResult<Value> {
        Ok(v.clone())
    }
}

impl FromRedisValue for () {
    fn from_redis_value(v: &Value) -> RedisResult<()> {
        Ok(())
    }
}
