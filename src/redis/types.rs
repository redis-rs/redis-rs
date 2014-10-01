use std::fmt;
use std::hash::Hash;
use std::io::IoError;
use std::from_str::from_str;
use std::str::from_utf8;
use std::collections::{HashMap, HashSet};
use serialize::json;


/// An enum of all error kinds.
#[deriving(PartialEq, Eq, Clone, Show)]
pub enum ErrorKind {
    /// The server generated an invalid response.
    ResponseError,
    /// Operation failed because of a type mismatch.
    TypeError,
    /// A script execution was aborted.
    ExecAbortError,
    /// The server cannot response because it's loading a dump.
    BusyLoadingError,
    /// A script that was requested does not actually exist.
    NoScriptError,
    /// A error that is unknown to the library.  This might be used
    /// by future redis servers or custom extensions.
    ExtensionError(String),
    /// An IO error was encountered while talking to the server.
    InternalIoError(IoError),
}


/// Internal low-level redis value enum.
#[deriving(PartialEq, Eq, Clone)]
pub enum Value {
    /// A nil response from the server.
    Nil,
    /// An integer response.  Note that there are a few situations
    /// in which redis actually returns a string for an integer which
    /// is why this library generally treats integers and strings
    /// the same for all numeric responses.
    Int(i64),
    /// An arbitary binary data.
    Data(Vec<u8>),
    /// A bulk response of more data.  This is generally used by redis
    /// to express nested structures.
    Bulk(Vec<Value>),
    /// A status response.
    Status(String),
    /// A status response which represents the string "OK".
    Okay,
}

/// Values are generally not used directly unless you are using the
/// more low level functionality in the library.  For the most part
/// this is hidden with the help of the `FromRedisValue` trait.
///
/// While on the redis protocol there is an error type this is already
/// separated at an early point so the value only holds the remaining
/// types.
impl Value {

    /// Checks if the return value looks like it fulfils the cursor
    /// protocol.  That means the result is a bulk item of length
    /// two with the first one being a cursor and the second a
    /// bulk response.
    pub fn looks_like_cursor(&self) -> bool {
        match self {
            &Bulk(ref items) => {
                if items.len() != 2 {
                    return false;
                }
                match items[0] {
                    Data(_) => {},
                    _ => { return false; }
                };
                match items[1] {
                    Bulk(_) => {},
                    _ => { return false; }
                }
                return true;
            }
            _ => { return false; }
        }
    }
}

impl fmt::Show for Value {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Nil => write!(fmt, "nil"),
            &Int(val) => write!(fmt, "int({})", val),
            &Data(ref val) => {
                match from_utf8(val.as_slice()) {
                    Some(x) => write!(fmt, "string-data('{}')", x.escape_default()),
                    None => write!(fmt, "binary-data({})", val),
                }
            },
            &Bulk(ref values) => {
                try!(write!(fmt, "bulk("));
                let mut is_first = true;
                for val in values.iter() {
                    if !is_first {
                        try!(write!(fmt, ", "));
                    }
                    try!(write!(fmt, "{}", val));
                    is_first = false;
                }
                write!(fmt, ")")
            },
            &Okay => write!(fmt, "ok"),
            &Status(ref s) => write!(fmt, "status({})", s),
        }
    }
}


/// Represents a redis error.
#[deriving(PartialEq, Eq, Clone, Show)]
pub struct Error {
    pub kind: ErrorKind,
    pub desc: &'static str,
    pub detail: Option<String>,
}


/// Library generic result type.
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


/// An info dictionary type.
pub struct InfoDict {
    map: HashMap<String, Value>,
}

/// This type provides convenient access to key/value data returned by
/// the "INFO" command.  It acts like a regular mapping but also has
/// a convenience method `get` which can return data in the appropriate
/// type.
///
/// For instance this can be used to query the server for the role it's
/// in (master, slave) etc:
///
/// ```rust,no_run
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let con = client.get_connection().unwrap();
/// let info : redis::InfoDict = redis::cmd("INFO").query(&con).unwrap();
/// let role : String = info.get("role").unwrap();
/// ```
impl InfoDict {
    /// Creates a new info dictionary from a string in the response of
    /// the INFO command.  Each line is a key, value pair with the
    /// key and value separated by a colon (`:`).  Lines starting with a
    /// hash (`#`) are ignored.
    pub fn new(kvpairs: &str) -> InfoDict {
        let mut map = HashMap::new();
        for line in kvpairs.lines_any() {
            if line.len() == 0 || line.starts_with("#") {
                continue;
            }
            let mut p = line.splitn(1, ':');
            let k = unwrap_or!(p.next(), continue).to_string();
            let v = unwrap_or!(p.next(), continue).to_string();
            map.insert(k, Status(v));
        }
        InfoDict { map: map }
    }

    /// Fetches a value by key and converts it into the given type.
    /// Typical types are `String`, `bool` and integer types.
    pub fn get<T: FromRedisValue>(&self, key: &str) -> Option<T> {
        match self.find(&key) {
            Some(ref x) => from_redis_value(*x).ok(),
            None => None,
        }
    }
}

impl<'a> Map<&'a str, Value> for InfoDict {

    fn find<'x>(&'x self, key: &&str) -> Option<&'x Value> {
        self.map.find_equiv(key)
    }

    fn contains_key<'x>(&'x self, key: &&str) -> bool {
        self.find(key).is_some()
    }
}

impl Collection for InfoDict {
    fn len(&self) -> uint {
        self.map.len()
    }
}


/// Used to convert a value into one or multiple redis argument
/// strings.  Most values will produce exactly one item but in
/// some cases it might make sense to produce more than one.
pub trait ToRedisArgs {
    /// This converts the value into a vector of bytes.  Each item
    /// is a single argument.  Most items generate a vector of a
    /// single item.
    ///
    /// The exception to this rule currently are vectors of items.
    fn to_redis_args(&self) -> Vec<Vec<u8>>;

    /// This only exists internally as a workaround for the lack of
    /// specialization.
    #[doc(hidden)]
    fn make_arg_vec(items: &[Self]) -> Vec<Vec<u8>> {
        let mut rv = vec![];
        for item in items.iter() {
            rv.push_all(item.to_redis_args().as_slice());
        }
        rv
    }
}


macro_rules! invalid_type_error(
    ($v:expr, $det:expr) => ({
        return Err(Error {
            kind: TypeError,
            desc: "Response was of incompatible type",
            detail: Some(format!("{} (response was {})", $det, $v)),
         });
    })
)

macro_rules! string_based_to_redis_impl(
    ($t:ty) => (
        impl ToRedisArgs for $t {
            fn to_redis_args(&self) -> Vec<Vec<u8>> {
                let s = self.to_string();
                vec![s.as_bytes().to_vec()]
            }
        }
    )
)


impl ToRedisArgs for u8 {
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        let s = self.to_string();
        vec![s.as_bytes().to_vec()]
    }

    fn make_arg_vec(items: &[u8]) -> Vec<Vec<u8>> {
        vec![items.to_vec()]
    }
}

string_based_to_redis_impl!(i8)
string_based_to_redis_impl!(i16)
string_based_to_redis_impl!(u16)
string_based_to_redis_impl!(i32)
string_based_to_redis_impl!(u32)
string_based_to_redis_impl!(i64)
string_based_to_redis_impl!(u64)
string_based_to_redis_impl!(f32)
string_based_to_redis_impl!(f64)
string_based_to_redis_impl!(int)
string_based_to_redis_impl!(uint)
string_based_to_redis_impl!(bool)


impl ToRedisArgs for String {
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        vec![self.as_bytes().to_vec()]
    }
}

impl<'a> ToRedisArgs for &'a str {
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        vec![self.as_bytes().to_vec()]
    }
}

impl<T: ToRedisArgs> ToRedisArgs for Vec<T> {
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        ToRedisArgs::make_arg_vec(self.as_slice())
    }
}

impl<'a, T: ToRedisArgs> ToRedisArgs for &'a [T] {
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        ToRedisArgs::make_arg_vec(*self)
    }
}

impl ToRedisArgs for json::Json {
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        vec![json::encode(self).into_bytes()]
    }
}


/// This trait is used to convert a redis value into a more appropriate
/// type.  While a redis `Value` can represent any response that comes
/// back from the redis server, usually you want to map this into something
/// that works better in rust.  For instance you might want to convert the
/// return value into a `String` or an integer.
///
/// This trait is well supported throughout the library and you can
/// implement it for your own types if you want.
///
/// In addition to what you can see from the docs, this is also implemented
/// for tuples up to size 12 and for Vec<u8>.
pub trait FromRedisValue {
    /// Given a redis `Value` this attempts to convert it into the given
    /// destination type.  If that fails because it's not compatible an
    /// appropriate error is generated.
    fn from_redis_value(v: &Value) -> RedisResult<Self>;

    /// This only exists internally as a workaround for the lack of
    /// specialization.
    #[doc(hidden)]
    fn from_byte_vec(_vec: &[u8]) -> Option<Vec<Self>> {
        None
    }
}

macro_rules! from_redis_value_for_num_internal(
    ($t:ty, $v:expr) => (
        {
            let v = $v;
            match v {
                &Int(val) => Ok(val as $t),
                &Data(ref bytes) => {
                    match from_utf8(bytes.as_slice()) {
                        Some(s) => match from_str(s.as_slice()) {
                            Some(rv) => Ok(rv),
                            None => invalid_type_error!(v,
                                "Could not convert from string.")
                        },
                        None => invalid_type_error!(v,
                            "Invalid UTF-8 string."),
                    }
                },
                _ => invalid_type_error!(v,
                    "Response type not convertible to numeric.")
            }
        }
    )
)

macro_rules! from_redis_value_for_num(
    ($t:ty) => (
        impl FromRedisValue for $t {
            fn from_redis_value(v: &Value) -> RedisResult<$t> {
                from_redis_value_for_num_internal!($t, v)
            }
        }
    )
)

impl FromRedisValue for u8 {
    fn from_redis_value(v: &Value) -> RedisResult<u8> {
        from_redis_value_for_num_internal!(u8, v)
    }

    fn from_byte_vec(vec: &[u8]) -> Option<Vec<u8>> {
        Some(vec.to_vec())
    }
}

from_redis_value_for_num!(i8)
from_redis_value_for_num!(i16)
from_redis_value_for_num!(u16)
from_redis_value_for_num!(i32)
from_redis_value_for_num!(u32)
from_redis_value_for_num!(i64)
from_redis_value_for_num!(u64)
from_redis_value_for_num!(f32)
from_redis_value_for_num!(f64)
from_redis_value_for_num!(int)
from_redis_value_for_num!(uint)

impl FromRedisValue for bool {
    fn from_redis_value(v: &Value) -> RedisResult<bool> {
        match v {
            &Nil => Ok(false),
            &Int(val) => Ok(val != 0),
            &Status(ref s) => {
                if s.as_slice() == "1" { Ok(true) }
                else if s.as_slice() == "0" { Ok(false) }
                else {
                    invalid_type_error!(v,
                        "Response status not valid boolean");
                }
            }
            &Okay => Ok(true),
            _ => invalid_type_error!(v,
                "Response type not bool compatible."),
        }
    }
}

impl FromRedisValue for String {
    fn from_redis_value(v: &Value) -> RedisResult<String> {
        match v {
            &Data(ref bytes) => {
                match from_utf8(bytes.as_slice()) {
                    Some(s) => Ok(s.to_string()),
                    None => invalid_type_error!(v,
                        "Invalid UTF-8 string."),
                }
            },
            &Okay => Ok("OK".to_string()),
            &Status(ref val) => Ok(val.to_string()),
            _ => invalid_type_error!(v,
                "Response type not string compatible."),
        }
    }
}

impl<T: FromRedisValue> FromRedisValue for Vec<T> {
    fn from_redis_value(v: &Value) -> RedisResult<Vec<T>> {
        match v {
            // this hack allows us to specialize Vec<u8> to work with
            // binary data whereas all others will fail with an error.
            &Data(ref bytes) => {
                match FromRedisValue::from_byte_vec(bytes.as_slice()) {
                    Some(x) => Ok(x),
                    None => invalid_type_error!(v,
                        "Response type not vector compatible.")
                }
            },
            &Bulk(ref items) => {
                let mut rv = vec![];
                for item in items.iter() {
                    match from_redis_value(item) {
                        Ok(val) => rv.push(val),
                        Err(_) => {},
                    }
                }
                Ok(rv)
            }
            &Nil => {
                Ok(vec![])
            },
            _ => invalid_type_error!(v,
                "Response type not vector compatible.")
        }
    }
}

impl<K: FromRedisValue + Eq + Hash, V: FromRedisValue> FromRedisValue for HashMap<K, V> {
    fn from_redis_value(v: &Value) -> RedisResult<HashMap<K, V>> {
        match v {
            &Bulk(ref items) => {
                let mut rv = HashMap::new();
                let mut iter = items.iter();
                loop {
                    let k = unwrap_or!(iter.next(), break);
                    let v = unwrap_or!(iter.next(), break);
                    rv.insert(try!(from_redis_value(k)),
                              try!(from_redis_value(v)));
                }
                Ok(rv)
            },
            _ => invalid_type_error!(v,
                "Response type not hashmap compatible")
        }
    }
}

impl<T: FromRedisValue + Eq + Hash> FromRedisValue for HashSet<T> {
    fn from_redis_value(v: &Value) -> RedisResult<HashSet<T>> {
        match v {
            &Bulk(ref items) => {
                let mut rv = HashSet::new();
                for item in items.iter() {
                    rv.insert(try!(from_redis_value(item)));
                }
                Ok(rv)
            },
            _ => invalid_type_error!(v,
                "Response type not hashmap compatible")
        }
    }
}

impl FromRedisValue for Value {
    fn from_redis_value(v: &Value) -> RedisResult<Value> {
        Ok(v.clone())
    }
}

impl FromRedisValue for () {
    fn from_redis_value(_v: &Value) -> RedisResult<()> {
        Ok(())
    }
}


macro_rules! from_redis_value_for_tuple(
    () => ();
    ($($name:ident,)+) => (
        #[doc(hidden)]
        impl<$($name: FromRedisValue),*> FromRedisValue for ($($name,)*) {
            // we have local variables named T1 as dummies and those
            // variables are unused.
            #[allow(non_snake_case, unused_variable)]
            fn from_redis_value(v: &Value) -> RedisResult<($($name,)*)> {
                match v {
                    &Bulk(ref items) => {
                        // hacky way to count the tuple size
                        let mut n = 0;
                        $(let $name = (); n += 1;)*
                        if items.len() != n {
                            invalid_type_error!(v, "Bulk response of wrong dimension")
                        }

                        // this is pretty ugly too.  The { i += 1; i - 1} is rust's
                        // postfix increment :)
                        let mut i = 0;
                        Ok(($({let $name = (); try!(from_redis_value(
                             &items[{ i += 1; i - 1 }]))},)*))
                    }
                    _ => invalid_type_error!(v, "Not a bulk response")
                }
            }
        }
        from_redis_value_for_tuple_peel!($($name,)*)
    )
)

/// This chips of the leading one and recurses for the rest.  So if the first
/// iteration was T1, T2, T3 it will recurse to T2, T3.  It stops for tuples
/// of size 1 (does not implement down to unit).
macro_rules! from_redis_value_for_tuple_peel(
    ($name:ident, $($other:ident,)*) => (from_redis_value_for_tuple!($($other,)*))
)

from_redis_value_for_tuple! { T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }


impl FromRedisValue for InfoDict {
    fn from_redis_value(v: &Value) -> RedisResult<InfoDict> {
        let s : String = try!(from_redis_value(v));
        Ok(InfoDict::new(s.as_slice()))
    }
}

impl FromRedisValue for json::Json {
    fn from_redis_value(v: &Value) -> RedisResult<json::Json> {
        let rv = match v {
            &Data(ref b) => json::from_str(unwrap_or!(
                from_utf8(b.as_slice()), invalid_type_error!(v, "Invalid UTF-8"))),
            &Status(ref s) => json::from_str(s.as_slice()),
            _ => invalid_type_error!(v, "Not JSON compatible"),
        };
        match rv {
            Ok(value) => Ok(value),
            Err(_) => invalid_type_error!(v, "Not valid JSON"),
        }
    }
}

impl<T: FromRedisValue> FromRedisValue for Option<T> {
    fn from_redis_value(v: &Value) -> RedisResult<Option<T>> {
        match v {
            &Nil => { return Ok(None); }
            _ => {}
        }
        Ok(Some(try!(from_redis_value(v))))
    }
}

/// A shortcut function to invoke `FromRedisValue::from_redis_value`
/// to make the API slightly nicer.
pub fn from_redis_value<T: FromRedisValue>(v: &Value) -> RedisResult<T> {
    FromRedisValue::from_redis_value(v)
}
