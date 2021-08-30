use {
    crate::{ErrorKind, InfoDict, RedisError, RedisResult, Value},
    std::{
        collections::{BTreeMap, BTreeSet, HashMap, HashSet},
        hash::{BuildHasher, Hash},
        vec::IntoIter,
    },
};

pub struct MapIntoIter(pub(crate) IntoIter<Value>);

impl Iterator for MapIntoIter {
    type Item = (Value, Value);

    fn next(&mut self) -> Option<Self::Item> {
        Some((self.0.next()?, self.0.next()?))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (low, high) = self.0.size_hint();
        (low / 2, high.map(|h| h / 2))
    }
}

macro_rules! invalid_type_error {
    ($det:expr) => {{
        fail!(invalid_type_error_inner!($det))
    }};
}

macro_rules! invalid_type_error_inner {
    ($det:expr) => {
        RedisError::from((
            ErrorKind::TypeError,
            "Response was of incompatible type",
            format!("{:?}", $det),
        ))
    };
}

/// This trait is used to convert a redis value into a more appropriate
/// type like FromRedisValue but move instead of copy.
pub trait RedisValueInto: Sized {
    /// Given a redis `Value` this attempts to convert it into the given
    /// destination type by move. If that fails because it's not compatible an
    /// appropriate error is generated.
    fn redis_value_into(v: Value) -> RedisResult<Self>;

    #[doc(hidden)]
    fn from_byte_vec(_vec: Vec<u8>) -> Option<Vec<Self>> {
        None
    }
}

macro_rules! redis_value_into_for_num_internal {
    ($t:ty, $v:expr) => {{
        let v = $v;
        match v {
            Value::Int(val) => Ok(val as $t),
            Value::Status(s) => match s.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!("Could not convert from string."),
            },
            Value::Data(bytes) => match String::from_utf8(bytes)?.parse::<$t>() {
                Ok(rv) => Ok(rv),
                Err(_) => invalid_type_error!("Could not convert from string."),
            },
            _ => invalid_type_error!("Response type not convertible to numeric."),
        }
    }};
}

macro_rules! redis_value_into_for_num {
    ($t:ty) => {
        impl RedisValueInto for $t {
            fn redis_value_into(v: Value) -> RedisResult<$t> {
                redis_value_into_for_num_internal!($t, v)
            }
        }
    };
}

impl RedisValueInto for u8 {
    fn redis_value_into(v: Value) -> RedisResult<u8> {
        redis_value_into_for_num_internal!(u8, v)
    }

    fn from_byte_vec(vec: Vec<u8>) -> Option<Vec<u8>> {
        Some(vec)
    }
}

redis_value_into_for_num!(i8);
redis_value_into_for_num!(i16);
redis_value_into_for_num!(u16);
redis_value_into_for_num!(i32);
redis_value_into_for_num!(u32);
redis_value_into_for_num!(i64);
redis_value_into_for_num!(u64);
redis_value_into_for_num!(i128);
redis_value_into_for_num!(u128);
redis_value_into_for_num!(f32);
redis_value_into_for_num!(f64);
redis_value_into_for_num!(isize);
redis_value_into_for_num!(usize);

impl RedisValueInto for bool {
    fn redis_value_into(v: Value) -> RedisResult<bool> {
        match v {
            Value::Nil => Ok(false),
            Value::Int(val) => Ok(val != 0),
            Value::Status(s) => {
                if &s[..] == "1" {
                    Ok(true)
                } else if &s[..] == "0" {
                    Ok(false)
                } else {
                    invalid_type_error!("Response status not valid boolean");
                }
            }
            Value::Data(bytes) => {
                if bytes == b"1" {
                    Ok(true)
                } else if bytes == b"0" {
                    Ok(false)
                } else {
                    invalid_type_error!("Response type not bool compatible.");
                }
            }
            Value::Okay => Ok(true),
            _ => invalid_type_error!("Response type not bool compatible."),
        }
    }
}

impl RedisValueInto for String {
    fn redis_value_into(v: Value) -> RedisResult<String> {
        match v {
            Value::Data(bytes) => Ok(String::from_utf8(bytes)?),
            Value::Okay => Ok("OK".to_string()),
            Value::Status(val) => Ok(val),
            _ => invalid_type_error!("Response type not string compatible."),
        }
    }
}

impl<T: RedisValueInto> RedisValueInto for Vec<T> {
    fn redis_value_into(v: Value) -> RedisResult<Vec<T>> {
        match v {
            // this hack allows us to specialize Vec<u8> to work with
            // binary data whereas all others will fail with an error.
            Value::Data(bytes) => match RedisValueInto::from_byte_vec(bytes) {
                Some(x) => Ok(x),
                None => invalid_type_error!("Response type not vector compatible."),
            },
            Value::Bulk(items) => Ok(items
                .into_iter()
                .filter_map(|item| RedisValueInto::redis_value_into(item).ok())
                .collect()),
            Value::Nil => Ok(vec![]),
            _ => invalid_type_error!("Response type not vector compatible."),
        }
    }
}

impl<K: RedisValueInto + Eq + Hash, V: RedisValueInto, S: BuildHasher + Default> RedisValueInto
    for HashMap<K, V, S>
{
    fn redis_value_into(v: Value) -> RedisResult<HashMap<K, V, S>> {
        v.as_map_into_iter()
            .ok_or_else(|| invalid_type_error_inner!("Response type not hashmap compatible"))?
            .map(|(k, v)| Ok((redis_value_into(k)?, redis_value_into(v)?)))
            .collect()
    }
}

impl<K: RedisValueInto + Eq + Hash, V: RedisValueInto> RedisValueInto for BTreeMap<K, V>
where
    K: Ord,
{
    fn redis_value_into(v: Value) -> RedisResult<BTreeMap<K, V>> {
        v.as_map_into_iter()
            .ok_or_else(|| invalid_type_error_inner!("Response type not btreemap compatible"))?
            .map(|(k, v)| Ok((redis_value_into(k)?, redis_value_into(v)?)))
            .collect()
    }
}

impl<T: RedisValueInto + Eq + Hash, S: BuildHasher + Default> RedisValueInto for HashSet<T, S> {
    fn redis_value_into(v: Value) -> RedisResult<HashSet<T, S>> {
        let items = v
            .as_into_sequence()
            .ok_or_else(|| invalid_type_error_inner!("Response type not hashset compatible"))?;
        items
            .into_iter()
            .map(|item| redis_value_into(item))
            .collect()
    }
}

impl<T: RedisValueInto + Eq + Hash> RedisValueInto for BTreeSet<T>
where
    T: Ord,
{
    fn redis_value_into(v: Value) -> RedisResult<BTreeSet<T>> {
        let items = v
            .as_into_sequence()
            .ok_or_else(|| invalid_type_error_inner!("Response type not btreeset compatible"))?;
        items
            .into_iter()
            .map(|item| redis_value_into(item))
            .collect()
    }
}

impl RedisValueInto for Value {
    fn redis_value_into(v: Value) -> RedisResult<Value> {
        Ok(v.clone())
    }
}

impl RedisValueInto for () {
    fn redis_value_into(_v: Value) -> RedisResult<()> {
        Ok(())
    }
}

impl RedisValueInto for InfoDict {
    fn redis_value_into(v: Value) -> RedisResult<InfoDict> {
        let s: String = redis_value_into(v)?;
        Ok(InfoDict::new(&s))
    }
}

impl<T: RedisValueInto> RedisValueInto for Option<T> {
    fn redis_value_into(v: Value) -> RedisResult<Option<T>> {
        if let Value::Nil = v {
            return Ok(None);
        }
        Ok(Some(redis_value_into(v)?))
    }
}

#[cfg(feature = "bytes")]
impl RedisValueInto for bytes::Bytes {
    fn redis_value_into(v: Value) -> RedisResult<Self> {
        match v {
            Value::Data(bytes_vec) => Ok(bytes::Bytes::from(bytes_vec)),
            _ => invalid_type_error!("Not binary data"),
        }
    }
}

/// A shortcut function to invoke `RedisValueInto::redis_value_into`
/// to make the API slightly nicer.
pub fn redis_value_into<T: RedisValueInto>(v: Value) -> RedisResult<T> {
    RedisValueInto::redis_value_into(v)
}
