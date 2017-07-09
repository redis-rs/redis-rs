//! Defines types to use with the geospatial commands.

use super::{ErrorKind, RedisResult};
use types::{FromRedisValue, ToRedisArgs, Value};

/// Units used by [`geo_dist`][1] and [`geo_radius`][2].
///
/// [1]: ../trait.Commands.html#method.geo_dist
/// [2]: ../trait.Commands.html#method.geo_radius
pub enum Unit {
    Meters,
    Kilometers,
    Miles,
    Feet,
}

impl ToRedisArgs for Unit {
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        match *self {
            Unit::Meters => vec![vec![b'm']],
            Unit::Kilometers => vec![vec![b'k', b'm']],
            Unit::Miles => vec![vec![b'm', b'i']],
            Unit::Feet => vec![vec![b'f', b't']],
        }
    }
}

/// A coordinate (longitude, latitude). Can be used with [`geo_pos`][1]
/// to parse response from Redis.
///
/// [1]: ../trait.Commands.html#method.geo_pos
///
/// `T` is the type of the every value.
///
/// * You may want to use either `f64` or `f32` if you want to perform mathematical operations.
/// * To keep the raw value from Redis, use `String`.
#[derive(Debug)]
pub struct Coord<T> {
    pub longitude: T,
    pub latitude: T,
}

impl<T: FromRedisValue> FromRedisValue for Coord<T> {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let values: Vec<T> = FromRedisValue::from_redis_value(v)?;
        if values.len() != 2 {
            fail!((ErrorKind::TypeError,
                   "Response was of incompatible type",
                   format!("Expect a pair of numbers (response was {:?})", v)));
        }
        let mut values = values.into_iter();
        Ok(Coord { longitude: values.next().unwrap(), latitude: values.next().unwrap() })
    }
}
