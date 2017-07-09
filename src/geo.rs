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
        let args = match *self {
            Unit::Meters => "m",
            Unit::Kilometers => "km",
            Unit::Miles => "mi",
            Unit::Feet => "ft",
        };
        vec![args.as_bytes().to_vec()]
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

impl<T> Coord<T> {
    /// Create a new Coord with the (longitude, latitude)
    pub fn lon_lat(longitude: T, latitude: T) -> Coord<T> {
        Coord { longitude, latitude }
    }
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

impl<T: ToRedisArgs> ToRedisArgs for Coord<T> {
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        let mut args = Vec::new();
        for field in &[&self.longitude, &self.latitude] {
            args.extend(ToRedisArgs::to_redis_args(*field));
        }
        args
    }
}

/// Options to sort results from GEORADIUS and GEORADIUSBYMEMBER commands
pub enum RadiusOrder {
    /// Don't sort the results
    Unsorted,

    /// Sort returned items from the nearest to the farthest, relative to the center.
    Asc,

    /// Sort returned items from the farthest to the nearest, relative to the center.
    Desc,
}

impl Default for RadiusOrder {
    fn default() -> RadiusOrder {
        RadiusOrder::Unsorted
    }
}

/// Options for the GEORADIUS and GEORADIUSBYMEMBER commands
#[derive(Default)]
pub struct RadiusOptions {
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    order: RadiusOrder,
    store: Option<Vec<Vec<u8>>>,
    store_dist: Option<Vec<Vec<u8>>>,
}

impl RadiusOptions {

    /// Limit the results to the first N matching items.
    pub fn limit(&mut self, n: usize) -> &mut Self {
        self.count = Some(n);
        self
    }

    /// Return the distance of the returned items from the specified center.
    /// The distance is returned in the same unit as the unit specified as the
    /// radius argument of the command.
    pub fn with_dist(&mut self) -> &mut Self {
        self.with_dist = true;
        self
    }

    /// Return the longitude,latitude coordinates of the matching items.
    pub fn with_coord(&mut self) -> &mut Self {
        self.with_coord = true;
        self
    }

    /// Return the raw geohash-encoded sorted set score of the item, in the
    /// form of a 52 bit unsigned integer. This is only useful for low level
    /// hacks or debugging and is otherwise of little interest for the general
    /// user.
    pub fn with_hash(&mut self) -> &mut Self {
        self.with_hash = true;
        self
    }

    /// Sort the returned items
    pub fn order(&mut self, o: RadiusOrder) -> &mut Self {
        self.order = o;
        self
    }

    /// Store the results in a sorted set at key, instead of returning them.
    ///
    /// This feature can't be used with any `with_*` method.
    pub fn store<K: ToRedisArgs>(&mut self, key: K) -> &mut Self {
        self.store = Some(ToRedisArgs::to_redis_args(&key));
        self
    }

    /// Similar to `store`, but includes the distance of every member from the
    /// center. This feature can't be used with any `with_*` method.
    pub fn store_dist<K: ToRedisArgs>(&mut self, key: K) -> &mut Self {
        self.store_dist = Some(ToRedisArgs::to_redis_args(&key));
        self
    }

}

impl ToRedisArgs for RadiusOptions {
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        let mut args = Vec::new();

        if self.with_coord {
            args.push("WITHCOORD".as_bytes().to_vec());
        }

        if self.with_dist {
            args.push("WITHDIST".as_bytes().to_vec());
        }

        if self.with_hash {
            args.push("WITHHASH".as_bytes().to_vec());
        }

        if let Some(n) = self.count {
            args.push("COUNT".as_bytes().to_vec());
            args.push(format!("{}", n).into_bytes());
        }

        match self.order {
            RadiusOrder::Asc => args.push("ASC".as_bytes().to_vec()),
            RadiusOrder::Desc => args.push("DESC".as_bytes().to_vec()),
            _ => (),
        };

        if let Some(ref store) = self.store {
            args.push("STORE".as_bytes().to_vec());
            args.extend_from_slice(&store[..]);
        }

        if let Some(ref store_dist) = self.store_dist {
            args.push("STOREDIST".as_bytes().to_vec());
            args.extend_from_slice(&store_dist[..]);
        }

        args
    }
}

#[cfg(test)]
mod tests {
    use types::ToRedisArgs;
    use super::{Coord, RadiusOptions, RadiusOrder};
    use std::str;

    macro_rules! assert_args {
        ($value:expr, $($args:expr),+) => {
            let args = ToRedisArgs::to_redis_args($value);
            let strings: Vec<_> = args.iter()
                                      .map(|a| str::from_utf8(a.as_ref()).unwrap())
                                      .collect();
            assert_eq!(strings, vec![$($args),+]);
        }
    }

    #[test]
    fn test_coord_to_args() {
        let member = ("Palermo", Coord::lon_lat("13.361389", "38.115556"));
        assert_args!(&member, "Palermo", "13.361389", "38.115556");
    }

    #[test]
    fn test_radius_options() {
        // Without options, should not generate any argument
        let empty = RadiusOptions::default();
        assert_eq!(ToRedisArgs::to_redis_args(&empty).len(), 0);

        // Some combinations with WITH* options
        let opts = || RadiusOptions::default();

        assert_args!(
            opts().with_coord().with_dist(),
            "WITHCOORD", "WITHDIST");

        assert_args!(opts().limit(50), "COUNT", "50");

        assert_args!(
            opts().limit(50).store("x"),
            "COUNT", "50", "STORE", "x");

        assert_args!(
            opts().limit(100).store_dist("y"),
            "COUNT", "100", "STOREDIST", "y");

        assert_args!(
            opts().order(RadiusOrder::Asc).limit(10).with_dist(),
            "WITHDIST", "COUNT", "10", "ASC");

    }
}
