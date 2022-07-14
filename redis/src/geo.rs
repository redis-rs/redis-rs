//! Defines types to use with the geospatial commands.

use super::{ErrorKind, RedisResult};
use crate::types::{FromRedisValue, RedisWrite, ToRedisArgs, Value};

macro_rules! invalid_type_error {
    ($v:expr, $det:expr) => {{
        fail!((
            ErrorKind::TypeError,
            "Response was of incompatible type",
            format!("{:?} (response was {:?})", $det, $v)
        ));
    }};
}

/// Units used by [`geo_dist`][1] and [`geo_radius`][2].
///
/// [1]: ../trait.Commands.html#method.geo_dist
/// [2]: ../trait.Commands.html#method.geo_radius
pub enum Unit {
    /// Represents meters.
    Meters,
    /// Represents kilometers.
    Kilometers,
    /// Represents miles.
    Miles,
    /// Represents feed.
    Feet,
}

impl ToRedisArgs for Unit {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let unit = match *self {
            Unit::Meters => "m",
            Unit::Kilometers => "km",
            Unit::Miles => "mi",
            Unit::Feet => "ft",
        };
        out.write_arg(unit.as_bytes());
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
#[derive(Debug, PartialEq)]
pub struct Coord<T> {
    /// Longitude
    pub longitude: T,
    /// Latitude
    pub latitude: T,
}

impl<T> Coord<T> {
    /// Create a new Coord with the (longitude, latitude)
    pub fn lon_lat(longitude: T, latitude: T) -> Coord<T> {
        Coord {
            longitude,
            latitude,
        }
    }
}

impl<T: FromRedisValue> FromRedisValue for Coord<T> {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let values: Vec<T> = FromRedisValue::from_redis_value(v)?;
        let mut values = values.into_iter();
        let (longitude, latitude) = match (values.next(), values.next(), values.next()) {
            (Some(longitude), Some(latitude), None) => (longitude, latitude),
            _ => invalid_type_error!(v, "Expect a pair of numbers"),
        };
        Ok(Coord {
            longitude,
            latitude,
        })
    }
}

impl<T: ToRedisArgs> ToRedisArgs for Coord<T> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        ToRedisArgs::write_redis_args(&self.longitude, out);
        ToRedisArgs::write_redis_args(&self.latitude, out);
    }

    fn is_single_arg(&self) -> bool {
        false
    }
}

/// Options to sort results from [GEORADIUS][1] and [GEORADIUSBYMEMBER][2] commands
///
/// [1]: https://redis.io/commands/georadius
/// [2]: https://redis.io/commands/georadiusbymember
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

/// Options for the [GEORADIUS][1] and [GEORADIUSBYMEMBER][2] commands
///
/// [1]: https://redis.io/commands/georadius
/// [2]: https://redis.io/commands/georadiusbymember
///
/// # Example
///
/// ```rust,no_run
/// use redis::{Commands, RedisResult};
/// use redis::geo::{RadiusSearchResult, RadiusOptions, RadiusOrder, Unit};
/// fn nearest_in_radius(
///     con: &mut redis::Connection,
///     key: &str,
///     longitude: f64,
///     latitude: f64,
///     meters: f64,
///     limit: usize,
/// ) -> RedisResult<Vec<RadiusSearchResult>> {
///     let opts = RadiusOptions::default()
///         .order(RadiusOrder::Asc)
///         .limit(limit);
///     con.geo_radius(key, longitude, latitude, meters, Unit::Meters, opts)
/// }
/// ```
#[derive(Default)]
pub struct RadiusOptions {
    with_coord: bool,
    with_dist: bool,
    count: Option<usize>,
    order: RadiusOrder,
    store: Option<Vec<Vec<u8>>>,
    store_dist: Option<Vec<Vec<u8>>>,
}

impl RadiusOptions {
    /// Limit the results to the first N matching items.
    pub fn limit(mut self, n: usize) -> Self {
        self.count = Some(n);
        self
    }

    /// Return the distance of the returned items from the specified center.
    /// The distance is returned in the same unit as the unit specified as the
    /// radius argument of the command.
    pub fn with_dist(mut self) -> Self {
        self.with_dist = true;
        self
    }

    /// Return the `longitude, latitude` coordinates of the matching items.
    pub fn with_coord(mut self) -> Self {
        self.with_coord = true;
        self
    }

    /// Sort the returned items
    pub fn order(mut self, o: RadiusOrder) -> Self {
        self.order = o;
        self
    }

    /// Store the results in a sorted set at `key`, instead of returning them.
    ///
    /// This feature can't be used with any `with_*` method.
    pub fn store<K: ToRedisArgs>(mut self, key: K) -> Self {
        self.store = Some(ToRedisArgs::to_redis_args(&key));
        self
    }

    /// Store the results in a sorted set at `key`, with the distance from the
    /// center as its score. This feature can't be used with any `with_*` method.
    pub fn store_dist<K: ToRedisArgs>(mut self, key: K) -> Self {
        self.store_dist = Some(ToRedisArgs::to_redis_args(&key));
        self
    }
}

impl ToRedisArgs for RadiusOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if self.with_coord {
            out.write_arg(b"WITHCOORD");
        }

        if self.with_dist {
            out.write_arg(b"WITHDIST");
        }

        if let Some(n) = self.count {
            out.write_arg(b"COUNT");
            out.write_arg_fmt(n);
        }

        match self.order {
            RadiusOrder::Asc => out.write_arg(b"ASC"),
            RadiusOrder::Desc => out.write_arg(b"DESC"),
            _ => (),
        };

        if let Some(ref store) = self.store {
            out.write_arg(b"STORE");
            for i in store {
                out.write_arg(i);
            }
        }

        if let Some(ref store_dist) = self.store_dist {
            out.write_arg(b"STOREDIST");
            for i in store_dist {
                out.write_arg(i);
            }
        }
    }

    fn is_single_arg(&self) -> bool {
        false
    }
}

/// Contain an item returned by [`geo_radius`][1] and [`geo_radius_by_member`][2].
///
/// [1]: ../trait.Commands.html#method.geo_radius
/// [2]: ../trait.Commands.html#method.geo_radius_by_member
pub struct RadiusSearchResult {
    /// The name that was found.
    pub name: String,
    /// The coordinate if available.
    pub coord: Option<Coord<f64>>,
    /// The distance if available.
    pub dist: Option<f64>,
}

impl FromRedisValue for RadiusSearchResult {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        // If we receive only the member name, it will be a plain string
        if let Ok(name) = FromRedisValue::from_redis_value(v) {
            return Ok(RadiusSearchResult {
                name,
                coord: None,
                dist: None,
            });
        }

        // Try to parse the result from multitple values
        if let Value::Bulk(ref items) = *v {
            if let Some(result) = RadiusSearchResult::parse_multi_values(items) {
                return Ok(result);
            }
        }

        invalid_type_error!(v, "Response type not RadiusSearchResult compatible.");
    }
}

impl RadiusSearchResult {
    fn parse_multi_values(items: &[Value]) -> Option<Self> {
        let mut iter = items.iter();

        // First item is always the member name
        let name: String = match iter.next().map(FromRedisValue::from_redis_value) {
            Some(Ok(n)) => n,
            _ => return None,
        };

        let mut next = iter.next();

        // Next element, if present, will be the distance.
        let dist = match next.map(FromRedisValue::from_redis_value) {
            Some(Ok(c)) => {
                next = iter.next();
                Some(c)
            }
            _ => None,
        };

        // Finally, if present, the last item will be the coordinates

        let coord = match next.map(FromRedisValue::from_redis_value) {
            Some(Ok(c)) => Some(c),
            _ => None,
        };

        Some(RadiusSearchResult { name, coord, dist })
    }
}

#[cfg(test)]
mod tests {
    use super::{Coord, RadiusOptions, RadiusOrder};
    use crate::types::ToRedisArgs;
    use std::str;

    macro_rules! assert_args {
        ($value:expr, $($args:expr),+) => {
            let args = $value.to_redis_args();
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
        let opts = RadiusOptions::default;

        assert_args!(opts().with_coord().with_dist(), "WITHCOORD", "WITHDIST");

        assert_args!(opts().limit(50), "COUNT", "50");

        assert_args!(opts().limit(50).store("x"), "COUNT", "50", "STORE", "x");

        assert_args!(
            opts().limit(100).store_dist("y"),
            "COUNT",
            "100",
            "STOREDIST",
            "y"
        );

        assert_args!(
            opts().order(RadiusOrder::Asc).limit(10).with_dist(),
            "WITHDIST",
            "COUNT",
            "10",
            "ASC"
        );
    }
}
