
use types::ToRedisArgs;

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
