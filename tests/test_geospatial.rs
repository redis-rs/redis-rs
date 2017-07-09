extern crate net2;
extern crate rand;
extern crate redis;

#[macro_use]
extern crate assert_approx_eq;

use redis::{Commands, RedisResult};
use redis::geo::{Coord, Unit};

mod common;
use common::*;

const PALERMO: (&'static str, &'static str, &'static str) = ("13.361389", "38.115556", "Palermo");
const CATANIA: (&'static str, &'static str, &'static str) = ("15.087269", "37.502669", "Catania");

#[test]
fn test_geoadd_single_tuple() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", PALERMO), Ok(1));
}

#[test]
fn test_geoadd_multiple_tuples() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA]), Ok(2));
}

#[test]
fn test_geodist_existing_members() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA]), Ok(2));

    let dist: f64 = con.geo_dist("my_gis", PALERMO.2, CATANIA.2, Unit::Kilometers).unwrap();
    assert_approx_eq!(dist, 166.2742, 0.001);
}

#[test]
fn test_geodist_support_option() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA]), Ok(2));

    // We should be able to extract the value as an Option<_>, so we can detect
    // if a member is missing

    let result: RedisResult<Option<f64>> = con.geo_dist("my_gis", PALERMO.2, "none", Unit::Meters);
    assert_eq!(result, Ok(None));

    let result: RedisResult<Option<f64>> = con.geo_dist("my_gis", PALERMO.2, CATANIA.2, Unit::Meters);
    assert_ne!(result, Ok(None));

    let dist = result.unwrap().unwrap();
    assert_approx_eq!(dist, 166274.1516, 0.01);
}

#[test]
fn test_geohash() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA]), Ok(2));
    let result: RedisResult<Vec<String>> = con.geo_hash("my_gis", PALERMO.2);
    assert_eq!(result, Ok(vec![String::from("sqc8b49rny0")]));

    let result: RedisResult<Vec<String>> = con.geo_hash("my_gis", &[PALERMO.2, CATANIA.2]);
    assert_eq!(result, Ok(vec![String::from("sqc8b49rny0"), String::from("sqdtr74hyu0")]));

}

#[test]
fn test_geopos() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA]), Ok(2));

    let result: Vec<Vec<f64>> = con.geo_pos("my_gis", &[PALERMO.2]).unwrap();
    assert_eq!(result.len(), 1);

    assert_approx_eq!(result[0][0], 13.36138, 0.0001);
    assert_approx_eq!(result[0][1], 38.11555, 0.0001);

    // Using the Coord struct
    let result: Vec<Coord> = con.geo_pos("my_gis", &[PALERMO.2, CATANIA.2]).unwrap();
    assert_eq!(result.len(), 2);

    assert_approx_eq!(result[0].longitude, 13.36138, 0.0001);
    assert_approx_eq!(result[0].latitude, 38.11555, 0.0001);

    assert_approx_eq!(result[1].longitude, 15.08726, 0.0001);
    assert_approx_eq!(result[1].latitude, 37.50266, 0.0001);
}
