#![cfg(feature = "geospatial")]

use assert_approx_eq::assert_approx_eq;

use redis::geo::{Coord, RadiusOptions, RadiusOrder, RadiusSearchResult, Unit};
use redis::{Commands, RedisResult};

mod support;
use crate::support::*;

const PALERMO: (&str, &str, &str) = ("13.361389", "38.115556", "Palermo");
const CATANIA: (&str, &str, &str) = ("15.087269", "37.502669", "Catania");
const AGRIGENTO: (&str, &str, &str) = ("13.5833332", "37.316667", "Agrigento");

#[test]
fn test_geoadd_single_tuple() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", PALERMO), Ok(1));
}

#[test]
fn test_geoadd_multiple_tuples() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA]), Ok(2));
}

#[test]
fn test_geodist_existing_members() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA]), Ok(2));

    let dist: f64 = con
        .geo_dist("my_gis", PALERMO.2, CATANIA.2, Unit::Kilometers)
        .unwrap();
    assert_approx_eq!(dist, 166.2742, 0.001);
}

#[test]
fn test_geodist_support_option() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA]), Ok(2));

    // We should be able to extract the value as an Option<_>, so we can detect
    // if a member is missing

    let result: RedisResult<Option<f64>> = con.geo_dist("my_gis", PALERMO.2, "none", Unit::Meters);
    assert_eq!(result, Ok(None));

    let result: RedisResult<Option<f64>> =
        con.geo_dist("my_gis", PALERMO.2, CATANIA.2, Unit::Meters);
    assert_ne!(result, Ok(None));

    let dist = result.unwrap().unwrap();
    assert_approx_eq!(dist, 166_274.151_6, 0.01);
}

#[test]
fn test_geohash() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA]), Ok(2));
    let result: RedisResult<Vec<String>> = con.geo_hash("my_gis", PALERMO.2);
    assert_eq!(result, Ok(vec![String::from("sqc8b49rny0")]));

    let result: RedisResult<Vec<String>> = con.geo_hash("my_gis", &[PALERMO.2, CATANIA.2]);
    assert_eq!(
        result,
        Ok(vec![
            String::from("sqc8b49rny0"),
            String::from("sqdtr74hyu0"),
        ])
    );
}

#[test]
fn test_geopos() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA]), Ok(2));

    let result: Vec<Vec<f64>> = con.geo_pos("my_gis", &[PALERMO.2]).unwrap();
    assert_eq!(result.len(), 1);

    assert_approx_eq!(result[0][0], 13.36138, 0.0001);
    assert_approx_eq!(result[0][1], 38.11555, 0.0001);

    // Using the Coord struct
    let result: Vec<Coord<f64>> = con.geo_pos("my_gis", &[PALERMO.2, CATANIA.2]).unwrap();
    assert_eq!(result.len(), 2);

    assert_approx_eq!(result[0].longitude, 13.36138, 0.0001);
    assert_approx_eq!(result[0].latitude, 38.11555, 0.0001);

    assert_approx_eq!(result[1].longitude, 15.08726, 0.0001);
    assert_approx_eq!(result[1].latitude, 37.50266, 0.0001);
}

#[test]
fn test_use_coord_struct() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(
        con.geo_add(
            "my_gis",
            (Coord::lon_lat(13.361_389, 38.115_556), "Palermo")
        ),
        Ok(1)
    );

    let result: Vec<Coord<f64>> = con.geo_pos("my_gis", "Palermo").unwrap();
    assert_eq!(result.len(), 1);

    assert_approx_eq!(result[0].longitude, 13.36138, 0.0001);
    assert_approx_eq!(result[0].latitude, 38.11555, 0.0001);
}

#[test]
fn test_georadius() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA]), Ok(2));

    let mut geo_radius = |opts: RadiusOptions| -> Vec<RadiusSearchResult> {
        con.geo_radius("my_gis", 15.0, 37.0, 200.0, Unit::Kilometers, opts)
            .unwrap()
    };

    // Simple request, without extra data
    let mut result = geo_radius(RadiusOptions::default());
    result.sort_by(|a, b| Ord::cmp(&a.name, &b.name));

    assert_eq!(result.len(), 2);

    assert_eq!(result[0].name.as_str(), "Catania");
    assert_eq!(result[0].coord, None);
    assert_eq!(result[0].dist, None);

    assert_eq!(result[1].name.as_str(), "Palermo");
    assert_eq!(result[1].coord, None);
    assert_eq!(result[1].dist, None);

    // Get data with multiple fields
    let result = geo_radius(RadiusOptions::default().with_dist().order(RadiusOrder::Asc));

    assert_eq!(result.len(), 2);

    assert_eq!(result[0].name.as_str(), "Catania");
    assert_eq!(result[0].coord, None);
    assert_approx_eq!(result[0].dist.unwrap(), 56.4413, 0.001);

    assert_eq!(result[1].name.as_str(), "Palermo");
    assert_eq!(result[1].coord, None);
    assert_approx_eq!(result[1].dist.unwrap(), 190.4424, 0.001);

    let result = geo_radius(
        RadiusOptions::default()
            .with_coord()
            .order(RadiusOrder::Desc)
            .limit(1),
    );

    assert_eq!(result.len(), 1);

    assert_eq!(result[0].name.as_str(), "Palermo");
    assert_approx_eq!(result[0].coord.as_ref().unwrap().longitude, 13.361_389);
    assert_approx_eq!(result[0].coord.as_ref().unwrap().latitude, 38.115_556);
    assert_eq!(result[0].dist, None);
}

#[test]
fn test_georadius_by_member() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[PALERMO, CATANIA, AGRIGENTO]), Ok(3));

    // Simple request, without extra data
    let opts = RadiusOptions::default().order(RadiusOrder::Asc);
    let result: Vec<RadiusSearchResult> = con
        .geo_radius_by_member("my_gis", AGRIGENTO.2, 100.0, Unit::Kilometers, opts)
        .unwrap();
    let names: Vec<_> = result.iter().map(|c| c.name.as_str()).collect();

    assert_eq!(names, vec!["Agrigento", "Palermo"]);
}
