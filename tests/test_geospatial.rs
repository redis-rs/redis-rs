extern crate net2;
extern crate rand;
extern crate redis;

use redis::{Commands, PipelineCommands};

mod common;
use common::*;

#[test]
fn test_geoadd_single_tuple() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", ("13.361389", "38.115556", "Palermo")), Ok(1));
}

#[test]
fn test_geoadd_multiple_tuples() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    assert_eq!(con.geo_add("my_gis", &[("13.361389", "38.115556", "Palermo"), ("15.087269", "37.502669", "Catania")]), Ok(2));
}
