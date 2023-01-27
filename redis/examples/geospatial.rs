use std::process::exit;

use redis::RedisResult;

#[cfg(feature = "geospatial")]
fn run() -> RedisResult<()> {
    use redis::{geo, Commands};
    use std::env;
    use std::f64;

    let redis_url = match env::var("REDIS_URL") {
        Ok(url) => url,
        Err(..) => "redis://127.0.0.1/".to_string(),
    };

    let client = redis::Client::open(redis_url.as_str())?;
    let mut con = client.get_connection()?;

    // Add some members to the geospatial index.

    let added: isize = con.geo_add(
        "gis",
        &[
            (geo::Coord::lon_lat("13.361389", "38.115556"), "Palermo"),
            (geo::Coord::lon_lat("15.087269", "37.502669"), "Catania"),
            (geo::Coord::lon_lat("13.5833332", "37.316667"), "Agrigento"),
        ],
    )?;

    println!("[geo_add] Added {added} members.");

    // Get the position of one of them.

    let position: Vec<geo::Coord<f64>> = con.geo_pos("gis", "Palermo")?;
    println!("[geo_pos] Position for Palermo: {position:?}");

    // Search members near (13.5, 37.75)

    let options = geo::RadiusOptions::default()
        .order(geo::RadiusOrder::Asc)
        .with_dist()
        .limit(2);
    let items: Vec<geo::RadiusSearchResult> =
        con.geo_radius("gis", 13.5, 37.75, 150.0, geo::Unit::Kilometers, options)?;

    for item in items {
        println!(
            "[geo_radius] {}, dist = {} Km",
            item.name,
            item.dist.unwrap_or(f64::NAN)
        );
    }

    Ok(())
}

#[cfg(not(feature = "geospatial"))]
fn run() -> RedisResult<()> {
    Ok(())
}

fn main() {
    if let Err(e) = run() {
        println!("{e:?}");
        exit(1);
    }
}
