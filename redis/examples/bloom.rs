#[cfg(feature = "bloom")]
use std::process::exit;

use redis::bloom::{InfoType, SingleInfoResponse};
use redis::{bloom, RedisResult};

fn run() -> RedisResult<()> {
    use redis::Commands;
    use std::env;
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let client = redis::Client::open(redis_url.as_str())?;
    let mut con = client.get_connection()?;

    // Reserve & add
    let opts = bloom::ScalingOptions::ExpansionRate(2);
    let reserve_succeeded: bool = con.bf_reserve_options("bike:models", 0.001, 10000, opts)?;
    println!("[bf_reserve] Reserve succeeded: {reserve_succeeded}");

    let add_succeeded: bool = con.bf_add("bike:models", "Smoky Mountain Striker")?;
    println!("[bf_add] Add succeeded: {add_succeeded}");

    let exists: bool = con.bf_exists("bike:models", "Smoky Mountain Striker")?;
    println!("[bf_exists] {exists}.");

    // madd & mexists
    let items = [
        "Rocky Mountain Racer",
        "Cloudy City Cruiser",
        "Windy City Wippet",
    ];
    let results: Vec<bool> = con.bf_madd("bike:models", &items)?;
    print_results(items.to_vec(), results, "added", "not added");

    let results: Vec<bool> = con.bf_mexists("bike:models", &items)?;
    print_results(items.to_vec(), results, "exists", "does not exist");

    // info all
    let info: bloom::AllInfoResponse = con.bf_info_all("bike:models")?;
    println!("bike:models info: {:?}", info);

    // insert
    let cars = ["Audi", "BMW", "Mercedes"];
    let insert_opts = bloom::InsertOptions::default().scale(bloom::ScalingOptions::NonScaling);
    let insert_res: RedisResult<Vec<bool>> = con.bf_insert("car:models", &cars, insert_opts);
    match insert_res {
        Ok(results) => print_results(cars.to_vec(), results, "added", "not added"),
        Err(e) => println!("bf_insert failed: reason {:?}", e),
    }

    let info: bloom::AllInfoResponse = con.bf_info_all("car:models")?;
    println!("car:models info: {:?}", info);

    // return info for a specific type
    let nr_of_car_items: SingleInfoResponse = con.bf_info("car:models", InfoType::Items)?;
    println!("car:models number of items: {:?}", nr_of_car_items.value);

    let expansion_rate: SingleInfoResponse = con.bf_info("car:models", InfoType::Expansion)?;
    println!("car:models expansion rate : {:?}", expansion_rate.value);

    Ok(())
}

fn print_results(
    items: Vec<impl AsRef<str> + std::fmt::Debug>,
    results: Vec<bool>,
    success_string: &str,
    fail_string: &str,
) {
    println!("---------------------");
    for (x, y) in items.iter().zip(results.iter()) {
        let exists = if *y { success_string } else { fail_string };
        println!("item {:?} {:}.", x, exists);
    }
    println!("====================");
}

#[cfg(not(feature = "bloom"))]
fn run() -> RedisResult<()> {
    Ok(())
}

fn main() {
    if let Err(e) = run() {
        println!("{e:?}");
        exit(1);
    }
}
