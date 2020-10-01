// make sure to enable the `r2d2` feature in your dependencies:
// redis = { version = "0.17.0", features = ['r2d2'] }

use redis::Commands;

fn main() {
    // get redis client
    let client: redis::Client = redis::Client::open("redis://127.0.0.1/")
        .unwrap_or_else(|e| panic!("Error connecting to redis: {}", e));

    // create r2d2 pool
    let pool: r2d2::Pool<redis::Client> = r2d2::Pool::builder()
        .max_size(15)
        .build(client)
        .unwrap_or_else(|e| panic!("Error building redis pool: {}", e));

    // get a connection from the pool
    let mut conn: r2d2::PooledConnection<redis::Client> = pool.get().unwrap();

    // use the connection as usual
    let _: () = conn.set("KEY", "VALUE").unwrap();
    let val: String = conn.get("KEY").unwrap();
    println!("{}", val)
}
