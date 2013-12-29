#[crate_id = "redis-example#0.1"];
#[crate_type = "app"];

extern mod redis;

fn main() {
    /*
    let input = bytes!("*6\r\n$3\r\nabc\r\n:123\r\n:1\n$-1\r\n-ERR unknown command foo\r\n-\r\n");
    let resp = redis::parse_redis_value(input);
    println!("{:?}", resp);
    */

    let mut client = redis::Client::open("redis://127.0.0.1/").unwrap();
    println!("foo get: {:?}", client.get("foo"));
    println!("foo as int: {:?}", client.get_as::<int>("foo"));
    println!("Ping: {:?}", client.ping());
    println!("Keys: {:?}", client.keys("*"));
}
