#[crate_id = "redis-example#0.1"];
#[crate_type = "bin"];

extern mod redis;

fn main() {
    let mut client = redis::Client::open("redis://127.0.0.1/").unwrap();
    client.set("foo", 42);
    client.set("bar", "test");
    println!("foo get: {:?}", client.get("foo"));
    println!("foo as int: {:?}", client.get_as::<int>("foo"));
    println!("ping: {:?}", client.ping());
    println!("keys: {:?}", client.keys("*"));
    println!("foo type: {:?}", client.get_type("foo"));
    println!("foo exists: {:?}", client.exists("foo"));

    let info = client.info();
    println!("info role: {:?}", info.find(&~"role"));
    println!("info version: {:?}", info.find(&~"redis_version"));

    let script = redis::Script::new("
        return tonumber(ARGV[1]);
    ");
    println!("script result: {:?}", client.call_script(
        &script, [], [redis::StrArg("42")]));

    println!("last save: {}", client.lastsave().rfc822());
    println!("server time: {}", client.time().rfc822());
}
