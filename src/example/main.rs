#[crate_id = "redis-example#0.1"];
#[crate_type = "bin"];

extern mod redis;

use std::io::timer::sleep;

fn main() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();

    // for fun do it in a task
    do spawn {
        let mut con = client.get_connection().unwrap();
        con.set("foo", 42);
        con.set("bar", "test");
        println!("foo get: {:?}", con.get("foo"));
        println!("foo as int: {:?}", con.get_as::<int>("foo"));
        println!("ping: {:?}", con.ping());
        println!("keys: {:?}", con.keys("*"));
        println!("foo type: {:?}", con.get_type("foo"));
        println!("foo exists: {:?}", con.exists("foo"));

        let info = con.info();
        println!("info role: {:?}", info.find(&~"role"));
        println!("info version: {:?}", info.find(&~"redis_version"));

        let script = redis::Script::new("
            return tonumber(ARGV[1]);
        ");
        println!("script result: {:?}", con.call_script(
            &script, [], [redis::StrArg("42")]));

        println!("last save: {}", con.lastsave().rfc822());
        println!("server time: {}", con.time().rfc822());

        println!("Waiting for item: {:?}", con.blpop(["foox"], 5.0));
    }

    // second task that puts an item into a list
    do spawn {
        let mut con = client.get_connection().unwrap();
        println!("Pushing an item in 1 sec");
        sleep(1000);
        con.rpush("foox", "hello");
        println!("Pushed");
    }
}
