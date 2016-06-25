extern crate redis;
extern crate mioco;

fn follow_channel(channel: &str, coro_id: i32) -> redis::RedisResult<()> {
    let client = try!(redis::Client::open("redis://127.0.0.1/"));
    let mut pubsub = try!(client.get_pubsub());
    try!(pubsub.subscribe(channel));

    loop {
        let msg = try!(pubsub.get_message());
        let payload : String = try!(msg.get_payload());
        println!("Coroutine {}: {}: {}", coro_id, msg.get_channel_name(), payload);
    }
}

fn send_messages(channel: &str, message: &str) -> redis::RedisResult<()> {
    let client = try!(redis::Client::open("redis://127.0.0.1/"));
    let con = try!(client.get_connection());
    loop {
        let _ : () = try!(redis::cmd("PUBLISH").arg(channel).arg(message).query(&con));
        mioco::sleep_ms(1000);
    }
}

fn main() {
    mioco::start(move || {
        for i in 0..10 {
            mioco::spawn(move || {
                follow_channel("chan1", i).unwrap();
            });
        }
        mioco::spawn(move || {
            send_messages("chan1", "Hi followers!").unwrap();
        });
    }).unwrap();
}
