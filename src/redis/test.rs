extern mod redis;

fn prepare() -> redis::Client {
	let mut client = redis::Client::open("redis://127.0.0.1/").unwrap();
	client.flushdb();
	client
}

#[test]
fn ping() {
	let mut client = prepare();
	assert!(client.ping(), true);
}

#[test]
fn get_set() {
	let mut client = prepare();
	assert!(client.get("k").is_none());	
	assert!(client.set("k", "v"));
	assert!(client.get("k") == Some(~"v"));
}