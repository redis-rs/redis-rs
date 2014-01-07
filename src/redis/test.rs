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
fn get_set_exists() {
	let mut client = prepare();
	assert!(client.get("foo").is_none());	

	assert!(client.set("foo", "test"));
	assert!(client.get("foo") == Some(~"test"));
	assert!(client.exists("foo"));
	assert!(client.get_type("foo") == StringType);

	assert!(client.set("bar", 42));
	assert!(client.get_as::<int>("bar") == Some(42));
	assert!(client.get_type("bar") == StringType);
}

#[test]
fn script() {
	let mut client = prepare();
	let script = redis::Script::new("
        return tonumber(ARGV[1]);
    ");

	assert!(
		client.call_script(&script, [], [redis::StrArg("42")]) == redis::Int(42)
	);
}