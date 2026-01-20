use redis::acl::Rule;
use redis::{RedisResult, TypedCommands};
pub const DEFAULT_QUEUE_NAME: &str = "default";
#[cfg(feature = "acl")]
fn run() -> RedisResult<()> {
    use std::env;
    let redis_url = env::var("REDIS_URL").unwrap_or("redis://127.0.0.1/".to_string());
    let client = redis::Client::open(redis_url.as_str())?;
    let mut conn = client.get_connection()?;
    let rules = build_acl_rules("asynq", "password");
    // Use redis-rs acl_setuser_rules command
    // Vec<Rule> implements ToRedisArgs via blanket implementation for slices
    // Each Rule is converted to its Redis representation automatically
    conn.acl_setuser_rules("asynq", &rules)?;
    let asynq_user = conn.acl_getuser("asynq");
    println!("{:?}", asynq_user);
    let sample_rule = vec![
        Rule::On,
        Rule::NoPass,
        Rule::AddCommand("GET".to_string()),
        Rule::AllKeys,
        Rule::Channel("*".to_string()),
        Rule::Selector(vec!["+SET".to_string(), "~key2".to_string()]),
    ];
    conn.acl_setuser_rules("sample", &sample_rule)?;
    let sample_user = conn.acl_getuser("sample");
    println!("{:?}", sample_user);
    conn.acl_deluser(&["asynq", "sample"])?;
    Ok(())
}

/// Generate ACL rules list
fn build_acl_rules(username: &str, password: &str) -> Vec<Rule> {
    let mut rules = Vec::new();
    // Basic permissions: on, +@all, -@dangerous, +keys, -info
    rules.push(Rule::On);
    rules.push(Rule::AllCommands);
    rules.push(Rule::RemoveCategory("dangerous".to_string()));
    rules.push(Rule::AddCommand("keys".to_string()));
    rules.push(Rule::RemoveCommand("info".to_string()));
    // Database restrictions: -select
    rules.push(Rule::RemoveCommand("select".to_string()));
    // Password
    rules.push(Rule::AddPass(password.to_string()));
    // Add default queue pattern - uses hashtag {DEFAULT_QUEUE_NAME} for Redis cluster routing
    rules.push(Rule::Pattern(format!("asynq:{{{}}}:*", DEFAULT_QUEUE_NAME)));
    // Add tenant-specific key patterns
    rules.push(Rule::Pattern(format!("asynq:{{{}:*", username)));
    // Add default key patterns
    let default_key_patterns = vec![
        Rule::Pattern("asynq:queues".to_string()),
        Rule::Pattern("asynq:servers:*".to_string()),
        Rule::Pattern("asynq:servers".to_string()),
        Rule::Pattern("asynq:workers".to_string()),
        Rule::Pattern("asynq:workers:*".to_string()),
        Rule::Pattern("asynq:schedulers".to_string()),
        Rule::Pattern("asynq:schedulers:*".to_string()),
        Rule::Other("&asynq:cancel".to_string()),
    ];
    for pattern in default_key_patterns {
        rules.push(pattern);
    }
    rules
}
#[cfg(not(feature = "acl"))]
fn run() -> RedisResult<()> {
    Ok(())
}

fn main() {
    run().unwrap();
}
