//! Multi-tenant [asynq](https://github.com/emo-crab/asynq/) example using Redis ACLs
//!
//! Purpose
//! - Demonstrate a pattern for multi-tenant queues using key prefixes and Redis ACLs.
//! - Provide a public/shared default queue that everyone can access and tenant-specific
//!   queues that are isolated by prefixing keys with the tenant id.
//! - Show how an admin creates a tenant user with restricted permissions and how a tenant
//!   uses that account to operate on allowed keys only.
//!
//! Design summary (short):
//! - Default (public) queue: asynq:{default}:*  (accessible by all tenants)
//! - Tenant-specific queues: asynq:{<tenant>:<queue_name>}:*  (only accessible by that tenant)
//! - Admin creates users and assigns ACL rules that: enable the user, set a password,
//!   remove dangerous commands, allow only specific key patterns and channels.
//!
//! Contract (what this example shows):
//! - Inputs: an admin Redis connection (to run ACL SETUSER) and a tenant username/password.
//! - Outputs: a Redis user configured with ACL rules that limit commands and key patterns.
//! - Error modes: connection errors, ACL command failures.
//!
//! Quick ASCII flowchart (high level):
//!```
//!  Admin (operator)
//!     │
//!     │ 1. CONNECT as admin
//!     │ 2. ACL SETUSER tenant1 on >password +@all -@dangerous ~asynq:{default}:* ~asynq:{tenant1:* ...
//!     ▼
//!  Redis Server (stores ACLs + data)
//!     │
//!     ├─ stores ACL rules tied to 'tenant1' user
//!     ├─ keeps public queue keys: asynq:{default}:pending, asynq:{default}:processing, ...
//!     └─ keeps tenant queues: asynq:{tenant1:critical}:pending, asynq:{tenant1:low}:pending ...
//!
//!  Tenant (tenant1)
//!     │
//!     │ 3. CONNECT using tenant1 / password
//!     │ 4. PUSH tasks to queues within allowed key patterns
//!     │ 5. POP / PROCESS tasks only from allowed keys (or default public queue)
//!     ▼
//!  Behavior notes:
//!  - Tenant cannot run commands in the removed categories (e.g. -@dangerous)
//!  - Tenant cannot access keys outside the allowed patterns
//!  - Default queue is intentionally left accessible to allow cross-tenant shared tasks
//!```
//! Example usage flow (concrete):
//!  1) Admin: acl setuser tenant1 on >securepassword +@all -@dangerous ~asynq:{default}:* ~asynq:{tenant1:* ...
//!  2) Tenant: AUTH tenant1 securepassword
//!  3) Tenant: PUSH asynq:{tenant1:critical}:pending <task>
//!  4) Worker (authed as tenant1): POP asynq:{tenant1:critical}:pending
//!  5) All tenants can POP asynq:{default}:pending
//!
//! Why the curly-brace/hashtag? For Redis Cluster, using {...} creates a hashtag so all
//! keys that share the same tag are guaranteed to be in the same hash slot. This makes
//! multi-key atomic operations that rely on colocated keys work as expected.
//!
use redis::acl::Rule;
use redis::{RedisResult, TypedCommands};
const DEFAULT_QUEUE_NAME: &str = "default";
/// With ACL enabled (tenant: tenant1):
///
///     default queue → asynq:{default}:pending (shared, no prefix)
///     critical queue → asynq:{tenant1:critical}:pending (tenant-specific, prefixed)
///     low queue → asynq:{tenant1:low}:pending (tenant-specific, prefixed)
///
/// This design allows the default queue to serve as a shared public queue accessible by all tenants,
/// while still providing tenant isolation for custom queues. Tenants needing fully isolated queues
/// should use custom queue names which will be properly prefixed.
fn run() -> RedisResult<()> {
    use std::env;

    // Build base redis url (may be overridden by REDIS_URL env var)
    let redis_url = env::var("REDIS_URL").unwrap_or("redis://127.0.0.1/".to_string());
    let client = redis::Client::open(redis_url.as_str())?;
    let mut conn = client.get_connection()?;
    let username = "tenant1";
    let password = "securepassword";
    let rules = vec![
        // Basic permissions: on, +@all, -@dangerous, +keys, -info
        Rule::On,
        Rule::ResetChannels,
        Rule::AllCommands,
        Rule::RemoveCategory("dangerous".to_string()),
        Rule::AddCommand("keys".to_string()),
        Rule::RemoveCommand("info".to_string()),
        // Database restrictions: -select
        Rule::RemoveCommand("select".to_string()),
        // Password
        Rule::AddPass(password.to_string()),
        // Add default queue pattern - uses hashtag {DEFAULT_QUEUE_NAME} for Redis cluster routing
        Rule::Pattern(format!("asynq:{{{}}}:*", DEFAULT_QUEUE_NAME)),
        // Add tenant-specific key patterns
        Rule::Pattern(format!("asynq:{{{}:*", username)),
        // Add default key patterns
        Rule::Pattern("asynq:queues".to_string()),
        Rule::Pattern("asynq:servers:*".to_string()),
        Rule::Pattern("asynq:servers".to_string()),
        Rule::Pattern("asynq:workers".to_string()),
        Rule::Pattern("asynq:workers:*".to_string()),
        Rule::Pattern("asynq:schedulers".to_string()),
        Rule::Pattern("asynq:schedulers:*".to_string()),
        Rule::Channel("asynq:cancel".to_string()),
    ];
    // Use redis-rs acl_setuser_rules command
    // Vec<Rule> implements ToRedisArgs via blanket implementation for slices
    // Each Rule is converted to its Redis representation automatically
    conn.acl_setuser_rules(username, &rules)?;
    let tenant_user = conn.acl_getuser(username);
    println!("{:?}", tenant_user);
    let sample_rule = vec![
        Rule::On,
        Rule::ResetChannels,
        Rule::NoPass,
        Rule::AddCommand("GET".to_string()),
        Rule::AllKeys,
        Rule::Channel("*".to_string()),
        // Rule::Selector(vec![Rule::AddCommand("SET".to_string()), Rule::Pattern("key2".to_string())]),
    ];
    conn.acl_setuser_rules("sample", &sample_rule)?;
    let sample_user = conn.acl_getuser("sample");
    println!("{:?}", sample_user);
    conn.acl_deluser(&[username, "sample"])?;
    Ok(())
}

#[cfg(not(feature = "acl"))]
fn run() -> RedisResult<()> {
    Ok(())
}

fn main() {
    run().unwrap();
}
