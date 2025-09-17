# Redis-rs Security Guide

## ConnectionManager Security Configuration

The `ConnectionManager` provides several security features to protect against common vulnerabilities and attacks during connection initialization.

### Security Configuration

```rust
use redis::aio::{ConnectionManagerConfig, SecurityConfig};

let security_config = SecurityConfig::new()
    .set_max_client_name_size(32768)  // 32KB limit instead of default 64KB
    .set_validate_utf8(true)          // Enable UTF-8 validation
    .set_init_timeout(Duration::from_secs(10))  // Shorter timeout
    .set_enable_resource_monitoring(true);

let config = ConnectionManagerConfig::new()
    .set_security_config(security_config);

let manager = client.get_connection_manager_with_config(config).await?;
```

## Security Features

### 1. Client Name Size Limits

**Risk**: Large client names can cause memory exhaustion or DoS attacks.

**Mitigation**: Configure maximum client name size (default: 64KB).

```rust
let security_config = SecurityConfig::new()
    .set_max_client_name_size(1024); // 1KB limit
```

### 2. UTF-8 Validation

**Risk**: Invalid UTF-8 in client names can cause encoding issues in monitoring tools.

**Mitigation**: Enable UTF-8 validation for client names.

```rust
let security_config = SecurityConfig::new()
    .set_validate_utf8(true);
```

### 3. Initialization Timeouts

**Risk**: Hanging initialization commands can cause resource exhaustion.

**Mitigation**: Set reasonable timeouts for initialization pipeline.

```rust
let security_config = SecurityConfig::new()
    .set_init_timeout(Duration::from_secs(10));
```

### 4. Resource Monitoring

**Risk**: Memory leaks or resource exhaustion during initialization.

**Mitigation**: Enable resource monitoring with custom callbacks.

```rust
struct MyResourceMonitor;

impl ResourceMonitor for MyResourceMonitor {
    fn on_init_start(&self, client_info: &str) {
        log::info!("Starting initialization for: {}", client_info);
    }
    
    fn on_command_executed(&self, command: &str, duration: Duration, memory_delta: Option<i64>) {
        log::debug!("Command {} took {:?}, memory delta: {:?}", command, duration, memory_delta);
    }
    
    fn on_init_complete(&self, total_duration: Duration, final_memory: Option<u64>) {
        log::info!("Initialization completed in {:?}, final memory: {:?}", total_duration, final_memory);
    }
    
    fn on_init_failed(&self, error: &InitializationError, total_duration: Duration) {
        log::error!("Initialization failed after {:?}: {}", total_duration, error);
    }
}

let security_config = SecurityConfig::new()
    .set_resource_monitor(MyResourceMonitor);
```

## Redis CONFIG Command Security

### ⚠️ Critical Security Warning

The Redis `CONFIG` command can modify server configuration at runtime, including security settings. **Never allow untrusted clients to execute CONFIG commands.**

### Dangerous CONFIG Commands

```rust
// DANGEROUS - Can disable authentication
redis::cmd("CONFIG").arg("SET").arg("requirepass").arg("").exec(&mut conn)?;

// DANGEROUS - Can change memory limits
redis::cmd("CONFIG").arg("SET").arg("maxmemory").arg("0").exec(&mut conn)?;

// DANGEROUS - Can disable persistence
redis::cmd("CONFIG").arg("SET").arg("save").arg("").exec(&mut conn)?;
```

### Best Practices

1. **Restrict CONFIG access**: Use Redis ACLs to prevent CONFIG command access:
   ```
   ACL SETUSER myapp -config +@all
   ```

2. **Use configuration files**: Set critical settings in `redis.conf` instead of runtime CONFIG:
   ```
   requirepass mypassword
   maxmemory 100mb
   save 900 1
   ```

3. **Monitor CONFIG usage**: Log all CONFIG commands for security auditing.

4. **Validate configuration**: Always validate configuration changes before applying.

### Safe CONFIG Usage

If you must use CONFIG commands, validate inputs and use allowlists:

```rust
fn safe_config_set(conn: &mut Connection, key: &str, value: &str) -> RedisResult<()> {
    // Allowlist of safe configuration keys
    const SAFE_KEYS: &[&str] = &[
        "timeout",
        "tcp-keepalive", 
        "slowlog-log-slower-than",
        "slowlog-max-len"
    ];
    
    if !SAFE_KEYS.contains(&key) {
        return Err(RedisError::from((
            ErrorKind::ClientError,
            format!("Configuration key '{}' is not allowed", key)
        )));
    }
    
    redis::cmd("CONFIG").arg("SET").arg(key).arg(value).exec(conn)
}
```

## Error Handling Security

### Enhanced Error Handling

The ConnectionManager provides enhanced error handling for initialization failures:

```rust
use redis::aio::InitializationError;

match manager.send_packed_command(&cmd).await {
    Ok(result) => result,
    Err(e) => {
        if let Some(init_error) = e.downcast_ref::<InitializationError>() {
            if init_error.is_recoverable {
                // Retry logic for recoverable errors
                log::warn!("Recoverable initialization error: {}", init_error);
            } else {
                // Fail fast for non-recoverable errors
                log::error!("Non-recoverable initialization error: {}", init_error);
                return Err(e);
            }
        }
        Err(e)
    }
}
```

## Protocol Security

### RESP Protocol Validation

Redis-rs automatically handles malformed RESP protocol, but be aware of these security considerations:

1. **Large payloads**: Redis may accept very large command arguments. Use size limits.
2. **Protocol errors**: Malformed RESP is handled gracefully by Redis server.
3. **Connection drops**: Network interruptions are handled with automatic reconnection.

### Memory Safety

Redis server is memory-safe regarding protocol handling:
- Invalid UTF-8 sequences are accepted as binary data
- Large commands are processed but may hit memory limits
- Protocol violations return clear error messages

## Deployment Security Checklist

- [ ] Set `requirepass` in Redis configuration
- [ ] Configure Redis ACLs for fine-grained access control
- [ ] Use TLS for network connections (`rediss://` URLs)
- [ ] Set appropriate `maxmemory` limits
- [ ] Disable dangerous commands: `CONFIG`, `FLUSHALL`, `SHUTDOWN`
- [ ] Configure connection limits with `maxclients`
- [ ] Enable Redis logging for security monitoring
- [ ] Use ConnectionManager security configuration
- [ ] Implement resource monitoring
- [ ] Set appropriate timeouts
- [ ] Validate all user inputs before Redis commands

## Example Secure Configuration

```rust
use redis::aio::{ConnectionManagerConfig, SecurityConfig, ResourceMonitor};
use std::time::Duration;

// Security configuration
let security_config = SecurityConfig::new()
    .set_max_client_name_size(1024)           // 1KB limit
    .set_validate_utf8(true)                  // Validate UTF-8
    .set_init_timeout(Duration::from_secs(5)) // 5 second timeout
    .set_resource_monitor(MyResourceMonitor); // Custom monitoring

// Connection configuration
let config = ConnectionManagerConfig::new()
    .set_response_timeout(Duration::from_secs(30))
    .set_connection_timeout(Duration::from_secs(10))
    .set_number_of_retries(3)
    .set_security_config(security_config);

// Secure connection with authentication
let client = Client::open("rediss://:password@secure-redis:6380/")?;
let manager = client.get_connection_manager_with_config(config).await?;
```

This configuration provides:
- Encrypted connections (TLS)
- Authentication
- Size limits and validation
- Reasonable timeouts
- Resource monitoring
- Limited retry attempts
