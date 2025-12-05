# Token-Based Authentication with Azure Entra ID

This document describes how to use token-based authentication with Azure Entra ID in redis-rs, providing secure, dynamic authentication for Redis connections.

## Overview

Token-based authentication allows you to authenticate to Redis using Azure Entra ID tokens instead of static passwords. This provides several benefits:

- **Enhanced Security**: Tokens have limited lifetimes and can be automatically refreshed
- **Centralized Identity Management**: Leverage Azure Entra ID for user and service authentication
- **Audit and Compliance**: Better tracking and auditing of authentication events
- **Zero-Trust Architecture**: Support for modern security models

## Features

- **Automatic Token Refresh & Streaming of Credentials**: Seamlessly handle token expiration and stream updated credentials to prevent connection errors due to token expiration
- **Multiple Authentication Flows**: Service principals, managed identities, and custom `TokenCredential` implementations
- **Configurable Refresh Policies**: Customizable refresh thresholds and retry behavior

## Architecture

### Async-First Design
- Full async/await support for non-blocking operations
- Seamless integration with multiplexed connections

### Streaming Credentials Pattern
1. The `EntraIdCredentialsProvider` implements the `StreamingCredentialsProvider` trait, which allows clients to subscribe to a stream of credentials.
2. An `EntraIdCredentialsProvider` can be created with one of the public constructors. Each of them creates a specific `TokenCredential` implementation from the `azure_identity` crate.
3. The `EntraIdCredentialsProvider` starts a background task for token refresh.
4. The `Client` holds a `StreamingCredentialsProvider` and uses it to authenticate connections.
5. A `Client` can be instantiated with a credentials provider via the `Client::open_with_credentials_provider()` constructor. A credentials provider can also be added to an existing client using the client's `with_credentials_provider()` function.
6. The `EntraIdCredentialsProvider` keeps the current token and provides it to a `Client` when it establishes a new `multiplexed_connection`. Before the connection gets established, it creates a background task, which subscribes for credential updates.
7. When the token is refreshed, the `EntraIdCredentialsProvider` notifies all subscribers with the new credentials.
8. When a subscriber receives the new credentials, it uses them to re-authenticate itself.

## Quick Start

### 1. Enable the Feature

Add the `entra-id` feature to your `Cargo.toml`:

```toml
[dependencies]
redis = { version = "0.32.7", features = ["entra-id", "tokio-comp"] }
```

### 2. Basic Usage with DeveloperToolsCredential

```rust
use redis::{Client, EntraIdCredentialsProvider, RetryConfig};

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    // Create the credentials provider using the DeveloperToolsCredential
    let mut provider = EntraIdCredentialsProvider::new_developer_tools()?;
    provider.start(RetryConfig::default());

    // Create Redis client with credentials provider
    let client = Client::open_with_credentials_provider(
        "redis://your-redis-instance.com:6380",
        provider
    )?;

    // Use the client to get a multiplexed connection
    let mut con = client.get_multiplexed_async_connection().await?;
    redis::cmd("SET")
        .arg("my_key")
        .arg(42i32)
        .exec_async(&mut con)
        .await?;
    let result: Option<String> = redis::cmd("GET")
        .arg("my_key")
        .query_async(&mut con)
        .await?;

    Ok(())
}
```

## Authentication Flows

### DeveloperToolsCredential (Recommended for Development)

The `DeveloperToolsCredential` tries the following credential types, in this order, stopping when one provides a token:
* [`AzureCliCredential`]
* [`AzureDeveloperCliCredential`]

```rust
let provider = EntraIdCredentialsProvider::new_developer_tools()?;
```

### Service Principal with Client Secret

For production applications:

```rust
let provider = EntraIdCredentialsProvider::new_client_secret(
    "your-tenant-id".to_string(),
    "your-client-id".to_string(),
    "your-client-secret".to_string(),
)?;
```

### Service Principal with Certificate

For enhanced security:

```rust
use redis::ClientCertificate;
use std::fs;

// Load certificate from file
let certificate_base64 = fs::read_to_string("path/to/base64_pkcs12_certificate")
    .expect("Base64 PKCS12 certificate not found.")
    .trim()
    .to_string();

// Create the credentials provider using service principal with client certificate
let provider = EntraIdCredentialsProvider::new_client_certificate(
    "your-tenant-id".to_string(),
    "your-client-id".to_string(),
    ClientCertificate {
        base64_pkcs12: certificate_base64, // Base64 encoded PKCS12 data
        password: None,
    },
)?;
```

### Managed Identity

For Azure-hosted applications:

```rust
use redis::{ManagedIdentityCredentialOptions, UserAssignedId};

// System-assigned managed identity
let provider = EntraIdCredentialsProvider::new_system_assigned_managed_identity()?;

// User-assigned managed identity
let provider = EntraIdCredentialsProvider::new_user_assigned_managed_identity()?;
// or with custom scopes and identity specification
let provider = EntraIdCredentialsProvider::new_user_assigned_managed_identity_with_scopes(
    vec!["your-scope".to_string()],
    Some(ManagedIdentityCredentialOptions {
        // Specify the user-assigned identity using one of:
        user_assigned_id: Some(UserAssignedId::ClientId("your-client-id".to_string())),
        // or: user_assigned_id: Some(UserAssignedId::ObjectId("your-object-id".to_string())),
        // or: user_assigned_id: Some(UserAssignedId::ResourceId("your-resource-id".to_string())),
        ..Default::default()
    }),
)?;
```

## Advanced Configuration

### TokenRefreshConfig
The `TokenRefreshConfig` allows further customization of the token refresh behavior, based on the token's expiration time, when applicable.

- `expiration_refresh_ratio`: Fraction of token lifetime before refresh (0.0-1.0)
- `retry_config`: Configuration for retry behavior on failures

### RetryConfig

Configuration for handling failed token refresh attempts.

- `max_attempts`: Maximum number of retry attempts for token refresh.
- `initial_delay`: Initial delay before attempting to refresh the token after a failure. Subsequent retries use exponential backoff based on this value.
- `max_delay`: Upper bound for retry delays to prevent excessively long waits. Delays will never exceed this value, even with exponential backoff.
- `backoff_multiplier`: Growth factor for exponential backoff (typically 2.0 for doubling). Each retry delay is multiplied by this value: `delay = delay * backoff_multiplier`.
- `jitter_percentage`: Random variation added to delays as a fraction of the calculated delay (0.0 to 1.0). For example, 0.5 means up to ±50% variation to prevent synchronized retries.

### Token Refresh with Custom Configuration
The token refresh behavior can be customized by providing a `RetryConfig` when starting the provider:

```rust
use redis::RetryConfig;
use std::time::Duration;

let mut provider = EntraIdCredentialsProvider::new_developer_tools()?;

provider.start(RetryConfig {
    max_attempts: 3,
    initial_delay: Duration::from_millis(100),
    max_delay: Duration::from_secs(30),
    backoff_multiplier: 2.0,
    jitter_percentage: 0.1,
});
```

## Error Handling

The library provides comprehensive error handling for authentication and token refresh failures.
The background token refresh service will automatically retry failed token refreshes according to the retry configuration.
Once the maximum number of attempts is reached, the service will stop retrying and the underlying error will be propagated to the subscribers.

## Best Practices

### 1. Use Appropriate Credential Types

- **Development**: `DeveloperToolsCredential`
- **Production Services**: Service Principal with certificate
- **Azure-hosted Apps**: Managed Identity

### 2. Handle Token Expiration

- Use background refresh services for long-running applications
- Implement proper error handling for authentication failures

### 3. Security Considerations

- Store client secrets securely (Azure Key Vault, environment variables)
- Use certificates instead of secrets when possible

## Compatibility

- **Redis Versions**: Compatible with Redis 6.0+ (ACL support required)
- **Azure Redis**: Fully compatible with Azure Cache for Redis

## Troubleshooting

### Common Issues

1. **"Authentication failed"**: Check your credentials and permissions
2. **"Token expired"**: Ensure automatic refresh is properly configured
3. **"Connection timeout"**: Check network connectivity and Redis endpoint