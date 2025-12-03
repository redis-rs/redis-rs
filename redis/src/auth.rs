//! Token-based authentication support for Redis
use crate::types::RedisResult;
use futures_util::Stream;
use std::future::Future;
use std::pin::Pin;

/// Basic authentication credentials for a Redis connection
#[derive(Debug, Clone)]
pub struct BasicAuth {
    /// The username for authentication
    pub username: String,
    /// The password for authentication
    pub password: String,
}

/// Trait for providing authentication credentials
///
/// This trait allows different authentication mechanisms to be plugged into
/// the Redis client, such as static passwords, token-based authentication,
/// or dynamic credential providers like Azure Entra ID.
pub trait CredentialsProvider: Send + Sync {
    /// Get the current authentication credentials
    ///
    /// This function should return valid credentials that can be used for
    /// Redis authentication. If the credentials are expired or invalid,
    /// the implementation should refresh them before returning.
    fn get_credentials(&self) -> RedisResult<BasicAuth>;
}

/// Async version of the credentials provider trait
pub trait AsyncCredentialsProvider: Send + Sync {
    /// Get the current authentication credentials asynchronously
    fn get_credentials(&self) -> Box<dyn Future<Output = RedisResult<BasicAuth>> + Send>;
}

/// Trait for providing credentials in a streaming fashion
///
/// This allows connections to subscribe to credential updates and automatically
/// re-authenticate when tokens are refreshed, preventing connection failures
/// due to token expiration.
pub trait StreamingCredentialsProvider: Send + Sync {
    /// Get an independent stream of credentials.
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>>;
}

/// Static credentials provider that always returns the same credentials
#[derive(Debug, Clone)]
pub struct StaticCredentialsProvider {
    credentials: BasicAuth,
}

impl StaticCredentialsProvider {
    /// Create a new static credentials provider with a password
    pub fn new(username: String, password: String) -> Self {
        Self {
            credentials: BasicAuth { username, password },
        }
    }
}

impl CredentialsProvider for StaticCredentialsProvider {
    fn get_credentials(&self) -> RedisResult<BasicAuth> {
        Ok(self.credentials.clone())
    }
}
