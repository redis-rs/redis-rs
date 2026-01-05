//! Token-based authentication support for Redis
use crate::types::RedisResult;
use futures_util::Stream;
use std::pin::Pin;

/// Basic authentication credentials for a Redis connection
#[derive(Debug, Clone)]
pub struct BasicAuth {
    /// The username for authentication
    pub username: String,
    /// The password for authentication
    pub password: String,
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
