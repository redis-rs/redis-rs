//! Tests for connection-layer behavior when streaming credentials providers fail.
//!
//! These tests focus on how `MultiplexedConnection` handles credentials provider failures,
//! as opposed to:
//! - `entra_id.rs` tests → Provider internals (token fetching, retry logic)
//! - `test_acl.rs` tests → Authentication mechanisms over time (token rotation, ACL operations)
//! - `test_auth.rs` tests → Integration tests for Entra ID authentication

#![cfg(all(feature = "token-based-authentication", feature = "tokio-comp"))]

mod support;

use futures_util::{Stream, StreamExt};
use redis::auth::{BasicAuth, StreamingCredentialsProvider};
use redis::{ErrorKind, RedisError, RedisResult};
use std::pin::Pin;
use std::sync::Once;
use support::*;

static INIT_LOGGER: Once = Once::new();

/// Initialize the logger for tests. Only initializes once even if called multiple times.
/// Respects RUST_LOG environment variable if set, otherwise defaults to Debug level.
fn init_logger() {
    INIT_LOGGER.call_once(|| {
        let mut builder = env_logger::builder();
        builder.is_test(true);
        if std::env::var("RUST_LOG").is_err() {
            builder.filter_level(log::LevelFilter::Debug);
        }
        builder.init();
    });
}

/// A credentials provider that immediately returns an error on the first credentials request.
///
/// This simulates scenarios where the credentials provider fails during initial connection establishment.
#[derive(Clone)]
struct ImmediatelyFailingCredentialsProvider;

impl StreamingCredentialsProvider for ImmediatelyFailingCredentialsProvider {
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        futures_util::stream::once(async move {
            Err(RedisError::from((
                ErrorKind::AuthenticationFailed,
                "Unable to fetch token from credentials provider",
            )))
        })
        .boxed()
    }
}

/// A credentials provider that returns an empty stream (closes immediately without yielding).
///
/// This simulates scenarios where the credentials provider's background task ends before providing any credentials.
#[derive(Clone)]
struct EmptyStreamCredentialsProvider;

impl StreamingCredentialsProvider for EmptyStreamCredentialsProvider {
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        futures_util::stream::empty().boxed()
    }
}

/// A credentials provider that yields valid credentials initially, then closes the stream.
///
/// This simulates scenarios where the credentials provider's background task stops after providing initial credentials (e.g., provider shutdown).
#[derive(Clone)]
struct OneTimeCredentialsProvider;

impl StreamingCredentialsProvider for OneTimeCredentialsProvider {
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        // Yield one valid credentials (default user, no password for test Redis) then close the stream
        futures_util::stream::once(async move {
            Ok(BasicAuth::new("default".to_string(), "".to_string()))
        })
        .boxed()
    }
}

/// A credentials provider that yields valid credentials initially, then returns an error.
///
/// This simulates scenarios where the credentials provider exhausts retries after providing initial credentials.
#[derive(Clone)]
struct DelayedFailureCredentialsProvider;

impl StreamingCredentialsProvider for DelayedFailureCredentialsProvider {
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        futures_util::stream::iter(vec![
            // Valid credentials for initial connection
            Ok(BasicAuth::new("default".to_string(), "".to_string())),
            // Error simulating provider exhausted retries
            Err(RedisError::from((
                ErrorKind::AuthenticationFailed,
                "Token refresh failed after max retries",
            ))),
        ])
        .boxed()
    }
}

#[cfg(test)]
mod credentials_provider_failures_tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_fails_when_initial_credentials_request_returns_error() {
        init_logger();
        let ctx = TestContext::new();

        let provider = ImmediatelyFailingCredentialsProvider;
        let config = redis::AsyncConnectionConfig::new().set_credentials_provider(provider);

        let result = ctx
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await;

        assert!(
            result.is_err(),
            "Connection should fail when the initial credentials request fails."
        );

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::AuthenticationFailed);
    }

    #[tokio::test]
    async fn test_connection_fails_when_credentials_stream_closes() {
        init_logger();
        let ctx = TestContext::new();

        let provider = EmptyStreamCredentialsProvider;
        let config = redis::AsyncConnectionConfig::new().set_credentials_provider(provider);

        let result = ctx
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await;

        assert!(
            result.is_err(),
            "Connection should fail when the credentials stream closes."
        );

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::AuthenticationFailed);
    }

    #[tokio::test]
    async fn test_connection_renders_unusable_when_the_subscription_stream_closes() {
        init_logger();
        let ctx = TestContext::new();

        let provider = OneTimeCredentialsProvider;
        let config = redis::AsyncConnectionConfig::new().set_credentials_provider(provider);

        let mut con = ctx
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .expect("Initial connection should succeed.");

        let result: RedisResult<String> = redis::cmd("PING").query_async(&mut con).await;
        assert!(result.is_ok(), "PING should succeed.");

        // Give the token rotation task time to process the stream closure
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Subsequent commands should fail because the connection is unusable
        let result: RedisResult<String> = redis::cmd("PING").query_async(&mut con).await;
        assert!(
            result.is_err(),
            "Commands should fail after the subscription stream closes unexpectedly."
        );

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::AuthenticationFailed);
        assert!(
            err.to_string().contains("re-authentication failure"),
            "Error message should mention re-authentication failure: {err}"
        );
    }

    #[tokio::test]
    async fn test_connection_renders_unusable_when_the_subscription_stream_closes_after_an_error() {
        init_logger();
        let ctx = TestContext::new();

        let provider = DelayedFailureCredentialsProvider;
        let config = redis::AsyncConnectionConfig::new().set_credentials_provider(provider);

        let mut con = ctx
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .expect("Initial connection should succeed.");

        let result: RedisResult<String> = redis::cmd("PING").query_async(&mut con).await;
        assert!(result.is_ok(), "PING should succeed.");

        // Give the token rotation task time to process the error
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Subsequent commands should fail because the connection is unusable
        let result: RedisResult<String> = redis::cmd("PING").query_async(&mut con).await;
        assert!(
            result.is_err(),
            "Commands should fail after the subscription stream returns error."
        );

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::AuthenticationFailed);
        assert!(
            err.to_string().contains("re-authentication failure"),
            "Error message should mention re-authentication failure: {err}"
        );
    }
}
