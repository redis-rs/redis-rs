//! Tests for connection-layer behavior when streaming credentials providers fail.
//!
//! These tests focus on how connections handle credentials provider failures,
//! for both `MultiplexedConnection` (single-node) and `ClusterConnection` (cluster), as opposed to:
//! - `entra_id.rs` tests → Provider internals (token fetching, retry logic)
//! - `test_acl.rs` tests → Authentication mechanisms over time (token rotation, ACL operations)
//! - `test_auth.rs` tests → Integration tests for Entra ID authentication

#![cfg(feature = "token-based-authentication")]

mod support;

use futures_util::{Stream, StreamExt};
use redis::auth::{BasicAuth, StreamingCredentialsProvider};
use redis::{ErrorKind, RedisError};
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

#[cfg(test)]
mod credentials_provider_failures_tests {
    use super::*;
    use test_macros::async_test;

    #[async_test]
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

    #[async_test]
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

    #[cfg(feature = "cluster-async")]
    mod cluster {
        use super::*;
        use redis::cluster::ClusterClientBuilder;

        #[async_test]
        async fn test_cluster_connection_fails_when_credentials_provider_returns_error() {
            init_logger();
            let cluster = TestClusterContext::new_with_cluster_client_builder(
                |builder: ClusterClientBuilder| {
                    builder.set_credentials_provider(ImmediatelyFailingCredentialsProvider)
                },
            );

            let result = cluster.client.get_async_connection().await;

            assert!(
                result.is_err(),
                "Cluster connection should fail when the credentials provider returns an error."
            );

            let err = result.err().unwrap();
            assert_eq!(err.kind(), ErrorKind::Io);
        }

        #[async_test]
        async fn test_cluster_connection_fails_when_credentials_stream_is_empty() {
            init_logger();
            let cluster = TestClusterContext::new_with_cluster_client_builder(
                |builder: ClusterClientBuilder| {
                    builder.set_credentials_provider(EmptyStreamCredentialsProvider)
                },
            );

            let result = cluster.client.get_async_connection().await;

            assert!(
                result.is_err(),
                "Cluster connection should fail when the credentials stream closes without yielding."
            );

            let err = result.err().unwrap();
            assert_eq!(err.kind(), ErrorKind::Io);
        }
    }
}
