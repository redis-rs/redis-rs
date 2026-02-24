//! Tests for streaming credentials provider support with Redis Cluster connections.
//!
//! These tests verify that `ClusterClientBuilder::set_credentials_provider` correctly threads
//! the credentials provider through to each individual node connection, enabling token-based
//! authentication on cluster deployments.

#![cfg(all(
    feature = "cluster-async",
    feature = "token-based-authentication",
    feature = "tokio-comp"
))]

mod support;

use futures_util::{Stream, StreamExt};
use redis::auth::{BasicAuth, StreamingCredentialsProvider};
use redis::cluster::ClusterClientBuilder;
use redis::{cmd, ErrorKind, RedisError, RedisResult};
use std::pin::Pin;
use std::sync::Once;
use support::*;

static INIT_LOGGER: Once = Once::new();

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

/// Credentials provider that yields valid default-user credentials and keeps the stream open.
///
/// Uses `futures_util::stream::pending()` after the initial yield to prevent the
/// `MultiplexedConnection` from marking the connection as unusable due to stream closure.
#[derive(Clone)]
struct DefaultCredentialsProvider;

impl StreamingCredentialsProvider for DefaultCredentialsProvider {
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        futures_util::stream::once(async {
            Ok(BasicAuth::new("default".to_string(), String::new()))
        })
        .chain(futures_util::stream::pending())
        .boxed()
    }
}

/// Credentials provider that immediately returns an error.
#[derive(Clone)]
struct ImmediatelyFailingCredentialsProvider;

impl StreamingCredentialsProvider for ImmediatelyFailingCredentialsProvider {
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        futures_util::stream::once(async {
            Err(RedisError::from((
                ErrorKind::AuthenticationFailed,
                "Unable to fetch token from credentials provider",
            )))
        })
        .boxed()
    }
}

/// Credentials provider that returns an empty stream (closes without yielding).
#[derive(Clone)]
struct EmptyStreamCredentialsProvider;

impl StreamingCredentialsProvider for EmptyStreamCredentialsProvider {
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        futures_util::stream::empty().boxed()
    }
}

#[cfg(test)]
mod cluster_credentials_provider_tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_async_basic_auth_with_credentials_provider() {
        init_logger();
        let cluster =
            TestClusterContext::new_with_cluster_client_builder(|builder: ClusterClientBuilder| {
                builder.set_credentials_provider(DefaultCredentialsProvider)
            });

        let mut connection = cluster.async_connection().await;

        cmd("SET")
            .arg("test_key")
            .arg("test_value")
            .exec_async(&mut connection)
            .await
            .expect("SET should succeed with credentials provider");

        let result: String = cmd("GET")
            .arg("test_key")
            .query_async(&mut connection)
            .await
            .expect("GET should succeed with credentials provider");

        assert_eq!(result, "test_value");
    }

    #[tokio::test]
    async fn test_cluster_connection_fails_when_credentials_provider_returns_error() {
        init_logger();
        let cluster =
            TestClusterContext::new_with_cluster_client_builder(|builder: ClusterClientBuilder| {
                builder.set_credentials_provider(ImmediatelyFailingCredentialsProvider)
            });

        let result = cluster.client.get_async_connection().await;

        assert!(
            result.is_err(),
            "Cluster connection should fail when the credentials provider returns an error."
        );
    }

    #[tokio::test]
    async fn test_cluster_connection_fails_when_credentials_stream_is_empty() {
        init_logger();
        let cluster =
            TestClusterContext::new_with_cluster_client_builder(|builder: ClusterClientBuilder| {
                builder.set_credentials_provider(EmptyStreamCredentialsProvider)
            });

        let result = cluster.client.get_async_connection().await;

        assert!(
            result.is_err(),
            "Cluster connection should fail when the credentials stream closes without yielding."
        );
    }
}
