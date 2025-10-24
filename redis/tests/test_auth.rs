#![cfg(feature = "token-based-authentication")]

use redis::auth_management::credentials_management_utils;
use redis::TokenRefreshConfig;

use std::sync::LazyLock;

const TOKEN_HEADER: &str = "header";
const TOKEN_PAYLOAD: &str = "eyJvaWQiOiIxMjM0NTY3OC05YWJjLWRlZi0xMjM0LTU2Nzg5YWJjZGVmMCJ9"; // Payload with "oid" claim
const TOKEN_PAYLOAD_NO_OID: &str =
    "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNzM1Njg5NjAwfQ"; // Payload with no "oid" claim
const TOKEN_SIGNATURE: &str = "signature";

const OID_CLAIM_VALUE: &str = "12345678-9abc-def-1234-56789abcdef0";

static TOKEN: LazyLock<String> =
    LazyLock::new(|| format!("{TOKEN_HEADER}.{TOKEN_PAYLOAD}.{TOKEN_SIGNATURE}"));
static TOKEN_WITH_NO_OID: LazyLock<String> =
    LazyLock::new(|| format!("{TOKEN_HEADER}.{TOKEN_PAYLOAD_NO_OID}.{TOKEN_SIGNATURE}"));
static INVALID_TOKEN: LazyLock<String> =
    LazyLock::new(|| format!("{TOKEN_HEADER}.{TOKEN_PAYLOAD}"));

#[test]
fn test_token_refresh_config() {
    let config = TokenRefreshConfig::default();
    assert_eq!(config.expiration_refresh_ratio, 0.8);

    let custom_config = TokenRefreshConfig::default().set_expiration_refresh_ratio(0.9);
    assert_eq!(custom_config.expiration_refresh_ratio, 0.9);
}

#[test]
fn test_refresh_threshold_calculation() {
    use std::time::{Duration, SystemTime};
    let config = TokenRefreshConfig::default(); // Default refresh ratio is 0.8

    let received_at = SystemTime::now();
    let expires_at = received_at + Duration::from_secs(3600); // 1 hour

    let threshold = credentials_management_utils::calculate_refresh_threshold(
        received_at,
        expires_at,
        config.expiration_refresh_ratio,
    );
    assert!(threshold.is_some());
    assert_eq!(threshold.unwrap(), Duration::from_secs(2880)); // 80% of 3600
}

#[cfg(all(feature = "token-based-authentication", feature = "entra-id"))]
#[test]
fn test_extract_oid_from_jwt() {
    let result = credentials_management_utils::extract_oid_from_jwt(TOKEN.as_str());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), OID_CLAIM_VALUE);
}

#[cfg(all(feature = "token-based-authentication", feature = "entra-id"))]
#[test]
fn test_extract_oid_from_jwt_with_invalid_token() {
    let result = credentials_management_utils::extract_oid_from_jwt(INVALID_TOKEN.as_str());
    assert!(result.is_err());
    assert_eq!(result.err().unwrap(), "Invalid JWT: must have 3 parts");
}

#[cfg(all(feature = "token-based-authentication", feature = "entra-id"))]
#[test]
fn test_extract_oid_from_jwt_with_no_oid_claim() {
    let result = credentials_management_utils::extract_oid_from_jwt(TOKEN_WITH_NO_OID.as_str());
    assert!(result.is_err());
    assert_eq!(result.err().unwrap(), "OID claim not found");
}

#[cfg(all(feature = "entra-id", test))]
mod entra_id_mock_tests {
    use azure_core::credentials::{AccessToken, Secret, TokenCredential};
    use azure_core::Error as AzureError;
    use futures_util::StreamExt;
    use redis::{
        EntraIdCredentialsProvider, RetryConfig, StreamingCredentialsProvider, REDIS_SCOPE_DEFAULT,
    };
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, LazyLock};
    use time::OffsetDateTime;
    use tokio::sync::Mutex;

    use crate::{OID_CLAIM_VALUE, TOKEN_PAYLOAD, TOKEN_SIGNATURE};

    static MOCKED_TOKEN: LazyLock<String> =
        LazyLock::new(|| format!("mock_jwt_token.{}.{}", TOKEN_PAYLOAD, TOKEN_SIGNATURE));

    static MOCKED_TOKEN_1: LazyLock<String> =
        LazyLock::new(|| format!("mock_jwt_token1.{}.{}", TOKEN_PAYLOAD, TOKEN_SIGNATURE));
    static MOCKED_TOKEN_2: LazyLock<String> =
        LazyLock::new(|| format!("mock_jwt_token2.{}.{}", TOKEN_PAYLOAD, TOKEN_SIGNATURE));
    static MOCKED_TOKEN_3: LazyLock<String> =
        LazyLock::new(|| format!("mock_jwt_token3.{}.{}", TOKEN_PAYLOAD, TOKEN_SIGNATURE));

    /// Mock TokenCredential that simulates Azure Identity behavior
    #[derive(Debug)]
    struct MockTokenCredential {
        /// Counter to track how many times get_token was called
        call_count: Arc<AtomicUsize>,
        /// Predefined responses to return
        responses: Arc<Mutex<VecDeque<Result<AccessToken, AzureError>>>>,
    }

    impl MockTokenCredential {
        /// Create a mock that always succeeds
        fn success() -> Self {
            let token = AccessToken {
                token: Secret::new(MOCKED_TOKEN.as_str()),
                expires_on: OffsetDateTime::now_utc() + time::Duration::hours(1),
            };

            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                responses: Arc::new(Mutex::new(VecDeque::from(vec![Ok(token)]))),
            }
        }

        /// Create a mock that always fails with an authentication error
        fn failure() -> Self {
            let error = AzureError::new(
                azure_core::error::ErrorKind::Credential,
                "Authentication failed due to invalid credentials".to_string(),
            );

            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                responses: Arc::new(Mutex::new(VecDeque::from(vec![Err(error)]))),
            }
        }

        /// Creates a mock that simulates alternating token refresh responses. (first call fails, second succeeds)
        fn alternating_fail_success() -> Self {
            let error = AzureError::new(
                azure_core::error::ErrorKind::Credential,
                "Temporary failure".to_string(),
            );

            let token = AccessToken {
                token: Secret::new(MOCKED_TOKEN.as_str()),
                expires_on: OffsetDateTime::now_utc() + time::Duration::hours(1),
            };

            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                responses: Arc::new(Mutex::new(VecDeque::from(vec![Err(error), Ok(token)]))),
            }
        }

        /// Create a mock that simulates multiple token refreshes
        fn multiple_tokens() -> Self {
            let time_now = OffsetDateTime::now_utc();
            let tokens = vec![
                Ok(AccessToken {
                    token: Secret::new(MOCKED_TOKEN_1.as_str()),
                    expires_on: time_now + time::Duration::seconds(1),
                }),
                Ok(AccessToken {
                    token: Secret::new(MOCKED_TOKEN_2.as_str()),
                    expires_on: time_now + time::Duration::seconds(2),
                }),
                Ok(AccessToken {
                    token: Secret::new(MOCKED_TOKEN_3.as_str()),
                    expires_on: time_now + time::Duration::seconds(3),
                }),
            ];

            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                responses: Arc::new(Mutex::new(VecDeque::from(tokens))),
            }
        }
    }

    #[async_trait::async_trait]
    impl TokenCredential for MockTokenCredential {
        async fn get_token(
            &self,
            _scopes: &[&str],
            _options: Option<azure_core::credentials::TokenRequestOptions>,
        ) -> azure_core::Result<AccessToken> {
            self.call_count.fetch_add(1, Ordering::SeqCst);

            let mut responses = self.responses.lock().await;

            // After popping the response, queue it back to simulate a cyclic behavior.
            match responses.pop_front() {
                Some(Ok(token)) => {
                    responses.push_back(Ok(token.clone()));
                    Ok(token)
                }
                Some(Err(err)) => {
                    responses.push_back(Err(AzureError::new(
                        azure_core::error::ErrorKind::Credential,
                        "Mock authentication failed",
                    )));
                    Err(err)
                }
                None => {
                    unreachable!("No more responses");
                }
            }
        }
    }

    /// Helper to create a mock EntraIdCredentialsProvider
    fn create_mock_entra_id_credentials_provider(
        mock_credential: MockTokenCredential,
        scopes: Vec<String>,
    ) -> EntraIdCredentialsProvider {
        EntraIdCredentialsProvider::new_with_credential(Arc::new(mock_credential), scopes).unwrap()
    }

    #[tokio::test]
    async fn test_mock_successful_authentication() {
        let mock_credential = MockTokenCredential::success();
        let call_count_ref = mock_credential.call_count.clone();

        let mut provider = create_mock_entra_id_credentials_provider(
            mock_credential,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        );
        provider.start(RetryConfig::default());

        // Wait a bit for the background task to run
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify that get_token was called
        assert!(call_count_ref.load(Ordering::SeqCst) > 0);

        // Test that the subscription stream yields the correct credentials
        let mut stream = provider.subscribe();
        let credentials = stream.next().await.unwrap().unwrap();

        assert!(!credentials.username.is_empty());
        assert!(!credentials.password.is_empty());
        assert_eq!(credentials.username, OID_CLAIM_VALUE);
        assert_eq!(credentials.password, MOCKED_TOKEN.as_str());
    }

    #[tokio::test]
    async fn test_mock_authentication_failure() {
        let mock_credential = MockTokenCredential::failure();
        let call_count_ref = mock_credential.call_count.clone();

        let mut provider = create_mock_entra_id_credentials_provider(
            mock_credential,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        );
        provider.start(RetryConfig {
            max_attempts: 1, // It's really important to set the max_attempt to one, otherwise the refresh loop will cycle through the error.
            initial_delay: std::time::Duration::from_millis(10),
            max_delay: std::time::Duration::from_millis(100),
            backoff_multiplier: 2.0,
            jitter_percentage: 0.0,
        });

        // Test that the stream returns an error once the maximum number of retries is reached
        let mut stream = provider.subscribe();
        if let Some(result) = stream.next().await {
            assert!(call_count_ref.load(Ordering::SeqCst) > 0);
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("authentication failed"));
        }
    }

    #[tokio::test]
    async fn test_mock_retry_mechanism() {
        let mock_credential = MockTokenCredential::alternating_fail_success();
        let call_count_ref = mock_credential.call_count.clone();

        let mut provider = create_mock_entra_id_credentials_provider(
            mock_credential,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        );
        provider.start(RetryConfig::default());

        // Wait for the retries to complete
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // get_token should have been called multiple times (initial failure + retry)
        assert!(call_count_ref.load(Ordering::SeqCst) >= 2);

        // Eventually should get successful credentials
        let mut stream = provider.subscribe();
        let credentials = stream.next().await.unwrap().unwrap();
        assert_eq!(credentials.username, OID_CLAIM_VALUE);
        assert_eq!(credentials.password, MOCKED_TOKEN.as_str());
    }

    #[tokio::test]
    async fn test_mock_multiple_subscribers() {
        let mock_credential = MockTokenCredential::multiple_tokens();
        let call_count_ref = mock_credential.call_count.clone();

        let mut provider = create_mock_entra_id_credentials_provider(
            mock_credential,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        );
        provider.start(RetryConfig::default());

        // Create multiple subscribers
        let mut stream1 = provider.subscribe();
        let mut stream2 = provider.subscribe();
        let mut stream3 = provider.subscribe();

        // All subscribers should receive the same initial credentials
        let credentials1 = stream1.next().await.unwrap().unwrap();
        let credentials2 = stream2.next().await.unwrap().unwrap();
        let credentials3 = stream3.next().await.unwrap().unwrap();

        assert_eq!(credentials1.password, credentials2.password);
        assert_eq!(credentials2.password, credentials3.password);

        assert_eq!(credentials1.password, MOCKED_TOKEN_1.as_str());

        // Verify that get_token was called only once
        assert_eq!(call_count_ref.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_mock_multiple_tokens_over_time() {
        let mock_credential = MockTokenCredential::multiple_tokens();
        let call_count_ref = mock_credential.call_count.clone();
        let mut provider = create_mock_entra_id_credentials_provider(
            mock_credential,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        );
        provider.start(RetryConfig::default());

        let mut stream = provider.subscribe();
        // Wait for the first token to be received
        let credentials = stream.next().await.unwrap().unwrap();
        assert!(call_count_ref.load(Ordering::SeqCst) >= 1);
        assert_eq!(credentials.username, OID_CLAIM_VALUE);
        assert_eq!(credentials.password, MOCKED_TOKEN_1.as_str());

        // Wait for the next token to be received
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let credentials = stream.next().await.unwrap().unwrap();
        assert!(call_count_ref.load(Ordering::SeqCst) >= 2);
        assert_eq!(credentials.username, OID_CLAIM_VALUE);
        assert_eq!(credentials.password, MOCKED_TOKEN_2.as_str());

        // Wait for the next token to be received
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let credentials = stream.next().await.unwrap().unwrap();
        assert!(call_count_ref.load(Ordering::SeqCst) >= 3);
        assert_eq!(credentials.username, OID_CLAIM_VALUE);
        assert_eq!(credentials.password, MOCKED_TOKEN_3.as_str());
    }

    #[test]
    fn test_mock_scope_validation() {
        use std::panic;

        let result = panic::catch_unwind(|| {
            create_mock_entra_id_credentials_provider(MockTokenCredential::success(), Vec::new());
        });

        assert!(
            result.is_err(),
            "Expected `create_mock_entra_id_credentials_provider` to panic, but it did not."
        );
        assert!(result
            .unwrap_err()
            .downcast_ref::<String>()
            .unwrap()
            .contains("Scopes cannot be empty"));
    }

    #[tokio::test]
    async fn test_mock_provider_cleanup() {
        let mock_credential = MockTokenCredential::success();

        let mut provider = create_mock_entra_id_credentials_provider(
            mock_credential,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        );

        provider.start(RetryConfig::default());
        // Wait for the background task to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        drop(provider);
        // Wait for the background task to stop
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // Test passes if no panic occurs during cleanup
    }
}
