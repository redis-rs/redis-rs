//! Azure Entra ID authentication support for Redis
//!
//! This module provides token-based authentication using Azure Entra ID (formerly Azure Active Directory).
//! It supports multiple credential types including DefaultAzureCredential, service principals,
//! managed identities, as well as custom `TokenCredential` implementations.
//!
//! # Features
//!
//! - **Multiple Authentication Flows**: Service principals, managed identities, and custom `TokenCredential` implementations
//! - **Automatic Token Refresh**: Background token refresh with configurable policies
//! - **Retry Logic**: Robust error handling with exponential backoff
//! - **Async Support**: Full async/await support for non-blocking operations
//!
//! # Example
//!
//! ```rust,no_run
//! use redis::{Client, EntraIdCredentialsProvider, TokenRefreshConfig};
//!
//! # async fn example() -> redis::RedisResult<()> {
//! // Create credentials provider using DefaultAzureCredential
//! let mut provider = EntraIdCredentialsProvider::new_default()?;
//! provider.start(TokenRefreshConfig::default());
//!
//! // Create Redis client with credentials provider
//! let client = Client::open("redis://your-redis-instance.com:6380")?
//!     .with_credentials_provider(provider);
//!
//! // Use the client to get a multiplexed connection
//! let mut con = client.get_multiplexed_async_connection().await?;
//! redis::cmd("SET")
//!     .arg("my_key")
//!     .arg(42i32)
//!     .exec_async(&mut con)
//!     .await?;
//! let result: Option<String> = redis::cmd("GET")
//!     .arg("my_key")
//!     .query_async(&mut con)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::auth::BasicAuth;
use crate::auth::StreamingCredentialsProvider;
use crate::auth_management::credentials_management_utils;
use crate::errors::{ErrorKind, RedisError};
use crate::types::RedisResult;
use crate::{RetryConfig, TokenRefreshConfig};
use azure_core::credentials::{AccessToken, TokenCredential};
use azure_identity::{
    ClientCertificateCredential, ClientSecretCredential, DefaultAzureCredential,
    ManagedIdentityCredential, TokenCredentialOptions, UserAssignedId,
};
use futures_util::{Stream, StreamExt};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;

/// The default Redis scope for Azure Managed Redis
pub const REDIS_SCOPE_DEFAULT: &str = "https://redis.azure.com/.default";

/// Configuration for client certificate authentication
#[derive(Debug, Clone)]
pub struct ClientCertificateConfig {
    /// The client certificate in PEM format
    pub certificate_pem: String,
    /// The private key in PEM format
    pub private_key_pem: String,
}

type Subscriptions = Vec<Arc<Sender<RedisResult<BasicAuth>>>>;
type SharedSubscriptions = Arc<Mutex<Subscriptions>>;

/// Type that serves as a placeholder for various authentication flows
pub struct DefaultFlow;
/// Type representing the client secret authentication flow
pub struct ClientSecretFlow;

/// Entra ID credentials provider that uses Azure Identity for authentication
pub struct EntraIdCredentialsProvider<AuthenticationFlow> {
    credential_provider: Arc<dyn TokenCredential + Send + Sync>,
    scopes: Vec<String>,
    background_handle: Option<tokio::task::JoinHandle<()>>,
    subscribers: SharedSubscriptions,
    current_credentials: Arc<RwLock<Option<BasicAuth>>>,
    _mode: PhantomData<AuthenticationFlow>,
}

/// General methods for all authentication flows
impl<AuthenticationFlow> EntraIdCredentialsProvider<AuthenticationFlow> {
    /// Validate that scopes are not empty
    fn validate_scopes(scopes: &[String]) -> RedisResult<()> {
        if scopes.is_empty() {
            return Err(RedisError::from((
                ErrorKind::InvalidClientConfig,
                "Scopes cannot be empty for Entra ID authentication",
            )));
        }

        for scope in scopes {
            if scope.trim().is_empty() {
                return Err(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "Scope cannot be empty or whitespace-only",
                )));
            }

            // Basic URL validation - should start with https:// and end with /.default
            // Note: This should be verified because there could possibly be scopes without these properties.
            // For example custom scopes or OIDC like scopes... Commenting it out for now

            // if !scope.starts_with("https://") {
            //     return Err(RedisError::from((
            //         ErrorKind::InvalidClientConfig,
            //         "Invalid scope: must start with 'https://'",
            //         format!("Scope: '{scope}'"),
            //     )));
            // }

            // if !scope.ends_with("/.default") {
            //     return Err(RedisError::from((
            //         ErrorKind::InvalidClientConfig,
            //         "Invalid scope: must end with '/.default'",
            //         format!("Scope: '{scope}'"),
            //     )));
            // }
        }

        Ok(())
    }

    /// Convert Azure AccessToken to Redis BasicAuth
    fn convert_credentials(username: String, access_token: &AccessToken) -> BasicAuth {
        BasicAuth {
            username,
            password: access_token.token.secret().to_string(),
        }
    }

    /// Convert Azure Core error to Redis error
    fn convert_error(err: azure_core::Error) -> RedisError {
        RedisError::from((
            ErrorKind::AuthenticationFailed,
            "Entra ID authentication failed",
            format!("{err}"),
        ))
    }

    /// Convert Azure Core error to Redis error
    fn convert_error_ref(err: &azure_core::Error) -> RedisError {
        RedisError::from((
            ErrorKind::AuthenticationFailed,
            "Entra ID authentication failed",
            format!("{err}"),
        ))
    }

    async fn notify_subscribers(
        subscribers_arc: &SharedSubscriptions,
        username: &str,
        token_response: Result<AccessToken, azure_core::Error>,
    ) {
        let subscribers = subscribers_arc
            .lock()
            .expect("could not acquire lock for subscribers")
            .clone();

        futures_util::future::join_all(subscribers.iter().map(|sender| {
            let token_response = token_response.as_ref();
            let response = match token_response {
                Ok(access_token) => {
                    Ok(Self::convert_credentials(username.to_owned(), access_token))
                }
                Err(error) => Err(Self::convert_error_ref(error)),
            };

            sender.send(response)
        }))
        .await;

        subscribers_arc
            .lock()
            .expect("could not acquire lock for subscribers")
            .retain(|sender| !sender.is_closed());
    }

    /// Start the background refresh service
    fn start_refresh_service<F>(
        &mut self,
        retry_config: RetryConfig,
        compute_sleep_duration_on_success: F,
    ) where
        F: Fn(&AccessToken) -> std::time::Duration + Send + Sync + 'static,
    {
        // Prevent multiple calls to start
        if self.background_handle.is_some() {
            return;
        }

        let subscribers_arc = Arc::clone(&self.subscribers);
        let current_credentials_arc = Arc::clone(&self.current_credentials);

        let credential_provider_arc = Arc::clone(&self.credential_provider);
        let scopes = self.scopes.clone();

        self.background_handle = Some(tokio::spawn(async move {
            let scopes: Vec<&str> = scopes.iter().map(|s| s.as_str()).collect();
            let mut next_sleep_duration;
            let mut username = "default".to_string();
            let mut attempt = 0;
            let mut error_delay = retry_config.initial_delay;

            loop {
                println!("Refreshing token. Attempt {attempt}");
                let token_response = credential_provider_arc.get_token(&scopes, None).await;

                if let Ok(ref access_token) = token_response {
                    attempt = 0;

                    username = match credentials_management_utils::extract_oid_from_jwt(
                        access_token.token.secret(),
                    ) {
                        Ok(object_id) => object_id,
                        Err(error) => {
                            eprintln!("Failed to extract OID: {error}");
                            "default".to_string()
                        }
                    };

                    *current_credentials_arc.write().expect("rwlock poisoned") =
                        Some(Self::convert_credentials(username.clone(), access_token));

                    next_sleep_duration = compute_sleep_duration_on_success(access_token);
                } else {
                    attempt += 1;
                    if attempt < retry_config.max_attempts {
                        error_delay = credentials_management_utils::calculate_next_delay(
                            error_delay,
                            retry_config.backoff_multiplier,
                            retry_config.max_delay,
                        );
                        println!("An error occurred while refreshing the token. Attempt {attempt}. Sleeping for {:?}", error_delay);
                        tokio::time::sleep(error_delay).await;
                        continue;
                    }
                    println!("Max attempts reached. Stopping token refresh.");
                    Self::notify_subscribers(&subscribers_arc, &username, token_response).await;
                    break;
                }

                Self::notify_subscribers(&subscribers_arc, &username, token_response).await;

                tokio::time::sleep(std::time::Duration::from_millis(
                    next_sleep_duration.as_millis() as u64,
                ))
                .await;
            }
        }));
    }

    /// Stop the background refresh service
    fn stop(&mut self) {
        if let Some(handle) = self.background_handle.take() {
            handle.abort();
        }
    }
}

impl EntraIdCredentialsProvider<DefaultFlow> {
    /// Create a new provider using DefaultAzureCredential
    /// This is recommended for development and will try multiple credential types
    pub fn new_default() -> RedisResult<Self> {
        Self::new_default_with_scopes(vec![REDIS_SCOPE_DEFAULT.to_string()])
    }

    /// Create a new provider using DefaultAzureCredential with custom scopes
    fn new_default_with_scopes(scopes: Vec<String>) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        let credential_provider = DefaultAzureCredential::new().map_err(Self::convert_error)?;
        Ok(Self {
            credential_provider: Arc::new(
                std::sync::Arc::try_unwrap(credential_provider).map_err(|_| {
                    RedisError::from((
                        ErrorKind::AuthenticationFailed,
                        "Failed to unwrap credential",
                    ))
                })?,
            ),
            scopes,
            background_handle: None,
            subscribers: Default::default(),
            current_credentials: Default::default(),
            _mode: PhantomData,
        })
    }

    /// Create a new provider using client certificate authentication (service principal)
    pub fn new_client_certificate(
        tenant_id: String,
        client_id: String,
        certificate_config: ClientCertificateConfig,
    ) -> RedisResult<Self> {
        Self::new_client_certificate_with_scopes(
            tenant_id,
            client_id,
            certificate_config,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        )
    }

    /// Create a new provider using client certificate authentication with custom scopes
    fn new_client_certificate_with_scopes(
        tenant_id: String,
        client_id: String,
        certificate_config: ClientCertificateConfig,
        scopes: Vec<String>,
    ) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        let credential_provider = ClientCertificateCredential::new(
            tenant_id,
            client_id,
            certificate_config.certificate_pem,
            certificate_config.private_key_pem,
            azure_identity::ClientCertificateCredentialOptions::new(
                TokenCredentialOptions::default(),
                false,
            ),
        )
        .map_err(Self::convert_error)?;
        Ok(Self {
            credential_provider: Arc::new(
                std::sync::Arc::try_unwrap(credential_provider).map_err(|_| {
                    RedisError::from((
                        ErrorKind::AuthenticationFailed,
                        "Failed to unwrap credential",
                    ))
                })?,
            ),
            scopes,
            background_handle: None,
            subscribers: Default::default(),
            current_credentials: Default::default(),
            _mode: PhantomData,
        })
    }

    /// Create a new provider using system-assigned managed identity
    pub fn new_system_assigned_managed_identity() -> RedisResult<Self> {
        Self::new_system_assigned_managed_identity_with_scopes(
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        )
    }

    /// Create a new provider using system-assigned managed identity with custom scopes
    fn new_system_assigned_managed_identity_with_scopes(scopes: Vec<String>) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        let credential_provider =
            ManagedIdentityCredential::new(None).map_err(Self::convert_error)?;
        Ok(Self {
            credential_provider: Arc::new(
                std::sync::Arc::try_unwrap(credential_provider).map_err(|_| {
                    RedisError::from((
                        ErrorKind::AuthenticationFailed,
                        "Failed to unwrap credential",
                    ))
                })?,
            ),
            scopes,
            background_handle: None,
            subscribers: Default::default(),
            current_credentials: Default::default(),
            _mode: PhantomData,
        })
    }

    /// Create a new provider using user-assigned managed identity
    pub fn new_user_assigned_managed_identity(client_id: String) -> RedisResult<Self> {
        Self::new_user_assigned_managed_identity_with_scopes(
            client_id,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        )
    }

    /// Create a new provider using user-assigned managed identity with custom scopes
    fn new_user_assigned_managed_identity_with_scopes(
        client_id: String,
        scopes: Vec<String>,
    ) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        let options = azure_identity::ManagedIdentityCredentialOptions {
            user_assigned_id: Some(UserAssignedId::ClientId(client_id)),
            ..Default::default()
        };
        let credential_provider =
            ManagedIdentityCredential::new(Some(options)).map_err(Self::convert_error)?;
        Ok(Self {
            credential_provider: Arc::new(
                std::sync::Arc::try_unwrap(credential_provider).map_err(|_| {
                    RedisError::from((
                        ErrorKind::AuthenticationFailed,
                        "Failed to unwrap credential",
                    ))
                })?,
            ),
            scopes,
            background_handle: None,
            subscribers: Default::default(),
            current_credentials: Default::default(),
            _mode: PhantomData,
        })
    }

    /// Create a new provider with a custom credential implementation
    pub fn new_with_credential(
        credential_provider: Arc<dyn TokenCredential + Send + Sync>,
        scopes: Vec<String>,
    ) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        Ok(Self {
            credential_provider,
            scopes,
            background_handle: None,
            subscribers: Default::default(),
            current_credentials: Default::default(),
            _mode: PhantomData,
        })
    }

    /// Start the background refresh service
    pub fn start(&mut self, token_refresh_config: TokenRefreshConfig) {
        self.start_refresh_service(token_refresh_config.retry_config, move |access_token| {
            credentials_management_utils::calculate_refresh_threshold(
                SystemTime::now(),
                access_token.expires_on.into(),
                token_refresh_config.expiration_refresh_ratio,
            )
            .unwrap_or(std::time::Duration::ZERO)
        });
    }
}

impl EntraIdCredentialsProvider<ClientSecretFlow> {
    /// Create a new provider using client secret authentication (service principal)
    pub fn new_client_secret(
        tenant_id: String,
        client_id: String,
        client_secret: String,
    ) -> RedisResult<Self> {
        Self::new_client_secret_with_scopes(
            tenant_id,
            client_id,
            client_secret,
            vec![REDIS_SCOPE_DEFAULT.to_string()],
        )
    }

    /// Create a new provider using client secret authentication with custom scopes
    fn new_client_secret_with_scopes(
        tenant_id: String,
        client_id: String,
        client_secret: String,
        scopes: Vec<String>,
    ) -> RedisResult<Self> {
        Self::validate_scopes(&scopes)?;
        let credential_provider =
            ClientSecretCredential::new(&tenant_id, client_id, client_secret.into(), None)
                .map_err(Self::convert_error)?;
        Ok(Self {
            credential_provider: Arc::new(
                std::sync::Arc::try_unwrap(credential_provider).map_err(|_| {
                    RedisError::from((
                        ErrorKind::AuthenticationFailed,
                        "Failed to unwrap credential",
                    ))
                })?,
            ),
            scopes,
            background_handle: None,
            subscribers: Default::default(),
            current_credentials: Default::default(),
            _mode: PhantomData,
        })
    }

    /// Start the background refresh service
    pub fn start(&mut self, retry_config: RetryConfig) {
        self.start_refresh_service(retry_config, |access_token| {
            let remaining = access_token.expires_on - OffsetDateTime::now_utc();
            let remaining_duration = match remaining.try_into() {
                Ok(duration) => duration,
                Err(_) => std::time::Duration::from_secs(0),
            };
            remaining_duration
                .checked_sub(std::time::Duration::from_secs(240))
                .unwrap_or_else(|| {
                    eprintln!("Token expires soon; refreshing immediately");
                    std::time::Duration::from_secs(0)
                })
        });
    }
}

impl<AuthenticationFlow> StreamingCredentialsProvider
    for EntraIdCredentialsProvider<AuthenticationFlow>
where
    AuthenticationFlow: Send + Sync + 'static,
{
    fn subscribe(&self) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<RedisResult<BasicAuth>>(1);

        self.subscribers
            .lock()
            .expect("could not acquire guard for subscribers")
            .push(Arc::new(tx));

        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });

        if let Some(creds) = self
            .current_credentials
            .read()
            .expect("rwlock poisoned")
            .clone()
        {
            futures_util::stream::once(async move { Ok(creds) })
                .chain(stream)
                .boxed()
        } else {
            stream.boxed()
        }
    }
}

impl<AuthenticationFlow> std::fmt::Debug for EntraIdCredentialsProvider<AuthenticationFlow> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntraIdCredentialsProvider")
            .field("scopes", &self.scopes)
            .field("credential", &"<TokenCredential>")
            .finish()
    }
}

impl<AuthenticationFlow> Drop for EntraIdCredentialsProvider<AuthenticationFlow> {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(all(feature = "entra-id", test))]
mod tests {
    use super::*;

    #[test]
    fn test_entra_id_provider_creation() {
        // Test that credential providers can be created without panicking
        let _default_provider = EntraIdCredentialsProvider::new_default();

        let _client_secret_provider = EntraIdCredentialsProvider::new_client_secret(
            "tenant".to_string(),
            "client".to_string(),
            "secret".to_string(),
        );

        let _managed_identity_provider =
            EntraIdCredentialsProvider::new_system_assigned_managed_identity();
    }

    #[test]
    fn test_scope_validation() {
        // Test empty scopes
        let result = EntraIdCredentialsProvider::new_default_with_scopes(vec![]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Scopes cannot be empty"));

        // Test empty string scope
        let result = EntraIdCredentialsProvider::new_default_with_scopes(vec!["".to_string()]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Scope cannot be empty"));

        // Test whitespace-only scope
        let result = EntraIdCredentialsProvider::new_default_with_scopes(vec!["   ".to_string()]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Scope cannot be empty"));

        /*
        // Test invalid protocol
        let result = EntraIdCredentialsProvider::new_default_with_scopes(vec!["http://invalid.scope/.default".to_string()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must start with 'https://'"));

        // Test invalid suffix
        let result = EntraIdCredentialsProvider::new_default_with_scopes(vec!["https://valid.scope/invalid".to_string()]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must end with '/.default'"));
        */
    }

    #[test]
    fn test_custom_scopes() {
        let custom_scopes = vec!["https://custom.scope/.default".to_string()];
        let provider =
            EntraIdCredentialsProvider::new_default_with_scopes(custom_scopes.clone()).unwrap();
        assert_eq!(provider.scopes, custom_scopes);
    }
}
