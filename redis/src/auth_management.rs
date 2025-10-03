//! This module contains utilities for managing token-based authentication
use std::time::{Duration, SystemTime};

/// Configuration for token refresh behavior
#[derive(Debug, Clone)]
pub struct TokenRefreshConfig {
    /// Fraction of token lifetime after which refresh should be triggered (0.0 to 1.0)
    /// For example, 0.8 means refresh when 80% of the token's lifetime has elapsed
    pub expiration_refresh_ratio: f64,
    /// Retry configuration for failed refresh attempts
    pub retry_config: RetryConfig,
}

impl TokenRefreshConfig {
    /// Set the expiration refresh ratio
    pub fn set_expiration_refresh_ratio(mut self, ratio: f64) -> Self {
        self.expiration_refresh_ratio = ratio;
        self
    }

    /// Set the retry configuration
    pub fn set_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }
}

impl Default for TokenRefreshConfig {
    fn default() -> Self {
        Self {
            expiration_refresh_ratio: 0.8,
            retry_config: RetryConfig::default(),
        }
    }
}

/// Configuration for retry behavior when token refresh fails
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Maximum random jitter as a percentage of the delay (0.0 to 1.0)
    pub jitter_percentage: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter_percentage: 0.5,
        }
    }
}

/// Common logic for credentials management
pub mod credentials_management_utils {
    use super::*;

    /// Calculate the refresh threshold based on the token's lifetime and the refresh ratio
    pub fn calculate_refresh_threshold(
        received_at: SystemTime,
        expires_at: SystemTime,
        refresh_ratio: f64,
    ) -> Option<Duration> {
        if let Ok(total_lifetime) = expires_at.duration_since(received_at) {
            Some(Duration::from_secs_f64(
                total_lifetime.as_secs_f64() * refresh_ratio,
            ))
        } else {
            None
        }
    }

    /// Calculate next delay with exponential backoff
    pub fn calculate_next_delay(
        current_delay: Duration,
        backoff_multiplier: f64,
        max_delay: Duration,
    ) -> Duration {
        Duration::from_millis((current_delay.as_millis() as f64 * backoff_multiplier) as u64)
            .min(max_delay)
    }

    /// Extract the OID claim from a JWT
    #[cfg(all(feature = "token-based-authentication", feature = "entra-id"))]
    pub fn extract_oid_from_jwt(jwt: &str) -> Result<String, String> {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

        let parts: Vec<&str> = jwt.split('.').collect();
        if parts.len() != 3 {
            return Err("Invalid JWT: must have 3 parts".to_string());
        }

        let payload_bytes = URL_SAFE_NO_PAD
            .decode(parts[1])
            .map_err(|e| format!("Failed to decode payload: {e}"))?;
        let payload_str = String::from_utf8(payload_bytes)
            .map_err(|e| format!("Payload is not valid UTF-8: {e}"))?;

        if let Some(oid_claim_start_idx) = payload_str.find("\"oid\"") {
            if let Some(colon_idx) = payload_str[oid_claim_start_idx..].find(':') {
                let oid_value_str = payload_str[oid_claim_start_idx + colon_idx + 1..].trim_start();

                if let Some(stripped_oid_value) = oid_value_str.strip_prefix('"') {
                    if let Some(end_quote) = stripped_oid_value.find('"') {
                        return Ok(stripped_oid_value[..end_quote].to_string());
                    }
                }
            }
        }

        Err("OID claim not found".to_string())
    }
}
