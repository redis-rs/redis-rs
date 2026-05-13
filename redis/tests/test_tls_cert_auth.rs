//! Tests for Redis 8.6+ TLS certificate-based automatic authentication
//!
//! Redis 8.6 introduces automatic client authentication using TLS certificates.
//! When configured with `tls-auth-clients-user CN`, Redis extracts the Common Name (CN)
//! from the client's TLS certificate and uses it to authenticate the client as an ACL user.
//!
//! These tests verify:
//! - Automatic authentication when CN matches an existing ACL user
//! - Fallback to "default" user when CN doesn't match any ACL user
//! - ACL permissions are correctly enforced based on the authenticated user

#![cfg(feature = "tls-rustls")]

use redis::acl::Rule;
use redis::{Commands, RedisResult};
use tempfile::TempDir;

mod support;
use crate::support::*;
use redis_test::utils::{ClientCertPaths, build_client_cert_with_custom_cn};

/// Helper struct to manage cert-based auth testing with a single Redis server
struct CertAuthTestContext {
    server_ctx: TestContext,
    client_cert_paths: ClientCertPaths,
    server_tls_paths: redis_test::utils::TlsFilePaths,
    _tempdir: TempDir,
}

impl CertAuthTestContext {
    /// Create a connection using the server certificate (for setup operations)
    fn setup_connection(&self) -> redis::Connection {
        self.server_ctx.connection()
    }

    /// Create a connection using the client certificate (for cert auth testing)
    fn cert_auth_connection(&self) -> redis::Connection {
        use support::build_single_client_with_separate_client_cert;

        let client = build_single_client_with_separate_client_cert(
            self.server_ctx.server.connection_info(),
            &self.server_tls_paths,
            &self.client_cert_paths,
        )
        .expect("Failed to create client with client certificate");

        client
            .get_connection()
            .expect("Failed to get connection with client certificate")
    }
}

/// Helper function to create a cert-based auth test context
/// Returns a single server with the ability to create two types of connections:
/// 1. Setup connection using server certificate (for creating ACL users)
/// 2. Cert auth connection using client certificate with custom CN
fn create_cert_auth_context_with_username(username: &str) -> CertAuthTestContext {
    use redis_test::utils::build_keys_and_certs_for_tls;
    use tempfile::TempDir;

    // Create a temporary directory for certificates
    let tempdir = TempDir::new().expect("Failed to create temp dir");

    // Generate CA and server certificates
    let tls_paths = build_keys_and_certs_for_tls(&tempdir);

    // The ca.key path is needed and it should be in the same directory as ca.crt
    let ca_key_path = tempdir.path().join("ca.key");

    // Generate client certificate with custom CN
    let client_cert_paths =
        build_client_cert_with_custom_cn(&tempdir, username, &tls_paths.ca_crt, &ca_key_path);

    // Create a single server context with cert-based auth enabled
    let server_ctx = TestContext::new_with_cert_auth(tls_paths.clone());

    CertAuthTestContext {
        server_ctx,
        client_cert_paths,
        server_tls_paths: tls_paths,
        _tempdir: tempdir,
    }
}

/// Generate a random username for testing
fn generate_random_username() -> String {
    use rand::Rng;
    format!("testcertuser{}", rand::rng().random_range(10000..99999))
}

#[test]
fn test_tls_certificate_authentication_with_matching_acl_user() {
    // This test verifies that Redis 8.6+ can automatically authenticate a client
    // based on the Common Name (CN) field in the client's TLS certificate.

    // Check Redis binary version before creating the test context.
    run_test_if_redis_binary_version_supported!(&REDIS_VERSION_CE_8_6);

    // Generate a random username for the test.
    let test_username = generate_random_username();

    // Create a test context with cert-based authentication enabled.
    let ctx = create_cert_auth_context_with_username(&test_username);

    // First, create the ACL user on the server using the setup connection.
    let mut setup_conn = ctx.setup_connection();

    // Create ACL user with limited permissions:
    //  Allow: GET, SET, PING, ACL WHOAMI
    //  Deny: DEL, FLUSHDB, etc.
    let result: RedisResult<String> = setup_conn.acl_setuser_rules(
        &test_username,
        &[
            Rule::On,
            Rule::NoPass,  // No password required (will use cert auth)
            Rule::AllKeys, // Can access all keys (~*)
            Rule::AddCommand("GET".to_string()),
            Rule::AddCommand("SET".to_string()),
            Rule::AddCommand("PING".to_string()),
            Rule::AddCommand("ACL|WHOAMI".to_string()),
        ],
    );

    assert!(result.is_ok(), "Failed to create ACL user: {:?}", result);

    // Verify that the user was created.
    let users: Vec<String> = setup_conn.acl_users().expect("Failed to get ACL users");
    assert!(
        users.contains(&test_username),
        "User {test_username} not found in ACL users list"
    );

    drop(setup_conn);

    // Connect using the client certificate with CN=test_username.
    // This connection should automatically authenticate as the test user.
    let mut cert_conn = ctx.cert_auth_connection();

    // Verify that the correct user is authenticated.
    let whoami: String = cert_conn
        .acl_whoami()
        .expect("Failed to execute ACL WHOAMI");
    assert_eq!(
        whoami, test_username,
        "Expected to be authenticated as '{test_username}', but got '{whoami}'"
    );

    const TEST_KEY: &str = "test_cert_auth_key";
    const TEST_VALUE: &str = "test_value";

    // Test that the authenticated user can execute only the allowed commands.
    let _: () = cert_conn
        .set(TEST_KEY, TEST_VALUE)
        .expect("SET command should be allowed");

    let value: String = cert_conn
        .get(TEST_KEY)
        .expect("GET command should be allowed");
    assert_eq!(value, TEST_VALUE);

    // Test that the authenticated user CANNOT execute disallowed commands.
    // The user doesn't have +del permission, so this should fail.
    let del_result: RedisResult<()> = cert_conn.del(TEST_KEY);
    assert!(
        del_result.is_err(),
        "DEL command should have failed (user doesn't have +del permission)"
    );
}

#[test]
fn test_tls_certificate_authentication_no_matching_user() {
    // This test verifies that when a client certificate's CN doesn't match
    // any existing ACL user, Redis falls back to the "default" user.

    // Check Redis binary version before creating the test context.
    run_test_if_redis_binary_version_supported!(&REDIS_VERSION_CE_8_6);

    // Generate a random username (that won't have a corresponding ACL user).
    let test_username = generate_random_username();

    // Create a test context with cert-based authentication enabled.
    let ctx = create_cert_auth_context_with_username(&test_username);

    // Verify that the user doesn't exist.
    let mut setup_conn = ctx.setup_connection();
    let users: Vec<String> = setup_conn.acl_users().expect("Failed to get ACL users");
    assert!(
        !users.contains(&test_username),
        "User {test_username} should not exist for this test"
    );

    drop(setup_conn);

    // Connect with a certificate that has CN=test_username.
    // Since the user doesn't exist, the server should fall back to authenticating the user as "default".
    let mut cert_conn = ctx.cert_auth_connection();

    // Verify that the authenticated user is "default" (fallback behavior).
    let whoami: String = cert_conn
        .acl_whoami()
        .expect("Failed to execute ACL WHOAMI");
    assert_eq!(
        whoami, "default",
        "Expected to fall back to 'default' user, but got '{whoami}'"
    );

    const TEST_KEY: &str = "test_cert_auth_fallback";
    const TEST_VALUE: &str = "fallback_value";

    // Verify that the authenticated user can execute commands as the default user.
    let _: () = cert_conn
        .set(TEST_KEY, TEST_VALUE)
        .expect("SET should work as default user");

    let value: String = cert_conn
        .get(TEST_KEY)
        .expect("GET should work as default user");
    assert_eq!(value, TEST_VALUE);
}
