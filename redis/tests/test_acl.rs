#![cfg(feature = "acl")]

use redis::TypedCommands;
use redis::acl::{AclInfo, Rule};
use std::collections::HashSet;

mod support;
use crate::support::*;

#[test]
fn test_acl_whoami() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    assert_eq!(con.acl_whoami(), Ok("default".to_owned()));
}

#[test]
fn test_acl_help() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let res = con.acl_help().expect("Got help manual");
    assert!(!res.is_empty());
}

//TODO: do we need this test?
#[test]
#[ignore]
fn test_acl_getsetdel_users() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    assert_eq!(
        con.acl_list(),
        Ok(vec!["user default on nopass ~* +@all".to_owned()])
    );
    assert_eq!(con.acl_users(), Ok(vec!["default".to_owned()]));
    // bob
    assert_eq!(con.acl_setuser("bob"), Ok(()));
    assert_eq!(
        con.acl_users(),
        Ok(vec!["bob".to_owned(), "default".to_owned()])
    );

    // ACL SETUSER bob on ~redis:* +set
    assert_eq!(
        con.acl_setuser_rules(
            "bob",
            &[
                Rule::On,
                Rule::AddHashedPass(
                    "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2".to_owned()
                ),
                Rule::Pattern("redis:*".to_owned()),
                Rule::AddCommand("set".to_owned())
            ],
        ),
        Ok(())
    );
    let acl_info = con.acl_getuser("bob").expect("Got user").unwrap();
    assert_eq!(
        acl_info,
        AclInfo {
            flags: vec![Rule::On],
            passwords: vec![Rule::AddHashedPass(
                "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2".to_owned()
            )],
            commands: vec![
                Rule::RemoveCategory("all".to_owned()),
                Rule::AddCommand("set".to_owned())
            ],
            keys: vec![Rule::Pattern("redis:*".to_owned())],
            channels: vec![],
            selectors: vec![],
        }
    );
    assert_eq!(
        con.acl_list(),
        Ok(vec![
            "user bob on #c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2 ~redis:* -@all +set".to_owned(),
            "user default on nopass ~* +@all".to_owned(),
        ])
    );

    // ACL SETUSER eve
    assert_eq!(con.acl_setuser("eve"), Ok(()));
    assert_eq!(
        con.acl_users(),
        Ok(vec![
            "bob".to_owned(),
            "default".to_owned(),
            "eve".to_owned()
        ])
    );
    assert_eq!(con.acl_deluser(&["bob", "eve"]), Ok(2));
    assert_eq!(con.acl_users(), Ok(vec!["default".to_owned()]));
}

#[test]
fn test_acl_cat() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let res: HashSet<String> = con.acl_cat().expect("Got categories");
    let expects = vec![
        "keyspace",
        "read",
        "write",
        "set",
        "sortedset",
        "list",
        "hash",
        "string",
        "bitmap",
        "hyperloglog",
        "geo",
        "stream",
        "pubsub",
        "admin",
        "fast",
        "slow",
        "blocking",
        "dangerous",
        "connection",
        "transaction",
        "scripting",
    ];
    for cat in expects.iter() {
        assert!(res.contains(*cat), "Category `{cat}` does not exist");
    }

    let expects = ["pfmerge", "pfcount", "pfselftest", "pfadd"];
    let res = con
        .acl_cat_categoryname("hyperloglog")
        .expect("Got commands of a category");
    for cmd in expects.iter() {
        assert!(res.contains(*cmd), "Command `{cmd}` does not exist");
    }
}

#[test]
fn test_acl_genpass() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let pass: String = con.acl_genpass().expect("Got password");
    assert_eq!(pass.len(), 64);

    let pass: String = con.acl_genpass_bits(1024).expect("Got password");
    assert_eq!(pass.len(), 256);
}

#[test]
fn test_acl_log() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let logs: Vec<String> = con.acl_log(1).expect("Got logs");
    assert_eq!(logs.len(), 0);
    assert_eq!(con.acl_log_reset(), Ok(()));
}

#[test]
fn test_acl_dryrun() {
    let ctx = TestContext::new();
    run_test_if_version_supported!(&(7, 0, 0));

    let mut con = ctx.connection();

    redis::cmd("ACL")
        .arg("SETUSER")
        .arg("VIRGINIA")
        .arg("+SET")
        .arg("~*")
        .exec(&mut con)
        .unwrap();

    assert_eq!(
        con.acl_dryrun(b"VIRGINIA", String::from("SET"), &["foo", "bar"])
            .unwrap(),
        "OK"
    );

    let res: String = con
        .acl_dryrun(b"VIRGINIA", String::from("GET"), "foo")
        .unwrap();
    assert_eq!(
        res,
        "User VIRGINIA has no permissions to run the 'get' command"
    );
}
#[test]
fn test_acl_info() {
    let ctx = TestContext::new();
    run_test_if_version_supported!(&(7, 0, 0));
    let mut conn = ctx.connection();
    let username = "tenant";
    let password = "securepassword123";
    const DEFAULT_QUEUE_NAME: &str = "default";
    let rules = vec![
        // Basic permissions: on, +@all, -@dangerous, +keys, -info
        Rule::On,
        Rule::ResetChannels,
        Rule::AllCommands,
        Rule::RemoveCategory("dangerous".to_string()),
        Rule::AddCommand("keys".to_string()),
        Rule::RemoveCommand("info".to_string()),
        // Database restrictions: -select
        Rule::RemoveCommand("select".to_string()),
        // Password
        Rule::AddPass(password.to_string()),
        // Add default queue pattern - uses hashtag {DEFAULT_QUEUE_NAME} for Redis cluster routing
        Rule::Pattern(format!("asynq:{{{}}}:*", DEFAULT_QUEUE_NAME)),
        // Add tenant-specific key patterns
        Rule::Pattern(format!("asynq:{{{}:*", username)),
        // Add default key patterns
        Rule::Pattern("asynq:queues".to_string()),
        Rule::Pattern("asynq:servers:*".to_string()),
        Rule::Pattern("asynq:servers".to_string()),
        Rule::Pattern("asynq:workers".to_string()),
        Rule::Pattern("asynq:workers:*".to_string()),
        Rule::Pattern("asynq:schedulers".to_string()),
        Rule::Pattern("asynq:schedulers:*".to_string()),
        Rule::Channel("asynq:cancel".to_string()),
    ];
    assert_eq!(conn.acl_setuser_rules(username, &rules), Ok(()));
    let info = conn.acl_getuser(username).expect("Got user");
    assert!(info.is_some());
    let info = info.expect("Got asynq");
    assert_eq!(
        info.flags,
        vec![Rule::On, Rule::Other("sanitize-payload".to_string())]
    );
    assert_eq!(
        info.passwords,
        vec![Rule::AddHashedPass(
            "dda69783f28fdf6f1c5a83e8400f2472e9300887d1dffffe12a07b92a3d0aa25".to_string()
        )]
    );
    assert_eq!(
        info.commands,
        vec![
            Rule::AddCategory("all".to_string()),
            Rule::RemoveCategory("dangerous".to_string()),
            Rule::AddCommand("keys".to_string()),
            Rule::RemoveCommand("info".to_string()),
            Rule::RemoveCommand("select".to_string()),
        ]
    );
    assert_eq!(
        info.keys,
        vec![
            Rule::Pattern("asynq:{default}:*".to_string()),
            Rule::Pattern("asynq:{tenant:*".to_string()),
            Rule::Pattern("asynq:queues".to_string()),
            Rule::Pattern("asynq:servers:*".to_string()),
            Rule::Pattern("asynq:servers".to_string()),
            Rule::Pattern("asynq:workers".to_string()),
            Rule::Pattern("asynq:workers:*".to_string()),
            Rule::Pattern("asynq:schedulers".to_string()),
            Rule::Pattern("asynq:schedulers:*".to_string()),
        ]
    );
    assert_eq!(
        info.channels,
        vec![Rule::Channel("asynq:cancel".to_string())]
    );
    assert_eq!(info.selectors, vec![]);
}
#[test]
fn test_acl_sample_info() {
    let ctx = TestContext::new();
    run_test_if_version_supported!(&(7, 0, 0));
    let mut conn = ctx.connection();
    let sample_rule = vec![
        Rule::On,
        Rule::NoPass,
        Rule::AddCommand("GET".to_string()),
        Rule::AllKeys,
        Rule::Channel("*".to_string()),
        Rule::Selector(vec![
            Rule::AddCommand("SET".to_string()),
            Rule::Pattern("key2".to_string()),
        ]),
    ];
    conn.acl_setuser_rules("sample", &sample_rule)
        .expect("Set sample user");
    let sample_user = conn.acl_getuser("sample").expect("Got user");
    let sample_user = sample_user.expect("Got sample user");
    assert_eq!(
        sample_user.flags,
        vec![
            Rule::On,
            Rule::NoPass,
            Rule::Other("sanitize-payload".to_string())
        ]
    );
    assert_eq!(sample_user.passwords, vec![]);
    assert_eq!(
        sample_user.commands,
        vec![
            Rule::RemoveCategory("all".to_string()),
            Rule::AddCommand("get".to_string()),
        ]
    );
    assert_eq!(sample_user.keys, vec![Rule::AllKeys]);
    assert_eq!(sample_user.channels, vec![Rule::Channel("*".to_string())]);
    assert_eq!(
        sample_user.selectors,
        vec![
            Rule::RemoveCategory("all".to_string()),
            Rule::AddCommand("set".to_string()),
            Rule::Pattern("key2".to_string()),
        ]
    );
}

#[cfg(all(feature = "acl", feature = "token-based-authentication"))]
mod token_based_authentication_acl_tests {
    use crate::support::*;
    use futures_util::{Stream, StreamExt};
    use redis::aio::ConnectionLike;
    use redis::auth::BasicAuth;
    use redis::auth::StreamingCredentialsProvider;
    use redis::RedisResult;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;
    use tokio::sync::mpsc::Sender;

    const TOKEN_PAYLOAD: &str = "eyJvaWQiOiIxMjM0NTY3OC05YWJjLWRlZi0xMjM0LTU2Nzg5YWJjZGVmMCJ9"; // Payload with "oid" claim
    const OID_CLAIM_VALUE: &str = "12345678-9abc-def-1234-56789abcdef0";
    const TOKEN_SIGNATURE: &str = "signature";

    static MOCKED_TOKEN: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        format!("mock_jwt_token.{}.{}", TOKEN_PAYLOAD, TOKEN_SIGNATURE)
    });

    const DEFAULT_USER: &str = "default";
    const TEST_USER: &str = "test";

    const ALICE_OID_CLAIM: &str = "a11ce000-7a1c-4a1c-9e11-ace000000001";
    const ALICE_TOKEN: &str ="alice_mock_jwt_token.eyJvaWQiOiJhMTFjZTAwMC03YTFjLTRhMWMtOWUxMS1hY2UwMDAwMDAwMDEifQ.signature";
    const BOB_OID_CLAIM: &str = "b0b00000-0b01-4b0b-9b0b-0b0000000002";
    const BOB_TOKEN: &str = "bob_mock_jwt_token.eyJvaWQiOiJiMGIwMDAwMC0wYjAxLTRiMGItOWIwYi0wYjAwMDAwMDAwMDIifQ.signature";
    const CHARLIE_OID_CLAIM: &str = "c0a11e00-7c1a-4a1e-9c11-0ca11e000003";
    const CHARLIE_TOKEN: &str = "charlie_mock_jwt_token.eyJvaWQiOiJjMGExMWUwMC03YzFhLTRhMWUtOWMxMS0wY2ExMWUwMDAwMDAzIn0.signature";

    const CREDENTIALS: [(&str, &str); 3] = [
        (ALICE_OID_CLAIM, ALICE_TOKEN),
        (BOB_OID_CLAIM, BOB_TOKEN),
        (CHARLIE_OID_CLAIM, CHARLIE_TOKEN),
    ];
    /// Configuration for the mock streaming credentials provider
    ///
    /// This struct allows customization of the mock provider's behavior for testing
    /// different scenarios like token rotation, authentication errors, and timing.
    #[derive(Debug, Clone)]
    pub struct MockProviderConfig {
        /// Sequence of credentials to provide
        pub credentials_sequence: Vec<BasicAuth>,
        /// Interval between token refreshes
        pub refresh_interval: Duration,
        /// Whether to simulate errors (and at which positions in the sequence)
        pub error_positions: Vec<usize>,
    }

    impl Default for MockProviderConfig {
        /// Create a default config with a single token
        fn default() -> Self {
            Self {
                credentials_sequence: vec![BasicAuth {
                    username: OID_CLAIM_VALUE.to_string(),
                    password: MOCKED_TOKEN.clone(),
                }],
                refresh_interval: Duration::from_millis(100),
                error_positions: vec![],
            }
        }
    }

    impl MockProviderConfig {
        /// Create config for multiple token rotations
        pub fn multiple_tokens() -> Self {
            let mut credentials_sequence = Vec::new();

            for (username, token_payload) in CREDENTIALS.iter() {
                credentials_sequence.push(BasicAuth {
                    username: username.to_string(),
                    password: token_payload.to_string(),
                });
            }

            Self {
                credentials_sequence,
                refresh_interval: Duration::from_millis(500),
                error_positions: vec![],
            }
        }

        /// Create config with multiple tokens and error simulation
        pub fn multiple_tokens_with_errors(error_positions: Vec<usize>) -> Self {
            let mut config = Self::multiple_tokens();
            config.error_positions = error_positions;
            config
        }
    }

    type Subscriptions = Vec<Arc<Sender<RedisResult<BasicAuth>>>>;
    type SharedSubscriptions = Arc<Mutex<Subscriptions>>;
    /// Mock streaming credentials provider that simulates token-based authentication.
    ///
    /// This provider is designed to test the token-based authentication flow in Redis
    /// connections. It supports:
    ///
    /// - **Token rotation**: Cycling through multiple tokens over time
    /// - **Error simulation**: Injecting authentication failures at specific points
    /// - **Configurable timing**: Custom refresh intervals for testing
    ///
    /// # Example Usage
    ///
    /// ```rust
    /// // Basic usage with default token
    /// let mut provider = MockStreamingCredentialsProvider::new();
    /// provider.start();
    ///
    /// // Token rotation testing
    /// let mut provider = MockStreamingCredentialsProvider::multiple_tokens();
    /// provider.start();
    ///
    /// // Error simulation
    /// let mut provider = MockStreamingCredentialsProvider::multiple_tokens_with_errors(vec![1, 3]);
    /// provider.start();
    /// ```
    pub struct MockStreamingCredentialsProvider {
        config: MockProviderConfig,
        background_handle: Option<tokio::task::JoinHandle<()>>,
        subscribers: SharedSubscriptions,
        current_credentials: Arc<RwLock<Option<BasicAuth>>>,
        current_position: Arc<Mutex<usize>>,
    }

    impl MockStreamingCredentialsProvider {
        /// Create a new mock provider with default configuration
        pub fn new() -> Self {
            Self::with_config(MockProviderConfig::default())
        }

        /// Create a new mock provider with custom configuration
        pub fn with_config(config: MockProviderConfig) -> Self {
            Self {
                config,
                background_handle: None,
                subscribers: Arc::new(Mutex::new(Vec::new())),
                current_credentials: Arc::new(RwLock::new(None)),
                current_position: Arc::new(Mutex::new(0)),
            }
        }

        /// Create a provider that supports multiple token rotations
        pub fn multiple_tokens() -> Self {
            Self::with_config(MockProviderConfig::multiple_tokens())
        }

        /// Create a provider with multiple tokens and error simulation
        pub fn multiple_tokens_with_errors(error_positions: Vec<usize>) -> Self {
            Self::with_config(MockProviderConfig::multiple_tokens_with_errors(
                error_positions,
            ))
        }

        /// Start the background token refresh process
        pub fn start(&mut self) {
            // Prevent multiple calls to start
            if self.background_handle.is_some() {
                return;
            }

            let config = self.config.clone();
            let subscribers_arc = Arc::clone(&self.subscribers);
            let current_credentials_arc = Arc::clone(&self.current_credentials);
            let current_position_arc = Arc::clone(&self.current_position);

            self.background_handle = Some(tokio::spawn(async move {
                let mut attempt = 0;

                loop {
                    let position = {
                        let mut pos = current_position_arc
                            .lock()
                            .expect("could not acquire lock for current_position");
                        let current_pos = *pos;
                        *pos = (*pos + 1) % config.credentials_sequence.len();
                        current_pos
                    };

                    println!("Mock provider: Refreshing credentials. Attempt {attempt}");

                    let result = if config.error_positions.contains(&position) {
                        Err(redis::RedisError::from((
                            redis::ErrorKind::AuthenticationFailed,
                            "Mock authentication failed",
                        )))
                    } else {
                        // Use the credential at the current position
                        let credentials = config.credentials_sequence[position].clone();
                        {
                            let mut current =
                                current_credentials_arc.write().expect("rwlock poisoned");
                            *current = Some(credentials.clone());
                        }

                        println!("Mock provider: Providing credentials: {:?}", credentials);
                        Ok(credentials)
                    };

                    Self::notify_subscribers(&subscribers_arc, result.clone()).await;

                    attempt += 1;
                    tokio::time::sleep(config.refresh_interval).await;
                }
            }));
        }

        /// Stop the background refresh process
        pub fn stop(&mut self) {
            if let Some(handle) = self.background_handle.take() {
                handle.abort();
            }
        }

        /// Notify all subscribers of new credentials
        async fn notify_subscribers(
            subscribers_arc: &SharedSubscriptions,
            result: RedisResult<BasicAuth>,
        ) {
            let subscribers_list = subscribers_arc
                .lock()
                .expect("could not acquire lock for subscribers")
                .clone();

            futures_util::future::join_all(
                subscribers_list
                    .iter()
                    .map(|sender| sender.send(result.clone())),
            )
            .await;

            // Clean up closed channels
            subscribers_arc
                .lock()
                .expect("could not acquire lock for subscribers")
                .retain(|sender| !sender.is_closed());
        }
    }

    impl StreamingCredentialsProvider for MockStreamingCredentialsProvider {
        fn subscribe(
            &self,
        ) -> Pin<Box<dyn Stream<Item = RedisResult<BasicAuth>> + Send + 'static>> {
            let (tx, rx) = tokio::sync::mpsc::channel::<RedisResult<BasicAuth>>(1);

            self.subscribers
                .lock()
                .expect("could not acquire guard for subscribers")
                .push(Arc::new(tx));

            let stream = futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            });

            if let Some(credentials) = self
                .current_credentials
                .read()
                .expect("rwlock poisoned")
                .clone()
            {
                futures_util::stream::once(async move { Ok(credentials) })
                    .chain(stream)
                    .boxed()
            } else {
                stream.boxed()
            }
        }
    }

    impl Drop for MockStreamingCredentialsProvider {
        fn drop(&mut self) {
            self.stop();
        }
    }

    #[tokio::test]
    async fn test_authentication_with_mock_streaming_credentials_provider() {
        let mut ctx = TestContext::new();
        // Set up a Redis user that expects a JWT token as password
        let mut admin_con = ctx.async_connection().await.unwrap();
        let expected_username = OID_CLAIM_VALUE;
        let users_cmd = redis::cmd("ACL").arg("USERS").clone();

        // Create a user with the JWT token as password and full permissions
        println!("Setting up Redis user with JWT token authentication...");
        let result = admin_con.req_packed_command(redis::cmd("ACL")
            .arg("SETUSER")
            .arg(expected_username)
            .arg("on")  // Enable the user
            .arg(format!(">{}", MOCKED_TOKEN.as_str())) // Set JWT token as plain text password
            .arg("~*")  // Allow access to all keys
            .arg("+@all"))  // Allow all commands
            .await;
        assert_eq!(result, Ok(redis::Value::Okay));

        // Set up the mock streaming credentials provider and attach it to the client
        println!("Setting up mock streaming credentials provider with default token...");
        let mut mock_provider = MockStreamingCredentialsProvider::new();
        mock_provider.start();
        ctx.client = ctx.client.with_credentials_provider(mock_provider);

        println!("Establishing multiplexed connection with JWT authentication...");
        let mut con = ctx.multiplexed_async_connection_tokio().await.unwrap();

        // Verify that the currently authenticated user is the expected one
        let current_user: String = redis::cmd("ACL")
            .arg("WHOAMI")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(current_user, expected_username);
        println!("Authenticated as user: {current_user}.");

        // Perform a basic ACL test, using the connection authenticated with the JWT token
        let users: Vec<String> = users_cmd.query_async(&mut con).await.unwrap();
        assert!(users.contains(&DEFAULT_USER.to_owned()));
        assert!(users.contains(&expected_username.to_owned()));

        println!("Testing ACL admin operations...");
        let _: () = redis::cmd("ACL")
            .arg("SETUSER")
            .arg(TEST_USER)
            .query_async(&mut con)
            .await
            .unwrap();

        let updated_users: Vec<String> = users_cmd.query_async(&mut con).await.unwrap();
        assert!(updated_users.contains(&DEFAULT_USER.to_owned()));
        assert!(updated_users.contains(&expected_username.to_owned()));
        assert!(updated_users.contains(&TEST_USER.to_owned()));

        println!("JWT authentication and ACL operations completed successfully!");
    }

    #[tokio::test]
    async fn test_token_rotation_with_mock_streaming_credentials_provider() {
        let mut ctx = TestContext::new();
        // Set up Redis users for each token in the rotation sequence
        let mut admin_con = ctx.async_connection().await.unwrap();
        let users_cmd = redis::cmd("ACL").arg("USERS").clone();
        let whoami_cmd = redis::cmd("ACL").arg("WHOAMI").clone();

        // Create a user with the JWT token as password and full permissions for each token
        println!("Setting up Redis users for token rotation test...");
        for (username, token_payload) in CREDENTIALS.iter() {
            let result = admin_con.req_packed_command(redis::cmd("ACL")
            .arg("SETUSER")
            .arg(username)
            .arg("on")  // Enable the user
            .arg(format!(">{token_payload}")) // Set JWT token as plain text password
            .arg("~*")  // Allow access to all keys
            .arg("+@all"))  // Allow all commands
            .await;
            assert_eq!(result, Ok(redis::Value::Okay));
        }

        // Set up the mock streaming credentials provider with multiple tokens and attach it to the client
        println!("Setting up mock provider with multiple tokens...");
        let mut mock_provider = MockStreamingCredentialsProvider::multiple_tokens();
        mock_provider.start();
        ctx.client = ctx.client.with_credentials_provider(mock_provider);

        println!("Establishing multiplexed connection with JWT authentication...");
        let mut con = ctx.multiplexed_async_connection_tokio().await.unwrap();

        // Verify that the currently authenticated user is the first in the sequence
        let current_user: String = whoami_cmd.query_async(&mut con).await.unwrap();
        assert_eq!(current_user, ALICE_OID_CLAIM);
        println!("Authenticated as user: {current_user}.");

        // Wait for token rotation to occur and test that the connection can still be used
        println!("Waiting for token rotation...");
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Check who the current user is after the first rotation
        let current_user: String = whoami_cmd.query_async(&mut con).await.unwrap();
        println!("First rotation completed. Authenticated as user: {current_user}.");
        // Should now be authenticated as Bob
        assert_eq!(current_user, BOB_OID_CLAIM);

        // Test that operations can still be performed after the first rotation
        let users: Vec<String> = users_cmd.query_async(&mut con).await.unwrap();
        println!("Users after first rotation: {:?}", users);

        // Wait for another rotation
        println!("Waiting for second token rotation...");
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Check who the current user is after the second rotation
        let current_user: String = whoami_cmd.query_async(&mut con).await.unwrap();
        println!("Second rotation completed. Authenticated as user: {current_user}.");
        // Should now be authenticated as Charlie
        assert_eq!(current_user, CHARLIE_OID_CLAIM);

        // Test that operations can still be performed after the second rotation
        let users: Vec<String> = users_cmd.query_async(&mut con).await.unwrap();
        println!("Users after second rotation: {:?}", users);

        println!("Token rotation test completed successfully!");
    }

    #[tokio::test]
    async fn test_authentication_error_handling_with_mock_streaming_credentials_provider() {
        let mut ctx = TestContext::new();
        // Set up Redis users for each token in the rotation sequence
        let mut admin_con = ctx.async_connection().await.unwrap();
        let whoami_cmd = redis::cmd("ACL").arg("WHOAMI").clone();

        // Create a user with the JWT token as password and full permissions for each token
        println!("Setting up Redis users for authentication error test...");
        for (username, token_payload) in CREDENTIALS.iter() {
            let result = admin_con
                .req_packed_command(
                    redis::cmd("ACL")
                        .arg("SETUSER")
                        .arg(username)
                        .arg("on")
                        .arg(format!(">{token_payload}"))
                        .arg("~*")
                        .arg("+@all"),
                )
                .await;
            assert_eq!(result, Ok(redis::Value::Okay));
        }

        // Set up mock provider with error at position 1 (second attempt)
        println!("Setting up mock provider with authentication error at position 1...");
        let mut mock_provider =
            MockStreamingCredentialsProvider::multiple_tokens_with_errors(vec![1]);
        mock_provider.start();
        ctx.client = ctx.client.with_credentials_provider(mock_provider);

        println!("Establishing multiplexed connection with JWT authentication...");
        let mut con = ctx.multiplexed_async_connection_tokio().await.unwrap();

        // Verify initial authentication (position 0 - should succeed)
        let current_user: String = whoami_cmd.query_async(&mut con).await.unwrap();
        assert_eq!(current_user, ALICE_OID_CLAIM);
        println!("Initial authentication successful as user: {current_user}.");

        // Wait for the first rotation attempt to occur (position 1 - should fail)
        println!("Waiting for first rotation attempt (should fail)...");
        tokio::time::sleep(Duration::from_millis(600)).await;

        let current_user_after_error: String = whoami_cmd.query_async(&mut con).await.unwrap();
        // The current user should still be Alice since re-authentication failed
        println!("Current user after error: {current_user_after_error}");
        assert_eq!(current_user_after_error, ALICE_OID_CLAIM);

        // Wait for the second rotation attempt to occur (position 2 - should succeed)
        println!("Waiting for second rotation attempt (should succeed)...");
        tokio::time::sleep(Duration::from_millis(600)).await;

        let current_user: String = whoami_cmd.query_async(&mut con).await.unwrap();
        // Should now be authenticated as Charlie (position 2, since position 1 was skipped due to error)
        println!("User after successful rotation: {current_user}");
        assert_eq!(current_user, CHARLIE_OID_CLAIM);

        // Wait for a third rotation attempt (back to position 0 - Alice)
        println!("Waiting for third rotation attempt (back to Alice)...");
        tokio::time::sleep(Duration::from_millis(600)).await;

        let current_user: String = whoami_cmd.query_async(&mut con).await.unwrap();
        // Should now be back to Alice (position 0, cycling back)
        println!("User after cycling back: {current_user}");
        assert_eq!(current_user, ALICE_OID_CLAIM);

        println!("Authentication error handling test completed successfully!");
    }
}
