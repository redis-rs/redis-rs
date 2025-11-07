#[cfg(feature = "entra-id")]
mod entra_id_tests {
    use azure_identity::UserAssignedId;
    use redis::{Client, ClientCertificate, EntraIdCredentialsProvider, RetryConfig};
    use std::sync::OnceLock;

    const REDIS_URL: &str = "REDIS_URL";

    const AZURE_TENANT_ID: &str = "AZURE_TENANT_ID";
    const AZURE_CLIENT_ID: &str = "AZURE_CLIENT_ID";
    const AZURE_CLIENT_SECRET: &str = "AZURE_CLIENT_SECRET";
    const AZURE_CLIENT_CERTIFICATE_PATH: &str = "AZURE_CLIENT_CERTIFICATE_PATH";
    const AZURE_USER_ASSIGNED_MANAGED_ID: &str = "AZURE_USER_ASSIGNED_MANAGED_ID";
    const AZURE_REDIS_SCOPES: &str = "AZURE_REDIS_SCOPES";

    fn get_redis_url() -> String {
        std::env::var(REDIS_URL)
            .unwrap_or_else(|_| panic!("The `REDIS_URL` environment variable is not set."))
    }

    fn get_env_var(var_name: &str) -> String {
        std::env::var(var_name)
            .unwrap_or_else(|_| panic!("The `{var_name}` environment variable is not set."))
    }

    static REDIS_SCOPES: OnceLock<Vec<String>> = OnceLock::new();

    fn get_redis_scopes() -> &'static Vec<String> {
        REDIS_SCOPES.get_or_init(|| {
            get_env_var(AZURE_REDIS_SCOPES)
                .split(',')
                .map(|s| s.trim().to_string())
                .collect()
        })
    }

    async fn test_redis_connection(mut provider: EntraIdCredentialsProvider, test_key: &str) {
        provider.start(RetryConfig::default());

        let client = Client::open(get_redis_url())
            .unwrap()
            .with_credentials_provider(provider);

        let mut con = client.get_multiplexed_async_connection().await.unwrap();

        redis::cmd("SET")
            .arg(test_key)
            .arg(42i32)
            .exec_async(&mut con)
            .await
            .unwrap();

        let result: Option<String> = redis::cmd("GET")
            .arg(test_key)
            .query_async(&mut con)
            .await
            .unwrap();

        assert_eq!(result, Some("42".to_string()));
    }

    #[tokio::test]
    #[ignore]
    async fn test_service_principal_client_secret() {
        let provider = EntraIdCredentialsProvider::new_client_secret_with_scopes(
            get_env_var(AZURE_TENANT_ID),
            get_env_var(AZURE_CLIENT_ID),
            get_env_var(AZURE_CLIENT_SECRET),
            get_redis_scopes().clone(),
        )
        .unwrap();
        test_redis_connection(provider, "service_principal_client_secret").await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_service_principal_client_certificate() {
        use base64::Engine;
        use std::fs;

        let certificate_path = get_env_var(AZURE_CLIENT_CERTIFICATE_PATH);
        let certificate_data =
            fs::read(&certificate_path).expect("Failed to read client certificate");

        // Convert the certificate data to base64
        let certificate_base64 =
            base64::engine::general_purpose::STANDARD.encode(&certificate_data);

        let provider = EntraIdCredentialsProvider::new_client_certificate_with_scopes(
            get_env_var(AZURE_TENANT_ID),
            get_env_var(AZURE_CLIENT_ID),
            ClientCertificate {
                base64_pkcs12: certificate_base64,
                password: None,
            },
            get_redis_scopes().clone(),
        )
        .unwrap();
        test_redis_connection(provider, "service_principal_client_certificate").await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_system_assigned_managed_identity() {
        let provider =
            EntraIdCredentialsProvider::new_system_assigned_managed_identity_with_scopes(
                get_redis_scopes().clone(),
            )
            .unwrap();
        test_redis_connection(provider, "system_assigned_managed_identity").await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_user_assigned_managed_identity() {
        let provider = EntraIdCredentialsProvider::new_user_assigned_managed_identity_with_scopes(
            UserAssignedId::ObjectId(get_env_var(AZURE_USER_ASSIGNED_MANAGED_ID)),
            get_redis_scopes().clone(),
        )
        .unwrap();
        test_redis_connection(provider, "user_assigned_managed_identity").await;
    }
}
