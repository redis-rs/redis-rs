mod support;

#[cfg(all(test, feature = "connection-manager"))]
mod connection_manager_init {
    use super::support::*;
    use redis::aio::ConnectionLike;
    use redis::{
        self as redis_crate, aio::ConnectionManagerConfig, cmd, Client, ConnectionInfo,
        RedisConnectionInfo, Value,
    };
    use rstest::rstest;

    #[rstest]
    #[cfg(feature = "connection-manager")]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    fn test_client_setname_on_connect(#[case] runtime: RuntimeType) {
        block_on_all(
            async move {
                let ctx = TestContext::new();
                let client = ctx.client.clone();
                let mut config = ConnectionManagerConfig::new();
                config = config.set_client_name("cm_test_client".to_string());
                let mut cm = client
                    .get_connection_manager_with_config(config)
                    .await
                    .unwrap();

                cm.await_ready().await.unwrap();

                // Query CLIENT LIST and verify name
                let list: String = cmd("CLIENT")
                    .arg("LIST")
                    .query_async(&mut cm)
                    .await
                    .unwrap();
                assert!(list.contains("name=cm_test_client"));
                Ok::<(), redis_crate::RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg(feature = "connection-manager")]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    fn test_reinit_after_reconnect(#[case] runtime: RuntimeType) {
        block_on_all(
            async move {
                let ctx = TestContext::new();
                let client = ctx.client.clone();
                let mut config = ConnectionManagerConfig::new();
                config = config.set_client_name("cm_reconnect".to_string());
                let mut cm = client
                    .get_connection_manager_with_config(config)
                    .await
                    .unwrap();

                cm.await_ready().await.unwrap();

                // Kill the client connection to force reconnect
                let mut admin = ctx.async_connection().await.unwrap();
                kill_client_async(&mut admin, &client).await.unwrap();

                // Wait for readiness again
                cm.await_ready().await.unwrap();

                let list: String = cmd("CLIENT")
                    .arg("LIST")
                    .query_async(&mut cm)
                    .await
                    .unwrap();
                assert!(list.contains("name=cm_reconnect"));
                Ok::<(), redis_crate::RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg(feature = "connection-manager")]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    fn test_security_validation_too_large_name(#[case] runtime: RuntimeType) {
        block_on_all(
            async move {
                let ctx = TestContext::new();
                let client = ctx.client.clone();
                let mut config = ConnectionManagerConfig::new();
                // Create a very large name > 64KB
                let big = "x".repeat(70 * 1024);
                // Early validation should fail manager creation
                config = config.set_client_name(big);
                let cm_res = client.get_connection_manager_with_config(config).await;
                let err = cm_res
                    .err()
                    .expect("expected error for oversized client name");
                assert!(format!("{}", err).contains("Client name too large"));
                Ok::<(), redis_crate::RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg(feature = "connection-manager")]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    fn test_timeout_behavior(#[case] runtime: RuntimeType) {
        block_on_all(
            async move {
                let ctx = TestContext::new();
                let client = ctx.client.clone();
                let mut config = ConnectionManagerConfig::new();
                // Set very short init timeout
                let sec = redis_crate::aio::SecurityConfig::new()
                    .set_init_timeout(std::time::Duration::from_millis(10));
                config = config.set_security_config(sec);
                // Use a user pipeline that will delay deterministically (DEBUG SLEEP 2)
                let mut p = redis_crate::pipe();
                p.cmd("DEBUG").arg("SLEEP").arg(2);
                config = config.enable_arbitrary_init_commands().set_init_pipeline(p);
                let res = client.get_connection_manager_with_config(config).await;
                let err = res
                    .err()
                    .expect("expected manager creation to fail due to init timeout");
                let msg = format!("{}", err);
                assert!(
                    msg.contains("timed out") || msg.contains("Initialization pipeline failed"),
                    "unexpected error: {}",
                    msg
                );
                Ok::<(), redis_crate::RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg(feature = "connection-manager")]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    fn test_safe_vs_arbitrary_modes(#[case] runtime: RuntimeType) {
        block_on_all(
            async move {
                let ctx = TestContext::new();
                let client = ctx.client.clone();

                // Safe mode: user pipeline ignored
                let mut safe_cfg = ConnectionManagerConfig::new();
                let mut p = redis_crate::pipe();
                p.cmd("SET").arg("cm_flag").arg("1");
                safe_cfg = safe_cfg.set_init_pipeline(p.clone());
                let mut cm_safe = client
                    .get_connection_manager_with_config(safe_cfg)
                    .await
                    .unwrap();
                cm_safe.await_ready().await.unwrap();
                let v: Option<String> = cmd("GET")
                    .arg("cm_flag")
                    .query_async(&mut cm_safe)
                    .await
                    .unwrap();
                assert!(v.is_none());

                // Arbitrary enabled: user pipeline executes
                let arb_cfg = ConnectionManagerConfig::new()
                    .enable_arbitrary_init_commands()
                    .set_init_pipeline(p);
                let mut cm_arb = client
                    .get_connection_manager_with_config(arb_cfg)
                    .await
                    .unwrap();
                cm_arb.await_ready().await.unwrap();
                let v: String = cmd("GET")
                    .arg("cm_flag")
                    .query_async(&mut cm_arb)
                    .await
                    .unwrap();
                assert_eq!(v, "1");
                Ok::<(), redis_crate::RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg(feature = "connection-manager")]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    fn test_auth_ordering(#[case] runtime: RuntimeType) {
        block_on_all(
            async move {
                // Create a user requiring AUTH and ensure AUTH happens before init
                let ctx = TestContext::new();
                let mut admin_conn = ctx.async_connection().await.unwrap();

                let username = "cmuser";
                let password = "cmpass";

                // Add a user with permissions and password
                let mut set_user_cmd = redis_crate::Cmd::new();
                set_user_cmd
                    .arg("ACL")
                    .arg("SETUSER")
                    .arg(username)
                    .arg("on")
                    .arg("+acl")
                    .arg("+client")
                    .arg(format!(">{password}"));
                assert_eq!(
                    admin_conn.req_packed_command(&set_user_cmd).await,
                    Ok(Value::Okay)
                );

                let client = Client::open(ConnectionInfo {
                    addr: ctx.server.client_addr().clone(),
                    redis: RedisConnectionInfo {
                        username: Some(username.to_string()),
                        password: Some(password.to_string()),
                        ..Default::default()
                    },
                })
                .unwrap();

                let mut config = ConnectionManagerConfig::new();
                config = config.set_client_name("auth_name".to_string());
                let mut cm = client
                    .get_connection_manager_with_config(config)
                    .await
                    .unwrap();
                cm.await_ready().await.unwrap();

                // If AUTH occurred after init, SETNAME would fail. By reaching here, ordering is correct.
                let list: String = cmd("CLIENT")
                    .arg("LIST")
                    .query_async(&mut cm)
                    .await
                    .unwrap();
                assert!(list.contains("name=auth_name"));
                Ok::<(), redis_crate::RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }
}
