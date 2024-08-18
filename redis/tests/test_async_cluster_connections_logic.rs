#![allow(unknown_lints, dependency_on_unit_never_type_fallback)]
#![cfg(feature = "cluster-async")]
mod support;

use redis::{
    cluster_async::testing::{AsyncClusterNode, RefreshConnectionType},
    testing::ClusterParams,
    ErrorKind,
};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use support::{
    get_mock_connection, get_mock_connection_with_port, modify_mock_connection_behavior,
    respond_startup, ConnectionIPReturnType, MockConnection, MockConnectionBehavior,
};

mod test_connect_and_check {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::support::{get_mock_connection_handler, ShouldReturnConnectionError};
    use redis::cluster_async::testing::{
        connect_and_check, ConnectAndCheckResult, ConnectionWithIp,
    };

    fn assert_partial_result(
        result: ConnectAndCheckResult<MockConnection>,
    ) -> (AsyncClusterNode<MockConnection>, redis::RedisError) {
        match result {
            ConnectAndCheckResult::ManagementConnectionFailed { node, err } => (node, err),
            ConnectAndCheckResult::Success(_) => {
                panic!("Expected partial result, got full success")
            }
            ConnectAndCheckResult::Failed(_) => panic!("Expected partial result, got a failure"),
        }
    }

    fn assert_full_success(
        result: ConnectAndCheckResult<MockConnection>,
    ) -> AsyncClusterNode<MockConnection> {
        match result {
            ConnectAndCheckResult::Success(node) => node,
            ConnectAndCheckResult::ManagementConnectionFailed { .. } => {
                panic!("Expected full success, got partial success")
            }
            ConnectAndCheckResult::Failed(_) => panic!("Expected partial result, got a failure"),
        }
    }

    #[tokio::test]
    async fn test_connect_and_check_connect_successfully() {
        // Test that upon refreshing all connections, if both connections were successful,
        // the returned node contains both user and management connection
        let name = "test_connect_and_check_connect_successfully";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, _| {
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );

        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        modify_mock_connection_behavior(name, |behavior| {
            behavior.returned_ip_type = ConnectionIPReturnType::Specified(ip)
        });

        let result = connect_and_check::<MockConnection>(
            &format!("{name}:6379"),
            ClusterParams::default(),
            None,
            RefreshConnectionType::AllConnections,
            None,
            None,
            None,
        )
        .await;
        let node = assert_full_success(result);
        assert!(node.management_connection.is_some());
        assert_eq!(node.user_connection.ip, Some(ip));
        assert_eq!(node.management_connection.unwrap().ip, Some(ip));
    }

    #[tokio::test]
    async fn test_connect_and_check_all_connections_one_connection_err_returns_only_user_conn() {
        // Test that upon refreshing all connections, if only one of the new connections fail,
        // the other successful connection will be used as the user connection, as a partial success.
        let name = "all_connections_one_connection_err";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, _| {
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );
        modify_mock_connection_behavior(name, |behavior| {
            // The second connection will fail
            behavior.return_connection_err =
                ShouldReturnConnectionError::OnOddIdx(AtomicUsize::new(0))
        });

        let params = ClusterParams::default();

        let result = connect_and_check::<MockConnection>(
            &format!("{name}:6379"),
            params.clone(),
            None,
            RefreshConnectionType::AllConnections,
            None,
            None,
            None,
        )
        .await;
        let (node, _) = assert_partial_result(result);
        assert!(node.management_connection.is_none());

        modify_mock_connection_behavior(name, |behavior| {
            // The first connection will fail
            behavior.return_connection_err =
                ShouldReturnConnectionError::OnOddIdx(AtomicUsize::new(1));
        });

        let result = connect_and_check::<MockConnection>(
            &format!("{name}:6379"),
            params,
            None,
            RefreshConnectionType::AllConnections,
            None,
            None,
            None,
        )
        .await;
        let (node, _) = assert_partial_result(result);
        assert!(node.management_connection.is_none());
    }

    #[tokio::test]
    async fn test_connect_and_check_all_connections_different_ip_returns_both_connections() {
        // Test that node's connections (e.g. user and management) can have different IPs for the same DNS endpoint.
        // It is relevant for cases where the DNS entry holds multiple IPs that routes to the same node, for example with load balancers.
        // The test verifies that upon refreshing all connections, if the IPs of the new connections differ,
        // the function uses all connections.
        let name = "all_connections_different_ip";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, _| {
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );
        modify_mock_connection_behavior(name, |behavior| {
            behavior.returned_ip_type = ConnectionIPReturnType::Different(AtomicUsize::new(0));
        });

        // The first connection will have 0.0.0.0 IP, the second 1.0.0.0
        let result = connect_and_check::<MockConnection>(
            &format!("{name}:6379"),
            ClusterParams::default(),
            None,
            RefreshConnectionType::AllConnections,
            None,
            None,
            None,
        )
        .await;
        let node = assert_full_success(result);
        assert!(node.management_connection.is_some());
        assert_eq!(
            node.user_connection.ip,
            Some(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))
        );
        assert_eq!(
            node.management_connection.unwrap().ip,
            Some(IpAddr::V4(Ipv4Addr::new(1, 0, 0, 0)))
        );
    }

    #[tokio::test]
    async fn test_connect_and_check_all_connections_both_conn_error_returns_err() {
        // Test that when trying to refresh all connections and both connections fail, the function returns with an error
        let name = "both_conn_error_returns_err";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, _| {
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );
        modify_mock_connection_behavior(name, |behavior| {
            behavior.return_connection_err = ShouldReturnConnectionError::Yes
        });

        let result = connect_and_check::<MockConnection>(
            &format!("{name}:6379"),
            ClusterParams::default(),
            None,
            RefreshConnectionType::AllConnections,
            None,
            None,
            None,
        )
        .await;
        let err = result.get_error().unwrap();
        assert!(
            err.to_string()
                .contains("Failed to refresh both connections")
                && err.kind() == ErrorKind::IoError
        );
    }

    #[tokio::test]
    async fn test_connect_and_check_only_management_same_ip() {
        // Test that when we refresh only the management connection and the new connection returned with the same IP as the user's,
        // the returned node contains a new management connection and the user connection remains unchanged
        let name = "only_management_same_ip";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, _| {
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );

        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        modify_mock_connection_behavior(name, |behavior| {
            behavior.returned_ip_type = ConnectionIPReturnType::Specified(ip)
        });

        let user_conn_id: usize = 1000;
        let user_conn = MockConnection {
            id: user_conn_id,
            handler: get_mock_connection_handler(name),
            port: 6379,
        };
        let node = AsyncClusterNode::new(
            ConnectionWithIp {
                conn: user_conn,
                ip: Some(ip),
            }
            .into_future(),
            None,
        );

        let result = connect_and_check::<MockConnection>(
            &format!("{name}:6379"),
            ClusterParams::default(),
            None,
            RefreshConnectionType::OnlyManagementConnection,
            Some(node),
            None,
            None,
        )
        .await;
        let node = assert_full_success(result);
        assert!(node.management_connection.is_some());
        // Confirm that the user connection remains unchanged
        assert_eq!(node.user_connection.conn.await.id, user_conn_id);
    }

    #[tokio::test]
    async fn test_connect_and_check_only_management_connection_err() {
        // Test that when we try the refresh only the management connection and it fails, we receive a partial success with the same node.
        let name = "only_management_connection_err";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, _| {
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );
        modify_mock_connection_behavior(name, |behavior| {
            behavior.return_connection_err = ShouldReturnConnectionError::Yes;
        });

        let user_conn_id: usize = 1000;
        let user_conn = MockConnection {
            id: user_conn_id,
            handler: get_mock_connection_handler(name),
            port: 6379,
        };
        let prev_ip = Some(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));
        let node = AsyncClusterNode::new(
            ConnectionWithIp {
                conn: user_conn,
                ip: prev_ip,
            }
            .into_future(),
            None,
        );

        let result = connect_and_check::<MockConnection>(
            &format!("{name}:6379"),
            ClusterParams::default(),
            None,
            RefreshConnectionType::OnlyManagementConnection,
            Some(node),
            None,
            None,
        )
        .await;
        let (node, _) = assert_partial_result(result);
        assert!(node.management_connection.is_none());
        // Confirm that the user connection was changed
        assert_eq!(node.user_connection.conn.await.id, user_conn_id);
        assert_eq!(node.user_connection.ip, prev_ip);
    }

    #[tokio::test]
    async fn test_connect_and_check_only_user_connection_same_ip() {
        // Test that upon refreshing only the user connection, if the newly created connection share the same IP as the existing management connection,
        // the managament connection remains unchanged
        let name = "only_user_connection_same_ip";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, _| {
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );

        let prev_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        modify_mock_connection_behavior(name, |behavior| {
            behavior.returned_ip_type = ConnectionIPReturnType::Specified(prev_ip);
        });
        let old_user_conn_id: usize = 1000;
        let management_conn_id: usize = 2000;
        let old_user_conn = MockConnection {
            id: old_user_conn_id,
            handler: get_mock_connection_handler(name),
            port: 6379,
        };
        let management_conn = MockConnection {
            id: management_conn_id,
            handler: get_mock_connection_handler(name),
            port: 6379,
        };

        let node = AsyncClusterNode::new(
            ConnectionWithIp {
                conn: old_user_conn,
                ip: Some(prev_ip),
            }
            .into_future(),
            Some(
                ConnectionWithIp {
                    conn: management_conn,
                    ip: Some(prev_ip),
                }
                .into_future(),
            ),
        );

        let result = connect_and_check::<MockConnection>(
            &format!("{name}:6379"),
            ClusterParams::default(),
            None,
            RefreshConnectionType::OnlyUserConnection,
            Some(node),
            None,
            None,
        )
        .await;
        let node = assert_full_success(result);
        // Confirm that a new user connection was created
        assert_ne!(node.user_connection.conn.await.id, old_user_conn_id);
        // Confirm that the management connection remains unchanged
        assert_eq!(
            node.management_connection.unwrap().conn.await.id,
            management_conn_id
        );
    }
}

mod test_check_node_connections {

    use super::*;
    use redis::cluster_async::testing::{check_node_connections, ConnectionWithIp};
    fn create_node_with_all_connections(name: &str) -> AsyncClusterNode<MockConnection> {
        let ip = None;
        AsyncClusterNode::new(
            ConnectionWithIp {
                conn: get_mock_connection_with_port(name, 1, 6380),
                ip,
            }
            .into_future(),
            Some(
                ConnectionWithIp {
                    conn: get_mock_connection_with_port(name, 2, 6381),
                    ip,
                }
                .into_future(),
            ),
        )
    }

    #[tokio::test]
    async fn test_check_node_connections_find_no_problem() {
        // Test that upon when checking both connections, if both connections are healthy no issue is returned.
        let name = "test_check_node_connections_find_no_problem";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, _| {
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );
        let node = create_node_with_all_connections(name);
        let response = check_node_connections::<MockConnection>(
            &node,
            &ClusterParams::default(),
            RefreshConnectionType::AllConnections,
            name,
        )
        .await;
        assert_eq!(response, None);
    }

    #[tokio::test]
    async fn test_check_node_connections_find_management_connection_issue() {
        // Test that upon checking both connections, if management connection isn't responding to pings, `OnlyManagementConnection` will be returned.
        let name = "test_check_node_connections_find_management_connection_issue";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, port| {
                if port == 6381 {
                    return Err(Err((ErrorKind::ClientError, "some error").into()));
                }
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );

        let node = create_node_with_all_connections(name);
        let response = check_node_connections::<MockConnection>(
            &node,
            &ClusterParams::default(),
            RefreshConnectionType::AllConnections,
            name,
        )
        .await;
        assert_eq!(
            response,
            Some(RefreshConnectionType::OnlyManagementConnection)
        );
    }

    #[tokio::test]
    async fn test_check_node_connections_find_missing_management_connection() {
        // Test that upon checking both connections, if management connection isn't present, `OnlyManagementConnection` will be returned.
        let name = "test_check_node_connections_find_missing_management_connection";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, _| {
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );

        let ip = None;
        let node = AsyncClusterNode::new(
            ConnectionWithIp {
                conn: get_mock_connection(name, 1),
                ip,
            }
            .into_future(),
            None,
        );
        let response = check_node_connections::<MockConnection>(
            &node,
            &ClusterParams::default(),
            RefreshConnectionType::AllConnections,
            name,
        )
        .await;
        assert_eq!(
            response,
            Some(RefreshConnectionType::OnlyManagementConnection)
        );
    }

    #[tokio::test]
    async fn test_check_node_connections_find_both_connections_issue() {
        // Test that upon checking both connections, if management connection isn't responding to pings, `OnlyManagementConnection` will be returned.
        let name = "test_check_node_connections_find_both_connections_issue";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|_, _| Err(Err((ErrorKind::ClientError, "some error").into()))),
        );

        let node = create_node_with_all_connections(name);
        let response = check_node_connections::<MockConnection>(
            &node,
            &ClusterParams::default(),
            RefreshConnectionType::AllConnections,
            name,
        )
        .await;
        assert_eq!(response, Some(RefreshConnectionType::AllConnections));
    }

    #[tokio::test]
    async fn test_check_node_connections_find_user_connection_issue() {
        // Test that upon checking both connections, if user connection isn't responding to pings, `OnlyUserConnection` will be returned.
        let name = "test_check_node_connections_find_user_connection_issue";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, port| {
                if port == 6380 {
                    return Err(Err((ErrorKind::ClientError, "some error").into()));
                }
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );

        let node = create_node_with_all_connections(name);
        let response = check_node_connections::<MockConnection>(
            &node,
            &ClusterParams::default(),
            RefreshConnectionType::AllConnections,
            name,
        )
        .await;
        assert_eq!(response, Some(RefreshConnectionType::OnlyUserConnection));
    }

    #[tokio::test]
    async fn test_check_node_connections_ignore_missing_management_connection_when_refreshing_user()
    {
        // Test that upon checking only user connection, issues with management connection won't affect the result.
        let name =
            "test_check_node_connections_ignore_management_connection_issue_when_refreshing_user";

        let _handle = MockConnectionBehavior::register_new(
            name,
            Arc::new(|cmd, _| {
                respond_startup(name, cmd)?;
                Ok(())
            }),
        );

        let node = AsyncClusterNode::new(
            ConnectionWithIp {
                conn: get_mock_connection(name, 1),
                ip: None,
            }
            .into_future(),
            None,
        );
        let response = check_node_connections::<MockConnection>(
            &node,
            &ClusterParams::default(),
            RefreshConnectionType::OnlyUserConnection,
            name,
        )
        .await;
        assert_eq!(response, None);
    }
}
