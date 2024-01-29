#![cfg(feature = "cluster-async")]
mod support;

use futures_util::FutureExt;
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
    use super::*;
    use redis::cluster_async::testing::connect_and_check;

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

        let expected_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        modify_mock_connection_behavior(name, |behavior| {
            behavior.returned_ip_type = ConnectionIPReturnType::Specified(expected_ip)
        });

        let (_conn, ip) = connect_and_check::<MockConnection>(
            &format!("{name}:6379"),
            ClusterParams::default(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(ip, Some(expected_ip));
    }
}

mod test_check_node_connections {

    use super::*;
    use redis::cluster_async::testing::check_node_connections;

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

        let node = AsyncClusterNode::new(
            async { get_mock_connection(name, 1) }.boxed().shared(),
            Some(async { get_mock_connection(name, 2) }.boxed().shared()),
            None,
        );
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

        let node = AsyncClusterNode::new(
            async { get_mock_connection_with_port(name, 1, 6380) }
                .boxed()
                .shared(),
            Some(
                async { get_mock_connection_with_port(name, 2, 6381) }
                    .boxed()
                    .shared(),
            ),
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

        let node = AsyncClusterNode::new(
            async { get_mock_connection(name, 1) }.boxed().shared(),
            None,
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

        let node = AsyncClusterNode::new(
            async { get_mock_connection_with_port(name, 1, 6380) }
                .boxed()
                .shared(),
            Some(
                async { get_mock_connection_with_port(name, 2, 6381) }
                    .boxed()
                    .shared(),
            ),
            None,
        );
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

        let node = AsyncClusterNode::new(
            async { get_mock_connection_with_port(name, 1, 6380) }
                .boxed()
                .shared(),
            Some(
                async { get_mock_connection_with_port(name, 2, 6381) }
                    .boxed()
                    .shared(),
            ),
            None,
        );
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
            async { get_mock_connection(name, 1) }.boxed().shared(),
            None,
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
