#![cfg(feature = "cluster-async")]
mod support;

mod async_cluster_connection_logic {
    use super::*;
    use redis::{cluster_async::connections_logic::connect_and_check, ClusterParams};
    use std::net::{IpAddr, Ipv4Addr};
    use support::{respond_startup, ConnectionIPReturnType, MockEnv};

    use crate::support::{modify_mock_connection_behavior, MockConnection};

    #[test]
    fn test_connect_and_check_connect_successfully() {
        // Test that upon refreshing all connections, if both connections were successful,
        // the returned node contains both user and management connection
        let name = "test_connect_and_check_connect_successfully";

        let mock_env = MockEnv::new(name, move |cmd, _| {
            respond_startup(name, cmd)?;
            Ok(())
        });

        let expected_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        modify_mock_connection_behavior(name, |behavior| {
            behavior.returned_ip_type = ConnectionIPReturnType::Specified(expected_ip)
        });

        mock_env.runtime.block_on(async {
            let (_conn, ip) = connect_and_check::<MockConnection>(
                &format!("{name}:6379"),
                ClusterParams::default(),
                None,
            )
            .await
            .unwrap();
            assert_eq!(ip, Some(expected_ip));
        })
    }
}
