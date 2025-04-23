#![cfg(feature = "sentinel")]
mod support;

use std::collections::HashMap;

use crate::support::*;
use redis::sentinel::SentinelClientBuilder;
use redis::{
    sentinel::{Sentinel, SentinelClient, SentinelNodeConnectionInfo},
    Client, Connection, ConnectionAddr, ConnectionInfo, Role,
};

fn parse_replication_info(value: &str) -> HashMap<&str, &str> {
    let info_map: std::collections::HashMap<&str, &str> = value
        .split("\r\n")
        .filter(|line| !line.trim_start().starts_with('#'))
        .filter_map(|line| line.split_once(':'))
        .collect();
    info_map
}

fn assert_is_master_role(replication_info: String) {
    let info_map = parse_replication_info(&replication_info);
    assert_eq!(info_map.get("role"), Some(&"master"));
}

fn assert_replica_role_and_master_addr(replication_info: String, expected_master: &ConnectionInfo) {
    let info_map = parse_replication_info(&replication_info);

    assert_eq!(info_map.get("role"), Some(&"slave"));

    let (master_host, master_port) = match &expected_master.addr {
        ConnectionAddr::Tcp(host, port) => (host, port),
        ConnectionAddr::TcpTls {
            host,
            port,
            insecure: _,
            tls_params: _,
        } => (host, port),
        ConnectionAddr::Unix(..) => panic!("Unexpected master connection type"),
    };

    assert_eq!(info_map.get("master_host"), Some(&master_host.as_str()));
    assert_eq!(
        info_map.get("master_port"),
        Some(&master_port.to_string().as_str())
    );
}

fn assert_is_connection_to_master(conn: &mut Connection) {
    let info: String = redis::cmd("INFO").arg("REPLICATION").query(conn).unwrap();
    assert_is_master_role(info);
}

fn assert_connection_is_replica_of_correct_master(conn: &mut Connection, master_client: &Client) {
    let info: String = redis::cmd("INFO").arg("REPLICATION").query(conn).unwrap();
    assert_replica_role_and_master_addr(info, master_client.get_connection_info());
}

/// Get replica clients from the sentinel in a rotating fashion, asserting that they are
/// indeed replicas of the given master, and returning a list of their addresses.
fn connect_to_all_replicas(
    sentinel: &mut Sentinel,
    master_name: &str,
    master_client: &Client,
    node_conn_info: &SentinelNodeConnectionInfo,
    number_of_replicas: u16,
) -> Vec<ConnectionAddr> {
    let mut replica_conn_infos = vec![];

    for _ in 0..number_of_replicas {
        let replica_client = sentinel
            .replica_rotate_for(master_name, Some(node_conn_info))
            .unwrap();
        let mut replica_con = replica_client.get_connection().unwrap();

        assert!(!replica_conn_infos.contains(&replica_client.get_connection_info().addr));
        replica_conn_infos.push(replica_client.get_connection_info().addr.clone());

        assert_connection_is_replica_of_correct_master(&mut replica_con, master_client);
    }

    replica_conn_infos
}

fn assert_connect_to_known_replicas(
    sentinel: &mut Sentinel,
    replica_conn_infos: &[ConnectionAddr],
    master_name: &str,
    master_client: &Client,
    node_conn_info: &SentinelNodeConnectionInfo,
    number_of_connections: u32,
) {
    for _ in 0..number_of_connections {
        let replica_client = sentinel
            .replica_rotate_for(master_name, Some(node_conn_info))
            .unwrap();
        let mut replica_con = replica_client.get_connection().unwrap();

        assert!(replica_conn_infos.contains(&replica_client.get_connection_info().addr));

        assert_connection_is_replica_of_correct_master(&mut replica_con, master_client);
    }
}

#[test]
fn test_sentinel_role_no_permission() {
    let number_of_replicas = 3;
    let master_name = "master1";
    let mut cluster = TestSentinelContext::new(2, number_of_replicas, 3);
    let node_conn_info = cluster.sentinel_node_connection_info();
    let sentinel = cluster.sentinel_mut();

    let master_client = sentinel
        .master_for(master_name, Some(&node_conn_info))
        .unwrap();
    let mut master_con = master_client.get_connection().unwrap();

    let user: String = redis::cmd("ACL")
        .arg("whoami")
        .query(&mut master_con)
        .unwrap();
    //Remove ROLE permission for the given user on master
    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&user)
        .arg("-role")
        .query(&mut master_con)
        .unwrap();

    //Remove ROLE permission for the given user on replicas
    for _ in 0..number_of_replicas {
        let replica_client = sentinel
            .replica_rotate_for(master_name, Some(&node_conn_info))
            .unwrap();
        let mut replica_con = replica_client.get_connection().unwrap();
        let _: () = redis::cmd("ACL")
            .arg("SETUSER")
            .arg(&user)
            .arg("-role")
            .query(&mut replica_con)
            .unwrap();
    }

    let master_client = sentinel
        .master_for(master_name, Some(&node_conn_info))
        .unwrap();
    let mut master_con = master_client.get_connection().unwrap();

    assert_is_connection_to_master(&mut master_con);
}

#[test]
fn test_sentinel_connect_to_random_replica() {
    let number_of_replicas = 3;
    let master_name = "master1";
    let mut cluster = TestSentinelContext::new(2, number_of_replicas, 3);
    let node_conn_info = cluster.sentinel_node_connection_info();
    let sentinel = cluster.sentinel_mut();

    let master_client = sentinel
        .master_for(master_name, Some(&node_conn_info))
        .unwrap();
    let mut master_con = master_client.get_connection().unwrap();

    assert_is_connection_to_master(&mut master_con);

    let mut replica_con = sentinel
        .replica_for(master_name, Some(&node_conn_info))
        .unwrap()
        .get_connection()
        .unwrap();

    assert_connection_is_replica_of_correct_master(&mut replica_con, &master_client);
}

#[test]
fn test_sentinel_connect_to_multiple_replicas() {
    let number_of_replicas = 3;
    let master_name = "master1";
    let mut cluster = TestSentinelContext::new(2, number_of_replicas, 3);
    let node_conn_info = cluster.sentinel_node_connection_info();
    let sentinel = cluster.sentinel_mut();

    let master_client = sentinel
        .master_for(master_name, Some(&node_conn_info))
        .unwrap();
    let mut master_con = master_client.get_connection().unwrap();

    assert_is_connection_to_master(&mut master_con);

    let replica_conn_infos = connect_to_all_replicas(
        sentinel,
        master_name,
        &master_client,
        &node_conn_info,
        number_of_replicas,
    );

    assert_connect_to_known_replicas(
        sentinel,
        &replica_conn_infos,
        master_name,
        &master_client,
        &node_conn_info,
        10,
    );
}

#[test]
fn test_sentinel_server_down() {
    let number_of_replicas = 3;
    let master_name = "master1";
    let mut context = TestSentinelContext::new(2, number_of_replicas, 3);
    let node_conn_info = context.sentinel_node_connection_info();
    let sentinel = context.sentinel_mut();

    let master_client = sentinel
        .master_for(master_name, Some(&node_conn_info))
        .unwrap();
    let mut master_con = master_client.get_connection().unwrap();

    assert_is_connection_to_master(&mut master_con);

    context.cluster.sentinel_servers[0].stop();
    std::thread::sleep(std::time::Duration::from_millis(25));

    let sentinel = context.sentinel_mut();

    let replica_conn_infos = connect_to_all_replicas(
        sentinel,
        master_name,
        &master_client,
        &node_conn_info,
        number_of_replicas,
    );

    assert_connect_to_known_replicas(
        sentinel,
        &replica_conn_infos,
        master_name,
        &master_client,
        &node_conn_info,
        10,
    );
}

#[test]
fn test_sentinel_client() {
    let master_name = "master1";
    let mut context = TestSentinelContext::new(2, 3, 3);
    for sentinel in context.sentinels_connection_info() {
        let mut conn = Client::open(sentinel.clone())
            .unwrap()
            .get_connection()
            .unwrap();
        let role: Role = redis::cmd("ROLE").query(&mut conn).unwrap();
        assert!(matches!(role, Role::Sentinel { .. }));
    }

    let mut master_client = SentinelClient::build(
        context.sentinels_connection_info().clone(),
        String::from(master_name),
        Some(context.sentinel_node_connection_info()),
        redis::sentinel::SentinelServerType::Master,
    )
    .unwrap();

    let mut replica_client = SentinelClient::build(
        context.sentinels_connection_info().clone(),
        String::from(master_name),
        Some(context.sentinel_node_connection_info()),
        redis::sentinel::SentinelServerType::Replica,
    )
    .unwrap();

    let mut master_con = master_client.get_connection().unwrap();
    let role: Role = redis::cmd("ROLE").query(&mut master_con).unwrap();
    assert!(matches!(role, Role::Primary { .. }));

    assert_is_connection_to_master(&mut master_con);

    let node_conn_info = context.sentinel_node_connection_info();
    let sentinel = context.sentinel_mut();
    let master_client = sentinel
        .master_for(master_name, Some(&node_conn_info))
        .unwrap();

    for _ in 0..20 {
        let mut replica_con = replica_client.get_connection().unwrap();
        let role = redis::cmd("ROLE").query(&mut replica_con).unwrap();
        assert!(matches!(role, Role::Replica { .. }));

        assert_connection_is_replica_of_correct_master(&mut replica_con, &master_client);
    }
}

#[test]
fn test_sentinel_client_builder() {
    let master_name = "master1";
    let mut context = TestSentinelContext::new(2, 3, 3);
    for sentinel in context.sentinels_connection_info() {
        let mut conn = Client::open(sentinel.clone())
            .unwrap()
            .get_connection()
            .unwrap();
        let role: Role = redis::cmd("ROLE").query(&mut conn).unwrap();
        assert!(matches!(role, Role::Sentinel { .. }));
    }

    let mut master_client_builder = SentinelClientBuilder::new(
        context
            .sentinels_connection_info
            .iter()
            .map(|sentinel| sentinel.addr.clone()),
        String::from(master_name),
        redis::sentinel::SentinelServerType::Master,
    )
    .unwrap();

    let mut replica_client_builder = SentinelClientBuilder::new(
        context
            .sentinels_connection_info
            .iter()
            .map(|sentinel| sentinel.addr.clone()),
        String::from(master_name),
        redis::sentinel::SentinelServerType::Replica,
    )
    .unwrap();

    if let Some(username) = &context.sentinels_connection_info[0].redis.username.clone() {
        master_client_builder =
            master_client_builder.set_client_to_sentinel_username(username.clone());
    }

    if let Some(password) = &context.sentinels_connection_info[0].redis.password {
        master_client_builder =
            master_client_builder.set_client_to_sentinel_password(password.clone());
    }

    master_client_builder = master_client_builder
        .set_client_to_sentinel_protocol(context.sentinels_connection_info[0].redis.protocol);

    if let Some(tls_mode) = context.tls_mode() {
        master_client_builder = master_client_builder.set_client_to_redis_tls_mode(tls_mode);
        replica_client_builder = replica_client_builder.set_client_to_redis_tls_mode(tls_mode);
    }

    let mut master_client = master_client_builder.build().unwrap();
    let mut replica_client = replica_client_builder.build().unwrap();

    let mut master_con = master_client.get_connection().unwrap();
    let role: Role = redis::cmd("ROLE").query(&mut master_con).unwrap();
    assert!(matches!(role, Role::Primary { .. }));

    assert_is_connection_to_master(&mut master_con);

    let node_conn_info = context.sentinel_node_connection_info();
    let sentinel = context.sentinel_mut();
    let master_client = sentinel
        .master_for(master_name, Some(&node_conn_info))
        .unwrap();

    for _ in 0..20 {
        let mut replica_con = replica_client.get_connection().unwrap();
        let role = redis::cmd("ROLE").query(&mut replica_con).unwrap();
        assert!(matches!(role, Role::Replica { .. }));

        assert_connection_is_replica_of_correct_master(&mut replica_con, &master_client);
    }
}

#[cfg(feature = "aio")]
pub mod async_tests {
    use redis::{
        aio::MultiplexedConnection,
        sentinel::{Sentinel, SentinelClient, SentinelNodeConnectionInfo},
        AsyncConnectionConfig, Client, ConnectionAddr, RedisError,
    };
    use rstest::rstest;

    use crate::{assert_is_master_role, assert_replica_role_and_master_addr, support::*};

    async fn async_assert_is_connection_to_master(conn: &mut MultiplexedConnection) {
        let info: String = redis::cmd("INFO")
            .arg("REPLICATION")
            .query_async(conn)
            .await
            .unwrap();

        assert_is_master_role(info);
    }

    async fn async_assert_connection_is_replica_of_correct_master(
        conn: &mut MultiplexedConnection,
        master_client: &Client,
    ) {
        let info: String = redis::cmd("INFO")
            .arg("REPLICATION")
            .query_async(conn)
            .await
            .unwrap();

        assert_replica_role_and_master_addr(info, master_client.get_connection_info());
    }

    /// Async version of connect_to_all_replicas
    async fn async_connect_to_all_replicas(
        sentinel: &mut Sentinel,
        master_name: &str,
        master_client: &Client,
        node_conn_info: &SentinelNodeConnectionInfo,
        number_of_replicas: u16,
    ) -> Vec<ConnectionAddr> {
        let mut replica_conn_infos = vec![];

        for _ in 0..number_of_replicas {
            let replica_client = sentinel
                .async_replica_rotate_for(master_name, Some(node_conn_info))
                .await
                .unwrap();
            let mut replica_con = replica_client
                .get_multiplexed_async_connection()
                .await
                .unwrap();

            assert!(
                !replica_conn_infos.contains(&replica_client.get_connection_info().addr),
                "pushing {:?} into {:?}",
                replica_client.get_connection_info().addr,
                replica_conn_infos
            );
            replica_conn_infos.push(replica_client.get_connection_info().addr.clone());

            async_assert_connection_is_replica_of_correct_master(&mut replica_con, master_client)
                .await;
        }

        replica_conn_infos
    }

    async fn async_assert_connect_to_known_replicas(
        sentinel: &mut Sentinel,
        replica_conn_infos: &[ConnectionAddr],
        master_name: &str,
        master_client: &Client,
        node_conn_info: &SentinelNodeConnectionInfo,
        number_of_connections: u32,
    ) {
        for _ in 0..number_of_connections {
            let replica_client = sentinel
                .async_replica_rotate_for(master_name, Some(node_conn_info))
                .await
                .unwrap();
            let mut replica_con = replica_client
                .get_multiplexed_async_connection()
                .await
                .unwrap();

            assert!(replica_conn_infos.contains(&replica_client.get_connection_info().addr));

            async_assert_connection_is_replica_of_correct_master(&mut replica_con, master_client)
                .await;
        }
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_sentinel_connect_to_random_replica_async(#[case] runtime: RuntimeType) {
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, 3, 3);
        let node_conn_info = context.sentinel_node_connection_info();
        let sentinel = context.sentinel_mut();

        block_on_all(
            async move {
                let master_client = sentinel
                    .async_master_for(master_name, Some(&node_conn_info))
                    .await?;
                let mut master_con = master_client.get_multiplexed_async_connection().await?;

                let mut replica_con = sentinel
                    .async_replica_for(master_name, Some(&node_conn_info))
                    .await?
                    .get_multiplexed_async_connection()
                    .await?;

                async_assert_is_connection_to_master(&mut master_con).await;
                async_assert_connection_is_replica_of_correct_master(
                    &mut replica_con,
                    &master_client,
                )
                .await;

                Ok::<(), RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_sentinel_connect_to_multiple_replicas_async(#[case] runtime: RuntimeType) {
        let number_of_replicas = 3;
        let master_name = "master1";
        let mut cluster = TestSentinelContext::new(2, number_of_replicas, 3);
        let node_conn_info = cluster.sentinel_node_connection_info();
        let sentinel = cluster.sentinel_mut();

        block_on_all(
            async move {
                let master_client = sentinel
                    .async_master_for(master_name, Some(&node_conn_info))
                    .await?;
                let mut master_con = master_client.get_multiplexed_async_connection().await?;

                async_assert_is_connection_to_master(&mut master_con).await;

                let replica_conn_infos = async_connect_to_all_replicas(
                    sentinel,
                    master_name,
                    &master_client,
                    &node_conn_info,
                    number_of_replicas,
                )
                .await;

                async_assert_connect_to_known_replicas(
                    sentinel,
                    &replica_conn_infos,
                    master_name,
                    &master_client,
                    &node_conn_info,
                    10,
                )
                .await;

                Ok::<(), RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_sentinel_server_down_async(#[case] runtime: RuntimeType) {
        let number_of_replicas = 3;
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, number_of_replicas, 3);
        let node_conn_info = context.sentinel_node_connection_info();

        block_on_all(
            async move {
                let sentinel = context.sentinel_mut();

                let master_client = sentinel
                    .async_master_for(master_name, Some(&node_conn_info))
                    .await?;
                let mut master_con = master_client.get_multiplexed_async_connection().await?;

                async_assert_is_connection_to_master(&mut master_con).await;

                context.cluster.sentinel_servers[0].stop();
                std::thread::sleep(std::time::Duration::from_millis(25));

                let sentinel = context.sentinel_mut();

                let replica_conn_infos = async_connect_to_all_replicas(
                    sentinel,
                    master_name,
                    &master_client,
                    &node_conn_info,
                    number_of_replicas,
                )
                .await;

                async_assert_connect_to_known_replicas(
                    sentinel,
                    &replica_conn_infos,
                    master_name,
                    &master_client,
                    &node_conn_info,
                    10,
                )
                .await;

                Ok::<(), RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_sentinel_client_async(#[case] runtime: RuntimeType) {
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, 3, 3);
        let mut master_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from(master_name),
            Some(context.sentinel_node_connection_info()),
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();

        let mut replica_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from(master_name),
            Some(context.sentinel_node_connection_info()),
            redis::sentinel::SentinelServerType::Replica,
        )
        .unwrap();

        block_on_all(
            async move {
                let mut master_con = master_client.get_async_connection().await?;

                async_assert_is_connection_to_master(&mut master_con).await;

                let node_conn_info = context.sentinel_node_connection_info();
                let sentinel = context.sentinel_mut();
                let master_client = sentinel
                    .async_master_for(master_name, Some(&node_conn_info))
                    .await?;

                // Read commands to the replica node
                for _ in 0..20 {
                    let mut replica_con = replica_client.get_async_connection().await?;

                    async_assert_connection_is_replica_of_correct_master(
                        &mut replica_con,
                        &master_client,
                    )
                    .await;
                }

                Ok::<(), RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_sentinel_client_async_with_connection_timeout(#[case] runtime: RuntimeType) {
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, 3, 3);
        let mut master_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from(master_name),
            Some(context.sentinel_node_connection_info()),
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();

        let mut replica_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from(master_name),
            Some(context.sentinel_node_connection_info()),
            redis::sentinel::SentinelServerType::Replica,
        )
        .unwrap();

        let connection_options =
            AsyncConnectionConfig::new().set_connection_timeout(std::time::Duration::from_secs(1));

        block_on_all(
            async move {
                let mut master_con = master_client
                    .get_async_connection_with_config(&connection_options)
                    .await?;

                async_assert_is_connection_to_master(&mut master_con).await;

                let node_conn_info = context.sentinel_node_connection_info();
                let sentinel = context.sentinel_mut();
                let master_client = sentinel
                    .async_master_for(master_name, Some(&node_conn_info))
                    .await?;

                // Read commands to the replica node
                for _ in 0..20 {
                    let mut replica_con = replica_client
                        .get_async_connection_with_config(&connection_options)
                        .await?;

                    async_assert_connection_is_replica_of_correct_master(
                        &mut replica_con,
                        &master_client,
                    )
                    .await;
                }

                Ok::<(), RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_sentinel_client_async_with_response_timeout(#[case] runtime: RuntimeType) {
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, 3, 3);
        let mut master_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from(master_name),
            Some(context.sentinel_node_connection_info()),
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();

        let mut replica_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from(master_name),
            Some(context.sentinel_node_connection_info()),
            redis::sentinel::SentinelServerType::Replica,
        )
        .unwrap();

        let connection_options =
            AsyncConnectionConfig::new().set_response_timeout(std::time::Duration::from_secs(1));

        block_on_all(
            async move {
                let mut master_con = master_client
                    .get_async_connection_with_config(&connection_options)
                    .await?;

                async_assert_is_connection_to_master(&mut master_con).await;

                let node_conn_info = context.sentinel_node_connection_info();
                let sentinel = context.sentinel_mut();
                let master_client = sentinel
                    .async_master_for(master_name, Some(&node_conn_info))
                    .await?;

                // Read commands to the replica node
                for _ in 0..20 {
                    let mut replica_con = replica_client
                        .get_async_connection_with_config(&connection_options)
                        .await?;

                    async_assert_connection_is_replica_of_correct_master(
                        &mut replica_con,
                        &master_client,
                    )
                    .await;
                }

                Ok::<(), RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_sentinel_client_async_with_timeouts(#[case] runtime: RuntimeType) {
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, 3, 3);
        let mut master_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from(master_name),
            Some(context.sentinel_node_connection_info()),
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();

        let mut replica_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from(master_name),
            Some(context.sentinel_node_connection_info()),
            redis::sentinel::SentinelServerType::Replica,
        )
        .unwrap();

        let connection_options = AsyncConnectionConfig::new()
            .set_connection_timeout(std::time::Duration::from_secs(1))
            .set_response_timeout(std::time::Duration::from_secs(1));

        block_on_all(
            async move {
                let mut master_con = master_client
                    .get_async_connection_with_config(&connection_options)
                    .await?;

                async_assert_is_connection_to_master(&mut master_con).await;

                let node_conn_info = context.sentinel_node_connection_info();
                let sentinel = context.sentinel_mut();
                let master_client = sentinel
                    .async_master_for(master_name, Some(&node_conn_info))
                    .await?;

                // Read commands to the replica node
                for _ in 0..20 {
                    let mut replica_con = replica_client
                        .get_async_connection_with_config(&connection_options)
                        .await?;

                    async_assert_connection_is_replica_of_correct_master(
                        &mut replica_con,
                        &master_client,
                    )
                    .await;
                }

                Ok::<(), RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }
}

#[cfg(feature = "r2d2")]
pub mod pool_tests {
    use std::{collections::HashSet, ops::DerefMut};

    use super::*;
    use r2d2::Pool;
    use redis::sentinel::LockedSentinelClient;

    fn parse_client_info(value: &str) -> HashMap<&str, &str> {
        let info_map: std::collections::HashMap<&str, &str> = value
            .split(" ")
            .filter_map(|line| line.split_once('='))
            .collect();
        info_map
    }

    #[test]
    fn test_sentinel_client() {
        let master_name = "master1";
        let context = TestSentinelContext::new(2, 3, 3);
        let master_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from(master_name),
            Some(context.sentinel_node_connection_info()),
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();

        let pool = Pool::builder()
            .max_size(5)
            .build(LockedSentinelClient::new(master_client))
            .unwrap();

        let mut conns = Vec::new();
        for _ in 0..5 {
            let conn = pool.get().unwrap();

            conns.push(conn);
        }

        // since max_size is 5 and we haven't freed any connection this try should fail
        let try_conn = pool.try_get();
        assert!(try_conn.is_none());

        let mut client_id_set = HashSet::new();

        for mut conn in conns {
            let client_info_str: String = redis::cmd("CLIENT")
                .arg("INFO")
                .query(conn.deref_mut())
                .unwrap();

            let client_info_parsed = parse_client_info(client_info_str.as_str());

            // assert if all connections have different IDs
            assert!(client_id_set.insert(client_info_parsed.get("id").unwrap().to_string()));

            assert_is_connection_to_master(conn.deref_mut());
        }

        // since previous connections are freed, this should work
        let try_conn = pool.try_get();
        assert!(try_conn.is_some());
    }
}
