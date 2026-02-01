#![cfg(feature = "sentinel")]
mod support;

use std::collections::HashMap;
use std::time::Duration;

use crate::support::*;
use redis::sentinel::SentinelClientBuilder;
use redis::{
    Client, Connection, ConnectionAddr, ConnectionInfo, ErrorKind, RedisError, Role,
    sentinel::{Sentinel, SentinelClient, SentinelNodeConnectionInfo},
};
use redis_test::server::use_protocol;

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

    let (master_host, master_port) = match expected_master.addr() {
        ConnectionAddr::Tcp(host, port) => (host, port),
        ConnectionAddr::TcpTls {
            host,
            port,
            insecure: _,
            tls_params: _,
        } => (host, port),
        ConnectionAddr::Unix(..) => panic!("Unexpected master connection type"),
        _ => panic!("Unknown address type"),
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

        assert!(!replica_conn_infos.contains(replica_client.get_connection_info().addr()));
        replica_conn_infos.push(replica_client.get_connection_info().addr().clone());

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

        assert!(replica_conn_infos.contains(replica_client.get_connection_info().addr()));

        assert_connection_is_replica_of_correct_master(&mut replica_con, master_client);
    }
}

#[test]
fn test_get_replica_clients_success() {
    let number_of_replicas = 3;
    let master_name = "master1";
    let mut context = TestSentinelContext::new(2, number_of_replicas, 3);
    let node_conn_info = context.sentinel_node_connection_info();
    let sentinel = context.sentinel_mut();
    let replicas = sentinel
        .get_replica_clients(master_name, Some(&node_conn_info))
        .unwrap();

    assert_eq!(replicas.len(), number_of_replicas as usize);
}

#[test]
fn test_get_replica_clients_invalid_master_name() {
    let mut context = TestSentinelContext::new(2, 2, 3);
    let node_conn_info = context.sentinel_node_connection_info();
    let sentinel = context.sentinel_mut();

    let err = sentinel
        .get_replica_clients("invalid_master_name", Some(&node_conn_info))
        .unwrap_err();
    // sometimes we get unexplained connection refused errors.
    assert!(
        matches!(
            err.kind(),
            ErrorKind::Server(redis::ServerErrorKind::ResponseError) | ErrorKind::Io
        ),
        "{err}"
    );
}

#[test]
fn test_get_replica_clients_report_correct_master() {
    let number_of_replicas = 3;
    let master_name = "master1";
    let mut context = TestSentinelContext::new(2, number_of_replicas, 3);
    let node_conn_info = context.sentinel_node_connection_info();
    let sentinel = context.sentinel_mut();
    let master_client = sentinel
        .master_for(master_name, Some(&node_conn_info))
        .unwrap();

    let replicas = sentinel
        .get_replica_clients(master_name, Some(&node_conn_info))
        .unwrap();

    for replica_client in replicas {
        let mut con = replica_client.get_connection().unwrap();
        assert_connection_is_replica_of_correct_master(&mut con, &master_client);
    }
}

#[test]
fn test_get_replica_clients_with_one_replica_down() {
    let number_of_replicas = 3;
    let master_name = "master0";

    let mut context = TestSentinelContext::new(2, number_of_replicas, 3);
    let node_conn_info = context.sentinel_node_connection_info();
    let sentinel_conn_info = context.sentinels_connection_info()[0].clone();
    let mut conn = Client::open(sentinel_conn_info)
        .unwrap()
        .get_connection()
        .unwrap();

    redis::cmd("SENTINEL")
        .arg("set")
        .arg(master_name)
        .arg("down-after-milliseconds")
        .arg("500")
        .query::<()>(&mut conn)
        .unwrap();

    context.cluster.servers[1].stop();

    std::thread::sleep(std::time::Duration::from_millis(800));

    let sentinel = context.sentinel_mut();
    let replicas = sentinel
        .get_replica_clients(master_name, Some(&node_conn_info))
        .expect("Failed to get replicas");

    assert_eq!(
        replicas.len(),
        (number_of_replicas - 1) as usize,
        "Unexpected num of replicas total"
    );
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
fn test_sentinel_no_role_or_info_permission() {
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
    //Remove both commands used to detect master
    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&user)
        .arg("-role")
        .arg("-info")
        .query(&mut master_con)
        .unwrap();

    let err = sentinel
        .master_for(master_name, Some(&node_conn_info))
        .unwrap_err();
    assert_eq!(err.code(), Some("NOPERM"));
    // Ensure we get the error returned by role command
    assert!(err.detail().unwrap().contains("role"));
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
    std::thread::sleep(Duration::from_millis(25));

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
fn test_sentinel_redis_client() {
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
fn test_sentinel_client() {
    let master_name = "master1";
    let context = TestSentinelContext::new(2, 3, 3);
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

    let sentinel_client_1 = master_client.get_sentinel_client().unwrap();
    let sentinel_client_2 = replica_client.get_sentinel_client().unwrap();
    let first_configured_sentinel = context.sentinels_connection_info.first().unwrap();

    assert_eq!(
        sentinel_client_1.get_connection_info().addr(),
        sentinel_client_2.get_connection_info().addr()
    );

    assert_eq!(
        first_configured_sentinel.addr(),
        sentinel_client_2.get_connection_info().addr()
    );
}

#[test]
fn test_sentinel_client_io_error() {
    let master_name = "master1";

    let mut master_client = SentinelClient::build(
        vec!["redis://test:6379"],
        String::from(master_name),
        None,
        redis::sentinel::SentinelServerType::Master,
    )
    .unwrap();

    let sentinel_client = master_client.get_sentinel_client();
    let err = sentinel_client.expect_err("Expected an error");
    assert!(err.is_io_error());
}

#[test]
fn test_sentinel_client_not_sentinel_error() {
    let master_name = "master1";
    let mut context = TestSentinelContext::new(2, 3, 3);
    // Change the context with incorrect sentinel servers
    context.sentinels_connection_info = context
        .cluster
        .servers
        .iter()
        .map(|redis_server| redis_server.connection_info().clone())
        .collect::<Vec<_>>();
    let mut master_client = SentinelClient::build(
        context.sentinels_connection_info().clone(),
        String::from(master_name),
        None,
        redis::sentinel::SentinelServerType::Master,
    )
    .unwrap();

    let sentinel_client = master_client.get_sentinel_client();
    let err = sentinel_client.expect_err("Expected an error");
    assert_eq!(
        err,
        RedisError::from((
            ErrorKind::InvalidClientConfig,
            "Couldn't open connection to a sentinel node."
        ))
    );
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
            .map(|sentinel| sentinel.addr().clone()),
        String::from(master_name),
        redis::sentinel::SentinelServerType::Master,
    )
    .unwrap();

    let mut replica_client_builder = SentinelClientBuilder::new(
        context
            .sentinels_connection_info
            .iter()
            .map(|sentinel| sentinel.addr().clone()),
        String::from(master_name),
        redis::sentinel::SentinelServerType::Replica,
    )
    .unwrap();

    master_client_builder = master_client_builder.set_client_to_sentinel_protocol(use_protocol());

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
    use super::*;
    use redis::{AsyncConnectionConfig, aio::MultiplexedConnection};
    use rstest::rstest;
    use test_macros::async_test;

    use crate::{assert_is_master_role, assert_replica_role_and_master_addr};

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
                !replica_conn_infos.contains(replica_client.get_connection_info().addr()),
                "pushing {:?} into {:?}",
                replica_client.get_connection_info().addr(),
                replica_conn_infos
            );
            replica_conn_infos.push(replica_client.get_connection_info().addr().clone());

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

            assert!(replica_conn_infos.contains(replica_client.get_connection_info().addr()));

            async_assert_connection_is_replica_of_correct_master(&mut replica_con, master_client)
                .await;
        }
    }

    #[async_test]
    async fn sentinel_connect_to_random_replica_async() {
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, 3, 3);
        let node_conn_info = context.sentinel_node_connection_info();
        let sentinel = context.sentinel_mut();

        let master_client = sentinel
            .async_master_for(master_name, Some(&node_conn_info))
            .await
            .unwrap();
        let mut master_con = master_client
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let mut replica_con = sentinel
            .async_replica_for(master_name, Some(&node_conn_info))
            .await
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        async_assert_is_connection_to_master(&mut master_con).await;
        async_assert_connection_is_replica_of_correct_master(&mut replica_con, &master_client)
            .await;
    }

    #[async_test]
    async fn sentinel_connect_to_multiple_replicas_async() {
        let number_of_replicas = 3;
        let master_name = "master1";
        let mut cluster = TestSentinelContext::new(2, number_of_replicas, 3);
        let node_conn_info = cluster.sentinel_node_connection_info();
        let sentinel = cluster.sentinel_mut();

        let master_client = sentinel
            .async_master_for(master_name, Some(&node_conn_info))
            .await
            .unwrap();
        let mut master_con = master_client
            .get_multiplexed_async_connection()
            .await
            .unwrap();

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
    }

    #[async_test]
    async fn sentinel_server_down_async() {
        let number_of_replicas = 3;
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, number_of_replicas, 3);
        let node_conn_info = context.sentinel_node_connection_info();

        let sentinel = context.sentinel_mut();

        let master_client = sentinel
            .async_master_for(master_name, Some(&node_conn_info))
            .await
            .unwrap();
        let mut master_con = master_client
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        async_assert_is_connection_to_master(&mut master_con).await;

        context.cluster.sentinel_servers[0].stop();
        std::thread::sleep(Duration::from_millis(25));

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
    }

    #[async_test]
    async fn sentinel_redis_client_async() {
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

        let mut master_con = master_client.get_async_connection().await.unwrap();

        async_assert_is_connection_to_master(&mut master_con).await;

        let node_conn_info = context.sentinel_node_connection_info();
        let sentinel = context.sentinel_mut();
        let master_client = sentinel
            .async_master_for(master_name, Some(&node_conn_info))
            .await
            .unwrap();

        // Read commands to the replica node
        for _ in 0..20 {
            let mut replica_con = replica_client.get_async_connection().await.unwrap();

            async_assert_connection_is_replica_of_correct_master(&mut replica_con, &master_client)
                .await;
        }
    }

    #[async_test]
    async fn sentinel_client_async() {
        let master_name = "master1";
        let context = TestSentinelContext::new(2, 3, 3);
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

        let sentinel_client_1 = master_client.async_get_sentinel_client().await.unwrap();
        let sentinel_client_2 = replica_client.async_get_sentinel_client().await.unwrap();
        let first_configured_sentinel = context.sentinels_connection_info.first().unwrap();

        assert_eq!(
            sentinel_client_1.get_connection_info().addr(),
            sentinel_client_2.get_connection_info().addr()
        );

        assert_eq!(
            first_configured_sentinel.addr(),
            sentinel_client_2.get_connection_info().addr()
        );
    }

    #[async_test]
    async fn sentinel_client_async_not_sentinel_error() {
        let master_name = "master1";

        let mut context = TestSentinelContext::new(2, 3, 3);
        // Change the context with incorrect sentinel servers
        context.sentinels_connection_info = context
            .cluster
            .servers
            .iter()
            .map(|redis_server| redis_server.connection_info().clone())
            .collect::<Vec<_>>();
        let mut master_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from(master_name),
            Some(context.sentinel_node_connection_info()),
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();

        let sentinel_client = master_client.async_get_sentinel_client().await;
        let err = sentinel_client.expect_err("Expected an error");
        assert_eq!(
            err,
            RedisError::from((
                ErrorKind::InvalidClientConfig,
                "Couldn't open connection to a sentinel node."
            ))
        );
    }

    #[async_test]
    async fn sentinel_client_async_io_error() {
        let master_name = "master1";

        let mut master_client = SentinelClient::build(
            vec!["redis://test:6379"],
            String::from(master_name),
            None,
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();

        let sentinel_client = master_client.async_get_sentinel_client().await;
        let err = sentinel_client.expect_err("Expected an error");
        assert!(err.is_io_error());
    }

    #[async_test]
    async fn sentinel_client_async_with_connection_timeout() {
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
            AsyncConnectionConfig::new().set_connection_timeout(Some(Duration::from_secs(1)));

        let mut master_con = master_client
            .get_async_connection_with_config(&connection_options)
            .await
            .unwrap();

        async_assert_is_connection_to_master(&mut master_con).await;

        let node_conn_info = context.sentinel_node_connection_info();
        let sentinel = context.sentinel_mut();
        let master_client = sentinel
            .async_master_for(master_name, Some(&node_conn_info))
            .await
            .unwrap();

        // Read commands to the replica node
        for _ in 0..20 {
            let mut replica_con = replica_client
                .get_async_connection_with_config(&connection_options)
                .await
                .unwrap();

            async_assert_connection_is_replica_of_correct_master(&mut replica_con, &master_client)
                .await;
        }
    }

    #[async_test]
    async fn sentinel_client_async_with_response_timeout() {
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

        let connection_options = AsyncConnectionConfig::new();

        let mut master_con = master_client
            .get_async_connection_with_config(&connection_options)
            .await
            .unwrap();

        async_assert_is_connection_to_master(&mut master_con).await;

        let node_conn_info = context.sentinel_node_connection_info();
        let sentinel = context.sentinel_mut();
        let master_client = sentinel
            .async_master_for(master_name, Some(&node_conn_info))
            .await
            .unwrap();

        // Read commands to the replica node
        for _ in 0..20 {
            let mut replica_con = replica_client
                .get_async_connection_with_config(&connection_options)
                .await
                .unwrap();

            async_assert_connection_is_replica_of_correct_master(&mut replica_con, &master_client)
                .await;
        }
    }

    #[async_test]
    async fn sentinel_client_async_with_timeouts() {
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

        let connection_options = AsyncConnectionConfig::new();

        let mut master_con = master_client
            .get_async_connection_with_config(&connection_options)
            .await
            .unwrap();

        async_assert_is_connection_to_master(&mut master_con).await;

        let node_conn_info = context.sentinel_node_connection_info();
        let sentinel = context.sentinel_mut();
        let master_client = sentinel
            .async_master_for(master_name, Some(&node_conn_info))
            .await
            .unwrap();

        // Read commands to the replica node
        for _ in 0..20 {
            let mut replica_con = replica_client
                .get_async_connection_with_config(&connection_options)
                .await
                .unwrap();

            async_assert_connection_is_replica_of_correct_master(&mut replica_con, &master_client)
                .await;
        }
    }

    #[async_test]
    async fn get_replica_clients_success_async() {
        let number_of_replicas = 3;
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, number_of_replicas, 3);
        let node_conn_info = context.sentinel_node_connection_info();
        let sentinel = context.sentinel_mut();

        let replicas = sentinel
            .async_get_replica_clients(master_name, Some(&node_conn_info))
            .await
            .unwrap();
        assert_eq!(replicas.len(), number_of_replicas as usize);
    }

    #[async_test]
    async fn get_replica_clients_invalid_master_name_async() {
        let mut context = TestSentinelContext::new(2, 2, 3);
        let node_conn_info = context.sentinel_node_connection_info();
        let sentinel = context.sentinel_mut();

        let err = sentinel
            .async_get_replica_clients("invalid_master_name", Some(&node_conn_info))
            .await
            .unwrap_err();
        // sometimes we get unexplained connection refused errors.
        assert!(
            matches!(
                err.kind(),
                ErrorKind::Server(redis::ServerErrorKind::ResponseError) | ErrorKind::Io
            ),
            "{err}"
        );
    }

    #[async_test]
    async fn get_replica_clients_report_correct_master_async() {
        let number_of_replicas = 3;
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, number_of_replicas, 3);
        let node_conn_info = context.sentinel_node_connection_info();
        let sentinel = context.sentinel_mut();

        let master_client = sentinel
            .async_master_for(master_name, Some(&node_conn_info))
            .await
            .unwrap();
        let replicas = sentinel
            .async_get_replica_clients(master_name, Some(&node_conn_info))
            .await
            .unwrap();

        for replica_client in replicas {
            let mut con = replica_client
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            async_assert_connection_is_replica_of_correct_master(&mut con, &master_client).await;
        }
    }

    #[async_test]
    async fn get_replica_clients_with_one_replica_down_async() {
        let number_of_replicas = 3;
        let master_name = "master0";
        let mut context = TestSentinelContext::new(2, number_of_replicas, 3);
        let node_conn_info = context.sentinel_node_connection_info();
        let sentinel_conn_info = context.sentinels_connection_info()[0].clone();

        let mut conn = Client::open(sentinel_conn_info)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        redis::cmd("SENTINEL")
            .arg("set")
            .arg(master_name)
            .arg("down-after-milliseconds")
            .arg("500")
            .query_async::<()>(&mut conn)
            .await
            .unwrap();

        context.cluster.servers[1].stop();
        std::thread::sleep(std::time::Duration::from_millis(800));

        let sentinel = context.sentinel_mut();
        let replicas = sentinel
            .async_get_replica_clients(master_name, Some(&node_conn_info))
            .await
            .expect("Failed to get replicas");

        assert_eq!(
            replicas.len(),
            (number_of_replicas - 1) as usize,
            "Unexpected num of replicas total"
        );
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
