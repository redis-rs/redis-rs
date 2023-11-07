#![cfg(feature = "sentinel")]
mod support;

use std::collections::HashMap;

use redis::{
    sentinel::{Sentinel, SentinelClient, SentinelNodeConnectionInfo},
    Client, Connection, ConnectionAddr, ConnectionInfo,
};

use crate::support::*;

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
fn test_sentinel_connect_to_random_replica() {
    let master_name = "master1";
    let mut context = TestSentinelContext::new(2, 3, 3);
    let node_conn_info: SentinelNodeConnectionInfo = context.sentinel_node_connection_info();
    let sentinel = context.sentinel_mut();

    let master_client = sentinel
        .master_for(master_name, Some(&node_conn_info))
        .unwrap();
    let mut master_con = master_client.get_connection().unwrap();

    let mut replica_con = sentinel
        .replica_for(master_name, Some(&node_conn_info))
        .unwrap()
        .get_connection()
        .unwrap();

    assert_is_connection_to_master(&mut master_con);
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

    assert_is_connection_to_master(&mut master_con);

    let node_conn_info = context.sentinel_node_connection_info();
    let sentinel = context.sentinel_mut();
    let master_client = sentinel
        .master_for(master_name, Some(&node_conn_info))
        .unwrap();

    for _ in 0..20 {
        let mut replica_con = replica_client.get_connection().unwrap();

        assert_connection_is_replica_of_correct_master(&mut replica_con, &master_client);
    }
}

#[cfg(feature = "aio")]
pub mod async_tests {
    use redis::{
        aio::MultiplexedConnection,
        sentinel::{Sentinel, SentinelClient, SentinelNodeConnectionInfo},
        Client, ConnectionAddr, RedisError,
    };

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

    #[test]
    fn test_sentinel_connect_to_random_replica_async() {
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, 3, 3);
        let node_conn_info = context.sentinel_node_connection_info();
        let sentinel = context.sentinel_mut();

        block_on_all(async move {
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
            async_assert_connection_is_replica_of_correct_master(&mut replica_con, &master_client)
                .await;

            Ok::<(), RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_sentinel_connect_to_multiple_replicas_async() {
        let number_of_replicas = 3;
        let master_name = "master1";
        let mut cluster = TestSentinelContext::new(2, number_of_replicas, 3);
        let node_conn_info = cluster.sentinel_node_connection_info();
        let sentinel = cluster.sentinel_mut();

        block_on_all(async move {
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
        })
        .unwrap();
    }

    #[test]
    fn test_sentinel_server_down_async() {
        let number_of_replicas = 3;
        let master_name = "master1";
        let mut context = TestSentinelContext::new(2, number_of_replicas, 3);
        let node_conn_info = context.sentinel_node_connection_info();

        block_on_all(async move {
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
        })
        .unwrap();
    }

    #[test]
    fn test_sentinel_client_async() {
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

        block_on_all(async move {
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
        })
        .unwrap();
    }
}
