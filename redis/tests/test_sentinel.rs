mod support;

use std::collections::HashMap;

use redis::{sentinel::SentinelClient, Client, Connection, ConnectionAddr};

use crate::support::*;

fn parse_replication_info(value: &str) -> HashMap<&str, &str> {
    let info_map: std::collections::HashMap<&str, &str> = value
        .split("\r\n")
        .filter(|line| !line.trim_start().starts_with('#'))
        .filter_map(|line| line.split_once(':'))
        .collect();
    info_map
}

fn check_master_role(conn: &mut Connection) {
    let info: String = redis::cmd("INFO").arg("REPLICATION").query(conn).unwrap();

    let info_map = parse_replication_info(&info);
    assert_eq!(info_map.get("role"), Some(&"master"));
}

fn check_replica_info_result(info: String, master_client: &Client) {
    let info_map = parse_replication_info(&info);

    assert_eq!(info_map.get("role"), Some(&"slave"));

    let (master_host, master_port) = match &master_client.get_connection_info().addr {
        ConnectionAddr::Tcp(host, port) => (host, port),
        ConnectionAddr::TcpTls {
            host,
            port,
            insecure: _,
        } => (host, port),
        ConnectionAddr::Unix(..) => panic!("Unexpected master connection type"),
    };

    assert_eq!(info_map.get("master_host"), Some(&master_host.as_str()));
    assert_eq!(
        info_map.get("master_port"),
        Some(&master_port.to_string().as_str())
    );
}

fn check_replica_role_and_master(conn: &mut Connection, master_client: &Client) {
    let info: String = redis::cmd("INFO").arg("REPLICATION").query(conn).unwrap();
    check_replica_info_result(info, master_client);
}

#[test]
fn test_sentinel_basics() {
    let cluster = TestSentinelContext::new(3, 1, 3);
    let sentinel = cluster.sentinel();

    let mut master_con = sentinel
        .master_for("master1")
        .unwrap()
        .get_connection()
        .unwrap();

    check_master_role(&mut master_con);
}

#[test]
fn test_sentinel_read_from_random_replica() {
    let cluster = TestSentinelContext::new(3, 1, 3);
    let sentinel = cluster.sentinel();

    let master_client = sentinel.master_for("master1").unwrap();
    let mut master_con = master_client.get_connection().unwrap();

    let mut replica_con = sentinel
        .replica_for("master1")
        .unwrap()
        .get_connection()
        .unwrap();

    check_master_role(&mut master_con);
    check_replica_role_and_master(&mut replica_con, &master_client);
}

#[test]
fn test_sentinel_read_from_multiple_replicas() {
    let mut cluster = TestSentinelContext::new(3, 1, 3);
    let sentinel = cluster.sentinel_mut();

    let master_client = sentinel.master_for("master1").unwrap();
    let mut master_con = master_client.get_connection().unwrap();

    check_master_role(&mut master_con);

    for _ in 0..20 {
        let mut replica_con = sentinel
            .replica_rotate_for("master1")
            .unwrap()
            .get_connection()
            .unwrap();

        check_replica_role_and_master(&mut replica_con, &master_client);
    }
}

#[test]
fn test_sentinel_server_down() {
    let mut context = TestSentinelContext::new(3, 1, 3);
    let sentinel = context.sentinel_mut();

    let master_client = sentinel.master_for("master1").unwrap();
    let mut master_con = master_client.get_connection().unwrap();

    check_master_role(&mut master_con);

    context.cluster.sentinel_servers[0].stop();
    std::thread::sleep(std::time::Duration::from_millis(25));

    let sentinel = context.sentinel_mut();

    for _ in 0..20 {
        let mut replica_con = sentinel
            .replica_rotate_for("master1")
            .unwrap()
            .get_connection()
            .unwrap();

        check_replica_role_and_master(&mut replica_con, &master_client);
    }
}

#[test]
fn test_sentinel_client() {
    let mut context = TestSentinelContext::new(3, 1, 3);
    let master_client = SentinelClient::build(
        context.sentinels_connection_info().clone(),
        String::from("master1"),
        redis::sentinel::SentinelServerType::Master,
    )
    .unwrap();

    let replica_client = SentinelClient::build(
        context.sentinels_connection_info().clone(),
        String::from("master1"),
        redis::sentinel::SentinelServerType::Replica,
    )
    .unwrap();

    let mut master_con = master_client.get_connection().unwrap();

    check_master_role(&mut master_con);

    let sentinel = context.sentinel_mut();
    let master_client = sentinel.master_for("master1").unwrap();

    for _ in 0..20 {
        let mut replica_con = replica_client.get_connection().unwrap();

        check_replica_role_and_master(&mut replica_con, &master_client);
    }
}

#[cfg(feature = "aio")]
pub mod async_tests {
    use redis::{aio::Connection, sentinel::SentinelClient, Client, RedisError};

    use crate::{check_replica_info_result, parse_replication_info, support::*};

    async fn async_check_master_role(conn: &mut Connection) {
        let info: String = redis::cmd("INFO")
            .arg("REPLICATION")
            .query_async(conn)
            .await
            .unwrap();

        let info_map = parse_replication_info(&info);
        assert_eq!(info_map.get("role"), Some(&"master"));
    }

    async fn async_check_replica_role_and_master(conn: &mut Connection, master_client: &Client) {
        let info: String = redis::cmd("INFO")
            .arg("REPLICATION")
            .query_async(conn)
            .await
            .unwrap();

        check_replica_info_result(info, master_client);
    }

    #[test]
    fn test_sentinel_basics_async() {
        let cluster = TestSentinelContext::new(3, 1, 3);
        let sentinel = cluster.sentinel();

        block_on_all(async move {
            let mut master_con = sentinel
                .async_master_for("master1")
                .await?
                .get_async_connection()
                .await?;

            async_check_master_role(&mut master_con).await;

            Ok::<(), RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_sentinel_read_from_random_replica_async() {
        let cluster = TestSentinelContext::new(3, 1, 3);
        let sentinel = cluster.sentinel();

        block_on_all(async move {
            let master_client = sentinel.async_master_for("master1").await?;
            let mut master_con = master_client.get_async_connection().await?;

            let mut replica_con = sentinel
                .async_replica_for("master1")
                .await?
                .get_async_connection()
                .await?;

            async_check_master_role(&mut master_con).await;
            async_check_replica_role_and_master(&mut replica_con, &master_client).await;

            Ok::<(), RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_sentinel_read_from_multiple_replicas_async() {
        let mut cluster = TestSentinelContext::new(3, 1, 3);
        let sentinel = cluster.sentinel_mut();

        block_on_all(async move {
            let master_client = sentinel.async_master_for("master1").await?;
            let mut master_con = master_client.get_async_connection().await?;

            async_check_master_role(&mut master_con).await;

            // Read commands to the replica node
            for _ in 0..20 {
                let mut replica_con = sentinel
                    .async_replica_rotate_for("master1")
                    .await?
                    .get_async_connection()
                    .await?;

                async_check_replica_role_and_master(&mut replica_con, &master_client).await;
            }

            Ok::<(), RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_sentinel_server_down_async() {
        let mut context = TestSentinelContext::new(3, 1, 3);

        block_on_all(async move {
            let sentinel = context.sentinel_mut();

            let master_client = sentinel.async_master_for("master1").await?;
            let mut master_con = master_client.get_async_connection().await?;

            async_check_master_role(&mut master_con).await;

            context.cluster.sentinel_servers[0].stop();
            std::thread::sleep(std::time::Duration::from_millis(25));

            let sentinel = context.sentinel_mut();

            // Read commands to the replica node
            for _ in 0..20 {
                let mut replica_con = sentinel
                    .async_replica_rotate_for("master1")
                    .await?
                    .get_async_connection()
                    .await?;

                async_check_replica_role_and_master(&mut replica_con, &master_client).await;
            }

            Ok::<(), RedisError>(())
        })
        .unwrap();
    }

    #[test]
    fn test_sentinel_client_async() {
        let mut context = TestSentinelContext::new(3, 1, 3);
        let master_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from("master1"),
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();

        let replica_client = SentinelClient::build(
            context.sentinels_connection_info().clone(),
            String::from("master1"),
            redis::sentinel::SentinelServerType::Replica,
        )
        .unwrap();

        block_on_all(async move {
            let mut master_con = master_client.get_async_connection().await?;

            async_check_master_role(&mut master_con).await;

            let sentinel = context.sentinel_mut();
            let master_client = sentinel.async_master_for("master1").await?;

            // Read commands to the replica node
            for _ in 0..20 {
                let mut replica_con = replica_client.get_async_connection().await?;

                async_check_replica_role_and_master(&mut replica_con, &master_client).await;
            }

            Ok::<(), RedisError>(())
        })
        .unwrap();
    }
}
