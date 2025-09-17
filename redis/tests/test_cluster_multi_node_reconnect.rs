#![cfg(all(feature = "cluster-async", feature = "tokio-comp"))]

mod support;

use redis::{
    cluster::ClusterClient,
    cluster_routing::{RoutingInfo, SingleNodeRoutingInfo},
    cmd, Value,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use support::*;

#[test]
fn test_cluster_multi_node_disconnect_reconnects_successfully() {
    let name = "test_cluster_multi_node_disconnect_reconnects_successfully";

    let disconnect_node_1 = Arc::new(AtomicBool::new(true));
    let disconnect_node_2 = Arc::new(AtomicBool::new(true));

    // Keep the environment alive for the test duration so the handler isn't dropped.
    let mut env = MockEnv::with_client_builder(
        ClusterClient::builder(vec![
            &*format!("redis://{name}:6379"),
            &*format!("redis://{name}:6380"),
            &*format!("redis://{name}:6381"),
        ])
        .retries(3)
        .min_retry_wait(1)
        .max_retry_wait(2)
        .retry_wait_formula(1, 1),
        name,
        {
            let d1 = disconnect_node_1.clone();
            let d2 = disconnect_node_2.clone();
            move |cmd: &[u8], port: u16| {
                let slots_config = Some(vec![
                    MockSlotRange {
                        primary_port: 6379,
                        replica_ports: vec![],
                        slot_range: (0..5000),
                    },
                    MockSlotRange {
                        primary_port: 6380,
                        replica_ports: vec![],
                        slot_range: (5001..10000),
                    },
                    MockSlotRange {
                        primary_port: 6381,
                        replica_ports: vec![],
                        slot_range: (10001..16383),
                    },
                ]);
                if let Err(res) = respond_startup_with_replica_using_config(name, cmd, slots_config)
                {
                    return match res {
                        Ok(v) => Err(Ok(v)),
                        Err(e) => Err(Err(e)),
                    };
                }

                if contains_slice(cmd, b"ECHO") {
                    if port == 6379 && d1.swap(false, Ordering::SeqCst) {
                        return Err(Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "disconnect",
                        )
                        .into()));
                    }
                    if port == 6380 && d2.swap(false, Ordering::SeqCst) {
                        return Err(Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "disconnect",
                        )
                        .into()));
                    }
                }
                Err(Ok(Value::BulkString(b"PONG".to_vec())))
            }
        },
    );
    // Bind references to runtime and connection from the env so the handler stays alive
    let runtime = &env.runtime;
    let connection = &mut env.async_connection;

    // Send commands to all three nodes. The first two are mocked to disconnect.
    // Due to the speed of the reconnect, we don't assert on the result of these calls,
    // as they might succeed if the reconnect logic is fast enough.
    let _ = runtime.block_on(connection.route_command(
        &cmd("ECHO"),
        RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
            host: name.to_string(),
            port: 6379,
        }),
    ));
    let _ = runtime.block_on(connection.route_command(
        &cmd("ECHO"),
        RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
            host: name.to_string(),
            port: 6380,
        }),
    ));
    let _ = runtime.block_on(connection.route_command(
        &cmd("ECHO"),
        RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
            host: name.to_string(),
            port: 6381,
        }),
    ));

    // Subsequent commands should succeed on all nodes.
    // Allow a brief window for background reconnect to complete by retrying quickly.
    fn retry_echo(
        runtime: &tokio::runtime::Runtime,
        connection: &mut redis::cluster_async::ClusterConnection<MockConnection>,
        host: &str,
        port: u16,
    ) -> redis::RedisResult<Value> {
        use std::thread::sleep;
        use std::time::Duration;
        for _ in 0..10 {
            let res = runtime.block_on(connection.route_command(
                &cmd("ECHO"),
                RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                    host: host.to_string(),
                    port,
                }),
            ));
            if res == Ok(Value::BulkString(b"PONG".to_vec())) {
                return res;
            }
            sleep(Duration::from_millis(10));
        }
        runtime.block_on(connection.route_command(
            &cmd("ECHO"),
            RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                host: host.to_string(),
                port,
            }),
        ))
    }

    let res1_retry = retry_echo(runtime, connection, name, 6379);
    let res2_retry = retry_echo(runtime, connection, name, 6380);
    let res3_retry = retry_echo(runtime, connection, name, 6381);

    assert_eq!(res1_retry, Ok(Value::BulkString(b"PONG".to_vec())));
    assert_eq!(res2_retry, Ok(Value::BulkString(b"PONG".to_vec())));
    assert_eq!(res3_retry, Ok(Value::BulkString(b"PONG".to_vec())));
}
