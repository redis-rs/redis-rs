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

    let MockEnv {
        runtime,
        async_connection: mut connection,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![
            &*format!("redis://{name}:6379"),
            &*format!("redis://{name}:6380"),
            &*format!("redis://{name}:6381"),
        ]),
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
                        Ok(v) => ServerResponse::Value(v),
                        Err(e) => ServerResponse::Error(e),
                    };
                }

                if contains_slice(cmd, b"ECHO") {
                    if port == 6379 && d1.swap(false, Ordering::SeqCst) {
                        return ServerResponse::Disconnect;
                    }
                    if port == 6380 && d2.swap(false, Ordering::SeqCst) {
                        return ServerResponse::Disconnect;
                    }
                }
                ServerResponse::Value(Value::BulkString(b"PONG".to_vec()))
            }
        },
    );

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
    let res1_retry = runtime.block_on(connection.route_command(
        &cmd("ECHO"),
        RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
            host: name.to_string(),
            port: 6379,
        }),
    ));
    let res2_retry = runtime.block_on(connection.route_command(
        &cmd("ECHO"),
        RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
            host: name.to_string(),
            port: 6380,
        }),
    ));
    let res3_retry = runtime.block_on(connection.route_command(
        &cmd("ECHO"),
        RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
            host: name.to_string(),
            port: 6381,
        }),
    ));

    assert_eq!(res1_retry, Ok(Value::BulkString(b"PONG".to_vec())));
    assert_eq!(res2_retry, Ok(Value::BulkString(b"PONG".to_vec())));
    assert_eq!(res3_retry, Ok(Value::BulkString(b"PONG".to_vec())));
}
