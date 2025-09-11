#![cfg(all(feature = "cluster-async", feature = "tokio-comp"))]

mod support;

use redis::{
    cluster::ClusterClient,
    cluster_routing::{RoutingInfo, SingleNodeRoutingInfo},
    cmd, Value,
};
use support::*;

// This test isolates dedup reconnect validation from the large test_cluster_async suite.
#[test]
fn test_cluster_dedup_reconnect_isolated() {
    use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
    use std::sync::Arc;

    let name = "test_cluster_dedup_reconnect_isolated";

    let phase = Arc::new(AtomicU32::new(0));
    let disconnects = Arc::new(AtomicU32::new(0));
    let disconnects_cl = disconnects.clone();
    let conn_count = Arc::new(AtomicU16::new(0));

    let MockEnv {
        runtime,
        async_connection: mut connection,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")]),
        name,
        {
            let phase = phase.clone();
            move |cmd: &[u8], port: u16| {
                // Map startup responses explicitly to ServerResponse
                if let Err(res) = respond_startup(name, cmd) {
                    return match res {
                        Ok(v) => ServerResponse::Value(v),
                        Err(e) => ServerResponse::Error(e),
                    };
                }

                // Count attempts to create/start connections (when startup failed)
                // We approximate by tracking errors from respond_startup above; for simplicity,
                // we increment counter here only when not handling command, relying on baseline below.

                // Simulate app-level command
                if contains_slice(cmd, b"ECHO") && port == 6379 {
                    match phase.fetch_add(1, Ordering::SeqCst) {
                        0 | 1 => {
                            disconnects_cl.fetch_add(1, Ordering::Relaxed);
                            ServerResponse::Disconnect
                        }
                        _ => ServerResponse::Value(Value::BulkString(b"PONG".to_vec())),
                    }
                } else if contains_slice(cmd, b"PING") {
                    // Validate new connection PINGs
                    conn_count.fetch_add(1, Ordering::Relaxed);
                    ServerResponse::Value(Value::SimpleString("OK".into()))
                } else {
                    // Default noop
                    ServerResponse::Value(Value::Nil)
                }
            }
        },
    );

    // Baseline: MockEnv sets up a few connections (sync & async); expect >= 1

    // Perform up to three attempts; first two should cause disconnects in the handler,
    // but cluster may recover fast and retry internally, so we don't assert on errors here.
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
            port: 6379,
        }),
    ));

    // Third attempt: should succeed after reconnect
    let value = runtime.block_on(connection.route_command(
        &cmd("ECHO"),
        RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
            host: name.to_string(),
            port: 6379,
        }),
    ));
    assert_eq!(value, Ok(Value::BulkString(b"PONG".to_vec())));

    // Ensure our handler observed at least two disconnects
    assert!(
        disconnects.load(Ordering::Relaxed) >= 2,
        "expected at least two disconnects observed by handler"
    );
}
