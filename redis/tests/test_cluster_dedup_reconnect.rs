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

    // Keep the environment alive so the handler stays installed for the whole test.
    let mut env = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")])
            .retries(3)
            .min_retry_wait(1)
            .max_retry_wait(2)
            .retry_wait_formula(1, 1),
        name,
        {
            let phase = phase.clone();
            move |cmd: &[u8], port: u16| {
                // Pass startup responses through to the mock as Value/RedisError
                if let Err(res) = respond_startup(name, cmd) {
                    return match res {
                        Ok(v) => Err(Ok(v)),
                        Err(e) => Err(Err(e)),
                    };
                }

                // Simulate app-level command
                if contains_slice(cmd, b"ECHO") && port == 6379 {
                    match phase.fetch_add(1, Ordering::SeqCst) {
                        0 | 1 => {
                            disconnects_cl.fetch_add(1, Ordering::Relaxed);
                            // Simulate a disconnect via an IO error that should trigger reconnect logic
                            Err(Err(std::io::Error::new(
                                std::io::ErrorKind::ConnectionReset,
                                "disconnect",
                            )
                            .into()))
                        }
                        _ => Err(Ok(Value::BulkString(b"PONG".to_vec()))),
                    }
                } else if contains_slice(cmd, b"PING") {
                    // Validate new connection PINGs
                    conn_count.fetch_add(1, Ordering::Relaxed);
                    Err(Ok(Value::SimpleString("OK".into())))
                } else {
                    // Default noop
                    Err(Ok(Value::Nil))
                }
            }
        },
    );
    // Bind references so the handler remains installed via env's lifetime
    let runtime = &env.runtime;
    let connection = &mut env.async_connection;

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
