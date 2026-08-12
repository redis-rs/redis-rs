#![cfg(feature = "cluster-async")]

mod support;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use redis::{Value, cluster::ClusterClient, parse_redis_value};

use crate::support::{MockEnv, contains_slice, is_connection_check};

fn cluster_slots(name: &str, primary_port: u16) -> Value {
    Value::Array(vec![Value::Array(vec![
        Value::Int(0),
        Value::Int(16383),
        Value::Array(vec![
            Value::BulkString(name.as_bytes().to_vec()),
            Value::Int(primary_port as i64),
        ]),
    ])])
}

#[test]
fn nested_redirects_are_fully_reset_before_slot_refresh_retry() {
    let name = "nested_redirects_are_fully_reset_before_slot_refresh_retry";
    let refreshed = Arc::new(AtomicBool::new(false));
    let stale_route_used = Arc::new(AtomicBool::new(false));
    let refreshed_in_handler = Arc::clone(&refreshed);
    let stale_route_used_in_handler = Arc::clone(&stale_route_used);

    let MockEnv {
        runtime,
        async_connection: mut connection,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(4),
        name,
        move |cmd, port| {
            if is_connection_check(cmd) {
                return Err(Ok(Value::SimpleString("OK".into())));
            }

            if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                let primary_port = if refreshed_in_handler.load(Ordering::SeqCst) {
                    6382
                } else {
                    6379
                };
                return Err(Ok(cluster_slots(name, primary_port)));
            }

            if refreshed_in_handler.load(Ordering::SeqCst) && port != 6382 {
                stale_route_used_in_handler.store(true, Ordering::SeqCst);
                return Err(Ok(
                    parse_redis_value(b"-ERR stale redirect reused after refresh\r\n").unwrap(),
                ));
            }

            match port {
                6379 if contains_slice(cmd, b"GET") => Err(Ok(
                    parse_redis_value(format!("-ASK 123 {name}:6380\r\n").as_bytes()).unwrap(),
                )),
                6380 if contains_slice(cmd, b"ASKING") => {
                    Err(Ok(Value::SimpleString("OK".into())))
                }
                6380 if contains_slice(cmd, b"GET") => Err(Ok(
                    parse_redis_value(format!("-ASK 123 {name}:6381\r\n").as_bytes()).unwrap(),
                )),
                6381 if contains_slice(cmd, b"ASKING") => {
                    Err(Ok(Value::SimpleString("OK".into())))
                }
                6381 if contains_slice(cmd, b"GET") => {
                    refreshed_in_handler.store(true, Ordering::SeqCst);
                    Err(Ok(parse_redis_value(
                        b"-READONLY You can't write against a read only replica.\r\n",
                    )
                    .unwrap()))
                }
                6382 if contains_slice(cmd, b"GET") => {
                    Err(Ok(Value::BulkString(b"ok".to_vec())))
                }
                _ => panic!(
                    "unexpected command on port {port}: {}",
                    String::from_utf8_lossy(cmd)
                ),
            }
        },
    );

    let value = runtime
        .block_on(
            redis::cmd("GET")
                .arg("key")
                .query_async::<String>(&mut connection),
        )
        .expect("request should be rerouted through the refreshed slot map");

    assert_eq!(value, "ok");
    assert!(
        !stale_route_used.load(Ordering::SeqCst),
        "a nested redirect survived reset_routing and bypassed the refreshed slot map"
    );
}
