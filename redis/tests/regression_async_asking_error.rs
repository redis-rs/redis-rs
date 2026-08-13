#![cfg(feature = "cluster-async")]

mod support;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use redis::{Value, cluster::ClusterClient, parse_redis_value};

use crate::support::{MockEnv, contains_slice, respond_startup};

#[test]
fn async_ask_redirect_propagates_asking_failure() {
    let name = "async_ask_redirect_propagates_asking_failure";
    let redirected_command_sent = Arc::new(AtomicBool::new(false));
    let redirected_command_sent_in_handler = Arc::clone(&redirected_command_sent);

    let MockEnv {
        runtime,
        async_connection: mut connection,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(0),
        name,
        move |cmd, port| {
            respond_startup(name, cmd)?;

            match port {
                6379 if contains_slice(cmd, b"GET") => Err(Ok(parse_redis_value(
                    format!("-ASK 123 {name}:6380\r\n").as_bytes(),
                )
                .unwrap())),
                6380 if contains_slice(cmd, b"ASKING") => {
                    Err(Ok(parse_redis_value(b"-ERR ASKING failed\r\n").unwrap()))
                }
                6380 if contains_slice(cmd, b"GET") => {
                    redirected_command_sent_in_handler.store(true, Ordering::SeqCst);
                    Err(Ok(Value::BulkString(b"unexpected-success".to_vec())))
                }
                _ => panic!(
                    "unexpected command on port {port}: {}",
                    String::from_utf8_lossy(cmd)
                ),
            }
        },
    );

    let result = runtime.block_on(
        redis::cmd("GET")
            .arg("key")
            .query_async::<String>(&mut connection),
    );

    assert!(result.is_err(), "ASKING failure must be propagated");
    assert!(
        !redirected_command_sent.load(Ordering::SeqCst),
        "the redirected command must not be sent when ASKING fails"
    );
}
