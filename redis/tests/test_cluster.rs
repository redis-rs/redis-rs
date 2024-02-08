#![cfg(feature = "cluster")]
mod support;
use std::sync::{
    atomic::{self, AtomicI32, Ordering},
    Arc,
};

use crate::support::*;
use redis::{
    cluster::{cluster_pipe, ClusterClient},
    cmd, parse_redis_value, Commands, ConnectionLike, ErrorKind, RedisError, Value,
};

#[test]
fn test_cluster_basics() {
    let cluster = TestClusterContext::new(3, 0);
    let mut con = cluster.connection();

    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(&mut con);
    redis::cmd("SET").arg(&["{x}key2", "bar"]).execute(&mut con);

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["{x}key1", "{x}key2"])
            .query(&mut con),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

#[test]
fn test_cluster_with_username_and_password() {
    let cluster = TestClusterContext::new_with_cluster_client_builder(
        3,
        0,
        |builder| {
            builder
                .username(RedisCluster::username().to_string())
                .password(RedisCluster::password().to_string())
        },
        false,
    );
    cluster.disable_default_user();

    let mut con = cluster.connection();

    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(&mut con);
    redis::cmd("SET").arg(&["{x}key2", "bar"]).execute(&mut con);

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["{x}key1", "{x}key2"])
            .query(&mut con),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

#[test]
fn test_cluster_with_bad_password() {
    let cluster = TestClusterContext::new_with_cluster_client_builder(
        3,
        0,
        |builder| {
            builder
                .username(RedisCluster::username().to_string())
                .password("not the right password".to_string())
        },
        false,
    );
    assert!(cluster.client.get_connection().is_err());
}

#[test]
fn test_cluster_read_from_replicas() {
    let cluster = TestClusterContext::new_with_cluster_client_builder(
        6,
        1,
        |builder| builder.read_from_replicas(),
        false,
    );
    let mut con = cluster.connection();

    // Write commands would go to the primary nodes
    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(&mut con);
    redis::cmd("SET").arg(&["{x}key2", "bar"]).execute(&mut con);

    // Read commands would go to the replica nodes
    assert_eq!(
        redis::cmd("MGET")
            .arg(&["{x}key1", "{x}key2"])
            .query(&mut con),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

#[test]
fn test_cluster_eval() {
    let cluster = TestClusterContext::new(3, 0);
    let mut con = cluster.connection();

    let rv = redis::cmd("EVAL")
        .arg(
            r#"
            redis.call("SET", KEYS[1], "1");
            redis.call("SET", KEYS[2], "2");
            return redis.call("MGET", KEYS[1], KEYS[2]);
        "#,
        )
        .arg("2")
        .arg("{x}a")
        .arg("{x}b")
        .query(&mut con);

    assert_eq!(rv, Ok(("1".to_string(), "2".to_string())));
}

#[test]
fn test_cluster_multi_shard_commands() {
    let cluster = TestClusterContext::new(3, 0);

    let mut connection = cluster.connection();

    let res: String = connection
        .mset(&[("foo", "bar"), ("bar", "foo"), ("baz", "bazz")])
        .unwrap();
    assert_eq!(res, "OK");
    let res: Vec<String> = connection.mget(&["baz", "foo", "bar"]).unwrap();
    assert_eq!(res, vec!["bazz", "bar", "foo"]);
}

#[test]
#[cfg(feature = "script")]
fn test_cluster_script() {
    let cluster = TestClusterContext::new(3, 0);
    let mut con = cluster.connection();

    let script = redis::Script::new(
        r#"
        redis.call("SET", KEYS[1], "1");
        redis.call("SET", KEYS[2], "2");
        return redis.call("MGET", KEYS[1], KEYS[2]);
    "#,
    );

    let rv = script.key("{x}a").key("{x}b").invoke(&mut con);
    assert_eq!(rv, Ok(("1".to_string(), "2".to_string())));
}

#[test]
fn test_cluster_pipeline() {
    let cluster = TestClusterContext::new(3, 0);
    cluster.wait_for_cluster_up();
    let mut con = cluster.connection();

    let resp = cluster_pipe()
        .cmd("SET")
        .arg("key_1")
        .arg(42)
        .query::<Vec<String>>(&mut con)
        .unwrap();

    assert_eq!(resp, vec!["OK".to_string()]);
}

#[test]
fn test_cluster_pipeline_multiple_keys() {
    use redis::FromRedisValue;
    let cluster = TestClusterContext::new(3, 0);
    cluster.wait_for_cluster_up();
    let mut con = cluster.connection();

    let resp = cluster_pipe()
        .cmd("HSET")
        .arg("hash_1")
        .arg("key_1")
        .arg("value_1")
        .cmd("ZADD")
        .arg("zset")
        .arg(1)
        .arg("zvalue_2")
        .query::<Vec<i64>>(&mut con)
        .unwrap();

    assert_eq!(resp, vec![1i64, 1i64]);

    let resp = cluster_pipe()
        .cmd("HGET")
        .arg("hash_1")
        .arg("key_1")
        .cmd("ZCARD")
        .arg("zset")
        .query::<Vec<redis::Value>>(&mut con)
        .unwrap();

    let resp_1: String = FromRedisValue::from_redis_value(&resp[0]).unwrap();
    assert_eq!(resp_1, "value_1".to_string());

    let resp_2: usize = FromRedisValue::from_redis_value(&resp[1]).unwrap();
    assert_eq!(resp_2, 1);
}

#[test]
fn test_cluster_pipeline_invalid_command() {
    let cluster = TestClusterContext::new(3, 0);
    cluster.wait_for_cluster_up();
    let mut con = cluster.connection();

    let err = cluster_pipe()
        .cmd("SET")
        .arg("foo")
        .arg(42)
        .ignore()
        .cmd(" SCRIPT kill ")
        .query::<()>(&mut con)
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "This command cannot be safely routed in cluster mode - ClientError: Command 'SCRIPT KILL' can't be executed in a cluster pipeline."
    );

    let err = cluster_pipe().keys("*").query::<()>(&mut con).unwrap_err();

    assert_eq!(
        err.to_string(),
        "This command cannot be safely routed in cluster mode - ClientError: Command 'KEYS' can't be executed in a cluster pipeline."
    );
}

#[test]
fn test_cluster_pipeline_command_ordering() {
    let cluster = TestClusterContext::new(3, 0);
    cluster.wait_for_cluster_up();
    let mut con = cluster.connection();
    let mut pipe = cluster_pipe();

    let mut queries = Vec::new();
    let mut expected = Vec::new();
    for i in 0..100 {
        queries.push(format!("foo{i}"));
        expected.push(format!("bar{i}"));
        pipe.set(&queries[i], &expected[i]).ignore();
    }
    pipe.execute(&mut con);

    pipe.clear();
    for q in &queries {
        pipe.get(q);
    }

    let got = pipe.query::<Vec<String>>(&mut con).unwrap();
    assert_eq!(got, expected);
}

#[test]
#[ignore] // Flaky
fn test_cluster_pipeline_ordering_with_improper_command() {
    let cluster = TestClusterContext::new(3, 0);
    cluster.wait_for_cluster_up();
    let mut con = cluster.connection();
    let mut pipe = cluster_pipe();

    let mut queries = Vec::new();
    let mut expected = Vec::new();
    for i in 0..10 {
        if i == 5 {
            pipe.cmd("hset").arg("foo").ignore();
        } else {
            let query = format!("foo{i}");
            let r = format!("bar{i}");
            pipe.set(&query, &r).ignore();
            queries.push(query);
            expected.push(r);
        }
    }
    pipe.query::<()>(&mut con).unwrap_err();

    std::thread::sleep(std::time::Duration::from_secs(5));

    pipe.clear();
    for q in &queries {
        pipe.get(q);
    }

    let got = pipe.query::<Vec<String>>(&mut con).unwrap();
    assert_eq!(got, expected);
}

#[test]
fn test_cluster_retries() {
    let name = "tryagain";

    let requests = atomic::AtomicUsize::new(0);
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(5),
        name,
        move |cmd: &[u8], _| {
            respond_startup(name, cmd)?;

            match requests.fetch_add(1, atomic::Ordering::SeqCst) {
                0..=4 => Err(parse_redis_value(b"-TRYAGAIN mock\r\n")),
                _ => Err(Ok(Value::Data(b"123".to_vec()))),
            }
        },
    );

    let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

    assert_eq!(value, Ok(Some(123)));
}

#[test]
fn test_cluster_exhaust_retries() {
    let name = "tryagain_exhaust_retries";

    let requests = Arc::new(atomic::AtomicUsize::new(0));

    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(2),
        name,
        {
            let requests = requests.clone();
            move |cmd: &[u8], _| {
                respond_startup(name, cmd)?;
                requests.fetch_add(1, atomic::Ordering::SeqCst);
                Err(parse_redis_value(b"-TRYAGAIN mock\r\n"))
            }
        },
    );

    let result = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

    match result {
        Ok(_) => panic!("result should be an error"),
        Err(e) => match e.kind() {
            ErrorKind::TryAgain => {}
            _ => panic!("Expected TryAgain but got {:?}", e.kind()),
        },
    }
    assert_eq!(requests.load(atomic::Ordering::SeqCst), 3);
}

#[test]
fn test_cluster_move_error_when_new_node_is_added() {
    let name = "rebuild_with_extra_nodes";

    let requests = atomic::AtomicUsize::new(0);
    let started = atomic::AtomicBool::new(false);
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::new(name, move |cmd: &[u8], port| {
        if !started.load(atomic::Ordering::SeqCst) {
            respond_startup(name, cmd)?;
        }
        started.store(true, atomic::Ordering::SeqCst);

        if contains_slice(cmd, b"PING") {
            return Err(Ok(Value::Status("OK".into())));
        }

        let i = requests.fetch_add(1, atomic::Ordering::SeqCst);

        match i {
            // Respond that the key exists on a node that does not yet have a connection:
            0 => Err(parse_redis_value(b"-MOVED 123\r\n")),
            // Respond with the new masters
            1 => Err(Ok(Value::Bulk(vec![
                Value::Bulk(vec![
                    Value::Int(0),
                    Value::Int(1),
                    Value::Bulk(vec![
                        Value::Data(name.as_bytes().to_vec()),
                        Value::Int(6379),
                    ]),
                ]),
                Value::Bulk(vec![
                    Value::Int(2),
                    Value::Int(16383),
                    Value::Bulk(vec![
                        Value::Data(name.as_bytes().to_vec()),
                        Value::Int(6380),
                    ]),
                ]),
            ]))),
            _ => {
                // Check that the correct node receives the request after rebuilding
                assert_eq!(port, 6380);
                Err(Ok(Value::Data(b"123".to_vec())))
            }
        }
    });

    let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

    assert_eq!(value, Ok(Some(123)));
}

#[test]
fn test_cluster_ask_redirect() {
    let name = "node";
    let completed = Arc::new(AtomicI32::new(0));
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")]),
        name,
        {
            move |cmd: &[u8], port| {
                respond_startup_two_nodes(name, cmd)?;
                // Error twice with io-error, ensure connection is reestablished w/out calling
                // other node (i.e., not doing a full slot rebuild)
                let count = completed.fetch_add(1, Ordering::SeqCst);
                match port {
                    6379 => match count {
                        0 => Err(parse_redis_value(b"-ASK 14000 node:6380\r\n")),
                        _ => panic!("Node should not be called now"),
                    },
                    6380 => match count {
                        1 => {
                            assert!(contains_slice(cmd, b"ASKING"));
                            Err(Ok(Value::Okay))
                        }
                        2 => {
                            assert!(contains_slice(cmd, b"GET"));
                            Err(Ok(Value::Data(b"123".to_vec())))
                        }
                        _ => panic!("Node should not be called now"),
                    },
                    _ => panic!("Wrong node"),
                }
            }
        },
    );

    let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

    assert_eq!(value, Ok(Some(123)));
}

#[test]
fn test_cluster_ask_error_when_new_node_is_added() {
    let name = "ask_with_extra_nodes";

    let requests = atomic::AtomicUsize::new(0);
    let started = atomic::AtomicBool::new(false);

    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::new(name, move |cmd: &[u8], port| {
        if !started.load(atomic::Ordering::SeqCst) {
            respond_startup(name, cmd)?;
        }
        started.store(true, atomic::Ordering::SeqCst);

        if contains_slice(cmd, b"PING") {
            return Err(Ok(Value::Status("OK".into())));
        }

        let i = requests.fetch_add(1, atomic::Ordering::SeqCst);

        match i {
            // Respond that the key exists on a node that does not yet have a connection:
            0 => Err(parse_redis_value(
                format!("-ASK 123 {name}:6380\r\n").as_bytes(),
            )),
            1 => {
                assert_eq!(port, 6380);
                assert!(contains_slice(cmd, b"ASKING"));
                Err(Ok(Value::Okay))
            }
            2 => {
                assert_eq!(port, 6380);
                assert!(contains_slice(cmd, b"GET"));
                Err(Ok(Value::Data(b"123".to_vec())))
            }
            _ => {
                panic!("Unexpected request: {:?}", cmd);
            }
        }
    });

    let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

    assert_eq!(value, Ok(Some(123)));
}

#[test]
fn test_cluster_replica_read() {
    let name = "node";

    // requests should route to replica
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")])
            .retries(0)
            .read_from_replicas(),
        name,
        move |cmd: &[u8], port| {
            respond_startup_with_replica(name, cmd)?;

            match port {
                6380 => Err(Ok(Value::Data(b"123".to_vec()))),
                _ => panic!("Wrong node"),
            }
        },
    );

    let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);
    assert_eq!(value, Ok(Some(123)));

    // requests should route to primary
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")])
            .retries(0)
            .read_from_replicas(),
        name,
        move |cmd: &[u8], port| {
            respond_startup_with_replica(name, cmd)?;
            match port {
                6379 => Err(Ok(Value::Status("OK".into()))),
                _ => panic!("Wrong node"),
            }
        },
    );

    let value = cmd("SET")
        .arg("test")
        .arg("123")
        .query::<Option<Value>>(&mut connection);
    assert_eq!(value, Ok(Some(Value::Status("OK".to_owned()))));
}

#[test]
fn test_cluster_io_error() {
    let name = "node";
    let completed = Arc::new(AtomicI32::new(0));
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(2),
        name,
        move |cmd: &[u8], port| {
            respond_startup_two_nodes(name, cmd)?;
            // Error twice with io-error, ensure connection is reestablished w/out calling
            // other node (i.e., not doing a full slot rebuild)
            match port {
                6380 => panic!("Node should not be called"),
                _ => match completed.fetch_add(1, Ordering::SeqCst) {
                    0..=1 => Err(Err(RedisError::from(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "mock-io-error",
                    )))),
                    _ => Err(Ok(Value::Data(b"123".to_vec()))),
                },
            }
        },
    );

    let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

    assert_eq!(value, Ok(Some(123)));
}

#[test]
fn test_cluster_non_retryable_error_should_not_retry() {
    let name = "node";
    let completed = Arc::new(AtomicI32::new(0));
    let MockEnv { mut connection, .. } = MockEnv::new(name, {
        let completed = completed.clone();
        move |cmd: &[u8], _| {
            respond_startup_two_nodes(name, cmd)?;
            // Error twice with io-error, ensure connection is reestablished w/out calling
            // other node (i.e., not doing a full slot rebuild)
            completed.fetch_add(1, Ordering::SeqCst);
            Err(Err((ErrorKind::ReadOnly, "").into()))
        }
    });

    let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

    match value {
        Ok(_) => panic!("result should be an error"),
        Err(e) => match e.kind() {
            ErrorKind::ReadOnly => {}
            _ => panic!("Expected ReadOnly but got {:?}", e.kind()),
        },
    }
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

fn test_cluster_fan_out(
    command: &'static str,
    expected_ports: Vec<u16>,
    slots_config: Option<Vec<MockSlotRange>>,
) {
    let name = "node";
    let found_ports = Arc::new(std::sync::Mutex::new(Vec::new()));
    let ports_clone = found_ports.clone();
    let mut cmd = redis::Cmd::new();
    for arg in command.split_whitespace() {
        cmd.arg(arg);
    }
    let packed_cmd = cmd.get_packed_command();
    // requests should route to replica
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")])
            .retries(0)
            .read_from_replicas(),
        name,
        move |received_cmd: &[u8], port| {
            respond_startup_with_replica_using_config(name, received_cmd, slots_config.clone())?;
            if received_cmd == packed_cmd {
                ports_clone.lock().unwrap().push(port);
                return Err(Ok(Value::Status("OK".into())));
            }
            Ok(())
        },
    );

    let _ = cmd.query::<Option<()>>(&mut connection);
    found_ports.lock().unwrap().sort();
    // MockEnv creates 2 mock connections.
    assert_eq!(*found_ports.lock().unwrap(), expected_ports);
}

#[test]
fn test_cluster_fan_out_to_all_primaries() {
    test_cluster_fan_out("FLUSHALL", vec![6379, 6381], None);
}

#[test]
fn test_cluster_fan_out_to_all_nodes() {
    test_cluster_fan_out("CONFIG SET", vec![6379, 6380, 6381, 6382], None);
}

#[test]
fn test_cluster_fan_out_out_once_to_each_primary_when_no_replicas_are_available() {
    test_cluster_fan_out(
        "CONFIG SET",
        vec![6379, 6381],
        Some(vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: Vec::new(),
                slot_range: (0..8191),
            },
            MockSlotRange {
                primary_port: 6381,
                replica_ports: Vec::new(),
                slot_range: (8192..16383),
            },
        ]),
    );
}

#[test]
fn test_cluster_fan_out_out_once_even_if_primary_has_multiple_slot_ranges() {
    test_cluster_fan_out(
        "CONFIG SET",
        vec![6379, 6380, 6381, 6382],
        Some(vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![6380],
                slot_range: (0..4000),
            },
            MockSlotRange {
                primary_port: 6381,
                replica_ports: vec![6382],
                slot_range: (4001..8191),
            },
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![6380],
                slot_range: (8192..8200),
            },
            MockSlotRange {
                primary_port: 6381,
                replica_ports: vec![6382],
                slot_range: (8201..16383),
            },
        ]),
    );
}

#[test]
fn test_cluster_split_multi_shard_command_and_combine_arrays_of_values() {
    let name = "test_cluster_split_multi_shard_command_and_combine_arrays_of_values";
    let mut cmd = cmd("MGET");
    cmd.arg("foo").arg("bar").arg("baz");
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")])
            .retries(0)
            .read_from_replicas(),
        name,
        move |received_cmd: &[u8], port| {
            respond_startup_with_replica_using_config(name, received_cmd, None)?;
            let cmd_str = std::str::from_utf8(received_cmd).unwrap();
            let results = ["foo", "bar", "baz"]
                .iter()
                .filter_map(|expected_key| {
                    if cmd_str.contains(expected_key) {
                        Some(Value::Data(format!("{expected_key}-{port}").into_bytes()))
                    } else {
                        None
                    }
                })
                .collect();
            Err(Ok(Value::Bulk(results)))
        },
    );

    let result = cmd.query::<Vec<String>>(&mut connection).unwrap();
    assert_eq!(result, vec!["foo-6382", "bar-6380", "baz-6380"]);
}

#[test]
fn test_cluster_route_correctly_on_packed_transaction_with_single_node_requests() {
    let name = "test_cluster_route_correctly_on_packed_transaction_with_single_node_requests";
    let mut pipeline = redis::pipe();
    pipeline.atomic().set("foo", "bar").get("foo");
    let packed_pipeline = pipeline.get_packed_pipeline();

    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")])
            .retries(0)
            .read_from_replicas(),
        name,
        move |received_cmd: &[u8], port| {
            respond_startup_with_replica_using_config(name, received_cmd, None)?;
            if port == 6381 {
                let results = vec![
                    Value::Data("OK".as_bytes().to_vec()),
                    Value::Data("QUEUED".as_bytes().to_vec()),
                    Value::Data("QUEUED".as_bytes().to_vec()),
                    Value::Bulk(vec![
                        Value::Data("OK".as_bytes().to_vec()),
                        Value::Data("bar".as_bytes().to_vec()),
                    ]),
                ];
                return Err(Ok(Value::Bulk(results)));
            }
            Err(Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                format!("wrong port: {port}"),
            ))))
        },
    );

    let result = connection
        .req_packed_commands(&packed_pipeline, 3, 1)
        .unwrap();
    assert_eq!(
        result,
        vec![
            Value::Data("OK".as_bytes().to_vec()),
            Value::Data("bar".as_bytes().to_vec()),
        ]
    );
}

#[test]
fn test_cluster_route_correctly_on_packed_transaction_with_single_node_requests2() {
    let name = "test_cluster_route_correctly_on_packed_transaction_with_single_node_requests2";
    let mut pipeline = redis::pipe();
    pipeline.atomic().set("foo", "bar").get("foo");
    let packed_pipeline = pipeline.get_packed_pipeline();
    let results = vec![
        Value::Data("OK".as_bytes().to_vec()),
        Value::Data("QUEUED".as_bytes().to_vec()),
        Value::Data("QUEUED".as_bytes().to_vec()),
        Value::Bulk(vec![
            Value::Data("OK".as_bytes().to_vec()),
            Value::Data("bar".as_bytes().to_vec()),
        ]),
    ];
    let expected_result = Value::Bulk(results);
    let cloned_result = expected_result.clone();

    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")])
            .retries(0)
            .read_from_replicas(),
        name,
        move |received_cmd: &[u8], port| {
            respond_startup_with_replica_using_config(name, received_cmd, None)?;
            if port == 6381 {
                return Err(Ok(cloned_result.clone()));
            }
            Err(Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                format!("wrong port: {port}"),
            ))))
        },
    );

    let result = connection.req_packed_command(&packed_pipeline).unwrap();
    assert_eq!(result, expected_result);
}

#[test]
fn test_cluster_can_be_created_with_partial_slot_coverage() {
    let name = "test_cluster_can_be_created_with_partial_slot_coverage";
    let slots_config = Some(vec![
        MockSlotRange {
            primary_port: 6379,
            replica_ports: vec![],
            slot_range: (0..8000),
        },
        MockSlotRange {
            primary_port: 6381,
            replica_ports: vec![],
            slot_range: (8201..16380),
        },
    ]);

    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")])
            .retries(0)
            .read_from_replicas(),
        name,
        move |received_cmd: &[u8], _| {
            respond_startup_with_replica_using_config(name, received_cmd, slots_config.clone())?;
            Err(Ok(Value::Status("PONG".into())))
        },
    );

    let res = connection.req_command(&redis::cmd("PING"));
    assert!(res.is_ok());
}

#[cfg(feature = "tls-rustls")]
mod mtls_test {
    use super::*;
    use crate::support::mtls_test::create_cluster_client_from_cluster;
    use redis::ConnectionInfo;

    #[test]
    fn test_cluster_basics_with_mtls() {
        let cluster = TestClusterContext::new_with_mtls(3, 0);

        let client = create_cluster_client_from_cluster(&cluster, true).unwrap();
        let mut con = client.get_connection().unwrap();

        redis::cmd("SET")
            .arg("{x}key1")
            .arg(b"foo")
            .execute(&mut con);
        redis::cmd("SET").arg(&["{x}key2", "bar"]).execute(&mut con);

        assert_eq!(
            redis::cmd("MGET")
                .arg(&["{x}key1", "{x}key2"])
                .query(&mut con),
            Ok(("foo".to_string(), b"bar".to_vec()))
        );
    }

    #[test]
    fn test_cluster_should_not_connect_without_mtls() {
        let cluster = TestClusterContext::new_with_mtls(3, 0);

        let client = create_cluster_client_from_cluster(&cluster, false).unwrap();
        let connection = client.get_connection();

        match cluster.cluster.servers.first().unwrap().connection_info() {
            ConnectionInfo {
                addr: redis::ConnectionAddr::TcpTls { .. },
                ..
            } => {
                if connection.is_ok() {
                    panic!("Must NOT be able to connect without client credentials if server accepts TLS");
                }
            }
            _ => {
                if let Err(e) = connection {
                    panic!("Must be able to connect without client credentials if server does NOT accept TLS: {e:?}");
                }
            }
        }
    }
}
