#![cfg(feature = "cluster")]
mod support;
use std::sync::{
    atomic::{self, AtomicI32, Ordering},
    Arc,
};

use crate::support::*;
use redis::{
    cluster::{cluster_pipe, ClusterClient},
    cmd, parse_redis_value, ErrorKind, RedisError, Value,
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
    let cluster = TestClusterContext::new_with_cluster_client_builder(3, 0, |builder| {
        builder
            .username(RedisCluster::username().to_string())
            .password(RedisCluster::password().to_string())
    });
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
    let cluster = TestClusterContext::new_with_cluster_client_builder(3, 0, |builder| {
        builder
            .username(RedisCluster::username().to_string())
            .password("not the right password".to_string())
    });
    assert!(cluster.client.get_connection().is_err());
}

#[test]
fn test_cluster_read_from_replicas() {
    let cluster = TestClusterContext::new_with_cluster_client_builder(6, 1, |builder| {
        builder.read_from_replicas()
    });
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
fn test_cluster_rebuild_with_extra_nodes() {
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
            // Respond that the key exists elswehere (the slot, 123, is unused in the
            // implementation)
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
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder(vec![&*format!("redis://{name}")]),
        name,
        {
            let completed = completed.clone();
            move |cmd: &[u8], _| {
                respond_startup_two_nodes(name, cmd)?;
                // Error twice with io-error, ensure connection is reestablished w/out calling
                // other node (i.e., not doing a full slot rebuild)
                completed.fetch_add(1, Ordering::SeqCst);
                Err(parse_redis_value(b"-ERR mock\r\n"))
            }
        },
    );

    let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

    match value {
        Ok(_) => panic!("result should be an error"),
        Err(e) => match e.kind() {
            ErrorKind::ResponseError => {}
            _ => panic!("Expected ResponseError but got {:?}", e.kind()),
        },
    }
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}
