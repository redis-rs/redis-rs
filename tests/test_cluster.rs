#![cfg(feature = "cluster")]
mod support;
use crate::support::*;
use redis::cluster::cluster_pipe;

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
fn test_cluster_readonly() {
    let cluster =
        TestClusterContext::new_with_cluster_client_builder(6, 1, |builder| builder.readonly(true));
    let mut con = cluster.connection();

    // con is a READONLY replica, so we'll get the MOVED response and will be redirected
    // to the master
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
        "This command cannot be safely routed in cluster mode: Command 'SCRIPT KILL' can't be executed in a cluster pipeline."
    );

    let err = cluster_pipe().keys("*").query::<()>(&mut con).unwrap_err();

    assert_eq!(
        err.to_string(),
        "This command cannot be safely routed in cluster mode: Command 'KEYS' can't be executed in a cluster pipeline."
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
        queries.push(format!("foo{}", i));
        expected.push(format!("bar{}", i));
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
            let query = format!("foo{}", i);
            let r = format!("bar{}", i);
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
