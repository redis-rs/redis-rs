#![cfg(feature = "cluster")]
mod support;
use crate::support::*;

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
    let mut con = cluster.connection();

    let err = redis::pipe()
        .cmd("SET")
        .arg("key_1")
        .arg(42)
        .ignore()
        .query::<()>(&mut con)
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "This connection does not support pipelining."
    );
}
