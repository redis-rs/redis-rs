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
