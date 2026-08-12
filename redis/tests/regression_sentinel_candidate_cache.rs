#![cfg(feature = "sentinel")]

mod support;

use redis::sentinel::{SentinelClient, SentinelServerType};
use test_macros::async_test;

use crate::support::TestSentinelContext;

fn client_with_non_sentinel_first(context: &TestSentinelContext) -> (SentinelClient, redis::ConnectionInfo) {
    let non_sentinel = context.cluster.servers[0].connection_info();
    let healthy_sentinel = context.sentinels_connection_info()[0].clone();
    let client = SentinelClient::build(
        vec![non_sentinel, healthy_sentinel.clone()],
        String::from("master1"),
        Some(context.sentinel_node_connection_info()),
        SentinelServerType::Master,
    )
    .unwrap();

    (client, healthy_sentinel)
}

#[test]
fn sentinel_candidate_uses_matching_cached_connection() {
    let context = TestSentinelContext::new(2, 3, 3);
    let (mut client, healthy_sentinel) = client_with_non_sentinel_first(&context);

    let selected = client
        .get_sentinel_client()
        .expect("the second configured candidate is a valid Sentinel");

    assert_eq!(
        selected.get_connection_info().addr(),
        healthy_sentinel.addr(),
        "candidate validation must use the cache entry belonging to that candidate"
    );
}

#[cfg(feature = "aio")]
#[async_test]
async fn async_sentinel_candidate_uses_matching_cached_connection() {
    let context = TestSentinelContext::new(2, 3, 3);
    let (mut client, healthy_sentinel) = client_with_non_sentinel_first(&context);

    let selected = client
        .async_get_sentinel_client()
        .await
        .expect("the second configured candidate is a valid Sentinel");

    assert_eq!(
        selected.get_connection_info().addr(),
        healthy_sentinel.addr(),
        "async candidate validation must use the cache entry belonging to that candidate"
    );
}
