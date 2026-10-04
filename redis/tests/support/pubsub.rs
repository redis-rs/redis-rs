//! Helpers for the tests of the subscription fix of
//! <https://github.com/redis-rs/redis-rs/issues/2423>, shared by the test files of the connection
//! types that the fix covers, so that the same scenarios can run against all of them.

use std::future::Future;

use assert_matches::assert_matches;
use futures::future::join3;
use redis::aio::{ConnectionLike, PubSubSink};
use redis::{AsyncCommands, FromRedisValue, PushInfo, PushKind, RedisResult, Value, cmd};
use redis_test::redis_value;
use tokio::sync::mpsc::UnboundedReceiver;

/// Asserts that a `PING` response wasn't corrupted by the confirmations of another command that is
/// still in flight. RESP2 replies with an array while the connection is in subscribed mode, and
/// with a simple string otherwise; RESP3 always replies with a simple string.
pub fn assert_is_pong(response: Value) {
    match response {
        Value::SimpleString(message) => assert_eq!(message, "PONG"),
        Value::Array(values) => {
            assert_eq!(values.first(), Some(&redis_value!("pong")));
        }
        other => panic!("the ping response was corrupted: {other:?}"),
    }
}

/// Asserts that the next confirmation that was passed to `rx` reports the subscription of `name`,
/// with `remaining` subscriptions left on the connection.
pub async fn assert_confirmation(
    rx: &mut UnboundedReceiver<PushInfo>,
    kind: PushKind,
    name: &str,
    remaining: i64,
) {
    let push = rx.recv().await.unwrap();
    assert_eq!(
        push,
        PushInfo {
            kind,
            data: vec![redis_value!(name), redis_value!(remaining)],
        }
    );
}

/// Asserts that `rx` was passed one confirmation per name in `names`, and that the confirmations
/// report the remaining subscription counts in `counts`. The server confirms in its own iteration
/// order, so neither the names nor the counts are compared in order.
pub async fn assert_unordered_confirmations(
    rx: &mut UnboundedReceiver<PushInfo>,
    kind: PushKind,
    names: &[&str],
    counts: &[i64],
) {
    let mut received_names = Vec::new();
    let mut received_counts = Vec::new();
    for _ in names {
        let push = rx.recv().await.unwrap();
        assert_eq!(push.kind, kind);
        received_names.push(String::from_redis_value(push.data[0].clone()).unwrap());
        received_counts.push(i64::from_redis_value(push.data[1].clone()).unwrap());
    }
    received_names.sort();
    received_counts.sort();
    assert_eq!(
        received_names,
        names
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(received_counts, counts.to_vec());
}

/// Asserts that the server has no more confirmations or messages to pass.
pub fn assert_no_more_pushes(rx: &mut UnboundedReceiver<PushInfo>) {
    assert_matches!(rx.try_recv(), Err(_));
}

/// Publishes "banana" to `channel`, and asserts that it was passed to `rx` as a message of a
/// subscription of `kind` on `pattern`.
pub async fn assert_message_push<C: ConnectionLike + Send + Sync>(
    kind: SubscriptionKind,
    publish_conn: &mut C,
    rx: &mut UnboundedReceiver<PushInfo>,
    pattern: &str,
    channel: &str,
) {
    let _: () = publish_conn.publish(channel, "banana").await.unwrap();
    let push = rx.recv().await.unwrap();
    assert_eq!(
        push,
        match kind {
            SubscriptionKind::Channel => PushInfo {
                kind: PushKind::Message,
                data: vec![redis_value!(channel), redis_value!("banana")],
            },
            SubscriptionKind::Pattern => PushInfo {
                kind: PushKind::PMessage,
                data: vec![
                    redis_value!(pattern),
                    redis_value!(channel),
                    redis_value!("banana")
                ],
            },
        }
    );
}

/// The kind of subscription that a command applies to, so that the same scenario can be tested for
/// both channels and patterns.
#[derive(Clone, Copy)]
pub enum SubscriptionKind {
    Channel,
    Pattern,
}

impl SubscriptionKind {
    /// The opposite kind, used to keep the connection subscribed to something while the tested
    /// kind is unsubscribed.
    pub fn other(self) -> Self {
        match self {
            Self::Channel => Self::Pattern,
            Self::Pattern => Self::Channel,
        }
    }

    /// Subscribes to `channels` through a `PubSubSink` with the command of this kind.
    pub async fn subscribe(self, sink: &mut PubSubSink, channels: &[&str]) -> RedisResult<()> {
        match self {
            Self::Channel => sink.subscribe(channels).await,
            Self::Pattern => sink.psubscribe(channels).await,
        }
    }

    /// Unsubscribes from `channels` through a `PubSubSink` with the command of this kind.
    pub async fn unsubscribe(self, sink: &mut PubSubSink, channels: &[&str]) -> RedisResult<()> {
        match self {
            Self::Channel => sink.unsubscribe(channels).await,
            Self::Pattern => sink.punsubscribe(channels).await,
        }
    }

    /// Subscribes through `conn` with the command of this kind.
    pub async fn subscribe_on(
        self,
        conn: &mut impl SubscriptionSender,
        args: &[&str],
    ) -> RedisResult<()> {
        match self {
            Self::Channel => conn.subscribe(args).await,
            Self::Pattern => conn.psubscribe(args).await,
        }
    }

    /// Unsubscribes through `conn` with the command of this kind.
    pub async fn unsubscribe_on(
        self,
        conn: &mut impl SubscriptionSender,
        args: &[&str],
    ) -> RedisResult<()> {
        match self {
            Self::Channel => conn.unsubscribe(args).await,
            Self::Pattern => conn.punsubscribe(args).await,
        }
    }

    /// The kind of push that reports the confirmations of this kind's commands.
    pub fn confirmation_push_kind(self, subscribed: bool) -> PushKind {
        match (self, subscribed) {
            (Self::Channel, true) => PushKind::Subscribe,
            (Self::Channel, false) => PushKind::Unsubscribe,
            (Self::Pattern, true) => PushKind::PSubscribe,
            (Self::Pattern, false) => PushKind::PUnsubscribe,
        }
    }
}

/// A connection type that the subscription fix covers: it exposes the subscription commands, and
/// it can be cloned so that requests can run concurrently with a subscription command.
pub trait SubscriptionSender: ConnectionLike + Clone + Send + Sync {
    fn subscribe(&mut self, args: &[&str]) -> impl Future<Output = RedisResult<()>>;

    fn psubscribe(&mut self, args: &[&str]) -> impl Future<Output = RedisResult<()>>;

    fn unsubscribe(&mut self, args: &[&str]) -> impl Future<Output = RedisResult<()>>;

    fn punsubscribe(&mut self, args: &[&str]) -> impl Future<Output = RedisResult<()>>;

    /// Runs a `PING` on the connection that serves `key`. The default implementation ignores the
    /// key and runs a regular `PING`, which is enough for connection types that serve all requests
    /// on the same connection.
    fn ping_routed_by_key(&mut self, _key: &str) -> impl Future<Output = RedisResult<Value>> {
        async move { cmd("PING").query_async(self).await }
    }
}

impl SubscriptionSender for redis::aio::MultiplexedConnection {
    fn subscribe(&mut self, args: &[&str]) -> impl Future<Output = RedisResult<()>> {
        Self::subscribe(self, args)
    }

    fn psubscribe(&mut self, args: &[&str]) -> impl Future<Output = RedisResult<()>> {
        Self::psubscribe(self, args)
    }

    fn unsubscribe(&mut self, args: &[&str]) -> impl Future<Output = RedisResult<()>> {
        Self::unsubscribe(self, args)
    }

    fn punsubscribe(&mut self, args: &[&str]) -> impl Future<Output = RedisResult<()>> {
        Self::punsubscribe(self, args)
    }
}

/// The key that the requests which run concurrently with the tested commands read, so that their
/// responses can be checked for corruption. It shares `tag`, so that on a cluster it is served by
/// the same connection as the tested subscriptions.
pub fn probe_key(tag: &str) -> String {
    format!("{tag}-key")
}

/// Builds the names of the subscriptions of a scenario from `tag` and `suffixes`. The name of a
/// scenario's subscriptions shares `tag`, so that on a cluster they all map to the same slot and
/// are served by the same connection.
fn tagged_names(tag: &str, suffixes: &[&str]) -> Vec<String> {
    suffixes
        .iter()
        .map(|suffix| format!("{tag}-{suffix}"))
        .collect()
}

fn as_refs(names: &[String]) -> Vec<&str> {
    names.iter().map(String::as_str).collect()
}

/// Sets the key whose value the requests that run concurrently with the tested commands read, so
/// that they have a response that can be checked for corruption.
pub async fn set_probe_key<C: ConnectionLike + Send + Sync>(conn: &mut C, key: &str) {
    let _: () = conn.set(key, "value").await.unwrap();
}

/// Asserts that `conn` still serves requests.
pub async fn assert_connection_usable<C: SubscriptionSender>(conn: &mut C, key: &str) {
    let value: String = conn.get(key).await.unwrap();
    assert_eq!(value, "value");
    let response = conn.ping_routed_by_key(key).await.unwrap();
    assert_is_pong(response);
}

/// Runs `command` concurrently with a GET and a PING through clones of `conn`, and asserts that the
/// command succeeded, and that neither of the responses was corrupted by the command's
/// confirmations. The PING is routed by `key`'s slot, so that on connection types that route
/// commands it is served by the same connection as the GET, and as the tested command.
pub async fn assert_requests_uncorrupted_during<C: SubscriptionSender>(
    command: impl Future<Output = RedisResult<()>>,
    conn: &C,
    key: &str,
) {
    let mut get_conn = conn.clone();
    let mut ping_conn = conn.clone();

    let (command_result, get_result, ping_result) = join3(
        command,
        async move {
            let value: RedisResult<String> = get_conn.get(key).await;
            value
        },
        async move { ping_conn.ping_routed_by_key(key).await },
    )
    .await;

    command_result.unwrap();
    assert_eq!(get_result.unwrap(), "value");
    assert_is_pong(ping_result.unwrap());
}

/// Subscribes to two channels/patterns while requests are in flight, and asserts that the command
/// waited for all of its confirmations: neither of the concurrent requests was corrupted, both
/// confirmations were passed to the push sender, and the connection is still usable.
pub async fn assert_subscribe_awaits_all_confirmations_on<C: SubscriptionSender>(
    tag: &str,
    kind: SubscriptionKind,
    mut conn: C,
    rx: &mut UnboundedReceiver<PushInfo>,
) {
    let key = probe_key(tag);
    let mut request_conn = conn.clone();
    set_probe_key(&mut request_conn, &key).await;

    let names = tagged_names(tag, &["sub-a", "sub-b"]);
    let names = as_refs(&names);
    let mut command_conn = conn.clone();
    assert_requests_uncorrupted_during(kind.subscribe_on(&mut command_conn, &names), &conn, &key)
        .await;

    // Both confirmations were passed to the push sender, each with the number of subscriptions
    // that are left after it.
    assert_confirmation(rx, kind.confirmation_push_kind(true), names[0], 1).await;
    assert_confirmation(rx, kind.confirmation_push_kind(true), names[1], 2).await;
    assert_no_more_pushes(rx);

    // The connection isn't left with unattributed confirmations, so it still works.
    assert_connection_usable(&mut conn, &key).await;

    // Messages for the subscribed channels are still passed to the push sender.
    assert_message_push(kind, &mut request_conn, rx, names[0], names[0]).await;
}

/// Unsubscribes from two channels/patterns while requests are in flight, and asserts that the
/// command waited for all of its confirmations. A subscription of the other kind stays subscribed,
/// so it is included in the confirmations' count.
pub async fn assert_unsubscribe_awaits_all_confirmations_on<C: SubscriptionSender>(
    tag: &str,
    kind: SubscriptionKind,
    mut conn: C,
    rx: &mut UnboundedReceiver<PushInfo>,
) {
    let key = probe_key(tag);
    let mut request_conn = conn.clone();
    set_probe_key(&mut request_conn, &key).await;

    let names = tagged_names(tag, &["unsub-a", "unsub-b"]);
    let names = as_refs(&names);
    kind.subscribe_on(&mut conn, &names).await.unwrap();
    assert_confirmation(rx, kind.confirmation_push_kind(true), names[0], 1).await;
    assert_confirmation(rx, kind.confirmation_push_kind(true), names[1], 2).await;

    // A subscription of the other kind stays subscribed while the tested kind is unsubscribed.
    let stay = kind.other();
    // The staying subscription is suffixed with a wildcard, so that it also matches when it's a
    // pattern. The name is also used as the published channel, which is a legal channel name and
    // matches the pattern.
    let stay_name = format!("{tag}-unsub-stay-*");
    stay.subscribe_on(&mut conn, &[&stay_name]).await.unwrap();
    assert_confirmation(rx, stay.confirmation_push_kind(true), &stay_name, 3).await;

    let mut command_conn = conn.clone();
    assert_requests_uncorrupted_during(kind.unsubscribe_on(&mut command_conn, &names), &conn, &key)
        .await;

    // The confirmation count includes the subscription that stayed subscribed.
    assert_confirmation(rx, kind.confirmation_push_kind(false), names[0], 2).await;
    assert_confirmation(rx, kind.confirmation_push_kind(false), names[1], 1).await;
    assert_no_more_pushes(rx);

    // The staying subscription still receives messages.
    assert_message_push(stay, &mut request_conn, rx, &stay_name, &stay_name).await;
}

/// Unsubscribing from a name that was never subscribed must leave the connection usable - the
/// server sends a confirmation for every argument, including the ones that weren't subscribed.
pub async fn assert_partial_unsubscribe_leaves_connection_usable_on<C: SubscriptionSender>(
    tag: &str,
    kind: SubscriptionKind,
    mut conn: C,
    rx: &mut UnboundedReceiver<PushInfo>,
) {
    let key = probe_key(tag);
    let mut request_conn = conn.clone();
    set_probe_key(&mut request_conn, &key).await;

    let subscribed = format!("{tag}-partial-a");
    kind.subscribe_on(&mut conn, &[&subscribed]).await.unwrap();
    assert_confirmation(rx, kind.confirmation_push_kind(true), &subscribed, 1).await;

    let missing = tagged_names(tag, &["partial-b", "partial-c"]);
    let names = [
        subscribed.as_str(),
        missing[0].as_str(),
        missing[1].as_str(),
    ];
    let mut command_conn = conn.clone();
    assert_requests_uncorrupted_during(kind.unsubscribe_on(&mut command_conn, &names), &conn, &key)
        .await;

    // The server sends a confirmation for every argument, including the ones that weren't
    // subscribed.
    for _ in 0..3 {
        let push = rx.recv().await.unwrap();
        assert_eq!(push.kind, kind.confirmation_push_kind(false));
    }
    assert_no_more_pushes(rx);

    // The connection isn't left with unattributed confirmations, so messages can still be received
    // after re-subscribing.
    kind.subscribe_on(&mut conn, &[&subscribed]).await.unwrap();
    assert_confirmation(rx, kind.confirmation_push_kind(true), &subscribed, 1).await;
    assert_message_push(kind, &mut request_conn, rx, &subscribed, &subscribed).await;
}

/// A zero-argument UNSUBSCRIBE/PUNSUBSCRIBE is answered with one confirmation per active
/// subscription, so the number of confirmations is resolved from the tracked subscriptions when
/// the first confirmation arrives.
pub async fn assert_empty_unsubscribe_does_not_corrupt_connection_on<C: SubscriptionSender>(
    tag: &str,
    kind: SubscriptionKind,
    mut conn: C,
    rx: &mut UnboundedReceiver<PushInfo>,
) {
    let key = probe_key(tag);
    let mut request_conn = conn.clone();
    set_probe_key(&mut request_conn, &key).await;

    let names = tagged_names(tag, &["empty-a", "empty-b"]);
    let names = as_refs(&names);
    kind.subscribe_on(&mut conn, &names).await.unwrap();
    assert_confirmation(rx, kind.confirmation_push_kind(true), names[0], 1).await;
    assert_confirmation(rx, kind.confirmation_push_kind(true), names[1], 2).await;

    // A subscription of the other kind stays subscribed while the tested kind is unsubscribed.
    let stay = kind.other();
    // The staying subscription is suffixed with a wildcard, so that it also matches when it's a
    // pattern. The name is also used as the published channel, which is a legal channel name and
    // matches the pattern.
    let stay_name = format!("{tag}-empty-stay-*");
    stay.subscribe_on(&mut conn, &[&stay_name]).await.unwrap();
    assert_confirmation(rx, stay.confirmation_push_kind(true), &stay_name, 3).await;

    let mut command_conn = conn.clone();
    assert_requests_uncorrupted_during(kind.unsubscribe_on(&mut command_conn, &[]), &conn, &key)
        .await;

    // One confirmation per subscription of the tested kind was passed to the push sender, and the
    // count includes the subscription of the other kind.
    assert_unordered_confirmations(rx, kind.confirmation_push_kind(false), &names, &[1, 2]).await;
    assert_no_more_pushes(rx);

    // The staying subscription still receives messages.
    assert_message_push(stay, &mut request_conn, rx, &stay_name, &stay_name).await;

    // A subscription of the tested kind is added, so that the other kind's zero-argument
    // unsubscribe has a subscription to remove.
    let extra = format!("{tag}-empty-extra");
    kind.subscribe_on(&mut conn, &[&extra]).await.unwrap();
    assert_confirmation(rx, kind.confirmation_push_kind(true), &extra, 2).await;

    let mut command_conn = conn.clone();
    assert_requests_uncorrupted_during(stay.unsubscribe_on(&mut command_conn, &[]), &conn, &key)
        .await;

    // The subscription of the other kind is removed with a single confirmation, and the count
    // includes the subscription of the tested kind that stayed.
    assert_confirmation(rx, stay.confirmation_push_kind(false), &stay_name, 1).await;
    assert_no_more_pushes(rx);

    // The connection isn't left with unattributed confirmations, so it still works.
    assert_connection_usable(&mut conn, &key).await;

    // The staying subscription of the tested kind still receives messages.
    assert_message_push(kind, &mut request_conn, rx, &extra, &extra).await;
}

/// A scenario that is tested on every connection type that the fix covers.
#[derive(Clone, Copy)]
pub enum Scenario {
    Subscribe,
    Unsubscribe,
    PartialUnsubscribe,
    EmptyUnsubscribe,
}

/// Runs `scenario` over `conn`, and asserts that the tested command waited for all of its
/// confirmations. `tag` is used to build the names of the scenario's subscriptions and its probe
/// key, so that on a cluster they are all served by the same connection.
pub async fn assert_scenario_on<C: SubscriptionSender>(
    tag: &str,
    kind: SubscriptionKind,
    scenario: Scenario,
    conn: C,
    rx: &mut UnboundedReceiver<PushInfo>,
) {
    match scenario {
        Scenario::Subscribe => {
            assert_subscribe_awaits_all_confirmations_on(tag, kind, conn, rx).await;
        }
        Scenario::Unsubscribe => {
            assert_unsubscribe_awaits_all_confirmations_on(tag, kind, conn, rx).await;
        }
        Scenario::PartialUnsubscribe => {
            assert_partial_unsubscribe_leaves_connection_usable_on(tag, kind, conn, rx).await;
        }
        Scenario::EmptyUnsubscribe => {
            assert_empty_unsubscribe_does_not_corrupt_connection_on(tag, kind, conn, rx).await;
        }
    }
}
