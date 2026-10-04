//! Adds async IO support to redis.
use crate::cmd::Cmd;
use crate::connection::{
    AuthResult, ConnectionSetupComponents, RedisConnectionInfo, check_connection_setup,
    connection_setup_pipeline,
};
use crate::io::AsyncDNSResolver;
use crate::pipeline::ExpectedConfirmations;
use crate::types::{HashSet, RedisFuture, RedisResult, Value};
use crate::{ErrorKind, PushInfo, RedisError, errors::closed_connection_error};
use ::tokio::io::{AsyncRead, AsyncWrite};
use futures_util::{
    future::{Future, FutureExt},
    sink::{Sink, SinkExt},
    stream::{Stream, StreamExt},
};
pub use monitor::Monitor;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::Path;
use std::pin::Pin;

mod monitor;

#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
use crate::connection::TlsConnParams;

/// Enables the smol compatibility
#[cfg(feature = "smol-comp")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol-comp")))]
pub mod smol;
/// Enables the tokio compatibility
#[cfg(feature = "tokio-comp")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio-comp")))]
pub mod tokio;

mod pubsub;
pub use pubsub::{PubSub, PubSubSink, PubSubStream};

/// Represents the ability of connecting via TCP or via Unix socket
pub(crate) trait RedisRuntime: AsyncStream + Send + Sync + Sized + 'static {
    /// Performs a TCP connection
    async fn connect_tcp(
        socket_addr: SocketAddr,
        tcp_settings: &crate::io::tcp::TcpSettings,
    ) -> RedisResult<Self>;

    // Performs a TCP TLS connection
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    async fn connect_tcp_tls(
        hostname: &str,
        socket_addr: SocketAddr,
        insecure: bool,
        tls_params: &Option<TlsConnParams>,
        tcp_settings: &crate::io::tcp::TcpSettings,
    ) -> RedisResult<Self>;

    /// Performs a UNIX connection
    #[cfg(unix)]
    async fn connect_unix(path: &Path) -> RedisResult<Self>;

    fn spawn(f: impl Future<Output = ()> + Send + 'static) -> TaskHandle;

    fn boxed(self) -> Pin<Box<dyn AsyncStream + Send + Sync>> {
        Box::pin(self)
    }
}

/// Trait for objects that implements `AsyncRead` and `AsyncWrite`
pub trait AsyncStream: AsyncRead + AsyncWrite {}
impl<S> AsyncStream for S where S: AsyncRead + AsyncWrite {}

/// An async abstraction over connections.
pub trait ConnectionLike {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value>;

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    /// Important - this function is meant for internal usage, since it's
    /// easy to pass incorrect `offset` & `count` parameters, which might
    /// cause the connection to enter an erroneous state. Users shouldn't
    /// call it, instead using the Pipeline::query_async function.
    #[doc(hidden)]
    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>>;

    /// Returns the database this connection is bound to.  Note that this
    /// information might be unreliable because it's initially cached and
    /// also might be incorrect if the connection like object is not
    /// actually connected.
    fn get_db(&self) -> i64;
}

/// The pub/sub subscription commands that are answered with one confirmation per argument.
///
/// The variants of this enum share the command's name, the way the number of expected
/// confirmations is derived from its arguments, and the way confirmations are matched to the
/// subscriptions they change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubscribeLikeCommand {
    Subscribe,
    Unsubscribe,
    PSubscribe,
    PUnsubscribe,
}

impl SubscribeLikeCommand {
    /// The name of the command as sent to the server.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Subscribe => "SUBSCRIBE",
            Self::Unsubscribe => "UNSUBSCRIBE",
            Self::PSubscribe => "PSUBSCRIBE",
            Self::PUnsubscribe => "PUNSUBSCRIBE",
        }
    }

    /// How many confirmations the server sends for this command when called with `args`.
    pub(crate) fn expected_confirmations(self, args: &[Vec<u8>]) -> ExpectedConfirmations {
        let length = args.len();
        if length == 0 {
            match self {
                // A zero-argument un/psubscribe removes all of the connection's current
                // subscriptions of that kind, so the count depends on the connection's state.
                Self::Unsubscribe => ExpectedConfirmations::UnsubscribeAllChannels,
                Self::PUnsubscribe => ExpectedConfirmations::UnsubscribeAllPatterns,
                // A SUBSCRIBE/PSUBSCRIBE without arguments is rejected with a single error.
                Self::Subscribe | Self::PSubscribe => ExpectedConfirmations::Count(1),
            }
        } else {
            ExpectedConfirmations::Count(length)
        }
    }
}

/// The kind of subscription that a server confirmation changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubscriptionKind {
    /// A regular pub/sub channel.
    Channel,
    /// A pub/sub pattern.
    Pattern,
}

/// A subscription change described by a server confirmation of a subscription command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SubscriptionChange {
    /// The kind of subscription the change affects.
    kind: SubscriptionKind,
    /// `true` if the subscription was added, `false` if it was removed.
    subscribed: bool,
    /// The channel or pattern the confirmation refers to. `None` for the nil channel sent when
    /// an unsubscribe-all command had nothing to unsubscribe from.
    channel: Option<Vec<u8>>,
}

/// The channels and patterns the connection is subscribed to, as reported by the confirmations
/// the server has sent so far.
///
/// This is used to determine how many confirmations a zero-argument `UNSUBSCRIBE` or
/// `PUNSUBSCRIBE` will be answered with. The tracked state matches the
/// server's state whenever a command's confirmations start arriving, because all commands before
/// it are confirmed (in order) by the time the first confirmation of this command is processed -
/// even when the commands were sent concurrently.
///
/// # Why the subscriptions are tracked
///
/// The server answers a zero-argument unsubscribe with one confirmation per subscription that
/// existed when the command ran (or with a single confirmation carrying a nil channel when there
/// were none). The reply itself does not carry that count, so a request/response connection has
/// to derive it from the state the server used. Tracking the sets as confirmations arrive
/// provides exactly that state, which lets a connection wait for all of a command's
/// confirmations instead of leaving the surplus ones to be attributed to the commands that
/// follow. See <https://github.com/redis-rs/redis-rs/issues/2423>.
///
/// # Drift between the tracked sets and the server
///
/// The scheme is only correct while the tracked sets mirror the server. If they disagree, a
/// zero-argument unsubscribe resolves the wrong count and the connection is left in an invalid
/// state:
///
/// * Tracking **fewer** subscriptions than the server (an under-count) leaves surplus
///   confirmations unattributed; they are then consumed as the replies of the commands that
///   follow, which is the corruption this tracking exists to prevent.
/// * Tracking **more** subscriptions than the server (an over-count) makes the request wait for
///   confirmations that never arrive, leaving that request, and every request queued behind it,
///   stuck.
///
/// Under-counting can only happen when a server-side subscription change never reaches
/// [`subscription_change`], for example because a new subscription kind was added to [`PushKind`]
/// without being handled there. Every new subscription kind must therefore be handled by
/// [`subscription_change`]; `subscription_change_recognizes_every_subscription_kind` guards this.
///
/// # Missing work: state the server resets without confirmations
///
/// Over-counting can happen when the server drops subscriptions without sending a per-channel
/// confirmation. The known case is the RESP3 `RESET` command, which clears the connection's
/// subscriptions and replies with a single `+RESET`; the confirmations that this struct is built
/// from are never sent, so the tracked sets stay populated while the server has nothing. A
/// zero-argument unsubscribe issued afterwards then waits for confirmations that never arrive,
/// stalling the connection. This crate does not send `RESET` itself, but a caller can run it
/// through the raw command API, so the tracked sets should eventually be cleared whenever the
/// connection's server-side subscription state is reset. Reconnecting is not affected: a
/// reconnecting [`MultiplexedConnection`] starts with empty tracked sets, matching the fresh
/// (empty) server-side state before the resubscribe commands are confirmed.
///
/// [`PushKind`]: crate::PushKind
/// [`MultiplexedConnection`]: crate::aio::MultiplexedConnection
#[derive(Clone, Debug, Default)]
pub(crate) struct SubscriptionSets {
    channels: HashSet<Vec<u8>>,
    patterns: HashSet<Vec<u8>>,
}

impl SubscriptionSets {
    /// The number of subscriptions of the given kind that the server was aware of when it
    /// started confirming an unsubscribe-all command. Always at least 1, since the server sends
    /// a single confirmation with a nil channel when there's nothing to unsubscribe from.
    fn unsubscribe_all_target(&self, kind: SubscriptionKind) -> usize {
        let subscriptions = match kind {
            SubscriptionKind::Channel => &self.channels,
            SubscriptionKind::Pattern => &self.patterns,
        };
        subscriptions.len().max(1)
    }

    fn apply(&mut self, change: SubscriptionChange) {
        let subscriptions = match change.kind {
            SubscriptionKind::Channel => &mut self.channels,
            SubscriptionKind::Pattern => &mut self.patterns,
        };
        match change.channel {
            Some(channel) if change.subscribed => {
                subscriptions.insert(channel);
            }
            Some(channel) => {
                subscriptions.remove(&channel);
            }
            // Nothing to remove when the server reports a nil channel.
            None => {}
        }
    }
}

/// Extracts the subscription change that a server confirmation describes.
///
/// Returns `None` for values that aren't subscription confirmations (e.g. messages or `PONG`
/// replies).
pub(crate) fn subscription_change(value: &Value) -> Option<SubscriptionChange> {
    let ((kind, subscribed), channel) = match value {
        Value::Push { kind, data } => {
            let (kind, subscribed) = match kind {
                crate::PushKind::Subscribe => (SubscriptionKind::Channel, true),
                crate::PushKind::Unsubscribe => (SubscriptionKind::Channel, false),
                crate::PushKind::PSubscribe => (SubscriptionKind::Pattern, true),
                crate::PushKind::PUnsubscribe => (SubscriptionKind::Pattern, false),
                _ => return None,
            };
            ((kind, subscribed), data.first().and_then(value_channel))
        }
        Value::Array(data) => {
            let (kind, subscribed) = match data.first() {
                Some(Value::BulkString(kind)) => match kind.as_slice() {
                    b"subscribe" => (SubscriptionKind::Channel, true),
                    b"unsubscribe" => (SubscriptionKind::Channel, false),
                    b"psubscribe" => (SubscriptionKind::Pattern, true),
                    b"punsubscribe" => (SubscriptionKind::Pattern, false),
                    _ => return None,
                },
                _ => return None,
            };
            ((kind, subscribed), data.get(1).and_then(value_channel))
        }
        _ => return None,
    };

    Some(SubscriptionChange {
        kind,
        subscribed,
        channel,
    })
}

fn value_channel(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::BulkString(channel) => Some(channel.clone()),
        _ => None,
    }
}

async fn execute_connection_pipeline<T>(
    codec: &mut T,
    (pipeline, instructions): (crate::Pipeline, ConnectionSetupComponents),
) -> RedisResult<AuthResult>
where
    T: Sink<Vec<u8>, Error = RedisError>,
    T: Stream<Item = RedisResult<Value>>,
    T: Unpin + Send + 'static,
{
    let count = pipeline.len();
    if count == 0 {
        return Ok(AuthResult::Succeeded);
    }
    codec.send(pipeline.get_packed_pipeline()).await?;

    let mut results = Vec::with_capacity(count);
    for _ in 0..count {
        let value = codec.next().await.ok_or_else(closed_connection_error)??;
        results.push(value);
    }

    check_connection_setup(results, instructions)
}

pub(super) async fn setup_connection<T>(
    codec: &mut T,
    connection_info: &RedisConnectionInfo,
    #[cfg(feature = "cache-aio")] cache_config: Option<crate::caching::CacheConfig>,
) -> RedisResult<()>
where
    T: Sink<Vec<u8>, Error = RedisError>,
    T: Stream<Item = RedisResult<Value>>,
    T: Unpin + Send + 'static,
{
    if execute_connection_pipeline(
        codec,
        connection_setup_pipeline(
            connection_info,
            true,
            #[cfg(feature = "cache-aio")]
            cache_config,
        ),
    )
    .await?
        == AuthResult::ShouldRetryWithoutUsername
    {
        execute_connection_pipeline(
            codec,
            connection_setup_pipeline(
                connection_info,
                false,
                #[cfg(feature = "cache-aio")]
                cache_config,
            ),
        )
        .await?;
    }

    Ok(())
}

mod connection;
pub(crate) use connection::connect_simple;
pub use connection::transaction_async;
mod multiplexed_connection;
pub use multiplexed_connection::*;
#[cfg(feature = "connection-manager")]
mod connection_manager;
#[cfg(feature = "connection-manager")]
#[cfg_attr(docsrs, doc(cfg(feature = "connection-manager")))]
pub use connection_manager::*;
mod runtime;
#[cfg(all(feature = "smol-comp", feature = "tokio-comp"))]
pub use runtime::prefer_smol;
#[cfg(all(feature = "tokio-comp", feature = "smol-comp"))]
pub use runtime::prefer_tokio;
pub(super) use runtime::*;

/// An error showing that the receiver
#[derive(Debug)]
pub struct SendError;

/// A trait for sender parts of a channel that can be used for sending push messages from async
/// connection.
pub trait AsyncPushSender: Send + Sync + 'static {
    /// The sender must send without blocking, otherwise it will block the sending connection.
    fn send(&self, info: PushInfo) -> Result<(), SendError>;
}

impl AsyncPushSender for ::tokio::sync::mpsc::UnboundedSender<PushInfo> {
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        match self.send(info) {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError),
        }
    }
}

impl AsyncPushSender for ::tokio::sync::broadcast::Sender<PushInfo> {
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        match self.send(info) {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError),
        }
    }
}

impl<T, Func: Fn(PushInfo) -> Result<(), T> + Send + Sync + 'static> AsyncPushSender for Func {
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        match self(info) {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError),
        }
    }
}

impl AsyncPushSender for std::sync::mpsc::Sender<PushInfo> {
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        match self.send(info) {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError),
        }
    }
}

#[cfg(feature = "cluster-async")]
impl AsyncPushSender for futures_channel::mpsc::UnboundedSender<PushInfo> {
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        match self.unbounded_send(info) {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError),
        }
    }
}

impl<T> AsyncPushSender for std::sync::Arc<T>
where
    T: AsyncPushSender + ?Sized,
{
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        self.as_ref().send(info)
    }
}

/// Default DNS resolver which uses the system's DNS resolver.
#[derive(Clone)]
pub(crate) struct DefaultAsyncDNSResolver;

impl AsyncDNSResolver for DefaultAsyncDNSResolver {
    fn resolve<'a, 'b: 'a>(
        &'a self,
        host: &'b str,
        port: u16,
    ) -> RedisFuture<'a, Box<dyn Iterator<Item = SocketAddr> + Send + 'a>> {
        Box::pin(get_socket_addrs(host, port).map(|vec| {
            Ok(Box::new(vec?.into_iter()) as Box<dyn Iterator<Item = SocketAddr> + Send>)
        }))
    }
}

async fn get_socket_addrs(host: &str, port: u16) -> RedisResult<Vec<SocketAddr>> {
    let socket_addrs: Vec<_> = match Runtime::locate() {
        #[cfg(feature = "tokio-comp")]
        Runtime::Tokio => ::tokio::net::lookup_host((host, port))
            .await
            .map_err(RedisError::from)
            .map(|iter| iter.collect()),

        #[cfg(feature = "smol-comp")]
        Runtime::Smol => ::smol::net::resolve((host, port))
            .await
            .map_err(RedisError::from),
    }?;

    if socket_addrs.is_empty() {
        Err(RedisError::from((
            ErrorKind::InvalidClientConfig,
            "No address found for host",
        )))
    } else {
        Ok(socket_addrs)
    }
}

#[cfg(test)]
mod subscription_tracking_tests {
    use super::*;
    use crate::PushKind;

    fn push_confirmation(kind: PushKind, channel: &[u8]) -> Value {
        Value::Push {
            kind,
            data: vec![Value::BulkString(channel.to_vec()), Value::Int(1)],
        }
    }

    fn array_confirmation(kind: &str, channel: &[u8]) -> Value {
        Value::Array(vec![
            Value::BulkString(kind.as_bytes().to_vec()),
            Value::BulkString(channel.to_vec()),
            Value::Int(1),
        ])
    }

    /// `SubscriptionSets` can only stay in sync with the server if every subscription
    /// confirmation is recognized by `subscription_change`. A kind that is missing there would
    /// silently stop being tracked and reintroduce the surplus/unattributed-confirmation
    /// corruption described on `SubscriptionSets` (see issue #2423), so this test fails when a
    /// new subscription kind is added without being handled.
    #[test]
    fn subscription_change_recognizes_every_subscription_kind() {
        let cases = [
            (
                PushKind::Subscribe,
                SubscriptionKind::Channel,
                true,
                b"subscribe".as_slice(),
            ),
            (
                PushKind::Unsubscribe,
                SubscriptionKind::Channel,
                false,
                b"unsubscribe".as_slice(),
            ),
            (
                PushKind::PSubscribe,
                SubscriptionKind::Pattern,
                true,
                b"psubscribe".as_slice(),
            ),
            (
                PushKind::PUnsubscribe,
                SubscriptionKind::Pattern,
                false,
                b"punsubscribe".as_slice(),
            ),
        ];

        for (push_kind, kind, subscribed, name) in cases {
            // RESP3: the confirmation arrives as a push message.
            let change = subscription_change(&push_confirmation(push_kind, b"channel"))
                .expect("RESP3 subscription confirmation must be recognized");
            assert_eq!(change.kind, kind);
            assert_eq!(change.subscribed, subscribed);
            assert_eq!(change.channel.as_deref(), Some(b"channel".as_slice()));

            // RESP2: the confirmation arrives as an array.
            let name = std::str::from_utf8(name).unwrap();
            let change = subscription_change(&array_confirmation(name, b"channel"))
                .expect("RESP2 subscription confirmation must be recognized");
            assert_eq!(change.kind, kind);
            assert_eq!(change.subscribed, subscribed);
            assert_eq!(change.channel.as_deref(), Some(b"channel".as_slice()));
        }
    }

    #[test]
    fn subscription_change_ignores_values_that_are_not_confirmations() {
        for kind in [
            PushKind::Message,
            PushKind::PMessage,
            PushKind::SMessage,
            PushKind::Invalidate,
            PushKind::Other("pong".to_owned()),
            PushKind::Disconnection,
        ] {
            assert!(subscription_change(&push_confirmation(kind, b"channel")).is_none());
        }
        assert!(
            subscription_change(&array_confirmation("message", b"channel")).is_none(),
            "a regular pub/sub message isn't a subscription confirmation"
        );
        assert!(
            subscription_change(&Value::SimpleString("PONG".to_owned())).is_none(),
            "a PONG reply isn't a subscription confirmation"
        );
    }

    #[test]
    fn unsubscribe_all_target_follows_the_tracked_sets() {
        let mut subscriptions = SubscriptionSets::default();

        // With nothing tracked, the server still sends a single confirmation with a nil channel.
        for kind in [SubscriptionKind::Channel, SubscriptionKind::Pattern] {
            assert_eq!(subscriptions.unsubscribe_all_target(kind), 1);
        }

        // Each kind is tracked independently.
        let kinds = [
            (SubscriptionKind::Channel, b"channel".as_slice()),
            (SubscriptionKind::Pattern, b"pattern".as_slice()),
        ];
        for (kind, name) in kinds {
            subscriptions.apply(SubscriptionChange {
                kind,
                subscribed: true,
                channel: Some(name.to_vec()),
            });
        }
        for (kind, _) in kinds {
            assert_eq!(subscriptions.unsubscribe_all_target(kind), 1);
        }

        // Subscribing twice to the same channel only counts once.
        subscriptions.apply(SubscriptionChange {
            kind: SubscriptionKind::Channel,
            subscribed: true,
            channel: Some(b"channel".to_vec()),
        });
        assert_eq!(
            subscriptions.unsubscribe_all_target(SubscriptionKind::Channel),
            1
        );

        // A second channel is counted, without affecting the other kinds.
        subscriptions.apply(SubscriptionChange {
            kind: SubscriptionKind::Channel,
            subscribed: true,
            channel: Some(b"second".to_vec()),
        });
        assert_eq!(
            subscriptions.unsubscribe_all_target(SubscriptionKind::Channel),
            2
        );
        assert_eq!(
            subscriptions.unsubscribe_all_target(SubscriptionKind::Pattern),
            1
        );

        // Unsubscribing removes it again.
        subscriptions.apply(SubscriptionChange {
            kind: SubscriptionKind::Channel,
            subscribed: false,
            channel: Some(b"channel".to_vec()),
        });
        assert_eq!(
            subscriptions.unsubscribe_all_target(SubscriptionKind::Channel),
            1
        );

        // A confirmation with a nil channel (the server had nothing to unsubscribe from) is a
        // no-op rather than removing a real subscription.
        subscriptions.apply(SubscriptionChange {
            kind: SubscriptionKind::Channel,
            subscribed: false,
            channel: None,
        });
        assert_eq!(
            subscriptions.unsubscribe_all_target(SubscriptionKind::Channel),
            1
        );
    }
}
