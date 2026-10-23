#[cfg(test)]
use core::str;
#[cfg(any(feature = "cluster-async", test))]
use std::collections::HashMap;
use std::collections::HashSet;

#[cfg(any(feature = "connection-manager", feature = "cluster-async", test))]
use crate::Pipeline;
use crate::Value;
#[cfg(test)]
use crate::{Arg, Cmd};

/// The kind of subscription that a server confirmation changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubscriptionKind {
    Channel,
    Pattern,
    ShardChannel,
}

/// A subscription change described by a server confirmation of a subscription command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SubscriptionChange {
    /// The kind of subscription the change affects.
    pub(crate) kind: SubscriptionKind,
    /// `true` if the subscription was added, `false` if it was removed.
    pub(crate) subscribed: bool,
    /// The channel or pattern the confirmation refers to. `None` for the nil channel sent when
    /// an unsubscribe-all command had nothing to unsubscribe from.
    pub(crate) channel: Option<Vec<u8>>,
}

/// The subscriptions a connection has confirmed, used to resolve how many confirmations a
/// zero-argument `UNSUBSCRIBE`, `PUNSUBSCRIBE` or `SUNSUBSCRIBE` is answered with: the server
/// sends one confirmation per active subscription, and the reply doesn't carry that count.
/// See <https://github.com/redis-rs/redis-rs/issues/2423>.
///
/// Confirmations are applied in order, so the tracked sets match the server whenever a command's
/// first confirmation arrives. They can drift apart if the server drops subscriptions without
/// per-channel confirmations - the known case is the RESP3 `RESET` command - which makes a
/// zero-argument unsubscribe resolve the wrong count.
///
/// A shared instance (`Arc<Mutex<_>>`) is written by the connection(s) that read the
/// confirmations; its other holders only read it. A cluster connection shares one instance between
/// all of its node connections, so it tracks the cluster's subscriptions in one place.
#[derive(Default)]
pub(crate) struct SubscriptionTracker {
    subscriptions: HashSet<Vec<u8>>,
    s_subscriptions: HashSet<Vec<u8>>,
    p_subscriptions: HashSet<Vec<u8>>,
}

pub(crate) enum SubscriptionAction {
    Subscribe,
    Unsubscribe,
    PSubscribe,
    PUnsubscribe,
    SSubscribe,
    Sunsubscribe,
}

impl SubscriptionAction {
    fn additive(&self) -> bool {
        match self {
            Self::Subscribe | Self::PSubscribe | Self::SSubscribe => true,

            Self::Unsubscribe | Self::PUnsubscribe | Self::Sunsubscribe => false,
        }
    }
}

impl SubscriptionTracker {
    #[cfg(test)]
    pub(crate) fn to_request(
        cmd: &Cmd,
    ) -> Option<(SubscriptionAction, impl Iterator<Item = Vec<u8>>)> {
        let mut args_iter = cmd.args_iter();
        let first_arg = args_iter.next()?;

        let first_arg = match first_arg {
            Arg::Simple(arg) => arg,
            Arg::Cursor => return None,
        };
        let first_arg = str::from_utf8(first_arg).ok()?;

        let args = args_iter.filter_map(|arg| match arg {
            Arg::Simple(arg) => Some(arg.to_vec()),
            Arg::Cursor => None,
        });

        let action = if first_arg.eq_ignore_ascii_case("SUBSCRIBE") {
            SubscriptionAction::Subscribe
        } else if first_arg.eq_ignore_ascii_case("PSUBSCRIBE") {
            SubscriptionAction::PSubscribe
        } else if first_arg.eq_ignore_ascii_case("SSUBSCRIBE") {
            SubscriptionAction::SSubscribe
        } else if first_arg.eq_ignore_ascii_case("UNSUBSCRIBE") {
            SubscriptionAction::Unsubscribe
        } else if first_arg.eq_ignore_ascii_case("PUNSUBSCRIBE") {
            SubscriptionAction::PUnsubscribe
        } else if first_arg.eq_ignore_ascii_case("SUNSUBSCRIBE") {
            SubscriptionAction::Sunsubscribe
        } else {
            return None;
        };
        Some((action, args))
    }

    pub(crate) fn update_with_request(
        &mut self,
        action: SubscriptionAction,
        args: impl Iterator<Item = Vec<u8>>,
    ) {
        let set = match action {
            SubscriptionAction::Subscribe | SubscriptionAction::Unsubscribe => {
                &mut self.subscriptions
            }
            SubscriptionAction::PSubscribe | SubscriptionAction::PUnsubscribe => {
                &mut self.p_subscriptions
            }
            SubscriptionAction::SSubscribe | SubscriptionAction::Sunsubscribe => {
                &mut self.s_subscriptions
            }
        };

        if action.additive() {
            for sub in args {
                set.insert(sub);
            }
        } else {
            for sub in args {
                set.remove(&sub);
            }
        }
    }

    /// Applies a server confirmation to the tracked subscriptions.
    ///
    /// A confirmation that carries no channel (the nil channel the server sends when an
    /// unsubscribe-all had nothing to unsubscribe from) does not change the tracked state.
    pub(crate) fn apply(&mut self, change: SubscriptionChange) {
        let action = match (change.kind, change.subscribed) {
            (SubscriptionKind::Channel, true) => SubscriptionAction::Subscribe,
            (SubscriptionKind::Channel, false) => SubscriptionAction::Unsubscribe,
            (SubscriptionKind::Pattern, true) => SubscriptionAction::PSubscribe,
            (SubscriptionKind::Pattern, false) => SubscriptionAction::PUnsubscribe,
            (SubscriptionKind::ShardChannel, true) => SubscriptionAction::SSubscribe,
            (SubscriptionKind::ShardChannel, false) => SubscriptionAction::Sunsubscribe,
        };
        if let Some(channel) = change.channel {
            self.update_with_request(action, core::iter::once(channel));
        }
    }

    /// The number of subscriptions of the given kind that the server was aware of when it
    /// started confirming an unsubscribe-all command. Always at least 1, since the server sends
    /// a single confirmation with a nil channel when there's nothing to unsubscribe from.
    pub(crate) fn unsubscribe_all_target(&self, kind: SubscriptionKind) -> usize {
        let subscriptions = match kind {
            SubscriptionKind::Channel => &self.subscriptions,
            SubscriptionKind::Pattern => &self.p_subscriptions,
            SubscriptionKind::ShardChannel => &self.s_subscriptions,
        };
        subscriptions.len().max(1)
    }

    #[cfg(any(feature = "connection-manager", test))]
    pub(crate) fn get_subscription_pipeline(&self) -> Pipeline {
        let mut pipeline = crate::pipe();
        append_subscription_command(
            &mut pipeline,
            "SUBSCRIBE",
            self.subscriptions.iter().map(Vec::as_slice),
        );
        append_subscription_command(
            &mut pipeline,
            "SSUBSCRIBE",
            self.s_subscriptions.iter().map(Vec::as_slice),
        );
        append_subscription_command(
            &mut pipeline,
            "PSUBSCRIBE",
            self.p_subscriptions.iter().map(Vec::as_slice),
        );
        pipeline
    }

    /// Builds the resubscription pipeline for the subscriptions that `owner` maps to `target`.
    ///
    /// Cluster connections use this to resubscribe a single reconnected node without building the
    /// pipelines of the other nodes.
    #[cfg(any(feature = "cluster-async", test))]
    pub(crate) fn get_subscription_pipeline_for_owner<K>(
        &self,
        target: &K,
        mut owner: impl FnMut(&[u8]) -> Option<K>,
    ) -> Pipeline
    where
        K: Eq,
    {
        let mut channels = Vec::new();
        let mut shard_channels = Vec::new();
        let mut patterns = Vec::new();

        for subscription in &self.subscriptions {
            if owner(subscription).as_ref() == Some(target) {
                channels.push(subscription.as_slice());
            }
        }
        for subscription in &self.s_subscriptions {
            if owner(subscription).as_ref() == Some(target) {
                shard_channels.push(subscription.as_slice());
            }
        }
        for subscription in &self.p_subscriptions {
            if owner(subscription).as_ref() == Some(target) {
                patterns.push(subscription.as_slice());
            }
        }

        let mut pipeline = crate::pipe();
        append_subscription_command(&mut pipeline, "SUBSCRIBE", channels);
        append_subscription_command(&mut pipeline, "SSUBSCRIBE", shard_channels);
        append_subscription_command(&mut pipeline, "PSUBSCRIBE", patterns);
        pipeline
    }

    /// Builds one resubscription pipeline per owner that `owner` maps a tracked subscription to.
    ///
    /// Cluster connections use this to build a pipeline for each node from the subscriptions
    /// whose slots the node currently owns. Subscriptions without an owner are skipped.
    #[cfg(any(feature = "cluster-async", test))]
    pub(crate) fn get_subscription_pipelines_by<K>(
        &self,
        mut owner: impl FnMut(&[u8]) -> Option<K>,
    ) -> HashMap<K, Pipeline>
    where
        K: Eq + std::hash::Hash,
    {
        let mut groups: HashMap<K, [Vec<&[u8]>; 3]> = HashMap::new();
        for subscription in &self.subscriptions {
            if let Some(owner) = owner(subscription) {
                groups.entry(owner).or_default()[0].push(subscription.as_slice());
            }
        }
        for subscription in &self.s_subscriptions {
            if let Some(owner) = owner(subscription) {
                groups.entry(owner).or_default()[1].push(subscription.as_slice());
            }
        }
        for subscription in &self.p_subscriptions {
            if let Some(owner) = owner(subscription) {
                groups.entry(owner).or_default()[2].push(subscription.as_slice());
            }
        }

        groups
            .into_iter()
            .map(|(owner, [channels, shard_channels, patterns])| {
                let mut pipeline = crate::pipe();
                append_subscription_command(&mut pipeline, "SUBSCRIBE", channels);
                append_subscription_command(&mut pipeline, "SSUBSCRIBE", shard_channels);
                append_subscription_command(&mut pipeline, "PSUBSCRIBE", patterns);
                (owner, pipeline)
            })
            .collect()
    }
}

#[cfg(any(feature = "connection-manager", feature = "cluster-async", test))]
fn append_subscription_command<'a>(
    pipeline: &mut Pipeline,
    command: &str,
    subscriptions: impl IntoIterator<Item = &'a [u8]>,
) {
    let mut subscriptions = subscriptions.into_iter().peekable();
    if subscriptions.peek().is_none() {
        return;
    }

    let cmd = pipeline.cmd(command);
    for subscription in subscriptions {
        cmd.arg(subscription);
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
                crate::PushKind::SSubscribe => (SubscriptionKind::ShardChannel, true),
                crate::PushKind::SUnsubscribe => (SubscriptionKind::ShardChannel, false),
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
                    b"ssubscribe" => (SubscriptionKind::ShardChannel, true),
                    b"sunsubscribe" => (SubscriptionKind::ShardChannel, false),
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

#[cfg(test)]
mod tests {
    use crate::{PushKind, Value, cmd, pipe};

    use super::*;

    #[test]
    fn test_add_and_remove_subscriptions() {
        let mut tracker = SubscriptionTracker::default();

        tracker.update_with_cmd(cmd("subscribe").arg("foo").arg("bar"));
        tracker.update_with_cmd(cmd("PSUBSCRIBE").arg("fo*o").arg("b*ar"));
        tracker.update_with_cmd(cmd("SSUBSCRIBE").arg("sfoo").arg("sbar"));
        tracker.update_with_cmd(cmd("unsubscribe").arg("foo"));
        tracker.update_with_cmd(cmd("Punsubscribe").arg("b*ar"));
        tracker.update_with_cmd(cmd("Sunsubscribe").arg("sfoo").arg("SBAR"));
        // ignore irrelevant commands
        tracker.update_with_cmd(cmd("GET").arg("sfoo"));

        let result = tracker.get_subscription_pipeline();
        let mut expected = pipe();
        expected
            .cmd("SUBSCRIBE")
            .arg("bar")
            .cmd("SSUBSCRIBE")
            .arg("sbar")
            .cmd("PSUBSCRIBE")
            .arg("fo*o");
        assert_eq!(
            result.get_packed_pipeline(),
            expected.get_packed_pipeline(),
            "{}",
            String::from_utf8(result.get_packed_pipeline()).unwrap()
        );
    }

    #[test]
    fn test_skip_empty_subscriptions() {
        let mut tracker = SubscriptionTracker::default();

        tracker.update_with_cmd(cmd("subscribe").arg("foo").arg("bar"));
        tracker.update_with_cmd(cmd("PSUBSCRIBE").arg("fo*o").arg("b*ar"));
        tracker.update_with_cmd(cmd("unsubscribe").arg("foo").arg("bar"));
        tracker.update_with_cmd(cmd("punsubscribe").arg("fo*o"));

        let result = tracker.get_subscription_pipeline();
        let mut expected = pipe();
        expected.cmd("PSUBSCRIBE").arg("b*ar");
        assert_eq!(
            result.get_packed_pipeline(),
            expected.get_packed_pipeline(),
            "{}",
            String::from_utf8(result.get_packed_pipeline()).unwrap()
        );
    }

    #[test]
    fn test_only_unsubscribe_from_existing_subscriptions() {
        let mut tracker = SubscriptionTracker::default();

        tracker.update_with_cmd(cmd("unsubscribe").arg("foo"));
        tracker.update_with_cmd(cmd("subscribe").arg("foo"));

        let result = tracker.get_subscription_pipeline();
        let mut expected = pipe();
        expected.cmd("SUBSCRIBE").arg("foo");
        assert_eq!(
            result.get_packed_pipeline(),
            expected.get_packed_pipeline(),
            "{}",
            String::from_utf8(result.get_packed_pipeline()).unwrap()
        );
    }

    #[test]
    fn test_group_subscriptions_by_owner() {
        let mut tracker = SubscriptionTracker::default();

        tracker.update_with_cmd(cmd("subscribe").arg("foo").arg("bar"));
        tracker.update_with_cmd(cmd("PSUBSCRIBE").arg("fo*o").arg("b*ar"));
        tracker.update_with_cmd(cmd("SSUBSCRIBE").arg("sfoo").arg("sbar"));

        let result = tracker.get_subscription_pipelines_by(|subscription| {
            subscription.contains(&b'b').then_some(())
        });
        assert_eq!(result.len(), 1);
        let result = result.get(&()).unwrap();
        let mut expected = pipe();
        expected
            .cmd("SUBSCRIBE")
            .arg("bar")
            .cmd("SSUBSCRIBE")
            .arg("sbar")
            .cmd("PSUBSCRIBE")
            .arg("b*ar");
        assert_eq!(
            result.get_packed_pipeline(),
            expected.get_packed_pipeline(),
            "{}",
            String::from_utf8(result.get_packed_pipeline()).unwrap()
        );

        let only_b = tracker.get_subscription_pipeline_for_owner(&(), |subscription| {
            subscription.contains(&b'b').then_some(())
        });
        assert_eq!(only_b.get_packed_pipeline(), expected.get_packed_pipeline());

        let empty = tracker.get_subscription_pipelines_by(|_| None::<()>);
        assert!(empty.is_empty());
    }

    impl SubscriptionTracker {
        fn update_with_cmd<'a>(&'a mut self, cmd: &'a Cmd) {
            if let Some((action, args)) = Self::to_request(cmd) {
                self.update_with_request(action, args);
            }
        }
    }

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

    /// Fails when a subscription kind is added to `PushKind` without being handled by
    /// `subscription_change`: it would stop being tracked and corrupt the confirmation counts
    /// that `SubscriptionTracker` resolves.
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
            (
                PushKind::SSubscribe,
                SubscriptionKind::ShardChannel,
                true,
                b"ssubscribe".as_slice(),
            ),
            (
                PushKind::SUnsubscribe,
                SubscriptionKind::ShardChannel,
                false,
                b"sunsubscribe".as_slice(),
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
        let mut subscriptions = SubscriptionTracker::default();

        // With nothing tracked, the server still sends a single confirmation with a nil channel.
        for kind in [
            SubscriptionKind::Channel,
            SubscriptionKind::Pattern,
            SubscriptionKind::ShardChannel,
        ] {
            assert_eq!(subscriptions.unsubscribe_all_target(kind), 1);
        }

        // Each kind is tracked independently.
        let kinds = [
            (SubscriptionKind::Channel, b"channel".as_slice()),
            (SubscriptionKind::Pattern, b"pattern".as_slice()),
            (SubscriptionKind::ShardChannel, b"shard".as_slice()),
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
        assert_eq!(
            subscriptions.unsubscribe_all_target(SubscriptionKind::ShardChannel),
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
