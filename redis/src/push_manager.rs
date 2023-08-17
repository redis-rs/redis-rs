use crate::{ErrorKind, FromRedisValue, PushKind, RedisError, RedisResult, Value};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::Arc;

/// Holds information about received Push data
#[derive(Debug, Clone)]
pub struct PushInfo {
    /// Push Kind
    pub kind: PushKind,
    /// Data from push message
    pub data: Vec<Value>,

    /// Connection address to distinguish connections
    pub con_addr: Arc<String>,
}

/// `PushSender` holds multiple types of mpsc channels
#[derive(Clone)]
pub enum PushSender {
    /// Tokio mpsc UnboundedSender
    #[cfg(feature = "aio")]
    Tokio(tokio::sync::mpsc::UnboundedSender<PushInfo>),
    /// Standard mpsc SyncSender
    Standard(SyncSender<PushInfo>),
}
#[derive(Clone)]
struct ChannelSubscription {
    channel_id: usize,
    sender: PushSender,
}

#[derive(Default)]
struct SubscriptionHolder {
    subscriptions: Arc<ArcSwap<HashMap<String, Vec<ChannelSubscription>>>>,
    last_channel_id: Arc<AtomicUsize>,
}

impl SubscriptionHolder {
    fn subscribe_to_channel(&self, channel_name: String, sender: PushSender) -> usize {
        let channel_id = self.last_channel_id.fetch_add(1, Ordering::SeqCst);
        let cs = ChannelSubscription { channel_id, sender };
        self.subscriptions.rcu(|x| {
            let mut subscriptions = HashMap::clone(x);
            if let Some(sbs) = subscriptions.get_mut(&channel_name) {
                sbs.push(cs.clone());
            } else {
                subscriptions.insert(channel_name.clone(), vec![cs.clone()]);
            }
            subscriptions
        });
        channel_id
    }
    fn unsubscribe_from_channel(&self, channel_name: String, channel_id: Option<usize>) -> bool {
        let mut unsubscribed_from_all = false;
        self.subscriptions.rcu(|x| {
            unsubscribed_from_all = false;
            let mut subscriptions = HashMap::clone(x);
            if let Some(sbs) = subscriptions.get_mut(&channel_name) {
                if let Some(channel_id) = channel_id {
                    sbs.retain(|cs| cs.channel_id != channel_id)
                } else {
                    sbs.clear();
                }
                if sbs.is_empty() {
                    subscriptions.remove(&channel_name);
                    unsubscribed_from_all = true;
                }
            }
            subscriptions
        });
        unsubscribed_from_all
    }
    fn try_send(&self, val: &Value, con_addr: &Arc<String>) -> RedisResult<()> {
        let channel_name: String;
        if let Value::Push { kind, data } = val {
            let all_subscriptions = self.subscriptions.load();
            if data.is_empty() {
                return Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "At least one data is expected from this push information",
                )));
            }
            match kind {
                &PushKind::Message | &PushKind::PMessage | &PushKind::SMessage => {
                    channel_name = FromRedisValue::from_redis_value(&data[0])?
                }
                _ => {
                    return Err(RedisError::from((
                        ErrorKind::ResponseError,
                        "PushKind is unknown",
                    )));
                }
            };
            let subscriptions = all_subscriptions.get(&channel_name);
            if let Some(subscriptions) = subscriptions {
                for subscription in subscriptions {
                    let push_info = PushInfo {
                        kind: kind.clone(),
                        data: data.clone(),
                        con_addr: con_addr.clone(),
                    };
                    let is_closed = match &subscription.sender {
                        #[cfg(feature = "aio")]
                        PushSender::Tokio(tokio_sender) => tokio_sender.send(push_info).is_err(),
                        PushSender::Standard(std_sender) => std_sender
                            .try_send(push_info)
                            .is_err_and(|err| matches!(err, TrySendError::Disconnected(_))),
                    };
                    if is_closed {
                        //TODO check if it causes any problem when guard is hold
                        self.unsubscribe_from_channel(
                            channel_name.clone(),
                            Some(subscription.channel_id),
                        );
                    }
                }
            }
        }

        Ok(())
    }
}
/// Manages Push messages for both tokio and std channels
#[derive(Clone, Default)]
pub struct PushManager {
    senders: Arc<ArcSwap<HashMap<PushKind, Vec<PushSender>>>>,
    subscriptions: Arc<SubscriptionHolder>,
    psubscriptions: Arc<SubscriptionHolder>,
}
impl PushManager {
    /// Try to send `PushInfo` to mpsc channel without blocking
    pub(crate) fn send(&self, pi: PushInfo) {
        if let Some(senders) = self.senders.load().get(&pi.kind) {
            let mut indexes_to_remove = vec![];
            for (i,sender) in senders.iter().enumerate() {
                let is_closed = match sender {
                    #[cfg(feature = "aio")]
                    PushSender::Tokio(tokio_sender) => tokio_sender.send(pi.clone()).is_err(),
                    PushSender::Standard(std_sender) => std_sender
                        .try_send(pi.clone())
                        .is_err_and(|err| matches!(err, TrySendError::Disconnected(_))),
                };
                if is_closed {
                    indexes_to_remove.push(i);
                }
            }
            if !indexes_to_remove.is_empty() {
                self.unsubscribe(pi.kind, &indexes_to_remove);
            }
        }
    }

    /// Checks if `PushManager` has provided any channel.
    pub(crate) fn has_sender(&self, push_kind: &PushKind) -> bool {
        self.senders.load().contains_key(push_kind)
    }

    /// It checks if value's type is Push
    /// then it is checks Push's kind to see if there is any provided channel
    /// then creates PushInfo and invoke `send` method
    pub(crate) fn try_send(&self, value: &RedisResult<Value>, con_addr: &Arc<String>)  {
        if let Ok(value) = &value {
            self.try_send_raw(value, con_addr);
        }
    }

    pub(crate) fn try_send_raw(&self, value: &Value, con_addr: &Arc<String>)  {
        if let Value::Push { kind, data } = value {
            if kind == &PushKind::Message {
                let _ = self.subscriptions.try_send(value, con_addr);
            } else if kind == &PushKind::PMessage {
                let _ = self.psubscriptions.try_send(value, con_addr);
            };
            if self.has_sender(kind) {
                return self.send(PushInfo {
                    kind: kind.clone(),
                    data: data.clone(),
                    con_addr: con_addr.clone(),
                });
            }
        }
    }

    /// Creates new `PushManager`
    pub fn new() -> Self {
        PushManager {
            senders: Arc::from(ArcSwap::from(Arc::from(HashMap::new()))),
            ..Default::default()
        }
    }

    /// Subscribes to a `PushKind`
    pub fn subscribe(&self, push_kind: PushKind, sender_type: PushSender) -> &Self {
        self.senders.rcu(|x| {
            let mut cache = HashMap::clone(x);
            if let Some(sbs) = cache.get_mut(&push_kind) {
                sbs.push(sender_type.clone());
            } else {
                cache.insert(push_kind.clone(), vec![sender_type.clone()]);
            }
            cache
        });
        self
    }

    fn unsubscribe(&self, push_kind: PushKind, indexes: &[usize]) -> &Self {
        self.senders.rcu(|x| {
            let mut cache = HashMap::clone(x);
            if let Some(senders) = cache.get_mut(&push_kind) {
                let mut i: usize = 0;
                senders.retain(|_| {
                    let contains = indexes.contains(&i);
                    i+=1;
                    !contains
                });
            }
            cache
        });
        self
    }

    pub(crate) fn pb_subscribe(&self, channel_name: String, sender_type: PushSender) -> usize {
        self.subscriptions
            .subscribe_to_channel(channel_name, sender_type)
    }
    pub(crate) fn pb_unsubscribe(&self, channel_name: String, channel_id: Option<usize>) -> bool {
        self.subscriptions
            .unsubscribe_from_channel(channel_name, channel_id)
    }
    pub(crate) fn pb_psubscribe(&self, channel_name: String, sender_type: PushSender) -> usize {
        self.psubscriptions
            .subscribe_to_channel(channel_name, sender_type)
    }
    pub(crate) fn pb_punsubscribe(&self, channel_name: String, channel_id: Option<usize>) -> bool {
        self.psubscriptions
            .unsubscribe_from_channel(channel_name, channel_id)
    }
}
