use crate::{PushKind, RedisResult, Value};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::mpsc::SyncSender;
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
/// `PushSenderType` holds multiple types of mpsc channels
#[derive(Clone)]
pub enum PushSenderType {
    /// Tokio mpsc Sender
    #[cfg(feature = "tokio-comp")]
    Tokio(tokio::sync::mpsc::Sender<PushInfo>),
    /// Standard mpsc SyncSender
    Standard(SyncSender<PushInfo>),
}

/// Manages Push messages for both tokio and std channels
#[derive(Clone, Default)]
pub struct PushManager {
    sender: Arc<ArcSwap<HashMap<PushKind, PushSenderType>>>,
}
impl PushManager {
    /// Try to send `PushInfo` to mpsc channel without blocking
    /// if the function returns false it means
    /// `PushManager` has no channel for PushKind or channel is full.
    pub fn send(&self, pi: PushInfo) -> bool {
        if let Some(sender) = self.sender.load().get(&pi.kind) {
            return match sender {
                #[cfg(feature = "tokio-comp")]
                PushSenderType::Tokio(tokio_sender) => tokio_sender.try_send(pi).is_ok(),
                PushSenderType::Standard(std_sender) => std_sender.try_send(pi).is_ok(),
            };
        }
        false
    }

    /// Checks if `PushManager` has provided any channel.
    pub fn has_sender(&self, push_kind: &PushKind) -> bool {
        self.sender.load().contains_key(push_kind)
    }

    /// It checks if value's type is Push
    /// then it is checks Push's kind to see if there is any provided channel
    /// then creates PushInfo and invoke `send` method
    pub fn try_send(&self, value: &RedisResult<Value>, con_addr: &Arc<String>) -> bool {
        if let Ok(Value::Push { kind, data }) = &value {
            if self.has_sender(kind) {
                return self.send(PushInfo {
                    kind: kind.clone(),
                    data: data.clone(),
                    con_addr: con_addr.clone(),
                });
            }
        }
        false
    }

    /// Creates new `PushManager`
    pub fn new() -> Self {
        PushManager {
            sender: Arc::from(ArcSwap::from(Arc::from(HashMap::new()))),
        }
    }

    /// Subscribes to a `PushKind`
    pub fn subscribe(&self, push_kind: PushKind, sender_type: PushSenderType) -> &Self {
        self.sender.rcu(|x| {
            let mut cache = HashMap::clone(x);
            cache.insert(push_kind.clone(), sender_type.clone());
            cache
        });
        self
    }

    /// Unsubscribes from a `PushKind`
    pub fn unsubscribe(&self, push_kind: PushKind) -> &Self {
        self.sender.rcu(|x| {
            let mut cache = HashMap::clone(x);
            cache.remove(&push_kind);
            cache
        });
        self
    }
}
