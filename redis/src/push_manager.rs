use crate::{PushKind, RedisResult, Value};
use arc_swap::ArcSwap;
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

/// Manages Push messages for both tokio and std channels
#[derive(Clone, Default)]
pub struct PushManager {
    sender: Arc<ArcSwap<Option<tokio::sync::mpsc::UnboundedSender<PushInfo>>>>,
}
impl PushManager {
    /// Try to send `PushInfo` to mpsc channel without blocking
    pub(crate) fn send(&self, pi: PushInfo) {
        let guard = self.sender.load();
        if let Some(sender) = &**guard {
            if sender.send(pi).is_err() {
                self.sender.compare_and_swap(guard, Arc::new(None));
            }
        }
    }

    ///
    pub fn replace_sender(&self, sender: tokio::sync::mpsc::UnboundedSender<PushInfo>) {
        self.sender.store(Arc::new(Some(sender)));
    }

    /// Checks if `PushManager` has provided any channel.
    pub(crate) fn has_sender(&self) -> bool {
        self.sender.load().is_some()
    }

    /// It checks if value's type is Push
    /// then it is checks Push's kind to see if there is any provided channel
    /// then creates PushInfo and invoke `send` method
    pub(crate) fn try_send(&self, value: &RedisResult<Value>, con_addr: &Arc<String>) {
        if let Ok(value) = &value {
            self.try_send_raw(value, con_addr);
        }
    }

    pub(crate) fn try_send_raw(&self, value: &Value, con_addr: &Arc<String>) {
        if let Value::Push { kind, data } = value {
            if self.has_sender() {
                self.send(PushInfo {
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
            sender: Arc::from(ArcSwap::from(Arc::from(None))),
        }
    }
}
