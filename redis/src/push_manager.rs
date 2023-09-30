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
    /// It checks if value's type is Push
    /// then invokes `try_send_raw` method
    pub(crate) fn try_send(&self, value: &RedisResult<Value>, con_addr: &Arc<String>) {
        if let Ok(value) = &value {
            self.try_send_raw(value, con_addr);
        }
    }

    /// It checks if value's type is Push and there is a provided sender
    /// then creates PushInfo and invokes `send` method of sender
    pub(crate) fn try_send_raw(&self, value: &Value, con_addr: &Arc<String>) {
        if let Value::Push { kind, data } = value {
            let guard = self.sender.load();
            if let Some(sender) = guard.as_ref() {
                if sender
                    .send(PushInfo {
                        kind: kind.clone(),
                        data: data.clone(),
                        con_addr: con_addr.clone(),
                    })
                    .is_err()
                {
                    self.sender.compare_and_swap(guard, Arc::new(None));
                }
            }
        }
    }
    /// Replace mpsc channel of `PushManager` with provided sender.
    pub fn replace_sender(&self, sender: tokio::sync::mpsc::UnboundedSender<PushInfo>) {
        self.sender.store(Arc::new(Some(sender)));
    }

    /// Creates new `PushManager`
    pub fn new() -> Self {
        PushManager {
            sender: Arc::from(ArcSwap::from(Arc::new(None))),
        }
    }
}
