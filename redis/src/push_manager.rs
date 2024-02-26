use crate::{PushKind, RedisResult, Value};
use arc_swap::ArcSwap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Holds information about received Push data
#[derive(Debug, Clone)]
pub struct PushInfo {
    /// Push Kind
    pub kind: PushKind,
    /// Data from push message
    pub data: Vec<Value>,
}

/// Manages Push messages for single tokio channel
#[derive(Clone, Default)]
pub struct PushManager {
    sender: Arc<ArcSwap<Option<mpsc::UnboundedSender<PushInfo>>>>,
}
impl PushManager {
    /// It checks if value's type is Push
    /// then invokes `try_send_raw` method
    pub(crate) fn try_send(&self, value: &RedisResult<Value>) {
        if let Ok(value) = &value {
            self.try_send_raw(value);
        }
    }

    /// It checks if value's type is Push and there is a provided sender
    /// then creates PushInfo and invokes `send` method of sender
    pub(crate) fn try_send_raw(&self, value: &Value) {
        if let Value::Push { kind, data } = value {
            let guard = self.sender.load();
            if let Some(sender) = guard.as_ref() {
                let push_info = PushInfo {
                    kind: kind.clone(),
                    data: data.clone(),
                };
                if sender.send(push_info).is_err() {
                    self.sender.compare_and_swap(guard, Arc::new(None));
                }
            }
        }
    }
    /// Replace mpsc channel of `PushManager` with provided sender.
    pub fn replace_sender(&self, sender: mpsc::UnboundedSender<PushInfo>) {
        self.sender.store(Arc::new(Some(sender)));
    }

    /// Creates new `PushManager`
    pub fn new() -> Self {
        PushManager {
            sender: Arc::from(ArcSwap::from(Arc::new(None))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_and_receive_push_info() {
        let push_manager = PushManager::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        push_manager.replace_sender(tx);

        let value = Ok(Value::Push {
            kind: PushKind::Message,
            data: vec![Value::BulkString("hello".to_string().into_bytes())],
        });

        push_manager.try_send(&value);

        let push_info = rx.try_recv().unwrap();
        assert_eq!(push_info.kind, PushKind::Message);
        assert_eq!(
            push_info.data,
            vec![Value::BulkString("hello".to_string().into_bytes())]
        );
    }
    #[test]
    fn test_push_manager_receiver_dropped() {
        let push_manager = PushManager::new();
        let (tx, rx) = mpsc::unbounded_channel();
        push_manager.replace_sender(tx);

        let value = Ok(Value::Push {
            kind: PushKind::Message,
            data: vec![Value::BulkString("hello".to_string().into_bytes())],
        });

        drop(rx);

        push_manager.try_send(&value);
        push_manager.try_send(&value);
        push_manager.try_send(&value);
    }
    #[test]
    fn test_push_manager_without_sender() {
        let push_manager = PushManager::new();

        push_manager.try_send(&Ok(Value::Push {
            kind: PushKind::Message,
            data: vec![Value::BulkString("hello".to_string().into_bytes())],
        })); // nothing happens!

        let (tx, mut rx) = mpsc::unbounded_channel();
        push_manager.replace_sender(tx);
        push_manager.try_send(&Ok(Value::Push {
            kind: PushKind::Message,
            data: vec![Value::BulkString("hello2".to_string().into_bytes())],
        }));

        assert_eq!(
            rx.try_recv().unwrap().data,
            vec![Value::BulkString("hello2".to_string().into_bytes())]
        );
    }
    #[test]
    fn test_push_manager_multiple_channels_and_messages() {
        let push_manager = PushManager::new();
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        push_manager.replace_sender(tx1);

        let value1 = Ok(Value::Push {
            kind: PushKind::Message,
            data: vec![Value::Int(1)],
        });

        let value2 = Ok(Value::Push {
            kind: PushKind::Message,
            data: vec![Value::Int(2)],
        });

        push_manager.try_send(&value1);
        push_manager.try_send(&value2);

        assert_eq!(rx1.try_recv().unwrap().data, vec![Value::Int(1)]);
        assert_eq!(rx1.try_recv().unwrap().data, vec![Value::Int(2)]);

        push_manager.replace_sender(tx2);
        // make sure rx1 is disconnected after replacing tx1 with tx2.
        assert_eq!(
            rx1.try_recv().err().unwrap(),
            mpsc::error::TryRecvError::Disconnected
        );

        push_manager.try_send(&value1);
        push_manager.try_send(&value2);

        assert_eq!(rx2.try_recv().unwrap().data, vec![Value::Int(1)]);
        assert_eq!(rx2.try_recv().unwrap().data, vec![Value::Int(2)]);
    }

    #[tokio::test]
    async fn test_push_manager_multi_threaded() {
        // In this test we create 4 channels and send 1000 message, it switchs channels for each message we sent.
        // Then we check if all messages are received and sum of messages are equal to expected sum.
        // We also check if all channels are used.
        let push_manager = PushManager::new();
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        let (tx3, mut rx3) = mpsc::unbounded_channel();
        let (tx4, mut rx4) = mpsc::unbounded_channel();

        let mut handles = vec![];
        let txs = [tx1, tx2, tx3, tx4];
        let mut expected_sum = 0;
        for i in 0..1000 {
            expected_sum += i;
            let push_manager_clone = push_manager.clone();
            let new_tx = txs[(i % 4) as usize].clone();
            let value = Ok(Value::Push {
                kind: PushKind::Message,
                data: vec![Value::Int(i)],
            });
            let handle = tokio::spawn(async move {
                push_manager_clone.replace_sender(new_tx);
                push_manager_clone.try_send(&value);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let mut count1 = 0;
        let mut count2 = 0;
        let mut count3 = 0;
        let mut count4 = 0;
        let mut received_sum = 0;
        while let Ok(push_info) = rx1.try_recv() {
            assert_eq!(push_info.kind, PushKind::Message);
            if let Value::Int(i) = push_info.data[0] {
                received_sum += i;
            }
            count1 += 1;
        }
        while let Ok(push_info) = rx2.try_recv() {
            assert_eq!(push_info.kind, PushKind::Message);
            if let Value::Int(i) = push_info.data[0] {
                received_sum += i;
            }
            count2 += 1;
        }

        while let Ok(push_info) = rx3.try_recv() {
            assert_eq!(push_info.kind, PushKind::Message);
            if let Value::Int(i) = push_info.data[0] {
                received_sum += i;
            }
            count3 += 1;
        }

        while let Ok(push_info) = rx4.try_recv() {
            assert_eq!(push_info.kind, PushKind::Message);
            if let Value::Int(i) = push_info.data[0] {
                received_sum += i;
            }
            count4 += 1;
        }

        assert_ne!(count1, 0);
        assert_ne!(count2, 0);
        assert_ne!(count3, 0);
        assert_ne!(count4, 0);

        assert_eq!(count1 + count2 + count3 + count4, 1000);
        assert_eq!(received_sum, expected_sum);
    }
}
