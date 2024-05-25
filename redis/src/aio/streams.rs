use crate::{Msg, PushInfo, PushKind};
use tokio::sync::mpsc::UnboundedReceiver;

/// Returns a Stream of [`PushInfo`]s from this [`MultiplexedConnection`]s PushManager
/// This struct is basically a wrapper for `PushInfo` receiver that converts it into a Stream.
pub struct PushStream {
    pub(crate) rx: UnboundedReceiver<PushInfo>,
}

impl PushStream {
    pub async fn next(&mut self) -> Option<PushInfo> {
        self.rx.recv().await
    }
}

/// Returns [`Stream`] of [`Msg`]s from this [`MultiplexedConnection`]s `PushStream`.
/// `MsgStream` wraps `PushStream` and filters incoming stream whether it's PubSub Message or not.
pub struct MsgStream {
    ps: PushStream,
}

impl From<PushStream> for MsgStream {
    fn from(value: PushStream) -> Self {
        MsgStream { ps: value }
    }
}

impl MsgStream {
    pub async fn next(&mut self) -> Option<Msg> {
        loop {
            if let Some(push_info) = self.ps.next().await {
                if push_info.kind == PushKind::Disconnection {
                    return None;
                }
                if let Some(msg) = Msg::from_push_info(&push_info) {
                    return Some(msg);
                }
            }
        }
    }
}
