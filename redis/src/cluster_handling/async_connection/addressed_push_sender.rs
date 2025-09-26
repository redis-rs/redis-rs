use arcstr::ArcStr;
use futures_channel::mpsc::UnboundedSender;

use crate::{
    aio::{AsyncPushSender, SendError},
    PushInfo,
};

pub(super) struct AddressedPushSender {
    address: ArcStr,
    sender: UnboundedSender<(ArcStr, PushInfo)>,
}

impl AddressedPushSender {
    pub(super) fn new(address: ArcStr, sender: UnboundedSender<(ArcStr, PushInfo)>) -> Self {
        Self { address, sender }
    }
}

impl AsyncPushSender for AddressedPushSender {
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        self.sender
            .unbounded_send((self.address.clone(), info))
            .map_err(|_| SendError)
    }
}
