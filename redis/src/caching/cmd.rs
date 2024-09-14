use crate::Value;
use std::time::Duration;
use tokio::sync::watch::Sender;

pub(crate) struct CommandCacheInformationByRef<'a> {
    pub(crate) redis_key: &'a [u8],
    pub(crate) cmd: &'a [u8],
    pub(crate) client_side_ttl: Option<Duration>,
    pub(crate) is_mget: bool,
}

/// This struct is used to hold information during MGET caching process.
pub(crate) struct MultipleCommandCacheInformation {
    pub(crate) redis_key: Vec<u8>,
    pub(crate) cmd: Vec<u8>,
    pub(crate) sender: Option<Sender<Value>>,
    pub(crate) key_index: usize,
}
