use std::{os::unix::net::UnixStream, path::Path};

use crate::RedisResult;

pub struct UnixConnection {
    pub(super) sock: UnixStream,
    pub(super) open: bool,
}

impl UnixConnection {
    pub(super) fn try_new(path: impl AsRef<Path>) -> RedisResult<Self> {
        Ok(Self {
            sock: UnixStream::connect(path)?,
            open: true,
        })
    }
}
