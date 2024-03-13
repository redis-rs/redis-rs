use std::{io, time::Duration};

use futures_util::Future;

#[cfg(feature = "async-std-comp")]
use super::async_std;
#[cfg(feature = "tokio-comp")]
use super::tokio;
use super::RedisRuntime;
use crate::types::RedisError;

#[derive(Clone, Debug)]
pub(crate) enum Runtime {
    #[cfg(feature = "tokio-comp")]
    Tokio,
    #[cfg(feature = "async-std-comp")]
    AsyncStd,
}

impl Runtime {
    pub(crate) fn locate() -> Self {
        #[cfg(all(feature = "tokio-comp", not(feature = "async-std-comp")))]
        {
            Runtime::Tokio
        }

        #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
        {
            Runtime::AsyncStd
        }

        #[cfg(all(feature = "tokio-comp", feature = "async-std-comp"))]
        {
            if ::tokio::runtime::Handle::try_current().is_ok() {
                Runtime::Tokio
            } else {
                Runtime::AsyncStd
            }
        }

        #[cfg(all(not(feature = "tokio-comp"), not(feature = "async-std-comp")))]
        {
            compile_error!("tokio-comp or async-std-comp features required for aio feature")
        }
    }

    #[allow(dead_code)]
    pub(super) fn spawn(&self, f: impl Future<Output = ()> + Send + 'static) {
        match self {
            #[cfg(feature = "tokio-comp")]
            Runtime::Tokio => tokio::Tokio::spawn(f),
            #[cfg(feature = "async-std-comp")]
            Runtime::AsyncStd => async_std::AsyncStd::spawn(f),
        }
    }

    pub(crate) async fn timeout<F: Future>(
        &self,
        duration: Duration,
        future: F,
    ) -> Result<F::Output, Elapsed> {
        match self {
            #[cfg(feature = "tokio-comp")]
            Runtime::Tokio => ::tokio::time::timeout(duration, future)
                .await
                .map_err(|_| Elapsed(())),
            #[cfg(feature = "async-std-comp")]
            Runtime::AsyncStd => ::async_std::future::timeout(duration, future)
                .await
                .map_err(|_| Elapsed(())),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Elapsed(());

impl From<Elapsed> for RedisError {
    fn from(_: Elapsed) -> Self {
        io::Error::from(io::ErrorKind::TimedOut).into()
    }
}
