use std::{io, sync::Arc, time::Duration};

use futures_util::Future;

#[cfg(feature = "async-std-comp")]
use super::async_std as crate_async_std;
#[cfg(feature = "tokio-comp")]
use super::tokio as crate_tokio;
use super::RedisRuntime;
use crate::types::RedisError;

#[derive(Clone, Debug)]
pub(crate) enum Runtime {
    #[cfg(feature = "tokio-comp")]
    Tokio,
    #[cfg(feature = "async-std-comp")]
    AsyncStd,
}

pub(crate) enum TaskHandle {
    #[cfg(feature = "tokio-comp")]
    Tokio(tokio::task::JoinHandle<()>),
    #[cfg(feature = "async-std-comp")]
    AsyncStd(async_std::task::JoinHandle<()>),
}

pub(crate) struct HandleContainer(Option<TaskHandle>);

impl HandleContainer {
    pub(crate) fn new(handle: TaskHandle) -> Self {
        Self(Some(handle))
    }
}

impl Drop for HandleContainer {
    fn drop(&mut self) {
        match self.0.take() {
            None => {}
            #[cfg(feature = "tokio-comp")]
            Some(TaskHandle::Tokio(handle)) => handle.abort(),
            #[cfg(feature = "async-std-comp")]
            Some(TaskHandle::AsyncStd(handle)) => {
                // schedule for cancellation without waiting for result.
                Runtime::locate().spawn(async move { handle.cancel().await.unwrap_or_default() });
            }
        }
    }
}

#[derive(Clone)]
// we allow dead code here because the container isn't used directly, only in the derived drop.
#[allow(dead_code)]
pub(crate) struct SharedHandleContainer(Arc<HandleContainer>);

impl SharedHandleContainer {
    pub(crate) fn new(handle: TaskHandle) -> Self {
        Self(Arc::new(HandleContainer::new(handle)))
    }
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
    pub(crate) fn spawn(&self, f: impl Future<Output = ()> + Send + 'static) -> TaskHandle {
        match self {
            #[cfg(feature = "tokio-comp")]
            Runtime::Tokio => crate_tokio::Tokio::spawn(f),
            #[cfg(feature = "async-std-comp")]
            Runtime::AsyncStd => crate_async_std::AsyncStd::spawn(f),
        }
    }

    pub(crate) async fn timeout<F: Future>(
        &self,
        duration: Duration,
        future: F,
    ) -> Result<F::Output, Elapsed> {
        match self {
            #[cfg(feature = "tokio-comp")]
            Runtime::Tokio => tokio::time::timeout(duration, future)
                .await
                .map_err(|_| Elapsed(())),
            #[cfg(feature = "async-std-comp")]
            Runtime::AsyncStd => async_std::future::timeout(duration, future)
                .await
                .map_err(|_| Elapsed(())),
        }
    }

    #[cfg(any(feature = "connection-manager", feature = "cluster-async"))]
    pub(crate) async fn sleep(&self, duration: Duration) {
        match self {
            #[cfg(feature = "tokio-comp")]
            Runtime::Tokio => {
                tokio::time::sleep(duration).await;
            }
            #[cfg(feature = "async-std-comp")]
            Runtime::AsyncStd => {
                async_std::task::sleep(duration).await;
            }
        }
    }

    #[cfg(feature = "cluster-async")]
    pub(crate) async fn locate_and_sleep(duration: Duration) {
        Self::locate().sleep(duration).await
    }
}

#[derive(Debug)]
pub(crate) struct Elapsed(());

impl From<Elapsed> for RedisError {
    fn from(_: Elapsed) -> Self {
        io::Error::from(io::ErrorKind::TimedOut).into()
    }
}
