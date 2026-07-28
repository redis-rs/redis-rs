use std::{io, sync::Arc, time::Duration};

use futures_util::Future;

// Helper to make monoio futures Send safe while they are driven inside a monoio runtime.
// Public redis-rs handles may be moved across threads, but the non-Send monoio I/O
// futures stay inside the spawned driver task on the runtime thread.
#[cfg(feature = "monoio-comp")]
pub(crate) mod monoio_future_safety {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    // Wrapper to make monoio futures Send safe
    pub struct SendSafeFuture<F>(F);

    impl<F: Future> Future for SendSafeFuture<F> {
        type Output = F::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            // SAFETY: We're projecting the pin to the inner future
            unsafe {
                let this = self.get_unchecked_mut();
                Pin::new_unchecked(&mut this.0).poll(cx)
            }
        }
    }

    unsafe impl<F> Send for SendSafeFuture<F> {}

    pub fn make_send_safe<F: Future>(f: F) -> SendSafeFuture<F> {
        SendSafeFuture(f)
    }
}

#[cfg(any(
    all(feature = "tokio-comp", feature = "smol-comp"),
    all(
        feature = "monoio-comp",
        any(feature = "tokio-comp", feature = "smol-comp")
    )
))]
use std::sync::OnceLock;

use super::RedisRuntime;
#[cfg(feature = "monoio-comp")]
use super::monoio as crate_monoio;
#[cfg(feature = "smol-comp")]
use super::smol as crate_smol;
#[cfg(feature = "tokio-comp")]
use super::tokio as crate_tokio;
use crate::errors::RedisError;
#[cfg(feature = "smol-comp")]
use smol_timeout::TimeoutExt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Runtime {
    #[cfg(feature = "tokio-comp")]
    Tokio,
    #[cfg(feature = "smol-comp")]
    Smol,
    #[cfg(feature = "monoio-comp")]
    Monoio,
}

pub(crate) enum TaskHandle {
    #[cfg(feature = "tokio-comp")]
    Tokio(tokio::task::JoinHandle<()>),
    #[cfg(feature = "smol-comp")]
    Smol(smol::Task<()>),
    #[cfg(feature = "monoio-comp")]
    Monoio(futures_util::future::AbortHandle),
}

impl TaskHandle {
    #[cfg(feature = "connection-manager")]
    pub(crate) fn detach(self) {
        match self {
            #[cfg(feature = "smol-comp")]
            TaskHandle::Smol(task) => task.detach(),
            #[cfg(feature = "monoio-comp")]
            TaskHandle::Monoio(_) => {}
            #[cfg(feature = "tokio-comp")]
            _ => {}
        }
    }
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
            #[cfg(feature = "smol-comp")]
            Some(TaskHandle::Smol(task)) => drop(task),
            #[cfg(feature = "monoio-comp")]
            Some(TaskHandle::Monoio(handle)) => handle.abort(),
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

#[cfg(any(
    all(feature = "tokio-comp", feature = "smol-comp"),
    all(
        feature = "monoio-comp",
        any(feature = "tokio-comp", feature = "smol-comp")
    )
))]
static CHOSEN_RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[cfg(any(
    all(feature = "tokio-comp", feature = "smol-comp"),
    all(
        feature = "monoio-comp",
        any(feature = "tokio-comp", feature = "smol-comp")
    )
))]
fn set_runtime(runtime: Runtime) -> Result<(), RedisError> {
    const PREFER_RUNTIME_ERROR: &str = "Another runtime preference was already set. Please call this function before any other runtime preference is set.";

    if CHOSEN_RUNTIME
        .get()
        .is_some_and(|chosen| *chosen == runtime)
    {
        return Ok(());
    }

    CHOSEN_RUNTIME
        .set(runtime)
        .map_err(|_| RedisError::from((crate::ErrorKind::Client, PREFER_RUNTIME_ERROR)))
}

/// Mark Smol as the preferred runtime.
///
/// If the function returns `Err`, another runtime preference was already set, and won't be changed.
/// Call this function if the application doesn't use multiple runtimes,
/// but the crate is compiled with multiple runtimes enabled, which is a bad pattern that should be avoided.
#[cfg(all(
    feature = "smol-comp",
    any(feature = "tokio-comp", feature = "monoio-comp")
))]
pub fn prefer_smol() -> Result<(), RedisError> {
    set_runtime(Runtime::Smol)
}

/// Mark Tokio as the preferred runtime.
///
/// If the function returns `Err`, another runtime preference was already set, and won't be changed.
/// Call this function if the application doesn't use multiple runtimes,
/// but the crate is compiled with multiple runtimes enabled, which is a bad pattern that should be avoided.
#[cfg(all(
    feature = "tokio-comp",
    any(feature = "smol-comp", feature = "monoio-comp")
))]
pub fn prefer_tokio() -> Result<(), RedisError> {
    set_runtime(Runtime::Tokio)
}

/// Mark Monoio as the preferred runtime.
///
/// If the function returns `Err`, another runtime preference was already set, and won't be changed.
/// Call this function if the application doesn't use multiple runtimes,
/// but the crate is compiled with multiple runtimes enabled, which is a bad pattern that should be avoided.
#[cfg(all(
    feature = "monoio-comp",
    any(feature = "tokio-comp", feature = "smol-comp")
))]
pub fn prefer_monoio() -> Result<(), RedisError> {
    set_runtime(Runtime::Monoio)
}

impl Runtime {
    pub(crate) fn locate() -> Self {
        #[cfg(any(
            all(feature = "smol-comp", feature = "tokio-comp"),
            all(
                feature = "monoio-comp",
                any(feature = "tokio-comp", feature = "smol-comp")
            )
        ))]
        if let Some(runtime) = CHOSEN_RUNTIME.get() {
            return *runtime;
        }

        #[cfg(all(
            feature = "tokio-comp",
            not(feature = "smol-comp"),
            not(feature = "monoio-comp")
        ))]
        {
            Runtime::Tokio
        }

        #[cfg(all(
            not(feature = "tokio-comp"),
            feature = "smol-comp",
            not(feature = "monoio-comp")
        ))]
        {
            Runtime::Smol
        }

        #[cfg(all(
            not(feature = "tokio-comp"),
            not(feature = "smol-comp"),
            feature = "monoio-comp"
        ))]
        {
            Runtime::Monoio
        }

        cfg_if::cfg_if! {
            if #[cfg(all(feature = "tokio-comp", feature = "smol-comp"))] {
                if ::tokio::runtime::Handle::try_current().is_ok() {
                    Runtime::Tokio
                } else {
                    Runtime::Smol
                }
            } else if #[cfg(all(feature = "tokio-comp", feature = "monoio-comp"))] {
                if ::tokio::runtime::Handle::try_current().is_ok() {
                    Runtime::Tokio
                } else {
                    Runtime::Monoio
                }
            } else if #[cfg(all(feature = "smol-comp", feature = "monoio-comp"))] {
                Runtime::Smol
            }
        }

        #[cfg(all(
            not(feature = "tokio-comp"),
            not(feature = "smol-comp"),
            not(feature = "monoio-comp")
        ))]
        {
            compile_error!(
                "tokio-comp, smol-comp, or monoio-comp features required for aio feature"
            )
        }
    }

    #[must_use]
    pub(crate) fn spawn(&self, f: impl Future<Output = ()> + Send + 'static) -> TaskHandle {
        match self {
            #[cfg(feature = "tokio-comp")]
            Runtime::Tokio => crate_tokio::Tokio::spawn(f),
            #[cfg(feature = "smol-comp")]
            Runtime::Smol => crate_smol::Smol::spawn(f),
            #[cfg(feature = "monoio-comp")]
            Runtime::Monoio => crate_monoio::Monoio::spawn(f),
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
            #[cfg(feature = "smol-comp")]
            Runtime::Smol => future.timeout(duration).await.ok_or(Elapsed(())),
            #[cfg(feature = "monoio-comp")]
            Runtime::Monoio => monoio_future_safety::make_send_safe(
                monoio::time::timeout(duration, future),
            )
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

            #[cfg(feature = "smol-comp")]
            Runtime::Smol => {
                smol::Timer::after(duration).await;
            }

            #[cfg(feature = "monoio-comp")]
            Runtime::Monoio => {
                // Monoio's sleep returns a Future that contains non-Send types,
                // but in thread-per-core model, this is safe because tasks never transfer.
                // SAFETY: In monoio's thread-per-core model, the sleep future
                // is only polled on the same thread, so it's safe to treat as Send.
                monoio_future_safety::make_send_safe(monoio::time::sleep(duration)).await;
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
