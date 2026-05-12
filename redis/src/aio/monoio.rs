#[cfg(unix)]
use std::path::Path;
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    task::{self, Poll},
};

use crate::aio::{AsyncStream, RedisRuntime};
use crate::types::RedisResult;
use super::TaskHandle;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};


// State for pending read operations
// Note: Monoio futures are NOT Send, but we mark them as such because
// they will only be polled on their bound thread (thread-per-core model)
enum ReadState {
    Idle,
    Reading(Pin<Box<dyn Future<Output = (io::Result<usize>, Vec<u8>)>>>),
}

// State for pending write operations
// Note: Monoio futures are NOT Send, but we mark them as such because
// they will only be polled on their bound thread (thread-per-core model)
enum WriteState {
    Idle,
    Writing(Pin<Box<dyn Future<Output = (io::Result<usize>, Vec<u8>)>>>),
}

// SAFETY: These states contain monoio futures which are not Send,
// but monoio's thread-per-core model ensures they're only accessed
// from their bound thread, so marking the states as Send is safe.
unsafe impl Send for ReadState {}
unsafe impl Send for WriteState {}

pin_project_lite::pin_project! {
    /// Wraps monoio's AsyncReadRent/AsyncWriteRent to implement tokio's AsyncRead/AsyncWrite traits
    ///
    /// This adapter bridges the impedance mismatch between:
    /// - Monoio: Completion-based I/O with owned buffers
    /// - Tokio: Readiness-based I/O with borrowed buffers
    ///
    /// It maintains a state machine to store in-flight read/write futures.
    pub struct MonoioWrapped<T> {
        #[pin]
        inner: T,
        // State machine for in-flight read operation
        read_state: ReadState,
        // State machine for in-flight write operation
        write_state: WriteState,
        // Reusable buffer for reads to avoid allocations
        read_buf: Option<Vec<u8>>,
    }
}

impl<T> MonoioWrapped<T> {
    pub(super) fn new(inner: T) -> Self {
        Self {
            inner,
            read_state: ReadState::Idle,
            write_state: WriteState::Idle,
            read_buf: None,
        }
    }
}

// SAFETY: Monoio uses a thread-per-core model where tasks are bound to a specific thread.
// Even though monoio types contain Rc/UnsafeCell (not Send/Sync), in practice:
// 1. Tasks never migrate between threads in monoio
// 2. The wrapped types are only accessed from their bound thread
// 3. Redis-rs requires Send + Sync for its connection types
//
// This is a compromise: we mark as Send + Sync to satisfy redis-rs's requirements,
// relying on monoio's runtime guarantees that the types won't actually be sent
// between threads. If you use this outside monoio's thread-per-core model, this
// will be unsound.
unsafe impl<T> Send for MonoioWrapped<T> {}
unsafe impl<T> Sync for MonoioWrapped<T> {}

impl<T> AsyncWrite for MonoioWrapped<T>
where
    T: monoio::io::AsyncWriteRent + Unpin + 'static,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            // We need to get a new projection each iteration
            let this = self.as_mut().project();

            match this.write_state {
                WriteState::Idle => {
                    // No write in progress, start a new one
                    // Copy the buffer since monoio requires owned buffers
                    let owned_buf = buf.to_vec();

                    // SAFETY: We need to create a future that borrows from inner,
                    // but we can't have a normal borrow because we're storing the future
                    // in the same struct. We use unsafe to get a 'static lifetime,
                    // which is sound because:
                    // 1. The Pin guarantees inner won't move
                    // 2. We only access the future while self is alive
                    // 3. We clear the future before self is dropped
                    let inner_ptr = this.inner.get_mut() as *mut T;
                    let write_fut = unsafe { (*inner_ptr).write(owned_buf) };

                    // Box the future (not Send, but safe in thread-per-core model)
                    let boxed_fut: Pin<Box<dyn Future<Output = _>>> =
                        Box::pin(async move { write_fut.await });

                    *this.write_state = WriteState::Writing(boxed_fut);
                    // Loop to poll the future immediately
                }
                WriteState::Writing(ref mut fut) => {
                    // Poll the in-flight write future
                    match fut.as_mut().poll(cx) {
                        Poll::Ready((res, _buf)) => {
                            // Write completed, reset state
                            // We need a fresh projection to modify write_state
                            let this = self.as_mut().project();
                            *this.write_state = WriteState::Idle;
                            return Poll::Ready(res);
                        }
                        Poll::Pending => {
                            // Still waiting for write to complete
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        // Monoio streams are typically auto-flushed
        // If there's a pending write, we should wait for it, but that's
        // handled by the protocol layer calling poll_write until complete
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        // Monoio doesn't have explicit shutdown in the same way
        // The connection will be closed when dropped
        Poll::Ready(Ok(()))
    }
}

impl<T> AsyncRead for MonoioWrapped<T>
where
    T: monoio::io::AsyncReadRent + Unpin + 'static,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Check if we have space to read
        let capacity = buf.remaining();
        if capacity == 0 {
            return Poll::Ready(Ok(()));
        }

        loop {
            // Get a fresh projection each iteration
            let this = self.as_mut().project();

            match this.read_state {
                ReadState::Idle => {
                    // No read in progress, start a new one
                    // Reuse buffer if available and appropriately sized, otherwise allocate
                    let owned_buf = this.read_buf.take()
                        .filter(|b| b.capacity() >= 4096 && b.capacity() <= 65536)
                        .unwrap_or_else(|| Vec::with_capacity(capacity.max(8192)));

                    // SAFETY: Same reasoning as poll_write - we use unsafe to extend
                    // the lifetime of the borrow to 'static so we can store the future
                    let inner_ptr = this.inner.get_mut() as *mut T;
                    let read_fut = unsafe { (*inner_ptr).read(owned_buf) };

                    // Box the future (not Send, but safe in thread-per-core model)
                    let boxed_fut: Pin<Box<dyn Future<Output = _>>> =
                        Box::pin(async move { read_fut.await });

                    *this.read_state = ReadState::Reading(boxed_fut);
                    // Loop to poll the future immediately
                }
                ReadState::Reading(ref mut fut) => {
                    // Poll the in-flight read future
                    match fut.as_mut().poll(cx) {
                        Poll::Ready((res, mut read_buf)) => {
                            let n = res?;

                            if n > 0 {
                                // Copy data to the caller's buffer
                                let to_copy = n.min(buf.remaining());
                                buf.put_slice(&read_buf[..to_copy]);
                            }

                            // Get a fresh projection to modify state
                            let this = self.as_mut().project();

                            // Reset state
                            *this.read_state = ReadState::Idle;

                            // Store buffer for reuse if it's a reasonable size
                            read_buf.clear();
                            if read_buf.capacity() >= 4096 && read_buf.capacity() <= 65536 {
                                *this.read_buf = Some(read_buf);
                            }

                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending => {
                            // Still waiting for read to complete
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

#[inline(always)]
async fn connect_tcp(
    addr: &SocketAddr,
    _tcp_settings: &crate::io::tcp::TcpSettings,
) -> io::Result<monoio::net::TcpStream> {
    let stream = monoio::net::TcpStream::connect(addr).await?;

    stream.set_nodelay(_tcp_settings.nodelay())?;

    Ok(stream)
}

/// Represents a Monoio connectable
pub(crate) enum Monoio {
    /// Represents a TCP connection.
    Tcp(MonoioWrapped<monoio::net::TcpStream>),
    /// Represents a Unix connection.
    #[cfg(unix)]
    Unix(MonoioWrapped<monoio::net::UnixStream>),
}

// SAFETY: Same reasoning as MonoioWrapped - monoio's thread-per-core model ensures
// these types are only accessed from their bound thread.
unsafe impl Send for Monoio {}
unsafe impl Sync for Monoio {}

impl AsyncWrite for Monoio {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Monoio::Tcp(r) => Pin::new(r).poll_write(cx, buf),
            #[cfg(unix)]
            Monoio::Unix(r) => Pin::new(r).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Monoio::Tcp(r) => Pin::new(r).poll_flush(cx),
            #[cfg(unix)]
            Monoio::Unix(r) => Pin::new(r).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Monoio::Tcp(r) => Pin::new(r).poll_shutdown(cx),
            #[cfg(unix)]
            Monoio::Unix(r) => Pin::new(r).poll_shutdown(cx),
        }
    }
}

impl AsyncRead for Monoio {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Monoio::Tcp(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(unix)]
            Monoio::Unix(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

impl RedisRuntime for Monoio {
    async fn connect_tcp(
        socket_addr: SocketAddr,
        tcp_settings: &crate::io::tcp::TcpSettings,
    ) -> RedisResult<Self> {
        Ok(connect_tcp(&socket_addr, tcp_settings)
            .await
            .map(|con| Self::Tcp(MonoioWrapped::new(con)))?)
    }


    #[cfg(unix)]
    async fn connect_unix(path: &Path) -> RedisResult<Self> {
        Ok(monoio::net::UnixStream::connect(path)
            .await
            .map(|con| Self::Unix(MonoioWrapped::new(con)))?)
    }

    fn spawn(f: impl Future<Output = ()> + Send + 'static) -> TaskHandle {
        // Monoio's spawn doesn't return a handle we can use
        // Tasks are managed by the runtime automatically
        monoio::spawn(f);
        TaskHandle::Monoio(())
    }

    fn boxed(self) -> Pin<Box<dyn AsyncStream + Send + Sync>> {
        match self {
            Monoio::Tcp(x) => Box::pin(x),
            #[cfg(unix)]
            Monoio::Unix(x) => Box::pin(x),
        }
    }
}
