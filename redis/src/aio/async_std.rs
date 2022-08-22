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
#[cfg(feature = "tls")]
use async_native_tls::{TlsConnector, TlsStream};
use async_std::net::TcpStream;
#[cfg(unix)]
use async_std::os::unix::net::UnixStream;
use async_trait::async_trait;
use futures_util::ready;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pin_project_lite::pin_project! {
    /// Wraps the async_std `AsyncRead/AsyncWrite` in order to implement the required the tokio traits
    /// for it
    pub struct AsyncStdWrapped<T> {  #[pin] inner: T }
}

impl<T> AsyncStdWrapped<T> {
    pub(super) fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> AsyncWrite for AsyncStdWrapped<T>
where
    T: async_std::io::Write,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut core::task::Context,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, tokio::io::Error>> {
        async_std::io::Write::poll_write(self.project().inner, cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut core::task::Context,
    ) -> std::task::Poll<Result<(), tokio::io::Error>> {
        async_std::io::Write::poll_flush(self.project().inner, cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut core::task::Context,
    ) -> std::task::Poll<Result<(), tokio::io::Error>> {
        async_std::io::Write::poll_close(self.project().inner, cx)
    }
}

impl<T> AsyncRead for AsyncStdWrapped<T>
where
    T: async_std::io::Read,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut core::task::Context,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<Result<(), tokio::io::Error>> {
        let n = ready!(async_std::io::Read::poll_read(
            self.project().inner,
            cx,
            buf.initialize_unfilled()
        ))?;
        buf.advance(n);
        std::task::Poll::Ready(Ok(()))
    }
}

/// Represents an AsyncStd connectable
pub enum AsyncStd {
    /// Represents an Async_std TCP connection.
    Tcp(AsyncStdWrapped<TcpStream>),
    /// Represents an Async_std TLS encrypted TCP connection.
    #[cfg(feature = "async-std-tls-comp")]
    TcpTls(AsyncStdWrapped<Box<TlsStream<TcpStream>>>),
    /// Represents an Async_std Unix connection.
    #[cfg(unix)]
    Unix(AsyncStdWrapped<UnixStream>),
}

impl AsyncWrite for AsyncStd {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            AsyncStd::Tcp(r) => Pin::new(r).poll_write(cx, buf),
            #[cfg(feature = "async-std-tls-comp")]
            AsyncStd::TcpTls(r) => Pin::new(r).poll_write(cx, buf),
            #[cfg(unix)]
            AsyncStd::Unix(r) => Pin::new(r).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match &mut *self {
            AsyncStd::Tcp(r) => Pin::new(r).poll_flush(cx),
            #[cfg(feature = "async-std-tls-comp")]
            AsyncStd::TcpTls(r) => Pin::new(r).poll_flush(cx),
            #[cfg(unix)]
            AsyncStd::Unix(r) => Pin::new(r).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match &mut *self {
            AsyncStd::Tcp(r) => Pin::new(r).poll_shutdown(cx),
            #[cfg(feature = "async-std-tls-comp")]
            AsyncStd::TcpTls(r) => Pin::new(r).poll_shutdown(cx),
            #[cfg(unix)]
            AsyncStd::Unix(r) => Pin::new(r).poll_shutdown(cx),
        }
    }
}

impl AsyncRead for AsyncStd {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            AsyncStd::Tcp(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(feature = "async-std-tls-comp")]
            AsyncStd::TcpTls(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(unix)]
            AsyncStd::Unix(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

#[async_trait]
impl RedisRuntime for AsyncStd {
    async fn connect_tcp(socket_addr: SocketAddr) -> RedisResult<Self> {
        Ok(TcpStream::connect(&socket_addr)
            .await
            .map(|con| Self::Tcp(AsyncStdWrapped::new(con)))?)
    }

    #[cfg(feature = "tls")]
    async fn connect_tcp_tls(
        hostname: &str,
        socket_addr: SocketAddr,
        insecure: bool,
    ) -> RedisResult<Self> {
        let tcp_stream = TcpStream::connect(&socket_addr).await?;
        let tls_connector = if insecure {
            TlsConnector::new()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .use_sni(false)
        } else {
            TlsConnector::new()
        };
        Ok(tls_connector
            .connect(hostname, tcp_stream)
            .await
            .map(|con| Self::TcpTls(AsyncStdWrapped::new(Box::new(con))))?)
    }

    #[cfg(unix)]
    async fn connect_unix(path: &Path) -> RedisResult<Self> {
        Ok(UnixStream::connect(path)
            .await
            .map(|con| Self::Unix(AsyncStdWrapped::new(con)))?)
    }

    fn spawn(f: impl Future<Output = ()> + Send + 'static) {
        async_std::task::spawn(f);
    }

    fn boxed(self) -> Pin<Box<dyn AsyncStream + Send + Sync>> {
        match self {
            AsyncStd::Tcp(x) => Box::pin(x),
            #[cfg(feature = "async-std-tls-comp")]
            AsyncStd::TcpTls(x) => Box::pin(x),
            #[cfg(unix)]
            AsyncStd::Unix(x) => Box::pin(x),
        }
    }
}
