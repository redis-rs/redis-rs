#[cfg(unix)]
use std::path::Path;
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{self, Poll},
};

use crate::aio::{AsyncStream, Connect};
use crate::types::RedisResult;
#[cfg(feature = "tls")]
use async_native_tls::{TlsConnector, TlsStream};
use async_std::net::TcpStream;
#[cfg(unix)]
use async_std::os::unix::net::UnixStream;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

/// Wraps the async_std TcpStream in order to implement the required Traits for it
pub struct TcpStreamAsyncStdWrapped(TcpStream);

/// Wraps the async_native_tls TlsStream in order to implement the required Traits for it
#[cfg(feature = "tls")]
pub struct TlsStreamAsyncStdWrapped(TlsStream<TcpStream>);

#[cfg(unix)]
/// Wraps the async_std UnixStream in order to implement the required Traits for it
pub struct UnixStreamAsyncStdWrapped(UnixStream);

impl AsyncWrite for TcpStreamAsyncStdWrapped {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, tokio::io::Error>> {
        async_std::io::Write::poll_write(Pin::new(&mut self.0), cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
    ) -> std::task::Poll<Result<(), tokio::io::Error>> {
        async_std::io::Write::poll_flush(Pin::new(&mut self.0), cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
    ) -> std::task::Poll<Result<(), tokio::io::Error>> {
        async_std::io::Write::poll_close(Pin::new(&mut self.0), cx)
    }
}

impl AsyncRead for TcpStreamAsyncStdWrapped {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
        buf: &mut [u8],
    ) -> std::task::Poll<Result<usize, tokio::io::Error>> {
        async_std::io::Read::poll_read(Pin::new(&mut self.0), cx, buf)
    }
}

#[cfg(feature = "tls")]
impl AsyncWrite for TlsStreamAsyncStdWrapped {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, tokio::io::Error>> {
        async_std::io::Write::poll_write(Pin::new(&mut self.0), cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
    ) -> std::task::Poll<Result<(), tokio::io::Error>> {
        async_std::io::Write::poll_flush(Pin::new(&mut self.0), cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
    ) -> std::task::Poll<Result<(), tokio::io::Error>> {
        async_std::io::Write::poll_close(Pin::new(&mut self.0), cx)
    }
}

#[cfg(feature = "tls")]
impl AsyncRead for TlsStreamAsyncStdWrapped {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
        buf: &mut [u8],
    ) -> std::task::Poll<Result<usize, tokio::io::Error>> {
        async_std::io::Read::poll_read(Pin::new(&mut self.0), cx, buf)
    }
}

#[cfg(unix)]
impl AsyncWrite for UnixStreamAsyncStdWrapped {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, tokio::io::Error>> {
        async_std::io::Write::poll_write(Pin::new(&mut self.0), cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
    ) -> std::task::Poll<Result<(), tokio::io::Error>> {
        async_std::io::Write::poll_flush(Pin::new(&mut self.0), cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
    ) -> std::task::Poll<Result<(), tokio::io::Error>> {
        async_std::io::Write::poll_close(Pin::new(&mut self.0), cx)
    }
}

#[cfg(unix)]
impl AsyncRead for UnixStreamAsyncStdWrapped {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context,
        buf: &mut [u8],
    ) -> std::task::Poll<Result<usize, tokio::io::Error>> {
        async_std::io::Read::poll_read(Pin::new(&mut self.0), cx, buf)
    }
}

/// Represents an AsyncStd connectable
pub enum AsyncStd {
    /// Represents an Async_std TCP connection.
    #[cfg(feature = "async-std-comp")]
    Tcp(TcpStreamAsyncStdWrapped),
    /// Represents an Async_std TLS encrypted TCP connection.
    #[cfg(feature = "async-std-tls-comp")]
    TcpTls(TlsStreamAsyncStdWrapped),
    /// Represents an Async_std Unix connection.
    #[cfg(feature = "async-std-comp")]
    #[cfg(unix)]
    Unix(UnixStreamAsyncStdWrapped),
}

impl AsyncWrite for AsyncStd {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            AsyncStd::Tcp(r) => Pin::new(r).poll_write(cx, buf),
            #[cfg(feature = "tokio-tls-comp")]
            AsyncStd::TcpTls(r) => Pin::new(r).poll_write(cx, buf),
            #[cfg(unix)]
            #[cfg(feature = "tokio-comp")]
            AsyncStd::Unix(r) => Pin::new(r).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match &mut *self {
            AsyncStd::Tcp(r) => Pin::new(r).poll_flush(cx),
            #[cfg(feature = "tokio-tls-comp")]
            AsyncStd::TcpTls(r) => Pin::new(r).poll_flush(cx),
            #[cfg(unix)]
            AsyncStd::Unix(r) => Pin::new(r).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match &mut *self {
            AsyncStd::Tcp(r) => Pin::new(r).poll_shutdown(cx),
            #[cfg(feature = "tokio-tls-comp")]
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
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            AsyncStd::Tcp(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(feature = "tokio-tls-comp")]
            AsyncStd::TcpTls(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(unix)]
            AsyncStd::Unix(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

#[async_trait]
impl Connect for AsyncStd {
    async fn connect_tcp(socket_addr: SocketAddr) -> RedisResult<Self> {
        Ok(TcpStream::connect(&socket_addr)
            .await
            .map(|con| Self::Tcp(TcpStreamAsyncStdWrapped(con)))?)
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
            .map(|con| Self::TcpTls(TlsStreamAsyncStdWrapped(con)))?)
    }

    #[cfg(unix)]
    async fn connect_unix(path: &Path) -> RedisResult<Self> {
        Ok(UnixStream::connect(path)
            .await
            .map(|con| Self::Unix(UnixStreamAsyncStdWrapped(con)))?)
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
