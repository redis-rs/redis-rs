use crate::aio::{ActualConnection, Connect};
use crate::types::RedisResult;
use async_std::net::TcpStream;
#[cfg(unix)]
use async_std::os::unix::net::UnixStream;
use async_trait::async_trait;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::Path;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};

/// Wraps the async_std TcpStream in order to implemented the required Traits for it
pub struct TcpStreamAsyncStdWrapped(TcpStream);
#[cfg(unix)]
/// Wraps the async_std UnixStream in order to implemented the required Traits for it
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
pub struct AsyncStd;

#[async_trait]
impl Connect for AsyncStd {
    async fn connect_tcp(socket_addr: SocketAddr) -> RedisResult<ActualConnection> {
        Ok(TcpStream::connect(&socket_addr)
            .await
            .map(|con| ActualConnection::TcpAsyncStd(TcpStreamAsyncStdWrapped(con)))?)
    }

    #[cfg(unix)]
    async fn connect_unix(path: &Path) -> RedisResult<ActualConnection> {
        Ok(UnixStream::connect(path)
            .await
            .map(|con| ActualConnection::UnixAsyncStd(UnixStreamAsyncStdWrapped(con)))?)
    }
}
