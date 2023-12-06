use super::{AsyncStream, RedisResult, RedisRuntime, SocketAddr};
use async_trait::async_trait;
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{self, Poll},
};
#[cfg(unix)]
use tokio::net::UnixStream as UnixStreamTokio;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream as TcpStreamTokio,
};

#[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
use native_tls::TlsConnector;

#[cfg(feature = "tls-rustls")]
use crate::connection::create_rustls_config;
#[cfg(feature = "tls-rustls")]
use std::sync::Arc;
#[cfg(feature = "tls-rustls")]
use tokio_rustls::{client::TlsStream, TlsConnector};

#[cfg(all(feature = "tokio-native-tls-comp", not(feature = "tokio-rustls-comp")))]
use tokio_native_tls::TlsStream;

#[cfg(feature = "tokio-rustls-comp")]
use crate::tls::TlsConnParams;

#[cfg(all(feature = "tokio-native-tls-comp", not(feature = "tls-rustls")))]
use crate::connection::TlsConnParams;

#[cfg(unix)]
use super::Path;

#[inline(always)]
async fn connect_tcp(addr: &SocketAddr) -> io::Result<TcpStreamTokio> {
    let socket = TcpStreamTokio::connect(addr).await?;
    #[cfg(feature = "tcp_nodelay")]
    socket.set_nodelay(true)?;
    #[cfg(feature = "keep-alive")]
    {
        //For now rely on system defaults
        const KEEP_ALIVE: socket2::TcpKeepalive = socket2::TcpKeepalive::new();
        //these are useless error that not going to happen
        let std_socket = socket.into_std()?;
        let socket2: socket2::Socket = std_socket.into();
        socket2.set_tcp_keepalive(&KEEP_ALIVE)?;
        TcpStreamTokio::from_std(socket2.into())
    }

    #[cfg(not(feature = "keep-alive"))]
    {
        Ok(socket)
    }
}

pub(crate) enum Tokio {
    /// Represents a Tokio TCP connection.
    Tcp(TcpStreamTokio),
    /// Represents a Tokio TLS encrypted TCP connection
    #[cfg(any(feature = "tokio-native-tls-comp", feature = "tokio-rustls-comp"))]
    TcpTls(Box<TlsStream<TcpStreamTokio>>),
    /// Represents a Tokio Unix connection.
    #[cfg(unix)]
    Unix(UnixStreamTokio),
}

impl AsyncWrite for Tokio {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Tokio::Tcp(r) => Pin::new(r).poll_write(cx, buf),
            #[cfg(any(feature = "tokio-native-tls-comp", feature = "tokio-rustls-comp"))]
            Tokio::TcpTls(r) => Pin::new(r).poll_write(cx, buf),
            #[cfg(unix)]
            Tokio::Unix(r) => Pin::new(r).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match &mut *self {
            Tokio::Tcp(r) => Pin::new(r).poll_flush(cx),
            #[cfg(any(feature = "tokio-native-tls-comp", feature = "tokio-rustls-comp"))]
            Tokio::TcpTls(r) => Pin::new(r).poll_flush(cx),
            #[cfg(unix)]
            Tokio::Unix(r) => Pin::new(r).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match &mut *self {
            Tokio::Tcp(r) => Pin::new(r).poll_shutdown(cx),
            #[cfg(any(feature = "tokio-native-tls-comp", feature = "tokio-rustls-comp"))]
            Tokio::TcpTls(r) => Pin::new(r).poll_shutdown(cx),
            #[cfg(unix)]
            Tokio::Unix(r) => Pin::new(r).poll_shutdown(cx),
        }
    }
}

impl AsyncRead for Tokio {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Tokio::Tcp(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(any(feature = "tokio-native-tls-comp", feature = "tokio-rustls-comp"))]
            Tokio::TcpTls(r) => Pin::new(r).poll_read(cx, buf),
            #[cfg(unix)]
            Tokio::Unix(r) => Pin::new(r).poll_read(cx, buf),
        }
    }
}

#[async_trait]
impl RedisRuntime for Tokio {
    async fn connect_tcp(socket_addr: SocketAddr) -> RedisResult<Self> {
        Ok(connect_tcp(&socket_addr).await.map(Tokio::Tcp)?)
    }

    #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
    async fn connect_tcp_tls(
        hostname: &str,
        socket_addr: SocketAddr,
        insecure: bool,
        _: &Option<TlsConnParams>,
    ) -> RedisResult<Self> {
        let tls_connector: tokio_native_tls::TlsConnector = if insecure {
            TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .use_sni(false)
                .build()?
        } else {
            TlsConnector::new()?
        }
        .into();
        Ok(tls_connector
            .connect(hostname, connect_tcp(&socket_addr).await?)
            .await
            .map(|con| Tokio::TcpTls(Box::new(con)))?)
    }

    #[cfg(feature = "tls-rustls")]
    async fn connect_tcp_tls(
        hostname: &str,
        socket_addr: SocketAddr,
        insecure: bool,
        tls_params: &Option<TlsConnParams>,
    ) -> RedisResult<Self> {
        let config = create_rustls_config(insecure, tls_params.clone())?;
        let tls_connector = TlsConnector::from(Arc::new(config));

        Ok(tls_connector
            .connect(
                rustls_pki_types::ServerName::try_from(hostname)?.to_owned(),
                connect_tcp(&socket_addr).await?,
            )
            .await
            .map(|con| Tokio::TcpTls(Box::new(con)))?)
    }

    #[cfg(unix)]
    async fn connect_unix(path: &Path) -> RedisResult<Self> {
        Ok(UnixStreamTokio::connect(path).await.map(Tokio::Unix)?)
    }

    #[cfg(feature = "tokio-comp")]
    fn spawn(f: impl Future<Output = ()> + Send + 'static) {
        tokio::spawn(f);
    }

    #[cfg(not(feature = "tokio-comp"))]
    fn spawn(_: impl Future<Output = ()> + Send + 'static) {
        unreachable!()
    }

    fn boxed(self) -> Pin<Box<dyn AsyncStream + Send + Sync>> {
        match self {
            Tokio::Tcp(x) => Box::pin(x),
            #[cfg(any(feature = "tokio-native-tls-comp", feature = "tokio-rustls-comp"))]
            Tokio::TcpTls(x) => Box::pin(x),
            #[cfg(unix)]
            Tokio::Unix(x) => Box::pin(x),
        }
    }
}
