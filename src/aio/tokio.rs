use super::{async_trait, AsyncStream, RedisResult, RedisRuntime, SocketAddr};

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

#[cfg(feature = "tls")]
use super::TlsConnector;

#[cfg(feature = "rustls")]
use crate::connection::NoCertificateVerification;
#[cfg(feature = "rustls")]
use rustls::{cipher_suite, kx_group, OwnedTrustAnchor, RootCertStore};
#[cfg(feature = "rustls")]
use std::{convert::TryInto, sync::Arc};
#[cfg(feature = "rustls")]
use tokio_rustls::{client::TlsStream, TlsConnector};
#[cfg(feature = "rustls")]
use webpki_roots::TLS_SERVER_ROOTS;

#[cfg(feature = "tokio-native-tls-comp")]
use tokio_native_tls::TlsStream;

#[cfg(unix)]
use super::Path;

// The rustls implementation of TlsStream is a larger enum, but it will only exist if the "rustls"
// feature is enabled, and in that case, it should be the variant that is used.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Tokio {
    /// Represents a Tokio TCP connection.
    Tcp(TcpStreamTokio),
    /// Represents a Tokio TLS encrypted TCP connection
    #[cfg(any(feature = "tokio-native-tls-comp", feature = "tokio-rustls-comp"))]
    TcpTls(TlsStream<TcpStreamTokio>),
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
        Ok(TcpStreamTokio::connect(&socket_addr)
            .await
            .map(Tokio::Tcp)?)
    }

    #[cfg(all(feature = "tls", not(feature = "rustls")))]
    async fn connect_tcp_tls(
        hostname: &str,
        socket_addr: SocketAddr,
        insecure: bool,
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
            .connect(hostname, TcpStreamTokio::connect(&socket_addr).await?)
            .await
            .map(Tokio::TcpTls)?)
    }

    #[cfg(all(feature = "rustls", not(feature = "tls")))]
    async fn connect_tcp_tls(
        hostname: &str,
        socket_addr: SocketAddr,
        insecure: bool,
    ) -> RedisResult<Self> {
        let mut root_store = RootCertStore::empty();
        root_store.add_server_trust_anchors(TLS_SERVER_ROOTS.0.iter().map(|ta| {
            OwnedTrustAnchor::from_subject_spki_name_constraints(
                ta.subject,
                ta.spki,
                ta.name_constraints,
            )
        }));

        let mut config = rustls::ClientConfig::builder()
            .with_cipher_suites(&[cipher_suite::TLS13_CHACHA20_POLY1305_SHA256])
            .with_kx_groups(&[&kx_group::X25519])
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        if insecure {
            config.enable_sni = false;
            config
                .dangerous()
                .set_certificate_verifier(Arc::new(NoCertificateVerification))
        }

        let tls_connector = TlsConnector::from(Arc::new(config));

        Ok(tls_connector
            .connect(
                hostname.try_into()?,
                TcpStreamTokio::connect(&socket_addr).await?,
            )
            .await
            .map(Tokio::TcpTls)?)
    }
    #[cfg(all(feature = "rustls", feature = "tls"))]
    async fn connect_tcp_tls(
        _hostname: &str,
        _socket_addr: SocketAddr,
        _insecure: bool,
    ) -> RedisResult<Self> {
        fail!((
            ErrorKind::InvalidClientConfig,
            "Cannot have both `tls` and `rustls` features active at the same time"
        ));
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
