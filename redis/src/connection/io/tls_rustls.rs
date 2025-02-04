use rustls::{
    pki_types::{pem::PemObject as _, CertificateDer, PrivateKeyDer},
    RootCertStore, StreamOwned,
};
use std::{
    fmt,
    net::{TcpStream, ToSocketAddrs as _},
    sync::Arc,
    time::Duration,
};

use crate::{Client, ConnectionAddr, ConnectionInfo, ErrorKind, RedisError, RedisResult};

use super::tcp::{connect_tcp, connect_tcp_timeout};

pub struct TcpRustlsConnection {
    pub(super) reader: StreamOwned<rustls::ClientConnection, TcpStream>,
    pub(super) open: bool,
}

impl TcpRustlsConnection {
    pub(super) fn try_new(
        host: &str,
        port: u16,
        insecure: bool,
        tls_params: Option<&TlsConnParams>,
        timeout: Option<Duration>,
    ) -> RedisResult<Self> {
        let host: &str = host;
        let config = create_rustls_config(insecure, tls_params.cloned())?;
        let conn = rustls::ClientConnection::new(
            Arc::new(config),
            rustls::pki_types::ServerName::try_from(host)?.to_owned(),
        )?;
        let reader = match timeout {
            None => {
                let tcp = connect_tcp((host, port))?;
                rustls::StreamOwned::new(conn, tcp)
            }
            Some(timeout) => {
                let mut tcp = None;
                let mut last_error = None;
                for addr in (host, port).to_socket_addrs()? {
                    match connect_tcp_timeout(&addr, timeout) {
                        Ok(l) => {
                            tcp = Some(l);
                            break;
                        }
                        Err(e) => {
                            last_error = Some(e);
                        }
                    };
                }
                match (tcp, last_error) {
                    (Some(tcp), _) => rustls::StreamOwned::new(conn, tcp),
                    (None, Some(e)) => {
                        fail!(e);
                    }
                    (None, None) => {
                        fail!((
                            ErrorKind::InvalidClientConfig,
                            "could not resolve to any addresses"
                        ));
                    }
                }
            }
        };

        Ok(TcpRustlsConnection { reader, open: true })
    }
}

/// Structure to hold mTLS client _certificate_ and _key_ binaries in PEM format
///
#[derive(Clone)]
pub struct ClientTlsConfig {
    /// client certificate byte stream in PEM format
    pub client_cert: Vec<u8>,
    /// client key byte stream in PEM format
    pub client_key: Vec<u8>,
}

/// Structure to hold TLS certificates
/// - `client_tls`: binaries of clientkey and certificate within a `ClientTlsConfig` structure if mTLS is used
/// - `root_cert`: binary CA certificate in PEM format if CA is not in local truststore
///
#[derive(Clone)]
pub struct TlsCertificates {
    /// 'ClientTlsConfig' containing client certificate and key if mTLS is to be used
    pub client_tls: Option<ClientTlsConfig>,
    /// root certificate byte stream in PEM format if the local truststore is *not* to be used
    pub root_cert: Option<Vec<u8>>,
}

pub(crate) fn inner_build_with_tls(
    mut connection_info: ConnectionInfo,
    certificates: TlsCertificates,
) -> RedisResult<Client> {
    let tls_params = retrieve_tls_certificates(certificates)?;

    connection_info.addr = if let ConnectionAddr::TcpTls {
        host,
        port,
        insecure,
        ..
    } = connection_info.addr
    {
        ConnectionAddr::TcpTls {
            host,
            port,
            insecure,
            tls_params: Some(tls_params),
        }
    } else {
        return Err(RedisError::from((
            ErrorKind::InvalidClientConfig,
            "Constructing a TLS client requires a URL with the `rediss://` scheme",
        )));
    };

    Ok(Client { connection_info })
}

pub(crate) fn retrieve_tls_certificates(
    certificates: TlsCertificates,
) -> RedisResult<TlsConnParams> {
    let TlsCertificates {
        client_tls,
        root_cert,
    } = certificates;

    let client_tls_params = if let Some(ClientTlsConfig {
        client_cert,
        client_key,
    }) = client_tls
    {
        let client_cert_chain = CertificateDer::pem_slice_iter(&client_cert)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Unable to parse client certificate chain PEM: {err}"),
                )
            })?;

        let client_key = PrivateKeyDer::from_pem_slice(&client_key).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unable to extract private key from PEM file: {err}"),
            )
        })?;

        Some(ClientTlsParams {
            client_cert_chain,
            client_key,
        })
    } else {
        None
    };

    let root_cert_store = if let Some(root_cert) = root_cert {
        let mut root_cert_store = RootCertStore::empty();
        for result in CertificateDer::pem_slice_iter(&root_cert) {
            let cert = result.map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Unable to parse root certificate PEM: {err}"),
                )
            })?;

            if root_cert_store.add(cert).is_err() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Unable to parse TLS trust anchors",
                )
                .into());
            }
        }

        Some(root_cert_store)
    } else {
        None
    };

    Ok(TlsConnParams {
        client_tls_params,
        root_cert_store,
    })
}

#[derive(Debug)]
pub struct ClientTlsParams {
    pub(crate) client_cert_chain: Vec<CertificateDer<'static>>,
    pub(crate) client_key: PrivateKeyDer<'static>,
}

/// [`PrivateKeyDer`] does not implement `Clone` so we need to implement it manually.
impl Clone for ClientTlsParams {
    fn clone(&self) -> Self {
        Self {
            client_cert_chain: self.client_cert_chain.clone(),
            client_key: match &self.client_key {
                PrivateKeyDer::Pkcs1(key) => {
                    PrivateKeyDer::Pkcs1(key.secret_pkcs1_der().to_vec().into())
                }
                PrivateKeyDer::Pkcs8(key) => {
                    PrivateKeyDer::Pkcs8(key.secret_pkcs8_der().to_vec().into())
                }
                PrivateKeyDer::Sec1(key) => {
                    PrivateKeyDer::Sec1(key.secret_sec1_der().to_vec().into())
                }
                _ => unreachable!(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct TlsConnParams {
    pub(crate) client_tls_params: Option<ClientTlsParams>,
    pub(crate) root_cert_store: Option<RootCertStore>,
}

pub(crate) fn create_rustls_config(
    insecure: bool,
    tls_params: Option<TlsConnParams>,
) -> RedisResult<rustls::ClientConfig> {
    #[allow(unused_mut)]
    let mut root_store = RootCertStore::empty();
    #[cfg(feature = "tls-rustls-webpki-roots")]
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    #[cfg(all(
        feature = "tls-rustls",
        not(feature = "tls-native-tls"),
        not(feature = "tls-rustls-webpki-roots")
    ))]
    for cert in load_native_certs()? {
        root_store.add(cert)?;
    }

    let config = rustls::ClientConfig::builder();
    let config = if let Some(tls_params) = tls_params {
        let config_builder =
            config.with_root_certificates(tls_params.root_cert_store.unwrap_or(root_store));

        if let Some(ClientTlsParams {
            client_cert_chain: client_cert,
            client_key,
        }) = tls_params.client_tls_params
        {
            config_builder
                .with_client_auth_cert(client_cert, client_key)
                .map_err(|err| {
                    RedisError::from((
                        ErrorKind::InvalidClientConfig,
                        "Unable to build client with TLS parameters provided.",
                        err.to_string(),
                    ))
                })?
        } else {
            config_builder.with_no_client_auth()
        }
    } else {
        config
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

    match (insecure, cfg!(feature = "tls-rustls-insecure")) {
        #[cfg(feature = "tls-rustls-insecure")]
        (true, true) => {
            let mut config = config;
            config.enable_sni = false;
            config
                .dangerous()
                .set_certificate_verifier(Arc::new(NoCertificateVerification {
                    supported: rustls::crypto::ring::default_provider()
                        .signature_verification_algorithms,
                }));

            Ok(config)
        }
        (true, false) => {
            fail!((
                ErrorKind::InvalidClientConfig,
                "Cannot create insecure client without tls-rustls-insecure feature"
            ));
        }
        _ => Ok(config),
    }
}

#[cfg(feature = "tls-rustls-insecure")]
struct NoCertificateVerification {
    supported: rustls::crypto::WebPkiSupportedAlgorithms,
}

#[cfg(feature = "tls-rustls-insecure")]
impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.supported.supported_schemes()
    }
}

#[cfg(feature = "tls-rustls-insecure")]
impl fmt::Debug for NoCertificateVerification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NoCertificateVerification").finish()
    }
}
