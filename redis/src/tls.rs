use std::io::Error;

#[cfg(feature = "tls-rustls")]
use rustls::RootCertStore;
#[cfg(feature = "tls-rustls")]
use rustls::pki_types::pem::PemObject;
#[cfg(feature = "tls-rustls")]
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::connection::TlsConnParams;
use crate::types::redacted_if_set;
use crate::{Client, ConnectionAddr, ConnectionInfo, ErrorKind, RedisError, RedisResult};

/// Structure to hold mTLS client _certificate_ and _key_ binaries in PEM format
///
#[non_exhaustive]
#[derive(Clone)]
pub struct ClientTlsConfig {
    /// client certificate byte stream in PEM format
    pub client_cert: Vec<u8>,
    /// client key byte stream in PEM format
    pub client_key: Vec<u8>,
}

impl ClientTlsConfig {
    /// Builds a new instance
    pub fn new(client_cert: Vec<u8>, client_key: Vec<u8>) -> Self {
        Self {
            client_cert,
            client_key,
        }
    }
}

impl std::fmt::Debug for ClientTlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Destructured so that adding or removing a field breaks this impl; the bindings are
        // unused because neither field's contents may be printed.
        let Self {
            client_cert: _,
            client_key: _,
        } = self;
        f.debug_struct("ClientTlsConfig")
            .field("client_cert", &"<redacted>")
            .field("client_key", &"<redacted>")
            .finish()
    }
}

/// Structure to hold TLS certificates
/// - `client_tls`: binaries of clientkey and certificate within a `ClientTlsConfig` structure if mTLS is used
/// - `root_cert`: binary CA certificate in PEM format if CA is not in local truststore
///
#[non_exhaustive]
#[derive(Clone, Default)]
pub struct TlsCertificates {
    /// 'ClientTlsConfig' containing client certificate and key if mTLS is to be used
    pub client_tls: Option<ClientTlsConfig>,
    /// root certificate byte stream in PEM format if the local truststore is *not* to be used
    pub root_cert: Option<Vec<u8>>,
}

impl TlsCertificates {
    /// Builds a new instance
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a client TLS Config
    pub fn client_tls_config(mut self, client_tls_config: ClientTlsConfig) -> Self {
        self.client_tls = Some(client_tls_config);
        self
    }

    /// Sets a root certificate
    pub fn root_cert(mut self, root_cert: Vec<u8>) -> Self {
        self.root_cert = Some(root_cert);
        self
    }
}

impl std::fmt::Debug for TlsCertificates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            client_tls,
            root_cert,
        } = self;

        // Both fields are certificates, so their contents are never printed.
        f.debug_struct("TlsCertificates")
            .field("client_tls", &redacted_if_set(client_tls))
            .field("root_cert", &redacted_if_set(root_cert))
            .finish()
    }
}

pub(crate) fn inner_build_with_tls(
    mut connection_info: ConnectionInfo,
    certificates: &TlsCertificates,
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

#[cfg(feature = "tls-rustls")]
pub(crate) fn retrieve_tls_certificates(
    certificates: &TlsCertificates,
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
        let client_cert_chain = CertificateDer::pem_slice_iter(client_cert)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                Error::other(format!(
                    "Unable to parse client certificate chain PEM: {err}"
                ))
            })?;

        let client_key = PrivateKeyDer::from_pem_slice(client_key).map_err(|err| {
            Error::other(format!(
                "Unable to extract private key from PEM file: {err}"
            ))
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
        for result in CertificateDer::pem_slice_iter(root_cert) {
            let cert = result.map_err(|err| {
                Error::other(format!("Unable to parse root certificate PEM: {err}"))
            })?;

            if root_cert_store.add(cert).is_err() {
                return Err(Error::other("Unable to parse TLS trust anchors").into());
            }
        }

        Some(root_cert_store)
    } else {
        None
    };

    Ok(TlsConnParams {
        client_tls_params,
        root_cert_store,
        // Populated as well as the rustls fields, for a build in which another runtime selects
        // `native-tls`.
        #[cfg(any(
            all(feature = "tls-native-tls", not(feature = "tls-rustls")),
            all(feature = "tokio-native-tls-comp", not(feature = "tokio-rustls-comp")),
            all(feature = "smol-native-tls-comp", not(feature = "smol-rustls-comp"))
        ))]
        native_tls_params: parse_native_tls_params(certificates)?,
        #[cfg(any(feature = "tls-rustls-insecure", feature = "tls-native-tls"))]
        danger_accept_invalid_hostnames: false,
    })
}

#[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
pub(crate) fn retrieve_tls_certificates(
    certificates: &TlsCertificates,
) -> RedisResult<TlsConnParams> {
    Ok(TlsConnParams {
        native_tls_params: parse_native_tls_params(certificates)?,
        danger_accept_invalid_hostnames: false,
    })
}

/// Certificates and keys in the form the `native-tls` backends consume them.
///
/// Parsed when the client is built rather than when it connects, so that a malformed PEM is
/// reported at the call that supplied it.
#[cfg(any(
    all(feature = "tls-native-tls", not(feature = "tls-rustls")),
    all(feature = "tokio-native-tls-comp", not(feature = "tokio-rustls-comp")),
    all(feature = "smol-native-tls-comp", not(feature = "smol-rustls-comp"))
))]
#[derive(Clone)]
pub(crate) struct NativeTlsParams {
    pub(crate) identity: Option<native_tls::Identity>,
    pub(crate) root_certs: Vec<native_tls::Certificate>,
}

#[cfg(any(
    all(feature = "tls-native-tls", not(feature = "tls-rustls")),
    all(feature = "tokio-native-tls-comp", not(feature = "tokio-rustls-comp")),
    all(feature = "smol-native-tls-comp", not(feature = "smol-rustls-comp"))
))]
impl std::fmt::Debug for NativeTlsParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeTlsParams")
            .field("identity", &redacted_if_set(&self.identity))
            .field("root_certs", &self.root_certs.len())
            .finish()
    }
}

#[cfg(any(
    all(feature = "tls-native-tls", not(feature = "tls-rustls")),
    all(feature = "tokio-native-tls-comp", not(feature = "tokio-rustls-comp")),
    all(feature = "smol-native-tls-comp", not(feature = "smol-rustls-comp"))
))]
pub(crate) fn parse_native_tls_params(
    certificates: &TlsCertificates,
) -> RedisResult<Option<NativeTlsParams>> {
    const PEM_BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const PEM_END: &str = "-----END CERTIFICATE-----";

    let TlsCertificates {
        client_tls,
        root_cert,
    } = certificates;

    if client_tls.is_none() && root_cert.is_none() {
        return Ok(None);
    }

    let identity = client_tls
        .as_ref()
        .map(
            |ClientTlsConfig {
                 client_cert,
                 client_key,
             }| {
                native_tls::Identity::from_pkcs8(client_cert, client_key).map_err(|err| {
                    Error::other(format!("Unable to parse client identity PEM: {err}"))
                })
            },
        )
        .transpose()?;

    // `Certificate::from_pem` reads a single certificate, so a bundle is split into its blocks
    // first. Otherwise every certificate after the first is dropped without an error.
    let root_certs = match root_cert {
        Some(pem) => {
            let text = std::str::from_utf8(pem).map_err(|err| {
                Error::other(format!("Root certificate PEM is not valid UTF-8: {err}"))
            })?;
            let certs = text
                .split_inclusive(PEM_END)
                .filter_map(|block| block.find(PEM_BEGIN).map(|start| &block[start..]))
                .map(|block| {
                    native_tls::Certificate::from_pem(block.as_bytes()).map_err(|err| {
                        Error::other(format!("Unable to parse root certificate PEM: {err}"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            if certs.is_empty() {
                return Err(Error::other("Root certificate PEM contains no certificates").into());
            }
            certs
        }
        None => Vec::new(),
    };

    Ok(Some(NativeTlsParams {
        identity,
        root_certs,
    }))
}

#[cfg(feature = "tls-rustls")]
#[derive(Debug)]
pub struct ClientTlsParams {
    pub(crate) client_cert_chain: Vec<CertificateDer<'static>>,
    pub(crate) client_key: PrivateKeyDer<'static>,
}

/// [`PrivateKeyDer`] does not implement `Clone` so we need to implement it manually.
#[cfg(feature = "tls-rustls")]
impl Clone for ClientTlsParams {
    fn clone(&self) -> Self {
        use PrivateKeyDer::*;
        Self {
            client_cert_chain: self.client_cert_chain.clone(),
            client_key: match &self.client_key {
                Pkcs1(key) => Pkcs1(key.secret_pkcs1_der().to_vec().into()),
                Pkcs8(key) => Pkcs8(key.secret_pkcs8_der().to_vec().into()),
                Sec1(key) => Sec1(key.secret_sec1_der().to_vec().into()),
                _ => unreachable!(),
            },
        }
    }
}
