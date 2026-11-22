use std::io::Error;
#[cfg(feature = "tls-rustls")]
use std::sync::Arc;

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

/// Where the TLS settings of one connection come from: certificates that redis-rs turns into a
/// connector, or a complete rustls configuration that is used as it is.
///
/// The rustls variant is only reachable through its own entry points, so the certificate setters
/// cannot be applied to it.
#[cfg(any(
    all(feature = "tls-rustls", feature = "sentinel"),
    all(
        any(feature = "tls-rustls", feature = "tls-native-tls"),
        feature = "cluster"
    )
))]
#[derive(Clone)]
pub(crate) enum TlsSource {
    Certificates(TlsCertificates),
    #[cfg(feature = "tls-rustls")]
    Rustls(Arc<rustls::ClientConfig>),
}

#[cfg(any(
    all(feature = "tls-rustls", feature = "sentinel"),
    all(
        any(feature = "tls-rustls", feature = "tls-native-tls"),
        feature = "cluster"
    )
))]
impl TlsSource {
    pub(crate) fn conn_params(&self) -> RedisResult<TlsConnParams> {
        match self {
            Self::Certificates(certificates) => retrieve_tls_certificates(certificates),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls(config) => Ok(TlsConnParams::custom(Arc::clone(config))),
        }
    }
}

#[cfg(feature = "tls-rustls")]
pub(crate) const CUSTOM_CONFIG_INSECURE: &str = "A custom rustls ClientConfig cannot be combined with an insecure connection; use a custom certificate verifier instead.";

pub(crate) fn inner_build_with_tls(
    connection_info: ConnectionInfo,
    certificates: &TlsCertificates,
) -> RedisResult<Client> {
    let tls_params = retrieve_tls_certificates(certificates)?;

    client_with_tls_params(connection_info, tls_params)
}

#[cfg(feature = "tls-rustls")]
pub(crate) fn inner_build_with_rustls_config(
    connection_info: ConnectionInfo,
    config: Arc<rustls::ClientConfig>,
) -> RedisResult<Client> {
    // An insecure connection would replace the verifier of the supplied configuration.
    if let ConnectionAddr::TcpTls { insecure: true, .. } = connection_info.addr {
        return Err(RedisError::from((
            ErrorKind::InvalidClientConfig,
            CUSTOM_CONFIG_INSECURE,
        )));
    }

    client_with_tls_params(connection_info, TlsConnParams::custom(config))
}

fn client_with_tls_params(
    mut connection_info: ConnectionInfo,
    tls_params: TlsConnParams,
) -> RedisResult<Client> {
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

    Ok(TlsConnParams::new(
        client_tls_params,
        root_cert_store,
        // Populated as well as the rustls fields, for a build in which another runtime selects
        // `native-tls`.
        #[cfg(any(
            all(feature = "tls-native-tls", not(feature = "tls-rustls")),
            all(feature = "tokio-native-tls-comp", not(feature = "tokio-rustls-comp")),
            all(feature = "smol-native-tls-comp", not(feature = "smol-rustls-comp"))
        ))]
        parse_native_tls_params(certificates)?,
    ))
}

#[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
pub(crate) fn retrieve_tls_certificates(
    certificates: &TlsCertificates,
) -> RedisResult<TlsConnParams> {
    Ok(TlsConnParams::new(parse_native_tls_params(certificates)?))
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

/// A lazily-populated cache of the platform's native root certificates.
///
/// Reading the native trust store is expensive (it hits the filesystem, or the
/// platform keychain) and it is read for every new TLS connection, including
/// the connections made by health checks and topology refreshes.
///
/// A successful read is cached forever; a failed read is not cached, so the
/// next caller retries the load. Under concurrent calls, one caller attempts to
/// parse the certs and all others wait for the result.
#[cfg(feature = "tls-rustls-native-roots")]
pub(crate) struct NativeCertBundle {
    parsed: std::sync::Mutex<Option<RootCertStore>>,
}

#[cfg(feature = "tls-rustls-native-roots")]
impl NativeCertBundle {
    pub(crate) const fn new() -> Self {
        Self {
            parsed: std::sync::Mutex::new(None),
        }
    }

    /// Return the native root certificate store, loading it from the platform
    /// trust store if it has not already been loaded successfully.
    pub(crate) fn load(&self) -> RedisResult<RootCertStore> {
        // The lock is held across the load so that concurrent callers wait for,
        // and then share, a single read of the platform trust store.
        let mut guard = self.parsed.lock().unwrap();

        if let Some(store) = guard.as_ref() {
            return Ok(store.clone());
        }

        let mut certificate_result = rustls_native_certs::load_native_certs();
        if let Some(error) = certificate_result.errors.pop() {
            // Not cached - the next caller retries the load.
            return Err(error.into());
        }

        let mut store = RootCertStore::empty();
        for cert in certificate_result.certs {
            store.add(cert)?;
        }

        Ok(guard.insert(store).clone())
    }

    /// Purge the cached certs so that the next call to [`Self::load()`] causes
    /// the bundle to be reparsed.
    pub(crate) fn invalidate(&self) {
        *self.parsed.lock().unwrap() = None;
    }
}

#[cfg(all(test, feature = "tls-rustls-native-roots"))]
mod tests {
    use super::*;

    /// The platform trust store is read at most once, and every caller observes
    /// the same set of roots.
    #[test]
    fn test_native_cert_bundle_cache() {
        let bundle = NativeCertBundle::new();

        // Reads the platform trust store.
        let first = bundle.load().expect("failed to load native certs");

        assert!(!first.is_empty(), "no native certs found");

        // Helper fn to capture the identity of the backing allocation of the
        // cached store.
        let cached_ptr = |bundle: &NativeCertBundle| {
            bundle
                .parsed
                .lock()
                .unwrap()
                .as_ref()
                .expect("successful load was not cached")
                .roots
                .as_ptr()
        };

        let first_ptr = cached_ptr(&bundle);
        let second = bundle.load().expect("failed to load native certs");

        // The cached store was neither reloaded nor mutated.
        assert_eq!(first_ptr, cached_ptr(&bundle));
        assert_eq!(first.roots, second.roots);

        // Invalidate the cached bundle.
        bundle.invalidate();
        assert!(bundle.parsed.lock().unwrap().is_none());
    }
}

// The native-tls runtimes in these builds take a further constructor argument.
#[cfg(all(
    test,
    feature = "tls-rustls",
    not(any(
        all(feature = "tokio-native-tls-comp", not(feature = "tokio-rustls-comp")),
        all(feature = "smol-native-tls-comp", not(feature = "smol-rustls-comp"))
    ))
))]
mod debug_tests {
    use super::*;
    use rustls::pki_types::{Der, TrustAnchor};

    fn params(client_tls: bool, root_certs: usize) -> TlsConnParams {
        let client_tls_params = client_tls.then(|| ClientTlsParams {
            client_cert_chain: vec![CertificateDer::from(b"secret-client-certificate".to_vec())],
            client_key: PrivateKeyDer::Pkcs8(b"secret-client-key".to_vec().into()),
        });
        let root_cert_store = (root_certs > 0).then(|| RootCertStore {
            roots: vec![
                TrustAnchor {
                    subject: Der::from_slice(b"secret-trust-anchor-subject"),
                    subject_public_key_info: Der::from_slice(b"secret-trust-anchor-spki"),
                    name_constraints: None,
                };
                root_certs
            ],
        });
        TlsConnParams::new(client_tls_params, root_cert_store)
    }

    #[test]
    fn debug_does_not_print_client_certificate_or_key() {
        let debug = format!("{:?}", params(true, 0));

        assert!(
            debug.contains("client_tls_params: \"<redacted>\""),
            "{debug}"
        );
        assert!(!debug.contains("client_cert_chain"), "{debug}");
        assert!(!debug.contains("client_key"), "{debug}");
        assert!(!debug.contains("secret"), "{debug}");
    }

    #[test]
    fn debug_prints_only_the_number_of_root_certificates() {
        let debug = format!("{:?}", params(false, 2));

        assert!(debug.contains("root_cert_store: Some(2)"), "{debug}");
        assert!(debug.contains("client_tls_params: \"not set\""), "{debug}");
        assert!(!debug.contains("secret"), "{debug}");
        assert!(!debug.contains("subject"), "{debug}");

        let debug = format!("{:?}", params(false, 0));
        assert!(debug.contains("root_cert_store: None"), "{debug}");
    }
}

/// A rustls configuration for tests, recognisable by its ALPN protocol.
#[cfg(all(test, feature = "tls-rustls"))]
pub(crate) fn test_client_config() -> Arc<rustls::ClientConfig> {
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(RootCertStore::empty())
    .with_no_client_auth();
    // Distinguishes this configuration from one built by redis-rs.
    config.alpn_protocols = vec![b"custom-test-alpn".to_vec()];
    Arc::new(config)
}

#[cfg(all(test, feature = "tls-rustls"))]
mod rustls_client_config_tests {
    use super::*;
    use crate::connection::create_rustls_config;

    #[test]
    fn custom_config_is_used_as_is() {
        let config = test_client_config();
        let params = TlsConnParams::custom(config.clone());

        let used = create_rustls_config(false, Some(params)).unwrap();
        assert!(Arc::ptr_eq(&used, &config));
        assert_eq!(used.alpn_protocols, vec![b"custom-test-alpn".to_vec()]);
    }

    #[test]
    fn custom_config_rejects_insecure() {
        let params = TlsConnParams::custom(test_client_config());

        let err = create_rustls_config(true, Some(params)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidClientConfig);
    }

    /// Through the public setter, so that a caller who mutates the address cannot loosen the
    /// verification of a custom configuration.
    #[cfg(any(feature = "tls-rustls-insecure", feature = "tls-native-tls"))]
    #[test]
    fn public_invalid_hostnames_setter_leaves_custom_config_alone() {
        let config = test_client_config();
        let mut addr = ConnectionAddr::TcpTls {
            host: "127.0.0.1".to_string(),
            port: 6379,
            insecure: false,
            tls_params: Some(TlsConnParams::custom(config.clone())),
        };
        addr.set_danger_accept_invalid_hostnames(true);

        let ConnectionAddr::TcpTls { tls_params, .. } = addr else {
            panic!("expected a TLS address");
        };
        let used = create_rustls_config(false, tls_params).unwrap();
        assert!(Arc::ptr_eq(&used, &config));
    }

    #[test]
    fn build_with_rustls_config_rejects_insecure_url() {
        let config = test_client_config();
        let err =
            Client::build_with_rustls_config("rediss://127.0.0.1:6379/#insecure", config.clone())
                .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidClientConfig);
        assert!(err.to_string().contains("insecure connection"), "{err}");

        Client::build_with_rustls_config("rediss://127.0.0.1:6379", config.clone()).unwrap();

        // A URL that is not TLS is rejected exactly as `build_with_tls` rejects it.
        let err = Client::build_with_rustls_config("redis://127.0.0.1:6379", config).unwrap_err();
        let expected =
            Client::build_with_tls("redis://127.0.0.1:6379", TlsCertificates::new()).unwrap_err();
        assert_eq!(err.kind(), expected.kind());
        assert_eq!(err.to_string(), expected.to_string());
    }

    #[test]
    fn debug_does_not_print_config() {
        let params = TlsConnParams::custom(test_client_config());
        let debug = format!("{params:?}");
        assert!(debug.contains("<redacted>"), "{debug}");
        assert!(!debug.contains("custom-test-alpn"), "{debug}");
    }
}

#[cfg(all(
    test,
    feature = "tls-rustls",
    feature = "tokio-native-tls-comp",
    not(feature = "tokio-rustls-comp")
))]
mod native_tls_with_rustls_config_tests {
    use super::*;
    use crate::connection::create_native_tls_connector_builder;

    /// With both backends compiled in, a native-tls runtime must fail rather than silently ignore
    /// the custom rustls configuration and trust the system roots.
    #[test]
    fn native_tls_connector_rejects_custom_rustls_config() {
        let params = TlsConnParams::custom(test_client_config());

        for insecure in [false, true] {
            let Err(err) = create_native_tls_connector_builder(insecure, Some(params.clone()))
            else {
                panic!("expected an error");
            };
            assert_eq!(err.kind(), ErrorKind::InvalidClientConfig);
        }
    }
}
