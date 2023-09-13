use std::io::{BufRead, Cursor, Error, ErrorKind as IOErrorKind};

use rustls::{Certificate, OwnedTrustAnchor, PrivateKey, RootCertStore};

use crate::{Client, ConnectionAddr, ErrorKind, IntoConnectionInfo, RedisError, RedisResult};

/// Structure to hold mTLS client _certificate_ and _key_ binaries in PEM format
///
#[derive(Clone)]
pub struct ClientTlsBinary {
    /// client certificate byte stream in PEM format
    pub client_cert: Vec<u8>,
    /// client key byte stream in PEM format
    pub client_key: Vec<u8>,
}

/// Structure to hold TLS certificates
/// - `client_tls`: binaries of clientkey and certificate within a `ClientTlsBinary` structure if mTLS is used
/// - `root_cert`: binary CA certificate in PEM format if CA is not in local truststore
///
#[derive(Clone)]
pub struct CertificatesBinary {
    /// 'ClientTlsBinary' containing client certificate and key if mTLS is to be used
    pub client_tls: Option<ClientTlsBinary>,
    /// root certificate byte stream in PEM format if the local truststore is *not* to be used
    pub root_cert: Option<Vec<u8>>,
}

pub(crate) fn inner_build_with_tls<C: IntoConnectionInfo>(
    conn_info: C,
    certificates: CertificatesBinary,
) -> RedisResult<Client> {
    let mut connection_info = conn_info.into_connection_info()?;
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
    certificates: CertificatesBinary,
) -> RedisResult<TlsConnParams> {
    let CertificatesBinary {
        client_tls,
        root_cert,
    } = certificates;

    let client_tls_params = if let Some(ClientTlsBinary {
        client_cert,
        client_key,
    }) = client_tls
    {
        let mut certs = rustls_pemfile::certs(&mut Cursor::new(client_cert))?;
        let client_cert_chain = certs.drain(..).map(Certificate).collect();

        let client_key = load_key(&mut Cursor::new(client_key))?;

        Some(ClientTlsParams {
            client_cert_chain,
            client_key,
        })
    } else {
        None
    };

    let root_cert_store = if let Some(root_cert) = root_cert {
        let certs = rustls_pemfile::certs(&mut Cursor::new(root_cert))?;

        let trust_anchors = certs
            .iter()
            .map(|cert| {
                let ta = webpki::TrustAnchor::try_from_cert_der(cert).map_err(|_| {
                    Error::new(IOErrorKind::Other, "Unable to parse TLS trust anchors")
                })?;

                Ok(OwnedTrustAnchor::from_subject_spki_name_constraints(
                    ta.subject,
                    ta.spki,
                    ta.name_constraints,
                ))
            })
            .collect::<RedisResult<Vec<_>>>()?;

        let mut root_cert_store = RootCertStore::empty();
        root_cert_store.add_trust_anchors(trust_anchors.into_iter());
        Some(root_cert_store)
    } else {
        None
    };

    Ok(TlsConnParams {
        client_tls_params,
        root_cert_store,
    })
}

#[derive(Debug, Clone)]
pub struct ClientTlsParams {
    pub(crate) client_cert_chain: Vec<Certificate>,
    pub(crate) client_key: PrivateKey,
}

#[derive(Debug, Clone)]
pub struct TlsConnParams {
    pub(crate) client_tls_params: Option<ClientTlsParams>,
    pub(crate) root_cert_store: Option<RootCertStore>,
}

fn load_key(mut reader: &mut dyn BufRead) -> RedisResult<PrivateKey> {
    loop {
        match rustls_pemfile::read_one(&mut reader)? {
            Some(rustls_pemfile::Item::ECKey(key))
            | Some(rustls_pemfile::Item::RSAKey(key))
            | Some(rustls_pemfile::Item::PKCS8Key(key)) => {
                return Ok(PrivateKey(key));
            }
            None => {
                break;
            }
            _ => {}
        }
    }
    Err(RedisError::from(Error::new(
        IOErrorKind::Other,
        "Unable to extract private key from PEM file",
    )))
}
