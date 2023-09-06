use std::io::{BufRead, Error, ErrorKind as IOErrorKind};

use rustls::{Certificate, OwnedTrustAnchor, PrivateKey, RootCertStore};
use rustls_pemfile::{certs, read_one};

use crate::{Client, ConnectionAddr, ErrorKind, IntoConnectionInfo, RedisError, RedisResult};

pub(crate) fn inner_build_with_tls<C: IntoConnectionInfo>(
    conn_info: C,
    client_tls_params: Option<(&mut dyn BufRead, &mut dyn BufRead)>,
    root_cert: Option<&mut dyn BufRead>,
) -> RedisResult<Client> {
    let mut connection_info = conn_info.into_connection_info()?;
    let tls_params = retrieve_tls_certificates(client_tls_params, root_cert)?;

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
            "TLS client requires a `rediss://` address",
        )));
    };

    Ok(Client { connection_info })
}

pub(crate) fn retrieve_tls_certificates(
    client_tls_params: Option<(&mut dyn BufRead, &mut dyn BufRead)>,
    root_cert: Option<&mut dyn BufRead>,
) -> RedisResult<TlsConnParams> {
    let client_tls_params = if let Some((client_cert, client_key)) = client_tls_params {
        let client_cert: Vec<Certificate> =
            certs(client_cert).map(|mut certs| certs.drain(..).map(Certificate).collect())?;

        let client_key = load_key(client_key)?;
        Some(ClientTlsParams {
            client_cert,
            client_key,
        })
    } else {
        None
    };

    let root_cert = if root_cert.is_some() {
        let certs = certs(root_cert.unwrap())?;

        let trust_anchors: RedisResult<Vec<OwnedTrustAnchor>> = certs
            .iter()
            .map(|cert| {
                let ta = webpki::TrustAnchor::try_from_cert_der(cert).map_err(|_| {
                    Error::new(IOErrorKind::Other, "Unable to parse CA certificate")
                })?;

                Ok(OwnedTrustAnchor::from_subject_spki_name_constraints(
                    ta.subject,
                    ta.spki,
                    ta.name_constraints,
                ))
            })
            .collect();
        let trust_anchors = trust_anchors?;

        let mut root_cert = RootCertStore::empty();
        root_cert.add_trust_anchors(trust_anchors.into_iter());
        Some(root_cert)
    } else {
        None
    };

    Ok(TlsConnParams {
        client_tls_params,
        root_cert,
    })
}

#[derive(Debug, Clone)]
pub struct ClientTlsParams {
    pub(crate) client_cert: Vec<Certificate>,
    pub(crate) client_key: PrivateKey,
}

#[derive(Debug, Clone)]
pub struct TlsConnParams {
    pub(crate) client_tls_params: Option<ClientTlsParams>,
    pub(crate) root_cert: Option<RootCertStore>,
}

fn load_key(mut reader: &mut dyn BufRead) -> RedisResult<PrivateKey> {
    loop {
        match read_one(&mut reader)? {
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
