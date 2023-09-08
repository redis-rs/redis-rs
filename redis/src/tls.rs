use std::io::{BufRead, Error, ErrorKind as IOErrorKind};

use rustls::{Certificate, OwnedTrustAnchor, PrivateKey, RootCertStore};

use crate::{Client, ConnectionAddr, ErrorKind, IntoConnectionInfo, RedisError, RedisResult};

pub(crate) fn inner_build_with_tls<C: IntoConnectionInfo>(
    conn_info: C,
    client_tls_params: Option<(&mut dyn BufRead, &mut dyn BufRead)>,
    trust_anchors_pem: Option<&mut dyn BufRead>,
) -> RedisResult<Client> {
    let mut connection_info = conn_info.into_connection_info()?;
    let tls_params = retrieve_tls_certificates(client_tls_params, trust_anchors_pem)?;

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
    client_tls_params: Option<(&mut dyn BufRead, &mut dyn BufRead)>,
    trust_anchors_pem: Option<&mut dyn BufRead>,
) -> RedisResult<TlsConnParams> {
    let client_tls_params = if let Some((client_cert_chain_pem, client_key_pem)) = client_tls_params
    {
        let mut certs = rustls_pemfile::certs(client_cert_chain_pem)?;
        let client_cert_chain = certs.drain(..).map(Certificate).collect();

        let client_key = load_key(client_key_pem)?;

        Some(ClientTlsParams {
            client_cert_chain,
            client_key,
        })
    } else {
        None
    };

    let root_cert_store = if let Some(root_cert) = trust_anchors_pem {
        let certs = rustls_pemfile::certs(root_cert)?;

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
