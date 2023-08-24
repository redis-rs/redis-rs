use std::io::{BufRead, Error, ErrorKind as IOErrorKind};

use rustls::{Certificate, OwnedTrustAnchor, PrivateKey, RootCertStore};
use rustls_pemfile::{certs, read_one};

use crate::{Client, ConnectionAddr, ErrorKind, IntoConnectionInfo, RedisError, RedisResult};

/// The builder takes all parameters necessary to create a TLS connection.
///
/// In addition to an `url` with `rediss` location, it must be provided with
/// binary streams corresponding to PEM file's contents for:
/// - CA's certificate
/// - one client certificate and
/// - client's key
///
/// Example usage:
///
/// ```no_run
/// use redis::{self, build_with_tls};
/// use redis::AsyncCommands;
///
/// use std::fs::File;
/// use std::io::BufReader;
///
/// async fn do_redis_code(url: &str, ca_file: &str, cert_file: &str, key_file: &str) -> redis::RedisResult<()> {
///     let cert_file = File::open(cert_file).expect("cannot open private cert file");
///     let mut reader_cert = BufReader::new(cert_file);
///
///     let key_file = File::open(key_file).expect("Cannot open private key file");
///     let mut reader_key = BufReader::new(key_file);
///
///     let ca_file = File::open(ca_file).expect("Cannot open CA cert file");
///     let mut reader_ca = BufReader::new(ca_file);
///
///     let client = build_with_tls(
///         url,
///         &mut reader_cert,
///         &mut reader_key,
///         &mut reader_ca
///     )
///     .expect("Unable to build client");
///
///     let connection_info = client.get_connection_info();
///
///     println!(">>> connection info: {connection_info:?}");
///
///     let mut con = client.get_async_connection().await?;
///
///     con.set("key1", b"foo").await?;
///
///     redis::cmd("SET")
///         .arg(&["key2", "bar"])
///         .query_async(&mut con)
///         .await?;
///
///     let result = redis::cmd("MGET")
///         .arg(&["key1", "key2"])
///         .query_async(&mut con)
///         .await;
///     assert_eq!(result, Ok(("foo".to_string(), b"bar".to_vec())));
///     println!("Result from MGET: {result:?}");
///
///     Ok(())
/// }
/// ```
pub fn build_with_tls<C: IntoConnectionInfo>(
    conn_info: C,
    client_cert: &mut dyn BufRead,
    client_key: &mut dyn BufRead,
    root_cert: &mut dyn BufRead,
) -> RedisResult<Client> {
    let mut connection_info = conn_info.into_connection_info()?;
    let tls_params = retrieve_tls_certificates(client_cert, client_key, root_cert)?;

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
    mut client_cert: &mut dyn BufRead,
    client_key: &mut dyn BufRead,
    mut root_cert: &mut dyn BufRead,
) -> RedisResult<TlsConnParams> {
    let client_cert: Vec<Certificate> =
        certs(&mut client_cert).map(|mut certs| certs.drain(..).map(Certificate).collect())?;

    let client_key = load_key(client_key)?;

    let certs = certs(&mut root_cert)?;

    let trust_anchors: RedisResult<Vec<OwnedTrustAnchor>> = certs
        .iter()
        .map(|cert| {
            let ta = webpki::TrustAnchor::try_from_cert_der(cert)
                .map_err(|_| Error::new(IOErrorKind::Other, "Unable to parse CA certificate"))?;

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

    Ok(TlsConnParams {
        client_key,
        client_cert,
        root_cert,
    })
}

#[derive(Debug, Clone)]
pub struct TlsConnParams {
    pub(crate) client_key: PrivateKey,
    pub(crate) client_cert: Vec<Certificate>,
    pub(crate) root_cert: RootCertStore,
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
