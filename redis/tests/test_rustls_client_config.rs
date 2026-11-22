//! Tests for passing an `Arc<rustls::ClientConfig>` as the TLS configuration. None of them need a running Redis server:
//! connection tests use an in-process TLS server that only performs the handshake.
#![cfg(feature = "tls-rustls")]

use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use redis::Client;
use redis_test::utils::build_keys_and_certs_for_tls;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

const VERIFIER_MESSAGE: &str = "custom verifier was consulted";

/// Records that it was called and then refuses every certificate.
#[derive(Debug, Default)]
struct RejectingVerifier {
    called: Arc<AtomicBool>,
}

impl ServerCertVerifier for RejectingVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        self.called.store(true, Ordering::SeqCst);
        Err(rustls::Error::General(VERIFIER_MESSAGE.into()))
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Starts a server that accepts connections forever and runs only the TLS handshake.
fn start_tls_server() -> SocketAddr {
    let dir = tempfile::tempdir().unwrap();
    let paths = build_keys_and_certs_for_tls(&dir);
    let certs = CertificateDer::pem_file_iter(&paths.redis_crt)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_file(&paths.redis_key).unwrap();
    let config = Arc::new(
        rustls::ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap(),
    );
    drop(dir);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let Ok(mut conn) = rustls::ServerConnection::new(config.clone()) else {
                continue;
            };
            // The client aborts the handshake, so errors are expected.
            let _ = conn.complete_io(&mut stream);
        }
    });
    addr
}

/// A client configuration whose verifier records that it was used.
fn rejecting_config() -> (Arc<rustls::ClientConfig>, Arc<AtomicBool>) {
    let called = Arc::new(AtomicBool::new(false));
    let config = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(RejectingVerifier {
            called: called.clone(),
        }))
        .with_no_client_auth();
    (Arc::new(config), called)
}

/// The error is the custom verifier's, so the configuration was used for the handshake.
fn assert_rejected_by_verifier(err: redis::RedisError, called: &AtomicBool) {
    assert!(err.to_string().contains(VERIFIER_MESSAGE), "{err}");
    assert!(called.load(Ordering::SeqCst));
}

fn client_for(addr: SocketAddr, config: Arc<rustls::ClientConfig>) -> Client {
    Client::build_with_rustls_config(format!("rediss://127.0.0.1:{}", addr.port()), config).unwrap()
}

#[test]
fn sync_connection_uses_the_custom_config() {
    let addr = start_tls_server();
    let (config, called) = rejecting_config();
    let client = client_for(addr, config);

    let result = client
        .get_connection()
        .and_then(|mut con| redis::cmd("PING").query::<String>(&mut con));

    assert_rejected_by_verifier(result.unwrap_err(), &called);
}

#[cfg(feature = "tokio-rustls-comp")]
#[test]
fn tokio_connection_uses_the_custom_config() {
    let addr = start_tls_server();
    let (config, called) = rejecting_config();
    let client = client_for(addr, config);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(async {
        let mut con = client.get_multiplexed_async_connection().await?;
        redis::cmd("PING").query_async::<String>(&mut con).await
    });

    assert_rejected_by_verifier(result.unwrap_err(), &called);
}

#[cfg(feature = "smol-rustls-comp")]
#[test]
fn smol_connection_uses_the_custom_config() {
    let addr = start_tls_server();
    let (config, called) = rejecting_config();
    let client = client_for(addr, config);

    let result = smol::block_on(async {
        let mut con = client.get_multiplexed_async_connection().await?;
        redis::cmd("PING").query_async::<String>(&mut con).await
    });

    assert_rejected_by_verifier(result.unwrap_err(), &called);
}

/// Hostname verification can only be made stricter by the public setter, never looser: the
/// custom verifier is still consulted and still decides.
#[cfg(any(feature = "tls-rustls-insecure", feature = "tls-native-tls"))]
#[test]
fn public_invalid_hostnames_setter_does_not_bypass_the_custom_verifier() {
    let addr = start_tls_server();
    let (config, called) = rejecting_config();
    let client = client_for(addr, config);

    let connection_info = client.get_connection_info().clone();
    let mut addr = connection_info.addr().clone();
    addr.set_danger_accept_invalid_hostnames(true);
    let client = Client::open(connection_info.set_addr(addr)).unwrap();

    let Err(err) = client.get_connection() else {
        panic!("the verifier rejects every certificate");
    };
    assert_rejected_by_verifier(err, &called);
}

/// `ConnectionAddr` is public and its `insecure` field can be changed after the client was
/// built, which must not replace the verifier of the custom configuration.
#[test]
fn insecure_address_is_rejected_when_connecting() {
    let addr = start_tls_server();
    let (config, called) = rejecting_config();
    let client = client_for(addr, config);

    let connection_info = client.get_connection_info().clone();
    let redis::ConnectionAddr::TcpTls {
        host,
        port,
        tls_params,
        ..
    } = connection_info.addr().clone()
    else {
        panic!("expected a TLS address");
    };
    let insecure_addr = redis::ConnectionAddr::TcpTls {
        host,
        port,
        insecure: true,
        tls_params,
    };
    let client = Client::open(connection_info.set_addr(insecure_addr)).unwrap();

    let Err(err) = client.get_connection() else {
        panic!("an insecure connection must be rejected");
    };
    assert_eq!(err.kind(), redis::ErrorKind::InvalidClientConfig);
    assert!(err.to_string().contains("insecure connection"), "{err}");
    assert!(!called.load(Ordering::SeqCst));
}
