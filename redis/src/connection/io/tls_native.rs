use native_tls::{TlsConnector, TlsStream};
use std::{
    net::{TcpStream, ToSocketAddrs as _},
    time::Duration,
};

use crate::{ErrorKind, RedisResult};

use super::tcp::{connect_tcp, connect_tcp_timeout};

pub struct TcpNativeTlsConnection {
    pub(super) reader: TlsStream<TcpStream>,
    pub(super) open: bool,
}

impl TcpNativeTlsConnection {
    pub(super) fn try_new(
        host: &str,
        port: u16,
        insecure: bool,
        timeout: Option<Duration>,
    ) -> RedisResult<Self> {
        let tls_connector = if insecure {
            TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .use_sni(false)
                .build()?
        } else {
            TlsConnector::new()?
        };
        let addr = (host, port);
        let tls = match timeout {
            None => {
                let tcp = connect_tcp(addr)?;
                match tls_connector.connect(host, tcp) {
                    Ok(res) => res,
                    Err(e) => {
                        fail!((ErrorKind::IoError, "SSL Handshake error", e.to_string()));
                    }
                }
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
                    (Some(tcp), _) => tls_connector.connect(host, tcp).unwrap(),
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
        Ok(TcpNativeTlsConnection {
            reader: tls,
            open: true,
        })
    }
}
