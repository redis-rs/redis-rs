use std::{
    net::{SocketAddr, TcpStream, ToSocketAddrs as _},
    time::Duration,
};

use crate::{
    io::tcp::{stream_with_settings, TcpSettings},
    ErrorKind, RedisResult,
};

#[inline(always)]
pub(super) fn connect_tcp(addr: (&str, u16)) -> std::io::Result<TcpStream> {
    let socket = TcpStream::connect(addr)?;
    stream_with_settings(socket, &TcpSettings::default())
}

#[inline(always)]
pub(super) fn connect_tcp_timeout(
    addr: &SocketAddr,
    timeout: Duration,
) -> std::io::Result<TcpStream> {
    let socket = TcpStream::connect_timeout(addr, timeout)?;
    stream_with_settings(socket, &TcpSettings::default())
}

pub struct TcpConnection {
    pub(super) reader: TcpStream,
    pub(super) open: bool,
}

impl TcpConnection {
    pub(super) fn try_new(host: &str, port: u16, timeout: Option<Duration>) -> RedisResult<Self> {
        let addr = (host, port);
        let tcp = match timeout {
            None => connect_tcp(addr)?,
            Some(timeout) => {
                let mut tcp = None;
                let mut last_error = None;
                for addr in addr.to_socket_addrs()? {
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
                    (Some(tcp), _) => tcp,
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
        Ok(Self {
            reader: tcp,
            open: true,
        })
    }
}
