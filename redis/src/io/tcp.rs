use std::{io, net::TcpStream, time::Duration};

/// Settings for a TCP stream.
#[derive(Clone, Debug)]
pub struct TcpSettings {
    nodelay: bool,
    keepalive: socket2::TcpKeepalive,
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    user_timeout: Option<Duration>,
}

impl TcpSettings {
    /// Sets the value of the `TCP_NODELAY` option on this socket.
    pub fn set_nodelay(self, nodelay: bool) -> Self {
        Self { nodelay, ..self }
    }

    /// Set parameters configuring TCP keepalive probes for this socket.
    pub fn set_keepalive(self, keepalive: socket2::TcpKeepalive) -> Self {
        Self { keepalive, ..self }
    }

    /// Set the value of the `TCP_USER_TIMEOUT` option on this socket.
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    pub fn set_user_timeout(self, user_timeout: Option<Duration>) -> Self {
        Self {
            user_timeout,
            ..self
        }
    }
}

impl Default for TcpSettings {
    fn default() -> Self {
        Self {
            #[cfg(feature = "tcp_nodelay")]
            nodelay: true,
            #[cfg(not(feature = "tcp_nodelay"))]
            nodelay: false,
            keepalive: socket2::TcpKeepalive::new(),
            #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
            user_timeout: Some(Duration::from_secs(5)),
        }
    }
}

pub(crate) fn stream_with_settings(
    socket: TcpStream,
    settings: &TcpSettings,
) -> io::Result<TcpStream> {
    socket.set_nodelay(settings.nodelay)?;
    let socket2: socket2::Socket = socket.into();
    socket2.set_tcp_keepalive(&settings.keepalive)?;
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    socket2.set_tcp_user_timeout(settings.user_timeout)?;
    Ok(socket2.into())
}
