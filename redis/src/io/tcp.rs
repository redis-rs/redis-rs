use std::{io, net::TcpStream};

#[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
use std::time::Duration;

pub use socket2;

/// Settings for a TCP stream.
#[derive(Clone, Debug)]
pub struct TcpSettings {
    nodelay: bool,
    keepalive: Option<socket2::TcpKeepalive>,
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    user_timeout: Option<Duration>,
}

impl TcpSettings {
    /// Sets the value of the `TCP_NODELAY` option on this socket.
    pub fn set_nodelay(self, nodelay: bool) -> Self {
        Self { nodelay, ..self }
    }

    /// Set parameters configuring TCP keepalive probes for this socket.
    ///
    /// Default values are system-specific
    pub fn set_keepalive(self, keepalive: socket2::TcpKeepalive) -> Self {
        Self {
            keepalive: Some(keepalive),
            ..self
        }
    }

    /// Set the value of the `TCP_USER_TIMEOUT` option on this socket.
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    pub fn set_user_timeout(self, user_timeout: Duration) -> Self {
        Self {
            user_timeout: Some(user_timeout),
            ..self
        }
    }
}

#[allow(clippy::derivable_impls)]
impl Default for TcpSettings {
    fn default() -> Self {
        Self {
            #[cfg(feature = "tcp_nodelay")]
            nodelay: true,
            #[cfg(not(feature = "tcp_nodelay"))]
            nodelay: false,
            keepalive: None,
            #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
            user_timeout: None,
        }
    }
}

pub(crate) fn stream_with_settings(
    socket: TcpStream,
    settings: &TcpSettings,
) -> io::Result<TcpStream> {
    socket.set_nodelay(settings.nodelay)?;
    let socket2: socket2::Socket = socket.into();
    if let Some(keepalive) = &settings.keepalive {
        socket2.set_tcp_keepalive(keepalive)?;
    }
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    socket2.set_tcp_user_timeout(settings.user_timeout)?;
    Ok(socket2.into())
}
