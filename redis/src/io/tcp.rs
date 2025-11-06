use std::{io, net::TcpStream};

#[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
use std::time::Duration;

#[cfg(not(target_family = "wasm"))]
pub use socket2;

/// Settings for a TCP stream.
#[derive(Clone, Debug)]
pub struct TcpSettings {
    nodelay: bool,
    #[cfg(not(target_family = "wasm"))]
    keepalive: Option<socket2::TcpKeepalive>,
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    user_timeout: Option<Duration>,
}

impl TcpSettings {
    /// Returns the value of the `TCP_NODELAY` option on this socket.
    pub fn nodelay(&self) -> bool {
        self.nodelay
    }

    /// Returns parameters configuring TCP keepalive probes for this socket.
    #[cfg(not(target_family = "wasm"))]
    pub fn keepalive(&self) -> Option<&socket2::TcpKeepalive> {
        self.keepalive.as_ref()
    }

    /// Returns the value of the `TCP_USER_TIMEOUT` option on this socket.
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    pub fn user_timeout(&self) -> Option<Duration> {
        self.user_timeout
    }

    /// Sets the value of the `TCP_NODELAY` option on this socket.
    pub fn set_nodelay(self, nodelay: bool) -> Self {
        Self { nodelay, ..self }
    }

    /// Set parameters configuring TCP keepalive probes for this socket.
    ///
    /// Default values are system-specific
    #[cfg(not(target_family = "wasm"))]
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
            nodelay: false,
            #[cfg(not(target_family = "wasm"))]
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
    #[cfg(not(target_family = "wasm"))]
    {
        let socket2: socket2::Socket = socket.into();
        if let Some(keepalive) = &settings.keepalive {
            socket2.set_tcp_keepalive(keepalive)?;
        }
        #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
        socket2.set_tcp_user_timeout(settings.user_timeout)?;
        Ok(socket2.into())
    }
    #[cfg(target_family = "wasm")]
    {
        Ok(socket)
    }
}
