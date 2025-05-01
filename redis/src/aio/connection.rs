use super::AsyncDNSResolver;
use super::RedisRuntime;

use crate::connection::{ConnectionAddr, ConnectionInfo};
use crate::io::tcp::TcpSettings;
#[cfg(feature = "aio")]
use crate::types::RedisResult;

use futures_util::future::select_ok;

pub(crate) async fn connect_simple<T: RedisRuntime>(
    connection_info: &ConnectionInfo,
    dns_resolver: &dyn AsyncDNSResolver,
    tcp_settings: &TcpSettings,
) -> RedisResult<T> {
    Ok(match connection_info.addr {
        ConnectionAddr::Tcp(ref host, port) => {
            let socket_addrs = dns_resolver.resolve(host, port).await?;
            select_ok(socket_addrs.map(|addr| Box::pin(<T>::connect_tcp(addr, tcp_settings))))
                .await?
                .0
        }

        #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
        ConnectionAddr::TcpTls {
            ref host,
            port,
            insecure,
            ref tls_params,
        } => {
            let socket_addrs = dns_resolver.resolve(host, port).await?;
            select_ok(socket_addrs.map(|socket_addr| {
                Box::pin(<T>::connect_tcp_tls(
                    host,
                    socket_addr,
                    insecure,
                    tls_params,
                    tcp_settings,
                ))
            }))
            .await?
            .0
        }

        #[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]
        ConnectionAddr::TcpTls { .. } => {
            fail!((
                crate::types::ErrorKind::InvalidClientConfig,
                "Cannot connect to TCP with TLS without the tls feature"
            ));
        }

        #[cfg(unix)]
        ConnectionAddr::Unix(ref path) => <T>::connect_unix(path).await?,

        #[cfg(not(unix))]
        ConnectionAddr::Unix(_) => {
            fail!((
                crate::types::ErrorKind::InvalidClientConfig,
                "Cannot connect to unix sockets \
                 on this platform",
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::cluster_async;

    use super::super::*;

    #[test]
    fn test_is_sync() {
        const fn assert_sync<T: Sync>() {}

        assert_sync::<MultiplexedConnection>();
        assert_sync::<PubSub>();
        assert_sync::<Monitor>();
        #[cfg(feature = "connection-manager")]
        assert_sync::<ConnectionManager>();
        #[cfg(feature = "cluster-async")]
        assert_sync::<cluster_async::ClusterConnection>();
    }

    #[test]
    fn test_is_send() {
        const fn assert_send<T: Send>() {}

        assert_send::<MultiplexedConnection>();
        assert_send::<PubSub>();
        assert_send::<Monitor>();
        #[cfg(feature = "connection-manager")]
        assert_send::<ConnectionManager>();
        #[cfg(feature = "cluster-async")]
        assert_send::<cluster_async::ClusterConnection>();
    }
}
