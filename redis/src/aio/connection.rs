use super::AsyncDNSResolver;
use super::RedisRuntime;

use crate::connection::{ConnectionAddr, ConnectionInfo};
#[cfg(feature = "aio")]
use crate::types::RedisResult;

use super::ConnectionLike;
use crate::cmd::{cmd, pipe};
use crate::pipeline::Pipeline;
use crate::{FromRedisValue, RedisError, ToRedisArgs};
use futures_util::future::select_ok;
use log::trace;
use std::future::Future;

pub(crate) async fn connect_simple<T: RedisRuntime>(
    connection_info: &ConnectionInfo,
    dns_resolver: &dyn AsyncDNSResolver,
) -> RedisResult<T> {
    Ok(match connection_info.addr {
        ConnectionAddr::Tcp(ref host, port) => {
            let socket_addrs = dns_resolver.resolve(host, port).await?;
            select_ok(
                socket_addrs
                    .map(|addr| Box::pin(<T>::connect_tcp(addr, &connection_info.tcp_settings))),
            )
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
                    &connection_info.tcp_settings,
                ))
            }))
            .await?
            .0
        }

        #[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]
        ConnectionAddr::TcpTls { .. } => {
            fail!((
                crate::errors::ErrorKind::InvalidClientConfig,
                "Cannot connect to TCP with TLS without the tls feature"
            ));
        }

        #[cfg(unix)]
        ConnectionAddr::Unix(ref path) => <T>::connect_unix(path).await?,

        #[cfg(not(unix))]
        ConnectionAddr::Unix(_) => {
            fail!((
                crate::errors::ErrorKind::InvalidClientConfig,
                "Cannot connect to unix sockets \
                 on this platform",
            ))
        }
    })
}

/// Executes a Redis transaction asynchronously by automatically watching keys and running
/// a transaction loop until it succeeds. Similar to the synchronous [`transaction`](crate::transaction)
/// function but for async execution.
///
/// The provided closure may be executed multiple times if the transaction fails due to
/// watched keys being modified between WATCH and EXEC. Any side effects in the closure
/// should account for possible multiple executions.
///
/// # Arguments
///
/// * `connection` - A connection implementing [`ConnectionLike`](crate::aio::ConnectionLike) + Clone
/// * `keys` - Array of keys to watch for changes during the transaction
/// * `func` - Closure that implements the transaction logic. Will receive a cloned connection
///   and an atomic pipeline (pre-configured with `MULTI`/`EXEC`). Should return
///   `Result<Option<T>, RedisError>` where:
///   - `None` indicates the transaction should be retried
///   - `Some(T)` contains the final result if successful
///   - `Err` aborts the transaction
///
/// # Type Parameters
///
/// * `C` - Connection type implementing ConnectionLike + Clone
/// * `K` - Key type implementing ToRedisArgs
/// * `T` - Return value type implementing FromRedisValue
/// * `F` - Closure type taking (C, Pipeline) and returning Fut
/// * `Fut` - Future returning Result<Option<T>, RedisError>
///
/// # Examples
///
/// ```rust,no_run
/// use redis::{AsyncCommands, RedisResult, pipe};
///
/// async fn increment(con: redis::aio::ConnectionManager) -> RedisResult<isize> {
///     let key = "my_counter";
///     redis::aio::transaction_async(con, &[key], |mut con, mut pipe| async move {
///         // Read the current value first
///         let val: isize = con.get(key).await?;
///         // Build the pipeline and execute it atomically (MULTI/EXEC are added automatically)
///         pipe.set(key, val + 1)
///             .ignore()
///             .get(key)
///             .query_async(&mut con)
///             .await
///     })
///     .await
/// }
/// ```
///
/// # Notes
///
/// - The closure may be executed multiple times if watched keys are modified by other
///   clients between `WATCH` and `EXEC`; its side effects must be idempotent.
/// - A successful `EXEC` automatically discards all `WATCH`es, so no explicit `UNWATCH`
///   is needed on the success path.
/// - The transaction is automatically abandoned if the closure returns an error; an
///   explicit `UNWATCH` is sent in that case to leave the connection in a clean state.
///
/// ## Warning: Concurrent Transactions on Multiplexed Connections
///
/// When using a multiplexed connection (e.g. [`ConnectionManager`](crate::aio::ConnectionManager)),
/// cloning shares the underlying channel. Running concurrent transactions on clones of
/// the same multiplexed connection is **unsafe**: the `WATCH`/`MULTI`/`EXEC` sequence
/// from one transaction may interleave with commands from another, causing data
/// corruption. Ensure at most one transaction is active on a given multiplexed
/// connection at a time.
///
/// For more details on Redis transactions, see the [Redis documentation](https://redis.io/topics/transactions)
pub async fn transaction_async<
    C: ConnectionLike + Clone,
    K: ToRedisArgs,
    T: FromRedisValue,
    F: Fn(C, Pipeline) -> Fut,
    Fut: Future<Output = Result<Option<T>, RedisError>>,
>(
    mut connection: C,
    keys: &[K],
    func: F,
) -> Result<T, RedisError> {
    loop {
        cmd("WATCH").arg(keys).exec_async(&mut connection).await?;

        let mut p = pipe();
        let response = func(connection.clone(), p.atomic().to_owned()).await;

        match response {
            Ok(None) => {
                trace!("Transaction failed, retrying");
                // WATCH is automatically cleared by a failed EXEC, so loop back directly.
                // Send UNWATCH as a best-effort safety net for any edge cases where EXEC
                // was not reached (e.g. the closure returned None before calling query_async).
                let _ = cmd("UNWATCH").exec_async(&mut connection).await;
                continue;
            }
            Ok(Some(value)) => {
                // A successful EXEC already discards all WATCHes; no extra UNWATCH needed.
                return Ok(value);
            }
            Err(err) => {
                // The closure aborted. WATCH may still be active, so discard it to leave
                // the connection in a clean state for subsequent commands.
                let _ = cmd("UNWATCH").exec_async(&mut connection).await;
                return Err(err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "cluster-async")]
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
