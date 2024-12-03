//! Adds async IO support to redis.
use crate::cmd::Cmd;
use crate::connection::{
    check_connection_setup, connection_setup_pipeline, AuthResult, ConnectionSetupComponents,
    RedisConnectionInfo,
};
use crate::types::{RedisFuture, RedisResult, Value};
use crate::PushInfo;
use ::tokio::io::{AsyncRead, AsyncWrite};
use futures_util::Future;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::Path;
use std::pin::Pin;

/// Enables the async_std compatibility
#[cfg(feature = "async-std-comp")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std-comp")))]
pub mod async_std;

#[cfg(feature = "tls-rustls")]
use crate::tls::TlsConnParams;

#[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
use crate::connection::TlsConnParams;

/// Enables the tokio compatibility
#[cfg(feature = "tokio-comp")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio-comp")))]
pub mod tokio;

mod pubsub;
pub use pubsub::{PubSub, PubSubSink, PubSubStream};

/// Represents the ability of connecting via TCP or via Unix socket
pub(crate) trait RedisRuntime: AsyncStream + Send + Sync + Sized + 'static {
    /// Performs a TCP connection
    async fn connect_tcp(socket_addr: SocketAddr) -> RedisResult<Self>;

    // Performs a TCP TLS connection
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    async fn connect_tcp_tls(
        hostname: &str,
        socket_addr: SocketAddr,
        insecure: bool,
        tls_params: &Option<TlsConnParams>,
    ) -> RedisResult<Self>;

    /// Performs a UNIX connection
    #[cfg(unix)]
    async fn connect_unix(path: &Path) -> RedisResult<Self>;

    fn spawn(f: impl Future<Output = ()> + Send + 'static) -> TaskHandle;

    fn boxed(self) -> Pin<Box<dyn AsyncStream + Send + Sync>> {
        Box::pin(self)
    }
}

/// Trait for objects that implements `AsyncRead` and `AsyncWrite`
pub trait AsyncStream: AsyncRead + AsyncWrite {}
impl<S> AsyncStream for S where S: AsyncRead + AsyncWrite {}

/// An async abstraction over connections.
pub trait ConnectionLike {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value>;

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    /// Important - this function is meant for internal usage, since it's
    /// easy to pass incorrect `offset` & `count` parameters, which might
    /// cause the connection to enter an erroneous state. Users shouldn't
    /// call it, instead using the Pipeline::query_async function.
    #[doc(hidden)]
    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>>;

    /// Returns the database this connection is bound to.  Note that this
    /// information might be unreliable because it's initially cached and
    /// also might be incorrect if the connection like object is not
    /// actually connected.
    fn get_db(&self) -> i64;
}

async fn execute_connection_pipeline(
    rv: &mut impl ConnectionLike,
    (pipeline, instructions): (crate::Pipeline, ConnectionSetupComponents),
) -> RedisResult<AuthResult> {
    if pipeline.len() == 0 {
        return Ok(AuthResult::Succeeded);
    }

    let results = rv.req_packed_commands(&pipeline, 0, pipeline.len()).await?;

    check_connection_setup(results, instructions)
}

// Initial setup for every connection.
async fn setup_connection(
    connection_info: &RedisConnectionInfo,
    con: &mut impl ConnectionLike,
) -> RedisResult<()> {
    if execute_connection_pipeline(con, connection_setup_pipeline(connection_info, true)).await?
        == AuthResult::ShouldRetryWithoutUsername
    {
        execute_connection_pipeline(con, connection_setup_pipeline(connection_info, false)).await?;
    }

    Ok(())
}

mod connection;
pub use connection::*;
mod multiplexed_connection;
pub use multiplexed_connection::*;
#[cfg(feature = "connection-manager")]
mod connection_manager;
#[cfg(feature = "connection-manager")]
#[cfg_attr(docsrs, doc(cfg(feature = "connection-manager")))]
pub use connection_manager::*;
mod runtime;
pub(super) use runtime::*;

macro_rules! check_resp3 {
    ($protocol: expr) => {
        use crate::types::ProtocolVersion;
        if $protocol == ProtocolVersion::RESP2 {
            return Err(RedisError::from((
                crate::ErrorKind::InvalidClientConfig,
                "RESP3 is required for this command",
            )));
        }
    };

    ($protocol: expr, $message: expr) => {
        use crate::types::ProtocolVersion;
        if $protocol == ProtocolVersion::RESP2 {
            return Err(RedisError::from((
                crate::ErrorKind::InvalidClientConfig,
                $message,
            )));
        }
    };
}

pub(crate) use check_resp3;

/// An error showing that the receiver
pub struct SendError;

/// A trait for sender parts of a channel that can be used for sending push messages from async
/// connection.
pub trait AsyncPushSender: Send + Sync + 'static {
    /// The sender must send without blocking, otherwise it will block the sending connection.
    /// Should error when the receiver was closed, and pushing values on the sender is no longer viable.
    fn send(&self, info: PushInfo) -> Result<(), SendError>;
}

impl AsyncPushSender for ::tokio::sync::mpsc::UnboundedSender<PushInfo> {
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        match self.send(info) {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError),
        }
    }
}

impl AsyncPushSender for ::tokio::sync::broadcast::Sender<PushInfo> {
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        match self.send(info) {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError),
        }
    }
}

impl<T, Func: Fn(PushInfo) -> Result<(), T> + Send + Sync + 'static> AsyncPushSender for Func {
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        match self(info) {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError),
        }
    }
}

impl AsyncPushSender for std::sync::mpsc::Sender<PushInfo> {
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        match self.send(info) {
            Ok(_) => Ok(()),
            Err(_) => Err(SendError),
        }
    }
}

impl<T> AsyncPushSender for std::sync::Arc<T>
where
    T: AsyncPushSender,
{
    fn send(&self, info: PushInfo) -> Result<(), SendError> {
        self.as_ref().send(info)
    }
}
