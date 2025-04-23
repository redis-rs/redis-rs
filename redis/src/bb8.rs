use crate::aio::MultiplexedConnection;
use crate::{Client, Cmd, ErrorKind, RedisError};

#[cfg(feature = "cluster-async")]
use crate::{cluster::ClusterClient, cluster_async::ClusterConnection};

macro_rules! impl_bb8_manage_connection {
    ($client:ty, $connectioin:ty, $get_conn:expr) => {
        impl bb8::ManageConnection for $client {
            type Connection = $connectioin;
            type Error = RedisError;

            async fn connect(&self) -> Result<Self::Connection, Self::Error> {
                $get_conn(self).await
            }

            async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
                let pong: String = Cmd::ping().query_async(conn).await?;
                match pong.as_str() {
                    "PONG" => Ok(()),
                    _ => Err((ErrorKind::ResponseError, "ping request").into()),
                }
            }

            fn has_broken(&self, _: &mut Self::Connection) -> bool {
                false
            }
        }
    };
}

impl_bb8_manage_connection!(
    Client,
    MultiplexedConnection,
    Client::get_multiplexed_async_connection
);

#[cfg(feature = "cluster-async")]
impl_bb8_manage_connection!(
    ClusterClient,
    ClusterConnection,
    ClusterClient::get_async_connection
);

// TODO: support bb8 for sentinel client which required
// [`crate::sentinel::LockedSentinelClient`] implement async method of
// `get_multiplexed_async_connection`.
