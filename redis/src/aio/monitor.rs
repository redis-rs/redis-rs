use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{ready, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Decoder;

use crate::{
    cmd, parser::ValueCodec, types::closed_connection_error, FromRedisValue, RedisConnectionInfo,
    RedisResult, Value,
};

use super::setup_connection;

/// Represents a `Monitor` connection.
pub struct Monitor {
    stream: Box<dyn Stream<Item = RedisResult<Value>> + Send + Sync + Unpin>,
}

impl Monitor {
    pub(crate) async fn new<C>(
        connection_info: &RedisConnectionInfo,
        stream: C,
    ) -> RedisResult<Self>
    where
        C: Unpin + AsyncRead + AsyncWrite + Send + Sync + 'static,
    {
        let mut codec = ValueCodec::default().framed(stream);
        setup_connection(
            &mut codec,
            connection_info,
            #[cfg(feature = "cache-aio")]
            None,
        )
        .await?;
        codec.send(cmd("MONITOR").get_packed_command()).await?;
        codec.next().await.ok_or_else(closed_connection_error)??;
        let stream = Box::new(codec);

        Ok(Self { stream })
    }

    /// Deliver the MONITOR command to this [`Monitor`]ing wrapper.
    #[deprecated(note = "A monitor now sends the MONITOR command automatically")]
    pub async fn monitor(&mut self) -> RedisResult<()> {
        Ok(())
    }

    /// Returns [`Stream`] of [`FromRedisValue`] values from this [`Monitor`]ing connection
    pub fn on_message<'a, T: FromRedisValue + 'a>(&'a mut self) -> impl Stream<Item = T> + 'a {
        MonitorStreamRef {
            monitor: self,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns [`Stream`] of [`FromRedisValue`] values from this [`Monitor`]ing connection
    pub fn into_on_message<T: FromRedisValue>(self) -> impl Stream<Item = T> {
        MonitorStream {
            stream: self.stream,
            _phantom: std::marker::PhantomData,
        }
    }
}

struct MonitorStream<T> {
    stream: Box<dyn Stream<Item = RedisResult<Value>> + Send + Sync + Unpin>,
    _phantom: std::marker::PhantomData<T>,
}
impl<T> Unpin for MonitorStream<T> {}

fn convert_value<T>(value: RedisResult<Value>) -> Option<T>
where
    T: FromRedisValue,
{
    value
        .ok()
        .and_then(|value| T::from_owned_redis_value(value).ok())
}

impl<T> Stream for MonitorStream<T>
where
    T: FromRedisValue,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(ready!(self.stream.poll_next_unpin(cx)).and_then(convert_value))
    }
}

struct MonitorStreamRef<'a, T> {
    monitor: &'a mut Monitor,
    _phantom: std::marker::PhantomData<T>,
}
impl<T> Unpin for MonitorStreamRef<'_, T> {}

impl<T> Stream for MonitorStreamRef<'_, T>
where
    T: FromRedisValue,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(ready!(self.monitor.stream.poll_next_unpin(cx)).and_then(convert_value))
    }
}
