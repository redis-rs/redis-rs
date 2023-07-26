use std::pin::Pin;

use crate::{aio::ConnectionManager, cmd, pipe, Pipeline, RedisError, ToRedisArgs};
use futures::Future;

/// This function encapsulates the boilerplate required to establish a Redis transaction.
/// Do not use it directly but use the `transaction_async!` macro instead.
/// See the `transaction_async!` macro for more details.
pub async fn transaction_async<
    T,
    E: From<RedisError>,
    K: ToRedisArgs,
    F: FnMut(
        ConnectionManager,
        &mut Pipeline,
    ) -> Pin<Box<dyn Future<Output = Result<Option<T>, E>> + Send>>,
>(
    mut mgr: ConnectionManager,
    keys: &[K],
    mut func: F,
) -> Result<T, E> {
    loop {
        cmd("WATCH").arg(keys).query_async(&mut mgr).await?;
        let mut p = pipe();
        let response: Option<T> = func(mgr.clone(), p.atomic()).await?;
        match response {
            None => {
                println!("retrying transaction");
                continue;
            }
            Some(response) => {
                // make sure no watch is left in the connection, even if
                // someone forgot to use the pipeline.
                cmd("UNWATCH").query_async(&mut mgr).await?;
                return Ok(response);
            }
        }
    }
}

/// Asynchronous transaction macro for Redis operations.
///
/// This macro encapsulates the boilerplate required to establish a Redis transaction
/// and apply a function to it. What it
/// does is automatically watching keys and then going into a transaction
/// loop util it succeeds.  Once it goes through the results are
/// returned.
///
/// To use the transaction two pieces of information are needed: a list
/// of all the keys that need to be watched for modifications and a
/// closure with the code that should be execute in the context of the
/// transaction.  The closure is invoked with a fresh pipeline in atomic
/// mode.  To use the transaction the function needs to return the result
/// from querying the pipeline with the connection.
///
/// The end result of the transaction is then available as the return
/// value from the function call.
///
/// # Parameters
///
/// - `$mgr`: A cloned connection manager for Redis.
/// - `$key`: A key (or array of keys) for the Redis operation.
/// - `$func`: Either a path to a function or a lambda function. This function
///   should have the signature `async fn(ConnectionManager, Pipeline) -> Result<Option<T>, RedisError>`
///   where `T` is the expected return type.
///
/// # Examples
///
/// Example with function path:
///
/// ```
///async fn return_blah(
///     mut mgr: ConnectionManager,
///     mut pipeline: Pipeline,
///) -> Result<Option<Vec<String>>, RedisError> {
///     pipeline
///         .set("key", "blah")
///         .ignore()
///         .get("key")
///         .query_async(&mut mgr)
///     .await
///}
///
/// let res = transaction_async!(mgr.clone(), &["key"], return_blah)?;
/// assert_eq!(res, vec!["blah".to_string()]);
/// ```
///
/// Example with lambda function:
///
/// ```
/// let res: Vec<String> = transaction_async!(
///     mgr.clone(),
///     &["key"],
///     |mut mgr: ConnectionManager, mut pipeline: Pipeline| async move {
///         pipeline
///             .set("key", "blah")
///             .ignore()
///             .get("key")
///             .query_async(&mut mgr)
///             .await
///     }
/// )?;
/// assert_eq!(res, vec!["blah".to_string()]);
/// ```
///
/// # Returns
///
/// Returns a `Result` containing the return value of the passed function, or an error.
///
/// Note: This macro is exported so it can be used in other modules.
#[macro_export]
macro_rules! transaction_async {
    ($mgr:expr, $key:expr, $func:path) => {{
        $crate::aio::async_transactions::transaction_async($mgr, $key, |mgr, pipeline| {
            let pipeline = pipeline.clone();
            Box::pin(async move { $func(mgr, pipeline).await })
        })
        .await
    }};
    ($mgr:expr, $key:expr, $func:expr) => {{
        $crate::aio::async_transactions::transaction_async($mgr, $key, |mgr, pipeline| {
            let pipeline = pipeline.clone();
            Box::pin($func(mgr, pipeline))
        })
        .await
    }};
}
