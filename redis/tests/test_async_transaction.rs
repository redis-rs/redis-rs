use redis::{
    aio::ConnectionManager, transaction_async, AsyncCommands, Client, Pipeline, RedisError,
    RedisResult,
};

const REDIS_URL: &str = "redis://localhost:6379";

#[tokio::test]
pub async fn test_async_transaction() -> RedisResult<()> {
    let client = Client::open(REDIS_URL)?;
    let mgr = ConnectionManager::new(client).await?;

    async fn return_blah(
        mut mgr: ConnectionManager,
        mut pipeline: Pipeline,
    ) -> Result<Option<Vec<String>>, RedisError> {
        pipeline
            .set("key", "blah")
            .ignore()
            .get("key")
            .query_async(&mut mgr)
            .await
    }

    let res = transaction_async!(mgr.clone(), &["key"], return_blah)?;
    assert_eq!(res, vec!["blah".to_string()]);

    let res: Vec<String> = transaction_async!(
        mgr.clone(),
        &["key"],
        |mut mgr: ConnectionManager, mut pipeline: Pipeline| async move {
            pipeline
                .set("key", "blah")
                .ignore()
                .get("key")
                .query_async(&mut mgr)
                .await
        }
    )?;
    assert_eq!(res, vec!["blah".to_string()]);

    // now insert a key/value and modify it in a transaction
    mgr.clone().set("key", "value").await?;
    async fn modify_key(
        mut mgr: ConnectionManager,
        mut pipeline: Pipeline,
    ) -> Result<Option<Vec<String>>, RedisError> {
        let value: String = mgr.get("key").await?;
        // do some dummy stuff
        let new_value = format!("{}{}", value, value);
        pipeline
            .set("key", &new_value)
            .ignore()
            .get("key")
            .query_async(&mut mgr)
            .await
    }
    let new_value: Vec<String> = transaction_async!(mgr.clone(), &["key"], modify_key)?;
    let actual_value: String = mgr.clone().get("key").await?;
    assert_eq!(new_value[0], actual_value);

    Ok(())
}
