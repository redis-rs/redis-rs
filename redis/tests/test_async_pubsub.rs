mod support;

#[cfg(test)]
mod async_pubsub {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{self, Poll};
    use std::{collections::HashMap, time::Duration};

    use futures::{Stream, StreamExt};
    use futures_time::task::sleep;

    use redis::aio::{
        PubSub, PubSubManager, PubSubManagerSink, PubSubManagerStream, PubSubSink, PubSubStream,
        PubsubManagerConfig,
    };
    use redis::{AsyncCommands, FromRedisValue, Msg, RedisError, RedisResult, ToRedisArgs};
    use rstest::rstest;

    use crate::support::*;

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn pub_sub_subscription(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let mut pubsub_conn = ctx.async_pubsub().await?;
                let _: () = pubsub_conn.subscribe("phonewave").await?;
                let mut pubsub_stream = pubsub_conn.on_message();
                let mut publish_conn = ctx.async_connection().await?;
                let _: () = publish_conn.publish("phonewave", "banana").await?;

                let repeats = 6;
                for _ in 0..repeats {
                    let _: () = publish_conn.publish("phonewave", "banana").await?;
                }

                for _ in 0..repeats {
                    let message: String =
                        pubsub_stream.next().await.unwrap().get_payload().unwrap();

                    assert_eq!("banana".to_string(), message);
                }

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn pub_sub_subscription_to_multiple_channels(#[case] runtime: RuntimeType) {
        use redis::RedisError;

        let ctx = TestContext::new();
        block_on_all(
            async move {
                let mut pubsub_conn = ctx.async_pubsub().await?;
                let _: () = pubsub_conn.subscribe(&["phonewave", "foo", "bar"]).await?;
                let mut pubsub_stream = pubsub_conn.on_message();
                let mut publish_conn = ctx.async_connection().await?;
                let _: () = publish_conn.publish("phonewave", "banana").await?;

                let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
                assert_eq!("banana".to_string(), msg_payload);

                let _: () = publish_conn.publish("foo", "foobar").await?;
                let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
                assert_eq!("foobar".to_string(), msg_payload);

                Ok::<_, RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn pub_sub_unsubscription(#[case] runtime: RuntimeType) {
        const SUBSCRIPTION_KEY: &str = "phonewave-pub-sub-unsubscription";

        let ctx = TestContext::new();
        block_on_all(
            async move {
                let mut pubsub_conn = ctx.async_pubsub().await?;
                pubsub_conn.subscribe(SUBSCRIPTION_KEY).await?;
                pubsub_conn.unsubscribe(SUBSCRIPTION_KEY).await?;

                let mut conn = ctx.async_connection().await?;
                let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                    .arg("NUMSUB")
                    .arg(SUBSCRIPTION_KEY)
                    .query_async(&mut conn)
                    .await?;
                let subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
                assert_eq!(subscription_count, 0);

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn can_receive_messages_while_sending_requests_from_split_pub_sub(
        #[case] runtime: RuntimeType,
    ) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let (mut sink, mut stream) = ctx.async_pubsub().await?.split();
                let mut publish_conn = ctx.async_connection().await?;

                let _: () = sink.subscribe("phonewave").await?;
                let repeats = 6;
                for _ in 0..repeats {
                    let _: () = publish_conn.publish("phonewave", "banana").await?;
                }

                for _ in 0..repeats {
                    let message: String = stream.next().await.unwrap().get_payload().unwrap();

                    assert_eq!("banana".to_string(), message);
                }

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn can_send_ping_on_split_pubsub(#[case] runtime: RuntimeType) {
        use redis::ProtocolVersion;

        let ctx = TestContext::new();
        block_on_all(
            async move {
                let (mut sink, mut stream) = ctx.async_pubsub().await?.split();
                let mut publish_conn = ctx.async_connection().await?;

                let _: () = sink.subscribe("phonewave").await?;

                // we publish before the ping, to verify that published messages don't distort the ping's resuilt.
                let repeats = 6;
                for _ in 0..repeats {
                    let _: () = publish_conn.publish("phonewave", "banana").await?;
                }

                if ctx.protocol == ProtocolVersion::RESP3 {
                    let message: String = sink.ping().await?;
                    assert_eq!(message, "PONG");
                } else {
                    let message: Vec<String> = sink.ping().await?;
                    assert_eq!(message, vec!["pong", ""]);
                }

                if ctx.protocol == ProtocolVersion::RESP3 {
                    let message: String = sink.ping_message("foobar").await?;
                    assert_eq!(message, "foobar");
                } else {
                    let message: Vec<String> = sink.ping_message("foobar").await?;
                    assert_eq!(message, vec!["pong", "foobar"]);
                }

                for _ in 0..repeats {
                    let message: String = stream.next().await.unwrap().get_payload()?;

                    assert_eq!("banana".to_string(), message);
                }

                // after the stream is closed, pinging should fail.
                drop(stream);
                let err = sink.ping_message::<()>("foobar").await.unwrap_err();
                assert!(err.is_unrecoverable_error());

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn can_receive_messages_from_split_pub_sub_after_sink_was_dropped(
        #[case] runtime: RuntimeType,
    ) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let (mut sink, mut stream) = ctx.async_pubsub().await?.split();
                let mut publish_conn = ctx.async_connection().await?;

                let _: () = sink.subscribe("phonewave").await?;
                drop(sink);
                let repeats = 6;
                for _ in 0..repeats {
                    let _: () = publish_conn.publish("phonewave", "banana").await?;
                }

                for _ in 0..repeats {
                    let message: String = stream.next().await.unwrap().get_payload().unwrap();

                    assert_eq!("banana".to_string(), message);
                }

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn can_receive_messages_from_split_pub_sub_after_into_on_message(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let mut pubsub = ctx.async_pubsub().await?;
                let mut publish_conn = ctx.async_connection().await?;

                let _: () = pubsub.subscribe("phonewave").await?;
                let mut stream = pubsub.into_on_message();
                // wait a bit
                sleep(Duration::from_secs(2).into()).await;
                let repeats = 6;
                for _ in 0..repeats {
                    let _: () = publish_conn.publish("phonewave", "banana").await?;
                }

                for _ in 0..repeats {
                    let message: String = stream.next().await.unwrap().get_payload().unwrap();

                    assert_eq!("banana".to_string(), message);
                }

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn cannot_subscribe_on_split_pub_sub_after_stream_was_dropped(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let (mut sink, stream) = ctx.async_pubsub().await?.split();
                drop(stream);

                assert!(sink.subscribe("phonewave").await.is_err());

                Ok(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn automatic_unsubscription(#[case] runtime: RuntimeType) {
        const SUBSCRIPTION_KEY: &str = "phonewave-automatic-unsubscription";

        let ctx = TestContext::new();
        block_on_all(
            async move {
                let mut pubsub_conn = ctx.async_pubsub().await?;
                pubsub_conn.subscribe(SUBSCRIPTION_KEY).await?;
                drop(pubsub_conn);

                let mut conn = ctx.async_connection().await?;
                let mut subscription_count = 1;
                // Allow for the unsubscription to occur within 5 seconds
                for _ in 0..100 {
                    let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                        .arg("NUMSUB")
                        .arg(SUBSCRIPTION_KEY)
                        .query_async(&mut conn)
                        .await?;
                    subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
                    if subscription_count == 0 {
                        break;
                    }

                    sleep(Duration::from_millis(50).into()).await;
                }
                assert_eq!(subscription_count, 0);

                Ok::<_, RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn automatic_unsubscription_on_split(#[case] runtime: RuntimeType) {
        const SUBSCRIPTION_KEY: &str = "phonewave-automatic-unsubscription-on-split";

        let ctx = TestContext::new();
        block_on_all(
            async move {
                let (mut sink, stream) = ctx.async_pubsub().await?.split();
                sink.subscribe(SUBSCRIPTION_KEY).await?;
                let mut conn = ctx.async_connection().await?;
                sleep(Duration::from_millis(100).into()).await;

                let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                    .arg("NUMSUB")
                    .arg(SUBSCRIPTION_KEY)
                    .query_async(&mut conn)
                    .await?;
                let mut subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
                assert_eq!(subscription_count, 1);

                drop(stream);

                // Allow for the unsubscription to occur within 5 seconds
                for _ in 0..100 {
                    let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                        .arg("NUMSUB")
                        .arg(SUBSCRIPTION_KEY)
                        .query_async(&mut conn)
                        .await?;
                    subscription_count = *subscriptions_counts.get(SUBSCRIPTION_KEY).unwrap();
                    if subscription_count == 0 {
                        break;
                    }

                    sleep(Duration::from_millis(50).into()).await;
                }
                assert_eq!(subscription_count, 0);

                // verify that the sink is unusable after the stream is dropped.
                let err = sink.subscribe(SUBSCRIPTION_KEY).await.unwrap_err();
                assert!(err.is_unrecoverable_error(), "{err:?}");

                Ok::<_, RedisError>(())
            },
            runtime,
            pubsub_type,
        );
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[case::async_std(RuntimeType::AsyncStd)]
    fn manager_should_reconnect_after_disconnect(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let mut pubsub_conn = ctx.client.get_async_pubsub_manager().await?;
                let _: () = pubsub_conn.subscribe(&["phonewave", "foo", "bar"]).await?;
                let _: () = pubsub_conn.psubscribe(&["zoom*"]).await?;
                let _: () = pubsub_conn.unsubscribe("foo").await?;

                let addr = ctx.server.client_addr().clone();
                drop(ctx);
                // a yield, to let the pubsub to notice the broken connection.
                // this is required to reduce differences in test runs between async-std & tokio runtime.
                sleep(Duration::from_millis(1).into()).await;
                let ctx = TestContext::new_with_addr(addr);

                // wait until reconnected
                while pubsub_conn.ping::<()>().await.is_err() {}
                let mut pubsub_stream = pubsub_conn.on_message();

                let mut publish_conn = ctx.async_connection().await?;
                let _: () = publish_conn.publish("phonewave", "banana").await?;

                let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
                assert_eq!("banana".to_string(), msg_payload);

                // this should be skipped, because we unsubscribed from foo
                let _: () = publish_conn.publish("foo", "goo").await?;
                let _: () = publish_conn.publish("zoomer", "foobar").await?;
                let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
                assert_eq!("foobar".to_string(), msg_payload);

                Ok::<_, RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[case::async_std(RuntimeType::AsyncStd)]
    fn split_manager_should_reconnect_after_disconnect(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let (mut sink, mut pubsub_stream) = ctx
                    .client
                    .get_async_pubsub_manager_with_config(
                        PubsubManagerConfig::new().set_max_delay(5),
                    )
                    .await?
                    .split();
                let _: () = sink.subscribe(&["phonewave", "foo", "bar"]).await?;
                let _: () = sink.psubscribe(&["zoom*"]).await?;
                let _: () = sink.unsubscribe("foo").await?;

                let addr = ctx.server.client_addr().clone();
                drop(ctx);
                // a yield, to let the pubsub to notice the broken connection.
                // this is required to reduce differences in test runs between async-std & tokio runtime.
                sleep(Duration::from_millis(1).into()).await;
                let ctx = TestContext::new_with_addr(addr);

                // wait until reconnected
                while sink.ping::<()>().await.is_err() {}

                let mut publish_conn = ctx.async_connection().await?;
                let _: () = publish_conn.publish("phonewave", "banana").await?;

                let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
                assert_eq!("banana".to_string(), msg_payload);

                // this should be skipped, because we unsubscribed from foo
                let _: () = publish_conn.publish("foo", "goo").await?;
                let _: () = publish_conn.publish("zoomer", "foobar").await?;
                let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
                assert_eq!("foobar".to_string(), msg_payload);

                Ok::<_, RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }

    #[rstest]
    #[case::tokio(RuntimeType::Tokio)]
    #[cfg_attr(feature = "async-std-comp", case::async_std(RuntimeType::AsyncStd))]
    fn manager_can_subscribe_after_reconnections(#[case] runtime: RuntimeType) {
        let ctx = TestContext::new();
        block_on_all(
            async move {
                let mut pubsub_conn = ctx
                    .client
                    .get_async_pubsub_manager_with_config(
                        PubsubManagerConfig::new().set_max_delay(5),
                    )
                    .await?;

                let addr = ctx.server.client_addr().clone();
                drop(ctx);
                let ctx = TestContext::new_with_addr(addr);

                while pubsub_conn
                    .subscribe(&["phonewave", "foo", "bar"])
                    .await
                    .is_err()
                {}
                let _: () = pubsub_conn.psubscribe(&["zoom*"]).await?;
                let _: () = pubsub_conn.unsubscribe("foo").await?;
                let mut pubsub_stream = pubsub_conn.on_message();
                let mut publish_conn = ctx.async_connection().await?;
                let _: () = publish_conn.publish("phonewave", "banana").await?;

                let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
                assert_eq!("banana".to_string(), msg_payload);

                // this should be skipped, because we unsubscribed from foo
                let _: () = publish_conn.publish("foo", "goo").await?;
                let _: () = publish_conn.publish("zoomer", "foobar").await?;
                let msg_payload: String = pubsub_stream.next().await.unwrap().get_payload()?;
                assert_eq!("foobar".to_string(), msg_payload);

                Ok::<_, RedisError>(())
            },
            runtime,
        )
        .unwrap();
    }
}
