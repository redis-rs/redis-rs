use std::time::Duration;
use futures::{prelude::*};
use tokio::time::timeout;

use redis::{
    AsyncCommands,
};

use crate::support::*;

mod support;

#[test]
fn iter_async_hscan_match() {
    let ctx = TestContext::new();
    let connect = ctx.multiplexed_async_connection();

    block_on_all(connect.and_then(|mut con| async move {
        let max = 100_000;
        for i in 0..max {
            con.hset::<_, _, _, i32>("map1", format!("k_{}", i), i).await.unwrap();
        }

        let now = std::time::Instant::now();
        let mut iter = con.hscan_match::<_, _, (String, i32)>("map1", "kk*").await.unwrap();
        while let Ok(Some((_, _v))) = timeout( Duration::from_millis(500), iter.next_item()).await {
        }
        println!("cost time: {:?}", now.elapsed());
        assert!(now.elapsed().as_millis() < 500);

        Ok(())
    })).unwrap();
}


#[test]
fn iter_async_sscan_match() {
    let ctx = TestContext::new();
    let connect = ctx.multiplexed_async_connection();

    block_on_all(connect.and_then(|mut con| async move {
        let max = 100_000;
        for i in 0..max {
            con.sadd::<_, String, usize>("set1", format!("k_{}", i)).await.unwrap();
        }

        let now = std::time::Instant::now();
        let mut iter = con.sscan_match::<_, _, String>("set1", "kk*").await.unwrap();
        while let Ok(Some(_v)) = timeout( Duration::from_millis(500), iter.next_item()).await {

        }
        println!("cost time: {:?}", now.elapsed());
        assert!(now.elapsed().as_millis() < 500);

        Ok(())
    })).unwrap();
}