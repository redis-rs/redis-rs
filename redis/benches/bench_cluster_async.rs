#![allow(clippy::unit_arg)] // want to allow this for `black_box()`
#![cfg(feature = "cluster")]
use criterion::{criterion_group, criterion_main, Criterion};
use redis::RedisError;

use support::*;
use tokio::runtime::Runtime;

#[path = "../tests/support/mod.rs"]
mod support;

fn bench_basic(
    c: &mut Criterion,
    con: &mut redis::cluster_async::ClusterConnection,
    runtime: &Runtime,
) {
    let mut group = c.benchmark_group("cluster_async_basic");
    group.sample_size(1_000);
    group.measurement_time(std::time::Duration::from_secs(30));
    group.bench_function("set_get_and_del", |b| {
        b.iter(|| {
            runtime
                .block_on(async {
                    let key = "test_key";
                    redis::cmd("SET").arg(key).arg(42).query_async(con).await?;
                    let _: isize = redis::cmd("GET").arg(key).query_async(con).await?;
                    redis::cmd("DEL").arg(key).query_async(con).await?;

                    Ok::<_, RedisError>(())
                })
                .unwrap()
        })
    });
    group.finish();
}

fn bench_cluster_setup(c: &mut Criterion) {
    let cluster = TestClusterContext::new(6, 1);
    cluster.wait_for_cluster_up();
    let runtime = current_thread_runtime();
    let mut con = runtime.block_on(cluster.async_connection());

    bench_basic(c, &mut con, &runtime);
}

criterion_group!(cluster_async_bench, bench_cluster_setup,);
criterion_main!(cluster_async_bench);
