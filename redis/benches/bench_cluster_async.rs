#![allow(clippy::unit_arg)] // want to allow this for `black_box()`
#![cfg(feature = "cluster")]
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use futures_util::{stream, TryStreamExt};
use redis::RedisError;

use support::*;
use tokio::runtime::Runtime;

#[path = "../tests/support/mod.rs"]
mod support;

fn bench_cluster_async(
    c: &mut Criterion,
    con: &mut redis::cluster_async::ClusterConnection,
    runtime: &Runtime,
) {
    let mut group = c.benchmark_group("cluster_async");
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
                .unwrap();
            black_box(())
        })
    });

    group.bench_function("parallel_requests", |b| {
        let num_parallel = 100;
        let cmds: Vec<_> = (0..num_parallel)
            .map(|i| redis::cmd("SET").arg(format!("foo{i}")).arg(i).clone())
            .collect();

        let mut connections = (0..num_parallel).map(|_| con.clone()).collect::<Vec<_>>();

        b.iter(|| {
            runtime
                .block_on(async {
                    cmds.iter()
                        .zip(&mut connections)
                        .map(|(cmd, con)| cmd.query_async::<_, ()>(con))
                        .collect::<stream::FuturesUnordered<_>>()
                        .try_for_each(|()| async { Ok(()) })
                        .await
                })
                .unwrap();
            black_box(())
        });
    });

    group.bench_function("pipeline", |b| {
        let num_queries = 100;

        let mut pipe = redis::pipe();

        for _ in 0..num_queries {
            pipe.set("foo".to_string(), "bar").ignore();
        }

        b.iter(|| {
            runtime
                .block_on(async { pipe.query_async::<_, ()>(con).await })
                .unwrap();
            black_box(())
        });
    });

    group.finish();
}

fn bench_cluster_setup(c: &mut Criterion) {
    let cluster = TestClusterContext::new(6, 1);
    cluster.wait_for_cluster_up();
    let runtime = current_thread_runtime();
    let mut con = runtime.block_on(cluster.async_connection());

    bench_cluster_async(c, &mut con, &runtime);
}

criterion_group!(cluster_async_bench, bench_cluster_setup,);
criterion_main!(cluster_async_bench);
