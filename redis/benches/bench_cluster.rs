#![allow(clippy::unit_arg)] // want to allow this for `black_box()`
#![cfg(feature = "cluster")]
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use futures::{prelude::*, stream};
use redis::cluster::cluster_pipe;
use redis::RedisError;

use support::*;

#[path = "../tests/support/mod.rs"]
mod support;

const PIPELINE_QUERIES: usize = 1_000;

fn bench_set_get_and_del(c: &mut Criterion, con: &mut redis::cluster::ClusterConnection) {
    let key = "test_key";

    let mut group = c.benchmark_group("cluster_basic");

    group.bench_function("set", |b| {
        b.iter(|| {
            redis::cmd("SET").arg(key).arg(42).execute(con);
            black_box(())
        })
    });

    group.bench_function("get", |b| {
        b.iter(|| black_box(redis::cmd("GET").arg(key).query::<isize>(con).unwrap()))
    });

    let mut set_and_del = || {
        redis::cmd("SET").arg(key).arg(42).execute(con);
        redis::cmd("DEL").arg(key).execute(con);
    };
    group.bench_function("set_and_del", |b| {
        b.iter(|| {
            set_and_del();
            black_box(())
        })
    });

    group.finish();
}

fn bench_pipeline(c: &mut Criterion, con: &mut redis::cluster::ClusterConnection) {
    let mut group = c.benchmark_group("cluster_pipeline");
    group.throughput(Throughput::Elements(PIPELINE_QUERIES as u64));

    let mut queries = Vec::new();
    for i in 0..PIPELINE_QUERIES {
        queries.push(format!("foo{}", i));
    }

    let build_pipeline = || {
        let mut pipe = cluster_pipe();
        for q in &queries {
            pipe.set(q, "bar").ignore();
        }
    };
    group.bench_function("build_pipeline", |b| {
        b.iter(|| {
            build_pipeline();
            black_box(())
        })
    });

    let mut pipe = cluster_pipe();
    for q in &queries {
        pipe.set(q, "bar").ignore();
    }
    group.bench_function("query_pipeline", |b| {
        b.iter(|| {
            pipe.query::<()>(con).unwrap();
            black_box(())
        })
    });

    group.finish();
}

fn bench_set_get_and_del_async(c: &mut Criterion, client: &mut redis::cluster::ClusterClient) {
    let key = "test_key";

    let mut group = c.benchmark_group("cluster_async_basic");
    let runtime = current_thread_runtime();
    let con = client.get_async_connection();
    let mut con = runtime.block_on(con).unwrap();

    group.bench_function("set", |b| {
        b.iter(|| {
            runtime
                .block_on(async {
                    redis::cmd("SET")
                        .arg(key)
                        .arg(42)
                        .query_async(&mut con)
                        .await?;
                    Ok::<(), RedisError>(())
                })
                .unwrap()
        })
    });

    group.bench_function("get", |b| {
        b.iter(|| {
            runtime
                .block_on(async {
                    redis::cmd("GET").arg(key).query_async(&mut con).await?;
                    Ok::<(), RedisError>(())
                })
                .unwrap()
        })
    });

    group.bench_function("set_and_del", |b| {
        b.iter(|| {
            runtime
                .block_on(async {
                    redis::cmd("SET")
                        .arg(key)
                        .arg(42)
                        .query_async(&mut con)
                        .await?;
                    redis::cmd("DEL").arg(key).query_async(&mut con).await?;
                    Ok::<(), RedisError>(())
                })
                .unwrap()
        })
    });

    group.finish();
}

fn bench_pipeline_async(c: &mut Criterion, client: &mut redis::cluster::ClusterClient) {
    let mut group = c.benchmark_group("cluster_async_pipeline");
    let runtime = current_thread_runtime();
    let con = client.get_async_connection();
    let mut con = runtime.block_on(con).unwrap();

    group.throughput(Throughput::Elements(PIPELINE_QUERIES as u64));

    let mut queries = Vec::new();
    for i in 0..PIPELINE_QUERIES {
        queries.push(format!("foo{}", i));
    }

    let build_pipeline = || {
        let mut pipe = cluster_pipe();
        for q in &queries {
            pipe.set(q, "bar").ignore();
        }
    };
    group.bench_function("build_pipeline", |b| {
        b.iter(|| {
            build_pipeline();
            black_box(())
        })
    });

    let mut pipe = cluster_pipe();
    for q in &queries {
        pipe.set(q, "bar").ignore();
    }
    group.bench_function("query_pipeline", |b| {
        b.iter(|| {
            runtime
                .block_on(async { pipe.query_async::<_, ()>(&mut con).await })
                .unwrap();
        })
    });

    let cmds: Vec<_> = (0..PIPELINE_QUERIES)
        .map(|i| redis::cmd("SET").arg(format!("foo{}", i)).arg(i).clone())
        .collect();

    let mut connections = (0..PIPELINE_QUERIES)
        .map(|_| con.clone())
        .collect::<Vec<_>>();

    group.bench_function("query_implicit_pipeline", |b| {
        b.iter(|| {
            runtime
                .block_on(async {
                    cmds.iter()
                        .zip(&mut connections)
                        .map(|(cmd, con)| cmd.query_async(con))
                        .collect::<stream::FuturesUnordered<_>>()
                        .try_for_each(|()| async { Ok(()) })
                        .await
                })
                .unwrap();
        })
    });

    group.finish();
}

fn bench_cluster_setup(c: &mut Criterion) {
    let cluster = TestClusterContext::new(6, 1);
    cluster.wait_for_cluster_up();

    let mut con = cluster.connection();
    bench_set_get_and_del(c, &mut con);
    bench_pipeline(c, &mut con);
}

fn bench_cluster_async_setup(c: &mut Criterion) {
    let mut cluster = TestClusterContext::new(6, 1);
    cluster.wait_for_cluster_up();

    bench_set_get_and_del_async(c, &mut cluster.client);
    bench_pipeline_async(c, &mut cluster.client);
}

#[allow(dead_code)]
fn bench_cluster_read_from_replicas_setup(c: &mut Criterion) {
    let cluster = TestClusterContext::new_with_cluster_client_builder(6, 1, |builder| {
        builder.read_from_replicas()
    });
    cluster.wait_for_cluster_up();

    let mut con = cluster.connection();
    bench_set_get_and_del(c, &mut con);
    bench_pipeline(c, &mut con);
}

#[allow(dead_code)]
fn bench_cluster_async_read_from_replicas_setup(c: &mut Criterion) {
    let mut cluster = TestClusterContext::new_with_cluster_client_builder(6, 1, |builder| {
        builder.read_from_replicas()
    });
    cluster.wait_for_cluster_up();

    bench_set_get_and_del_async(c, &mut cluster.client);
    bench_pipeline_async(c, &mut cluster.client);
}

criterion_group!(
    cluster_bench,
    bench_cluster_setup,
    // bench_cluster_read_from_replicas_setup,
    bench_cluster_async_setup,
    // bench_cluster_async_read_from_replicas_setup,
);
criterion_main!(cluster_bench);
