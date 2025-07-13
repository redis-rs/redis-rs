#![cfg(feature = "cluster-async")]
use futures_util::{stream, TryStreamExt};
use iai_callgrind::{library_benchmark, library_benchmark_group, main, LibraryBenchmarkConfig};
use redis::cluster_async::ClusterConnection;
use std::hint::black_box;

use redis_test::cluster::RedisClusterConfiguration;
use support::*;
use tokio::runtime::Runtime;

#[path = "../tests/support/mod.rs"]
mod support;

const PIPELINE_QUERIES: usize = 100;

async fn bench_set_get_and_del(con: &mut ClusterConnection) {
    let key = "test_key";

    redis::cmd("SET")
        .arg(key)
        .arg(42)
        .exec_async(con)
        .await
        .unwrap();

    black_box(
        redis::cmd("GET")
            .arg(key)
            .query_async::<isize>(con)
            .await
            .unwrap(),
    );

    redis::cmd("SET")
        .arg(key)
        .arg(42)
        .exec_async(con)
        .await
        .unwrap();
    redis::cmd("DEL").arg(key).exec_async(con).await.unwrap();
}

async fn bench_parallel_requests(con: &mut ClusterConnection) {
    let num_parallel = 100;
    let cmds: Vec<_> = (0..num_parallel)
        .map(|i| {
            let mut cmd = redis::cmd("SET");
            cmd.arg(format!("foo{i}")).arg(i);
            (cmd, con.clone())
        })
        .collect();

    cmds.into_iter()
        .map(|(cmd, mut con)| async move { cmd.exec_async(&mut con).await })
        .collect::<stream::FuturesUnordered<_>>()
        .try_for_each(|()| async { Ok(()) })
        .await
        .unwrap();
}

async fn bench_pipeline(con: &mut ClusterConnection) {
    let mut keys: Vec<_> = Vec::new();
    for i in 0..PIPELINE_QUERIES {
        keys.push(format!("{{foo}}{i}"));
    }

    let mut pipe = redis::pipe();
    for q in &keys {
        pipe.set(q, "bar").ignore();
    }
    pipe.exec_async(con).await.unwrap();

    let mut pipe = redis::pipe();
    for q in &keys {
        pipe.get(q).ignore();
    }
    pipe.exec_async(con).await.unwrap();
}

type Dependencies = (TestClusterContext, ClusterConnection, Runtime);

fn make_dependencies(cluster: TestClusterContext) -> Dependencies {
    cluster.wait_for_cluster_up();

    let runtime = current_thread_runtime();
    let connection = runtime.block_on(cluster.async_connection());
    (cluster, connection, runtime)
}

fn setup() -> Dependencies {
    make_dependencies(TestClusterContext::new())
}

fn setup_with_replicas() -> Dependencies {
    let cluster =
        TestClusterContext::new_with_config(RedisClusterConfiguration::single_replica_config());
    make_dependencies(cluster)
}

#[library_benchmark]
#[benches::with_setup(setup = setup)]
fn bench_cluster_set_get_and_del(tuple: Dependencies) {
    let (_cluster, mut connection, runtime) = tuple;
    runtime.block_on(bench_set_get_and_del(&mut connection));
}

#[library_benchmark]
#[benches::with_setup(setup = setup_with_replicas)]
fn bench_cluster_set_get_and_del_with_replicas(tuple: Dependencies) {
    let (_cluster, mut connection, runtime) = tuple;
    runtime.block_on(bench_set_get_and_del(&mut connection));
}

#[library_benchmark]
#[benches::with_setup(setup = setup)]
fn bench_cluster_parallel_requests(tuple: Dependencies) {
    let (_cluster, mut connection, runtime) = tuple;
    runtime.block_on(bench_parallel_requests(&mut connection));
}

#[library_benchmark]
#[benches::with_setup(setup = setup_with_replicas)]
fn bench_cluster_parallel_requests_with_replicas(tuple: Dependencies) {
    let (_cluster, mut connection, runtime) = tuple;
    runtime.block_on(bench_parallel_requests(&mut connection));
}

#[library_benchmark]
#[benches::with_setup(setup = setup)]
fn bench_cluster_pipeline(tuple: Dependencies) {
    let (_cluster, mut connection, runtime) = tuple;
    runtime.block_on(bench_pipeline(&mut connection));
}

#[library_benchmark]
#[benches::with_setup(setup = setup_with_replicas)]
fn bench_cluster_pipeline_with_replicas(tuple: Dependencies) {
    let (_cluster, mut connection, runtime) = tuple;
    runtime.block_on(bench_pipeline(&mut connection));
}

library_benchmark_group!(
    name = async_cluster_bench;
    benchmarks =bench_cluster_set_get_and_del,
    bench_cluster_set_get_and_del_with_replicas,
    bench_cluster_parallel_requests,
    bench_cluster_parallel_requests_with_replicas,
    bench_cluster_pipeline,
    bench_cluster_pipeline_with_replicas,
);

main!(
    config = LibraryBenchmarkConfig::default().env_clear(false);
    library_benchmark_groups = async_cluster_bench,
);
