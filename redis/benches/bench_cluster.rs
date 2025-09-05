#![cfg(feature = "cluster")]
use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main, LibraryBenchmarkConfig};
use redis::cluster::{cluster_pipe, ClusterConnection};
use redis_test::cluster::RedisClusterConfiguration;

use support::*;

#[path = "../tests/support/mod.rs"]
mod support;

const PIPELINE_QUERIES: usize = 100;

fn bench_set_get_and_del(con: &mut ClusterConnection) {
    let key = "test_key";

    redis::cmd("SET").arg(key).arg(42).exec(con).unwrap();

    black_box(redis::cmd("GET").arg(key).query::<isize>(con).unwrap());

    redis::cmd("SET").arg(key).arg(42).exec(con).unwrap();
    redis::cmd("DEL").arg(key).exec(con).unwrap();
}

fn bench_pipeline(con: &mut ClusterConnection) {
    let mut keys = Vec::new();
    for i in 0..PIPELINE_QUERIES {
        keys.push(format!("foo{i}"));
    }

    let mut pipe = cluster_pipe();
    for q in &keys {
        pipe.set(q, "bar").ignore();
    }
    pipe.exec(con).unwrap();

    let mut pipe = cluster_pipe();
    for q in &keys {
        pipe.get(q).ignore();
    }
    pipe.exec(con).unwrap();
}

type Dependencies = (TestClusterContext, ClusterConnection);

fn setup() -> Dependencies {
    let cluster = TestClusterContext::new();
    cluster.wait_for_cluster_up();
    let connection = cluster.connection();
    (cluster, connection)
}

fn setup_with_replicas() -> Dependencies {
    let cluster =
        TestClusterContext::new_with_config(RedisClusterConfiguration::single_replica_config());
    cluster.wait_for_cluster_up();
    let connection = cluster.connection();
    (cluster, connection)
}

#[library_benchmark]
#[benches::with_setup(setup = setup)]
fn bench_cluster_set_get_and_del(tuple: Dependencies) {
    let (_cluster, mut connection) = tuple;
    bench_set_get_and_del(&mut connection);
}

#[library_benchmark]
#[benches::with_setup(setup = setup_with_replicas)]
fn bench_cluster_set_get_and_del_with_replicas(tuple: Dependencies) {
    let (_cluster, mut connection) = tuple;
    bench_set_get_and_del(&mut connection);
}

#[library_benchmark]
#[benches::with_setup(setup = setup)]
fn bench_cluster_pipeline(tuple: Dependencies) {
    let (_cluster, mut connection) = tuple;
    bench_pipeline(&mut connection);
}

#[library_benchmark]
#[benches::with_setup(setup = setup_with_replicas)]
fn bench_cluster_pipeline_with_replicas(tuple: Dependencies) {
    let (_cluster, mut connection) = tuple;
    bench_pipeline(&mut connection);
}

library_benchmark_group!(
    name = cluster_bench;
    benchmarks =bench_cluster_set_get_and_del,
    bench_cluster_set_get_and_del_with_replicas,
    bench_cluster_pipeline,
    bench_cluster_pipeline_with_replicas,
);

main!(
    config = LibraryBenchmarkConfig::default().env_clear(false);
    library_benchmark_groups = cluster_bench,
);
