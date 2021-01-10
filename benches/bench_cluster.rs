#![cfg(feature = "cluster")]
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use support::*;

#[path = "../tests/support/mod.rs"]
mod support;

fn bench_set_get_and_del(c: &mut Criterion) {
    let cluster = TestClusterContext::new(6, 1);
    cluster.wait_for_cluster_up();
    let mut con = cluster.connection();
    let key = "test_key";

    let mut group = c.benchmark_group("cluster_basic");

    group.bench_function("set", |b| {
        b.iter(|| black_box(redis::cmd("SET").arg(key).arg(42).execute(&mut con)))
    });

    group.bench_function("get", |b| {
        b.iter(|| black_box(redis::cmd("GET").arg(key).query::<isize>(&mut con).unwrap()))
    });

    let mut set_and_del = || {
        redis::cmd("SET").arg(key).arg(42).execute(&mut con);
        redis::cmd("DEL").arg(key).execute(&mut con);
    };
    group.bench_function("set_and_del", |b| b.iter(|| black_box(set_and_del())));

    group.finish();
}

criterion_group!(cluster_bench, bench_set_get_and_del);
criterion_main!(cluster_bench);
