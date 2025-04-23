use criterion::{criterion_group, criterion_main, Bencher, Criterion, Throughput};
use redis::Cmd;

use support::*;

#[path = "../tests/support/mod.rs"]
mod support;

use rand::{
    distr::{Bernoulli, Distribution},
    Rng,
};
use redis::caching::CacheConfig;
use std::env;
fn generate_commands(key: String, total_command: u32, total_read: u32) -> Vec<Cmd> {
    let mut cmds = vec![];
    let mut rng = rand::rng();
    let distribution = Bernoulli::from_ratio(total_read, total_command).unwrap();
    for is_read in distribution
        .sample_iter(&mut rand::rng())
        .take(total_command as usize)
    {
        if is_read {
            cmds.push(redis::cmd("GET").arg(&key).clone());
        } else {
            cmds.push(
                redis::cmd("SET")
                    .arg(&key)
                    .arg(rng.random_range(1..1000))
                    .clone(),
            );
        }
    }
    cmds
}

async fn benchmark_executer(
    is_cache_enabled: bool,
    read_ratio: f32,
    per_key_command: u32,
    key_count: u32,
) {
    let ctx = TestContext::new();
    let con = if is_cache_enabled {
        ctx.async_connection_with_cache_config(CacheConfig::new())
            .await
            .unwrap()
    } else {
        ctx.multiplexed_async_connection_tokio().await.unwrap()
    };

    let mut rng = rand::rng();
    let mut handles = Vec::new();
    for _ in 0..key_count {
        let mut con = con.clone();
        let key = format!("{}", rng.random_range(1..1000));
        handles.push(tokio::spawn(async move {
            for cmd in generate_commands(
                key,
                per_key_command,
                (read_ratio * per_key_command as f32) as u32,
            ) {
                let _: () = cmd.query_async(&mut con).await.unwrap();
            }
        }));
    }
    for job_handle in handles {
        job_handle.await.unwrap();
    }
}

fn prepare_benchmark(
    bencher: &mut Bencher,
    thread_num: usize,
    is_cache_enabled: bool,
    read_ratio: f32,
    per_key_command: u32,
    key_count: u32,
) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(thread_num)
        .enable_all()
        .build()
        .unwrap();
    bencher.iter(|| {
        runtime.block_on(benchmark_executer(
            is_cache_enabled,
            read_ratio,
            per_key_command,
            key_count,
        ));
    });
}

fn bench_cache(c: &mut Criterion) {
    if env::var("PROTOCOL").unwrap_or_default() != "RESP3" {
        return;
    }
    let is_cache_enabled = env::var("ENABLE_CLIENT_SIDE_CACHE").unwrap_or_default() == "true";
    let test_cases: Vec<(f32, u32, u32)> = vec![
        (0.99, 100, 10_000),
        (0.90, 100, 10_000),
        (0.5, 100, 10_000),
        (0.1, 100, 10_000),
        (0.5, 5, 100_000),
        (0.5, 1, 100_000),
    ];
    for (read_ratio, per_key_command, total_key_count) in test_cases.clone() {
        let group_name = format!("{read_ratio}-{per_key_command}-{total_key_count}");
        let mut group = c.benchmark_group(group_name);
        group.throughput(Throughput::Elements(
            (per_key_command * total_key_count) as u64,
        ));
        group.sample_size(10);

        for thread_num in [1, 4, 16, 32] {
            group.bench_function(format!("thread-{thread_num}"), |bencher| {
                prepare_benchmark(
                    bencher,
                    thread_num,
                    is_cache_enabled,
                    read_ratio,
                    per_key_command,
                    total_key_count,
                )
            });
        }
        group.finish();
    }
}

criterion_group!(bench, bench_cache);
criterion_main!(bench);
