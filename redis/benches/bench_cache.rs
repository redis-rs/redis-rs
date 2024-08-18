use criterion::{criterion_group, criterion_main, Bencher, Criterion, Throughput};
use redis::Cmd;

use support::*;

#[path = "../tests/support/mod.rs"]
mod support;

use rand::{
    distributions::{Bernoulli, Distribution},
    Rng,
};
use redis::caching::CacheConfig;
fn generate_commands(key: String, total_command: u32, total_read: u32) -> Vec<Cmd> {
    let mut cmds = vec![];
    let mut rng = rand::thread_rng();
    let d = Bernoulli::from_ratio(total_read, total_command).unwrap();
    for is_read in d
        .sample_iter(&mut rand::thread_rng())
        .take(total_command as usize)
    {
        if is_read {
            cmds.push(redis::cmd("GET").arg(&key).clone());
        } else {
            cmds.push(
                redis::cmd("SET")
                    .arg(&key)
                    .arg(&mut rng.gen_range(1..1000))
                    .clone(),
            );
        }
    }
    cmds
}

async fn benchmark_executer(
    is_cache_enabled: bool,
    r_ratio: f32,
    per_key_command: u32,
    key_count: u32,
) {
    let ctx = TestContext::new();
    let con = if is_cache_enabled {
        ctx.multiplexed_async_connection_tokio_with_cache_config(CacheConfig::enabled())
            .await
            .unwrap()
    } else {
        ctx.multiplexed_async_connection_tokio().await.unwrap()
    };

    let mut rng = rand::thread_rng();
    let mut handles = Vec::new();
    for _ in 0..key_count {
        let mut con = con.clone();
        let key = format!("{}", rng.gen_range(1..1000));
        handles.push(tokio::spawn(async move {
            for cmd in generate_commands(
                key,
                per_key_command,
                (r_ratio * per_key_command as f32) as u32,
            ) {
                let _: () = cmd.query_async(&mut con).await.unwrap();
            }
        }));
    }
    for jh in handles {
        jh.await.unwrap();
    }
}

fn prepare_benchmark(
    b: &mut Bencher,
    t_num: usize,
    is_cache_enabled: bool,
    r_ratio: f32,
    per_key_command: u32,
    key_count: u32,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(t_num)
        .enable_all()
        .build()
        .unwrap();
    b.iter(|| {
        rt.block_on(benchmark_executer(
            is_cache_enabled,
            r_ratio,
            per_key_command,
            key_count,
        ));
    });
}

fn bench_cache(c: &mut Criterion) {
    for tnum in vec![1, 4, 16, 32] {
        let test_cases: Vec<(f32, u32, u32)> = vec![
            (0.99, 100, 10_000),
            (0.90, 100, 10_000),
            (0.5, 100, 10_000),
            (0.1, 100, 10_000),
            (0.5, 5, 100_000),
            (0.5, 1, 100_000),
        ];
        for (read_ratio, per_key_command, total_key_count) in test_cases.clone() {
            let group_name = format!("{tnum}-{read_ratio}-{per_key_command}-{total_key_count}");
            let mut group = c.benchmark_group(group_name);
            group.throughput(Throughput::Elements(
                (per_key_command * total_key_count) as u64,
            ));
            group.sample_size(10);

            for is_cache_enabled in vec![true, false] {
                group.bench_function(format!("cache={is_cache_enabled}"), |b| {
                    prepare_benchmark(
                        b,
                        tnum,
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
}

criterion_group!(bench, bench_cache);
criterion_main!(bench);
