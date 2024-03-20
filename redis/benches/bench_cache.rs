use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use rand::{Rng, SeedableRng};
use redis::caching::CacheConfig;
use redis::ConnectionConfigBuilder;

use support::*;

#[path = "../tests/support/mod.rs"]
mod support;

fn get_client() -> redis::Client {
    redis::Client::open("redis://127.0.0.1:6379/?resp3=true").unwrap()
}

struct BenchmarkConfig {
    cache_config: CacheConfig,
    per_round_command: usize,
    total_distinct_key: usize,
    read_ratio: f64,
}

fn bench_multiplexed_async(b: &mut Bencher, benchmark_config: BenchmarkConfig) {
    let client = get_client();
    let runtime = current_thread_runtime();
    let mut con = runtime
        .block_on(
            client.get_multiplexed_tokio_connection_with_config(
                &ConnectionConfigBuilder::new()
                    .cache_config(benchmark_config.cache_config)
                    .build(),
            ),
        )
        .unwrap();

    let mut small_rng = rand::rngs::SmallRng::from_entropy();
    b.iter(|| {
        runtime.block_on(async {
            for _ in 0..benchmark_config.per_round_command {
                let i: usize = small_rng.gen_range(0..benchmark_config.total_distinct_key);
                let mut cmd = redis::Cmd::new();
                if small_rng.gen_bool(benchmark_config.read_ratio) {
                    cmd.arg("GET").arg(format!("{i}"))
                } else {
                    cmd.arg("SET").arg(format!("{i}")).arg(i)
                };
                let _ = cmd.query_async::<_, ()>(&mut con).await.unwrap();
            }
        });
    });
}

fn bench_query(c: &mut Criterion) {
    let per_round_command = 1000;
    let benchmarks = [
        ("SMALL_READ_ONLY", 100, 1.0),
        ("SMALL_READ_MOSTLY", 100, 0.9),
        ("SMALL_RW_BALANCED", 100, 0.5),
        ("SMALL_WRITE_MOSTLY", 100, 0.1),
        ("SMALL_WRITE_ONLY", 100, 0.0),
        ("MEDIUM_READ_ONLY", 5_000, 1.0),
        ("MEDIUM_READ_MOSTLY", 5_000, 0.9),
        ("MEDIUM_RW_BALANCED", 5_000, 0.5),
        ("MEDIUM_WRITE_MOSTLY", 5_000, 0.1),
        ("MEDIUM_WRITE_ONLY", 5_000, 0.0),
        ("BIG_READ_ONLY", 10_000, 1.0),
        ("BIG_READ_MOSTLY", 10_000, 0.9),
        ("BIG_RW_BALANCED", 10_000, 0.5),
        ("BIG_WRITE_MOSTLY", 10_000, 0.1),
        ("BIG_WRITE_ONLY", 10_000, 0.0),
        ("HUGE_READ_ONLY", 50_000, 1.0),
        ("HUGE_READ_MOSTLY", 50_000, 0.9),
        ("HUGE_RW_BALANCED", 50_000, 0.5),
        ("HUGE_WRITE_MOSTLY", 50_000, 0.1),
        ("HUGE_WRITE_ONLY", 50_000, 0.0),
    ];
    for (name, total_distinct_key, read_ratio) in benchmarks {
        let mut group = c.benchmark_group(name);
        group
            .bench_function("without_cache", |b| {
                bench_multiplexed_async(
                    b,
                    BenchmarkConfig {
                        cache_config: CacheConfig::disabled(),
                        per_round_command,
                        total_distinct_key,
                        read_ratio,
                    },
                )
            })
            .bench_function("with_cache", |b| {
                bench_multiplexed_async(
                    b,
                    BenchmarkConfig {
                        cache_config: CacheConfig::enabled(),
                        per_round_command,
                        total_distinct_key,
                        read_ratio,
                    },
                )
            });
        group.finish();
    }
}
//, bench_encode, bench_decode
criterion_group!(cache, bench_query);
criterion_main!(cache);
