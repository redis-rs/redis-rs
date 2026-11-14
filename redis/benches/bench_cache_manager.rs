//! Client-side cache microbenchmarks. No server required.
//!
//! These drive the cache through `CacheBenchHarness`, which takes the same path as
//! `MultiplexedConnection` (look up, pack the request on a miss, resolve the reply into the
//! cache) but answers with canned replies, so they measure the cache's own overhead.
//! `bench_cache` measures the end-to-end effect against a real server.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use redis::caching::{CacheBenchHarness, CacheConfig};
use redis::{Cmd, Value};

fn harness() -> CacheBenchHarness {
    CacheBenchHarness::new(CacheConfig::new())
}

fn keys(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("key:{i}")).collect()
}

fn multi_key_cmd(name: &str, keys: &[String]) -> Cmd {
    let mut cmd = redis::cmd(name);
    for key in keys {
        cmd.arg(key);
    }
    cmd
}

fn mget_reply(count: usize) -> Value {
    Value::Array(
        (0..count)
            .map(|i| Value::BulkString(format!("value:{i}").into_bytes()))
            .collect(),
    )
}

fn as_bytes(keys: &[String]) -> Vec<&[u8]> {
    keys.iter().map(String::as_bytes).collect()
}

fn bench_single_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache/single_key");

    for (label, size) in [("16B", 16), ("16KiB", 16 * 1024)] {
        let value = Value::BulkString(vec![b'x'; size]);
        let get = redis::cmd("GET").arg("key").take();

        let cache = harness();
        cache.query(&get, &value).unwrap();
        group.bench_function(format!("get_hit/{label}"), |b| {
            b.iter(|| black_box(cache.query(black_box(&get), &value).unwrap()));
        });

        let cache = harness();
        group.bench_function(format!("get_miss/{label}"), |b| {
            b.iter(|| {
                cache.invalidate(&[b"key"]);
                black_box(cache.query(black_box(&get), &value).unwrap())
            });
        });
    }

    // A hit among a full cache, to catch lookups that degrade with the number of entries.
    let cache = harness();
    let filler = Value::Int(1);
    for key in keys(10_000) {
        cache
            .query(&redis::cmd("GET").arg(&key).take(), &filler)
            .unwrap();
    }
    let get = redis::cmd("GET").arg("key:5000").take();
    group.bench_function("get_hit/full_cache", |b| {
        b.iter(|| black_box(cache.query(black_box(&get), &filler).unwrap()));
    });

    // Commands with arguments past the key are matched on every argument.
    let hget = redis::cmd("HGET").arg("hash").arg("field").take();
    let value = Value::BulkString(b"value".to_vec());
    let cache = harness();
    cache.query(&hget, &value).unwrap();
    group.bench_function("hget_hit", |b| {
        b.iter(|| black_box(cache.query(black_box(&hget), &value).unwrap()));
    });

    // The overhead the cache adds to commands it can't cache.
    let set = redis::cmd("SET").arg("key").arg("value").take();
    let cache = harness();
    group.bench_function("set_not_cacheable", |b| {
        b.iter(|| black_box(cache.query(black_box(&set), &Value::Okay).unwrap()));
    });

    group.finish();
}

fn bench_multi_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache/multi_key");

    for count in [10, 100] {
        let keys = keys(count);
        let key_bytes = as_bytes(&keys);
        group.throughput(Throughput::Elements(count as u64));

        // MGET is split into one cache entry per key.
        let mget = multi_key_cmd("MGET", &keys);
        let reply = mget_reply(count);
        let cache = harness();
        cache.query(&mget, &reply).unwrap();
        group.bench_function(format!("mget_hit/{count}"), |b| {
            b.iter(|| black_box(cache.query(black_box(&mget), &reply).unwrap()));
        });

        let cache = harness();
        group.bench_function(format!("mget_miss/{count}"), |b| {
            b.iter(|| {
                cache.invalidate(&key_bytes);
                black_box(cache.query(black_box(&mget), &reply).unwrap())
            });
        });

        // Commands whose single reply depends on all of their keys.
        let sinter = multi_key_cmd("SINTER", &keys);
        let reply = Value::Array(vec![Value::BulkString(b"member".to_vec())]);
        let cache = harness();
        cache.query(&sinter, &reply).unwrap();
        group.bench_function(format!("sinter_hit/{count}"), |b| {
            b.iter(|| black_box(cache.query(black_box(&sinter), &reply).unwrap()));
        });

        let cache = harness();
        group.bench_function(format!("sinter_miss/{count}"), |b| {
            b.iter(|| {
                cache.invalidate(&key_bytes);
                black_box(cache.query(black_box(&sinter), &reply).unwrap())
            });
        });
    }

    group.finish();
}

fn bench_invalidation(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache/invalidation");

    // One key that holds the replies of many commands.
    for entries in [1, 100] {
        let cmds = (0..entries)
            .map(|i| redis::cmd("HGET").arg("hash").arg(i).take())
            .collect::<Vec<_>>();
        let value = Value::Int(1);
        let cache = harness();
        group.bench_function(format!("key/{entries}_entries"), |b| {
            b.iter_batched(
                || {
                    for cmd in &cmds {
                        cache.query(cmd, &value).unwrap();
                    }
                },
                |()| cache.invalidate(black_box(&[b"hash"])),
                BatchSize::PerIteration,
            );
        });
    }

    let cmds = keys(1_000)
        .iter()
        .map(|key| redis::cmd("GET").arg(key).take())
        .collect::<Vec<_>>();
    let value = Value::Int(1);
    let cache = harness();
    group.bench_function("all/1000_keys", |b| {
        b.iter_batched(
            || {
                for cmd in &cmds {
                    cache.query(cmd, &value).unwrap();
                }
            },
            |()| cache.invalidate_all(),
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

/// Hits from several threads at once, to show contention on the cache's locks.
fn bench_concurrent_hits(c: &mut Criterion) {
    const QUERIES_PER_THREAD: usize = 10_000;
    let mut group = c.benchmark_group("cache/concurrent_get_hit");

    let cmds = keys(1_000)
        .iter()
        .map(|key| redis::cmd("GET").arg(key).take())
        .collect::<Vec<_>>();
    let value = Value::Int(1);
    let cache = harness();
    for cmd in &cmds {
        cache.query(cmd, &value).unwrap();
    }

    for threads in [1, 4, 8] {
        group.throughput(Throughput::Elements((threads * QUERIES_PER_THREAD) as u64));
        group.bench_function(format!("threads/{threads}"), |b| {
            b.iter(|| {
                std::thread::scope(|scope| {
                    for thread in 0..threads {
                        let (cache, cmds, value) = (&cache, &cmds, &value);
                        scope.spawn(move || {
                            for i in 0..QUERIES_PER_THREAD {
                                let cmd = &cmds[(thread * 131 + i) % cmds.len()];
                                black_box(cache.query(cmd, value).unwrap());
                            }
                        });
                    }
                });
            });
        });
    }

    group.finish();
}

criterion_group!(
    bench,
    bench_single_key,
    bench_multi_key,
    bench_invalidation,
    bench_concurrent_hits
);
criterion_main!(bench);
