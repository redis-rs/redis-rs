use criterion::{Bencher, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::{prelude::*, stream};
use redis::{RedisError, Value};
use redis_test::TestContext;

use support::*;

#[path = "../tests/support/mod.rs"]
mod support;

fn bench_simple_getsetdel(b: &mut Bencher) {
    let ctx = TestContext::default();
    let mut con = ctx.connection();

    b.iter(|| {
        let key = "test_key";
        redis::cmd("SET").arg(key).arg(42).exec(&mut con).unwrap();
        let _: isize = redis::cmd("GET").arg(key).query(&mut con).unwrap();
        redis::cmd("DEL").arg(key).exec(&mut con).unwrap();
    });
}

fn bench_simple_getsetdel_async(b: &mut Bencher) {
    let ctx = TestContext::default();
    let runtime = current_thread_runtime();
    let mut con = runtime.block_on(ctx.async_connection()).unwrap();

    b.iter(|| {
        runtime
            .block_on(async {
                let key = "test_key";
                redis::cmd("SET")
                    .arg(key)
                    .arg(42)
                    .exec_async(&mut con)
                    .await?;
                let _: isize = redis::cmd("GET").arg(key).query_async(&mut con).await?;
                redis::cmd("DEL").arg(key).exec_async(&mut con).await?;
                Ok::<_, RedisError>(())
            })
            .unwrap();
    });
}

fn bench_simple_getsetdel_pipeline(b: &mut Bencher) {
    let ctx = TestContext::default();
    let mut con = ctx.connection();

    b.iter(|| {
        let key = "test_key";
        let _: (usize,) = redis::pipe()
            .set(key, 42)
            .ignore()
            .get(key)
            .del(key)
            .ignore()
            .query(&mut con)
            .unwrap();
    });
}

fn bench_simple_getsetdel_pipeline_precreated(b: &mut Bencher) {
    let ctx = TestContext::default();
    let mut con = ctx.connection();
    let key = "test_key";
    let mut pipe = redis::pipe();
    pipe.set(key, 42).ignore().get(key).del(key).ignore();

    b.iter(|| {
        let _: (usize,) = pipe.query(&mut con).unwrap();
    });
}

const PIPELINE_QUERIES: usize = 1_000;

fn long_pipeline() -> redis::Pipeline {
    let mut pipe = redis::pipe();

    for i in 0..PIPELINE_QUERIES {
        pipe.set(format!("foo{i}"), "bar").ignore();
    }
    pipe
}

fn bench_long_pipeline(b: &mut Bencher) {
    let ctx = TestContext::default();
    let mut con = ctx.connection();

    let pipe = long_pipeline();

    b.iter(|| {
        pipe.exec(&mut con).unwrap();
    });
}

fn bench_async_long_pipeline(b: &mut Bencher) {
    let ctx = TestContext::default();
    let runtime = current_thread_runtime();
    let mut con = runtime.block_on(ctx.async_connection()).unwrap();

    let pipe = long_pipeline();

    b.iter(|| {
        runtime
            .block_on(async { pipe.exec_async(&mut con).await })
            .unwrap();
    });
}

fn bench_multiplexed_async_long_pipeline(b: &mut Bencher) {
    let ctx = TestContext::default();
    let runtime = current_thread_runtime();
    let mut con = runtime
        .block_on(ctx.multiplexed_async_connection_tokio())
        .unwrap();

    let pipe = long_pipeline();

    b.iter(|| {
        runtime
            .block_on(async { pipe.exec_async(&mut con).await })
            .unwrap();
    });
}

fn bench_multiplexed_async_implicit_pipeline(b: &mut Bencher) {
    let ctx = TestContext::default();
    let runtime = current_thread_runtime();
    let con = runtime
        .block_on(ctx.multiplexed_async_connection_tokio())
        .unwrap();

    let cmds: Vec<_> = (0..PIPELINE_QUERIES)
        .map(|i| redis::cmd("SET").arg(format!("foo{i}")).arg(i).clone())
        .collect();

    let mut connections = std::iter::repeat_with(|| con.clone())
        .take(PIPELINE_QUERIES)
        .collect::<Vec<_>>();

    b.iter(|| {
        runtime
            .block_on(async {
                cmds.iter()
                    .zip(&mut connections)
                    .map(|(cmd, con)| cmd.exec_async(con))
                    .collect::<stream::FuturesUnordered<_>>()
                    .try_for_each(|()| async { Ok(()) })
                    .await
            })
            .unwrap();
    });
}

fn bench_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("query");
    group
        .bench_function("simple_getsetdel", bench_simple_getsetdel)
        .bench_function("simple_getsetdel_async", bench_simple_getsetdel_async)
        .bench_function("simple_getsetdel_pipeline", bench_simple_getsetdel_pipeline)
        .bench_function(
            "simple_getsetdel_pipeline_precreated",
            bench_simple_getsetdel_pipeline_precreated,
        );
    group.finish();

    let mut group = c.benchmark_group("query_pipeline");
    group
        .bench_function(
            "multiplexed_async_implicit_pipeline",
            bench_multiplexed_async_implicit_pipeline,
        )
        .bench_function(
            "multiplexed_async_long_pipeline",
            bench_multiplexed_async_long_pipeline,
        )
        .bench_function("async_long_pipeline", bench_async_long_pipeline)
        .bench_function("long_pipeline", bench_long_pipeline)
        .throughput(Throughput::Elements(PIPELINE_QUERIES as u64));
    group.finish();
}

// Payload sizes for the small / medium / large data variants: low tens of bytes, hundreds of
// bytes, and several kilobytes respectively.
const PAYLOAD_SIZES: &[(&str, usize)] = &[("small", 16), ("medium", 256), ("large", 4096)];

// Deterministic filler bytes so every run encodes identical input.
fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| b'a' + (i % 26) as u8).collect()
}

// Encode a single command whose value carries the given payload.
fn bench_encode_small(b: &mut Bencher, value: &[u8]) {
    b.iter(|| {
        let mut cmd = redis::cmd("HSETX");

        cmd.arg("ABC:1237897325302:878241asdyuxpioaswehqwu")
            .arg("some hash key")
            .arg(value);

        cmd.get_packed_command()
    });
}

fn bench_encode_integer(b: &mut Bencher) {
    b.iter(|| {
        let mut pipe = redis::pipe();

        for _ in 0..1_000 {
            pipe.set(123, 45679123).ignore();
        }
        pipe.get_packed_pipeline()
    });
}

fn bench_encode_set_ex(b: &mut Bencher, value: &[u8]) {
    // `SET key val EX <secs>` — exercises the `SetExpiry` option encoder.
    b.iter(|| {
        let mut pipe = redis::pipe();

        for _ in 0..1_000 {
            pipe.set_ex("session:abc123", value, 3600).ignore();
        }
        pipe.get_packed_pipeline()
    });
}

fn bench_encode_pipeline(b: &mut Bencher, value: &[u8]) {
    b.iter(|| {
        let mut pipe = redis::pipe();

        for _ in 0..1_000 {
            pipe.set("foo", value).ignore();
        }
        pipe.get_packed_pipeline()
    });
}

fn bench_encode_pipeline_nested(b: &mut Bencher, value: &[u8]) {
    b.iter(|| {
        let mut pipe = redis::pipe();

        for _ in 0..200 {
            pipe.mset(&[
                ("foo1", value),
                ("foo2", value),
                ("foo3", value),
                ("foo4", value),
            ])
            .ignore();
        }
        pipe.get_packed_pipeline()
    });
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    // Integer args are fixed-width and size-independent, so they stay a single control case.
    group.bench_function("integer", bench_encode_integer);

    for &(name, len) in PAYLOAD_SIZES {
        let value = payload(len);

        group.bench_function(BenchmarkId::new("pipeline", name), |b| {
            bench_encode_pipeline(b, &value);
        });
        group.bench_function(BenchmarkId::new("pipeline_nested", name), |b| {
            bench_encode_pipeline_nested(b, &value);
        });
        group.bench_function(BenchmarkId::new("hsetx", name), |b| {
            bench_encode_small(b, &value);
        });
        group.bench_function(BenchmarkId::new("set_ex", name), |b| {
            bench_encode_set_ex(b, &value);
        });
    }
    group.finish();
}

fn bench_decode_simple(b: &mut Bencher, input: &[u8]) {
    b.iter(|| redis::parse_redis_value(input).unwrap());
}
fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");

    for &(name, len) in PAYLOAD_SIZES {
        let value = Value::Array(vec![
            Value::Okay,
            Value::SimpleString("testing".to_string()),
            Value::Array(vec![]),
            Value::Nil,
            Value::BulkString(vec![b'a'; len]),
            Value::Int(7512182390),
        ]);

        let mut input = Vec::new();
        support::encode_value(&value, &mut input).unwrap();
        assert_eq!(redis::parse_redis_value(&input).unwrap(), value);
        group.bench_function(name, move |b| bench_decode_simple(b, &input));
    }
    group.finish();
}

criterion_group!(bench, bench_query, bench_encode, bench_decode);
criterion_main!(bench);
