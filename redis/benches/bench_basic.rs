use criterion::{criterion_group, criterion_main, Bencher, Criterion, Throughput};
use futures::{prelude::*, stream};
use redis::{RedisError, Value};

use support::*;

#[path = "../tests/support/mod.rs"]
mod support;

fn bench_simple_getsetdel(b: &mut Bencher) {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    b.iter(|| {
        let key = "test_key";
        redis::cmd("SET").arg(key).arg(42).execute(&mut con);
        let _: isize = redis::cmd("GET").arg(key).query(&mut con).unwrap();
        redis::cmd("DEL").arg(key).execute(&mut con);
    });
}

fn bench_simple_getsetdel_async(b: &mut Bencher) {
    let ctx = TestContext::new();
    let runtime = current_thread_runtime();
    let mut con = runtime.block_on(ctx.async_connection()).unwrap();

    b.iter(|| {
        runtime
            .block_on(async {
                let key = "test_key";
                redis::cmd("SET")
                    .arg(key)
                    .arg(42)
                    .query_async(&mut con)
                    .await?;
                let _: isize = redis::cmd("GET").arg(key).query_async(&mut con).await?;
                redis::cmd("DEL").arg(key).query_async(&mut con).await?;
                Ok::<_, RedisError>(())
            })
            .unwrap()
    });
}

fn bench_simple_getsetdel_pipeline(b: &mut Bencher) {
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    b.iter(|| {
        let key = "test_key";
        let _: (usize,) = redis::pipe()
            .cmd("SET")
            .arg(key)
            .arg(42)
            .ignore()
            .cmd("GET")
            .arg(key)
            .cmd("DEL")
            .arg(key)
            .ignore()
            .query(&mut con)
            .unwrap();
    });
}

fn bench_simple_getsetdel_pipeline_precreated(b: &mut Bencher) {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let key = "test_key";
    let mut pipe = redis::pipe();
    pipe.cmd("SET")
        .arg(key)
        .arg(42)
        .ignore()
        .cmd("GET")
        .arg(key)
        .cmd("DEL")
        .arg(key)
        .ignore();

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
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let pipe = long_pipeline();

    b.iter(|| {
        pipe.query::<()>(&mut con).unwrap();
    });
}

fn bench_async_long_pipeline(b: &mut Bencher) {
    let ctx = TestContext::new();
    let runtime = current_thread_runtime();
    let mut con = runtime.block_on(ctx.async_connection()).unwrap();

    let pipe = long_pipeline();

    b.iter(|| {
        runtime
            .block_on(async { pipe.query_async::<_, ()>(&mut con).await })
            .unwrap();
    });
}

fn bench_multiplexed_async_long_pipeline(b: &mut Bencher) {
    let ctx = TestContext::new();
    let runtime = current_thread_runtime();
    let mut con = runtime
        .block_on(ctx.multiplexed_async_connection_tokio())
        .unwrap();

    let pipe = long_pipeline();

    b.iter(|| {
        runtime
            .block_on(async { pipe.query_async::<_, ()>(&mut con).await })
            .unwrap();
    });
}

fn bench_multiplexed_async_implicit_pipeline(b: &mut Bencher) {
    let ctx = TestContext::new();
    let runtime = current_thread_runtime();
    let con = runtime
        .block_on(ctx.multiplexed_async_connection_tokio())
        .unwrap();

    let cmds: Vec<_> = (0..PIPELINE_QUERIES)
        .map(|i| redis::cmd("SET").arg(format!("foo{i}")).arg(i).clone())
        .collect();

    let mut connections = (0..PIPELINE_QUERIES)
        .map(|_| con.clone())
        .collect::<Vec<_>>();

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

fn bench_encode_small(b: &mut Bencher) {
    b.iter(|| {
        let mut cmd = redis::cmd("HSETX");

        cmd.arg("ABC:1237897325302:878241asdyuxpioaswehqwu")
            .arg("some hash key")
            .arg(124757920);

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

fn bench_encode_pipeline(b: &mut Bencher) {
    b.iter(|| {
        let mut pipe = redis::pipe();

        for _ in 0..1_000 {
            pipe.set("foo", "bar").ignore();
        }
        pipe.get_packed_pipeline()
    });
}

fn bench_encode_pipeline_nested(b: &mut Bencher) {
    b.iter(|| {
        let mut pipe = redis::pipe();

        for _ in 0..200 {
            pipe.set(
                "foo",
                ("bar", 123, b"1231279712", &["test", "test", "test"][..]),
            )
            .ignore();
        }
        pipe.get_packed_pipeline()
    });
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");
    group
        .bench_function("pipeline", bench_encode_pipeline)
        .bench_function("pipeline_nested", bench_encode_pipeline_nested)
        .bench_function("integer", bench_encode_integer)
        .bench_function("small", bench_encode_small);
    group.finish();
}

fn bench_decode_simple(b: &mut Bencher, input: &[u8]) {
    b.iter(|| redis::parse_redis_value(input).unwrap());
}
fn bench_decode(c: &mut Criterion) {
    let value = Value::Bulk(vec![
        Value::Okay,
        Value::Status("testing".to_string()),
        Value::Bulk(vec![]),
        Value::Nil,
        Value::Data(vec![b'a'; 10]),
        Value::Int(7512182390),
    ]);

    let mut group = c.benchmark_group("decode");
    {
        let mut input = Vec::new();
        support::encode_value(&value, &mut input).unwrap();
        assert_eq!(redis::parse_redis_value(&input).unwrap(), value);
        group.bench_function("decode", move |b| bench_decode_simple(b, &input));
    }
    group.finish();
}

criterion_group!(bench, bench_query, bench_encode, bench_decode);
criterion_main!(bench);
