use futures::{prelude::*, stream};
use iai_callgrind::{
    library_benchmark, library_benchmark_group, main, Callgrind, FlamegraphConfig, FlamegraphKind,
    LibraryBenchmarkConfig,
};
use redis::{aio::MultiplexedConnection, RedisError, Value};
use std::hint::black_box;

use support::*;
use tokio::runtime::Runtime;

fn allocate_a_lot() {
    for i in 0..50000 {
        black_box(Vec::<usize>::with_capacity(1000 * i));
    }
}

#[path = "../tests/support/mod.rs"]
mod support;

type AsyncDependencies = (TestContext, Runtime, MultiplexedConnection);
type SyncDependencies = (TestContext, redis::Connection);

fn async_setup() -> AsyncDependencies {
    allocate_a_lot();
    let ctx = TestContext::new();
    let runtime = current_thread_runtime();
    let connection = runtime.block_on(ctx.async_connection()).unwrap();
    (ctx, runtime, connection)
}

fn sync_setup() -> SyncDependencies {
    allocate_a_lot();
    let ctx = TestContext::new();
    let connection = ctx.connection();
    (ctx, connection)
}

#[library_benchmark]
#[benches::with_setup(setup = sync_setup)]
fn bench_simple_getsetdel(tuple: SyncDependencies) {
    let (_ctx, mut con) = tuple;

    let key = "test_key";
    redis::cmd("SET").arg(key).arg(42).exec(&mut con).unwrap();
    let _: isize = redis::cmd("GET").arg(key).query(&mut con).unwrap();
    redis::cmd("DEL").arg(key).exec(&mut con).unwrap();
}

#[library_benchmark]
#[benches::with_setup(setup = async_setup)]
fn bench_simple_getsetdel_async(tuple: AsyncDependencies) {
    let (_ctx, runtime, mut con) = tuple;
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
        .unwrap()
}

#[library_benchmark]
#[benches::with_setup(setup = sync_setup)]
fn bench_simple_getsetdel_pipeline(tuple: SyncDependencies) {
    let (_ctx, mut con) = tuple;

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
}

#[library_benchmark]
#[benches::with_setup(setup = sync_setup)]
fn bench_simple_getsetdel_pipeline_precreated(tuple: SyncDependencies) {
    let (_ctx, mut con) = tuple;

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

    let _: (usize,) = pipe.query(&mut con).unwrap();
}

const PIPELINE_QUERIES: usize = 1_000;

fn long_pipeline() -> redis::Pipeline {
    let mut pipe = redis::pipe();

    for i in 0..PIPELINE_QUERIES {
        pipe.set(format!("foo{i}"), "bar").ignore();
    }
    pipe
}

#[library_benchmark]
#[benches::with_setup(setup = sync_setup)]
fn bench_long_pipeline(tuple: SyncDependencies) {
    let (_ctx, mut con) = tuple;

    let pipe = long_pipeline();

    pipe.exec(&mut con).unwrap();
}

#[library_benchmark]
#[benches::with_setup(setup = async_setup)]
fn bench_async_long_pipeline(tuple: AsyncDependencies) {
    let (_ctx, runtime, mut con) = tuple;

    let pipe = long_pipeline();

    runtime
        .block_on(async { pipe.exec_async(&mut con).await })
        .unwrap();
}

#[library_benchmark]
#[bench::with_setup(setup = async_setup)]
fn bench_multiplexed_async_implicit_pipeline(tuple: AsyncDependencies) {
    let (_ctx, runtime, con) = tuple;

    let cmds: Vec<_> = (0..PIPELINE_QUERIES)
        .map(|i| {
            let mut cmd = redis::cmd("SET");
            cmd.arg(format!("foo{i}")).arg(i);
            cmd
        })
        .collect();

    let mut connections = (0..PIPELINE_QUERIES)
        .map(|_| con.clone())
        .collect::<Vec<_>>();

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
}

#[library_benchmark]
#[bench::with_setup(setup = allocate_a_lot)]
fn bench_encode_small(_: ()) {
    let mut cmd = redis::cmd("HSETX");

    cmd.arg("ABC:1237897325302:878241asdyuxpioaswehqwu")
        .arg("some hash key")
        .arg(124757920);

    black_box(cmd.get_packed_command());
}

#[library_benchmark]
#[bench::with_setup(setup = allocate_a_lot)]
fn bench_encode_integer(_: ()) {
    let mut pipe = redis::pipe();

    for _ in 0..1_000 {
        pipe.set(123, 45679123).ignore();
    }
    black_box(pipe.get_packed_pipeline());
}

#[library_benchmark]
#[bench::with_setup(setup = allocate_a_lot)]
fn bench_encode_pipeline(_: ()) {
    let mut pipe = redis::pipe();

    for _ in 0..1_000 {
        pipe.set("foo", "bar").ignore();
    }
    black_box(pipe.get_packed_pipeline());
}

#[library_benchmark]
#[bench::with_setup(setup = allocate_a_lot)]
fn bench_encode_pipeline_nested(_: ()) {
    let mut pipe = redis::pipe();

    for _ in 0..200 {
        pipe.set(
            "foo",
            ("bar", 123, b"1231279712", &["test", "test", "test"][..]),
        )
        .ignore();
    }
    black_box(pipe.get_packed_pipeline());
}

#[library_benchmark]
#[bench::with_setup(setup = allocate_a_lot)]
fn bench_decode(_: ()) {
    let value = Value::Array(vec![
        Value::Okay,
        Value::SimpleString("testing".to_string()),
        Value::Array(vec![]),
        Value::Nil,
        Value::BulkString(vec![b'a'; 10]),
        Value::Int(7512182390),
    ]);

    let mut input = Vec::new();
    support::encode_value(&value, &mut input).unwrap();
    assert_eq!(redis::parse_redis_value(&input).unwrap(), value);
}

library_benchmark_group!(
    name = encode_decode;
    benchmarks = bench_decode,bench_encode_pipeline,bench_encode_pipeline_nested,bench_encode_integer,bench_encode_small
);

library_benchmark_group!(
    name = sync_benches;
    benchmarks = bench_simple_getsetdel,bench_simple_getsetdel_pipeline,bench_simple_getsetdel_pipeline_precreated,bench_long_pipeline
);

library_benchmark_group!(
    name = async_benches;
    benchmarks = bench_simple_getsetdel_async,bench_async_long_pipeline,bench_async_long_pipeline,bench_multiplexed_async_implicit_pipeline
);

main!(
    config = LibraryBenchmarkConfig::default()
        .env_clear(false)
        .tool(Callgrind::default()
            .flamegraph(FlamegraphConfig::default()
                .kind(FlamegraphKind::Differential)
            )
        );
    library_benchmark_groups = encode_decode,
    sync_benches,
    async_benches
);
