#[macro_use]
extern crate criterion;
extern crate redis;

extern crate futures;
extern crate tokio;

#[path = "../tests/support/mod.rs"]
mod support;

use futures::{future, stream, Future, Stream};

use tokio::runtime::current_thread::Runtime;

use criterion::{Bencher, Benchmark, Criterion, Throughput};

use redis::{PipelineCommands, Value};

fn get_client() -> redis::Client {
    redis::Client::open("redis://127.0.0.1:6379").unwrap()
}

fn bench_simple_getsetdel(b: &mut Bencher) {
    let client = get_client();
    let con = client.get_connection().unwrap();

    b.iter(|| {
        let key = "test_key";
        redis::cmd("SET").arg(key).arg(42).execute(&con);
        let _: isize = redis::cmd("GET").arg(key).query(&con).unwrap();
        redis::cmd("DEL").arg(key).execute(&con);
    });
}

fn bench_simple_getsetdel_async(b: &mut Bencher) {
    let client = get_client();
    let mut runtime = Runtime::new().unwrap();
    let con = client.get_async_connection();
    let mut opt_con = Some(runtime.block_on(con).unwrap());

    b.iter(|| {
        let con = opt_con.take().expect("No connection");

        let key = "test_key";
        let future = redis::cmd("SET")
            .arg(key)
            .arg(42)
            .query_async(con)
            .and_then(|(con, ())| redis::cmd("GET").arg(key).query_async(con))
            .and_then(|(con, _): (_, isize)| redis::cmd("DEL").arg(key).query_async(con));
        let (con, ()) = runtime.block_on(future).unwrap();

        opt_con = Some(con);
    });
}

fn bench_simple_getsetdel_pipeline(b: &mut Bencher) {
    let client = get_client();
    let con = client.get_connection().unwrap();

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
            .query(&con)
            .unwrap();
    });
}

fn bench_simple_getsetdel_pipeline_precreated(b: &mut Bencher) {
    let client = get_client();
    let con = client.get_connection().unwrap();
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
        let _: (usize,) = pipe.query(&con).unwrap();
    });
}

const PIPELINE_QUERIES: usize = 1_000;

fn long_pipeline() -> redis::Pipeline {
    let mut pipe = redis::pipe();

    for i in 0..PIPELINE_QUERIES {
        pipe.set(format!("foo{}", i), "bar").ignore();
    }
    pipe
}

fn bench_long_pipeline(b: &mut Bencher) {
    let client = get_client();
    let con = client.get_connection().unwrap();

    let pipe = long_pipeline();

    b.iter(|| {
        let _: () = pipe.query(&con).unwrap();
    });
}

fn bench_async_long_pipeline(b: &mut Bencher) {
    let client = get_client();
    let mut runtime = Runtime::new().unwrap();
    let mut con = Some(runtime.block_on(client.get_async_connection()).unwrap());

    let pipe = long_pipeline();

    b.iter(|| {
        con = runtime
            .block_on(future::lazy(|| {
                pipe.clone()
                    .query_async(con.take().expect("Connection"))
                    .map(|(con, ())| Some(con))
            }))
            .unwrap();
    });
}

fn bench_shared_async_long_pipeline(b: &mut Bencher) {
    let client = get_client();
    let mut runtime = Runtime::new().unwrap();
    let con = runtime
        .block_on(client.get_shared_async_connection())
        .unwrap();

    let pipe = long_pipeline();

    b.iter(|| {
        let _: () = runtime
            .block_on(future::lazy(|| {
                pipe.clone().query_async(con.clone()).map(|(_, ())| ())
            }))
            .unwrap();
    });
}

fn bench_shared_async_implicit_pipeline(b: &mut Bencher) {
    let client = get_client();
    let mut runtime = Runtime::new().unwrap();
    let con = runtime
        .block_on(client.get_shared_async_connection())
        .unwrap();

    let cmds: Vec<_> = (0..PIPELINE_QUERIES)
        .map(|i| redis::cmd("SET").arg(format!("foo{}", i)).arg(i).clone())
        .collect();

    b.iter(|| {
        let _: () = runtime
            .block_on(future::lazy(|| {
                stream::futures_unordered(
                    cmds.iter().cloned().map(|cmd| cmd.query_async(con.clone())),
                )
                .for_each(|(_, ())| Ok(()))
            }))
            .unwrap();
    });
}

fn bench_query(c: &mut Criterion) {
    c.bench(
        "query",
        Benchmark::new("simple_getsetdel", bench_simple_getsetdel)
            .with_function("simple_getsetdel_async", bench_simple_getsetdel_async)
            .with_function("simple_getsetdel_pipeline", bench_simple_getsetdel_pipeline)
            .with_function(
                "simple_getsetdel_pipeline_precreated",
                bench_simple_getsetdel_pipeline_precreated,
            ),
    );
    c.bench(
        "query_pipeline",
        Benchmark::new(
            "shared_async_implicit_pipeline",
            bench_shared_async_implicit_pipeline,
        )
        .with_function(
            "shared_async_long_pipeline",
            bench_shared_async_long_pipeline,
        )
        .with_function("async_long_pipeline", bench_async_long_pipeline)
        .with_function("long_pipeline", bench_long_pipeline)
        .throughput(Throughput::Elements(PIPELINE_QUERIES as u32)),
    );
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
        pipe.get_packed_pipeline(false)
    });
}

fn bench_encode_pipeline(b: &mut Bencher) {
    b.iter(|| {
        let mut pipe = redis::pipe();

        for _ in 0..1_000 {
            pipe.set("foo", "bar").ignore();
        }
        pipe.get_packed_pipeline(false)
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
        pipe.get_packed_pipeline(false)
    });
}

fn bench_encode(c: &mut Criterion) {
    c.bench(
        "encode",
        Benchmark::new("pipeline", bench_encode_pipeline)
            .with_function("pipeline_nested", bench_encode_pipeline_nested)
            .with_function("integer", bench_encode_integer)
            .with_function("small", bench_encode_small),
    );
}

fn bench_decode_simple(b: &mut Bencher) {
    let value = Value::Bulk(vec![
        Value::Okay,
        Value::Status("testing".to_string()),
        Value::Bulk(vec![]),
        Value::Nil,
        Value::Data(vec![b'a'; 1000]),
        Value::Int(7512182390),
    ]);

    let mut input = Vec::new();
    support::encode_value(&value, &mut input).unwrap();
    b.iter(|| redis::parse_redis_value(&input).unwrap());
}
fn bench_decode(c: &mut Criterion) {
    c.bench("decode", Benchmark::new("decode", bench_decode_simple));
}

criterion_group!(bench, bench_query, bench_encode, bench_decode);
criterion_main!(bench);
