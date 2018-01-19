extern crate redis;
#[macro_use]
extern crate bencher;

extern crate futures;
extern crate tokio_core;

use futures::Future;
use tokio_core::reactor::Core;

use bencher::Bencher;

use redis::PipelineCommands;

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
    let mut core = Core::new().unwrap();
    let con = client.get_async_connection(&core.handle());
    let mut opt_con = Some(core.run(con).unwrap());

    b.iter(|| {
        let con = opt_con.take().expect("No connection");

        let key = "test_key";
        let future = redis::cmd("SET").arg(key).arg(42).query_async(con).and_then(|(con, ())| {
            redis::cmd("GET").arg(key).query_async(con)
        }).and_then(|(con, _): (_, isize)| {
            redis::cmd("DEL").arg(key).query_async(con)
        });
        let (con, ()) = core.run(future).unwrap();

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

fn bench_long_pipeline(b: &mut Bencher) {
    let client = get_client();
    let con = client.get_connection().unwrap();
    let mut pipe = redis::pipe();

    for _ in 0..1_000 {
        pipe.set("foo", "bar").ignore();
    }

    b.iter(|| {
        let _: () = pipe.query(&con).unwrap();
    });
}

fn bench_encode_pipeline(b: &mut Bencher) {
    b.iter(|| {
        let mut pipe = redis::pipe();

        for _ in 0..1_000 {
            pipe.set("foo", "bar").ignore();
        }
        pipe
    });
}

fn bench_encode_pipeline_nested(b: &mut Bencher) {
    b.iter(|| {
        let mut pipe = redis::pipe();

        for _ in 0..200 {
            pipe.set("foo", ("bar", 123, b"1231279712", &["test", "test", "test"][..])).ignore();
        }
        pipe
    });
}


benchmark_group!(
    bench,
    bench_simple_getsetdel,
    bench_simple_getsetdel_async,
    bench_simple_getsetdel_pipeline,
    bench_simple_getsetdel_pipeline_precreated,
    bench_long_pipeline,
    bench_encode_pipeline,
    bench_encode_pipeline_nested,
    bench_long_pipeline
);
benchmark_main!(bench);
