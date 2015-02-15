extern crate test;
extern crate redis;

use test::Bencher;

fn get_client() -> redis::Client {
    redis::Client::open("redis://127.0.0.1:6379").unwrap()
}

#[bench]
fn bench_simple_getsetdel(b: &mut Bencher) {
    let client = get_client();
    let con = client.get_connection().unwrap();

    b.iter(|| {
        let key = "test_key";
        redis::cmd("SET").arg(key).arg(42i).execute(&con);
        let _ : int = redis::cmd("GET").arg(key).query(&con).unwrap();
        redis::cmd("DEL").arg(key).execute(&con);
    });
}

#[bench]
fn bench_simple_getsetdel_pipeline(b: &mut Bencher) {
    let client = get_client();
    let con = client.get_connection().unwrap();

    b.iter(|| {
        let key = "test_key";
        let _ : (uint,) = redis::pipe()
            .cmd("SET").arg(key).arg(42i).ignore()
            .cmd("GET").arg(key)
            .cmd("DEL").arg(key).ignore().query(&con).unwrap();
    });
}

#[bench]
fn bench_simple_getsetdel_pipeline_precreated(b: &mut Bencher) {
    let client = get_client();
    let con = client.get_connection().unwrap();
    let key = "test_key";
    let mut pipe = redis::pipe();
    pipe
        .cmd("SET").arg(key).arg(42i).ignore()
        .cmd("GET").arg(key)
        .cmd("DEL").arg(key).ignore();

    b.iter(|| {
        let _ : (uint,) = pipe.query(&con).unwrap();
    });
}
