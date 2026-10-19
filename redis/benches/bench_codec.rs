//! Codec-level decode benchmarks: the path `MultiplexedConnection` actually
//! uses — one `ValueCodec` reused across replies over one reused `BytesMut` —
//! as opposed to `bench_decode`, which calls `parse_redis_value` and builds a
//! fresh parser per call.
//!
//! Two things are reported per payload:
//!
//! - heap allocations for one decode with a fresh codec and buffer, and
//!   allocations per cycle with both reused over 100 cycles (the connection
//!   shape);
//! - wall-clock time through the reused codec, split into single-reply and
//!   pipelined groups.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::BytesMut;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use redis::ValueCodec;
use tokio_util::codec::Decoder;

/// A global allocator that counts allocations, used to quantify the heap
/// traffic of decoding.
struct CountingAllocator;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

// --- RESP payload builders ---------------------------------------------------

fn bulk(out: &mut Vec<u8>, data: &[u8]) {
    out.extend_from_slice(format!("${}\r\n", data.len()).as_bytes());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

fn simple(out: &mut Vec<u8>, data: &str) {
    out.extend_from_slice(format!("+{data}\r\n").as_bytes());
}

fn array_header(out: &mut Vec<u8>, len: usize) {
    out.extend_from_slice(format!("*{len}\r\n").as_bytes());
}

/// An array nested `depth` deep: `*1\r\n` x `depth` around a single integer.
fn nested(depth: usize) -> Vec<u8> {
    let mut v = Vec::new();
    for _ in 0..depth {
        array_header(&mut v, 1);
    }
    v.extend_from_slice(b":1\r\n");
    v
}

/// A flat array of `count` bulk strings of `leaf` bytes each.
fn leaf_array(count: usize, leaf: usize) -> Vec<u8> {
    let mut v = Vec::new();
    array_header(&mut v, count);
    let data = vec![b'x'; leaf];
    for _ in 0..count {
        bulk(&mut v, &data);
    }
    v
}

/// Single-reply payloads: one `decode` per cycle.
fn singles() -> Vec<(&'static str, Vec<u8>, usize)> {
    let mut out: Vec<(&'static str, Vec<u8>, usize)> = Vec::new();

    out.push(("ok", b"+OK\r\n".to_vec(), 1));
    out.push(("int", b":1\r\n".to_vec(), 1));

    let mut get_20b = Vec::new();
    bulk(&mut get_20b, &[b'a'; 20]);
    out.push(("get_20b", get_20b, 1));

    out.push((
        "array_3_ints",
        b"*3\r\n:100\r\n:101\r\n:102\r\n".to_vec(),
        1,
    ));

    for depth in [1usize, 3, 10, 50, 90] {
        out.push((
            Box::leak(format!("nest_{depth}").into_boxed_str()),
            nested(depth),
            1,
        ));
    }

    // Same payload as bench_decode's array_5000_small_bulks.
    let mut array_5000 = Vec::new();
    array_header(&mut array_5000, 5000);
    for i in 0..5000 {
        bulk(&mut array_5000, format!("value:{i:08}").as_bytes());
    }
    out.push(("array_5000_small_bulks", array_5000, 1));

    for leaf in [8usize, 128, 512, 1024, 2048] {
        out.push((
            Box::leak(format!("array_500_leaf_{leaf}").into_boxed_str()),
            leaf_array(500, leaf),
            1,
        ));
    }

    let mut bulk_1mb = Vec::new();
    bulk(&mut bulk_1mb, &vec![b'x'; 1024 * 1024]);
    out.push(("bulk_1mb", bulk_1mb, 1));

    out
}

/// Pipelined payloads: many replies appended per cycle, decoded one per call
/// on the same codec/buffer — the shape of a pipelined connection.
fn pipelines() -> Vec<(&'static str, Vec<u8>, usize)> {
    let mut out: Vec<(&'static str, Vec<u8>, usize)> = Vec::new();

    let mut pipe_1000_ok = Vec::new();
    for _ in 0..1000 {
        simple(&mut pipe_1000_ok, "OK");
    }
    out.push(("pipe_1000_ok", pipe_1000_ok, 1000));

    let mut pipe_1000_get20 = Vec::new();
    for _ in 0..1000 {
        bulk(&mut pipe_1000_get20, &[b'a'; 20]);
    }
    out.push(("pipe_1000_get20", pipe_1000_get20, 1000));

    for leaf in [8usize, 128, 512, 1024, 2048] {
        // A flat run of 500 bulk replies (no array header): the true
        // pipelined shape, one decode per reply.
        let mut v = Vec::new();
        let data = vec![b'x'; leaf];
        for _ in 0..500 {
            bulk(&mut v, &data);
        }
        out.push((
            Box::leak(format!("pipe_500_leaf_{leaf}").into_boxed_str()),
            v,
            500,
        ));
    }

    out
}

/// One fill-and-drain cycle through a reused codec and buffer, mirroring a
/// connection: keep spare capacity, append the payload, decode exactly the
/// number of replies it contains (a connection only decodes while it has
/// bytes).
fn cycle(codec: &mut ValueCodec, buf: &mut BytesMut, payload: &[u8], replies: usize) {
    buf.reserve(payload.len());
    buf.extend_from_slice(payload);
    for _ in 0..replies {
        let value = codec.decode(buf).expect("complete reply");
        black_box(value);
    }
    debug_assert!(buf.is_empty(), "the drain loop must consume every reply");
}

fn report_allocations(payloads: &[(&'static str, Vec<u8>, usize)]) {
    println!("\n=== codec allocations (lower is better) ===");
    println!(
        "{:<26} {:>12} {:>12} {:>14} {:>14}",
        "payload", "fresh_allocs", "fresh_bytes", "steady_alloc/100", "steady_bytes/100"
    );
    for (name, bytes, replies) in payloads {
        // Fresh: a new codec and buffer per decode (bench_decode's shape).
        let warm = {
            let mut codec = ValueCodec::default();
            let mut buf = BytesMut::from(&bytes[..]);
            codec.decode(&mut buf).unwrap()
        };
        drop(warm);
        let count_before = ALLOC_COUNT.load(Ordering::Relaxed);
        let bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
        {
            let mut codec = ValueCodec::default();
            let mut buf = BytesMut::from(&bytes[..]);
            let value = codec.decode(&mut buf).unwrap();
            black_box(&value);
            drop(value);
        }
        let fresh_count = ALLOC_COUNT.load(Ordering::Relaxed) - count_before;
        let fresh_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - bytes_before;

        // Steady: the same codec and buffer across cycles.
        let mut codec = ValueCodec::default();
        let mut buf = BytesMut::with_capacity(bytes.len() * 4 + 64 * 1024);
        cycle(&mut codec, &mut buf, bytes, *replies);
        cycle(&mut codec, &mut buf, bytes, *replies);
        let count_before = ALLOC_COUNT.load(Ordering::Relaxed);
        let bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
        for _ in 0..100 {
            cycle(&mut codec, &mut buf, bytes, *replies);
        }
        let steady_count = ALLOC_COUNT.load(Ordering::Relaxed) - count_before;
        let steady_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - bytes_before;

        println!(
            "{name:<26} {fresh_count:>12} {fresh_bytes:>12} {steady_count:>14} {steady_bytes:>14}"
        );
    }
    println!();
}

fn bench_single(c: &mut Criterion) {
    let payloads = singles();
    let mut group = c.benchmark_group("codec_single");
    for (name, bytes, replies) in &payloads {
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_function(*name, |b| {
            let mut codec = ValueCodec::default();
            let mut buf = BytesMut::with_capacity(bytes.len() * 4 + 64 * 1024);
            b.iter(|| cycle(&mut codec, &mut buf, bytes, *replies));
        });
    }
    group.finish();
}

fn bench_pipeline(c: &mut Criterion) {
    let payloads = pipelines();
    let mut group = c.benchmark_group("codec_pipeline");
    for (name, bytes, replies) in &payloads {
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_function(*name, |b| {
            let mut codec = ValueCodec::default();
            let mut buf = BytesMut::with_capacity(bytes.len() * 4 + 64 * 1024);
            b.iter(|| cycle(&mut codec, &mut buf, bytes, *replies));
        });
    }
    group.finish();
}

fn bench_codec(c: &mut Criterion) {
    let mut payloads = singles();
    payloads.extend(pipelines());
    report_allocations(&payloads);
    bench_single(c);
    bench_pipeline(c);
}

criterion_group!(benches, bench_codec);
criterion_main!(benches);
