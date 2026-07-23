//! Benchmarks for response parsing (`parse_redis_value`).
//!
//! These measure the cost of turning a RESP byte buffer into a [`redis::Value`].
//! The zero-copy parser slices the response buffer (cheap reference-counted
//! `Bytes`) instead of allocating a fresh `Vec`/`String` for every leaf, so the
//! biggest win is on responses with many leaves (large arrays/maps).
//!
//! In addition to wall-clock time, this bench reports the number of heap
//! allocations performed while parsing each payload (via a counting global
//! allocator). Allocation count is deterministic and machine-independent, which
//! makes it the clearest signal of the zero-copy improvement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use redis::parse_redis_value;

/// A global allocator that counts allocations, used to quantify how many heap
/// allocations parsing performs.
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

/// Builds the set of payloads to benchmark: `(name, encoded bytes)`.
fn payloads() -> Vec<(&'static str, Vec<u8>)> {
    let mut out = Vec::new();

    // A single large bulk string (1 MiB). Old & new both copy ~once here, so
    // this is the "bandwidth bound, not allocation bound" baseline.
    let single_bulk_1mb = {
        let mut v = Vec::new();
        bulk(&mut v, &vec![b'x'; 1024 * 1024]);
        v
    };

    // Many small bulk strings: the old parser allocated one Vec per element.
    let array_5000_small_bulks = {
        let mut v = Vec::new();
        array_header(&mut v, 5000);
        for i in 0..5000 {
            bulk(&mut v, format!("value:{i:08}").as_bytes());
        }
        v
    };

    // Fewer, larger bulk strings (1 KiB each).
    let array_500_1kb_bulks = {
        let mut v = Vec::new();
        array_header(&mut v, 500);
        let chunk = vec![b'y'; 1024];
        for _ in 0..500 {
            bulk(&mut v, &chunk);
        }
        v
    };

    // Many simple strings (old parser allocated a String per element).
    let array_5000_simple_strings = {
        let mut v = Vec::new();
        array_header(&mut v, 5000);
        for i in 0..5000 {
            simple(&mut v, &format!("status:{i:08}"));
        }
        v
    };

    // A map-shaped reply: 1000 key/value bulk-string pairs (2000 leaves).
    let map_1000_pairs = {
        let mut v = Vec::new();
        array_header(&mut v, 2000);
        for i in 0..1000 {
            bulk(&mut v, format!("key:{i:08}").as_bytes());
            bulk(&mut v, format!("val:{i:08}").as_bytes());
        }
        v
    };

    out.push(("single_bulk_1mb", single_bulk_1mb));
    out.push(("array_5000_small_bulks", array_5000_small_bulks));
    out.push(("array_500_1kb_bulks", array_500_1kb_bulks));
    out.push(("array_5000_simple_strings", array_5000_simple_strings));
    out.push(("map_1000_pairs", map_1000_pairs));
    out
}

/// Parses each payload once and reports the number of heap allocations and
/// bytes allocated during parsing. This is deterministic across runs.
fn report_allocations(payloads: &[(&'static str, Vec<u8>)]) {
    println!("\n=== allocations while parsing (lower is better) ===");
    println!(
        "{:<28} {:>12} {:>14}",
        "payload", "allocations", "bytes_alloc'd"
    );
    for (name, bytes) in payloads {
        // Warm up / stabilize, then measure a single parse.
        let _ = parse_redis_value(bytes).unwrap();

        let allocs_before = ALLOC_COUNT.load(Ordering::Relaxed);
        let bytes_before = ALLOC_BYTES.load(Ordering::Relaxed);
        let value = parse_redis_value(black_box(bytes)).unwrap();
        let allocs = ALLOC_COUNT.load(Ordering::Relaxed) - allocs_before;
        let alloc_bytes = ALLOC_BYTES.load(Ordering::Relaxed) - bytes_before;
        // Keep `value` alive across the measurement, then drop it afterwards.
        black_box(&value);
        drop(value);

        println!("{name:<28} {allocs:>12} {alloc_bytes:>14}");
    }
    println!();
}

fn bench_parse(c: &mut Criterion) {
    let payloads = payloads();
    report_allocations(&payloads);

    let mut group = c.benchmark_group("parse_redis_value");
    for (name, bytes) in &payloads {
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_function(*name, |b| {
            b.iter(|| {
                let value = parse_redis_value(black_box(bytes)).unwrap();
                black_box(value);
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_parse);
criterion_main!(benches);
