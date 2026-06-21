use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

const N: usize = 1_000;

// Builds an empty pipeline with capacity pre-reserved for the expected command, argument, and
// argument-byte counts. This is the ONE spot that differs between the old and new layouts: the
// new layout reserves each buffer via `reserve_for_*`, while the old layout only had
// `with_capacity(command_count)`. Keeping the bench function names identical lets criterion
// compare them directly.
fn preallocated_pipe(commands: usize, args: usize, data: usize) -> redis::Pipeline {
    let mut pipe = redis::pipe();
    pipe.reserve_for_commands(commands)
        .reserve_for_args(args)
        .reserve_for_data(data);
    pipe
}

// Create and populate a pipeline with N simple commands.
fn bench_build_pipeline(c: &mut Criterion) {
    c.bench_function("build_pipeline", |b| {
        b.iter(|| {
            let mut pipe = redis::pipe();
            for _ in 0..N {
                pipe.cmd("SET").arg("some_key").arg(42i64).ignore();
            }
            black_box(&pipe);
        });
    });
}

// Create and populate a pipeline with N multi-arg commands.
fn bench_build_pipeline_nested(c: &mut Criterion) {
    c.bench_function("build_pipeline_nested", |b| {
        b.iter(|| {
            let mut pipe = redis::pipe();
            for _ in 0..N / 5 {
                pipe.cmd("MSET")
                    .arg(&[
                        ("foo1", &b"bar"[..]),
                        ("foo2", &b"123"[..]),
                        ("foo3", &b"1231279712"[..]),
                        ("foo4", &b"test"[..]),
                    ])
                    .ignore();
            }
            black_box(&pipe);
        });
    });
}

// Write the packed command bytes for a pre-built pipeline.
fn bench_packed_pipeline(c: &mut Criterion) {
    let mut pipe = redis::pipe();
    for _ in 0..N {
        pipe.cmd("SET").arg("some_key").arg(42i64).ignore();
    }
    c.bench_function("packed_pipeline", |b| {
        b.iter(|| black_box(pipe.get_packed_pipeline()));
    });
}

// End-to-end: create, populate, and write the packed command in one go. This is the realistic
// per-request cost, and verifies the net change is positive across both phases.
fn bench_build_and_pack(c: &mut Criterion) {
    c.bench_function("build_and_pack", |b| {
        b.iter(|| {
            let mut pipe = redis::pipe();
            for _ in 0..N {
                pipe.cmd("SET").arg("some_key").arg(42i64).ignore();
            }
            black_box(pipe.get_packed_pipeline())
        });
    });
}

// Create and populate a pipeline whose buffers were pre-allocated up front. Compared against the
// default `build_pipeline`, this isolates the benefit of reserving capacity; compared across the
// old/new layouts, it pits the old `with_capacity` against the new `reserve_for_*`.
fn bench_build_pipeline_preallocated(c: &mut Criterion) {
    c.bench_function("build_pipeline_preallocated", |b| {
        b.iter(|| {
            let mut pipe = preallocated_pipe(N, N * 3, N * 16);
            for _ in 0..N {
                pipe.cmd("SET").arg("some_key").arg(42i64).ignore();
            }
            black_box(&pipe);
        });
    });
}

// End-to-end with pre-allocated buffers: create, populate, and write the packed command.
fn bench_build_and_pack_preallocated(c: &mut Criterion) {
    c.bench_function("build_and_pack_preallocated", |b| {
        b.iter(|| {
            let mut pipe = preallocated_pipe(N, N * 3, N * 16);
            for _ in 0..N {
                pipe.cmd("SET").arg("some_key").arg(42i64).ignore();
            }
            black_box(pipe.get_packed_pipeline())
        });
    });
}

// Write the packed command bytes for a pre-built atomic (MULTI/EXEC) pipeline.
fn bench_packed_pipeline_atomic(c: &mut Criterion) {
    let mut pipe = redis::pipe();
    pipe.atomic();
    for _ in 0..N {
        pipe.cmd("SET").arg("some_key").arg(42i64).ignore();
    }
    c.bench_function("packed_pipeline_atomic", |b| {
        b.iter(|| black_box(pipe.get_packed_pipeline()));
    });
}

// End-to-end for an atomic pipeline: create, populate, and write the packed command in one go.
fn bench_build_and_pack_atomic(c: &mut Criterion) {
    c.bench_function("build_and_pack_atomic", |b| {
        b.iter(|| {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for _ in 0..N {
                pipe.cmd("SET").arg("some_key").arg(42i64).ignore();
            }
            black_box(pipe.get_packed_pipeline())
        });
    });
}

// A small atomic pipeline — the realistic transaction size, where the fixed cost of the
// MULTI/EXEC wrapper is a meaningful fraction of the work.
const N_SMALL: usize = 5;

fn bench_packed_pipeline_atomic_small(c: &mut Criterion) {
    let mut pipe = redis::pipe();
    pipe.atomic();
    for _ in 0..N_SMALL {
        pipe.cmd("SET").arg("some_key").arg(42i64).ignore();
    }
    c.bench_function("packed_pipeline_atomic_small", |b| {
        b.iter(|| black_box(pipe.get_packed_pipeline()));
    });
}

fn bench_build_and_pack_atomic_small(c: &mut Criterion) {
    c.bench_function("build_and_pack_atomic_small", |b| {
        b.iter(|| {
            let mut pipe = redis::pipe();
            pipe.atomic();
            for _ in 0..N_SMALL {
                pipe.cmd("SET").arg("some_key").arg(42i64).ignore();
            }
            black_box(pipe.get_packed_pipeline())
        });
    });
}

criterion_group!(
    benches,
    bench_build_pipeline,
    bench_build_pipeline_nested,
    bench_build_pipeline_preallocated,
    bench_packed_pipeline,
    bench_build_and_pack,
    bench_build_and_pack_preallocated,
    bench_packed_pipeline_atomic,
    bench_build_and_pack_atomic,
    bench_packed_pipeline_atomic_small,
    bench_build_and_pack_atomic_small
);
criterion_main!(benches);
