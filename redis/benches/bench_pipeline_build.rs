use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

const N: usize = 1_000;

// Payload sizes for the small / medium / large data variants: low tens of bytes, hundreds of
// bytes, and several kilobytes respectively.
const PAYLOAD_SIZES: &[(&str, usize)] = &[("small", 16), ("medium", 256), ("large", 4096)];

// A five-command atomic pipeline — the realistic transaction size, where the fixed cost of the
// MULTI/EXEC wrapper is a meaningful fraction of the work.
const N_TXN: usize = 5;

// Non-payload byte budget per `SET some_key <payload>` command (command name, key, and RESP
// framing), used to size the preallocated-buffer reservations.
const FRAMING_BYTES_PER_CMD: usize = 24;

// Builds an empty pipeline with capacity pre-reserved for the expected command, argument, and
// argument-byte counts. This is the ONE spot that differs between the old and new layouts: the
// new layout reserves each buffer via `reserve_for_*`, while the old layout only had
// `with_capacity(command_count)`.
fn preallocated_pipe(commands: usize, args: usize, data: usize) -> redis::Pipeline {
    let mut pipe = redis::pipe();
    pipe.reserve_for_commands(commands)
        .reserve_for_args(args)
        .reserve_for_data(data);
    pipe
}

// Deterministic filler bytes so every run encodes identical input.
fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| b'a' + (i % 26) as u8).collect()
}

fn add_set(pipe: &mut redis::Pipeline, value: &[u8]) {
    pipe.set("some_key", value).ignore();
}

// Create and populate a pipeline with N simple commands carrying a payload of the given size.
fn bench_build_pipeline(c: &mut Criterion) {
    for &(name, len) in PAYLOAD_SIZES {
        let value = payload(len);
        c.bench_function(&format!("build_pipeline_{name}"), |b| {
            b.iter(|| {
                let mut pipe = redis::pipe();
                for _ in 0..N {
                    add_set(&mut pipe, &value);
                }
                black_box(&pipe);
            });
        });
    }
}

// Create and populate a pipeline with N multi-arg commands whose values carry the payload.
fn bench_build_pipeline_nested(c: &mut Criterion) {
    for &(name, len) in PAYLOAD_SIZES {
        let value = payload(len);
        c.bench_function(&format!("build_pipeline_nested_{name}"), |b| {
            b.iter(|| {
                let mut pipe = redis::pipe();
                for _ in 0..N / 5 {
                    pipe.mset(&[
                        ("foo1", &value[..]),
                        ("foo2", &value[..]),
                        ("foo3", &value[..]),
                        ("foo4", &value[..]),
                    ])
                    .ignore();
                }
                black_box(&pipe);
            });
        });
    }
}

// Write the packed command bytes for a pre-built pipeline.
fn bench_packed_pipeline(c: &mut Criterion) {
    for &(name, len) in PAYLOAD_SIZES {
        let value = payload(len);
        let mut pipe = redis::pipe();
        for _ in 0..N {
            add_set(&mut pipe, &value);
        }
        c.bench_function(&format!("packed_pipeline_{name}"), |b| {
            b.iter(|| black_box(pipe.get_packed_pipeline()));
        });
    }
}

// End-to-end: create, populate, and write the packed command in one go. This is the realistic
// per-request cost, and verifies the net change is positive across both phases.
fn bench_build_and_pack(c: &mut Criterion) {
    for &(name, len) in PAYLOAD_SIZES {
        let value = payload(len);
        c.bench_function(&format!("build_and_pack_{name}"), |b| {
            b.iter(|| {
                let mut pipe = redis::pipe();
                for _ in 0..N {
                    add_set(&mut pipe, &value);
                }
                black_box(pipe.get_packed_pipeline())
            });
        });
    }
}

// Create and populate a pipeline whose buffers were pre-allocated up front. Compared against the
// default `build_pipeline`, this isolates the benefit of reserving capacity; compared across the
// old/new layouts, it pits the old `with_capacity` against the new `reserve_for_*`.
fn bench_build_pipeline_preallocated(c: &mut Criterion) {
    for &(name, len) in PAYLOAD_SIZES {
        let value = payload(len);
        c.bench_function(&format!("build_pipeline_preallocated_{name}"), |b| {
            b.iter(|| {
                let mut pipe = preallocated_pipe(N, N * 3, N * (FRAMING_BYTES_PER_CMD + len));
                for _ in 0..N {
                    add_set(&mut pipe, &value);
                }
                black_box(&pipe);
            });
        });
    }
}

// End-to-end with pre-allocated buffers: create, populate, and write the packed command.
fn bench_build_and_pack_preallocated(c: &mut Criterion) {
    for &(name, len) in PAYLOAD_SIZES {
        let value = payload(len);
        c.bench_function(&format!("build_and_pack_preallocated_{name}"), |b| {
            b.iter(|| {
                let mut pipe = preallocated_pipe(N, N * 3, N * (FRAMING_BYTES_PER_CMD + len));
                for _ in 0..N {
                    add_set(&mut pipe, &value);
                }
                black_box(pipe.get_packed_pipeline())
            });
        });
    }
}

// Write the packed command bytes for a pre-built atomic (MULTI/EXEC) pipeline.
fn bench_packed_pipeline_atomic(c: &mut Criterion) {
    for &(name, len) in PAYLOAD_SIZES {
        let value = payload(len);
        let mut pipe = redis::pipe();
        pipe.atomic();
        for _ in 0..N {
            add_set(&mut pipe, &value);
        }
        c.bench_function(&format!("packed_pipeline_atomic_{name}"), |b| {
            b.iter(|| black_box(pipe.get_packed_pipeline()));
        });
    }
}

// End-to-end for an atomic pipeline: create, populate, and write the packed command in one go.
fn bench_build_and_pack_atomic(c: &mut Criterion) {
    for &(name, len) in PAYLOAD_SIZES {
        let value = payload(len);
        c.bench_function(&format!("build_and_pack_atomic_{name}"), |b| {
            b.iter(|| {
                let mut pipe = redis::pipe();
                pipe.atomic();
                for _ in 0..N {
                    add_set(&mut pipe, &value);
                }
                black_box(pipe.get_packed_pipeline())
            });
        });
    }
}

// Write the packed command bytes for a pre-built transaction-sized atomic pipeline.
fn bench_packed_pipeline_txn(c: &mut Criterion) {
    for &(name, len) in PAYLOAD_SIZES {
        let value = payload(len);
        let mut pipe = redis::pipe();
        pipe.atomic();
        for _ in 0..N_TXN {
            add_set(&mut pipe, &value);
        }
        c.bench_function(&format!("packed_pipeline_txn_{name}"), |b| {
            b.iter(|| black_box(pipe.get_packed_pipeline()));
        });
    }
}

// End-to-end for a transaction-sized atomic pipeline: create, populate, and pack.
fn bench_build_and_pack_txn(c: &mut Criterion) {
    for &(name, len) in PAYLOAD_SIZES {
        let value = payload(len);
        c.bench_function(&format!("build_and_pack_txn_{name}"), |b| {
            b.iter(|| {
                let mut pipe = redis::pipe();
                pipe.atomic();
                for _ in 0..N_TXN {
                    add_set(&mut pipe, &value);
                }
                black_box(pipe.get_packed_pipeline())
            });
        });
    }
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
    bench_packed_pipeline_txn,
    bench_build_and_pack_txn
);
criterion_main!(benches);
