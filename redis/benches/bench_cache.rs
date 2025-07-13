use iai_callgrind::{library_benchmark, library_benchmark_group, main, LibraryBenchmarkConfig};
use redis::{aio::MultiplexedConnection, Cmd};

use support::*;

#[path = "../tests/support/mod.rs"]
mod support;

use rand::{
    distr::{Bernoulli, Distribution},
    Rng,
};
use std::env;
use tokio::runtime::Runtime;

fn generate_commands(key: &str, total_command: u32, total_read: u32) -> Vec<Cmd> {
    let mut cmds = vec![];
    let mut rng = rand::rng();
    let distribution = Bernoulli::from_ratio(total_read, total_command).unwrap();
    for is_read in distribution
        .sample_iter(&mut rand::rng())
        .take(total_command as usize)
    {
        if is_read {
            cmds.push(redis::cmd("GET").arg(key).clone());
        } else {
            cmds.push(
                redis::cmd("SET")
                    .arg(key)
                    .arg(rng.random_range(1..300))
                    .clone(),
            );
        }
    }
    cmds
}

struct Dependencies {
    _context: TestContext,
    runtime: Runtime,
    commands: Vec<Vec<Cmd>>,
    connection: MultiplexedConnection,
}

fn setup(
    thread_count: usize,
    is_cache_enabled: bool,
    read_ratio: f32,
    per_key_command: u32,
    key_count: u32,
) -> Dependencies {
    env::set_var("PROTOCOL", "RESP3");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(thread_count)
        .enable_all()
        .build()
        .unwrap();
    let context = TestContext::new();
    let connection = if is_cache_enabled {
        runtime.block_on(context.async_connection()).unwrap()
    } else {
        runtime
            .block_on(context.async_connection_with_cache())
            .unwrap()
    };

    let mut commands = Vec::with_capacity(key_count as usize);
    for i in 0..key_count {
        let key = format!("KEY{i}");
        commands.push(generate_commands(
            &key,
            per_key_command,
            (read_ratio * per_key_command as f32) as u32,
        ))
    }

    Dependencies {
        _context: context,
        runtime,
        commands,
        connection,
    }
}

async fn run_bench(commands: Vec<Vec<Cmd>>, connection: MultiplexedConnection) {
    let mut handles = Vec::new();
    for command_set in commands {
        let mut con = connection.clone();
        handles.push(tokio::spawn(async move {
            for cmd in command_set {
                let _: () = cmd.query_async(&mut con).await.unwrap();
            }
        }));
    }
    for job_handle in handles {
        job_handle.await.unwrap();
    }
}

#[library_benchmark]
#[benches::with_setup(args = [
        (1,true,0.99, 30, 30),
        (1,true,0.90, 30, 30),
        (1,true,0.5, 30, 30),
        (1,true,0.1, 30, 30),
        (1,true,0.5, 5, 200),
        (1,true,0.5, 1, 200),
        (1,false,0.99, 30, 30),
        (1,false,0.90, 30, 30),
        (1,false,0.5, 30, 30),
        (1,false,0.1, 30, 30),
        (1,false,0.5, 5, 200),
        (1,false,0.5, 1, 200),
        (4,true,0.99, 30, 30),
        (4,true,0.90, 30, 30),
        (4,true,0.5, 30, 30),
        (4,true,0.1, 30, 30),
        (4,true,0.5, 5, 200),
        (4,true,0.5, 1, 200),
        (4,false,0.99, 30, 30),
        (4,false,0.90, 30, 30),
        (4,false,0.5, 30, 30),
        (4,false,0.1, 30, 30),
        (4,false,0.5, 5, 200),
        (4,false,0.5, 1, 200),
    ], setup = setup)]
fn bench(dependencies: Dependencies) {
    dependencies
        .runtime
        .block_on(run_bench(dependencies.commands, dependencies.connection))
}

library_benchmark_group!(
    name = async_cache_bench;
    benchmarks=bench,
);

main!(
    config = LibraryBenchmarkConfig::default().env_clear(false);
    library_benchmark_groups = async_cache_bench,
);
