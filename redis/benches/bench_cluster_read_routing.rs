#![cfg(feature = "cluster")]

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use redis::cluster::ClusterClient;
use redis::cluster_read_routing::{
    RandomReplicaStrategy, RoundRobinReplicaStrategy, UniformRandom, ZonalReadRoutingStrategy,
};
use redis::{Value, cmd};

use support::{
    MockEnv, MockSlotRange, contains_slice, is_connection_check,
    respond_startup_with_replica_using_config,
};

#[allow(dead_code)]
#[path = "../tests/support/mock_cluster.rs"]
mod support;

fn slots_config() -> Vec<MockSlotRange> {
    vec![
        MockSlotRange {
            primary_port: 6379,
            replica_ports: vec![6380, 6381],
            slot_range: 0..4095,
        },
        MockSlotRange {
            primary_port: 6382,
            replica_ports: vec![6383, 6384],
            slot_range: 4096..8191,
        },
        MockSlotRange {
            primary_port: 6385,
            replica_ports: vec![6386, 6387],
            slot_range: 8192..12287,
        },
        MockSlotRange {
            primary_port: 6388,
            replica_ports: vec![6389, 6390],
            slot_range: 12288..16383,
        },
    ]
}

fn zones() -> Vec<(u16, &'static str)> {
    vec![
        (6379, "us-east-1a"),
        (6380, "us-east-1b"),
        (6381, "us-east-1c"),
        (6382, "us-east-1a"),
        (6383, "us-east-1b"),
        (6384, "us-east-1c"),
        (6385, "us-east-1a"),
        (6386, "us-east-1c"),
        (6387, "us-east-1d"),
        (6388, "us-east-1a"),
        (6389, "us-east-1b"),
        (6390, "us-east-1b"),
    ]
}

fn bulk(value: &str) -> Value {
    Value::BulkString(value.as_bytes().to_vec())
}

fn cluster_shards_with_zones(
    name: &str,
    slots_config: &[MockSlotRange],
    zones: &[(u16, &str)],
) -> Value {
    let zone_for_port = |port| {
        zones
            .iter()
            .find_map(|(candidate_port, zone)| (*candidate_port == port).then_some(*zone))
            .unwrap_or("unknown-zone")
    };

    Value::Array(
        slots_config
            .iter()
            .map(|slot| {
                let mut nodes = vec![Value::Map(vec![
                    (bulk("endpoint"), bulk(name)),
                    (bulk("port"), Value::Int(slot.primary_port as i64)),
                    (
                        bulk("availability-zone"),
                        bulk(zone_for_port(slot.primary_port)),
                    ),
                ])];
                nodes.extend(slot.replica_ports.iter().map(|port| {
                    Value::Map(vec![
                        (bulk("endpoint"), bulk(name)),
                        (bulk("port"), Value::Int(*port as i64)),
                        (bulk("availability-zone"), bulk(zone_for_port(*port))),
                    ])
                }));

                Value::Map(vec![
                    (
                        bulk("slots"),
                        Value::Array(vec![
                            Value::Int(slot.slot_range.start as i64),
                            Value::Int(slot.slot_range.end as i64),
                        ]),
                    ),
                    (bulk("nodes"), Value::Array(nodes)),
                ])
            })
            .collect(),
    )
}

fn mock_env(name: &'static str, builder: redis::cluster::ClusterClientBuilder) -> MockEnv {
    let slots_config = slots_config();
    let zones = zones();

    MockEnv::with_client_builder(builder, name, move |packed_command: &[u8], _port| {
        if is_connection_check(packed_command) {
            return Err(Ok(Value::SimpleString("OK".into())));
        }
        if contains_slice(packed_command, b"CLUSTER") && contains_slice(packed_command, b"SLOTS") {
            return respond_startup_with_replica_using_config(
                name,
                packed_command,
                Some(slots_config.clone()),
            );
        }
        if contains_slice(packed_command, b"CLUSTER") && contains_slice(packed_command, b"SHARDS") {
            return Err(Ok(cluster_shards_with_zones(name, &slots_config, &zones)));
        }
        if contains_slice(packed_command, b"GET") {
            return Err(Ok(Value::BulkString(b"123".to_vec())));
        }
        Ok(())
    })
}

fn bench_get(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &'static str,
    builder: redis::cluster::ClusterClientBuilder,
) {
    let mut env = mock_env(name, builder);
    let value: Option<i32> = cmd("GET")
        .arg(black_box("{bench}key"))
        .query(&mut env.connection)
        .unwrap();
    assert_eq!(value, Some(123));

    group.bench_function(name, |b| {
        b.iter(|| {
            let value: Option<i32> = cmd("GET")
                .arg(black_box("{bench}key"))
                .query(&mut env.connection)
                .unwrap();
            black_box(value)
        });
    });
}

fn bench_cluster_read_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("cluster_read_routing_mock_get");
    group.throughput(Throughput::Elements(1));

    bench_get(
        &mut group,
        "primary_default",
        ClusterClient::builder(vec!["redis://primary_default"]).retries(0),
    );
    bench_get(
        &mut group,
        "random_replica",
        ClusterClient::builder(vec!["redis://random_replica"])
            .retries(0)
            .read_routing_strategy(RandomReplicaStrategy),
    );
    bench_get(
        &mut group,
        "round_robin_replica",
        ClusterClient::builder(vec!["redis://round_robin_replica"])
            .retries(0)
            .read_routing_strategy(RoundRobinReplicaStrategy::new()),
    );
    bench_get(
        &mut group,
        "uniform_random",
        ClusterClient::builder(vec!["redis://uniform_random"])
            .retries(0)
            .read_routing_strategy(UniformRandom::new()),
    );
    bench_get(
        &mut group,
        "zonal_local_replica",
        ClusterClient::builder(vec!["redis://zonal_local_replica"])
            .retries(0)
            .read_routing_strategy(ZonalReadRoutingStrategy::new("us-east-1b")),
    );
    bench_get(
        &mut group,
        "zonal_shared_local_replica",
        ClusterClient::builder(vec!["redis://zonal_shared_local_replica"])
            .retries(0)
            .read_routing_strategy(ZonalReadRoutingStrategy::shared("us-east-1b")),
    );

    group.finish();
}

criterion_group!(cluster_read_routing_bench, bench_cluster_read_routing);
criterion_main!(cluster_read_routing_bench);
