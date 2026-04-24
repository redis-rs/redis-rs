#![allow(clippy::let_unit_value)]

#[macro_use]
mod support;

#[cfg(test)]
mod hotkeys {
    use crate::support::*;
    use redis::{Commands, HotkeysCommands, HotkeysOptions, ProtocolVersion, RedisConnectionInfo};
    use rstest::rstest;
    use std::thread::sleep;
    use std::time::Duration;

    const TEST_KEYS_AND_VALUES: [(&str, &str); 3] = [
        ("test_key_1", "value1"),
        ("test_key_2", "value2"),
        ("test_key_3", "value3"),
    ];

    fn setup_connection_with_protocol(
        ctx: &TestContext,
        protocol: ProtocolVersion,
    ) -> redis::Connection {
        let connection_info = ctx
            .server
            .connection_info()
            .set_redis_settings(RedisConnectionInfo::default().set_protocol(protocol));
        let client = redis::Client::open(connection_info).unwrap();
        client.get_connection().unwrap()
    }

    fn setup_test_keys_and_make_hot_keys(con: &mut redis::Connection) {
        for (i, (key, value)) in TEST_KEYS_AND_VALUES.iter().enumerate() {
            let _: () = con.set(key, value).unwrap();
            for _ in 0..i * 10 {
                let _: String = con.get(key).unwrap();
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub(super) enum Metric {
        Cpu,
        Net,
        All,
    }

    impl Metric {
        pub(super) fn options(self) -> HotkeysOptions {
            match self {
                Metric::Cpu => HotkeysOptions::new_with_cpu(),
                Metric::Net => HotkeysOptions::new_with_net(),
                Metric::All => HotkeysOptions::new_with_cpu().and_net(),
            }
        }
    }

    impl std::fmt::Display for Metric {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(match self {
                Metric::Cpu => "CPU",
                Metric::Net => "NET",
                Metric::All => "CPU+NET",
            })
        }
    }

    #[rstest]
    #[case(ProtocolVersion::RESP2)]
    #[case(ProtocolVersion::RESP3)]
    fn test_hotkeys_state_machine_behavior(#[case] protocol: ProtocolVersion) {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_6);
        let mut con = setup_connection_with_protocol(&ctx, protocol);

        println!("Starting test_hotkeys_state_machine_behavior - Protocol: {protocol:?}");

        // When there isn't an active tracking session:
        //  STOP returns false
        assert!(!con.hotkeys_stop().unwrap());
        //  RESET succeeds as a no-op
        assert!(con.hotkeys_reset().is_ok());
        //  GET returns None
        assert!(con.hotkeys_get().unwrap().is_none());

        // Start a tracking session by CPU.
        assert!(con.hotkeys_start(HotkeysOptions::new_with_cpu()).is_ok());
        // When there is a running tracking session START returns an error.
        assert!(con.hotkeys_start(HotkeysOptions::new_with_net()).is_err());

        // Setup test keys and make some hot keys.
        setup_test_keys_and_make_hot_keys(&mut con);

        // Get a snapshot of the running session.
        let tracking_session_snapshot = con.hotkeys_get().unwrap();
        assert!(tracking_session_snapshot.is_some());
        let tracking_session_snapshot = tracking_session_snapshot.unwrap();
        assert!(tracking_session_snapshot.tracking_active);
        assert!(tracking_session_snapshot.by_cpu_time_us.is_some());
        assert!(!tracking_session_snapshot.by_cpu_time_us.unwrap().is_empty());
        assert!(tracking_session_snapshot.by_net_bytes.is_none());

        // RESET only works when no tracking session is running.
        // With a live session it should return a server error.
        assert!(con.hotkeys_reset().is_err());

        // STOP should return true when there is a running session.
        assert!(con.hotkeys_stop().unwrap());
        // STOP returns false when there is no running session.
        assert!(!con.hotkeys_stop().unwrap());

        // Get a snapshot of the final state.
        let final_snapshot = con.hotkeys_get().unwrap();
        assert!(final_snapshot.is_some());
        let final_snapshot = final_snapshot.unwrap();
        assert!(!final_snapshot.tracking_active);
        assert!(final_snapshot.by_cpu_time_us.is_some());
        assert!(!final_snapshot.by_cpu_time_us.unwrap().is_empty());
        assert!(final_snapshot.by_net_bytes.is_none());

        // Verify that starting a new session will override any existing tracking state.
        assert!(con.hotkeys_start(HotkeysOptions::new_with_cpu()).is_ok());
        let new_session_snapshot = con.hotkeys_get().unwrap();
        assert!(new_session_snapshot.is_some());
        let new_session_snapshot = new_session_snapshot.unwrap();
        assert!(new_session_snapshot.tracking_active);
        // The new session should not have any data, since no keys were accessed.
        // Note: since the tracking session is by CPU, it is expected that the by_cpu_time_us is present but empty.
        assert!(new_session_snapshot.by_cpu_time_us.is_some());
        assert!(new_session_snapshot.by_cpu_time_us.unwrap().is_empty());
        assert!(new_session_snapshot.by_net_bytes.is_none());

        // Stop the new session to enable resetting the state.
        assert!(con.hotkeys_stop().unwrap());

        // Clear the state and verify that GET returns None.
        assert!(con.hotkeys_reset().is_ok());
        assert!(con.hotkeys_get().unwrap().is_none());
    }

    #[rstest]
    #[case(ProtocolVersion::RESP2, Metric::Cpu)]
    #[case(ProtocolVersion::RESP3, Metric::Cpu)]
    #[case(ProtocolVersion::RESP2, Metric::Net)]
    #[case(ProtocolVersion::RESP3, Metric::Net)]
    #[case(ProtocolVersion::RESP2, Metric::All)]
    #[case(ProtocolVersion::RESP3, Metric::All)]
    fn test_hotkeys_with_metric(#[case] protocol: ProtocolVersion, #[case] metric: Metric) {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_6);
        let mut con = setup_connection_with_protocol(&ctx, protocol);

        println!("Starting test_hotkeys_with_metric - Metric: {metric}, Protocol: {protocol:?}");

        assert!(con.hotkeys_start(metric.options()).is_ok());
        setup_test_keys_and_make_hot_keys(&mut con);

        let result = con.hotkeys_get().unwrap().unwrap();

        assert!(result.tracking_active);
        assert_eq!(result.sample_ratio, 1);
        assert!(result.collection_duration_ms > 0);

        let expect_cpu = matches!(metric, Metric::Cpu);
        let expect_net = matches!(metric, Metric::Net);
        let expect_all = matches!(metric, Metric::All);

        assert_eq!(result.by_cpu_time_us.is_some(), expect_cpu || expect_all);
        assert_eq!(result.by_net_bytes.is_some(), expect_net || expect_all);
    }

    #[rstest]
    #[case(ProtocolVersion::RESP2)]
    #[case(ProtocolVersion::RESP3)]
    fn test_hotkeys_options_with_duration_and_count(#[case] protocol: ProtocolVersion) {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_6);
        let mut con = setup_connection_with_protocol(&ctx, protocol);

        println!("Starting test_hotkeys_options_with_duration_and_count - Protocol: {protocol:?}");

        assert!(
            con.hotkeys_start(
                HotkeysOptions::new_with_cpu()
                    .with_count(2)
                    .unwrap()
                    .with_duration_secs(2)
            )
            .is_ok()
        );
        setup_test_keys_and_make_hot_keys(&mut con);
        let result = con.hotkeys_get().unwrap().unwrap();
        assert!(result.tracking_active);
        assert!(result.by_cpu_time_us.is_some());
        assert_eq!(result.by_cpu_time_us.unwrap().len(), 2);

        sleep(Duration::from_secs(3));

        let result = con.hotkeys_get().unwrap().unwrap();
        assert!(!result.tracking_active);
        assert!(result.by_cpu_time_us.is_some());
        assert_eq!(result.by_cpu_time_us.unwrap().len(), 2);
    }

    #[rstest]
    #[case(ProtocolVersion::RESP2)]
    #[case(ProtocolVersion::RESP3)]
    fn test_hotkeys_options_with_sample_ratio(#[case] protocol: ProtocolVersion) {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_6);
        let mut con = setup_connection_with_protocol(&ctx, protocol);

        println!("Starting test_hotkeys_options_with_sample_ratio - Protocol: {protocol:?}");

        const SAMPLE_RATIO: u64 = 100;
        assert!(
            con.hotkeys_start(HotkeysOptions::new_with_cpu().with_sample_ratio(SAMPLE_RATIO))
                .is_ok()
        );

        let result = con.hotkeys_get().unwrap().unwrap();
        assert!(result.tracking_active);
        assert_eq!(result.sample_ratio, SAMPLE_RATIO);
    }

    #[rstest]
    #[case(ProtocolVersion::RESP2)]
    #[case(ProtocolVersion::RESP3)]
    fn test_hotkeys_start_with_slots_on_standalone_errors(#[case] protocol: ProtocolVersion) {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_6);
        let mut con = setup_connection_with_protocol(&ctx, protocol);

        println!(
            "Starting test_hotkeys_start_with_slots_on_standalone_errors - Protocol: {protocol:?}"
        );

        let err = con
            .hotkeys_start(HotkeysOptions::new_with_cpu().with_slots(vec![100]))
            .expect_err("SLOTS on a standalone server must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("SLOTS parameter cannot be used in non-cluster mode"),
            "unexpected error for SLOTS on standalone: {msg}",
        );

        assert!(con.hotkeys_get().unwrap().is_none());
    }
}

#[cfg(all(test, feature = "cluster"))]
mod hotkeys_cluster {
    use crate::support::*;
    use redis::cluster::ClusterClientBuilder;
    use redis::cluster_routing::{Route, RoutingInfo, SingleNodeRoutingInfo, SlotAddr};
    use redis::{
        Commands, ConnectionAddr, ConnectionInfo, HotkeysCommands, HotkeysOptions, HotkeysResponse,
        ProtocolVersion, RedisConnectionInfo, Value, cmd, from_redis_value,
    };
    use rstest::rstest;

    /// Open a direct (non-cluster) connection to a specific node using `protocol`.
    fn direct_connection(info: ConnectionInfo, protocol: ProtocolVersion) -> redis::Connection {
        let info = info.set_redis_settings(RedisConnectionInfo::default().set_protocol(protocol));
        redis::Client::open(info).unwrap().get_connection().unwrap()
    }

    /// Extract the TCP port from a `ConnectionAddr`.
    fn port_of(addr: &ConnectionAddr) -> u16 {
        match addr {
            ConnectionAddr::Tcp(_, p) => *p,
            ConnectionAddr::TcpTls { port, .. } => *port,
            _ => panic!("Unsupported address type for cluster tests: {addr:?}"),
        }
    }

    /// Return the (start, end) of the first slot range owned by the primary on `port`.
    fn first_owned_slot_range(con: &mut redis::Connection, port: u16) -> (u16, u16) {
        let value: Value = cmd("CLUSTER").arg("SLOTS").query(con).unwrap();
        let Value::Array(ranges) = value else {
            panic!("Expected array from CLUSTER SLOTS, got {value:?}");
        };
        for range in ranges {
            let Value::Array(fields) = range else {
                continue;
            };
            if fields.len() < 3 {
                continue;
            }
            let start: u16 = from_redis_value(fields[0].clone()).unwrap();
            let end: u16 = from_redis_value(fields[1].clone()).unwrap();
            let Value::Array(master) = &fields[2] else {
                continue;
            };
            if master.len() < 2 {
                continue;
            }
            let master_port: u16 = from_redis_value(master[1].clone()).unwrap();
            if master_port == port {
                return (start, end);
            }
        }
        panic!("No slot range found for port {port}");
    }

    /// Find a hash-tagged key whose slot is in the inclusive range `[start, end]`, using `CLUSTER KEYSLOT`.
    fn find_key_in_range(con: &mut redis::Connection, start: u16, end: u16) -> (String, u16) {
        for i in 0..50_000u32 {
            let key = format!("{{hot{i}}}:k");
            let slot: u16 = cmd("CLUSTER").arg("KEYSLOT").arg(&key).query(con).unwrap();
            if slot >= start && slot <= end {
                return (key, slot);
            }
        }
        panic!("Could not find a key hashing to [{start}, {end}]");
    }

    /// Common setup: build a cluster, pick the first primary, find a key whose slot it owns.
    /// Returns `(cluster_ctx, primary_info, hot_key, target_slot)`.
    ///
    /// The version check is performed via a direct standalone connection to a single
    /// primary; `TestClusterContext::get_version()` would broadcast `INFO` to every
    /// node and fail to parse the multi-node map response.
    fn setup_cluster_and_target(
        protocol: ProtocolVersion,
    ) -> Option<(TestClusterContext, ConnectionInfo, String, u16)> {
        let cluster_ctx = TestClusterContext::new();
        cluster_ctx.wait_for_cluster_up();

        let server = cluster_ctx.cluster.iter_servers().next().unwrap();
        let info = server.connection_info();
        let port = port_of(info.addr());
        let mut direct = direct_connection(info.clone(), protocol);

        let server_info: redis::InfoDict =
            redis::Cmd::new().arg("INFO").query(&mut direct).unwrap();
        let version = parse_version(server_info);
        if version < REDIS_VERSION_CE_8_6 {
            eprintln!(
                "Skipping: Redis version {version:?} < {:?}",
                REDIS_VERSION_CE_8_6
            );
            return None;
        }

        let (range_start, range_end) = first_owned_slot_range(&mut direct, port);
        let (hot_key, target_slot) = find_key_in_range(&mut direct, range_start, range_end);
        println!(
            "cluster node on port {port} owns slots [{range_start}, {range_end}]; \
             using key {hot_key} -> slot {target_slot}"
        );

        Some((cluster_ctx, info, hot_key, target_slot))
    }

    #[rstest]
    #[case(ProtocolVersion::RESP2)]
    #[case(ProtocolVersion::RESP3)]
    fn test_hotkeys_cluster_with_slots_via_direct_client(#[case] protocol: ProtocolVersion) {
        println!(
            "Starting test_hotkeys_cluster_with_slots_via_direct_client - Protocol: {protocol:?}"
        );

        let Some((_cluster_ctx, info, hot_key, target_slot)) = setup_cluster_and_target(protocol)
        else {
            return;
        };

        let mut con = direct_connection(info, protocol);

        let opts = HotkeysOptions::new_with_cpu()
            .and_net()
            .with_slots(vec![target_slot]);
        con.hotkeys_start(opts).unwrap();

        // Generate activity on the tracked key.
        let _: () = con.set(&hot_key, "value").unwrap();
        for _ in 0..50 {
            let _: String = con.get(&hot_key).unwrap();
        }

        let snapshot = con.hotkeys_get().unwrap().expect("session is active");
        assert!(snapshot.tracking_active);

        // SLOTS filter reported back in the response.
        assert!(
            snapshot
                .selected_slots
                .iter()
                .any(|r| r.start <= target_slot && target_slot <= r.end),
            "selected_slots {:?} should include slot {target_slot}",
            snapshot.selected_slots,
        );

        // Cluster-only fields populated when SLOTS was used.
        assert!(snapshot.all_commands_selected_slots_us.is_some());
        assert!(snapshot.net_bytes_all_commands_selected_slots.is_some());

        // The hot key should appear in both CPU and NET breakdowns.
        let cpu = snapshot
            .by_cpu_time_us
            .as_ref()
            .expect("CPU metric requested");
        assert!(cpu.iter().any(|e| e.key == hot_key));
        let net = snapshot
            .by_net_bytes
            .as_ref()
            .expect("NET metric requested");
        assert!(net.iter().any(|e| e.key == hot_key));
    }

    #[rstest]
    #[case(ProtocolVersion::RESP2)]
    #[case(ProtocolVersion::RESP3)]
    fn test_hotkeys_cluster_with_slots_via_cluster_routing(#[case] protocol: ProtocolVersion) {
        println!(
            "Starting test_hotkeys_cluster_with_slots_via_cluster_routing - Protocol: {protocol:?}"
        );

        let Some((cluster_ctx, _info, hot_key, target_slot)) = setup_cluster_and_target(protocol)
        else {
            return;
        };

        // Build a ClusterClient with the requested protocol.
        let client = ClusterClientBuilder::new(cluster_ctx.nodes.clone())
            .use_protocol(protocol)
            .build()
            .unwrap();
        let mut cluster_con = client.get_connection().unwrap();

        let routing = RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(Route::new(
            target_slot,
            SlotAddr::Master,
        )));

        // START with SLOTS filter via route_command.
        let opts = HotkeysOptions::new_with_cpu()
            .and_net()
            .with_slots(vec![target_slot]);
        let mut start_cmd = cmd("HOTKEYS");
        start_cmd.arg("START").arg(&opts);
        let start_value = cluster_con
            .route_command(&start_cmd, routing.clone())
            .unwrap();
        assert_eq!(start_value, Value::Okay);

        // Generate activity. ClusterConnection routes the hash-tagged key to its owning primary.
        let _: () = cluster_con.set(&hot_key, "value").unwrap();
        for _ in 0..50 {
            let _: String = cluster_con.get(&hot_key).unwrap();
        }

        // GET via route_command and parse manually.
        let mut get_cmd = cmd("HOTKEYS");
        get_cmd.arg("GET");
        let get_value = cluster_con
            .route_command(&get_cmd, routing.clone())
            .unwrap();
        let snapshot: Option<HotkeysResponse> = from_redis_value(get_value).unwrap();
        let snapshot = snapshot.expect("session is active");

        assert!(snapshot.tracking_active);
        assert!(
            snapshot
                .selected_slots
                .iter()
                .any(|r| r.start <= target_slot && target_slot <= r.end),
            "selected_slots {:?} should include slot {target_slot}",
            snapshot.selected_slots,
        );
        assert!(snapshot.all_commands_selected_slots_us.is_some());
        assert!(snapshot.net_bytes_all_commands_selected_slots.is_some());
        assert!(
            snapshot
                .by_cpu_time_us
                .as_ref()
                .expect("CPU metric requested")
                .iter()
                .any(|e| e.key == hot_key)
        );
        assert!(
            snapshot
                .by_net_bytes
                .as_ref()
                .expect("NET metric requested")
                .iter()
                .any(|e| e.key == hot_key)
        );
    }
}

#[cfg(all(test, feature = "aio"))]
mod async_hotkeys {
    use crate::support::*;
    use redis::{
        AsyncCommands, AsyncHotkeysCommands, HotkeysOptions, ProtocolVersion, RedisConnectionInfo,
    };
    use rstest::rstest;
    use std::time::Duration;

    const TEST_KEYS_AND_VALUES: [(&str, &str); 3] = [
        ("async_test_key_1", "value1"),
        ("async_test_key_2", "value2"),
        ("async_test_key_3", "value3"),
    ];

    async fn setup_async_connection_with_protocol(
        ctx: &TestContext,
        protocol: ProtocolVersion,
    ) -> redis::aio::MultiplexedConnection {
        let connection_info = ctx
            .server
            .connection_info()
            .set_redis_settings(RedisConnectionInfo::default().set_protocol(protocol));
        let client = redis::Client::open(connection_info).unwrap();
        client.get_multiplexed_async_connection().await.unwrap()
    }

    async fn setup_test_keys_and_make_hot_keys(con: &mut redis::aio::MultiplexedConnection) {
        for (i, (key, value)) in TEST_KEYS_AND_VALUES.iter().enumerate() {
            let _: () = con.set(key, value).await.unwrap();
            for _ in 0..i * 10 {
                let _: String = con.get(key).await.unwrap();
            }
        }
    }

    use super::hotkeys::Metric;

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_hotkeys_state_machine_behavior_async(
        #[case] runtime: RuntimeType,
        #[values(ProtocolVersion::RESP2, ProtocolVersion::RESP3)] protocol: ProtocolVersion,
    ) {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_6);

        println!("Starting test_hotkeys_state_machine_behavior_async - Protocol: {protocol:?}");

        block_on_all(
            async move {
                let mut con = setup_async_connection_with_protocol(&ctx, protocol).await;

                // When there isn't an active tracking session:
                //  STOP returns false
                assert!(!con.hotkeys_stop().await.unwrap());
                //  RESET succeeds as a no-op
                assert!(con.hotkeys_reset().await.is_ok());
                //  GET returns None
                assert!(con.hotkeys_get().await.unwrap().is_none());

                // Start a tracking session by CPU.
                assert!(
                    con.hotkeys_start(HotkeysOptions::new_with_cpu())
                        .await
                        .is_ok()
                );
                // When there is a running tracking session START returns an error.
                assert!(
                    con.hotkeys_start(HotkeysOptions::new_with_net())
                        .await
                        .is_err()
                );

                setup_test_keys_and_make_hot_keys(&mut con).await;

                let snapshot = con.hotkeys_get().await.unwrap().unwrap();
                assert!(snapshot.tracking_active);
                assert!(snapshot.by_cpu_time_us.is_some());
                assert!(!snapshot.by_cpu_time_us.unwrap().is_empty());
                assert!(snapshot.by_net_bytes.is_none());

                // RESET while a live session exists must error.
                assert!(con.hotkeys_reset().await.is_err());

                // STOP returns true when a session is running, false afterwards.
                assert!(con.hotkeys_stop().await.unwrap());
                assert!(!con.hotkeys_stop().await.unwrap());

                let final_snapshot = con.hotkeys_get().await.unwrap().unwrap();
                assert!(!final_snapshot.tracking_active);
                assert!(final_snapshot.by_cpu_time_us.is_some());
                assert!(!final_snapshot.by_cpu_time_us.unwrap().is_empty());
                assert!(final_snapshot.by_net_bytes.is_none());

                // Starting a new session overrides any existing tracking state.
                assert!(
                    con.hotkeys_start(HotkeysOptions::new_with_cpu())
                        .await
                        .is_ok()
                );
                let new_snapshot = con.hotkeys_get().await.unwrap().unwrap();
                assert!(new_snapshot.tracking_active);
                assert!(new_snapshot.by_cpu_time_us.is_some());
                assert!(new_snapshot.by_cpu_time_us.unwrap().is_empty());
                assert!(new_snapshot.by_net_bytes.is_none());

                assert!(con.hotkeys_stop().await.unwrap());
                assert!(con.hotkeys_reset().await.is_ok());
                assert!(con.hotkeys_get().await.unwrap().is_none());
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_hotkeys_with_metric_async(
        #[case] runtime: RuntimeType,
        #[values(ProtocolVersion::RESP2, ProtocolVersion::RESP3)] protocol: ProtocolVersion,
        #[values(Metric::Cpu, Metric::Net, Metric::All)] metric: Metric,
    ) {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_6);

        println!(
            "Starting test_hotkeys_with_metric_async - Metric: {metric}, Protocol: {protocol:?}"
        );

        block_on_all(
            async move {
                let mut con = setup_async_connection_with_protocol(&ctx, protocol).await;

                assert!(con.hotkeys_start(metric.options()).await.is_ok());
                setup_test_keys_and_make_hot_keys(&mut con).await;

                let result = con.hotkeys_get().await.unwrap().unwrap();
                assert!(result.tracking_active);
                assert_eq!(result.sample_ratio, 1);
                assert!(result.collection_duration_ms > 0);

                let expect_cpu = matches!(metric, Metric::Cpu);
                let expect_net = matches!(metric, Metric::Net);
                let expect_all = matches!(metric, Metric::All);

                assert_eq!(result.by_cpu_time_us.is_some(), expect_cpu || expect_all);
                assert_eq!(result.by_net_bytes.is_some(), expect_net || expect_all);
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_hotkeys_options_with_duration_and_count_async(
        #[case] runtime: RuntimeType,
        #[values(ProtocolVersion::RESP2, ProtocolVersion::RESP3)] protocol: ProtocolVersion,
    ) {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_6);

        println!(
            "Starting test_hotkeys_options_with_duration_and_count_async - Protocol: {protocol:?}"
        );

        block_on_all(
            async move {
                let mut con = setup_async_connection_with_protocol(&ctx, protocol).await;

                assert!(
                    con.hotkeys_start(
                        HotkeysOptions::new_with_cpu()
                            .with_count(2)
                            .unwrap()
                            .with_duration_secs(2),
                    )
                    .await
                    .is_ok()
                );
                setup_test_keys_and_make_hot_keys(&mut con).await;
                let result = con.hotkeys_get().await.unwrap().unwrap();
                assert!(result.tracking_active);
                assert!(result.by_cpu_time_us.is_some());
                assert_eq!(result.by_cpu_time_us.unwrap().len(), 2);

                futures_time::task::sleep(Duration::from_secs(3).into()).await;

                let result = con.hotkeys_get().await.unwrap().unwrap();
                assert!(!result.tracking_active);
                assert!(result.by_cpu_time_us.is_some());
                assert_eq!(result.by_cpu_time_us.unwrap().len(), 2);
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_hotkeys_options_with_sample_ratio_async(
        #[case] runtime: RuntimeType,
        #[values(ProtocolVersion::RESP2, ProtocolVersion::RESP3)] protocol: ProtocolVersion,
    ) {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_6);

        println!("Starting test_hotkeys_options_with_sample_ratio_async - Protocol: {protocol:?}");

        block_on_all(
            async move {
                let mut con = setup_async_connection_with_protocol(&ctx, protocol).await;

                const SAMPLE_RATIO: u64 = 100;
                assert!(
                    con.hotkeys_start(
                        HotkeysOptions::new_with_cpu().with_sample_ratio(SAMPLE_RATIO)
                    )
                    .await
                    .is_ok()
                );

                let result = con.hotkeys_get().await.unwrap().unwrap();
                assert!(result.tracking_active);
                assert_eq!(result.sample_ratio, SAMPLE_RATIO);
            },
            runtime,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "tokio-comp", case::tokio(RuntimeType::Tokio))]
    #[cfg_attr(feature = "smol-comp", case::smol(RuntimeType::Smol))]
    fn test_hotkeys_start_with_slots_on_standalone_errors_async(
        #[case] runtime: RuntimeType,
        #[values(ProtocolVersion::RESP2, ProtocolVersion::RESP3)] protocol: ProtocolVersion,
    ) {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_6);

        println!(
            "Starting test_hotkeys_start_with_slots_on_standalone_errors_async - \
             Protocol: {protocol:?}"
        );

        block_on_all(
            async move {
                let mut con = setup_async_connection_with_protocol(&ctx, protocol).await;

                let err = con
                    .hotkeys_start(HotkeysOptions::new_with_cpu().with_slots(vec![100]))
                    .await
                    .expect_err("SLOTS on a standalone server must be rejected");
                let msg = err.to_string();
                assert!(
                    msg.contains("SLOTS parameter cannot be used in non-cluster mode"),
                    "unexpected error for SLOTS on standalone: {msg}",
                );

                assert!(con.hotkeys_get().await.unwrap().is_none());
            },
            runtime,
        );
    }
}
