#![cfg(feature = "cluster-async")]
mod support;

#[cfg(test)]
mod test_cluster_scan_async {
    use crate::support::*;
    use rand::Rng;
    use redis::cluster_routing::{RoutingInfo, SingleNodeRoutingInfo};
    use redis::{cmd, from_redis_value, ObjectType, RedisResult, ScanStateRC, Value};

    async fn kill_one_node(
        cluster: &TestClusterContext,
        slot_distribution: Vec<(String, String, String, Vec<Vec<u16>>)>,
    ) -> RoutingInfo {
        let mut cluster_conn = cluster.async_connection(None).await;
        let distribution_clone = slot_distribution.clone();
        let index_of_random_node = rand::thread_rng().gen_range(0..slot_distribution.len());
        let random_node = distribution_clone.get(index_of_random_node).unwrap();
        let random_node_route_info = RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
            host: random_node.1.clone(),
            port: random_node.2.parse::<u16>().unwrap(),
        });
        let random_node_id = &random_node.0;
        // Create connections to all nodes
        for node in &distribution_clone {
            if random_node_id == &node.0 {
                continue;
            }
            let node_route = RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                host: node.1.clone(),
                port: node.2.parse::<u16>().unwrap(),
            });

            let mut forget_cmd = cmd("CLUSTER");
            forget_cmd.arg("FORGET").arg(random_node_id);
            let _: RedisResult<Value> = cluster_conn
                .route_command(&forget_cmd, node_route.clone())
                .await;
        }
        let mut shutdown_cmd = cmd("SHUTDOWN");
        shutdown_cmd.arg("NOSAVE");
        let _: RedisResult<Value> = cluster_conn
            .route_command(&shutdown_cmd, random_node_route_info.clone())
            .await;
        random_node_route_info
    }

    #[tokio::test]
    async fn test_async_cluster_scan() {
        let cluster = TestClusterContext::new();
        let mut connection = cluster.async_connection(None).await;

        // Set some keys
        for i in 0..10 {
            let key = format!("key{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
        }

        // Scan the keys
        let mut scan_state_rc = ScanStateRC::new();
        let mut keys: Vec<String> = vec![];
        loop {
            let (next_cursor, scan_keys): (ScanStateRC, Vec<Value>) = connection
                .cluster_scan(scan_state_rc, None, None, None)
                .await
                .unwrap();
            scan_state_rc = next_cursor;
            let mut scan_keys = scan_keys
                .into_iter()
                .map(|v| from_redis_value(&v).unwrap())
                .collect::<Vec<String>>(); // Change the type of `keys` to `Vec<String>`
            keys.append(&mut scan_keys);
            if scan_state_rc.is_finished() {
                break;
            }
        }
        // Check if all keys were scanned
        keys.sort();
        keys.dedup();
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(key.to_owned(), format!("key{}", i));
        }
    }

    #[tokio::test] // test cluster scan with slot migration in the middle
    async fn test_async_cluster_scan_with_migration() {
        let cluster = TestClusterContext::new();

        let mut connection = cluster.async_connection(None).await;
        // Set some keys
        let mut expected_keys: Vec<String> = Vec::new();

        for i in 0..1000 {
            let key = format!("key{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            expected_keys.push(key);
        }

        // Scan the keys
        let mut scan_state_rc = ScanStateRC::new();
        let mut keys: Vec<String> = Vec::new();
        let mut count = 0;
        loop {
            count += 1;
            let (next_cursor, scan_keys): (ScanStateRC, Vec<Value>) = connection
                .cluster_scan(scan_state_rc, None, None, None)
                .await
                .unwrap();
            scan_state_rc = next_cursor;
            let scan_keys = scan_keys
                .into_iter()
                .map(|v| from_redis_value(&v).unwrap())
                .collect::<Vec<String>>();
            keys.extend(scan_keys);
            if scan_state_rc.is_finished() {
                break;
            }
            if count == 5 {
                let mut cluster_nodes = cluster.get_cluster_nodes().await;
                let slot_distribution = cluster.get_slots_ranges_distribution(&cluster_nodes);
                cluster
                    .migrate_slots_from_node_to_another(slot_distribution.clone())
                    .await;
                for node in &slot_distribution {
                    let ready = cluster
                        .wait_for_connection_is_ready(&RoutingInfo::SingleNode(
                            SingleNodeRoutingInfo::ByAddress {
                                host: node.1.clone(),
                                port: node.2.parse::<u16>().unwrap(),
                            },
                        ))
                        .await;
                    match ready {
                        Ok(_) => {}
                        Err(e) => {
                            println!("error: {:?}", e);
                            break;
                        }
                    }
                }

                cluster_nodes = cluster.get_cluster_nodes().await;
                // Compare slot distribution before and after migration
                let new_slot_distribution = cluster.get_slots_ranges_distribution(&cluster_nodes);
                assert_ne!(slot_distribution, new_slot_distribution);
            }
        }
        keys.sort();
        keys.dedup();
        expected_keys.sort();
        expected_keys.dedup();
        // check if all keys were scanned
        assert_eq!(keys, expected_keys);
    }

    #[tokio::test] // test cluster scan with node fail in the middle
    async fn test_async_cluster_scan_with_fail() {
        let cluster = TestClusterContext::new();
        let mut connection = cluster.async_connection(None).await;
        // Set some keys
        for i in 0..1000 {
            let key = format!("key{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
        }

        // Scan the keys
        let mut scan_state_rc = ScanStateRC::new();
        let mut keys: Vec<String> = Vec::new();
        let mut count = 0;
        let mut result: RedisResult<Value> = Ok(Value::Nil);
        loop {
            count += 1;
            let scan_response: RedisResult<(ScanStateRC, Vec<Value>)> = connection
                .cluster_scan(scan_state_rc, None, None, None)
                .await;
            let (next_cursor, scan_keys) = match scan_response {
                Ok((cursor, keys)) => (cursor, keys),
                Err(e) => {
                    result = Err(e);
                    break;
                }
            };
            scan_state_rc = next_cursor;
            keys.extend(scan_keys.into_iter().map(|v| from_redis_value(&v).unwrap()));
            if scan_state_rc.is_finished() {
                break;
            }
            if count == 5 {
                let cluster_nodes = cluster.get_cluster_nodes().await;
                let slot_distribution = cluster.get_slots_ranges_distribution(&cluster_nodes);
                // simulate node failure
                let killed_node_routing = kill_one_node(&cluster, slot_distribution.clone()).await;
                let ready = cluster.wait_for_fail_to_finish(&killed_node_routing).await;
                match ready {
                    Ok(_) => {}
                    Err(e) => {
                        println!("error: {:?}", e);
                        break;
                    }
                }
                let cluster_nodes = cluster.get_cluster_nodes().await;
                let new_slot_distribution = cluster.get_slots_ranges_distribution(&cluster_nodes);
                assert_ne!(slot_distribution, new_slot_distribution);
            }
        }

        // We expect an error of finding address covering slot or not all slots are covered
        // Both errors message contains "please check the cluster configuration"
        let res = result
            .unwrap_err()
            .to_string()
            .contains("please check the cluster configuration");
        assert!(res);
    }

    #[tokio::test] // Test cluster scan with killing all masters during scan
    async fn test_async_cluster_scan_with_all_masters_down() {
        let cluster = TestClusterContext::new_with_config(RedisClusterConfiguration {
            nodes: 6,
            replicas: 1,
            ..Default::default()
        });
        let mut connection = cluster.async_connection(None).await;

        let mut expected_keys: Vec<String> = Vec::new();

        cluster.wait_for_cluster_up();

        let mut cluster_nodes = cluster.get_cluster_nodes().await;

        let slot_distribution = cluster.get_slots_ranges_distribution(&cluster_nodes);
        let masters = cluster.get_masters(&cluster_nodes).await;
        let replicas = cluster.get_replicas(&cluster_nodes).await;

        for i in 0..1000 {
            let key = format!("key{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            expected_keys.push(key);
        }
        // Scan the keys
        let mut scan_state_rc = ScanStateRC::new();
        let mut keys: Vec<String> = Vec::new();
        let mut count = 0;
        loop {
            count += 1;
            let scan_response: RedisResult<(ScanStateRC, Vec<Value>)> = connection
                .cluster_scan(scan_state_rc, None, None, None)
                .await;
            if scan_response.is_err() {
                println!("error: {:?}", scan_response);
            }
            let (next_cursor, scan_keys) = scan_response.unwrap();
            scan_state_rc = next_cursor;
            keys.extend(scan_keys.into_iter().map(|v| from_redis_value(&v).unwrap()));
            if scan_state_rc.is_finished() {
                break;
            }
            if count == 5 {
                for replica in replicas.iter() {
                    let mut failover_cmd = cmd("CLUSTER");
                    let _: RedisResult<Value> = connection
                        .route_command(
                            failover_cmd.arg("FAILOVER").arg("TAKEOVER"),
                            RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                                host: replica[1].clone(),
                                port: replica[2].parse::<u16>().unwrap(),
                            }),
                        )
                        .await;
                    let ready = cluster
                        .wait_for_connection_is_ready(&RoutingInfo::SingleNode(
                            SingleNodeRoutingInfo::ByAddress {
                                host: replica[1].clone(),
                                port: replica[2].parse::<u16>().unwrap(),
                            },
                        ))
                        .await;
                    match ready {
                        Ok(_) => {}
                        Err(e) => {
                            println!("error: {:?}", e);
                            break;
                        }
                    }
                }

                for master in masters.iter() {
                    for replica in replicas.clone() {
                        let mut forget_cmd = cmd("CLUSTER");
                        forget_cmd.arg("FORGET").arg(master[0].clone());
                        let _: RedisResult<Value> = connection
                            .route_command(
                                &forget_cmd,
                                RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                                    host: replica[1].clone(),
                                    port: replica[2].parse::<u16>().unwrap(),
                                }),
                            )
                            .await;
                    }
                }
                for master in masters.iter() {
                    let mut shut_cmd = cmd("SHUTDOWN");
                    shut_cmd.arg("NOSAVE");
                    let _ = connection
                        .route_command(
                            &shut_cmd,
                            RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                                host: master[1].clone(),
                                port: master[2].parse::<u16>().unwrap(),
                            }),
                        )
                        .await;
                    let ready = cluster
                        .wait_for_fail_to_finish(&RoutingInfo::SingleNode(
                            SingleNodeRoutingInfo::ByAddress {
                                host: master[1].clone(),
                                port: master[2].parse::<u16>().unwrap(),
                            },
                        ))
                        .await;
                    match ready {
                        Ok(_) => {}
                        Err(e) => {
                            println!("error: {:?}", e);
                            break;
                        }
                    }
                }
                for replica in replicas.iter() {
                    let ready = cluster
                        .wait_for_connection_is_ready(&RoutingInfo::SingleNode(
                            SingleNodeRoutingInfo::ByAddress {
                                host: replica[1].clone(),
                                port: replica[2].parse::<u16>().unwrap(),
                            },
                        ))
                        .await;
                    match ready {
                        Ok(_) => {}
                        Err(e) => {
                            println!("error: {:?}", e);
                            break;
                        }
                    }
                }
                cluster_nodes = cluster.get_cluster_nodes().await;
                let new_slot_distribution = cluster.get_slots_ranges_distribution(&cluster_nodes);
                assert_ne!(slot_distribution, new_slot_distribution);
            }
        }
        keys.sort();
        keys.dedup();
        expected_keys.sort();
        expected_keys.dedup();
        // check if all keys were scanned
        assert_eq!(keys, expected_keys);
    }

    #[tokio::test]
    // Test cluster scan with killing all replicas during scan
    async fn test_async_cluster_scan_with_all_replicas_down() {
        let cluster = TestClusterContext::new_with_config(RedisClusterConfiguration {
            nodes: 6,
            replicas: 1,
            ..Default::default()
        });
        let mut connection = cluster.async_connection(None).await;

        let mut expected_keys: Vec<String> = Vec::new();

        for server in cluster.cluster.servers.iter() {
            let address = server.addr.clone().to_string();
            let host_and_port = address.split(':');
            let host = host_and_port.clone().next().unwrap().to_string();
            let port = host_and_port
                .clone()
                .last()
                .unwrap()
                .parse::<u16>()
                .unwrap();
            let ready = cluster
                .wait_for_connection_is_ready(&RoutingInfo::SingleNode(
                    SingleNodeRoutingInfo::ByAddress { host, port },
                ))
                .await;
            match ready {
                Ok(_) => {}
                Err(e) => {
                    println!("error: {:?}", e);
                    break;
                }
            }
        }

        let cluster_nodes = cluster.get_cluster_nodes().await;

        let replicas = cluster.get_replicas(&cluster_nodes).await;

        for i in 0..1000 {
            let key = format!("key{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            expected_keys.push(key);
        }
        // Scan the keys
        let mut scan_state_rc = ScanStateRC::new();
        let mut keys: Vec<String> = Vec::new();
        let mut count = 0;
        loop {
            count += 1;
            let scan_response: RedisResult<(ScanStateRC, Vec<Value>)> = connection
                .cluster_scan(scan_state_rc, None, None, None)
                .await;
            if scan_response.is_err() {
                println!("error: {:?}", scan_response);
            }
            let (next_cursor, scan_keys) = scan_response.unwrap();
            scan_state_rc = next_cursor;
            keys.extend(scan_keys.into_iter().map(|v| from_redis_value(&v).unwrap()));
            if scan_state_rc.is_finished() {
                break;
            }
            if count == 5 {
                for replica in replicas.iter() {
                    let mut shut_cmd = cmd("SHUTDOWN");
                    shut_cmd.arg("NOSAVE");
                    let ready: RedisResult<Value> = connection
                        .route_command(
                            &shut_cmd,
                            RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                                host: replica[1].clone(),
                                port: replica[2].parse::<u16>().unwrap(),
                            }),
                        )
                        .await;
                    match ready {
                        Ok(_) => {}
                        Err(e) => {
                            println!("error: {:?}", e);
                            break;
                        }
                    }
                }
                let new_cluster_nodes = cluster.get_cluster_nodes().await;
                assert_ne!(cluster_nodes, new_cluster_nodes);
            }
        }
        keys.sort();
        keys.dedup();
        expected_keys.sort();
        expected_keys.dedup();
        // check if all keys were scanned
        assert_eq!(keys, expected_keys);
    }
    #[tokio::test]
    // Test cluster scan with setting keys for each iteration
    async fn test_async_cluster_scan_set_in_the_middle() {
        let cluster = TestClusterContext::new();
        let mut connection = cluster.async_connection(None).await;
        let mut expected_keys: Vec<String> = Vec::new();
        let mut i = 0;
        // Set some keys
        loop {
            let key = format!("key{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            expected_keys.push(key);
            i += 1;
            if i == 1000 {
                break;
            }
        }
        // Scan the keys
        let mut scan_state_rc = ScanStateRC::new();
        let mut keys: Vec<String> = vec![];
        loop {
            let (next_cursor, scan_keys): (ScanStateRC, Vec<Value>) = connection
                .cluster_scan(scan_state_rc, None, None, None)
                .await
                .unwrap();
            scan_state_rc = next_cursor;
            let mut scan_keys = scan_keys
                .into_iter()
                .map(|v| from_redis_value(&v).unwrap())
                .collect::<Vec<String>>(); // Change the type of `keys` to `Vec<String>`
            keys.append(&mut scan_keys);
            if scan_state_rc.is_finished() {
                break;
            }
            let key = format!("key{}", i);
            i += 1;
            let res: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            assert!(res.is_ok());
        }
        // Check if all keys were scanned
        keys.sort();
        keys.dedup();
        expected_keys.sort();
        expected_keys.dedup();
        // check if all keys were scanned
        for key in expected_keys.iter() {
            assert!(keys.contains(key));
        }
        assert!(keys.len() >= expected_keys.len());
    }

    #[tokio::test]
    // Test cluster scan with deleting keys for each iteration
    async fn test_async_cluster_scan_dell_in_the_middle() {
        let cluster = TestClusterContext::new();

        let mut connection = cluster.async_connection(None).await;
        let mut expected_keys: Vec<String> = Vec::new();
        let mut i = 0;
        // Set some keys
        loop {
            let key = format!("key{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            expected_keys.push(key);
            i += 1;
            if i == 1000 {
                break;
            }
        }
        // Scan the keys
        let mut scan_state_rc = ScanStateRC::new();
        let mut keys: Vec<String> = vec![];
        loop {
            let (next_cursor, scan_keys): (ScanStateRC, Vec<Value>) = connection
                .cluster_scan(scan_state_rc, None, None, None)
                .await
                .unwrap();
            scan_state_rc = next_cursor;
            let mut scan_keys = scan_keys
                .into_iter()
                .map(|v| from_redis_value(&v).unwrap())
                .collect::<Vec<String>>(); // Change the type of `keys` to `Vec<String>`
            keys.append(&mut scan_keys);
            if scan_state_rc.is_finished() {
                break;
            }
            i -= 1;
            let key = format!("key{}", i);

            let res: Result<(), redis::RedisError> = redis::cmd("del")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            assert!(res.is_ok());
            expected_keys.remove(i as usize);
        }
        // Check if all keys were scanned
        keys.sort();
        keys.dedup();
        expected_keys.sort();
        expected_keys.dedup();
        // check if all keys were scanned
        for key in expected_keys.iter() {
            assert!(keys.contains(key));
        }
        assert!(keys.len() >= expected_keys.len());
    }

    #[tokio::test]
    // Testing cluster scan with Pattern option
    async fn test_async_cluster_scan_with_pattern() {
        let cluster = TestClusterContext::new();
        let mut connection = cluster.async_connection(None).await;
        let mut expected_keys: Vec<String> = Vec::new();
        let mut i = 0;
        // Set some keys
        loop {
            let key = format!("key:pattern:{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            expected_keys.push(key);
            let non_relevant_key = format!("key{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&non_relevant_key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            i += 1;
            if i == 500 {
                break;
            }
        }

        // Scan the keys
        let mut scan_state_rc = ScanStateRC::new();
        let mut keys: Vec<String> = vec![];
        loop {
            let (next_cursor, scan_keys): (ScanStateRC, Vec<Value>) = connection
                .cluster_scan(scan_state_rc, Some("key:pattern:*"), None, None)
                .await
                .unwrap();
            scan_state_rc = next_cursor;
            let mut scan_keys = scan_keys
                .into_iter()
                .map(|v| from_redis_value(&v).unwrap())
                .collect::<Vec<String>>(); // Change the type of `keys` to `Vec<String>`
            keys.append(&mut scan_keys);
            if scan_state_rc.is_finished() {
                break;
            }
        }
        // Check if all keys were scanned
        keys.sort();
        keys.dedup();
        expected_keys.sort();
        expected_keys.dedup();
        // check if all keys were scanned
        for key in expected_keys.iter() {
            assert!(keys.contains(key));
        }
        assert!(keys.len() == expected_keys.len());
    }

    #[tokio::test]
    // Testing cluster scan with TYPE option
    async fn test_async_cluster_scan_with_type() {
        let cluster = TestClusterContext::new();
        let mut connection = cluster.async_connection(None).await;
        let mut expected_keys: Vec<String> = Vec::new();
        let mut i = 0;
        // Set some keys
        loop {
            let key = format!("key{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SADD")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            expected_keys.push(key);
            let key = format!("key-that-is-not-set{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            i += 1;
            if i == 500 {
                break;
            }
        }

        // Scan the keys
        let mut scan_state_rc = ScanStateRC::new();
        let mut keys: Vec<String> = vec![];
        loop {
            let (next_cursor, scan_keys): (ScanStateRC, Vec<Value>) = connection
                .cluster_scan(scan_state_rc, None, None, Some(ObjectType::Set))
                .await
                .unwrap();
            scan_state_rc = next_cursor;
            let mut scan_keys = scan_keys
                .into_iter()
                .map(|v| from_redis_value(&v).unwrap())
                .collect::<Vec<String>>(); // Change the type of `keys` to `Vec<String>`
            keys.append(&mut scan_keys);
            if scan_state_rc.is_finished() {
                break;
            }
        }
        // Check if all keys were scanned
        keys.sort();
        keys.dedup();
        expected_keys.sort();
        expected_keys.dedup();
        // check if all keys were scanned
        for key in expected_keys.iter() {
            assert!(keys.contains(key));
        }
        assert!(keys.len() == expected_keys.len());
    }

    #[tokio::test]
    // Testing cluster scan with COUNT option
    async fn test_async_cluster_scan_with_count() {
        let cluster = TestClusterContext::new();
        let mut connection = cluster.async_connection(None).await;
        let mut expected_keys: Vec<String> = Vec::new();
        let mut i = 0;
        // Set some keys
        loop {
            let key = format!("key{}", i);
            let _: Result<(), redis::RedisError> = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut connection)
                .await;
            expected_keys.push(key);
            i += 1;
            if i == 1000 {
                break;
            }
        }

        // Scan the keys
        let mut scan_state_rc = ScanStateRC::new();
        let mut keys: Vec<String> = vec![];
        let mut comparing_times = 0;
        loop {
            let (next_cursor, scan_keys): (ScanStateRC, Vec<Value>) = connection
                .cluster_scan(scan_state_rc.clone(), None, Some(100), None)
                .await
                .unwrap();
            let (_, scan_without_count_keys): (ScanStateRC, Vec<Value>) = connection
                .cluster_scan(scan_state_rc, None, Some(100), None)
                .await
                .unwrap();
            if !scan_keys.is_empty() && !scan_without_count_keys.is_empty() {
                assert!(scan_keys.len() >= scan_without_count_keys.len());

                comparing_times += 1;
            }
            scan_state_rc = next_cursor;
            let mut scan_keys = scan_keys
                .into_iter()
                .map(|v| from_redis_value(&v).unwrap())
                .collect::<Vec<String>>(); // Change the type of `keys` to `Vec<String>`
            keys.append(&mut scan_keys);
            if scan_state_rc.is_finished() {
                break;
            }
        }
        assert!(comparing_times > 0);
        // Check if all keys were scanned
        keys.sort();
        keys.dedup();
        expected_keys.sort();
        expected_keys.dedup();
        // check if all keys were scanned
        for key in expected_keys.iter() {
            assert!(keys.contains(key));
        }
        assert!(keys.len() == expected_keys.len());
    }
}
