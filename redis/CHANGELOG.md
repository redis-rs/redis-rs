### 0.31.0 (2025-05-08)

#### Changes & Bug fixes

* Document variadic tuple implementations for FromRedisValue and ToRedisValue ([#1617](https://github.com/redis-rs/redis-rs/pull/1617) by @Kriskras99)
* Add an optional bytes::BufMut function to RedisWrite ([#1618](https://github.com/redis-rs/redis-rs/pull/1618) by @Kriskras99)
* Reorganize flag-frenzy comnfiguration ([#1620](https://github.com/redis-rs/redis-rs/pull/1620) by @nihohit)
* Make `reserve_space_for_args` take a `IntoIterator` ([#1622](https://github.com/redis-rs/redis-rs/pull/1622) by @Kriskras99)
* Improve Send/Sync checks. ([#1623](https://github.com/redis-rs/redis-rs/pull/1623) by @nihohit)
* **BREAKING CHANGE** Remove the `ring` dependency from `rustls`. ([#1632](https://github.com/redis-rs/redis-rs/pull/1632) by @nihohit)
* Implement Error::source for the RedisError struct ([#1634](https://github.com/redis-rs/redis-rs/pull/1634) by @StefanPalashev)
* ConnectionManager: Reconnect when RESP3 is set. ([#1612](https://github.com/redis-rs/redis-rs/pull/1612) by @nihohit)

### 0.30.0 (2025-04-22)

#### Changes & Bug fixes

* Add epoch for CacheManager ([#1583](https://github.com/redis-rs/redis-rs/pull/1583) by @altanozlu)
* **Breaking change** Add support for the Smol runtime. ([#1606](https://github.com/redis-rs/redis-rs/pull/1606) by @nihohit)
* Add support for hash field expiration commands ([#1611](https://github.com/redis-rs/redis-rs/pull/1611) by @StefanPalashev)
* **Breaking change** Remove deprecated aio::Connection. ([#1613](https://github.com/redis-rs/redis-rs/pull/1613) by @nihohit)

#### Documentation & CI improvements

* Reduce number of flag-frenzy checks and format file. ([#1608](https://github.com/redis-rs/redis-rs/pull/1608) by @nihohit)
* Fix `make docs` ([#1607](https://github.com/redis-rs/redis-rs/pull/1607) by @somechris)
* Fail CI on warnings in docs. ([#1609](https://github.com/redis-rs/redis-rs/pull/1609) by @nihohit)

### 0.29.5 (2025-04-06)

#### Changes & Bug fixes

* Fix build on Windows. ([#1601](https://github.com/redis-rs/redis-rs/pull/1601) by @nihohit)

### 0.29.4 (2025-04-06)

#### Changes & Bug fixes

* Add async dns resolver for cluster config and fix doc ([#1595](https://github.com/redis-rs/redis-rs/pull/1595) by @wiserfz)
* Fix error kind declaration on non-unix machines. ([#1598](https://github.com/redis-rs/redis-rs/pull/1598) by @nihohit)


### 0.29.3 (2025-04-04)

#### Changes & Bug fixes

* re-export socket2. ([#1573](https://github.com/redis-rs/redis-rs/pull/1573) by @nihohit)
* Add commands to flush database(s) ([#1576](https://github.com/redis-rs/redis-rs/pull/1576) by @somechris)
* Fix valkey(s) url schemes not able to be converted to connection infos; Add valkey+unix url scheme ([#1574](https://github.com/redis-rs/redis-rs/pull/1574) by @MarkusTieger)
* A spec compliant version, with less changed code. ([#1572](https://github.com/redis-rs/redis-rs/pull/1572) by @65001)
* Support bb8 for cluster client ([#1577](https://github.com/redis-rs/redis-rs/pull/1577) by @wiserfz)
* Support custom DNS resolver for async client ([#1581](https://github.com/redis-rs/redis-rs/pull/1581) by @wiserfz)
* Update danger_accept_invalid_hostnames for rustls 0.23.24 ([#1592](https://github.com/redis-rs/redis-rs/pull/1592) by @jorendorff)

### 0.29.2 (2025-03-21)

#### Changes & Bug fixes

* Add Valkey URL scheme ([#1558](https://github.com/redis-rs/redis-rs/pull/1558) by @displexic)
* Fix unreachable error when parsing a nested tuple.  ([#1562](https://github.com/redis-rs/redis-rs/pull/1562) by @nihohit)
* Remove PFCOUNT and PFMERGE from the list of illegal cluster pipeline commands (#1565) ([#1566](https://github.com/redis-rs/redis-rs/pull/1566) by @stepanmracek)
* Remove EVALSHA from the list of illegal cluster pipeline commands ([#1568](https://github.com/redis-rs/redis-rs/pull/1568) by @stepanmracek)
* feat: Add bb8 support for async client ([#1564](https://github.com/redis-rs/redis-rs/pull/1564) by @Xuanwo)
* Add cache support to ConnectionManager. ([#1567](https://github.com/redis-rs/redis-rs/pull/1567) by @nihohit)
* perf: Run reconnection attempts concurrently ([#1557](https://github.com/redis-rs/redis-rs/pull/1557) by @Marwes)

### 0.29.1 (2025-03-01)

#### Changes & Bug fixes

* Update rustls-native-certs. ([#1498](https://github.com/redis-rs/redis-rs/pull/1498))
* Async cluster connection: Move response timeout out.  ([#1532](https://github.com/redis-rs/redis-rs/pull/1532))
* Expose `Pipeline.len()` function as public ([#1539](https://github.com/redis-rs/redis-rs/pull/1539) by @Harry-Lees)
* Implement `danger_accept_invalid_hostnames` option. ([#1529](https://github.com/redis-rs/redis-rs/pull/1529) by @jorendorff)
* Timeout on queuing requests. ([#1552](https://github.com/redis-rs/redis-rs/pull/1552))

#### Documentation improvements

* docs: Fix double quotes ([#1537](https://github.com/redis-rs/redis-rs/pull/1537) by @somechris)
* docs: added CLIENT SETINFO optional feature ([#1536](https://github.com/redis-rs/redis-rs/pull/1536) by @bourdeau)

#### CI improvements

* Run most cluster tests in secure mode when rustls is enabled ([#1534](https://github.com/redis-rs/redis-rs/pull/1534) by @jorendorff)

### 0.29.0 (2025-02-16)

#### Changes & Bug fixes

* Tweaks to rustls usage ([#1499](https://github.com/redis-rs/redis-rs/pull/1499) by @djc)
* Add client side caching support for MultiplexedConnection ([#1296](https://github.com/redis-rs/redis-rs/pull/1296) by @altanozlu)
* Add buffered write methods to RedisWrite ([#905](https://github.com/redis-rs/redis-rs/pull/905) by @swwu)
* Include the reason for one connection failure if cluster connect fails ([#1497](https://github.com/redis-rs/redis-rs/pull/1497) by @Marwes)
* Upgrade to rand 0.9 ([#1525](https://github.com/redis-rs/redis-rs/pull/1525) by @gkorland)
* Allow configuring Sentinel with custom TLS certificates ([#1335](https://github.com/redis-rs/redis-rs/pull/1335) by @ergonjomeier)
* Fix caching with the JSON module ([#1520](https://github.com/redis-rs/redis-rs/pull/1520) by @kudlatyamroth)
* Allow users of async connections to set TCP settings. ([#1523](https://github.com/redis-rs/redis-rs/pull/1523))

#### Documentation improvements

* Clarify build instructions in README. ([#1515](https://github.com/redis-rs/redis-rs/pull/1515))
* Improve pubsub docs. ([#1519](https://github.com/redis-rs/redis-rs/pull/1519))
* Improve docs around TTLs/ expiry times. ([#1522](https://github.com/redis-rs/redis-rs/pull/1522) by @clbarnes)

### 0.28.2 (2025-01-24)

#### Changes & Bug fixes

* feat(cluster): add `get(_async)?_connection_with_config` ([#1487](https://github\.com/redis-rs/redis-rs/pull/1487) by @Totodore) 
* Fix connection creation timeout. ([#1490](https://github\.com/redis-rs/redis-rs/pull/1490)) 
* Fix RetryMethod Import ([#1494](https://github\.com/redis-rs/redis-rs/pull/1494) by @Braedon-Wooding-Displayr) 

### 0.28.1 (2025-01-11)

#### Changes & Bug fixes

* fix changelogs. ([#1473](https://github.com/redis-rs/redis-rs/pull/1473))
* Remove itertools dependency ([#1476](https://github.com/redis-rs/redis-rs/pull/1476) by @jplatte)
* Clean up feature dependencies ([#1474](https://github.com/redis-rs/redis-rs/pull/1474) by @jplatte)
* Fix new version lints ([#1479](https://github.com/redis-rs/redis-rs/pull/1479))
* Fix routing for `PUBSUB SHARDNUMSUB` ([#1483](https://github.com/redis-rs/redis-rs/pull/1483))

#### CI improvements

### 0.28.0 (2025-01-7)
* Double test_async_cluster_basic_failover timeout. ([#1478](https://github.com/redis-rs/redis-rs/pull/1478))
* Upgrade rstest ([#1475](https://github.com/redis-rs/redis-rs/pull/1475) by @jplatte)

#### Changes & Bug fixes

* ConnectionManager: reconnect on disconnect pushes ([#1407](https://github.com/redis-rs/redis-rs/pull/1407))
* ConnectionManager: Support automatic resubscription ([#1408](https://github.com/redis-rs/redis-rs/pull/1408))
* Remove async_trait dependency ([#1451](https://github.com/redis-rs/redis-rs/pull/1451))
* Expose RetryMethod to consumers ([#1454](https://github.com/redis-rs/redis-rs/pull/1454) @Braedon-Wooding-Displayr)
* Add ping support to commands & pubsubs ([#1341](https://github.com/redis-rs/redis-rs/pull/1341))
* Expose server creation logic through redis-test ([#1363](https://github.com/redis-rs/redis-rs/pull/1363))
* Depend only on the required sub-crates of futures ([#1465](https://github.com/redis-rs/redis-rs/pull/1465) @jplatte)
* **Breaking change**: Mark more `Connection` functions as deprecated ([#1468](https://github.com/redis-rs/redis-rs/pull/1468))
* Move repeated logic to function ([#1469](https://github.com/redis-rs/redis-rs/pull/1469))

#### Documentation improvements

* Update README to use rustls ([#1397](https://github.com/redis-rs/redis-rs/pull/1397) @khacminh)
* Update README.md regarding cargo-nextest ([#1445](https://github.com/redis-rs/redis-rs/pull/1445) @altanozlu)
* Improve features and TLS docs ([#1464](https://github.com/redis-rs/redis-rs/pull/1464))
* Mention Valkey in Readme ([#1467](https://github.com/redis-rs/redis-rs/pull/1467))
* Fix automatic resubscription docs ([#1450](https://github.com/redis-rs/redis-rs/pull/1450))
* Replace `get_multiplexed_tokio_connection` in examples & tests ([#1443](https://github.com/redis-rs/redis-rs/pull/1443))

#### CI improvements

* Run tests concurrently ([#1444](https://github.com/redis-rs/redis-rs/pull/1444))
* Report slow tests more eagerly ([#1441](https://github.com/redis-rs/redis-rs/pull/1441))
* Improve testing of optional features ([#1448](https://github.com/redis-rs/redis-rs/pull/1448))

### 0.27.6 (2024-12-3)

#### Changes & Bug fixes

* Use runtime agnostic retry mechanism. ([#1392](https://github.com/redis-rs/redis-rs/pull/1392))
* Choose sleep function according to active runtime. ([#1369](https://github.com/redis-rs/redis-rs/pull/1369))
* MpxConnection: Allow pipelines to continue after error. ([#1293](https://github.com/redis-rs/redis-rs/pull/1293))
* Async cluster - Do not retry requests that have been dropped by the user. ([#1318](https://github.com/redis-rs/redis-rs/pull/1318))
* Sync cluster - reconnect after complete disconnect. ([#1233](https://github.com/redis-rs/redis-rs/pull/1233))
* feat: Add high level type for ROLE command ([#1321](https://github.com/redis-rs/redis-rs/pull/1321) @DCjanus)
* impl CLIENT ID ([#1382](https://github.com/redis-rs/redis-rs/pull/1382) @ArtemIsmagilov)
* impl CLIENT SETNAME ([#1381](https://github.com/redis-rs/redis-rs/pull/1381) @ArtemIsmagilov)
* Async push messages: Allow for more channel types. ([#1295]https://github.com/redis-rs/redis-rs/pull/1295)

#### Documentation improvements

* Add docs clarifying connection pool usage. ([#1411](https://github.com/redis-rs/redis-rs/pull/1411))
* Describe as "for Redis" ([#1423](https://github.com/redis-rs/redis-rs/pull/1423) @zuiderkwast)
* Clarify RESP3 subscription documentation. ([#1436](https://github.com/redis-rs/redis-rs/pull/1436))
* Add keywords to increase crate visibility. ([#1425](https://github.com/redis-rs/redis-rs/pull/1425))
* Improve docs. ([#1339](https://github.com/redis-rs/redis-rs/pull/1339))
* Add doc clarifying behavior of invoke_script. ([#1396](https://github.com/redis-rs/redis-rs/pull/1396))

#### CI improvements

* Use cargo-nextest for testing. ([#1336](https://github.com/redis-rs/redis-rs/pull/1336))
* test in CI against valkey 8. ([#1362](https://github.com/redis-rs/redis-rs/pull/1362))
* Fix new lints. ([#1426](https://github.com/redis-rs/redis-rs/pull/1426))
* Fix test_block_on_all_panics_from_spawns consistently failing on async_std. ([#1368](https://github.com/redis-rs/redis-rs/pull/1368))

### 0.27.5 (2024-10-18)

#### Changes & Bug fixes

* Allow disabling timeouts in `ConnectionManager` ([#1372](https://github.com/redis-rs/redis-rs/pull/1372) @dcreager)
* Fix timeouts throwing Pubsub::get_message. ([#1379](https://github.com/redis-rs/redis-rs/pull/1379))
* implement command `DRYRUN` ([#1373](https://github.com/redis-rs/redis-rs/pull/1373) @ArtemIsmagilov)
* Support for `hashbrown::HashMap` and `hashbrown::HashSet`  ([#1359](https://github.com/redis-rs/redis-rs/pull/1359) @feelingsonice)
* impl CLIENT GETNAME ([#1380](https://github.com/redis-rs/redis-rs/pull/1380) @ArtemIsmagilov)

#### CI improvements

* Increase test CI action timeout. ([#1370](https://github.com/redis-rs/redis-rs/pull/1370))

### 0.27.4 (2024-10-09)

#### Changes & Bug fixes

* Add lastid option to xclaim ([#1360](https://github.com/redis-rs/redis-rs/pull/1360) @urkle)
* Add xadd_options and xtrim_options ([#1361](https://github.com/redis-rs/redis-rs/pull/1361) @urkle)
* Sync connection: Handle timed-out responses by ignoring them. ([#1290](https://github.com/redis-rs/redis-rs/pull/1290))
* Expose the sink and stream parts of an async pubsub to the user. ([#1366](https://github.com/redis-rs/redis-rs/pull/1366))

#### CI improvements
* Add async iterator tests. ([#1364](https://github.com/redis-rs/redis-rs/pull/1364))

### 0.27.3 (2024-10-01)

#### Changes & Bug fixes

* Add support for [TYPE type] in SCAN commands ([#1332](https://github.com/redis-rs/redis-rs/pull/1332) @Reiuji-ch)
* Align default timeouts on cluster client. ([#1333](https://github.com/redis-rs/redis-rs/pull/1333))
* Updates unmaintained tokio-retry to tokio-retry2 ([#1334](https://github.com/redis-rs/redis-rs/pull/1334) @naomijub)
* Align verification of protocol & TLS during cluster creation. ([#1289](https://github.com/redis-rs/redis-rs/pull/1289))
* Include the StreamExt use statement in docs ([#1345](https://github.com/redis-rs/redis-rs/pull/1345) @joshrotenberg)
* Further limit parser recursion ([#1346](https://github.com/redis-rs/redis-rs/pull/1346))

#### CI improvements

* Improve testing async-std ([#1314](https://github.com/redis-rs/redis-rs/pull/1314))
* Test against Valkey in CI. ([#1315](https://github.com/redis-rs/redis-rs/pull/1315))
* Add CI action to test whether feature combinations compile. ([#1328](https://github.com/redis-rs/redis-rs/pull/1328))

### 0.27.2 (2024-09-14)

#### Changes & Bug fixes

* Pubsub: Keep stream running after sink was closed. ([#1330](https://github.com/redis-rs/redis-rs/pull/1330))

### 0.27.1 (2024-09-14)

#### Changes & Bug fixes

* fix sentinel feature error and update dependency ([#1323](https://github.com/redis-rs/redis-rs/pull/1323) @MokerWill)

### 0.27.0 (2024-09-07)

#### Features

* **Breaking change**: Abort backing task to multiplexed connection on drop ([#1264](https://github.com/redis-rs/redis-rs/pull/1264))
* Add r2d2 support for SentinelClient ([#1297](https://github.com/redis-rs/redis-rs/pull/1297) @smf8)
* Xinfo groups lag and entries-read support ([#837](https://github.com/redis-rs/redis-rs/pull/837) @massimiliano-mantione)
* Improve cluster documentation. [#1263](https://github.com/redis-rs/redis-rs/pull/1263)
* Allow splitting async PubSub to Sink &  Stream. [#1144](https://github.com/redis-rs/redis-rs/pull/1144)
* Default for ConnectionManagerConfig ([#1308](https://github.com/redis-rs/redis-rs/pull/1308) @feelingsonice)

#### Changes & Bug fixes

* Fix new lints [#1310](https://github.com/redis-rs/redis-rs/pull/1310)
* Use pipelines to setup connections [#1250](https://github.com/redis-rs/redis-rs/pull/1250)
* Bump MSRV to 1.70 [#1286](https://github.com/redis-rs/redis-rs/pull/1286)

### 0.26.1 (2024-08-02)

* bug: Exported configured-out item ([#1273](https://github.com/redis-rs/redis-rs/pull/1273) @EmilyMatt)

### 0.26.0 (2024-07-26)

#### Features

* **Breaking change**: Add RESP3 support ([#1058](https://github.com/redis-rs/redis-rs/pull/1058) @altanozlu)
* **Breaking change**: Expose Errors in `Value` [#1093](https://github.com/redis-rs/redis-rs/pull/1093)
* Add max retry delay for every reconnect ([#1194](https://github.com/redis-rs/redis-rs/pull/1194) tonynguyen-sotatek)
* Add support for routing by node address. [#1062](https://github.com/redis-rs/redis-rs/pull/1062)
* **Breaking change**: Deprecate function that erroneously use tokio in its name. [#1087](https://github.com/redis-rs/redis-rs/pull/1087)
* **Breaking change**: Change is_single_arg to num_of_args in ToRedisArgs trait ([#1238](https://github.com/redis-rs/redis-rs/pull/1238) @git-hulk)
* feat: add implementation of `ToRedisArgs`,`FromRedisValue` traits for `Arc<T>`,`Box<T>`,`Rc<T>` ([#1088](https://github.com/redis-rs/redis-rs/pull/1088) @xoac)
* MultiplexedConnection: Relax type requirements for pubsub functions. [#1129](https://github.com/redis-rs/redis-rs/pull/1129)
* Add `invoke_script` to commands to allow for pipelining of scripts ([#1097](https://github.com/redis-rs/redis-rs/pull/1097) @Dav1dde)
* Adde MultiplexedConnection configuration, usable through Sentinel ([#1167](https://github.com/redis-rs/redis-rs/pull/1167) @jrylander)
* Slot parsing: Added handling to "?" and NULL hostnames in CLUSTER SLOTS. [#1094](https://github.com/redis-rs/redis-rs/pull/1094)
* Add scan_options ([#1231](https://github.com/redis-rs/redis-rs/pull/1231) @alekspickle)
* Add un/subscribe commands to `aio::ConnectionManager`. [#1149](https://github.com/redis-rs/redis-rs/pull/1149)
* Mark deprecated constructor functions. [#1218](https://github.com/redis-rs/redis-rs/pull/1218)

#### Changes & Bug fixes

* Add xautoclaim command support ([#1169](https://github.com/redis-rs/redis-rs/pull/1169) @urkle)
* Add support of EXPIRETIME/PEXPIRETIME command ([#1235](https://github.com/redis-rs/redis-rs/pull/1235) @git-hulk)
* Implement `ToRedisArgs` for `std::borrow::Cow` ([#1219](https://github.com/redis-rs/redis-rs/pull/1219) @caass)
* Correct the document of default feature flags ([#1184](https://github.com/redis-rs/redis-rs/pull/1184) @naskya)
* Add xgroup_createconsumer command support ([#1170](https://github.com/redis-rs/redis-rs/pull/1170) @urkle)
* Route unkeyed commands to a random node. [#1095](https://github.com/redis-rs/redis-rs/pull/1095)
* Add dependabot ([#1053](https://github.com/redis-rs/redis-rs/pull/1053) @oriontvv)
* impl `Clone` for `Msg` ([#1116](https://github.com/redis-rs/redis-rs/pull/1116) @publicqi)
* Make response_timeout Optional ([#1134](https://github.com/redis-rs/redis-rs/pull/1134) @zhixinwen)
* Remove redundant match. [#1135](https://github.com/redis-rs/redis-rs/pull/1135)
* Update cluster_async router_command docs ([#1141](https://github.com/redis-rs/redis-rs/pull/1141) @joachimbulow)
* Remove unnecessary generics from multiplexed_connection. [#1142](https://github.com/redis-rs/redis-rs/pull/1142)
* Fix compilation on Windows. ([#1146](https://github.com/redis-rs/redis-rs/pull/1146) @Yury-Fridlyand)
* fix #1150: change int types for expiry to `u64` ([#1152](https://github.com/redis-rs/redis-rs/pull/1152) @ahmadbky)
* check tls mode before setting it in the call of certs() ([#1166](https://github.com/redis-rs/redis-rs/pull/1166) @MyBitterCoffee)
* Fix explicit IoError not being recognized. [#1191](https://github.com/redis-rs/redis-rs/pull/1191)
* Fix typos ([#1198](https://github.com/redis-rs/redis-rs/pull/1198) @wutchzone)
* Fix typos ([#1213](https://github.com/redis-rs/redis-rs/pull/1213) @jayvdb)
* Fix some typos in connection_manager.rs and client.rs ([#1217](https://github.com/redis-rs/redis-rs/pull/1217) @meierfra-ergon)
* Send retries in multi-node reconnect to new connection. [#1202](https://github.com/redis-rs/redis-rs/pull/1202)
* Remove unnecessary clones from pubsub codepaths. [#1127](https://github.com/redis-rs/redis-rs/pull/1127)
* MultiplexedConnection: Report disconnects without polling. [#1096](https://github.com/redis-rs/redis-rs/pull/1096)
* Various documentation improvements. [#1082](https://github.com/redis-rs/redis-rs/pull/1082)
* Fix compilation break. [#1224](https://github.com/redis-rs/redis-rs/pull/1224)
* Split `Request` and routing from cluster async to separate files. [#1226](https://github.com/redis-rs/redis-rs/pull/1226)
* Improve documentation of multiplexed connection. [#1237](https://github.com/redis-rs/redis-rs/pull/1237)
* Fix async cluster documentation. [#1259](https://github.com/redis-rs/redis-rs/pull/1259)
* Cluster connection - Refactor response handling. [#1222](https://github.com/redis-rs/redis-rs/pull/1222)
* Add support of HASH expiration commands ([#1232](https://github.com/redis-rs/redis-rs/pull/1232) @git-hulk)
* Remove push manager [#1251](https://github.com/redis-rs/redis-rs/pull/1251)
* Remove tokio dependency from non-aio build. [#1265](https://github.com/redis-rs/redis-rs/pull/1265)

#### Dependency updates, lints & testing improvements

* Fix new lints. [#1268](https://github.com/redis-rs/redis-rs/pull/1268)
* Fix flakey multi-threaded test runs. [#1261](https://github.com/redis-rs/redis-rs/pull/1261)
* Fix documentation warning. [#1258](https://github.com/redis-rs/redis-rs/pull/1258)
* Fix nightly compilation warnings. [#1229](https://github.com/redis-rs/redis-rs/pull/1229)
* Fix fuzzer. [#1145](https://github.com/redis-rs/redis-rs/pull/1145)
* Fix flakey test. [#1221](https://github.com/redis-rs/redis-rs/pull/1221)
* Cluster creation in test: Try getting a new port if the current port isn't available. [#1214](https://github.com/redis-rs/redis-rs/pull/1214)
* Log the server / cluster logfile on error. [#1200](https://github.com/redis-rs/redis-rs/pull/1200)
* Remove loop from test. [#1187](https://github.com/redis-rs/redis-rs/pull/1187)
* Add `valkey` crate [#1168](https://github.com/redis-rs/redis-rs/pull/1168)
* Add tests for username+password authentication. [#1157](https://github.com/redis-rs/redis-rs/pull/1157)
* Improve PushManager tests in sync connection ([#1100](https://github.com/redis-rs/redis-rs/pull/1100) @altanozlu)
* Fix issues that prevented cluster tests from running concurrently. [#1130](https://github.com/redis-rs/redis-rs/pull/1130)
* Fix issue in cluster tests. [#1139](https://github.com/redis-rs/redis-rs/pull/1139)
* Remove redundant call. [#1112](https://github.com/redis-rs/redis-rs/pull/1112)
* Fix clippy warnings [#1180](https://github.com/redis-rs/redis-rs/pull/1180)
* Wrap tests with modules. [#1084](https://github.com/redis-rs/redis-rs/pull/1084)
* Add missing module skips. [#1083](https://github.com/redis-rs/redis-rs/pull/1083)
* Add vscode settings to gitignore. [#1085](https://github.com/redis-rs/redis-rs/pull/1085)

### 0.25.3 (2024-04-04)

* Handle empty results in multi-node operations ([#1099](https://github.com/redis-rs/redis-rs/pull/1099))

### 0.25.2 (2024-03-15)

* MultiplexedConnection: Separate response handling for pipeline. ([#1078](https://github.com/redis-rs/redis-rs/pull/1078))

### 0.25.1 (2024-03-12)

* Fix small disambiguity in examples ([#1072](https://github.com/redis-rs/redis-rs/pull/1072) @sunhuachuang)
* Upgrade to socket2 0.5 ([#1073](https://github.com/redis-rs/redis-rs/pull/1073) @djc)
* Avoid library dependency on futures-time ([#1074](https://github.com/redis-rs/redis-rs/pull/1074) @djc)


### 0.25.0 (2024-03-08)

#### Features

* **Breaking change**: Add connection timeout to the cluster client ([#834](https://github.com/redis-rs/redis-rs/pull/834))
* **Breaking change**: Deprecate aio::Connection ([#889](https://github.com/redis-rs/redis-rs/pull/889))
* Cluster: fix read from replica & missing slots ([#965](https://github.com/redis-rs/redis-rs/pull/965))
* Async cluster connection: Improve handling of missing connections ([#968](https://github.com/redis-rs/redis-rs/pull/968))
* Add support for parsing to/from any sized arrays ([#981](https://github.com/redis-rs/redis-rs/pull/981))
* Upgrade to rustls 0.22 ([#1000](https://github.com/redis-rs/redis-rs/pull/1000) @djc)
* add SMISMEMBER command ([#1002](https://github.com/redis-rs/redis-rs/pull/1002) @Zacaria)
* Add support for some big number types ([#1014](https://github.com/redis-rs/redis-rs/pull/1014) @AkiraMiyakoda)
* Add Support for UUIDs ([#1029](https://github.com/redis-rs/redis-rs/pull/1029) @Rabbitminers)
* Add FromRedisValue::from_owned_redis_value to reduce copies while parsing response ([#1030](https://github.com/redis-rs/redis-rs/pull/1030) @Nathan-Fenner)
* Save reconnected connections during retries ([#1033](https://github.com/redis-rs/redis-rs/pull/1033))
* Avoid panic on connection failure ([#1035](https://github.com/redis-rs/redis-rs/pull/1035))
* add disable client setinfo feature and its default mode is off ([#1036](https://github.com/redis-rs/redis-rs/pull/1036) @Ggiggle)
* Reconnect on parsing errors ([#1051](https://github.com/redis-rs/redis-rs/pull/1051))
* preallocate buffer for evalsha in Script ([#1044](https://github.com/redis-rs/redis-rs/pull/1044) @framlog)

#### Changes

* Align more commands routings ([#938](https://github.com/redis-rs/redis-rs/pull/938))
* Fix HashMap conversion ([#977](https://github.com/redis-rs/redis-rs/pull/977) @mxbrt)
* MultiplexedConnection: Remove unnecessary allocation in send ([#990](https://github.com/redis-rs/redis-rs/pull/990))
* Tests: Reduce cluster setup flakiness ([#999](https://github.com/redis-rs/redis-rs/pull/999))
* Remove the unwrap_or! macro ([#1010](https://github.com/redis-rs/redis-rs/pull/1010))
* Remove allocation from command function ([#1008](https://github.com/redis-rs/redis-rs/pull/1008))
* Catch panics from task::spawn in tests ([#1015](https://github.com/redis-rs/redis-rs/pull/1015))
* Fix lint errors from new Rust version ([#1016](https://github.com/redis-rs/redis-rs/pull/1016))
* Fix warnings that appear only with native-TLS ([#1018](https://github.com/redis-rs/redis-rs/pull/1018))
* Hide the req_packed_commands from docs ([#1020](https://github.com/redis-rs/redis-rs/pull/1020))
* Fix documentation error ([#1022](https://github.com/redis-rs/redis-rs/pull/1022) @rcl-viveksharma)
* Fixes minor grammar mistake in json.rs file ([#1026](https://github.com/redis-rs/redis-rs/pull/1026) @RScrusoe)
* Enable ignored pipe test ([#1027](https://github.com/redis-rs/redis-rs/pull/1027))
* Fix names of existing async cluster tests ([#1028](https://github.com/redis-rs/redis-rs/pull/1028))
* Add lock file to keep MSRV constant ([#1039](https://github.com/redis-rs/redis-rs/pull/1039))
* Fail CI if lock file isn't updated ([#1042](https://github.com/redis-rs/redis-rs/pull/1042))
* impl Clone/Copy for SetOptions ([#1046](https://github.com/redis-rs/redis-rs/pull/1046) @ahmadbky)
* docs: add "connection-manager" cfg attr ([#1048](https://github.com/redis-rs/redis-rs/pull/1048) @DCNick3)
* Remove the usage of aio::Connection in tests ([#1049](https://github.com/redis-rs/redis-rs/pull/1049))
* Fix new clippy lints ([#1052](https://github.com/redis-rs/redis-rs/pull/1052))
* Handle server errors in array response ([#1056](https://github.com/redis-rs/redis-rs/pull/1056))
* Appease Clippy ([#1061](https://github.com/redis-rs/redis-rs/pull/1061))
* make Pipeline handle returned bulks correctly ([#1063](https://github.com/redis-rs/redis-rs/pull/1063) @framlog)
* Update mio dependency due to vulnerability ([#1064](https://github.com/redis-rs/redis-rs/pull/1064))
* Simplify Sink polling logic ([#1065](https://github.com/redis-rs/redis-rs/pull/1065))
* Separate parsing errors from general response errors ([#1069](https://github.com/redis-rs/redis-rs/pull/1069))

### 0.24.0 (2023-12-05)

#### Features
* **Breaking change**: Support Mutual TLS ([#858](https://github.com/redis-rs/redis-rs/pull/858) @sp-angel)
* Implement `FromRedisValue` for `Box<[T]>` and `Arc<[T]>` ([#799](https://github.com/redis-rs/redis-rs/pull/799) @JOT85)
* Sync Cluster: support multi-slot operations. ([#967](https://github.com/redis-rs/redis-rs/pull/967))
* Execute multi-node requests using try_request. ([#919](https://github.com/redis-rs/redis-rs/pull/919))
* Sorted set blocking commands ([#962](https://github.com/redis-rs/redis-rs/pull/962) @gheorghitamutu)
* Allow passing routing information to cluster. ([#899](https://github.com/redis-rs/redis-rs/pull/899))
* Add `tcp_nodelay` feature ([#941](https://github.com/redis-rs/redis-rs/pull/941) @PureWhiteWu)
* Add support for multi-shard commands. ([#900](https://github.com/redis-rs/redis-rs/pull/900))

#### Changes
* Order in usage of ClusterParams. ([#997](https://github.com/redis-rs/redis-rs/pull/997))
* **Breaking change**: Fix StreamId::contains_key signature ([#783](https://github.com/redis-rs/redis-rs/pull/783) @Ayush1325)
* **Breaking change**: Update Command expiration values to be an appropriate type ([#589](https://github.com/redis-rs/redis-rs/pull/589) @joshleeb)
* **Breaking change**: Bump aHash to v0.8.6 ([#966](https://github.com/redis-rs/redis-rs/pull/966) @aumetra)
* Fix features for `load_native_certs`. ([#996](https://github.com/redis-rs/redis-rs/pull/996))
* Revert redis-test versioning changes ([#993](https://github.com/redis-rs/redis-rs/pull/993))
* Tests: Add retries to test cluster creation ([#994](https://github.com/redis-rs/redis-rs/pull/994))
* Fix sync cluster behavior with transactions. ([#983](https://github.com/redis-rs/redis-rs/pull/983))
* Sync Pub/Sub - cache received pub/sub messages. ([#910](https://github.com/redis-rs/redis-rs/pull/910))
* Prefer routing to primary in a transaction. ([#986](https://github.com/redis-rs/redis-rs/pull/986))
* Accept iterator at `ClusterClient` initialization ([#987](https://github.com/redis-rs/redis-rs/pull/987) @ruanpetterson)
* **Breaking change**: Change timeouts from usize and isize to f64 ([#988](https://github.com/redis-rs/redis-rs/pull/988) @eythorhel19)
* Update minimal rust version to 1.6.5 ([#982](https://github.com/redis-rs/redis-rs/pull/982))
* Disable JSON module tests for redis 6.2.4. ([#980](https://github.com/redis-rs/redis-rs/pull/980))
* Add connection string examples ([#976](https://github.com/redis-rs/redis-rs/pull/976) @NuclearOreo)
* Move response policy into multi-node routing. ([#952](https://github.com/redis-rs/redis-rs/pull/952))
* Added functions that allow tests to check version. ([#963](https://github.com/redis-rs/redis-rs/pull/963))
* Fix XREADGROUP command ordering as per Redis Docs, and compatibility with Upstash Redis ([#960](https://github.com/redis-rs/redis-rs/pull/960) @prabhpreet)
* Optimize make_pipeline_results by pre-allocate memory ([#957](https://github.com/redis-rs/redis-rs/pull/957) @PureWhiteWu)
* Run module tests sequentially.  ([#956](https://github.com/redis-rs/redis-rs/pull/956))
* Log cluster creation output in tests. ([#955](https://github.com/redis-rs/redis-rs/pull/955))
* CI: Update and use better maintained github actions. ([#954](https://github.com/redis-rs/redis-rs/pull/954))
* Call CLIENT SETINFO on new connections. ([#945](https://github.com/redis-rs/redis-rs/pull/945))
* Deprecate functions that erroneously use `tokio` in their name. ([#913](https://github.com/redis-rs/redis-rs/pull/913))
* CI: Increase timeouts and use newer redis. ([#949](https://github.com/redis-rs/redis-rs/pull/949))
* Remove redis version from redis-test. ([#943](https://github.com/redis-rs/redis-rs/pull/943))

### 0.23.4 (2023-11-26)
**Yanked** -- Inadvertently introduced breaking changes (sorry!). The changes in this tag
have been pushed to 0.24.0.

### 0.23.3 (2023-09-01)

Note that this release fixes a small regression in async Redis Cluster handling of the `PING` command.
Based on updated response aggregation logic in [#888](https://github.com/redis-rs/redis-rs/pull/888), it 
will again return a single response instead of an array.

#### Features
* Add `key_type` command ([#933](https://github.com/redis-rs/redis-rs/pull/933) @bruaba)
* Async cluster: Group responses by response_policy. ([#888](https://github.com/redis-rs/redis-rs/pull/888))


#### Fixes
* Remove unnecessary heap allocation ([#939](https://github.com/redis-rs/redis-rs/pull/939) @thechampagne)
* Sentinel tests: Ensure no ports are used twice ([#915](https://github.com/redis-rs/redis-rs/pull/915))
* Fix lint issues ([#937](https://github.com/redis-rs/redis-rs/pull/937))
* Fix JSON serialization error test ([#928](https://github.com/redis-rs/redis-rs/pull/928))
* Remove unused dependencies ([#916](https://github.com/redis-rs/redis-rs/pull/916))

<a name="0.23.2"></a>
### 0.23.2 (2023-08-10)

#### Fixes
* Fix sentinel tests flakiness ([#912](https://github.com/redis-rs/redis-rs/pull/912))
* Rustls: Remove usage of deprecated method ([#921](https://github.com/redis-rs/redis-rs/pull/921))
* Fix compiling with sentinel feature, without aio feature ([#922](https://github.com/redis-rs/redis-rs/pull/923) @brocaar)
* Add timeouts to tests github action ([#911](https://github.com/redis-rs/redis-rs/pull/911))

### 0.23.1 (2023-07-28)

#### Features
* Add basic Sentinel functionality ([#836](https://github.com/redis-rs/redis-rs/pull/836) @felipou)
* Enable keep alive on tcp connections via feature ([#886](https://github.com/redis-rs/redis-rs/pull/886) @DoumanAsh)
* Support fan-out commands in cluster-async ([#843](https://github.com/redis-rs/redis-rs/pull/843) @nihohit)
* connection_manager: retry and backoff on reconnect ([#804](https://github.com/redis-rs/redis-rs/pull/804) @nihohit)

#### Changes
* Tests: Wait for all servers ([#901](https://github.com/redis-rs/redis-rs/pull/901) @barshaul)
* Pin `tempfile` dependency ([#902](https://github.com/redis-rs/redis-rs/pull/902))
* Update routing data for commands. ([#887](https://github.com/redis-rs/redis-rs/pull/887) @nihohit)
* Add basic benchmark reporting to CI ([#880](https://github.com/redis-rs/redis-rs/pull/880))
* Add `set_options` cmd ([#879](https://github.com/redis-rs/redis-rs/pull/879) @RokasVaitkevicius)
* Move random connection creation to when needed. ([#882](https://github.com/redis-rs/redis-rs/pull/882) @nihohit)
* Clean up existing benchmarks ([#881](https://github.com/redis-rs/redis-rs/pull/881))
* Improve async cluster client performance. ([#877](https://github.com/redis-rs/redis-rs/pull/877) @nihohit)
* Allow configuration of cluster retry wait duration ([#859](https://github.com/redis-rs/redis-rs/pull/859) @nihohit)
* Fix async connect when ns resolved to multi ip ([#872](https://github.com/redis-rs/redis-rs/pull/872) @hugefiver)
* Reduce the number of unnecessary clones. ([#874](https://github.com/redis-rs/redis-rs/pull/874) @nihohit)
* Remove connection checking on every request. ([#873](https://github.com/redis-rs/redis-rs/pull/873) @nihohit)
* cluster_async: Wrap internal state with Arc. ([#864](https://github.com/redis-rs/redis-rs/pull/864) @nihohit)
* Fix redirect routing on request with no route. ([#870](https://github.com/redis-rs/redis-rs/pull/870) @nihohit)
* Amend README for macOS users ([#869](https://github.com/redis-rs/redis-rs/pull/869) @sarisssa)
* Improved redirection error handling ([#857](https://github.com/redis-rs/redis-rs/pull/857))
* Fix minor async client bug. ([#862](https://github.com/redis-rs/redis-rs/pull/862) @nihohit)
* Split aio.rs to separate files. ([#821](https://github.com/redis-rs/redis-rs/pull/821) @nihohit)
* Add time feature to tokio dependency ([#855](https://github.com/redis-rs/redis-rs/pull/855) @robjtede)
* Refactor cluster error handling ([#844](https://github.com/redis-rs/redis-rs/pull/844))
* Fix unnecessarily mutable variable ([#849](https://github.com/redis-rs/redis-rs/pull/849) @kamulos)
* Newtype SlotMap ([#845](https://github.com/redis-rs/redis-rs/pull/845))
* Bump MSRV to 1.60 ([#846](https://github.com/redis-rs/redis-rs/pull/846))
* Improve error logging. ([#838](https://github.com/redis-rs/redis-rs/pull/838) @nihohit)
* Improve documentation, add references to `redis-macros` ([#769](https://github.com/redis-rs/redis-rs/pull/769) @daniel7grant)
* Allow creating Cmd with capacity. ([#817](https://github.com/redis-rs/redis-rs/pull/817) @nihohit)

<a name="0.23.0"></a>
### 0.23.0 (2023-04-05)
In addition to *everything mentioned in 0.23.0-beta.1 notes*, this release adds support for Rustls, a long-
sought feature. Thanks to @rharish101 and @LeoRowan for getting this in!

#### Changes
* Update Rustls to v0.21.0 ([#820](https://github.com/redis-rs/redis-rs/pull/820) @rharish101)
* Implement support for Rustls ([#725](https://github.com/redis-rs/redis-rs/pull/725) @rharish101, @LeoRowan)

<a name="0.23.0-beta.1"></a>
### 0.23.0-beta.1 (2023-03-28)

This release adds the `cluster_async` module, which introduces async Redis Cluster support. The code therein
is largely taken from @Marwes's [redis-cluster-async crate](https://github.com/redis-rs/redis-cluster-async), which itself
appears to have started from a sync Redis Cluster implementation started by @atuk721. In any case, thanks to @Marwes and @atuk721
for the great work, and we hope to keep development moving forward in `redis-rs`. 

Though async Redis Cluster functionality for the time being has been kept as close to the originating crate as possible, previous users of 
`redis-cluster-async` should note the following changes:
* Retries, while still configurable, can no longer be set to `None`/infinite retries
* Routing and slot parsing logic has been removed and merged with existing `redis-rs` functionality
* The client has been removed and superseded by common `ClusterClient`
* Renamed `Connection` to `ClusterConnection`
* Added support for reading from replicas
* Added support for insecure TLS
* Added support for setting both username and password

#### Breaking Changes
* Fix long-standing bug related to `AsyncIter`'s stream implementation in which polling the server
  for additional data yielded broken data in most cases. Type bounds for `AsyncIter` have changed slightly,
  making this a potentially breaking change. ([#597](https://github.com/redis-rs/redis-rs/pull/597) @roger)

#### Changes
* Commands: Add additional generic args for key arguments ([#795](https://github.com/redis-rs/redis-rs/pull/795) @MaxOhn)
* Add `mset` / deprecate `set_multiple` ([#766](https://github.com/redis-rs/redis-rs/pull/766) @randomairborne)
* More efficient interfaces for `MultiplexedConnection` and `ConnectionManager` ([#811](https://github.com/redis-rs/redis-rs/pull/811) @nihohit)
* Refactor / remove flaky test ([#810](https://github.com/redis-rs/redis-rs/pull/810))
* `cluster_async`: rename `Connection` to `ClusterConnection`, `Pipeline` to `ClusterConnInner` ([#808](https://github.com/redis-rs/redis-rs/pull/808))
* Support parsing IPV6 cluster nodes ([#796](https://github.com/redis-rs/redis-rs/pull/796) @socs)
* Common client for sync/async cluster connections ([#798](https://github.com/redis-rs/redis-rs/pull/798))
  * `cluster::ClusterConnection` underlying connection type is now generic (with existing type as default)
  * Support `read_from_replicas` in cluster_async
  * Set retries in `ClusterClientBuilder`
  * Add mock tests for `cluster`
* cluster-async common slot parsing([#793](https://github.com/redis-rs/redis-rs/pull/793))
* Support async-std in cluster_async module ([#790](https://github.com/redis-rs/redis-rs/pull/790))
* Async-Cluster use same routing as Sync-Cluster ([#789](https://github.com/redis-rs/redis-rs/pull/789))
* Add Async Cluster Support ([#696](https://github.com/redis-rs/redis-rs/pull/696))
* Fix broken json-module tests ([#786](https://github.com/redis-rs/redis-rs/pull/786))
* `cluster`: Tls Builder support / simplify cluster connection map ([#718](https://github.com/redis-rs/redis-rs/pull/718) @0xWOF, @utkarshgupta137)

<a name="0.22.3"></a>
### 0.22.3 (2023-01-23)

#### Changes
*   Restore inherent `ClusterConnection::check_connection()` method ([#758](https://github.com/redis-rs/redis-rs/pull/758) @robjtede)


<a name="0.22.2"></a>
### 0.22.2 (2023-01-07)

This release adds various incremental improvements and fixes a few long-standing bugs. Thanks to all our
contributors for making this release happen.

#### Features
*   Implement ToRedisArgs for HashMap ([#722](https://github.com/redis-rs/redis-rs/pull/722) @gibranamparan) 
*   Add explicit `MGET` command ([#729](https://github.com/redis-rs/redis-rs/pull/729) @vamshiaruru-virgodesigns)

#### Bug fixes
*   Enable single-item-vector `get` responses ([#507](https://github.com/redis-rs/redis-rs/pull/507) @hank121314)
*   Fix empty result from xread_options with deleted entries ([#712](https://github.com/redis-rs/redis-rs/pull/712) @Quiwin)
*   Limit Parser Recursion ([#724](https://github.com/redis-rs/redis-rs/pull/724))
*   Improve MultiplexedConnection Error Handling ([#699](https://github.com/redis-rs/redis-rs/pull/699))

#### Changes
*   Add test case for atomic pipeline ([#702](https://github.com/redis-rs/redis-rs/pull/702) @CNLHC)
*   Capture subscribe result error in PubSub doc example ([#739](https://github.com/redis-rs/redis-rs/pull/739) @baoyachi) 
*   Use async-std name resolution when necessary ([#701](https://github.com/redis-rs/redis-rs/pull/701) @UgnilJoZ) 
*   Add Script::invoke_async method ([#711](https://github.com/redis-rs/redis-rs/pull/711) @r-bk)
*   Cluster Refactorings ([#717](https://github.com/redis-rs/redis-rs/pull/717), [#716](https://github.com/redis-rs/redis-rs/pull/716), [#709](https://github.com/redis-rs/redis-rs/pull/709), [#707](https://github.com/redis-rs/redis-rs/pull/707), [#706](https://github.com/redis-rs/redis-rs/pull/706) @0xWOF, @utkarshgupta137)
*   Fix intermittent test failure ([#714](https://github.com/redis-rs/redis-rs/pull/714) @0xWOF, @utkarshgupta137)
*   Doc changes ([#705](https://github.com/redis-rs/redis-rs/pull/705) @0xWOF, @utkarshgupta137)
*   Lint fixes ([#704](https://github.com/redis-rs/redis-rs/pull/704) @0xWOF)


<a name="0.22.1"></a>
### 0.22.1 (2022-10-18)

#### Changes
* Add README attribute to Cargo.toml
* Update LICENSE file / symlink from parent directory

<a name="0.22.0"></a>
### 0.22.0 (2022-10-05)

This release adds various incremental improvements, including
additional convenience commands, improved Cluster APIs, and various other bug
fixes/library improvements.

Although the changes here are incremental, this is a major release due to the 
breaking changes listed below.

This release would not be possible without our many wonderful 
contributors -- thank you!

#### Breaking changes
*   Box all large enum variants; changes enum signature ([#667](https://github.com/redis-rs/redis-rs/pull/667) @nihohit)
*   Support ACL commands by adding Rule::Other to cover newly defined flags; adds new enum variant ([#685](https://github.com/redis-rs/redis-rs/pull/685) @garyhai)
*   Switch from sha1 to sha1_smol; renames `sha1` feature ([#576](https://github.com/redis-rs/redis-rs/pull/576))

#### Features
*   Add support for RedisJSON ([#657](https://github.com/redis-rs/redis-rs/pull/657) @d3rpp)
*   Add support for weights in zunionstore and zinterstore ([#641](https://github.com/redis-rs/redis-rs/pull/641) @ndd7xv)
*   Cluster: Create read_from_replicas option ([#635](https://github.com/redis-rs/redis-rs/pull/635) @utkarshgupta137)
*   Make Direction a public enum to use with Commands like BLMOVE ([#646](https://github.com/redis-rs/redis-rs/pull/646) @thorbadour)
*   Add `ahash` feature for using ahash internally & for redis values ([#636](https://github.com/redis-rs/redis-rs/pull/636) @utkarshgupta137)
*   Add Script::load function ([#603](https://github.com/redis-rs/redis-rs/pull/603) @zhiburt)
*   Add support for OBJECT ([[#610]](https://github.com/redis-rs/redis-rs/pull/610) @roger)
*   Add GETEX and GETDEL support ([#582](https://github.com/redis-rs/redis-rs/pull/582) @arpandaze)
*   Add support for ZMPOP ([#605](https://github.com/redis-rs/redis-rs/pull/605) @gkorland)

#### Changes
*   Rust 2021 Edition / MSRV 1.59.0
*   Fix: Support IPV6 link-local address parsing ([#679](https://github.com/redis-rs/redis-rs/pull/679) @buaazp)
*   Derive Clone and add Deref trait to InfoDict ([#661](https://github.com/redis-rs/redis-rs/pull/661) @danni-m)
*   ClusterClient: add handling for empty initial_nodes, use ClusterParams to store cluster parameters, improve builder pattern ([#669](https://github.com/redis-rs/redis-rs/pull/669) @utkarshgupta137)
*   Implement Debug for MultiplexedConnection & Pipeline ([#664](https://github.com/redis-rs/redis-rs/pull/664) @elpiel)
*   Add support for casting RedisResult to CString ([#660](https://github.com/redis-rs/redis-rs/pull/660) @nihohit)
*   Move redis crate to subdirectory to support multiple crates in project ([#465](https://github.com/redis-rs/redis-rs/pull/465) @tdyas)
*   Stop versioning Cargo.lock ([#620](https://github.com/redis-rs/redis-rs/pull/620))
*   Auto-implement ConnectionLike for DerefMut ([#567](https://github.com/redis-rs/redis-rs/pull/567) @holmesmr)
*   Return errors from parsing inner items ([#608](https://github.com/redis-rs/redis-rs/pull/608))
*   Make dns resolution async, in async runtime ([#606](https://github.com/redis-rs/redis-rs/pull/606) @roger)
*   Make async_trait dependency optional ([#572](https://github.com/redis-rs/redis-rs/pull/572) @kamulos)
*   Add username to ClusterClient and ClusterConnection ([#596](https://github.com/redis-rs/redis-rs/pull/596) @gildaf)


<a name="0.21.6"></a>
### 0.21.6 (2022-08-24)

*   Update dependencies ([#588](https://github.com/mitsuhiko/redis-rs/pull/588))

<a name="0.21.5"></a>
### 0.21.5 (2022-01-10)

#### Features

*   Add new list commands ([#570](https://github.com/mitsuhiko/redis-rs/pull/570))


<a name="0.21.4"></a>
### 0.21.4 (2021-11-04)

#### Features

*   Add convenience command: zrandbember ([#556](https://github.com/mitsuhiko/redis-rs/pull/556))



<a name="0.21.3"></a>
### 0.21.3 (2021-10-15)

#### Features

*   Add support for TLS with cluster mode ([#548](https://github.com/mitsuhiko/redis-rs/pull/548))

#### Changes

*   Remove stunnel as a dep, use redis native tls ([#542](https://github.com/mitsuhiko/redis-rs/pull/542))




<a name="0.21.2"></a>
### 0.21.2 (2021-09-02)


#### Bug Fixes

*   Compile with tokio-comp and up-to-date dependencies ([282f997e](https://github.com/mitsuhiko/redis-rs/commit/282f997e41cc0de2a604c0f6a96d82818dacc373), closes [#531](https://github.com/mitsuhiko/redis-rs/issues/531), breaks [#](https://github.com/mitsuhiko/redis-rs/issues/))

#### Breaking Changes

*   Compile with tokio-comp and up-to-date dependencies ([282f997e](https://github.com/mitsuhiko/redis-rs/commit/282f997e41cc0de2a604c0f6a96d82818dacc373), closes [#531](https://github.com/mitsuhiko/redis-rs/issues/531), breaks [#](https://github.com/mitsuhiko/redis-rs/issues/))



<a name="0.21.1"></a>
### 0.21.1 (2021-08-25)


#### Bug Fixes

*   pin futures dependency to required version ([9be392bc](https://github.com/mitsuhiko/redis-rs/commit/9be392bc5b22326a8a0eafc7aa18cc04c5d79e0e))



<a name="0.21.0"></a>
### 0.21.0 (2021-07-16)


#### Performance

*   Don't enqueue multiplexed commands if the receiver is dropped ([ca5019db](https://github.com/mitsuhiko/redis-rs/commit/ca5019dbe76cc56c93eaecb5721de8fcf74d1641))

#### Features

*   Refactor ConnectionAddr to remove boxing and clarify fields

<a name="0.20.2"></a>
### 0.20.2 (2021-06-17)

#### Features

*   Provide a new_async_std function ([c3716d15](https://github.com/mitsuhiko/redis-rs/commit/c3716d154f067b71acdd5bd927e118305cd0830b))

#### Bug Fixes

*   Return Ready(Ok(())) when we have flushed all messages ([ca319c06](https://github.com/mitsuhiko/redis-rs/commit/ca319c06ad80fc37f1f701aecebbd5dabb0dceb0))
*   Don't loop forever on shutdown of the multiplexed connection ([ddecce9e](https://github.com/mitsuhiko/redis-rs/commit/ddecce9e10b8ab626f41409aae289d62b4fb74be))



<a name="0.20.1"></a>
### 0.20.1 (2021-05-18)


#### Bug Fixes

*   Error properly if eof is reached in the decoder ([306797c3](https://github.com/mitsuhiko/redis-rs/commit/306797c3c55ab24e0a29b6517356af794731d326))



<a name="0.20.0"></a>
## 0.20.0 (2021-02-17)


#### Features

*   Make ErrorKind non_exhaustive for forwards compatibility ([ac5e1a60](https://github.com/mitsuhiko/redis-rs/commit/ac5e1a60d398814b18ed1b579fe3f6b337e545e9))
* **aio:**  Allow the underlying IO stream to be customized ([6d2fc8fa](https://github.com/mitsuhiko/redis-rs/commit/6d2fc8faa707fbbbaae9fe092bbc90ce01224523))



<a name="0.19.0"></a>
## 0.19.0 (2020-12-26)


#### Features

*   Update to tokio 1.0 ([41960194](https://github.com/mitsuhiko/redis-rs/commit/4196019494aafc2bab718bafd1fdfd5e8c195ffa))
*   use the node specified in the MOVED error ([8a53e269](https://github.com/mitsuhiko/redis-rs/commit/8a53e2699d7d7bd63f222de778ed6820b0a65665))



<a name="0.18.0"></a>
## 0.18.0 (2020-12-03)


#### Bug Fixes

*   Don't require tokio for the connection manager ([46be86f3](https://github.com/mitsuhiko/redis-rs/commit/46be86f3f07df4900559bf9a4dfd0b5138c3ac52))

* Make ToRedisArgs and FromRedisValue consistent for booleans

BREAKING CHANGE

bool are now written as 0 and 1 instead of true and false. Parsing a bool still accept true and false so this should not break anything for most users however if you are reading something out that was stored as a bool you may see different results.

#### Features

*   Update tokio dependency to 0.3 ([bf5e0af3](https://github.com/mitsuhiko/redis-rs/commit/bf5e0af31c08be1785656031ffda96c355ee83c4), closes [#396](https://github.com/mitsuhiko/redis-rs/issues/396))
*   add doc_cfg for Makefile and docs.rs config ([1bf79517](https://github.com/mitsuhiko/redis-rs/commit/1bf795174521160934f3695326897458246e4978))
*   Impl FromRedisValue for i128 and u128


# Changelog

## [0.18.0](https://github.com/mitsuhiko/redis-rs/compare/0.17.0...0.18.0) - 2020-12-03

## [0.17.0](https://github.com/mitsuhiko/redis-rs/compare/0.16.0...0.17.0) - 2020-07-29

**Fixes and improvements**

* Added Redis Streams commands ([#162](https://github.com/mitsuhiko/redis-rs/pull/319))
* Added support for zpopmin and zpopmax ([#351](https://github.com/mitsuhiko/redis-rs/pull/351))
* Added TLS support, gated by a feature flag ([#305](https://github.com/mitsuhiko/redis-rs/pull/305))
* Added Debug and Clone implementations to redis::Script ([#365](https://github.com/mitsuhiko/redis-rs/pull/365))
* Added FromStr for ConnectionInfo ([#368](https://github.com/mitsuhiko/redis-rs/pull/368))
* Support SCAN methods on async connections ([#326](https://github.com/mitsuhiko/redis-rs/pull/326))
* Removed unnecessary overhead around `Value` conversions ([#327](https://github.com/mitsuhiko/redis-rs/pull/327))
* Support for Redis 6 auth ([#341](https://github.com/mitsuhiko/redis-rs/pull/341))
* BUGFIX: Make aio::Connection Sync again ([#321](https://github.com/mitsuhiko/redis-rs/pull/321))
* BUGFIX: Return UnexpectedEof if we try to decode at eof ([#322](https://github.com/mitsuhiko/redis-rs/pull/322))
* Added support to create a connection from a (host, port) tuple ([#370](https://github.com/mitsuhiko/redis-rs/pull/370))

## [0.16.0](https://github.com/mitsuhiko/redis-rs/compare/0.15.1...0.16.0) - 2020-05-10

**Fixes and improvements**

* Reduce dependencies without async IO ([#266](https://github.com/mitsuhiko/redis-rs/pull/266))
* Add an afl fuzz target ([#274](https://github.com/mitsuhiko/redis-rs/pull/274))
* Updated to combine 4 and avoid async dependencies for sync-only ([#272](https://github.com/mitsuhiko/redis-rs/pull/272))
    * **BREAKING CHANGE**: The parser type now only persists the buffer and takes the Read instance in `parse_value`
* Implement a connection manager for automatic reconnection ([#278](https://github.com/mitsuhiko/redis-rs/pull/278))
* Add async-std support ([#281](https://github.com/mitsuhiko/redis-rs/pull/281))
* Fix key extraction for some stream commands ([#283](https://github.com/mitsuhiko/redis-rs/pull/283))
* Add asynchronous PubSub support ([#287](https://github.com/mitsuhiko/redis-rs/pull/287))

### Breaking changes

#### Changes to the `Parser` type ([#272](https://github.com/mitsuhiko/redis-rs/pull/272))

The parser type now only persists the buffer and takes the Read instance in `parse_value`.
`redis::parse_redis_value` is unchanged and continues to work.


Old:

```rust
let mut parser = Parser::new(bytes);
let result = parser.parse_value();
```

New:

```rust
let mut parser = Parser::new();
let result = parser.parse_value(bytes);
```

## [0.15.1](https://github.com/mitsuhiko/redis-rs/compare/0.15.0...0.15.1) - 2020-01-15

**Fixes and improvements**

* Fixed the `r2d2` feature (re-added it) ([#265](https://github.com/mitsuhiko/redis-rs/pull/265))

## [0.15.0](https://github.com/mitsuhiko/redis-rs/compare/0.14.0...0.15.0) - 2020-01-15

**Fixes and improvements**

* Added support for redis cluster ([#239](https://github.com/mitsuhiko/redis-rs/pull/239))

## [0.14.0](https://github.com/mitsuhiko/redis-rs/compare/0.13.0...0.14.0) - 2020-01-08

**Fixes and improvements**

* Fix the command verb being sent to redis for `zremrangebyrank` ([#240](https://github.com/mitsuhiko/redis-rs/pull/240))
* Add `get_connection_with_timeout` to Client ([#243](https://github.com/mitsuhiko/redis-rs/pull/243))
* **Breaking change:**  Add Cmd::get, Cmd::set and remove PipelineCommands ([#253](https://github.com/mitsuhiko/redis-rs/pull/253))
* Async-ify the API ([#232](https://github.com/mitsuhiko/redis-rs/pull/232))
* Bump minimal required Rust version to 1.39 (required for the async/await API)
* Add async/await examples ([#261](https://github.com/mitsuhiko/redis-rs/pull/261), [#263](https://github.com/mitsuhiko/redis-rs/pull/263))
* Added support for PSETEX and PTTL commands. ([#259](https://github.com/mitsuhiko/redis-rs/pull/259))

### Breaking changes

#### Add Cmd::get, Cmd::set and remove PipelineCommands ([#253](https://github.com/mitsuhiko/redis-rs/pull/253))

If you are using pipelines and were importing the `PipelineCommands` trait you can now remove that import
and only use the `Commands` trait.

Old:

```rust
use redis::{Commands, PipelineCommands};
```

New:

```rust
use redis::Commands;
```

## [0.13.0](https://github.com/mitsuhiko/redis-rs/compare/0.12.0...0.13.0) - 2019-10-14

**Fixes and improvements**

* **Breaking change:** rename `parse_async` to `parse_redis_value_async` for consistency ([ce59cecb](https://github.com/mitsuhiko/redis-rs/commit/ce59cecb830d4217115a4e74e38891e76cf01474)).
* Run clippy over the entire codebase ([#238](https://github.com/mitsuhiko/redis-rs/pull/238))
* **Breaking change:** Make `Script#invoke_async` generic over `aio::ConnectionLike` ([#242](https://github.com/mitsuhiko/redis-rs/pull/242))

### BREAKING CHANGES

#### Rename `parse_async` to `parse_redis_value_async` for consistency ([ce59cecb](https://github.com/mitsuhiko/redis-rs/commit/ce59cecb830d4217115a4e74e38891e76cf01474)).

If you used `redis::parse_async` before, you now need to change this to `redis::parse_redis_value_async`
or import the method under the new name: `use redis::parse_redis_value_async`.

#### Make `Script#invoke_async` generic over `aio::ConnectionLike` ([#242](https://github.com/mitsuhiko/redis-rs/pull/242))

`Script#invoke_async` was changed to be generic over `aio::ConnectionLike` in order to support wrapping a `SharedConnection` in user code.
This required adding a new generic parameter to the method, causing an error when the return type is defined using the turbofish syntax.

Old:

```rust
redis::Script::new("return ...")
  .key("key1")
  .arg("an argument")
  .invoke_async::<String>()
```

New:

```rust
redis::Script::new("return ...")
  .key("key1")
  .arg("an argument")
  .invoke_async::<_, String>()
```

## [0.12.0](https://github.com/mitsuhiko/redis-rs/compare/0.11.0...0.12.0) - 2019-08-26

**Fixes and improvements**

* **Breaking change:** Use `dyn` keyword to avoid deprecation warning ([#223](https://github.com/mitsuhiko/redis-rs/pull/223))
* **Breaking change:** Update url dependency to v2 ([#234](https://github.com/mitsuhiko/redis-rs/pull/234))
* **Breaking change:** (async) Fix `Script::invoke_async` return type error ([#233](https://github.com/mitsuhiko/redis-rs/pull/233))
* Add `GETRANGE` and `SETRANGE` commands ([#202](https://github.com/mitsuhiko/redis-rs/pull/202))
* Fix `SINTERSTORE` wrapper name, it's now correctly `sinterstore` ([#225](https://github.com/mitsuhiko/redis-rs/pull/225))
* Allow running `SharedConnection` with any other runtime ([#229](https://github.com/mitsuhiko/redis-rs/pull/229))
* Reformatted as Edition 2018 code ([#235](https://github.com/mitsuhiko/redis-rs/pull/235))

### BREAKING CHANGES

#### Use `dyn` keyword to avoid deprecation warning ([#223](https://github.com/mitsuhiko/redis-rs/pull/223))

Rust nightly deprecated bare trait objects.
This PR adds the `dyn` keyword to all trait objects in order to get rid of the warning.
This bumps the minimal supported Rust version to [Rust 1.27](https://blog.rust-lang.org/2018/06/21/Rust-1.27.html).

#### Update url dependency to v2 ([#234](https://github.com/mitsuhiko/redis-rs/pull/234))

We updated the `url` dependency to v2. We do expose this on our public API on the `redis::parse_redis_url` function. If you depend on that, make sure to also upgrade your direct dependency.

#### (async) Fix Script::invoke_async return type error ([#233](https://github.com/mitsuhiko/redis-rs/pull/233))

Previously, invoking a script with a complex return type would cause the following error:

```
Response was of incompatible type: "Not a bulk response" (response was string data('"4b98bef92b171357ddc437b395c7c1a5145ca2bd"'))
```

This was because the Future returned when loading the script into the database returns the hash of the script, and thus the return type of `String` would not match the intended return type.

This commit adds an enum to account for the different Future return types.


## [0.11.0](https://github.com/mitsuhiko/redis-rs/compare/0.11.0-beta.2...0.11.0) - 2019-07-19

This release includes all fixes & improvements from the two beta releases listed below.
This release contains breaking changes.

**Fixes and improvements**

* (async) Fix performance problem for SharedConnection ([#222](https://github.com/mitsuhiko/redis-rs/pull/222))

## [0.11.0-beta.2](https://github.com/mitsuhiko/redis-rs/compare/0.11.0-beta.1...0.11.0-beta.2) - 2019-07-14

**Fixes and improvements**

* (async) Don't block the executor from shutting down ([#217](https://github.com/mitsuhiko/redis-rs/pull/217))

## [0.11.0-beta.1](https://github.com/mitsuhiko/redis-rs/compare/0.10.0...0.11.0-beta.1) - 2019-05-30

**Fixes and improvements**

* (async) Simplify implicit pipeline handling ([#182](https://github.com/mitsuhiko/redis-rs/pull/182))
* (async) Use `tokio_sync`'s channels instead of futures ([#195](https://github.com/mitsuhiko/redis-rs/pull/195))
* (async) Only allocate one oneshot per request ([#194](https://github.com/mitsuhiko/redis-rs/pull/194))
* Remove redundant BufReader when parsing ([#197](https://github.com/mitsuhiko/redis-rs/pull/197))
* Hide actual type returned from async parser ([#193](https://github.com/mitsuhiko/redis-rs/pull/193))
* Use more performant operations for line parsing ([#198](https://github.com/mitsuhiko/redis-rs/pull/198))
* Optimize the command encoding, see below for **breaking changes** ([#165](https://github.com/mitsuhiko/redis-rs/pull/165))
* Add support for geospatial commands ([#130](https://github.com/mitsuhiko/redis-rs/pull/130))
* (async) Add support for async Script invocation ([#206](https://github.com/mitsuhiko/redis-rs/pull/206))

### BREAKING CHANGES

#### Renamed the async module to aio ([#189](https://github.com/mitsuhiko/redis-rs/pull/189))

`async` is a reserved keyword in Rust 2018, so this avoids the need to write `r#async` in it.

Old code:

```rust
use redis::async::SharedConnection;
```

New code:

```rust
use redis::aio::SharedConnection;
```

#### The trait `ToRedisArgs` was changed ([#165](https://github.com/mitsuhiko/redis-rs/pull/165))

`ToRedisArgs` has been changed to take take an instance of `RedisWrite` instead of `Vec<Vec<u8>>`. Use the `write_arg` method instead of `Vec::push`.

#### Minimum Rust version is now 1.26 ([#165](https://github.com/mitsuhiko/redis-rs/pull/165))

Upgrade your compiler.
`impl Iterator` is used, requiring a more recent version of the Rust compiler.

#### `iter` now takes `self` by value ([#165](https://github.com/mitsuhiko/redis-rs/pull/165))

`iter` now takes `self` by value instead of cloning `self` inside the method.

Old code:

```rust
let mut iter : redis::Iter<isize> = cmd.arg("my_set").cursor_arg(0).iter(&con).unwrap();
```

New code:

```rust
let mut iter : redis::Iter<isize> = cmd.arg("my_set").cursor_arg(0).clone().iter(&con).unwrap();
```

(The above line calls `clone()`.)

#### A mutable connection object is now required ([#148](https://github.com/mitsuhiko/redis-rs/pull/148))

We removed the internal usage of `RefCell` and `Cell` and instead require a mutable reference, `&mut ConnectionLike`,
on all command calls.

Old code:

```rust
let client = redis::Client::open("redis://127.0.0.1/")?;
let con = client.get_connection()?;
redis::cmd("SET").arg("my_key").arg(42).execute(&con);
```

New code:

```rust
let client = redis::Client::open("redis://127.0.0.1/")?;
let mut con = client.get_connection()?;
redis::cmd("SET").arg("my_key").arg(42).execute(&mut con);
```

Due to this, `transaction` has changed. The callback now also receives a mutable reference to the used connection.

Old code:

```rust
let client = redis::Client::open("redis://127.0.0.1/").unwrap();
let con = client.get_connection().unwrap();
let key = "the_key";
let (new_val,) : (isize,) = redis::transaction(&con, &[key], |pipe| {
    let old_val : isize = con.get(key)?;
    pipe
        .set(key, old_val + 1).ignore()
        .get(key).query(&con)
})?;
```

New code:

```rust
let client = redis::Client::open("redis://127.0.0.1/").unwrap();
let mut con = client.get_connection().unwrap();
let key = "the_key";
let (new_val,) : (isize,) = redis::transaction(&mut con, &[key], |con, pipe| {
    let old_val : isize = con.get(key)?;
    pipe
        .set(key, old_val + 1).ignore()
        .get(key).query(&con)
})?;
```

#### Remove `rustc-serialize` feature ([#200](https://github.com/mitsuhiko/redis-rs/pull/200))

We removed serialization to/from JSON. The underlying library is deprecated for a long time.

Old code in `Cargo.toml`:

```
[dependencies.redis]
version = "0.9.1"
features = ["with-rustc-json"]
```

There's no replacement for the feature.
Use [serde](https://serde.rs/) and handle the serialization/deserialization in your own code.

#### Remove `with-unix-sockets` feature ([#201](https://github.com/mitsuhiko/redis-rs/pull/201))

We removed the Unix socket feature. It is now always enabled.
We also removed auto-detection.

Old code in `Cargo.toml`:

```
[dependencies.redis]
version = "0.9.1"
features = ["with-unix-sockets"]
```

There's no replacement for the feature. Unix sockets will continue to work by default.

## [0.10.0](https://github.com/mitsuhiko/redis-rs/compare/0.9.1...0.10.0) - 2019-02-19

* Fix handling of passwords with special characters (#163)
* Better performance for async code due to less boxing (#167)
    * CAUTION: redis-rs will now require Rust 1.26
* Add `clear` method to the pipeline (#176)
* Better benchmarking (#179)
* Fully formatted source code (#181)

## [0.9.1](https://github.com/mitsuhiko/redis-rs/compare/0.9.0...0.9.1) (2018-09-10)

* Add ttl command

## [0.9.0](https://github.com/mitsuhiko/redis-rs/compare/0.8.0...0.9.0) (2018-08-08)

Some time has passed since the last release.
This new release will bring less bugs, more commands, experimental async support and better performance.

Highlights:

* Implement flexible PubSub API (#136)
* Avoid allocating some redundant Vec's during encoding (#140)
* Add an async interface using futures-rs (#141)
* Allow the async connection to have multiple in flight requests (#143)

The async support is currently experimental.

## [0.8.0](https://github.com/mitsuhiko/redis-rs/compare/0.7.1...0.8.0) (2016-12-26)

* Add publish command

## [0.7.1](https://github.com/mitsuhiko/redis-rs/compare/0.7.0...0.7.1) (2016-12-17)

* Fix unix socket builds
* Relax lifetimes for scripts

## [0.7.0](https://github.com/mitsuhiko/redis-rs/compare/0.6.0...0.7.0) (2016-07-23)

* Add support for built-in unix sockets

## [0.6.0](https://github.com/mitsuhiko/redis-rs/compare/0.5.4...0.6.0) (2016-07-14)

* feat: Make rustc-serialize an optional feature (#96)

## [0.5.4](https://github.com/mitsuhiko/redis-rs/compare/0.5.3...0.5.4) (2016-06-25)

* fix: Improved single arg handling (#95)
* feat: Implement ToRedisArgs for &String (#89)
* feat: Faster command encoding (#94)

## [0.5.3](https://github.com/mitsuhiko/redis-rs/compare/0.5.2...0.5.3) (2016-05-03)

* fix: Use explicit versions for dependencies
* fix: Send `AUTH` command before other commands
* fix: Shutdown connection upon protocol error
* feat: Add `keys` method
* feat: Possibility to set read and write timeouts for the connection
