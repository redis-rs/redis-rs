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
* The client has been removed and superceded by common `ClusterClient`
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
*   Fix intermitent test failure ([#714](https://github.com/redis-rs/redis-rs/pull/714) @0xWOF, @utkarshgupta137)
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
let result = parser.pase_value(bytes);
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
