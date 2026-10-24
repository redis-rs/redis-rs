### 2.0.0-rc.1 (2026-10-24)

* Track redis 2.0.0-rc.1 release
* redis-test/RedisServer: Dedupe server creation (1/19) ([#2220](https://github.com/redis-rs/redis-rs/pull/2220) by @somechris)
* redis-test: Add dedicated struct for a Redis server's start commands (2/19) ([#2221](https://github.com/redis-rs/redis-rs/pull/2221) by @somechris)
* redis-test: Add syntactic sugar to get config directives onto one line (3/19) ([#2225](https://github.com/redis-rs/redis-rs/pull/2225) by @somechris)
* redis-test: Move module loading into `RedisServerCommand` (4/19) ([#2226](https://github.com/redis-rs/redis-rs/pull/2226) by @somechris)
* redis-test: Panic, when trying to load a module that does not exist (5/19) ([#2227](https://github.com/redis-rs/redis-rs/pull/2227) by @somechris)
* redis-test/RedisServer: Use same variables to denote the same things (6/19) ([#2230](https://github.com/redis-rs/redis-rs/pull/2230) by @somechris)
* redis-test/RedisServer: Dedupe disabling Redis' default listening (7/19) ([#2232](https://github.com/redis-rs/redis-rs/pull/2232) by @somechris)
* redis-test/Sentinel: Streamline TLS aspects (8/19) ([#2233](https://github.com/redis-rs/redis-rs/pull/2233) by @somechris)
* tests: Stop enforcing TLS when testing ConnectionManager's reconnect (9/19) ([#2234](https://github.com/redis-rs/redis-rs/pull/2234) by @somechris)
* redis-test/RedisServer: Guard against unused settings (10/19) ([#2235](https://github.com/redis-rs/redis-rs/pull/2235) by @somechris)
* redis-test: Add `RedisServerBuilder` (11/19) ([#2236](https://github.com/redis-rs/redis-rs/pull/2236) by @somechris)
* redis-test/RedisServer: Expose the used mTLS setting (12/19) ([#2237](https://github.com/redis-rs/redis-rs/pull/2237) by @somechris)
* tests: Add `TestContextBuilder` (13/19) ([#2238](https://github.com/redis-rs/redis-rs/pull/2238) by @somechris)
* RedisTest: Mark all types as non exhaustive. ([#2245](https://github.com/redis-rs/redis-rs/pull/2245) by @nihohit)
* tests: Migrate `TestContext::new_with_mtls` onto `TestContextBuilder` (14/19) ([#2241](https://github.com/redis-rs/redis-rs/pull/2241) by @somechris)
* tests: Migrate `TestContext::with_modules` onto `TestContextBuilder` (15/19) ([#2250](https://github.com/redis-rs/redis-rs/pull/2250) by @somechris)
* tests: Migrate `TestContext::new_with_addr` onto `TestContextBuilder` (16/19) ([#2252](https://github.com/redis-rs/redis-rs/pull/2252) by @somechris)
* tests: Migrate `TestContext::new_with_cert_auth` onto `TestContextBuilder` (17/19) ([#2253](https://github.com/redis-rs/redis-rs/pull/2253) by @somechris)
* redis-test: Add `OpensslCommand` (1/5) ([#2255](https://github.com/redis-rs/redis-rs/pull/2255) by @somechris)
* tests: Migrate away from `TestContext::new` (18/19) ([#2256](https://github.com/redis-rs/redis-rs/pull/2256) by @somechris)
* redis-test: Add syntactic sugar to `OpensslCommand` (2/5) ([#2257](https://github.com/redis-rs/redis-rs/pull/2257) by @somechris)
* redis-test: Dedupe TLS key generation (3/5) ([#2260](https://github.com/redis-rs/redis-rs/pull/2260) by @somechris)
* redis-test: Downgrade to release candidate version `2.0.0-rc.1` ([#2264](https://github.com/redis-rs/redis-rs/pull/2264) by @somechris)
* redis-test: Allow to re-use existing TLS keys (4/5) ([#2263](https://github.com/redis-rs/redis-rs/pull/2263) by @somechris)
* Move `TestContext` to `redis-test` (19/19) ([#2261](https://github.com/redis-rs/redis-rs/pull/2261) by @somechris)
* redis-test/TestContext: Break server log lines when panicking ([#2343](https://github.com/redis-rs/redis-rs/pull/2343) by @somechris)
* redis-test/TestContext: Avoid waiting for timeout if server died ([#2348](https://github.com/redis-rs/redis-rs/pull/2348) by @somechris)
* redis-test/RedisServer: Reduce server disk access ([#2375](https://github.com/redis-rs/redis-rs/pull/2375) by @somechris)
* redis-test/RedisServer: De-dupe module loading logic ([#2376](https://github.com/redis-rs/redis-rs/pull/2376) by @somechris)
* redis-test: Harmonize server aliveness check and error messages ([#2360](https://github.com/redis-rs/redis-rs/pull/2360) by @somechris)
* Mark `TestContext` as `non_exhaustive` (14/15) ([#2389](https://github.com/redis-rs/redis-rs/pull/2389) by @somechris)
* redis-test: Refactor parameter passing for building `RedisServer` (server-info 1/3) ([#2398](https://github.com/redis-rs/redis-rs/pull/2398) by @somechris)
* RedisServer: Allow to dump server info during panicking drops (server-info 2/3) ([#2400](https://github.com/redis-rs/redis-rs/pull/2400) by @somechris)
* RedisServer: Add server address to formatted server info (server-info 3/3) ([#2403](https://github.com/redis-rs/redis-rs/pull/2403) by @somechris)
* redis-test: Parametrize server type and protocol for matrix-based tests ([#2392](https://github.com/redis-rs/redis-rs/pull/2392) by @nihohit)
* TestContext: Build from builder instead of passing builder's values ([#2463](https://github.com/redis-rs/redis-rs/pull/2463) by @somechris)
* Move ServerType implementation to declaration ([#2466](https://github.com/redis-rs/redis-rs/pull/2466) by @somechris)
* RedisServer: Drop `log_file` function ([#2465](https://github.com/redis-rs/redis-rs/pull/2465) by @somechris)
* RedisServer: Randomize name of used log file ([#2476](https://github.com/redis-rs/redis-rs/pull/2476) by @somechris)
* Support `tls-native-tls` in `redis-test` ([#2509](https://github.com/redis-rs/redis-rs/pull/2509) by @nihohit)

### 1.0.4 (2026-05-29)

* Improve the redis-test documentation and make it more accessible ([#2082](https://github.com/redis-rs/redis-rs/pull/2082) by @nihohit)

### 1.0.3 (2026-04-04)

* Track redis 1.2.0 release
* Disable snapshotting when running Redis in tests ([#2010](https://github.com/redis-rs/redis-rs/pull/2010) by @somechris)

### 1.0.2 (2026-02-27)

* Add redis! macro to support building Values ([#1966](https://github.com/redis-rs/redis-rs/pull/1966) by @somechris)

### 1.0.1 (2025-12-24)

* MockRedisConnection with cosumation assert ([#1898](https://github.com/redis-rs/redis-rs/pull/1898) by @meierfra-ergon)

### 1.0.0 (2025-12-05)
* Track redis 1.0.0 release

### 0.13.0 (2025-10-06)
* Track redis 1.0.0-rc release

### 0.12.0 (2025-08-15)
* Track redis 0.32.0 release

### 0.11.0 (2025-05-08)
* Track redis 0.31.0 release

### 0.10.0 (2025-04-22)
* Track redis 0.30.0 release

### 0.9.0 (2025-02-16)
* Track redis 0.29.0 release

### 0.8.0 (2025-01-7)
* Track redis 0.28.0 release
* Expose server creation logic through redis-test ([#1363](https://github.com/redis-rs/redis-rs/pull/1363))

### 0.6.0 (2024-09-07)
* Track redis 0.27.0 release

### 0.5.0 (2024-07-26)
* Track redis 0.26.0 release

### 0.4.0 (2023-03-08)
* Track redis 0.25.0 release

### 0.3.0 (2023-12-05)
* Track redis 0.24.0 release

### 0.2.3 (2023-09-01)

* Track redis 0.23.3 release

### 0.2.2 (2023-08-10)

* Track redis 0.23.2 release

### 0.2.1 (2023-07-28)

* Track redis 0.23.1 release

<a name="0.2.0"></a>
### 0.2.0 (2023-04-05)

* Track redis 0.23.0 release

<a name="0.2.0-beta.1"></a>
### 0.2.0-beta.1 (2023-03-28)

* Track redis 0.23.0-beta.1 release

<a name="0.1.1"></a>
### 0.1.1 (2022-10-18)

#### Changes
* Add README
* Update LICENSE file / symlink from parent directory


<a name="0.1.0"></a>
### 0.1.0 (2022-10-05)

This is the initial release of the redis-test crate, which aims to provide mocking
for connections and commands. Thanks @tdyas!

#### Features
*   Testing module with support for mocking redis connections and commands ([#465](https://github.com/redis-rs/redis-rs/pull/465) @tdyas)
