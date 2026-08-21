# Workplan: Redesign Testing Infrastructure using Proc-Macros

## Objective
Redesign the testing infrastructure in `redis-rs` to eliminate repeated recompilations and multi-run invocations in the `Makefile` driven by `REDISRS_SERVER_TYPE` and `PROTOCOL` environment variables. Replace environment-variable-driven test runs with Rust proc-macros in `test-macros/src/lib.rs` that generate test matrices (different protocols, server/cluster/sentinel types, and async runtimes) directly within the Rust test framework.

all tests that use a test context need to use the macros. If there are edge cases, take them as arguments to the macro - for example, in order to replace #[tokio::test] maybe take a runtime = tokio argument in the macro, in order to specify the runtime.chat

---

## Key Components & Architecture

### 1. Parametrization in `redis-test`
Currently, `RedisServer`, `RedisCluster`, and `RedisSentinelCluster` infer protocol and connection types from `env::var("REDISRS_SERVER_TYPE")` and `env::var("PROTOCOL")`.
- **Refactoring Requirement**:
  - Expose explicit builder / constructor options for `ServerType` (`Tcp`, `TcpTls`, `Unix`), `ProtocolVersion` (`RESP2`, `RESP3`), and `ClusterType`.
  - Update `TestContext`, `TestClusterContext`, and `TestSentinelContext` to accept these explicit options so proc-macros can instantiate contexts programmatically for any matrix variant.

---

### 2. Proc-Macro Expansion in `test-macros/src/lib.rs`
Implement procedural macros generating test matrices for the 6 required target topologies:

#### A. Single Server (`#[single_server_test]`)
- **Matrix**: Protocol (`RESP2`, `RESP3`) × Server Type (`TCP`, `TCP+TLS`, `UNIX`).
- **Behavior**: Generates sync test variants that initialize a `TestContext` for each combination and pass `TestContext` (or connection) to the test body.

#### B. Async Single Server (`#[async_single_server_test]`)
- **Matrix**: Protocol (`RESP2`, `RESP3`) × Server Type (`TCP`, `TCP+TLS`, `UNIX`) × Async Runtime (`Tokio`, `Smol`).
- **Behavior**: Generates async test variants wrapping `block_on_all` with the corresponding runtime and created `TestContext`.

#### C. Cluster (`#[cluster_test]`)
- **Matrix**: Protocol (`RESP2`, `RESP3`) × Server/TLS configuration (`TCP`, `TCP+TLS`; Unix socket is not supported for Sentinel).
- **Behavior**: Generates sync test variants initializing a `TestClusterContext` and running the test block.

#### D. Async Cluster (`#[async_cluster_test]`)
- **Matrix**: Protocol (`RESP2`, `RESP3`) × Server/TLS configuration (`TCP`, `TCP+TLS`; Unix socket is not supported for Sentinel) × Async Runtime (`Tokio`, `Smol`).
- **Behavior**: Generates async cluster test variants initializing a `TestClusterContext` and executing under the runtime wrapper.

#### E. Sentinel (`#[sentinel_test]`)
- **Matrix**: Protocol (`RESP2`, `RESP3`) × Server/TLS configuration (`TCP`, `TCP+TLS`; Unix socket is not supported for Sentinel).
- **Behavior**: Generates sync sentinel test variants initializing `TestSentinelContext`.

#### F. Async Sentinel (`#[async_sentinel_test]`)
- **Matrix**: Protocol (`RESP2`, `RESP3`) × Server/TLS configuration (`TCP`, `TCP+TLS`; Unix socket is not supported for Sentinel) × Async Runtime (`Tokio`, `Smol`).
- **Behavior**: Generates async sentinel test variants initializing `TestSentinelContext`.

---

### 3. Test Suite Migration
Migrate existing test files in `redis/tests/` to consume the new macro attributes:
- `redis/tests/test_basic.rs` & `test_geospatial.rs` -> `#[single_server_test]`
- `redis/tests/test_async.rs` -> `#[async_single_server_test]`
- `redis/tests/test_cluster.rs` -> `#[cluster_test]`
- `redis/tests/test_cluster_async.rs` -> `#[async_cluster_test]`
- `redis/tests/test_sentinel.rs` -> `#[sentinel_test]` (if present) / async sentinel tests -> `#[async_sentinel_test]`

---

### 4. Makefile Simplification
Remove duplicate `cargo nextest run` invocations with distinct `REDISRS_SERVER_TYPE` and `PROTOCOL` environment variable pairs from `Makefile`.
- **New Makefile structure**:
  - Run a unified `cargo nextest run` command for test suites.
  - Retain feature flag targets (e.g. `--all-features`, `--no-default-features`) while eliminating redundant env-var loops.

---

## Detailed Step-by-Step Action Items

1. **Step 1: Parametrize `redis-test` Context Constructors**
   - Update `RedisServerBuilder` & `TestContextBuilder` in `redis-test` to accept explicit `ProtocolVersion` and `ServerType`.
   - Update `RedisClusterConfiguration` and `RedisSentinelCluster` to accept explicit `ProtocolVersion` and server options.

2. **Step 2: Implement Proc-Macros in `test-macros/src/lib.rs`**
   - Create attribute macros:
     - `#[single_server_test]`
     - `#[async_single_server_test]`
     - `#[cluster_test]`
     - `#[async_cluster_test]`
     - `#[sentinel_test]`
     - `#[async_sentinel_test]`
   - Add macro argument parsing for special cases (e.g., modules like `Module::Json` / `Module::Bloom`, custom cluster configs).

3. **Step 3: Refactor Tests to Use New Proc-Macros**
   - Update single-server sync & async tests.
   - Update cluster sync & async tests.
   - Update sentinel sync & async tests.

4. **Step 4: Streamline `Makefile`**
   - Clean up `Makefile` test rules (`test`, `test-module-json`, `test-module-bloom`).

5. **Step 5: Verification & Quality Assurance**
   - Run `cargo check --tests --all-features`.
   - Run `cargo nextest run --all-features` to verify that all matrix test cases execute and pass.
