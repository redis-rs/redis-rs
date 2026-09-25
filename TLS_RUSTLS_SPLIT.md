# Split rustls root-certificate store features — redis-rs #2297

Handoff / instruction document for finishing the change. The work lives on branch
`native-tls` in this repo; the core changes are already committed in `1c7efbf3 "t"`.

## Goal

`tls-rustls-webpki-roots` still links `rustls-native-certs`, which breaks
cross-compilation to Apple targets (issue
[#2297](https://github.com/redis-rs/redis-rs/issues/2297)). Fix: split the rustls
root-certificate store selection into **mutually exclusive** features, so users can
pick a store explicitly instead of silently getting native certs.

## Approved design

- `tls-rustls` — base feature only; requires exactly one store feature (guarded by `compile_error!`).
- `tls-rustls-native-roots` — `tls-rustls` + `rustls-native-certs`.
- `tls-rustls-webpki-roots` — `tls-rustls` + `webpki-roots`.
- Enabling both stores at once is a `compile_error!`, **except** on `docs.rs`
  (guarded by `not(docsrs)`), because docs.rs builds with `--all-features`.
- `tls-rustls-insecure` does **not** need a store (it uses `NoCertificateVerification`).
- Mirror the split + guards in `redis-test`.

## Status: already done (commit `1c7efbf3 "t"`)

- `redis/Cargo.toml` — split features; `tls-rustls` no longer pulls in `rustls-native-certs`.
- `redis/src/lib.rs` + `redis-test/src/lib.rs` — `compile_error!` guards.
- `redis/src/connection.rs` — `create_rustls_config` store selection keyed off the store features.
- `redis/src/errors/redis_error.rs` — `From<rustls_native_certs::Error>` gated on
  `tls-rustls-native-roots`.
- `Makefile` — rewritten: derives the redis feature list from
  `cargo metadata --no-deps` (portable `python`/`python3` lookup), drops `cargo-hack`
  (it was a pure pass-through with the explicit list), splits suites per store
  (`build-all`, `test-rustls-store`, `test-native-tls`, `doc-check`, `doc-tests`, `bench`),
  and fixes the `tcp+tls` nextest runs to use `--profile tcp_tls`.
- `.config/flag-frenzy/redis.toml` — store-mutual-exclusion + comp rules.
- `.github/workflows/rust.yml`, `.github/actions/lint-and-check/action.yml` — use the new
  make targets; benchmark target derives features only when the split features exist.
- `redis-test/Cargo.toml` + `redis-test/src/lib.rs` — store features + guards.

Feature-list derivation (Makefile lines 8-11):
`REDIS_ALL_FEATURES` from `cargo metadata` includes implicit features (`ahash`,
`bigdecimal`, `bytes`, `default`, `hashbrown`, `rust_decimal`, `uuid`); `WEBPKI_FEATURES`
and `NATIVE_FEATURES` are the all-features list minus the other store.

## Remaining work (do these next)

1. **Fix: `tls-rustls-insecure` without a store no longer compiles.** The new
   `compile_error!` does not exempt insecure mode, which needs no root store.
   Repro:
   ```sh
   cargo check -p redis --no-default-features --features tls-rustls-insecure,tokio-rustls-comp
   # error: the `tls-rustls` feature requires a root certificate store ... (redis/src/lib.rs:653)
   ```
   Add `not(feature = "tls-rustls-insecure")` to the store-required guard in
   `redis/src/lib.rs` *and* `redis-test/src/lib.rs` (and re-check the docs.rs exclusivity
   guard phrasing).
   Note: with store + insecure the build is fine (verified).

2. **README.md** — the feature list (line ~154-155) documents
   `tls-rustls-webpki-roots` but not `tls-rustls-native-roots`; document both + the
   mutual-exclusion requirement. Also consider the complete `tls-rustls` wording now that
   it no longer implies native certs.

3. **Confirm intended breaking behavior** — bare `tokio-rustls-comp` / `smol-rustls-comp`
   (i.e. `tls-rustls` with no store) now fail to compile without an explicit store. That is
   the point of #2297, but it is a breaking change for downstream users; call it out in the
   PR description and make sure the CI matrix exercises both stores explicitly.

4. **Verify docs.rs path** — `cargo doc --all-features` locally with
   `RUSTDOCFLAGS="--cfg docsrs"` should skip the exclusivity error.

## Verification commands

```sh
# builds (both stores)
make build-all

# full webpki + native test suites (needs a local redis-server / make server tooling)
make test-rustls-store
make test-native-tls

# lint / docs / bench
make lint
make doc-check
make doc-tests
make bench

# feature-combination sanity
make flag-frenzy

# individual checks the previous session confirmed working
cargo check -p redis --no-default-features --features tls-rustls-webpki-roots
cargo check -p redis --no-default-features --features tls-rustls-native-roots
cargo hack check -p redis-test --locked -F tls-rustls-webpki-roots,aio,tls-rustls-insecure,tokio-rustls-comp
cargo hack check -p redis-test --locked -F tls-rustls-native-roots,aio,tls-rustls-insecure,tokio-rustls-comp
```

## Notes / gotchas

- `tls-rustls` previously defaulted to native certs; after this change selecting it alone
  is a compile error (except insecure/docs.rs).
- nextest profiles: `tcp` excludes `test(tls)`; `tcp_tls` includes TLS tests. The previous
  Makefile rewrite had regressed the `tcp+tls` runs to `--profile tcp`; commit `1c7efbf3`
  fixes this to `tcp_tls`.
- The old session that produced this commit degraded into corrupted model output (see
  discussion); its concrete conclusions above are taken from its coherent state and
  re-verified where possible. The insecure/no-store repro in "Remaining work 1" was
  reproduced on this checkout.

## Addendum: remaining work completed (uncommitted)

The four items under "Remaining work" are done; the diff is NOT committed (3 files:
`redis/src/lib.rs`, `redis-test/src/lib.rs`, `README.md`).

### 1. `tls-rustls-insecure` without a store now compiles

Added `not(feature = "tls-rustls-insecure")` to the store-required guard in
`redis/src/lib.rs` and `redis-test/src/lib.rs`. The docs.rs exclusivity guard
(`not(docsrs)`) phrasing was re-checked and left as committed.

Verified:
- `cargo check -p redis --no-default-features --features tls-rustls-insecure,tokio-rustls-comp` — OK
  (the failing repro from "Remaining work 1" is fixed).
- `cargo check -p redis-test -F tls-rustls-insecure,aio,tokio-rustls-comp --no-default-features` — OK.
- Store + insecure still compiles.
- Bare `tls-rustls` (no store, no insecure) still fails to compile — the intended break persists,
  in both `redis` and `redis-test`.
- Both stores together still fail the exclusivity `compile_error!` (outside docs.rs).

### 2. README.md updated

- The rustls root-certificate store selection is now documented as a choice between the mutually
  exclusive `tls-rustls-native-roots` (platform native certs) and `tls-rustls-webpki-roots`
  (Mozilla roots).
- Added the mutual-exclusion / no-bare-`tls-rustls` / insecure-exception wording.
- The "To use rustls" examples now pick a store explicitly (`tls-rustls-native-roots` used in the
  samples).

### 3. Breaking behavior confirmed

- `tls-rustls` / `tokio-rustls-comp` / `smol-rustls-comp` without a store are now compile errors
  (issue #2297's intent). Call this out in the PR body.
- CI exercises both stores: `make test` runs `test-rustls-store` (which runs the full suites once
  per store) plus `test-native-tls`; lint/docs/bench also run once per store.

### 4. docs.rs path verified

- `RUSTDOCFLAGS="--cfg docsrs" cargo +nightly doc -p redis --all-features --no-deps` — OK
  (needs nightly because of the existing `#![feature(doc_cfg, rustdoc_internals)]`).
- Without `--cfg docsrs`, `cargo doc --all-features` correctly fails on the exclusivity guard.
- `cargo doc` on stable with `docsrs` cfg fails on the pre-existing `E0554` (unstable `#![feature]`
  on stable), unrelated to this change.

### Verification run on this checkout

- `make build-all` — OK (both stores).
- `make lint` — OK (both stores, `-D warnings`).
- `make doc-check`, `make doc-tests` — OK (both stores).
- `make test-rustls-store` — OK (redis + redis-test suites, both stores, against local
  `redis-server` 8.0.2, nextest incl. `--profile tcp_tls`).
- Bench binaries built and listed for both store feature derivations (full `make bench` run left to a
  machine with more time; CI's benchmark job covers it).
- `cargo fmt --all -- --check` — clean (fmt reformatted the two multi-line `#[cfg]` guards).

### Notes

- No `cargo-hack` check was needed for the fix; the doc's hack commands were run pre-fix.
- No commit was created; the branch still ends at `e77d3834` (+ the instruction file's edits).