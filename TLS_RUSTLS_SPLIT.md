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