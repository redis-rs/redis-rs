# Run the test suites once per rustls root-certificate store. The two store features are mutually
# exclusive, so each store variant runs the full set of features except the other store's feature.
# The lists must be passed explicitly, not via `--all-features`, because cargo's `--all-features`
# would enable both stores. The feature lists are derived from `cargo metadata` (i.e. the same set
# that `--all-features` would enable, including implicit features of optional dependencies), minus
# the excluded store feature. `cargo` and `cargo nextest` both accept space-separated features.
# The variables are lazy (`=`), so plain targets like `build` never run `cargo metadata`/python.
PYTHON = $(shell command -v python3 2>/dev/null || command -v python 2>/dev/null)
REDIS_ALL_FEATURES = $(shell cargo metadata --no-deps --format-version 1 | $(PYTHON) -c 'import json,sys; d=json.load(sys.stdin); p=[x for x in d["packages"] if x["name"]=="redis"][0]; print(" ".join(sorted(p["features"])))')
WEBPKI_FEATURES = $(filter-out tls-rustls-native-roots,$(REDIS_ALL_FEATURES))
NATIVE_FEATURES = $(filter-out tls-rustls-webpki-roots,$(REDIS_ALL_FEATURES))

build:
	@RUSTFLAGS="-D warnings" cargo build --locked -p redis

# The `--all-features` build is split per store, because the two store features are mutually exclusive.
build-all:
	@RUSTFLAGS="-D warnings" cargo build --locked -p redis --features "$(WEBPKI_FEATURES)"
	@RUSTFLAGS="-D warnings" cargo build --locked -p redis --features "$(NATIVE_FEATURES)"

.PHONY: test
test: test-rustls-store test-native-tls

# The `tls-rustls-webpki-roots`/`tls-rustls-native-roots` features are mutually exclusive, so the
# all-features suites must be run once per store. Because `-p redis` also builds the `redis-test`
# dev-dependency, the two store features would both be enabled under `--all-features`; we therefore
# pass the merged feature set of one store explicitly.
test-rustls-store:
	@echo "===================================================================="; \
	echo "Testing with rustls via webpki-roots (all other features)"; \
	echo "===================================================================="; \
	echo "Build all features with lock file"; \
	RUSTFLAGS="-D warnings" cargo build --locked -p redis --features "$(WEBPKI_FEATURES)"; \
	echo "Testing Connection Type TCP with all features and RESP2"; \
	RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run --locked -p redis --profile tcp --features "$(WEBPKI_FEATURES)"; \
	echo "Testing Connection Type TCP with all features and RESP3"; \
	RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 PROTOCOL=RESP3 cargo nextest run --locked -p redis --profile tcp --features "$(WEBPKI_FEATURES)"; \
	echo "Testing Connection Type TCP with all features and Rustls support"; \
	RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp+tls RUST_BACKTRACE=1 cargo nextest run --locked -p redis --profile tcp_tls --features "$(WEBPKI_FEATURES)"; \
	echo "Testing Connection Type UNIX SOCKETS"; \
	RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=unix RUST_BACKTRACE=1 cargo nextest run --locked -p redis --profile unix --features "$(WEBPKI_FEATURES)"; \
	echo "Testing Connection Type UNIX SOCKETS and RESP3"; \
	RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=unix PROTOCOL=RESP3 RUST_BACKTRACE=1 cargo nextest run --locked -p redis --profile unix --features "$(WEBPKI_FEATURES)"; \
	echo "Testing redis-test with webpki roots"; \
	RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo nextest run --locked -p redis-test --features "tls-rustls-webpki-roots,aio,tls-rustls-insecure,tokio-rustls-comp"
	@echo "===================================================================="; \
	echo "Testing with rustls via native-roots (all other features)"; \
	echo "===================================================================="; \
	echo "Build all features with lock file"; \
	RUSTFLAGS="-D warnings" cargo build --locked -p redis --features "$(NATIVE_FEATURES)"; \
	echo "Testing Connection Type TCP with all features and RESP2"; \
	RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run --locked -p redis --profile tcp --features "$(NATIVE_FEATURES)"; \
	echo "Testing Connection Type TCP with all features and RESP3"; \
	RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 PROTOCOL=RESP3 cargo nextest run --locked -p redis --profile tcp --features "$(NATIVE_FEATURES)"; \
	echo "Testing Connection Type TCP with all features and Rustls support"; \
	RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp+tls RUST_BACKTRACE=1 cargo nextest run --locked -p redis --profile tcp_tls --features "$(NATIVE_FEATURES)"; \
	echo "Testing Connection Type UNIX SOCKETS"; \
	RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=unix RUST_BACKTRACE=1 cargo nextest run --locked -p redis --profile unix --features "$(NATIVE_FEATURES)"; \
	echo "Testing Connection Type UNIX SOCKETS and RESP3"; \
	RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=unix PROTOCOL=RESP3 RUST_BACKTRACE=1 cargo nextest run --locked -p redis --profile unix --features "$(NATIVE_FEATURES)"; \
	echo "Testing redis-test with native roots"; \
	RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo nextest run --locked -p redis-test --features "tls-rustls-native-roots,aio,tls-rustls-insecure,tokio-rustls-comp"

test-native-tls:
	@echo "===================================================================="
	@echo "Testing Connection Type TCP without features"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run --locked -p redis --no-default-features --profile tcp

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with native-TLS support"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp+tls RUST_BACKTRACE=1 cargo nextest run --locked -p redis --features=json,tokio-native-tls-comp,smol-native-tls-comp,connection-manager,cluster-async --profile tcp_tls

	@echo "===================================================================="
	@echo "Testing redis-test without features"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo nextest run --locked -p redis-test --no-default-features

test-module-json:
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run -p redis --locked --features "$(WEBPKI_FEATURES)" --profile module_json
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 PROTOCOL=RESP3 cargo nextest run -p redis --locked --features "$(WEBPKI_FEATURES)" --profile module_json
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run -p redis --locked --features "$(NATIVE_FEATURES)" --profile module_json
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 PROTOCOL=RESP3 cargo nextest run -p redis --locked --features "$(NATIVE_FEATURES)" --profile module_json

test-module-bloom:
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run -p redis --locked --features "$(WEBPKI_FEATURES)" --profile module_bloom
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 PROTOCOL=RESP3 cargo nextest run -p redis --locked --features "$(WEBPKI_FEATURES)" --profile module_bloom
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run -p redis --locked --features "$(NATIVE_FEATURES)" --profile module_bloom
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 PROTOCOL=RESP3 cargo nextest run -p redis --locked --features "$(NATIVE_FEATURES)" --profile module_bloom

test-modules: test-module-json test-module-bloom

test-single: test

bench:
	cargo bench -p redis --features "$(WEBPKI_FEATURES)" $(BENCH_ARGS)
	cargo bench -p redis --features "$(NATIVE_FEATURES)" $(BENCH_ARGS)

# The `--all-features` doc builds are split per store. Run these on stable with the workflow's doc
# job, which relies on `--no-deps --document-private-items`.
doc-check:
	@RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items --features "$(WEBPKI_FEATURES)"
	@RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items --features "$(NATIVE_FEATURES)"

# The `--all-features` doc-tests are split per store.
doc-tests:
	@RUSTDOCFLAGS="-D warnings" cargo test --doc --locked --features "$(WEBPKI_FEATURES)"
	@RUSTDOCFLAGS="-D warnings" cargo test --doc --locked --features "$(NATIVE_FEATURES)"

docs:
	@RUSTDOCFLAGS="-D warnings --cfg docsrs" cargo +nightly doc --no-deps --features "$(WEBPKI_FEATURES)"
	@RUSTDOCFLAGS="-D warnings --cfg docsrs" cargo +nightly doc --no-deps --features "$(NATIVE_FEATURES)"

upload-docs: docs
	@./upload-docs.sh

flag-frenzy:
#	# This requires nihohit's flag-frenzy variant from https://github.com/nihohit/flag-frenzy.git
	flag-frenzy --config .config/flag-frenzy --package redis

style-check:
	@rustup component add rustfmt 2> /dev/null
	cargo fmt --all -- --check

lint:
	@rustup component add clippy 2> /dev/null
	cargo clippy --workspace --all-targets --features "$(WEBPKI_FEATURES)" -- -D clippy::all -D warnings
	cargo clippy --workspace --all-targets --features "$(NATIVE_FEATURES)" -- -D clippy::all -D warnings

fix:
	@rustup component add rustfmt 2> /dev/null
	@rustup component add clippy 2> /dev/null
	cargo fmt --all
	cargo clippy --workspace --all-targets --features "$(WEBPKI_FEATURES)" --fix --allow-dirty --allow-staged -- -D clippy::all -D warnings
	cargo clippy --workspace --all-targets --features "$(NATIVE_FEATURES)" --fix --allow-dirty --allow-staged -- -D clippy::all -D warnings

fuzz:
	cd afl/parser/ && \
	cargo afl build --bin fuzz-target && \
	cargo afl fuzz -i in -o out ../../target/debug/fuzz-target

.PHONY: build build-all test test-rustls-store test-native-tls bench docs doc-check doc-tests upload-docs style-check lint fuzz test-module-json test-module-bloom test-modules test-single