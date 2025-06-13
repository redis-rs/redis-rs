build:
	@RUSTFLAGS="-D warnings" cargo build -F safe_iterators --locked -p redis

test:
	@echo "===================================================================="
	@echo "Build all features with lock file"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" cargo build --locked -p redis -p redis-test --all-features

	@echo "===================================================================="
	@echo "Testing Connection Type TCP without features except safe_iterators"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run --locked -p redis --no-default-features -F safe_iterators -E 'not test(test_module)'

	@echo "===================================================================="
	@echo "Testing Connection Type TCP without safe_iterators, but with async"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings -A deprecated" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run --locked -p redis --no-default-features -F tokio-comp -E 'not test(test_module)'

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and RESP2"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run --locked -p redis --all-features -E 'not test(test_module)'

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and RESP3"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 PROTOCOL=RESP3 cargo nextest run --locked -p redis --all-features -E 'not test(test_module)'

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and Rustls support"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp+tls RUST_BACKTRACE=1 cargo nextest run --locked -p redis --all-features -E 'not test(test_module)'

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with native-TLS support"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp+tls RUST_BACKTRACE=1 cargo nextest run --locked -p redis --features=json,tokio-native-tls-comp,async-std-native-tls-comp,smol-native-tls-comp,connection-manager,cluster-async,safe_iterators -E 'not test(test_module)'

	@echo "===================================================================="
	@echo "Testing Connection Type UNIX"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=unix RUST_BACKTRACE=1 cargo nextest run --locked -p redis --test parser --test test_basic --test test_types --all-features -E 'not test(test_module)'

	@echo "===================================================================="
	@echo "Testing Connection Type UNIX SOCKETS"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=unix RUST_BACKTRACE=1 cargo nextest run --locked -p redis --all-features -E 'not (test(test_module) | test(cluster))'

	@echo "===================================================================="
	@echo "Testing redis-test"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo nextest run --locked -p redis-test


test-module:
	@echo "===================================================================="
	@echo "Testing RESP2 with module support enabled (currently only RedisJSON)"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run -p redis --locked --all-features e -E 'test(test_module)'

	@echo "===================================================================="
	@echo "Testing RESP3 with module support enabled (currently only RedisJSON)"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 RESP3=true cargo nextest run -p redis --all-features e -E 'test(test_module)'

test-single: test

bench:
	cargo bench --all-features

docs:
	@RUSTDOCFLAGS="-D warnings --cfg docsrs" cargo +nightly doc --all-features --no-deps

upload-docs: docs
	@./upload-docs.sh

style-check:
	@rustup component add rustfmt 2> /dev/null
	cargo fmt --all -- --check

lint:
	@rustup component add clippy 2> /dev/null
	cargo clippy --all-features --all --tests --examples -- -D clippy::all -D warnings

fuzz:
	cd afl/parser/ && \
	cargo afl build --bin fuzz-target && \
	cargo afl fuzz -i in -o out ../../target/debug/fuzz-target

.PHONY: build test bench docs upload-docs style-check lint fuzz
