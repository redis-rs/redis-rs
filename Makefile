build:
	@cargo build

test:
	@echo "===================================================================="
	@echo "Build all features with lock file"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" cargo build --locked --all-features

	@echo "===================================================================="
	@echo "Testing Connection Type TCP without features"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo test --locked -p redis --no-default-features -- --nocapture --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and RESP2"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo test --locked -p redis --all-features -- --nocapture --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and RESP3"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp PROTOCOL=RESP3 cargo test -p redis --all-features -- --nocapture --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and Rustls support"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp+tls RUST_BACKTRACE=1 cargo test --locked -p redis --all-features -- --nocapture --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and native-TLS support"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp+tls RUST_BACKTRACE=1 cargo test --locked -p redis --features=json,tokio-native-tls-comp,connection-manager,cluster-async -- --nocapture --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing Connection Type UNIX"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=unix RUST_BACKTRACE=1 cargo test --locked -p redis --test parser --test test_basic --test test_types --all-features -- --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing Connection Type UNIX SOCKETS"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=unix RUST_BACKTRACE=1 cargo test --locked -p redis --all-features -- --test-threads=1 --skip test_cluster --skip test_async_cluster --skip test_module

	@echo "===================================================================="
	@echo "Testing async-std with Rustls"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo test --locked -p redis --features=async-std-rustls-comp,cluster-async -- --nocapture --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing async-std with native-TLS"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo test --locked -p redis --features=async-std-native-tls-comp,cluster-async -- --nocapture --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing redis-test"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo test --locked -p redis-test 


test-module:
	@echo "===================================================================="
	@echo "Testing RESP2 with module support enabled (currently only RedisJSON)"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo test --locked --all-features test_module -- --test-threads=1

	@echo "===================================================================="
	@echo "Testing RESP3 with module support enabled (currently only RedisJSON)"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 RESP3=true cargo test --all-features test_module -- --test-threads=1

test-single: test

bench:
	cargo bench --all-features

docs:
	@RUSTFLAGS="-D warnings" RUSTDOCFLAGS="--cfg docsrs" cargo +nightly doc --all-features --no-deps

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
	cargo afl fuzz -i in -o out target/debug/fuzz-target

.PHONY: build test bench docs upload-docs style-check lint fuzz
