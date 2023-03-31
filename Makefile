build:
	@cargo build

test:

	@echo "===================================================================="
	@echo "Testing Connection Type TCP without features"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp cargo test -p redis --no-default-features -- --nocapture --test-threads=1

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp cargo test -p redis --all-features -- --nocapture --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and Rustls support"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp+tls cargo test -p redis --all-features -- --nocapture --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and native-TLS support"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp+tls cargo test -p redis --features=json,tokio-native-tls-comp,connection-manager,cluster-async -- --nocapture --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing Connection Type UNIX"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=unix cargo test -p redis --test parser --test test_basic --test test_types --all-features -- --test-threads=1 --skip test_module

	@echo "===================================================================="
	@echo "Testing Connection Type UNIX SOCKETS"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=unix cargo test -p redis --all-features -- --skip test_cluster --skip test_async_cluster --skip test_module

	@echo "===================================================================="
	@echo "Testing async-std with Rustls"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp cargo test -p redis --features=async-std-rustls-comp,cluster-async -- --nocapture --test-threads=1

	@echo "===================================================================="
	@echo "Testing async-std with native-TLS"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp cargo test -p redis --features=async-std-native-tls-comp,cluster-async -- --nocapture --test-threads=1

	@echo "===================================================================="
	@echo "Testing redis-test"
	@echo "===================================================================="
	@cargo test -p redis-test 


test-module:
	@echo "===================================================================="
	@echo "Testing with module support enabled (currently only RedisJSON)"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp cargo test --all-features test_module

test-single: test

bench:
	cargo bench --all-features

docs:
	@RUSTDOCFLAGS="--cfg docsrs" cargo +nightly doc --all-features --no-deps

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
