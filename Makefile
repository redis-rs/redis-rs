build:
	@cargo build

test:
	@echo "===================================================================="
	@echo "Testing Connection Type TCP without features"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp RUST_TEST_THREADS=1 cargo test --no-default-features --tests -- --nocapture

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp RUST_TEST_THREADS=1 cargo test --all-features -- --nocapture

	@echo "===================================================================="
	@echo "Testing Connection Type UNIX"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=unix cargo test --test parser --test test_basic --test test_types --all-features

	@echo "===================================================================="
	@echo "Testing Connection Type UNIX SOCKETS"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=unix cargo test --all-features -- --skip test_cluster

test-single: RUST_TEST_THREADS=1
test-single: test

bench:
	@RUST_TEST_THREADS=1 cargo bench --all-features

docs: build
	@cargo doc --no-deps

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
