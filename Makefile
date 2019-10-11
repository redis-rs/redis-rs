build:
	@cargo build

test:
	@echo "===================================================================="
	@echo "Testing Connection Type TCP"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp RUST_TEST_THREADS=1 cargo test --all-features
	@echo "Testing Connection Type UNIX"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=unix cargo test --test parser --test test_basic --test test_types --all-features
	@echo "===================================================================="
	@echo "Testing Connection Type UNIX SOCKETS"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=unix cargo test --all-features

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
	cargo clippy --all-features

.PHONY: build test bench docs upload-docs style-check lint
