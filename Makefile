build:
	@cargo build

test:
	@echo "===================================================================="
	@echo "Testing Connection Type TCP"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=tcp RUST_TEST_THREADS=1 cargo test
	@echo "Testing Connection Type UNIX"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=unix cargo test --test parser --test test_basic --test test_types
	@echo "===================================================================="
	@echo "Testing Connection Type UNIX SOCKETS"
	@echo "===================================================================="
	@REDISRS_SERVER_TYPE=unix cargo test

test-single: RUST_TEST_THREADS=1
test-single: test

bench:
	@RUST_TEST_THREADS=1 cargo bench

docs: build
	@cargo doc --no-deps

upload-docs: docs
	@./upload-docs.sh

style-check:
	@rustup component add rustfmt --toolchain stable 2> /dev/null
	cargo +stable style-check -- --check

lint:
	@rustup component add clippy --toolchain stable 2> /dev/null
	cargo clippy --all-features

.PHONY: build test bench docs upload-docs style-check lint
