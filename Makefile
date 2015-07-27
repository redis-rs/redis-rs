build:
	@cargo build

test:
	@RUST_TEST_THREADS=1 cargo test
	@RUST_TEST_THREADS=1 cargo test --features=unix_socket

bench:
	@RUST_TEST_THREADS=1 cargo bench

docs: build
	@cargo doc --no-deps --features=unix_socket

upload-docs: docs
	@./upload-docs.sh

.PHONY: build test bench docs upload-docs
