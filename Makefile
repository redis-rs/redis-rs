build:
	@cargo build

test:
	@echo "======================================================================"
	@echo "Testing without unix_socket"
	@echo "======================================================================"
	@echo
	@RUST_TEST_THREADS=1 cargo test --features="with-rustc-json"
	@echo "======================================================================"
	@echo "Testing with unix_socket"
	@echo "======================================================================"
	@echo
	@RUST_TEST_THREADS=1 cargo test --features="with-rustc-json with-unix-sockets"

bench:
	@RUST_TEST_THREADS=1 cargo bench

docs: build
	@cargo doc --no-deps --features=unix_socket

upload-docs: docs
	@./upload-docs.sh

.PHONY: build test bench docs upload-docs
