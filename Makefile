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
	@RUST_TEST_THREADS=1 cargo test --features="with-rustc-json with-unix-sockets test-unix-sockets"
	@echo "======================================================================"
	@echo "Testing with unix_socket (built in)"
	@echo "======================================================================"
	@echo
	@RUST_TEST_THREADS=1 cargo test --features="with-rustc-json test-unix-sockets"

bench:
	@RUST_TEST_THREADS=1 cargo bench

docs: build
	@cargo doc --no-deps

upload-docs: docs
	@./upload-docs.sh

.PHONY: build test bench docs upload-docs
