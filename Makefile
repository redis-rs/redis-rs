build:
	@cargo build

test:
	@RUST_TEST_TASKS=1 cargo test

bench:
	@RUST_TEST_TASKS=1 cargo bench

docs: build
	@cargo doc --no-deps

upload-docs: docs
	@./upload-docs.sh

.PHONY: build test bench docs upload-docs
