test:
	@RUST_TEST_TASKS=1 cargo test

docs:
	@cargo doc

upload-docs: docs
	@./upload-docs.sh

.PHONY: test docs
