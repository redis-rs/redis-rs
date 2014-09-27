test:
	@RUST_TEST_TASKS=1 cargo test

doc:
	@cargo doc

.PHONY: test doc
