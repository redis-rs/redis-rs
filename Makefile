build:
	@cargo build

test:
	@RUST_TEST_TASKS=1 cargo test

docs:
	@cargo doc --no-deps

upload-docs: docs
	@./upload-docs.sh

download-commands:
	curl https://raw.githubusercontent.com/antirez/redis-doc/master/commands.json > src/redis/commands.json

.PHONY: build test docs upload-docs
