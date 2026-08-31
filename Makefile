build:
	@RUSTFLAGS="-D warnings" cargo build --locked -p redis

test:
	@echo "===================================================================="
	@echo "Build all features with lock file"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" cargo build --locked -p redis -p redis-test --all-features

	@echo "===================================================================="
	@echo "Testing redis without default features"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo nextest run --locked -p redis --no-default-features --profile no_module

	@echo "===================================================================="
	@echo "Testing redis with all features"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo nextest run --locked -p redis --all-features --profile no_module

	@echo "===================================================================="
	@echo "Testing redis-test without features"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo nextest run --locked -p redis-test --no-default-features --profile no_module

	@echo "===================================================================="
	@echo "Testing redis-test with all features"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo nextest run --locked -p redis-test --all-features --profile no_module

test-module-json:
	@echo "===================================================================="
	@echo "Testing RedisJSON module"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo nextest run -p redis --locked --all-features --profile module_json

test-module-bloom:
	@echo "===================================================================="
	@echo "Testing RedisBloom module"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo nextest run -p redis --locked --all-features --profile module_bloom

test-modules: test-module-json test-module-bloom

test-single: test

bench:
	cargo bench --all-features

docs:
	@RUSTDOCFLAGS="-D warnings --cfg docsrs" cargo +nightly doc --all-features --no-deps

upload-docs: docs
	@./upload-docs.sh

flag-frenzy:
#	# This requires nihohit's flag-frenzy variant from https://github.com/nihohit/flag-frenzy.git
	flag-frenzy --config .config/flag-frenzy --package redis

style-check:
	@rustup component add rustfmt 2> /dev/null
	cargo fmt --all -- --check

lint:
	@rustup component add clippy 2> /dev/null
	cargo clippy --workspace --all-targets --all-features -- -D clippy::all -D warnings

fix:
	@rustup component add rustfmt 2> /dev/null
	@rustup component add clippy 2> /dev/null
	cargo fmt --all
	cargo clippy --workspace --all-targets --all-features --fix --allow-dirty --allow-staged -- -D clippy::all -D warnings

fuzz:
	cd afl/parser/ && \
	cargo afl build --bin fuzz-target && \
	cargo afl fuzz -i in -o out ../../target/debug/fuzz-target

.PHONY: build test bench docs upload-docs style-check lint fuzz
