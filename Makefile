build:
	@RUSTFLAGS="-D warnings" cargo build --locked -p redis

test:
	@echo "===================================================================="
	@echo "Build all features with lock file"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" cargo build --locked -p redis -p redis-test --all-features

	@echo "===================================================================="
	@echo "Testing Connection Type TCP without features"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run --locked -p redis --no-default-features -E 'not (test(test_module) | binary(test_ft_create))'

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and RESP2"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run --locked -p redis --all-features -E 'not (test(test_module) | binary(test_ft_create))'

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and RESP3"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 PROTOCOL=RESP3 cargo nextest run --locked -p redis --all-features -E 'not (test(test_module) | binary(test_ft_create))'

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with all features and Rustls support"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp+tls RUST_BACKTRACE=1 cargo nextest run --locked -p redis --all-features -E 'not (test(test_module) | binary(test_ft_create))'

	@echo "===================================================================="
	@echo "Testing Connection Type TCP with native-TLS support"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp+tls RUST_BACKTRACE=1 cargo nextest run --locked -p redis --features=json,tokio-native-tls-comp,smol-native-tls-comp,connection-manager,cluster-async -E 'not (test(test_module) | binary(test_ft_create))'

	@echo "===================================================================="
	@echo "Testing Connection Type UNIX SOCKETS"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=unix RUST_BACKTRACE=1 cargo nextest run --locked -p redis --all-features -E 'not (test(test_module) | binary(test_ft_create) | test(cluster))'

	@echo "===================================================================="
	@echo "Testing redis-test"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" RUST_BACKTRACE=1 cargo nextest run --locked -p redis-test


test-module-json:
	@echo "===================================================================="
	@echo "Testing RESP2 with RedisJSON module support enabled"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run -p redis --locked --all-features e -E 'test(test_module_json)'

	@echo "===================================================================="
	@echo "Testing RESP3 with RedisJSON module support enabled"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 PROTOCOL=RESP3 cargo nextest run -p redis --all-features e -E 'test(test_module_json)'

test-module-search:
	@echo "===================================================================="
	@echo "Testing RediSearch module support"
	@echo "===================================================================="
	@RUSTFLAGS="-D warnings" REDISRS_SERVER_TYPE=tcp RUST_BACKTRACE=1 cargo nextest run -p redis --locked --all-features -E 'binary(test_ft_create)'

test-single: test

bench:
	cargo bench --all-features

docs:
	@RUSTDOCFLAGS="-D warnings --cfg docsrs" cargo +nightly doc --all-features --no-deps

upload-docs: docs
	@./upload-docs.sh

style-check:
	@rustup component add rustfmt 2> /dev/null
	cargo fmt --all -- --check

lint:
	@rustup component add clippy 2> /dev/null
	cargo clippy --all-features --all --tests --examples -- -D clippy::all -D warnings

fuzz:
	cd afl/parser/ && \
	cargo afl build --bin fuzz-target && \
	cargo afl fuzz -i in -o out ../../target/debug/fuzz-target

.PHONY: build test bench docs upload-docs style-check lint fuzz
