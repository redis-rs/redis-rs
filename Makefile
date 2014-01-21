all: test

test:
	RUST_TEST_TASKS=1 rustpkg test redis

clean:
	rm -rf build

.PHONY: all test clean
