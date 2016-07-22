#!/bin/bash

test_run() {
  echo "======================================================================"
  echo -n "Testing Connection Type '$1'"
  if [ x$2 != x ]; then
    echo " [features=$2]"
  else
    echo
  fi
  echo "======================================================================"
  echo
  RUST_TEST_THREADS=1 REDISRS_SERVER_TYPE=$1 cargo test --features="with-rustc-json $2"
}

test_run "tcp"
if grep with-system-unix-sockets target/debug/build/redis-*/output -q; then
  test_run "unix"
fi
test_run "unix" "with-unix-sockets"
