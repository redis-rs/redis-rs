#!/bin/bash

test_run() {
  echo "======================================================================"
  echo -n "Testing Connection Type '$1'"
  if [ x$3 != x ]; then
    echo " [features=$3]"
  else
    echo
  fi
  echo "======================================================================"

  if [ x$REDISRS_UNIFIED_TARGET != x1 ]; then
    export CARGO_TARGET_DIR="target/test-$2"
  fi

  REDISRS_SERVER_TYPE=$1 cargo test --features="with-rustc-json $3"
}

test_run "tcp" "tcp-basic"
if grep with-system-unix-sockets target/debug/build/redis-*/output -q; then
  test_run "unix" "unix-basic"
fi
test_run "unix" "unix-dep" "with-unix-sockets"
