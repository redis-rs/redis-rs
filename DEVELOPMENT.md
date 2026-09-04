# Development

Be sure to also read the [contribution docs](CONTRIBUTING.md) and [coding conventions](CODING_CONVENTIONS.md) before working on a pull request.


## Development setup

To develop, you need:

* Rust >= 1.88 (and to build docs also the `nightly` toolchain because of rust-lang/rust#43781)
* Make (to run `make` commands)
* git (to fetch/upload code)
* nextest (to run tests)
* Redis (to run tests)
* afl (for fuzzing; only relevant if you're working on the parsing logic of the internal Redis protocol)

On Debian-like systems, the following command should install all you need:

```
sudo apt install --assume-yes rustup build-essential pkg-config libssl-dev git \
  redis redis-server redis-sentinel redis-tools && \
rustup default stable && \
rustup update nightly && \
cargo install --locked cargo-nextest && \
cargo install --locked cargo-afl
```

(The `redis-*` packages enable services that are not needed for `redis-rs`.
To disable them run `sudo systemctl disable --now redis-server && sudo systemctl disable --now redis-sentinel` )

Once the required software above is installed, you can fetch `redis-rs` and the tests should pass:

```
git clone https://github.com/redis-rs/redis-rs && \
cd redis-rs && \
make test
```

You're all set. Happy hacking!


## Building

To build the core crate, run `make build`


## Running tests

| Command | Description |
| --- | --- |
| `make test` | Runs basic tests. This does not include doc tests or module tests |
| `make test-modules` | Runs all module tests |
| `make test-module-bloom` | Runs tests for the `bloom` module |
| `make test-module-json` | Runs tests for the `json` module |
| `cargo test --doc --locked --all-features` | Runs doc tests |

The tests need to be able to find Redis' tools and the modules. If automatic detection fails, use the following environment variables to guide the test suite:

| EnvVar Name | Description |
| --- | --- |
| `REDISRS_SERVER_BIN` | Binary to start Redis |
| `REDISRS_MODULE_BLOOM_PATH` | Path to the `bloom` module |
| `REDISRS_MODULE_JSON_PATH` | Path to the `json` module |

### Speeding up TLS tests

TLS tests per-default create fresh keys for each tests. On entropy/resource-limited hosts, this might be time consuming.

To instead re-use the same keys, first generate the needed keys beforehand (this needs to be done only once):

```sh
mkdir -p "$HOME/redis-rs-keys"
openssl genrsa -out "$HOME/redis-rs-keys/ca.key" 4096
openssl genrsa -out "$HOME/redis-rs-keys/client.key" 2048
openssl genrsa -out "$HOME/redis-rs-keys/server.key" 2048
```

Then tell your environment to use them for testing (this needs to be executed in each terminal that runs the tests):

```sh
export REDISRS_TLS_KEY_CA="$HOME/redis-rs-keys/ca.key"
export REDISRS_TLS_KEY_CLIENT="$HOME/redis-rs-keys/client.key"
export REDISRS_TLS_KEY_SERVER="$HOME/redis-rs-keys/server.key"
```

Now `make test` will pick the pre-generated keys up, and TLS tests should breeze through.


## Building documentation

To build the documentation, run `make docs`

## Linting

To lint the code, run `make style-check lint`

If there are linting issues, running `make fix` tries to auto-correct issues.

## Running a fuzzer

This is only relevant if you're working on the parsing logic of the internal Redis protocol.

To start the fuzzer, run `make fuzz`

If the fuzzer finds a crash, in order to reproduce it, run:

```
cd afl/<target>/
cargo run --bin reproduce -- out/crashes/<crashfile>
```
