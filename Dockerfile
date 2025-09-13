# Minimal Dockerfile to run redis-rs tests in a Linux container, aligned with CI
# Base on Ubuntu 24.04 and install exact Rust toolchain(s)

ARG UBUNTU_VERSION=24.04
FROM ubuntu:${UBUNTU_VERSION} AS base

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git build-essential pkg-config \
    cmake clang \
    openssl libssl-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Install rustup and requested toolchain(s)
ARG RUST_TOOLCHAIN=1.85.0
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain ${RUST_TOOLCHAIN}
ENV PATH=/root/.cargo/bin:${PATH}

# Optionally install beta/nightly (uncomment as needed)
# RUN rustup toolchain install beta nightly

# Install cargo-nextest for parity with CI's `make test` (optional)
RUN cargo install cargo-nextest --locked || true

# Pre-create a work directory
WORKDIR /work

# You can either mount the repo at runtime (-v "$PWD":/work) or copy it in build
# COPY . /work

# Default command prints versions and help
CMD rustc --version && cargo --version && cargo nextest --version || true && bash

