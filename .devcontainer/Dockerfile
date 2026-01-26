FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

# Install protobuf compiler (required for tonic-build)
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Install cargo-nextest
RUN cargo install cargo-nextest --locked

# Ensure clippy and rustfmt are available
RUN rustup component add clippy rustfmt
