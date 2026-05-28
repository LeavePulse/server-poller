# syntax=docker/dockerfile:1

FROM rust:1.94-slim AS builder

# git: fetch the private service-toolkit-rust git dependency.
# pkg-config/libssl: native TLS deps. protoc is NOT needed — build.rs sources
# it from protoc-bin-vendored.
RUN apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Authenticate to GitHub for the private git dependency.
RUN --mount=type=secret,id=github_token \
    set -eu && \
    echo "machine github.com login x-access-token password $(cat /run/secrets/github_token)" > ~/.netrc && \
    chmod 600 ~/.netrc

# build.rs needs the protos at compile time, so copy the full source up front.
# (No dummy-build dependency caching: build.rs + tonic codegen need proto/ and
# src/ present, which defeats the dummy trick.)
COPY Cargo.toml Cargo.lock* build.rs ./
COPY proto/ ./proto/
COPY src/ ./src/

RUN --mount=type=secret,id=github_token \
    set -eu && \
    echo "machine github.com login x-access-token password $(cat /run/secrets/github_token)" > ~/.netrc && \
    chmod 600 ~/.netrc && \
    cargo build --release && \
    rm -f ~/.netrc

# --- Production image ---
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    adduser --disabled-password --gecos '' appuser

COPY --from=builder /app/target/release/server-poller /usr/local/bin/server-poller

RUN mkdir -p /app/data/buffer && chown -R appuser:appuser /app/data
WORKDIR /app

USER appuser

EXPOSE 9100

HEALTHCHECK CMD curl --fail http://localhost:${PROMETHEUS_PORT:-9100}/metrics || exit 1

CMD ["server-poller"]
