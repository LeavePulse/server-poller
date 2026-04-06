# syntax=docker/dockerfile:1

FROM rust:1.94-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies by building a dummy project first.
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Build the real binary.
COPY src/ ./src/
RUN touch src/main.rs && cargo build --release

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
