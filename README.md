# 🚀 server-poller

**High-performance, asynchronous Minecraft server status collector built with Rust.**

`server-poller` is a specialized service designed for the **LeavePulse** ecosystem. It monitors thousands of Minecraft servers (both Java and Bedrock editions) in real-time, detecting state changes, collecting telemetry, and providing observability via Prometheus.

## 🛠 Technical Highlights

- **Edition 2024 & Async-first:** Built on the latest Rust standards using `tokio` for massive concurrency and non-blocking I/O.
- **Protocol-level Implementation:** 
    - **Java SLP:** Manual implementation of the Server List Ping protocol over TCP.
    - **Bedrock (RakNet):** Implementation of Unconnected Ping over UDP for Bedrock edition support.
- **Efficient Syncing:** Uses a **Write-Ahead Log (WAL)**-inspired mechanism for reliable data synchronization and state persistence.
- **Change Detection:** Intelligent delta-tracking to minimize database writes and network overhead.
- **Observability:** Native Prometheus integration with fine-grained metrics for probe durations, failure rates, and worker saturation.
- **Geo-awareness:** Integrated GeoIP caching and resolution for server location mapping.

## 🏗 Stack & Core Dependencies

- **Runtime:** `tokio` (multi-threaded executor)
- **Networking:** `reqwest` (HTTP), `hickory-resolver` (Async DNS)
- **Data & State:** `redis` (async-comp), `serde` / `serde_json`
- **Observability:** `prometheus`, `tracing` (structured logging)
- **Security & Integrity:** `sha2` (payload hashing for change detection)

## 🧩 Key Components

- **`ping.rs`**: The heart of the service. Manages connection lifecycles and handles protocol-specific handshake logic.
- **`collector.rs`**: Orchestrates high-concurrency workers with configurable rate-limiting and backoff strategies.
- **`change_detection.rs`**: Compares incoming snapshots with the last known state to trigger events only when data actually changes.
- **`metrics.rs`**: Exports real-time telemetry for monitoring worker performance and probe health.

## ⚡ Performance Optimization

The project is optimized for low-latency and minimal footprint:
- **LTO (Link Time Optimization)** enabled for release builds.
- **Single codegen-unit** for maximum optimization.
- **Binary stripping** to keep production artifacts lightweight.

---
*Part of the [LeavePulse](https://github.com/LeavePulse) ecosystem.*
