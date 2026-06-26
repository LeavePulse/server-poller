//! Prometheus metrics definitions. The `/metrics` HTTP exporter itself is
//! served by the shared `service_toolkit_rust::metrics` helper over the
//! process-global default registry these `register_*!` macros write to.

use std::net::SocketAddr;
use std::sync::LazyLock;

use prometheus::{
    Gauge, GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts,
    register_gauge, register_gauge_vec, register_histogram, register_int_counter,
    register_int_counter_vec, register_int_gauge,
};
use tracing::info;

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

pub static POLL_SUCCESS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        Opts::new(
            "unverified_collector_poll_success_total",
            "Successful status polls."
        ),
        &["edition"]
    )
    .unwrap()
});

pub static POLL_FAILURE: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        Opts::new(
            "unverified_collector_poll_failure_total",
            "Failed status polls."
        ),
        &["edition"]
    )
    .unwrap()
});

pub static POLL_DURATION: LazyLock<GaugeVec> = LazyLock::new(|| {
    register_gauge_vec!(
        Opts::new(
            "unverified_collector_poll_duration_seconds",
            "Time spent polling a server."
        ),
        &["edition"]
    )
    .unwrap()
});

pub static POLL_INFLIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "unverified_collector_poll_inflight",
        "Active in-flight polls."
    )
    .unwrap()
});

// ---------------------------------------------------------------------------
// Bedrock probing
// ---------------------------------------------------------------------------

pub static BEDROCK_PROBE_SUCCESS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "unverified_collector_bedrock_probe_success_total",
        "Successful bedrock support probes."
    )
    .unwrap()
});

pub static BEDROCK_PROBE_FAILURE: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "unverified_collector_bedrock_probe_failure_total",
        "Failed bedrock support probes."
    )
    .unwrap()
});

pub static BEDROCK_PROBE_DURATION: LazyLock<Gauge> = LazyLock::new(|| {
    register_gauge!(
        "unverified_collector_bedrock_probe_duration_seconds",
        "Time spent probing bedrock support."
    )
    .unwrap()
});

pub static BEDROCK_PROBE_INFLIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "unverified_collector_bedrock_probe_inflight",
        "Active in-flight bedrock probes."
    )
    .unwrap()
});

// ---------------------------------------------------------------------------
// Ingestion
// ---------------------------------------------------------------------------

pub static INGEST_SUCCESS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "unverified_collector_ingest_success_total",
        "Successful ingest batches."
    )
    .unwrap()
});

pub static INGEST_FAILURE: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "unverified_collector_ingest_failure_total",
        "Failed ingest batches."
    )
    .unwrap()
});

pub static INGEST_ROWS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "unverified_collector_ingest_rows_total",
        "Rows forwarded to monitoring-service."
    )
    .unwrap()
});

pub static INGEST_BATCH_SIZE_HISTOGRAM: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        HistogramOpts::new(
            "unverified_collector_ingest_batch_size",
            "Batch sizes posted to monitoring-service."
        )
        .buckets(vec![
            1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 200.0, 500.0, 1000.0
        ])
    )
    .unwrap()
});

// ---------------------------------------------------------------------------
// Queues and system
// ---------------------------------------------------------------------------

pub static RESULT_QUEUE_DROPPED: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "unverified_collector_result_queue_dropped_total",
        "Payloads dropped because the result queue was full."
    )
    .unwrap()
});

pub static SERVERS_TOTAL: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "unverified_collector_servers_total",
        "Servers tracked by the scheduler."
    )
    .unwrap()
});

pub static WORK_QUEUE_SIZE: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("unverified_collector_work_queue_size", "Work queue size.").unwrap()
});

pub static RESULT_QUEUE_SIZE: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "unverified_collector_result_queue_size",
        "Result queue size."
    )
    .unwrap()
});

pub static GEO_CACHE_SIZE: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("unverified_collector_geo_cache_size", "Geo cache size.").unwrap()
});

pub static GEO_REFRESH_INFLIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "unverified_collector_geo_refresh_inflight",
        "Geo lookups currently in progress."
    )
    .unwrap()
});

pub static SERVER_LIST_REFRESH_SUCCESS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "unverified_collector_server_list_refresh_success_total",
        "Successful server catalog refreshes."
    )
    .unwrap()
});

pub static SERVER_LIST_REFRESH_FAILURE: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "unverified_collector_server_list_refresh_failure_total",
        "Failed server catalog refreshes."
    )
    .unwrap()
});

pub static SERVER_LIST_REFRESH_DURATION: LazyLock<Gauge> = LazyLock::new(|| {
    register_gauge!(
        "unverified_collector_server_list_refresh_duration_seconds",
        "Time spent refreshing the server catalog."
    )
    .unwrap()
});

// ---------------------------------------------------------------------------
// WAL buffer
// ---------------------------------------------------------------------------

pub static BUFFER_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "unverified_collector_buffer_bytes",
        "Current WAL buffer size on disk in bytes."
    )
    .unwrap()
});

pub static BUFFER_SEGMENTS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "unverified_collector_buffer_segments",
        "Current number of WAL segment files."
    )
    .unwrap()
});

pub static BUFFER_WRITES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "unverified_collector_buffer_writes_total",
        "Batches written to WAL buffer."
    )
    .unwrap()
});

pub static BUFFER_DRAINED: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "unverified_collector_buffer_drained_total",
        "WAL segments successfully drained and deleted."
    )
    .unwrap()
});

pub static BUFFER_EVICTED: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "unverified_collector_buffer_evicted_total",
        "WAL segments evicted because buffer exceeded max size."
    )
    .unwrap()
});

/// Ensure all lazy metrics are initialized.
pub fn init() {
    LazyLock::force(&POLL_SUCCESS);
    LazyLock::force(&POLL_FAILURE);
    LazyLock::force(&POLL_DURATION);
    LazyLock::force(&POLL_INFLIGHT);
    LazyLock::force(&BEDROCK_PROBE_SUCCESS);
    LazyLock::force(&BEDROCK_PROBE_FAILURE);
    LazyLock::force(&BEDROCK_PROBE_DURATION);
    LazyLock::force(&BEDROCK_PROBE_INFLIGHT);
    LazyLock::force(&INGEST_SUCCESS);
    LazyLock::force(&INGEST_FAILURE);
    LazyLock::force(&INGEST_ROWS);
    LazyLock::force(&INGEST_BATCH_SIZE_HISTOGRAM);
    LazyLock::force(&RESULT_QUEUE_DROPPED);
    LazyLock::force(&SERVERS_TOTAL);
    LazyLock::force(&WORK_QUEUE_SIZE);
    LazyLock::force(&RESULT_QUEUE_SIZE);
    LazyLock::force(&GEO_CACHE_SIZE);
    LazyLock::force(&GEO_REFRESH_INFLIGHT);
    LazyLock::force(&SERVER_LIST_REFRESH_SUCCESS);
    LazyLock::force(&SERVER_LIST_REFRESH_FAILURE);
    LazyLock::force(&SERVER_LIST_REFRESH_DURATION);
    LazyLock::force(&BUFFER_BYTES);
    LazyLock::force(&BUFFER_SEGMENTS);
    LazyLock::force(&BUFFER_WRITES);
    LazyLock::force(&BUFFER_DRAINED);
    LazyLock::force(&BUFFER_EVICTED);
}

/// Start the Prometheus metrics HTTP server on the given address. Serves the
/// process-global default registry via the shared toolkit helper.
pub async fn start_metrics_server(host: &str, port: u16) {
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], port)));
    info!("Prometheus metrics server listening on {addr}");
    service_toolkit_rust::metrics::serve(addr).await;
}
