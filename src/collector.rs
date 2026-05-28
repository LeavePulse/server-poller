//! Core collector orchestration: scheduler, workers, ingest loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};
use tracing::{info, warn};

use crate::change_detection::build_change_payload;
use crate::config::{MonitoringApiSettings, Settings};
use crate::geo::GeoCache;
use crate::grpc_clients::{auth_channel, monitoring_client, value_to_ingest_point};
use crate::grpc_clients::monitoring::v1::IngestUnverifiedRequest;
use crate::grpc_clients::monitoring::v1::IngestPoint;
use crate::metrics::*;
use crate::models::{ServerState, is_internal_only_host};
use crate::ping::{maybe_probe_bedrock, maybe_update_favicon, poll_server};
use crate::scheduler::SchedulerHandle;

/// Run the scheduler → work_queue dispatch loop.
async fn scheduler_loop(
    scheduler: SchedulerHandle,
    states: Arc<Mutex<HashMap<String, ServerState>>>,
    work_tx: mpsc::Sender<String>,
) {
    loop {
        let (key, due) = scheduler.next_due().await;
        // Verify the state still exists and its due time matches.
        {
            let map = states.lock().await;
            match map.get(&key) {
                Some(state) if state.next_due == due => {}
                _ => continue,
            }
        }
        if work_tx.send(key).await.is_err() {
            break;
        }
        WORK_QUEUE_SIZE.set(work_tx.max_capacity() as i64 - work_tx.capacity() as i64);
    }
}

/// Batch results and POST them to monitoring-service.
/// Failed batches are persisted to a WAL buffer on disk and drained on recovery.
async fn ingest_loop(mut result_rx: mpsc::Receiver<Value>, settings: Arc<Settings>) {
    let batch_size = settings.collector.ingest_batch_size;
    let flush_interval = Duration::from_secs_f64(settings.collector.ingest_flush_seconds);
    let monitoring = settings.monitoring.clone();

    let mut wal = match crate::wal::WalBuffer::open(
        std::path::PathBuf::from(&settings.collector.buffer_dir),
        settings.collector.buffer_max_bytes,
        settings.collector.buffer_segment_max_bytes,
    ) {
        Ok(w) => w,
        Err(e) => {
            warn!("Failed to open WAL buffer: {e} — running without disk buffer");
            // Fall back to old behaviour with a dummy WAL that never has pending data.
            // We still enter the loop; send_batch failures will just be logged.
            run_ingest_loop_no_wal(monitoring, result_rx, batch_size, flush_interval).await;
            return;
        }
    };

    let mut batch: Vec<Value> = Vec::with_capacity(batch_size);
    let mut deadline = tokio::time::Instant::now() + flush_interval;

    loop {
        tokio::select! {
            item = result_rx.recv() => {
                match item {
                    Some(payload) => {
                        batch.push(payload);
                        if batch.len() >= batch_size {
                            flush_batch(&monitoring, &mut batch, &mut wal, batch_size).await;
                            deadline = tokio::time::Instant::now() + flush_interval;
                        }
                    }
                    None => break,
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                if !batch.is_empty() {
                    flush_batch(&monitoring, &mut batch, &mut wal, batch_size).await;
                }
                deadline = tokio::time::Instant::now() + flush_interval;
            }
        }
    }
}

/// Fallback ingest loop when WAL cannot be opened (original behaviour).
async fn run_ingest_loop_no_wal(
    monitoring: MonitoringApiSettings,
    mut result_rx: mpsc::Receiver<Value>,
    batch_size: usize,
    flush_interval: Duration,
) {
    let mut batch: Vec<Value> = Vec::with_capacity(batch_size);
    let mut deadline = tokio::time::Instant::now() + flush_interval;

    loop {
        tokio::select! {
            item = result_rx.recv() => {
                match item {
                    Some(payload) => {
                        batch.push(payload);
                        if batch.len() >= batch_size {
                            send_batch(&monitoring, &batch).await;
                            batch.clear();
                            deadline = tokio::time::Instant::now() + flush_interval;
                        }
                    }
                    None => break,
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                if !batch.is_empty() {
                    send_batch(&monitoring, &batch).await;
                    batch.clear();
                }
                deadline = tokio::time::Instant::now() + flush_interval;
            }
        }
    }
}

/// Send a batch and handle WAL write-on-failure / drain-on-success.
async fn flush_batch(
    monitoring: &MonitoringApiSettings,
    batch: &mut Vec<Value>,
    wal: &mut crate::wal::WalBuffer,
    drain_max: usize,
) {
    let ok = send_batch(monitoring, batch).await;
    if ok {
        batch.clear();
        // Drain one buffered segment on success.
        if wal.has_pending() {
            match wal.read_oldest_batch(drain_max) {
                Ok(Some((path, items))) => {
                    if !items.is_empty() {
                        let drain_ok = send_batch(monitoring, &items).await;
                        if drain_ok && let Err(e) = wal.delete_segment(&path) {
                            warn!("Failed to delete drained WAL segment: {e}");
                        }
                    } else {
                        // Empty segment (all records corrupted), just remove it.
                        let _ = wal.delete_segment(&path);
                    }
                }
                Ok(None) => {}
                Err(e) => warn!("Failed to read WAL segment for drain: {e}"),
            }
        }
    } else {
        // Write failed batch to WAL for later retry.
        if let Err(e) = wal.write_batch(batch) {
            warn!("Failed to write batch to WAL buffer: {e}");
        }
        batch.clear();
    }
}

/// Send a batch to monitoring-service over gRPC. Returns `true` on success.
async fn send_batch(monitoring: &MonitoringApiSettings, batch: &[Value]) -> bool {
    let token = monitoring.api_token.as_deref().unwrap_or_default();
    let channel = match auth_channel(&monitoring.grpc_target, token) {
        Ok(c) => c,
        Err(e) => {
            INGEST_FAILURE.inc();
            warn!("Failed to build ingest gRPC channel: {e}");
            return false;
        }
    };
    let mut client = monitoring_client(channel);
    let items: Vec<IngestPoint> = batch.iter().map(value_to_ingest_point).collect();
    let request = IngestUnverifiedRequest { items };

    match client.ingest_unverified(request).await {
        Ok(_) => {
            INGEST_SUCCESS.inc();
            INGEST_ROWS.inc_by(batch.len() as u64);
            INGEST_BATCH_SIZE_HISTOGRAM.observe(batch.len() as f64);
            info!("Forwarded {} rows to monitoring-service", batch.len());
            true
        }
        Err(status) => {
            INGEST_FAILURE.inc();
            warn!("Failed to forward rows: {status}");
            false
        }
    }
}

/// Periodically update Prometheus gauges.
async fn metrics_loop(
    states: Arc<Mutex<HashMap<String, ServerState>>>,
    geo_cache: Arc<GeoCache>,
    update_seconds: f64,
) {
    let interval = Duration::from_secs_f64(update_seconds);
    loop {
        {
            let map = states.lock().await;
            SERVERS_TOTAL.set(map.len() as i64);
        }
        geo_cache.update_metrics().await;
        tokio::time::sleep(interval).await;
    }
}

/// Main entry point: set up all tasks and run forever.
pub async fn run(settings: Settings) {
    let settings = Arc::new(settings);

    // HTTP client.
    let timeout = Duration::from_secs_f64(settings.collector.http_timeout_seconds);
    let http = Client::builder()
        .timeout(timeout)
        .pool_max_idle_per_host(settings.collector.max_concurrency)
        .build()
        .expect("Failed to build HTTP client");

    // Shared state.
    let states: Arc<Mutex<HashMap<String, ServerState>>> = Arc::new(Mutex::new(HashMap::new()));
    let scheduler = SchedulerHandle::new();
    let geo_cache = GeoCache::new(
        settings.geo.clone(),
        settings.collector.lookup_timeout_seconds,
    );

    // Initial server catalog fetch.
    crate::sync::refresh_servers_once(&states, &scheduler, &settings).await;

    // Channels.
    let (work_tx, work_rx) = mpsc::channel::<String>(settings.collector.max_concurrency * 4);
    let (result_tx, result_rx) = mpsc::channel::<Value>(settings.collector.result_queue_maxsize);

    // Fan-out work_rx to multiple workers via a shared receiver.
    let work_rx = Arc::new(tokio::sync::Mutex::new(work_rx));

    // Spawn scheduler.
    tokio::spawn(scheduler_loop(scheduler.clone(), states.clone(), work_tx));

    // Spawn workers.
    for id in 0..settings.collector.max_concurrency {
        let http = http.clone();
        let states = states.clone();
        let scheduler = scheduler.clone();
        let geo_cache = geo_cache.clone();
        let settings = settings.clone();
        let result_tx = result_tx.clone();
        let work_rx = work_rx.clone();

        tokio::spawn(async move {
            loop {
                let key = {
                    let mut rx = work_rx.lock().await;
                    match rx.recv().await {
                        Some(k) => k,
                        None => break,
                    }
                };
                // Process this key inline (reuse worker_loop logic).
                process_work_item(
                    id,
                    &http,
                    &states,
                    &scheduler,
                    &geo_cache,
                    &settings,
                    &result_tx,
                    key,
                )
                .await;
            }
        });
    }
    // Drop original sender so ingest loop can detect shutdown.
    drop(result_tx.clone());

    // Spawn ingest loop.
    tokio::spawn(ingest_loop(result_rx, settings.clone()));

    // Spawn metrics loop.
    if settings.prometheus.enabled {
        tokio::spawn(metrics_loop(
            states.clone(),
            geo_cache.clone(),
            settings.prometheus.update_seconds,
        ));
    }

    // Spawn catalog refresh loop.
    tokio::spawn(crate::sync::refresh_servers_loop(
        states.clone(),
        scheduler.clone(),
        settings.clone(),
    ));

    // Spawn trigger loop.
    tokio::spawn(crate::sync::trigger_loop(
        states.clone(),
        scheduler.clone(),
        settings.clone(),
    ));

    // Spawn Redis force-ping loop (if enabled).
    if settings.redis.enabled {
        tokio::spawn(crate::sync::redis_force_ping_loop(
            settings.redis.url.clone(),
            states.clone(),
            scheduler.clone(),
            settings.clone(),
        ));

        // Spawn ad-hoc discovery candidate probe loop (admin preview).
        tokio::spawn(crate::sync::redis_discovery_probe_loop(
            http.clone(),
            settings.redis.url.clone(),
            geo_cache.clone(),
            settings.clone(),
        ));
    }

    // Prometheus metrics server.
    if settings.prometheus.enabled {
        let prom_host = settings.prometheus.host.clone();
        let prom_port = settings.prometheus.port;
        tokio::spawn(async move {
            crate::metrics::start_metrics_server(&prom_host, prom_port).await;
        });
    }

    // Run forever.
    info!(
        "Collector started with {} workers",
        settings.collector.max_concurrency
    );
    std::future::pending::<()>().await;
}

/// Process a single work item (called from each worker task).
#[allow(clippy::too_many_arguments)]
async fn process_work_item(
    _worker_id: usize,
    http: &Client,
    states: &Arc<Mutex<HashMap<String, ServerState>>>,
    scheduler: &SchedulerHandle,
    geo_cache: &Arc<GeoCache>,
    settings: &Arc<Settings>,
    result_tx: &mpsc::Sender<Value>,
    key: String,
) {
    let online_interval = settings.collector.online_interval_seconds as f64;
    let offline_interval = settings.collector.offline_interval_seconds as f64;
    let plugin_interval = settings.collector.plugin_compare_interval_seconds as f64;
    let failures_before_slowdown = settings.collector.initial_failures_before_slowdown;
    let slowdown_multiplier = settings.collector.initial_failure_slowdown_multiplier;
    let max_slow_interval = settings.collector.initial_failure_max_interval_seconds as f64;

    // Read state snapshot for polling.
    let (host, port, edition, plugin_managed, plugin_fallback_mode, server_id) = {
        let map = states.lock().await;
        match map.get(&key) {
            Some(s) => (
                s.host.clone(),
                s.port,
                s.edition,
                s.plugin_managed,
                s.plugin_fallback_mode,
                s.server_id.clone(),
            ),
            None => return,
        }
    };

    // Skip internal-only hosts for plugin-managed servers.
    if plugin_managed && is_internal_only_host(&host) {
        let mut map = states.lock().await;
        if let Some(state) = map.get_mut(&key) {
            let jitter = rand::thread_rng().gen_range(-0.1..0.1) * plugin_interval;
            state.next_due = scheduler.now() + (plugin_interval + jitter).max(60.0);
            scheduler.schedule(key.clone(), state.next_due);
        }
        return;
    }

    // Poll the server.
    let start = Instant::now();
    POLL_INFLIGHT.inc();

    let poll_result =
        poll_server(http, &server_id, &host, port, edition, geo_cache, settings).await;

    POLL_INFLIGHT.dec();
    POLL_DURATION
        .with_label_values(&[edition.as_str()])
        .set(start.elapsed().as_secs_f64());

    let status_ok = poll_result.status_ok;
    if status_ok {
        POLL_SUCCESS.with_label_values(&[edition.as_str()]).inc();
    } else {
        POLL_FAILURE.with_label_values(&[edition.as_str()]).inc();
    }

    // Update state.
    let changed_payload = {
        let mut map = states.lock().await;
        let state = match map.get_mut(&key) {
            Some(s) => s,
            None => return,
        };

        if status_ok {
            state.has_succeeded = true;
            state.consecutive_failures = 0;
        } else {
            state.consecutive_failures += 1;
            if !state.has_succeeded {
                state.initial_failures += 1;
            }
        }

        // Bedrock probe.
        maybe_probe_bedrock(state, settings).await;

        // Favicon update.
        if !poll_result.favicon.is_empty() {
            maybe_update_favicon(&settings.server, state, &poll_result.favicon).await;
        }

        // Next interval.
        let interval = if plugin_managed && !plugin_fallback_mode {
            plugin_interval
        } else {
            let mut iv = if status_ok {
                online_interval
            } else {
                offline_interval
            };
            if !status_ok
                && !state.has_succeeded
                && state.initial_failures >= failures_before_slowdown
            {
                let slowed = (offline_interval * slowdown_multiplier).max(offline_interval);
                iv = slowed.min(max_slow_interval);
            }
            iv
        };

        let jitter = rand::thread_rng().gen_range(-0.1..0.1) * interval;
        state.next_due = scheduler.now() + (interval + jitter).max(1.0);
        scheduler.schedule(key.clone(), state.next_due);

        if plugin_managed && !plugin_fallback_mode && !status_ok {
            return;
        }

        build_change_payload(
            state,
            &poll_result.payload,
            status_ok,
            scheduler.now(),
            settings.collector.heartbeat_interval_seconds as f64,
        )
    };

    if let Some(payload) = changed_payload
        && result_tx.try_send(payload).is_err()
    {
        warn!("Result queue full, dropping payload for {server_id}");
        RESULT_QUEUE_DROPPED.inc();
    }
}
