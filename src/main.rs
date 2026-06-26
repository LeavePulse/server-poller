mod bedrock;
mod change_detection;
mod collector;
mod config;
mod geo;
mod grpc_clients;
mod metrics;
mod models;
mod ping;
mod scheduler;
mod sync;
mod wal;

use config::Settings;
use tracing::info;

#[tokio::main]
async fn main() {
    let settings = Settings::load();

    service_toolkit_rust::telemetry::init_tracing(&settings.log_level);

    info!(
        service = %settings.service_name,
        workers = settings.collector.max_concurrency,
        online_interval = settings.collector.online_interval_seconds,
        offline_interval = settings.collector.offline_interval_seconds,
        "Starting server-poller"
    );

    // Initialize metrics (force lazy statics).
    metrics::init();

    // Run the collector.
    collector::run(settings).await;
}
