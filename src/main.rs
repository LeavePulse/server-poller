mod bedrock;
mod change_detection;
mod collector;
mod config;
mod geo;
mod metrics;
mod models;
mod ping;
mod scheduler;
mod sync;

use config::Settings;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let settings = Settings::load();

    // Initialize tracing.
    let filter = EnvFilter::try_new(&settings.log_level)
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();

    info!(
        service = %settings.service_name,
        workers = settings.collector.max_concurrency,
        online_interval = settings.collector.online_interval_seconds,
        offline_interval = settings.collector.offline_interval_seconds,
        "Starting unverified-collector-rs"
    );

    // Initialize metrics (force lazy statics).
    metrics::init();

    // Run the collector.
    collector::run(settings).await;
}
