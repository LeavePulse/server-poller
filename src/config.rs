//! Application configuration loaded from environment variables + .env file.

use std::env;

/// Server catalog API settings (server-service).
#[derive(Debug, Clone)]
pub struct ServerApiSettings {
    pub api: String,
    pub api_token: Option<String>,
}

/// Monitoring-service API settings.
#[derive(Debug, Clone)]
pub struct MonitoringApiSettings {
    pub api: String,
    pub api_token: Option<String>,
}

/// Collector runtime configuration.
#[derive(Debug, Clone)]
pub struct CollectorSettings {
    pub online_interval_seconds: u64,
    pub offline_interval_seconds: u64,
    pub server_list_refresh_seconds: u64,
    pub server_list_retry_seconds: u64,
    pub page_size: u64,
    pub max_concurrency: usize,
    pub status_timeout_seconds: f64,
    pub lookup_timeout_seconds: f64,
    pub startup_spread_seconds: u64,
    pub ingest_batch_size: usize,
    pub ingest_flush_seconds: f64,
    pub result_queue_maxsize: usize,
    pub http_timeout_seconds: f64,
    pub bedrock_probe_interval_seconds: u64,
    pub bedrock_probe_jitter_seconds: u64,
    pub trigger_file_path: String,
    pub trigger_poll_seconds: f64,
    pub force_ping_queue_key: String,
    pub force_ping_pop_timeout_seconds: f64,
    pub initial_failures_before_slowdown: u32,
    pub initial_failure_slowdown_multiplier: f64,
    pub initial_failure_max_interval_seconds: u64,
    pub plugin_compare_interval_seconds: u64,
}

/// Geo lookup cache settings.
#[derive(Debug, Clone)]
pub struct GeoSettings {
    pub ttl_seconds: u64,
    pub refresh_jitter_seconds: u64,
    pub retry_seconds: u64,
    pub dns_ttl_seconds: u64,
    pub dns_retry_seconds: u64,
    pub dns_cache_max_entries: usize,
    pub dns_max_concurrency: usize,
}

/// Prometheus exporter configuration.
#[derive(Debug, Clone)]
pub struct PrometheusSettings {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub update_seconds: f64,
}

/// Optional Redis configuration.
#[derive(Debug, Clone)]
pub struct RedisSettings {
    pub enabled: bool,
    pub url: String,
}

/// Top-level application settings.
#[derive(Debug, Clone)]
pub struct Settings {
    pub service_name: String,
    pub log_level: String,
    pub server: ServerApiSettings,
    pub monitoring: MonitoringApiSettings,
    pub collector: CollectorSettings,
    pub geo: GeoSettings,
    pub prometheus: PrometheusSettings,
    pub redis: RedisSettings,
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_str_opt(key: &str) -> Option<String> {
    env::var(key).ok().filter(|s| !s.is_empty())
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u16(key: &str, default: u16) -> u16 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|v| matches!(v.to_lowercase().as_str(), "true" | "1" | "yes" | "on"))
        .unwrap_or(default)
}

impl Settings {
    pub fn load() -> Self {
        // Load .env file (ignore errors — file may not exist).
        let env_file = env_str("ENV_FILE", ".env");
        let _ = dotenvy::from_filename(&env_file);

        let online_interval = env_u64("COLLECTOR_ONLINE_INTERVAL_SECONDS", 0)
            .max(env_u64("ONLINE_INTERVAL_SECONDS", 0))
            .max(env_u64("INTERVAL_SECONDS", 0));
        let online_interval = if online_interval > 0 {
            online_interval
        } else {
            300
        };

        let offline_interval = env_u64("COLLECTOR_OFFLINE_INTERVAL_SECONDS", 0)
            .max(env_u64("OFFLINE_INTERVAL_SECONDS", 0));
        let offline_interval = if offline_interval > 0 {
            offline_interval
        } else {
            online_interval * 2
        };

        let startup_spread = env_u64("COLLECTOR_STARTUP_SPREAD_SECONDS", 0)
            .max(env_u64("STARTUP_SPREAD_SECONDS", 0));
        let startup_spread = if startup_spread > 0 {
            startup_spread
        } else {
            online_interval
        };

        // Server API: try SERVER_ prefix first, fall back to CORE_ for compat.
        let server_api = env_str_opt("SERVER_API")
            .or_else(|| env_str_opt("CORE_API"))
            .unwrap_or_else(|| "http://server-service:8201".to_string());
        let server_api_token =
            env_str_opt("SERVER_API_TOKEN").or_else(|| env_str_opt("CORE_API_TOKEN"));

        Settings {
            service_name: env_str("SERVICE_NAME", "unverified-collector"),
            log_level: env_str("LOG_LEVEL", "info"),

            server: ServerApiSettings {
                api: server_api,
                api_token: server_api_token,
            },

            monitoring: MonitoringApiSettings {
                api: env_str("MONITORING_API", "http://monitoring-service:8200"),
                api_token: env_str_opt("MONITORING_API_TOKEN"),
            },

            collector: CollectorSettings {
                online_interval_seconds: online_interval,
                offline_interval_seconds: offline_interval,
                server_list_refresh_seconds: env_u64(
                    "COLLECTOR_SERVER_LIST_REFRESH_SECONDS",
                    env_u64("SERVER_LIST_REFRESH_SECONDS", 300),
                ),
                server_list_retry_seconds: env_u64(
                    "COLLECTOR_SERVER_LIST_RETRY_SECONDS",
                    env_u64("SERVER_LIST_RETRY_SECONDS", 30),
                ),
                page_size: env_u64("COLLECTOR_PAGE_SIZE", env_u64("PAGE_SIZE", 100)).min(100),
                max_concurrency: env_usize(
                    "COLLECTOR_MAX_CONCURRENCY",
                    env_usize("MAX_CONCURRENCY", 25),
                ),
                status_timeout_seconds: env_f64(
                    "COLLECTOR_STATUS_TIMEOUT_SECONDS",
                    env_f64("STATUS_TIMEOUT_SECONDS", 8.0),
                ),
                lookup_timeout_seconds: env_f64(
                    "COLLECTOR_LOOKUP_TIMEOUT_SECONDS",
                    env_f64("LOOKUP_TIMEOUT_SECONDS", 5.0),
                ),
                startup_spread_seconds: startup_spread,
                ingest_batch_size: env_usize(
                    "COLLECTOR_INGEST_BATCH_SIZE",
                    env_usize("INGEST_BATCH_SIZE", 200),
                ),
                ingest_flush_seconds: env_f64(
                    "COLLECTOR_INGEST_FLUSH_SECONDS",
                    env_f64("INGEST_FLUSH_SECONDS", 5.0),
                ),
                result_queue_maxsize: env_usize(
                    "COLLECTOR_RESULT_QUEUE_MAXSIZE",
                    env_usize("RESULT_QUEUE_MAXSIZE", 5000),
                ),
                http_timeout_seconds: env_f64(
                    "COLLECTOR_HTTP_TIMEOUT_SECONDS",
                    env_f64("HTTP_TIMEOUT_SECONDS", 15.0),
                ),
                bedrock_probe_interval_seconds: env_u64(
                    "COLLECTOR_BEDROCK_PROBE_INTERVAL_SECONDS",
                    env_u64("BEDROCK_PROBE_INTERVAL_SECONDS", 3600),
                ),
                bedrock_probe_jitter_seconds: env_u64(
                    "COLLECTOR_BEDROCK_PROBE_JITTER_SECONDS",
                    env_u64("BEDROCK_PROBE_JITTER_SECONDS", 600),
                ),
                trigger_file_path: env_str(
                    "COLLECTOR_TRIGGER_FILE_PATH",
                    &env_str("TRIGGER_FILE_PATH", "/tmp/unverified-collector-trigger"),
                ),
                trigger_poll_seconds: env_f64(
                    "COLLECTOR_TRIGGER_POLL_SECONDS",
                    env_f64("TRIGGER_POLL_SECONDS", 2.0),
                ),
                force_ping_queue_key: env_str(
                    "COLLECTOR_FORCE_PING_QUEUE_KEY",
                    &env_str("FORCE_PING_QUEUE_KEY", "leavepulse:unverified:force-ping"),
                ),
                force_ping_pop_timeout_seconds: env_f64(
                    "COLLECTOR_FORCE_PING_POP_TIMEOUT_SECONDS",
                    env_f64("FORCE_PING_POP_TIMEOUT_SECONDS", 1.0),
                ),
                initial_failures_before_slowdown: env_u64(
                    "COLLECTOR_INITIAL_FAILURES_BEFORE_SLOWDOWN",
                    env_u64("INITIAL_FAILURES_BEFORE_SLOWDOWN", 5),
                ) as u32,
                initial_failure_slowdown_multiplier: env_f64(
                    "COLLECTOR_INITIAL_FAILURE_SLOWDOWN_MULTIPLIER",
                    env_f64("INITIAL_FAILURE_SLOWDOWN_MULTIPLIER", 6.0),
                ),
                initial_failure_max_interval_seconds: env_u64(
                    "COLLECTOR_INITIAL_FAILURE_MAX_INTERVAL_SECONDS",
                    env_u64("INITIAL_FAILURE_MAX_INTERVAL_SECONDS", 3600),
                ),
                plugin_compare_interval_seconds: env_u64(
                    "COLLECTOR_PLUGIN_COMPARE_INTERVAL_SECONDS",
                    env_u64("PLUGIN_COMPARE_INTERVAL_SECONDS", 1800),
                )
                .max(60),
            },

            geo: GeoSettings {
                ttl_seconds: env_u64("GEO_TTL_SECONDS", 3600),
                refresh_jitter_seconds: env_u64("GEO_REFRESH_JITTER_SECONDS", 300),
                retry_seconds: env_u64("GEO_RETRY_SECONDS", 300),
                dns_ttl_seconds: env_u64("GEO_DNS_TTL_SECONDS", 600).max(60),
                dns_retry_seconds: env_u64("GEO_DNS_RETRY_SECONDS", 120).max(10),
                dns_cache_max_entries: env_usize("GEO_DNS_CACHE_MAX_ENTRIES", 10_000).max(100),
                dns_max_concurrency: env_usize("GEO_DNS_MAX_CONCURRENCY", 32).max(1),
            },

            prometheus: PrometheusSettings {
                enabled: env_bool("PROMETHEUS_ENABLED", env_bool("METRICS_ENABLED", true)),
                host: env_str("PROMETHEUS_HOST", &env_str("METRICS_HOST", "0.0.0.0")),
                port: env_u16("PROMETHEUS_PORT", env_u16("METRICS_PORT", 9100)),
                update_seconds: env_f64(
                    "PROMETHEUS_UPDATE_SECONDS",
                    env_f64("METRICS_UPDATE_SECONDS", 5.0),
                ),
            },

            redis: RedisSettings {
                enabled: env_bool("REDIS_ENABLED", false),
                url: env_str("REDIS_URL", "redis://127.0.0.1:6379"),
            },
        }
    }
}
