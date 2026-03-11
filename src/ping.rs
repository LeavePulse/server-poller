//! Minecraft server status polling via the Server List Ping protocol.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use reqwest::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

use crate::config::Settings;
use crate::geo::GeoCache;
use crate::metrics::{
    BEDROCK_PROBE_DURATION, BEDROCK_PROBE_FAILURE, BEDROCK_PROBE_INFLIGHT,
    BEDROCK_PROBE_SUCCESS,
};
use crate::models::{ServerState, schedule_next_bedrock_probe};

/// Result of polling one server.
pub struct PollResult {
    pub payload: Value,
    pub status_ok: bool,
    pub favicon: String,
}

/// Poll a single Minecraft server using the SLP (Server List Ping) protocol.
pub async fn poll_server(
    http: &Client,
    state: &ServerState,
    geo_cache: &Arc<GeoCache>,
    settings: &Settings,
) -> PollResult {
    let now = Utc::now().naive_utc();
    let collected_at = now.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();

    let (country, country_code) = geo_cache
        .fetch_geo(http, &state.host, state.port, state.edition)
        .await;

    let host = state.host.clone();
    let port = state.port.unwrap_or(state.edition.default_port());
    let timeout = Duration::from_secs_f64(settings.collector.status_timeout_seconds);

    // Connect TCP then SLP ping.
    let ping_result = tokio::time::timeout(timeout, async {
        let mut stream = TcpStream::connect((&*host, port)).await?;
        craftping::tokio::ping(&mut stream, &host, port).await
    })
    .await;

    match ping_result {
        Ok(Ok(response)) => {
            let online = response.online_players;
            let max_players = response.max_players;
            let version = response.version.clone();

            // MOTD: Chat has a text field; use Display or raw text.
            let motd = response
                .description
                .as_ref()
                .map(|d| d.text.clone())
                .unwrap_or_default();

            // Favicon is Option<Vec<u8>> (PNG bytes); encode as data URL.
            let favicon = response
                .favicon
                .as_ref()
                .filter(|v| !v.is_empty())
                .map(|v| {
                    use base64::Engine;
                    let b64 = base64::engine::general_purpose::STANDARD.encode(v);
                    format!("data:image/png;base64,{b64}")
                })
                .unwrap_or_default();

            // Extract player sample.
            let players: Vec<String> = response
                .sample
                .as_ref()
                .map(|sample: &Vec<craftping::Player>| {
                    let mut seen = std::collections::HashSet::new();
                    sample
                        .iter()
                        .filter_map(|p| {
                            let name = p.name.trim().to_string();
                            if name.is_empty() || !seen.insert(name.clone()) {
                                None
                            } else {
                                Some(name)
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut extra = serde_json::Map::new();
            if !players.is_empty() {
                extra.insert("players".to_string(), json!(players));
            }

            PollResult {
                payload: json!({
                    "server_id": state.server_id,
                    "collected_at": collected_at,
                    "online": online,
                    "max_players": max_players,
                    "version": version,
                    "motd": motd,
                    "country": country,
                    "country_code": country_code,
                    "extra": extra,
                }),
                status_ok: true,
                favicon,
            }
        }
        Ok(Err(e)) => {
            debug!("Status failed for {host}:{port}: {e}");
            PollResult {
                payload: json!({
                    "server_id": state.server_id,
                    "collected_at": collected_at,
                    "online": null,
                    "max_players": null,
                    "version": "",
                    "motd": "",
                    "country": country,
                    "country_code": country_code,
                }),
                status_ok: false,
                favicon: String::new(),
            }
        }
        Err(_) => {
            debug!("Status timed out for {host}:{port}");
            PollResult {
                payload: json!({
                    "server_id": state.server_id,
                    "collected_at": collected_at,
                    "online": null,
                    "max_players": null,
                    "version": "",
                    "motd": "",
                    "country": country,
                    "country_code": country_code,
                }),
                status_ok: false,
                favicon: String::new(),
            }
        }
    }
}

/// Hash a favicon data URL for change detection.
pub fn favicon_hash(value: &str) -> Option<String> {
    if value.is_empty() || !value.starts_with("data:image") {
        return None;
    }
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    Some(format!("{:x}", hasher.finalize()))
}

/// Upload a changed favicon to server-service.
pub async fn maybe_update_favicon(
    http: &Client,
    headers: &Option<reqwest::header::HeaderMap>,
    state: &mut ServerState,
    favicon_str: &str,
    server_api: &str,
) {
    let Some(hdrs) = headers else { return };
    let Some(hash) = favicon_hash(favicon_str) else {
        return;
    };
    if state.last_favicon_hash.as_deref() == Some(&hash) {
        return;
    }
    let url = format!("{server_api}/internal/servers/{}/favicon", state.server_id);
    let body = json!({"data_url": favicon_str, "hash": hash});
    match http.post(&url).headers(hdrs.clone()).json(&body).send().await {
        Ok(resp) if resp.status().is_success() => {
            state.last_favicon_hash = Some(hash);
            debug!("Updated favicon for {}", state.server_id);
        }
        Ok(resp) => {
            warn!(
                "Failed to update favicon for {}: HTTP {}",
                state.server_id,
                resp.status()
            );
        }
        Err(e) => {
            warn!("Failed to update favicon for {}: {e}", state.server_id);
        }
    }
}

/// Probe whether a Java server also supports Bedrock connections.
pub async fn probe_bedrock_support(host: &str, port: u16, timeout: Duration) -> bool {
    // Bedrock uses RakNet (UDP) — craftping only supports Java SLP (TCP).
    // Try a TCP SLP ping on the bedrock port as a basic connectivity check.
    let result = tokio::time::timeout(timeout, async {
        let mut stream = TcpStream::connect((host, port)).await?;
        craftping::tokio::ping(&mut stream, host, port).await
    })
    .await;
    match result {
        Ok(Ok(_)) => true,
        _ => {
            debug!("Bedrock probe failed for {host}:{port}");
            false
        }
    }
}

/// Check and potentially probe bedrock support, updating state and server-service.
pub async fn maybe_probe_bedrock(
    http: &Client,
    headers: &Option<reqwest::header::HeaderMap>,
    state: &mut ServerState,
    settings: &Settings,
) {
    let _next_probe = match state.next_bedrock_probe {
        Some(t) => t,
        None => return,
    };

    let bedrock_port = state
        .bedrock_port
        .unwrap_or(crate::models::BEDROCK_DEFAULT_PORT);
    let timeout = Duration::from_secs_f64(settings.collector.status_timeout_seconds);

    let start = Instant::now();
    BEDROCK_PROBE_INFLIGHT.inc();

    let ok = probe_bedrock_support(&state.host, bedrock_port, timeout).await;

    BEDROCK_PROBE_DURATION.set(start.elapsed().as_secs_f64());
    BEDROCK_PROBE_INFLIGHT.dec();

    if ok {
        BEDROCK_PROBE_SUCCESS.inc();
        // Update edition on server-service.
        if let Some(hdrs) = headers {
            let url = format!(
                "{}/internal/servers/{}/edition",
                settings.server.api, state.server_id
            );
            let body = json!({"game_edition": "java_bedrock"});
            match http.patch(&url).headers(hdrs.clone()).json(&body).send().await {
                Ok(resp) if resp.status().is_success() => {
                    if let Some(obj) = state.server.as_object_mut() {
                        obj.insert(
                            "game_edition".to_string(),
                            Value::String("java_bedrock".to_string()),
                        );
                    }
                    info!("Updated server {} edition to java_bedrock", state.server_id);
                }
                Ok(resp) => {
                    warn!(
                        "Failed to update edition for {}: HTTP {}",
                        state.server_id,
                        resp.status()
                    );
                }
                Err(e) => {
                    warn!("Failed to update edition for {}: {e}", state.server_id);
                }
            }
        }
    } else {
        BEDROCK_PROBE_FAILURE.inc();
    }

    // Schedule next probe.
    state.next_bedrock_probe = schedule_next_bedrock_probe(
        &state.server,
        0.0,
        settings.collector.bedrock_probe_interval_seconds,
        settings.collector.bedrock_probe_jitter_seconds,
    );
}
