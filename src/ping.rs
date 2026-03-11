//! Minecraft server status polling via SLP (Java) and RakNet (Bedrock).

use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use reqwest::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use crate::bedrock;
use crate::config::Settings;
use crate::geo::GeoCache;
use crate::metrics::{
    BEDROCK_PROBE_DURATION, BEDROCK_PROBE_FAILURE, BEDROCK_PROBE_INFLIGHT, BEDROCK_PROBE_SUCCESS,
};
use crate::models::{Edition, ServerState, schedule_next_bedrock_probe};

/// Result of polling one server.
pub struct PollResult {
    pub payload: Value,
    pub status_ok: bool,
    pub favicon: String,
}

/// Poll a single Minecraft server.
///
/// Uses SLP (TCP) for Java edition and RakNet Unconnected Ping (UDP) for Bedrock.
pub async fn poll_server(
    http: &Client,
    server_id: &str,
    host: &str,
    port: Option<u16>,
    edition: Edition,
    geo_cache: &Arc<GeoCache>,
    settings: &Settings,
) -> PollResult {
    let now = Utc::now().naive_utc();
    let collected_at = now.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();

    let (country, country_code) = geo_cache.fetch_geo(http, host, port, edition).await;

    let port = port.unwrap_or(edition.default_port());
    let timeout = Duration::from_secs_f64(settings.collector.status_timeout_seconds);

    let fail_payload = || {
        json!({
            "server_id": server_id,
            "collected_at": collected_at,
            "online": null,
            "max_players": null,
            "version": "",
            "motd": "",
            "country": country,
            "country_code": country_code,
        })
    };

    match edition {
        Edition::Bedrock => {
            poll_bedrock(
                server_id,
                host,
                port,
                timeout,
                &collected_at,
                &country,
                &country_code,
                fail_payload,
            )
            .await
        }
        Edition::Java => {
            poll_java(
                server_id,
                host,
                port,
                timeout,
                &collected_at,
                &country,
                &country_code,
                fail_payload,
            )
            .await
        }
    }
}

/// Poll a Java edition server via SLP (Server List Ping over TCP).
#[allow(clippy::too_many_arguments)]
async fn poll_java(
    server_id: &str,
    host: &str,
    port: u16,
    timeout: Duration,
    collected_at: &str,
    country: &str,
    country_code: &str,
    fail_payload: impl FnOnce() -> Value,
) -> PollResult {
    let socket_addrs =
        match tokio::time::timeout(timeout, tokio::net::lookup_host((host, port))).await {
            Ok(Ok(addrs)) => addrs.collect::<Vec<_>>(),
            Ok(Err(e)) => {
                debug!("Java status DNS lookup failed for server {server_id} {host}:{port}: {e}");
                return PollResult {
                    payload: fail_payload(),
                    status_ok: false,
                    favicon: String::new(),
                };
            }
            Err(_) => {
                debug!("Java status DNS lookup timed out for server {server_id} {host}:{port}");
                return PollResult {
                    payload: fail_payload(),
                    status_ok: false,
                    favicon: String::new(),
                };
            }
        };

    let handshake_host = host.to_string();
    let ping_result = tokio::task::spawn_blocking(move || {
        ping_java_status_blocking(socket_addrs, &handshake_host, port, timeout)
    })
    .await;

    match ping_result {
        Ok(Ok(response)) => {
            let online = response.online_players;
            let max_players = response.max_players;
            let version = response.version.clone();

            let motd = response
                .description
                .as_ref()
                .map(|d| d.text.clone())
                .unwrap_or_default();

            // Favicon: Option<Vec<u8>> (raw PNG) → data URL.
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

            // Player sample.
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
                    "server_id": server_id,
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
            debug!("Java status failed for server {server_id} {host}:{port}: {e}");
            PollResult {
                payload: fail_payload(),
                status_ok: false,
                favicon: String::new(),
            }
        }
        Err(e) => {
            debug!("Java status worker failed for server {server_id} {host}:{port}: {e}");
            PollResult {
                payload: fail_payload(),
                status_ok: false,
                favicon: String::new(),
            }
        }
    }
}

fn ping_java_status_blocking(
    socket_addrs: Vec<SocketAddr>,
    host: &str,
    port: u16,
    timeout: Duration,
) -> Result<craftping::Response, String> {
    if socket_addrs.is_empty() {
        return Err("no socket addresses resolved".to_string());
    }

    let started = Instant::now();
    let mut last_error = None;

    for socket_addr in socket_addrs {
        let elapsed = started.elapsed();
        let Some(remaining) = timeout.checked_sub(elapsed) else {
            return Err(last_error.unwrap_or_else(|| "java status timed out".to_string()));
        };

        match StdTcpStream::connect_timeout(&socket_addr, remaining) {
            Ok(mut stream) => {
                if let Err(e) = stream.set_read_timeout(Some(remaining)) {
                    return Err(format!("failed to set read timeout: {e}"));
                }
                if let Err(e) = stream.set_write_timeout(Some(remaining)) {
                    return Err(format!("failed to set write timeout: {e}"));
                }
                return craftping::sync::ping(&mut stream, host, port).map_err(|e| e.to_string());
            }
            Err(e) => {
                last_error = Some(format!("{socket_addr}: {e}"));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| "java status connect failed".to_string()))
}

/// Poll a Bedrock edition server via RakNet Unconnected Ping (UDP).
#[allow(clippy::too_many_arguments)]
async fn poll_bedrock(
    server_id: &str,
    host: &str,
    port: u16,
    timeout: Duration,
    collected_at: &str,
    country: &str,
    country_code: &str,
    fail_payload: impl FnOnce() -> Value,
) -> PollResult {
    match bedrock::ping_bedrock(host, port, timeout).await {
        Some(status) => {
            let motd = if status.motd_line2.is_empty() {
                status.motd.clone()
            } else {
                format!("{}\n{}", status.motd, status.motd_line2)
            };

            PollResult {
                payload: json!({
                    "server_id": server_id,
                    "collected_at": collected_at,
                    "online": status.online_players,
                    "max_players": status.max_players,
                    "version": status.version,
                    "motd": motd,
                    "country": country,
                    "country_code": country_code,
                    "extra": {
                        "edition": status.edition,
                        "gamemode": status.gamemode,
                    },
                }),
                status_ok: true,
                favicon: String::new(), // Bedrock doesn't have favicons.
            }
        }
        None => {
            debug!("Bedrock status failed for server {server_id} {host}:{port}");
            PollResult {
                payload: fail_payload(),
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
    match http
        .post(&url)
        .headers(hdrs.clone())
        .json(&body)
        .send()
        .await
    {
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

/// Probe whether a Java server also supports Bedrock via RakNet.
pub async fn probe_bedrock_support(host: &str, port: u16, timeout: Duration) -> bool {
    bedrock::ping_bedrock(host, port, timeout).await.is_some()
}

/// Check and potentially probe bedrock support, updating state and server-service.
pub async fn maybe_probe_bedrock(
    http: &Client,
    headers: &Option<reqwest::header::HeaderMap>,
    state: &mut ServerState,
    settings: &Settings,
) {
    if state.next_bedrock_probe.is_none() {
        return;
    }

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
        if let Some(hdrs) = headers {
            let url = format!(
                "{}/internal/servers/{}/edition",
                settings.server.api, state.server_id
            );
            let body = json!({"game_edition": "java_bedrock"});
            match http
                .patch(&url)
                .headers(hdrs.clone())
                .json(&body)
                .send()
                .await
            {
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

    state.next_bedrock_probe = schedule_next_bedrock_probe(
        &state.server,
        0.0,
        settings.collector.bedrock_probe_interval_seconds,
        settings.collector.bedrock_probe_jitter_seconds,
    );
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::ping_java_status_blocking;

    fn write_varint(buffer: &mut Vec<u8>, mut value: i32) {
        loop {
            let mut current = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                current |= 0x80;
            }
            buffer.push(current);
            if value == 0 {
                break;
            }
        }
    }

    fn build_latest_response(json_bytes: &[u8]) -> Vec<u8> {
        let mut packet = Vec::new();
        write_varint(&mut packet, 0x00);
        write_varint(&mut packet, json_bytes.len() as i32);
        packet.extend_from_slice(json_bytes);

        let mut full = Vec::new();
        write_varint(&mut full, packet.len() as i32);
        full.extend_from_slice(&packet);
        full
    }

    #[tokio::test]
    async fn blocking_java_ping_parses_latest_status() {
        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 512];
            let _ = socket.read(&mut request).await.unwrap();

            let response = build_latest_response(
                br#"{"version":{"name":"Velocity 1.7.2-1.21.11","protocol":767},"players":{"max":777,"online":30,"sample":[{"name":"Alice","id":"1"}]},"description":{"text":"Minecrafter"}}"#,
            );
            socket.write_all(&response).await.unwrap();
        });

        let response = tokio::task::spawn_blocking(move || {
            ping_java_status_blocking(
                vec![addr],
                "play.minecrafter.in.ua",
                addr.port(),
                Duration::from_secs(2),
            )
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(response.online_players, 30);
        assert_eq!(response.max_players, 777);
        assert_eq!(response.version, "Velocity 1.7.2-1.21.11");
        assert_eq!(
            response.description.as_ref().map(|d| d.text.as_str()),
            Some("Minecrafter")
        );
    }
}
