//! Server catalog synchronization, trigger files, and Redis force-ping.

use std::collections::{HashMap, HashSet};

use chrono::Utc;
use std::sync::Arc;
use std::time::Instant;

use reqwest::Client;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::Settings;
use crate::geo::GeoCache;
use crate::metrics::{
    SERVER_LIST_REFRESH_DURATION, SERVER_LIST_REFRESH_FAILURE, SERVER_LIST_REFRESH_SUCCESS,
    SERVERS_TOTAL,
};
use crate::models::{
    Edition, ServerListResponse, ServerState, build_state, is_plugin_fallback_mode,
    is_plugin_managed, normalize_edition, parse_address, resolve_state_ports, seed_bedrock_probe,
    wants_bedrock_probe,
};
use crate::ping::poll_server;
use crate::scheduler::SchedulerHandle;

/// Fetch all servers from server-service (paginated).
pub async fn fetch_servers(
    http: &Client,
    headers: &Option<reqwest::header::HeaderMap>,
    server_api: &str,
    page_size: u64,
) -> Vec<Value> {
    async fn fetch_page(
        http: &Client,
        headers: &Option<reqwest::header::HeaderMap>,
        server_api: &str,
        page_size: u64,
        role: Option<&str>,
    ) -> Vec<Value> {
        let mut page = 1u64;
        let mut result = Vec::new();
        loop {
            let mut url = format!("{server_api}/internal/servers?page={page}&limit={page_size}");
            if let Some(r) = role {
                url.push_str(&format!("&role={r}"));
            }
            let mut req = http.get(&url);
            if let Some(h) = headers {
                req = req.headers(h.clone());
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    warn!("Failed to fetch servers (page {page}, role={role:?}): {e}");
                    break;
                }
            };
            if !resp.status().is_success() {
                warn!(
                    "Failed to fetch servers (page {page}, role={role:?}): HTTP {}",
                    resp.status()
                );
                break;
            }
            let data: ServerListResponse = match resp.json().await {
                Ok(d) => d,
                Err(e) => {
                    warn!("Failed to parse server list response: {e}");
                    break;
                }
            };
            let total = data.total;
            result.extend(data.items);
            page += 1;
            if result.len() as u64 >= total {
                break;
            }
        }
        result
    }

    let mut servers = fetch_page(http, headers, server_api, page_size, None).await;
    let subs = fetch_page(http, headers, server_api, page_size, Some("subserver")).await;
    servers.extend(subs);
    servers
}

/// Fetch one server by ID from server-service.
pub async fn fetch_server_by_id(
    http: &Client,
    headers: &Option<reqwest::header::HeaderMap>,
    server_id: &str,
    server_api: &str,
    timeout_seconds: f64,
) -> Option<Value> {
    let id = server_id.trim();
    if id.is_empty() {
        return None;
    }
    let url = format!("{server_api}/internal/servers/{id}");
    let timeout = std::time::Duration::from_secs_f64(timeout_seconds);
    let mut req = http.get(&url).timeout(timeout);
    if let Some(h) = headers {
        req = req.headers(h.clone());
    }
    match req.send().await {
        Ok(resp) => {
            if resp.status().as_u16() == 404 {
                return None;
            }
            if !resp.status().is_success() {
                return None;
            }
            resp.json::<Value>().await.ok()
        }
        Err(_) => None,
    }
}

/// Refresh the server catalog once, updating states map.
pub async fn refresh_servers_once(
    http: &Client,
    headers: &Option<reqwest::header::HeaderMap>,
    states: &Arc<Mutex<HashMap<String, ServerState>>>,
    scheduler: &SchedulerHandle,
    settings: &Settings,
) -> bool {
    let started = Instant::now();

    let servers = fetch_servers(
        http,
        headers,
        &settings.server.api,
        settings.collector.page_size,
    )
    .await;
    if servers.is_empty() {
        SERVER_LIST_REFRESH_FAILURE.inc();
        SERVER_LIST_REFRESH_DURATION.set(started.elapsed().as_secs_f64());
        return false;
    }

    let now = scheduler.now();
    let mut map = states.lock().await;
    let mut incoming_keys = HashSet::new();
    let mut added = 0u32;
    let mut updated = 0u32;

    for server in &servers {
        let key = server
            .get("id")
            .map(|v| match v {
                Value::Number(n) => n.to_string(),
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .unwrap_or_default();
        if key.is_empty() {
            continue;
        }
        incoming_keys.insert(key.clone());

        if let Some(state) = map.get_mut(&key) {
            // Update existing.
            let host_value = server
                .get("ping_ip_or_domain")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .or_else(|| server.get("ip_or_domain").and_then(|v| v.as_str()))
                .unwrap_or("");
            let (host, parsed_port) = parse_address(host_value);
            let edition = normalize_edition(server.get("game_edition").and_then(|v| v.as_str()));
            let (port, bedrock_port) = resolve_state_ports(server, &host, parsed_port, edition);

            if state.host != host
                || state.port != port
                || state.edition != edition
                || state.bedrock_port != bedrock_port
            {
                state.host = host;
                state.port = port;
                state.edition = edition;
                state.bedrock_port = bedrock_port;
                state.next_query_attempt = now;
                state.reset_emitted();
                updated += 1;
            }
            state.server = server.clone();
            state.plugin_managed = is_plugin_managed(server);
            let previous_fallback_mode = state.plugin_fallback_mode;
            state.plugin_fallback_mode = is_plugin_fallback_mode(
                server,
                settings.collector.plugin_fallback_stale_seconds,
                Utc::now(),
            );
            if state.plugin_fallback_mode && !previous_fallback_mode {
                state.next_due = now;
                scheduler.schedule(key.clone(), state.next_due);
            }
            if wants_bedrock_probe(server, settings.collector.bedrock_probe_interval_seconds) {
                if state.next_bedrock_probe.is_none() {
                    state.next_bedrock_probe = seed_bedrock_probe(
                        server,
                        now,
                        settings.collector.bedrock_probe_interval_seconds,
                    );
                }
            } else {
                state.next_bedrock_probe = None;
            }
        } else {
            // New server.
            let state = build_state(
                server.clone(),
                now,
                settings.collector.startup_spread_seconds,
                settings.collector.bedrock_probe_interval_seconds,
                settings.collector.plugin_fallback_stale_seconds,
            );
            let next_due = state.next_due;
            let state_key = state.key.clone();
            map.insert(key, state);
            scheduler.schedule(state_key, next_due);
            added += 1;
        }
    }

    // Remove servers no longer in catalog.
    let removed_keys: Vec<String> = map
        .keys()
        .filter(|k| !incoming_keys.contains(*k))
        .cloned()
        .collect();
    for key in &removed_keys {
        map.remove(key);
    }

    SERVER_LIST_REFRESH_SUCCESS.inc();
    SERVER_LIST_REFRESH_DURATION.set(started.elapsed().as_secs_f64());
    SERVERS_TOTAL.set(map.len() as i64);

    info!(
        "Server catalog refreshed: total={} added={added} updated={updated} removed={}",
        map.len(),
        removed_keys.len()
    );
    true
}

/// Periodically refresh the server catalog.
pub async fn refresh_servers_loop(
    http: Client,
    headers: Option<reqwest::header::HeaderMap>,
    states: Arc<Mutex<HashMap<String, ServerState>>>,
    scheduler: SchedulerHandle,
    settings: Arc<Settings>,
) {
    loop {
        let success = refresh_servers_once(&http, &headers, &states, &scheduler, &settings).await;
        let delay = if success {
            settings.collector.server_list_refresh_seconds
        } else {
            settings.collector.server_list_retry_seconds
        };
        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
    }
}

/// Poll a trigger file for manual reschedule commands.
pub async fn trigger_loop(
    states: Arc<Mutex<HashMap<String, ServerState>>>,
    scheduler: SchedulerHandle,
    settings: Arc<Settings>,
) {
    let path = &settings.collector.trigger_file_path;
    if path.is_empty() {
        return;
    }
    let poll_interval =
        std::time::Duration::from_secs_f64(settings.collector.trigger_poll_seconds.max(0.5));

    loop {
        if let Ok(content) = tokio::fs::read_to_string(path).await {
            let _ = tokio::fs::remove_file(path).await;
            let targets = parse_trigger_content(&content);
            let now = scheduler.now();
            let mut map = states.lock().await;

            match targets {
                None => {
                    // All servers.
                    for state in map.values_mut() {
                        state.next_due = now;
                        scheduler.schedule(state.key.clone(), now);
                    }
                    info!("Manual trigger: scheduled all servers ({})", map.len());
                }
                Some(ids) => {
                    let mut scheduled = 0u32;
                    for id in &ids {
                        if let Some(state) = map.get_mut(id.as_str()) {
                            state.next_due = now;
                            scheduler.schedule(state.key.clone(), now);
                            scheduled += 1;
                        }
                    }
                    info!("Manual trigger: scheduled {scheduled} servers");
                }
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Parse trigger file content. Returns None for "all", Some(ids) for specific targets.
pub fn parse_trigger_content(content: &str) -> Option<Vec<String>> {
    let content = content.trim();
    if content.is_empty() {
        return Some(Vec::new());
    }
    let lowered = content.to_lowercase();
    if lowered == "all" || lowered == "*" {
        return None;
    }
    // Try JSON.
    if (content.starts_with('{') || content.starts_with('['))
        && let Ok(val) = serde_json::from_str::<Value>(content)
    {
        if let Some(obj) = val.as_object() {
            if obj.get("all").and_then(|v| v.as_bool()).unwrap_or(false) {
                return None;
            }
            let ids = obj
                .get("server_ids")
                .or_else(|| obj.get("ids"))
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| {
                            let s = match v {
                                Value::String(s) => s.trim().to_string(),
                                Value::Number(n) => n.to_string(),
                                _ => return None,
                            };
                            if s.is_empty() { None } else { Some(s) }
                        })
                        .collect()
                })
                .unwrap_or_default();
            return Some(ids);
        }
        if let Some(arr) = val.as_array() {
            let ids: Vec<String> = arr
                .iter()
                .filter_map(|v| {
                    let s = match v {
                        Value::String(s) => s.trim().to_string(),
                        Value::Number(n) => n.to_string(),
                        _ => return None,
                    };
                    if s.is_empty() { None } else { Some(s) }
                })
                .collect();
            return Some(ids);
        }
    }
    // Space/comma separated.
    let ids: Vec<String> = content
        .replace(',', " ")
        .split_whitespace()
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect();
    Some(ids)
}

/// Listen to Redis BLPOP for force-ping requests.
pub async fn redis_force_ping_loop(
    http: Client,
    headers: Option<reqwest::header::HeaderMap>,
    redis_url: String,
    states: Arc<Mutex<HashMap<String, ServerState>>>,
    scheduler: SchedulerHandle,
    settings: Arc<Settings>,
) {
    let queue_key = settings.collector.force_ping_queue_key.trim().to_string();
    if queue_key.is_empty() {
        return;
    }

    let client = match redis::Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to connect Redis for force-ping: {e}");
            return;
        }
    };

    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            warn!("Redis force-ping connection failed: {e}");
            return;
        }
    };

    let timeout_secs = settings.collector.force_ping_pop_timeout_seconds.max(0.5) as usize;

    loop {
        let result: Result<Option<(String, String)>, _> = redis::cmd("BLPOP")
            .arg(&queue_key)
            .arg(timeout_secs)
            .query_async(&mut conn)
            .await;

        match result {
            Ok(Some((_key, target))) => {
                let target = target.trim().to_string();
                if target.is_empty() {
                    continue;
                }
                let now = scheduler.now();
                let mut map = states.lock().await;

                if let Some(state) = map.get_mut(&target) {
                    state.next_due = now;
                    scheduler.schedule(state.key.clone(), now);
                    info!("Force ping: scheduled {target}");
                } else {
                    // Fetch from server-service.
                    drop(map);
                    if let Some(server) = fetch_server_by_id(
                        &http,
                        &headers,
                        &target,
                        &settings.server.api,
                        settings.collector.http_timeout_seconds,
                    )
                    .await
                    {
                        let state = build_state(
                            server,
                            now,
                            settings.collector.startup_spread_seconds,
                            settings.collector.bedrock_probe_interval_seconds,
                            settings.collector.plugin_fallback_stale_seconds,
                        );
                        let state_key = state.key.clone();
                        let mut map = states.lock().await;
                        map.insert(target.clone(), state);
                        scheduler.schedule(state_key, now);
                        info!("Force ping: fetched and scheduled {target}");
                    }
                }
            }
            Ok(None) => {} // Timeout, loop again.
            Err(e) => {
                warn!("Force ping loop error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// Listen to Redis BLPOP for ad-hoc discovery candidate probes.
///
/// Discovery candidates are not yet `Server` rows (they live in
/// `server_discovery_candidates` until an admin approves them) so they have
/// no entry in the poller's scheduler. Instead, the discovery worker and the
/// admin preview endpoint push `{candidate_id, host, port, edition}` JSON
/// payloads to `leavepulse:discovery:probe-request`, and we answer with the
/// raw probe result at `leavepulse:discovery:probe-result:{candidate_id}`.
pub async fn redis_discovery_probe_loop(
    http: Client,
    redis_url: String,
    geo_cache: Arc<GeoCache>,
    settings: Arc<Settings>,
) {
    let queue_key = settings.collector.discovery_probe_queue_key.trim().to_string();
    if queue_key.is_empty() {
        return;
    }
    let result_prefix = settings
        .collector
        .discovery_probe_result_key_prefix
        .trim()
        .to_string();
    if result_prefix.is_empty() {
        return;
    }
    let ttl_seconds = settings
        .collector
        .discovery_probe_result_ttl_seconds
        .max(60);

    let client = match redis::Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to connect Redis for discovery probe: {e}");
            return;
        }
    };

    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            warn!("Redis discovery probe connection failed: {e}");
            return;
        }
    };

    let pop_timeout = settings
        .collector
        .force_ping_pop_timeout_seconds
        .max(0.5) as usize;

    loop {
        let popped: Result<Option<(String, String)>, _> = redis::cmd("BLPOP")
            .arg(&queue_key)
            .arg(pop_timeout)
            .query_async(&mut conn)
            .await;

        match popped {
            Ok(Some((_key, raw))) => {
                let raw = raw.trim().to_string();
                if raw.is_empty() {
                    continue;
                }
                let request: DiscoveryProbeRequest = match serde_json::from_str(&raw) {
                    Ok(req) => req,
                    Err(e) => {
                        warn!("Discovery probe: invalid request payload {raw:?}: {e}");
                        continue;
                    }
                };
                let DiscoveryProbeRequest {
                    candidate_id,
                    host,
                    port,
                    edition,
                } = request;
                let host = host.trim().to_string();
                if candidate_id.trim().is_empty() || host.is_empty() {
                    warn!("Discovery probe: missing candidate_id or host in {raw:?}");
                    continue;
                }
                let edition_parsed = match edition.as_deref() {
                    Some(value) if value.eq_ignore_ascii_case("bedrock") => Edition::Bedrock,
                    _ => Edition::Java,
                };

                let started = Instant::now();
                let result = poll_server(
                    &http,
                    &candidate_id,
                    &host,
                    port,
                    edition_parsed,
                    &geo_cache,
                    &settings,
                )
                .await;
                let elapsed_ms = started.elapsed().as_millis() as u64;

                let probed_at = Utc::now().to_rfc3339();
                let payload_obj = result.payload.as_object();
                let online = payload_obj
                    .and_then(|m| m.get("online"))
                    .and_then(Value::as_u64);
                let max_players = payload_obj
                    .and_then(|m| m.get("max_players"))
                    .and_then(Value::as_u64);
                let version = payload_obj
                    .and_then(|m| m.get("version"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let motd = payload_obj
                    .and_then(|m| m.get("motd"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();

                let probe_response = json!({
                    "candidate_id": candidate_id,
                    "status_ok": result.status_ok,
                    "error": if result.status_ok { Value::Null } else { json!("probe failed") },
                    "probed_at": probed_at,
                    "host": host,
                    "port": port,
                    "edition": match edition_parsed {
                        Edition::Bedrock => "bedrock",
                        Edition::Java => "java",
                    },
                    "online": online,
                    "max_players": max_players,
                    "version": version,
                    "motd": motd,
                    "favicon": result.favicon,
                    "elapsed_ms": elapsed_ms,
                });
                let body = match serde_json::to_vec(&probe_response) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        warn!("Discovery probe: failed to encode result for {candidate_id}: {e}");
                        continue;
                    }
                };
                let key = format!("{result_prefix}{candidate_id}");
                let set: Result<(), _> = redis::cmd("SET")
                    .arg(&key)
                    .arg(body)
                    .arg("EX")
                    .arg(ttl_seconds)
                    .query_async(&mut conn)
                    .await;
                match set {
                    Ok(()) => {
                        info!(
                            "Discovery probe: candidate={} host={}:{:?} ok={} elapsed_ms={}",
                            candidate_id, host, port, result.status_ok, elapsed_ms
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Discovery probe: failed to write result for {candidate_id}: {e}"
                        );
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                debug!("Discovery probe loop error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct DiscoveryProbeRequest {
    candidate_id: String,
    host: String,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    edition: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_content() {
        assert_eq!(parse_trigger_content(""), Some(vec![]));
        assert_eq!(parse_trigger_content("  "), Some(vec![]));
    }

    #[test]
    fn test_all_keyword() {
        assert_eq!(parse_trigger_content("all"), None);
        assert_eq!(parse_trigger_content("ALL"), None);
        assert_eq!(parse_trigger_content("*"), None);
    }

    #[test]
    fn test_comma_separated() {
        let result = parse_trigger_content("1,2,3").unwrap();
        assert_eq!(result, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_space_separated() {
        let result = parse_trigger_content("10 20 30").unwrap();
        assert_eq!(result, vec!["10", "20", "30"]);
    }

    #[test]
    fn test_json_object_with_server_ids() {
        let result = parse_trigger_content(r#"{"server_ids": ["1", "2"]}"#).unwrap();
        assert_eq!(result, vec!["1", "2"]);
    }

    #[test]
    fn test_json_object_with_all() {
        assert_eq!(parse_trigger_content(r#"{"all": true}"#), None);
    }

    #[test]
    fn test_json_array() {
        let result = parse_trigger_content(r#"["5", "10"]"#).unwrap();
        assert_eq!(result, vec!["5", "10"]);
    }

    #[test]
    fn test_json_numeric_ids() {
        let result = parse_trigger_content(r#"{"ids": [1, 2, 3]}"#).unwrap();
        assert_eq!(result, vec!["1", "2", "3"]);
    }
}
