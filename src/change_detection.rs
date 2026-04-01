//! Change detection: compare current poll result with last emitted state.

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::models::ServerState;

/// Extract deduplicated player names from the `extra.players` field.
fn extract_players(extra: &Value) -> Vec<String> {
    let players = match extra.get("players").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Vec::new(),
    };
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();
    for item in players {
        if let Some(name) = item.as_str() {
            let name = name.trim().to_string();
            if !name.is_empty() && seen.insert(name.clone()) {
                result.push(name);
            }
        }
    }
    result
}

/// Hash player list for change detection.
fn players_hash(players: &[String]) -> Option<String> {
    if players.is_empty() {
        return None;
    }
    let payload = players.join("\n");
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    Some(format!("{:x}", hasher.finalize()))
}

/// Compare current poll payload against last emitted state.
/// Returns the enriched payload with event_type, or None if unchanged.
///
/// When `heartbeat_interval` is non-zero and no change is detected, a
/// `"heartbeat"` event is emitted if at least `heartbeat_interval` seconds
/// have elapsed since the last emission. This prevents gaps in ClickHouse
/// time-series data when server state is stable.
pub fn build_change_payload(
    state: &mut ServerState,
    payload: &Value,
    status_ok: bool,
    now: f64,
    heartbeat_interval: f64,
) -> Option<Value> {
    let current_up = status_ok;
    let current_online: Option<i64> = if !current_up {
        Some(0)
    } else {
        payload.get("online").and_then(|v| v.as_i64())
    };

    let mut current_max = payload.get("max_players").and_then(|v| v.as_i64());
    let mut current_version = payload
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let mut current_motd = payload
        .get("motd")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let mut current_country = payload
        .get("country")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let mut current_country_code = payload
        .get("country_code")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    let extra = payload.get("extra").cloned().unwrap_or(json!({}));
    let mut players = extract_players(&extra);
    let mut phash = players_hash(&players);

    // When offline, preserve last known metadata.
    if !current_up {
        current_max = state.last_emitted_max_players;
        current_version = state.last_emitted_version.clone().unwrap_or_default();
        current_motd = state.last_emitted_motd.clone().unwrap_or_default();
        if current_country.is_empty() {
            current_country = state.last_emitted_country.clone().unwrap_or_default();
        }
        if current_country_code.is_empty() {
            current_country_code = state.last_emitted_country_code.clone().unwrap_or_default();
        }
        players.clear();
        phash = None;
    }

    let changed_online = state.last_emitted_online != current_online;
    let changed_up = state.last_emitted_up != Some(current_up);
    let changed_meta = state.last_emitted_max_players != current_max
        || state.last_emitted_version.as_deref().unwrap_or("") != current_version
        || state.last_emitted_motd.as_deref().unwrap_or("") != current_motd
        || state.last_emitted_country.as_deref().unwrap_or("") != current_country
        || state.last_emitted_country_code.as_deref().unwrap_or("") != current_country_code;
    let changed_players = state.last_emitted_players_hash != phash;

    let event_type = if !state.has_emitted_state {
        "state_full"
    } else if changed_up {
        if current_up {
            "server_up"
        } else {
            "server_down"
        }
    } else if changed_online {
        "online_changed"
    } else if changed_meta || changed_players {
        "state_updated"
    } else if heartbeat_interval > 0.0 {
        // No change — check heartbeat.
        let elapsed = match state.last_emitted_at {
            Some(t) => now - t,
            None => heartbeat_interval, // force emit if never emitted
        };
        if elapsed >= heartbeat_interval {
            "heartbeat"
        } else {
            return None;
        }
    } else {
        // No change, heartbeat disabled.
        return None;
    };

    let mut out_extra = serde_json::Map::new();
    if !players.is_empty() {
        out_extra.insert("players".to_string(), json!(players));
    }
    out_extra.insert(
        "poll_status".to_string(),
        json!(if current_up { "ok" } else { "failed" }),
    );
    if state.plugin_managed && !state.plugin_fallback_mode {
        out_extra.insert("compare_probe".to_string(), json!(true));
    }
    if state.plugin_fallback_mode {
        out_extra.insert("plugin_fallback".to_string(), json!(true));
    }

    let out = json!({
        "server_id": state.server_id,
        "collected_at": payload.get("collected_at"),
        "online": current_online,
        "max_players": current_max,
        "version": current_version,
        "motd": current_motd,
        "country": current_country,
        "country_code": current_country_code,
        "event_type": event_type,
        "extra": out_extra,
    });

    // Update emitted state.
    state.has_emitted_state = true;
    state.last_emitted_up = Some(current_up);
    state.last_emitted_online = current_online;
    state.last_emitted_max_players = current_max;
    state.last_emitted_version = Some(current_version);
    state.last_emitted_motd = Some(current_motd);
    state.last_emitted_country = Some(current_country);
    state.last_emitted_country_code = Some(current_country_code);
    state.last_emitted_players_hash = phash;
    state.last_emitted_at = Some(now);

    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Edition, ServerState};

    fn make_state() -> ServerState {
        ServerState {
            key: "1".to_string(),
            server_id: "1".to_string(),
            server: json!({}),
            host: "mc.example.com".to_string(),
            port: None,
            bedrock_port: None,
            edition: Edition::Java,
            next_due: 0.0,
            next_bedrock_probe: None,
            next_query_attempt: 0.0,
            plugin_managed: false,
            plugin_fallback_mode: false,
            last_favicon_hash: None,
            has_succeeded: false,
            initial_failures: 0,
            consecutive_failures: 0,
            has_emitted_state: false,
            last_emitted_up: None,
            last_emitted_online: None,
            last_emitted_max_players: None,
            last_emitted_version: None,
            last_emitted_motd: None,
            last_emitted_country: None,
            last_emitted_country_code: None,
            last_emitted_players_hash: None,
            last_emitted_at: None,
        }
    }

    #[test]
    fn test_first_emission_is_state_full() {
        let mut state = make_state();
        let payload = json!({
            "server_id": "1",
            "collected_at": "2025-01-01T00:00:00",
            "online": 10,
            "max_players": 100,
            "version": "1.20.4",
            "motd": "Hello",
            "country": "US",
            "country_code": "US",
            "extra": {},
        });
        let result = build_change_payload(&mut state, &payload, true, 1000.0, 0.0).unwrap();
        assert_eq!(result["event_type"], "state_full");
        assert!(state.has_emitted_state);
        assert_eq!(state.last_emitted_up, Some(true));
        assert_eq!(state.last_emitted_online, Some(10));
    }

    #[test]
    fn test_no_change_returns_none() {
        let mut state = make_state();
        let payload = json!({
            "collected_at": "2025-01-01T00:00:00",
            "online": 10,
            "max_players": 100,
            "version": "1.20.4",
            "motd": "Hello",
            "country": "US",
            "country_code": "US",
            "extra": {},
        });
        // First call → state_full.
        build_change_payload(&mut state, &payload, true, 1000.0, 0.0);
        // Second call with same data → None.
        let result = build_change_payload(&mut state, &payload, true, 1001.0, 0.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_server_down_event() {
        let mut state = make_state();
        let up_payload = json!({
            "collected_at": "t1",
            "online": 5,
            "max_players": 20,
            "version": "1.20",
            "motd": "Hi",
            "country": "",
            "country_code": "",
            "extra": {},
        });
        build_change_payload(&mut state, &up_payload, true, 1000.0, 0.0);

        let down_payload = json!({
            "collected_at": "t2",
            "online": null,
            "max_players": null,
            "version": "",
            "motd": "",
            "country": "",
            "country_code": "",
        });
        let result = build_change_payload(&mut state, &down_payload, false, 1001.0, 0.0).unwrap();
        assert_eq!(result["event_type"], "server_down");
        assert_eq!(result["online"], 0);
        // Should preserve last known metadata.
        assert_eq!(result["version"], "1.20");
        assert_eq!(result["motd"], "Hi");
    }

    #[test]
    fn test_server_up_event() {
        let mut state = make_state();
        // First: state_full while down.
        let down_payload = json!({
            "collected_at": "t1",
            "online": null,
            "country": "",
            "country_code": "",
        });
        build_change_payload(&mut state, &down_payload, false, 1000.0, 0.0);

        // Second: server comes back up.
        let up_payload = json!({
            "collected_at": "t2",
            "online": 5,
            "max_players": 20,
            "version": "1.20",
            "motd": "Hi",
            "country": "",
            "country_code": "",
            "extra": {},
        });
        let result = build_change_payload(&mut state, &up_payload, true, 1001.0, 0.0).unwrap();
        assert_eq!(result["event_type"], "server_up");
    }

    #[test]
    fn test_online_changed_event() {
        let mut state = make_state();
        let payload1 = json!({
            "collected_at": "t1",
            "online": 10,
            "max_players": 100,
            "version": "1.20",
            "motd": "Hi",
            "country": "",
            "country_code": "",
            "extra": {},
        });
        build_change_payload(&mut state, &payload1, true, 1000.0, 0.0);

        let payload2 = json!({
            "collected_at": "t2",
            "online": 15,
            "max_players": 100,
            "version": "1.20",
            "motd": "Hi",
            "country": "",
            "country_code": "",
            "extra": {},
        });
        let result = build_change_payload(&mut state, &payload2, true, 1001.0, 0.0).unwrap();
        assert_eq!(result["event_type"], "online_changed");
    }

    #[test]
    fn test_player_change_detected() {
        let mut state = make_state();
        let payload1 = json!({
            "collected_at": "t1",
            "online": 2,
            "max_players": 20,
            "version": "1.20",
            "motd": "Hi",
            "country": "",
            "country_code": "",
            "extra": {"players": ["Alice", "Bob"]},
        });
        build_change_payload(&mut state, &payload1, true, 1000.0, 0.0);

        let payload2 = json!({
            "collected_at": "t2",
            "online": 2,
            "max_players": 20,
            "version": "1.20",
            "motd": "Hi",
            "country": "",
            "country_code": "",
            "extra": {"players": ["Alice", "Charlie"]},
        });
        let result = build_change_payload(&mut state, &payload2, true, 1001.0, 0.0).unwrap();
        assert_eq!(result["event_type"], "state_updated");
    }

    #[test]
    fn test_heartbeat_emits_when_interval_elapsed() {
        let mut state = make_state();
        let payload = json!({
            "collected_at": "2025-01-01T00:00:00",
            "online": 10,
            "max_players": 100,
            "version": "1.20.4",
            "motd": "Hello",
            "country": "US",
            "country_code": "US",
            "extra": {},
        });
        // First call → state_full at t=1000.
        let result = build_change_payload(&mut state, &payload, true, 1000.0, 300.0).unwrap();
        assert_eq!(result["event_type"], "state_full");
        assert_eq!(state.last_emitted_at, Some(1000.0));

        // Second call at t=1100, only 100s passed — no emit.
        let result = build_change_payload(&mut state, &payload, true, 1100.0, 300.0);
        assert!(result.is_none());
        // last_emitted_at unchanged.
        assert_eq!(state.last_emitted_at, Some(1000.0));

        // Third call at t=1300, 300s elapsed — heartbeat.
        let result = build_change_payload(&mut state, &payload, true, 1300.0, 300.0).unwrap();
        assert_eq!(result["event_type"], "heartbeat");
        assert_eq!(state.last_emitted_at, Some(1300.0));
    }

    #[test]
    fn test_heartbeat_disabled_when_zero() {
        let mut state = make_state();
        let payload = json!({
            "collected_at": "2025-01-01T00:00:00",
            "online": 10,
            "max_players": 100,
            "version": "1.20.4",
            "motd": "Hello",
            "country": "US",
            "country_code": "US",
            "extra": {},
        });
        build_change_payload(&mut state, &payload, true, 1000.0, 0.0);
        // With heartbeat_interval=0, no heartbeat even after long time.
        let result = build_change_payload(&mut state, &payload, true, 9999.0, 0.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_plugin_managed_adds_compare_probe() {
        let mut state = make_state();
        state.plugin_managed = true;
        let payload = json!({
            "collected_at": "t1",
            "online": 5,
            "max_players": 20,
            "version": "1.20",
            "motd": "Hi",
            "country": "",
            "country_code": "",
            "extra": {},
        });
        let result = build_change_payload(&mut state, &payload, true, 1000.0, 0.0).unwrap();
        assert_eq!(result["extra"]["compare_probe"], true);
    }

    #[test]
    fn test_plugin_fallback_does_not_add_compare_probe() {
        let mut state = make_state();
        state.plugin_managed = true;
        state.plugin_fallback_mode = true;
        let payload = json!({
            "collected_at": "t1",
            "online": 5,
            "max_players": 20,
            "version": "1.20",
            "motd": "Hi",
            "country": "",
            "country_code": "",
            "extra": {},
        });
        let result = build_change_payload(&mut state, &payload, true, 1000.0, 0.0).unwrap();
        assert_eq!(result["extra"]["plugin_fallback"], true);
        assert!(result["extra"].get("compare_probe").is_none());
    }
}
