//! State models and server address helpers.

use std::net::IpAddr;

use rand::Rng;
use serde::Deserialize;
use serde_json::Value;

pub const JAVA_DEFAULT_PORT: u16 = 25565;
pub const BEDROCK_DEFAULT_PORT: u16 = 19132;

/// Per-server polling state held in memory.
pub struct ServerState {
    pub key: String,
    pub server_id: String,
    pub server: Value,
    pub host: String,
    pub port: Option<u16>,
    pub bedrock_port: Option<u16>,
    pub edition: Edition,
    pub next_due: f64,
    pub next_bedrock_probe: Option<f64>,
    pub next_query_attempt: f64,
    pub plugin_managed: bool,
    pub last_favicon_hash: Option<String>,
    pub has_succeeded: bool,
    pub initial_failures: u32,
    pub consecutive_failures: u32,
    pub has_emitted_state: bool,
    pub last_emitted_up: Option<bool>,
    pub last_emitted_online: Option<i64>,
    pub last_emitted_max_players: Option<i64>,
    pub last_emitted_version: Option<String>,
    pub last_emitted_motd: Option<String>,
    pub last_emitted_country: Option<String>,
    pub last_emitted_country_code: Option<String>,
    pub last_emitted_players_hash: Option<String>,
}

impl ServerState {
    /// Reset all emitted fields (forces full re-emission).
    pub fn reset_emitted(&mut self) {
        self.has_emitted_state = false;
        self.last_emitted_up = None;
        self.last_emitted_online = None;
        self.last_emitted_max_players = None;
        self.last_emitted_version = None;
        self.last_emitted_motd = None;
        self.last_emitted_country = None;
        self.last_emitted_country_code = None;
        self.last_emitted_players_hash = None;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edition {
    Java,
    Bedrock,
}

impl Edition {
    pub fn as_str(&self) -> &'static str {
        match self {
            Edition::Java => "java",
            Edition::Bedrock => "bedrock",
        }
    }

    pub fn default_port(&self) -> u16 {
        match self {
            Edition::Java => JAVA_DEFAULT_PORT,
            Edition::Bedrock => BEDROCK_DEFAULT_PORT,
        }
    }
}

/// Paginated server list response from server-service.
#[derive(Debug, Deserialize)]
pub struct ServerListResponse {
    #[serde(default)]
    pub items: Vec<Value>,
    #[serde(default)]
    pub total: u64,
}

/// Parse "host[:port]" including IPv6 bracket notation.
pub fn parse_address(value: &str) -> (String, Option<u16>) {
    // IPv6 bracket notation: [::1]:25565
    if value.starts_with('[') {
        if let Some(bracket_end) = value.find(']') {
            let host = &value[1..bracket_end];
            let rest = &value[bracket_end + 1..];
            if let Some(port_str) = rest.strip_prefix(':') {
                if let Ok(port) = port_str.parse::<u16>() {
                    return (host.to_string(), Some(port));
                }
            }
            return (host.to_string(), None);
        }
    }
    // Regular host:port — only if exactly one colon (not raw IPv6)
    if value.contains(':') && value.matches(':').count() == 1 {
        if let Some((host, port_str)) = value.rsplit_once(':') {
            if let Ok(port) = port_str.parse::<u16>() {
                return (host.to_string(), Some(port));
            }
        }
    }
    (value.to_string(), None)
}

fn looks_like_ip(host: &str) -> bool {
    host.parse::<IpAddr>().is_ok()
}

pub fn normalize_edition(value: Option<&str>) -> Edition {
    match value.map(|v| v.to_lowercase()).as_deref() {
        Some("bedrock") => Edition::Bedrock,
        _ => Edition::Java,
    }
}

/// Resolve effective poll port and bedrock port from server catalog data.
pub fn resolve_state_ports(
    server: &Value,
    host: &str,
    parsed_port: Option<u16>,
    edition: Edition,
) -> (Option<u16>, Option<u16>) {
    let mut port = parsed_port;

    let ping_port = server
        .get("ping_port")
        .and_then(|v| match v {
            Value::Number(n) => n.as_u64().map(|n| n as u16),
            Value::String(s) => s.parse::<u16>().ok(),
            _ => None,
        });

    if let Some(pp) = ping_port {
        // Ignore default Java port for hostname targets to honor SRV records.
        let ignore_default_java =
            edition != Edition::Bedrock && pp == JAVA_DEFAULT_PORT && !looks_like_ip(host);
        if !ignore_default_java {
            port = Some(pp);
        }
    }

    let bedrock_port = server
        .get("bedrock_port")
        .and_then(|v| match v {
            Value::Number(n) => n.as_u64().map(|n| n as u16),
            Value::String(s) => s.parse::<u16>().ok(),
            _ => None,
        });

    // For Bedrock edition with no explicit port, use bedrock_port.
    if edition == Edition::Bedrock && port.is_none() {
        if let Some(bp) = bedrock_port {
            port = Some(bp);
        }
    }

    (port, bedrock_port)
}

pub fn is_plugin_managed(server: &Value) -> bool {
    let verified = server
        .get("is_verified")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !verified {
        return false;
    }
    let source = server
        .get("verification_source")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_lowercase();
    source == "plugin"
}

pub fn wants_bedrock_probe(server: &Value, probe_interval: u64) -> bool {
    if probe_interval == 0 {
        return false;
    }
    let edition = server
        .get("game_edition")
        .and_then(|v| v.as_str())
        .unwrap_or("java")
        .to_lowercase();
    edition == "java"
}

pub fn seed_bedrock_probe(server: &Value, now: f64, probe_interval: u64) -> Option<f64> {
    if !wants_bedrock_probe(server, probe_interval) {
        return None;
    }
    let spread = (probe_interval as f64).max(1.0);
    let jitter = rand::thread_rng().gen_range(0.0..spread);
    Some(now + jitter)
}

pub fn schedule_next_bedrock_probe(
    server: &Value,
    now: f64,
    probe_interval: u64,
    probe_jitter: u64,
) -> Option<f64> {
    if !wants_bedrock_probe(server, probe_interval) {
        return None;
    }
    let jitter = rand::thread_rng().gen_range(-1.0..1.0) * probe_jitter as f64;
    let interval = probe_interval as f64 + jitter;
    Some(now + interval.max(60.0))
}

pub fn is_internal_only_host(host: &str) -> bool {
    host.trim().to_lowercase().starts_with("internal-")
}

/// Build initial ServerState from a catalog server JSON object.
pub fn build_state(server: Value, now: f64, startup_spread: u64, probe_interval: u64) -> ServerState {
    let server_id = server
        .get("id")
        .map(|v| match v {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default();
    let key = server_id.clone();

    let host_value = server
        .get("ping_ip_or_domain")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| server.get("ip_or_domain").and_then(|v| v.as_str()))
        .unwrap_or("");
    let (host, parsed_port) = parse_address(host_value);

    let edition = normalize_edition(server.get("game_edition").and_then(|v| v.as_str()));
    let (port, bedrock_port) = resolve_state_ports(&server, &host, parsed_port, edition);

    let spread = (startup_spread as f64).max(1.0);
    let next_due = now + rand::thread_rng().gen_range(0.0..spread);
    let next_bedrock_probe = seed_bedrock_probe(&server, now, probe_interval);
    let plugin_managed = is_plugin_managed(&server);

    ServerState {
        key,
        server_id,
        server,
        host,
        port,
        bedrock_port,
        edition,
        next_due,
        next_bedrock_probe,
        next_query_attempt: now,
        plugin_managed,
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
    }
}
