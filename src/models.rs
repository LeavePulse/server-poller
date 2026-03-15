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
    pub last_emitted_at: Option<f64>,
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
        self.last_emitted_at = None;
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
    if value.starts_with('[')
        && let Some(bracket_end) = value.find(']')
    {
        let host = &value[1..bracket_end];
        let rest = &value[bracket_end + 1..];
        if let Some(port_str) = rest.strip_prefix(':')
            && let Ok(port) = port_str.parse::<u16>()
        {
            return (host.to_string(), Some(port));
        }
        return (host.to_string(), None);
    }
    // Regular host:port — only if exactly one colon (not raw IPv6)
    if value.contains(':')
        && value.matches(':').count() == 1
        && let Some((host, port_str)) = value.rsplit_once(':')
        && let Ok(port) = port_str.parse::<u16>()
    {
        return (host.to_string(), Some(port));
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

    let ping_port = server.get("ping_port").and_then(|v| match v {
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

    let bedrock_port = server.get("bedrock_port").and_then(|v| match v {
        Value::Number(n) => n.as_u64().map(|n| n as u16),
        Value::String(s) => s.parse::<u16>().ok(),
        _ => None,
    });

    // For Bedrock edition with no explicit port, use bedrock_port.
    if edition == Edition::Bedrock
        && port.is_none()
        && let Some(bp) = bedrock_port
    {
        port = Some(bp);
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
pub fn build_state(
    server: Value,
    now: f64,
    startup_spread: u64,
    probe_interval: u64,
) -> ServerState {
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
        last_emitted_at: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_address_simple() {
        let (host, port) = parse_address("mc.example.com");
        assert_eq!(host, "mc.example.com");
        assert_eq!(port, None);
    }

    #[test]
    fn test_parse_address_with_port() {
        let (host, port) = parse_address("mc.example.com:25566");
        assert_eq!(host, "mc.example.com");
        assert_eq!(port, Some(25566));
    }

    #[test]
    fn test_parse_address_ipv6_bracket() {
        let (host, port) = parse_address("[::1]:25565");
        assert_eq!(host, "::1");
        assert_eq!(port, Some(25565));
    }

    #[test]
    fn test_parse_address_ipv6_no_port() {
        let (host, port) = parse_address("[::1]");
        assert_eq!(host, "::1");
        assert_eq!(port, None);
    }

    #[test]
    fn test_parse_address_raw_ipv6() {
        // Raw IPv6 without brackets — multiple colons, no port parsing.
        let (host, port) = parse_address("2001:db8::1");
        assert_eq!(host, "2001:db8::1");
        assert_eq!(port, None);
    }

    #[test]
    fn test_normalize_edition() {
        assert_eq!(normalize_edition(None), Edition::Java);
        assert_eq!(normalize_edition(Some("java")), Edition::Java);
        assert_eq!(normalize_edition(Some("Java")), Edition::Java);
        assert_eq!(normalize_edition(Some("bedrock")), Edition::Bedrock);
        assert_eq!(normalize_edition(Some("Bedrock")), Edition::Bedrock);
        assert_eq!(normalize_edition(Some("java_bedrock")), Edition::Java);
    }

    #[test]
    fn test_is_plugin_managed() {
        let not_verified = serde_json::json!({"is_verified": false});
        assert!(!is_plugin_managed(&not_verified));

        let verified_no_source = serde_json::json!({"is_verified": true});
        assert!(!is_plugin_managed(&verified_no_source));

        let verified_plugin = serde_json::json!({
            "is_verified": true,
            "verification_source": "plugin"
        });
        assert!(is_plugin_managed(&verified_plugin));

        let verified_other = serde_json::json!({
            "is_verified": true,
            "verification_source": "manual"
        });
        assert!(!is_plugin_managed(&verified_other));
    }

    #[test]
    fn test_is_internal_only_host() {
        assert!(is_internal_only_host("internal-server.local"));
        assert!(is_internal_only_host("INTERNAL-server.local"));
        assert!(!is_internal_only_host("mc.example.com"));
        assert!(!is_internal_only_host("192.168.1.1"));
    }

    #[test]
    fn test_resolve_state_ports_ignores_default_java_for_hostname() {
        let server = serde_json::json!({"ping_port": 25565});
        let (port, _) = resolve_state_ports(&server, "mc.example.com", None, Edition::Java);
        // Should ignore 25565 for hostname to honor SRV.
        assert_eq!(port, None);
    }

    #[test]
    fn test_resolve_state_ports_keeps_default_java_for_ip() {
        let server = serde_json::json!({"ping_port": 25565});
        let (port, _) = resolve_state_ports(&server, "192.168.1.1", None, Edition::Java);
        assert_eq!(port, Some(25565));
    }

    #[test]
    fn test_resolve_state_ports_custom_port() {
        let server = serde_json::json!({"ping_port": 25566});
        let (port, _) = resolve_state_ports(&server, "mc.example.com", None, Edition::Java);
        assert_eq!(port, Some(25566));
    }

    #[test]
    fn test_resolve_state_ports_bedrock_fallback() {
        let server = serde_json::json!({"bedrock_port": 19133});
        let (port, bedrock_port) =
            resolve_state_ports(&server, "mc.example.com", None, Edition::Bedrock);
        assert_eq!(port, Some(19133));
        assert_eq!(bedrock_port, Some(19133));
    }

    #[test]
    fn test_wants_bedrock_probe() {
        let java = serde_json::json!({"game_edition": "java"});
        assert!(wants_bedrock_probe(&java, 3600));

        let bedrock = serde_json::json!({"game_edition": "bedrock"});
        assert!(!wants_bedrock_probe(&bedrock, 3600));

        // Disabled interval.
        assert!(!wants_bedrock_probe(&java, 0));
    }

    #[test]
    fn test_build_state() {
        let server = serde_json::json!({
            "id": "12345",
            "ip_or_domain": "mc.example.com:25566",
            "game_edition": "java",
        });
        let state = build_state(server, 100.0, 60, 3600);
        assert_eq!(state.server_id, "12345");
        assert_eq!(state.host, "mc.example.com");
        assert_eq!(state.port, Some(25566));
        assert_eq!(state.edition, Edition::Java);
        assert!(!state.has_emitted_state);
        assert!(state.next_due >= 100.0);
        assert!(state.next_due <= 160.0);
    }
}
