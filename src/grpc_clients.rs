//! tonic-generated gRPC client stubs for the internal services the poller
//! calls (server-service catalog + internal_servers, monitoring internal).
//! Replaces the legacy reqwest HTTP calls.

pub mod server {
    pub mod v1 {
        tonic::include_proto!("leavepulse.server.v1");
    }
}

pub mod monitoring {
    pub mod v1 {
        tonic::include_proto!("leavepulse.monitoring.v1");
    }
}

use serde_json::{Value, json};
use service_toolkit_rust::grpc::InternalTokenInterceptor;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;

use self::monitoring::v1::monitoring_internal_service_client::MonitoringInternalServiceClient;
use self::server::v1::catalog_service_client::CatalogServiceClient;
use self::server::v1::internal_servers_service_client::InternalServersServiceClient;

/// Channel wrapped with the internal-token interceptor used by all poller clients.
pub type AuthChannel = InterceptedService<Channel, InternalTokenInterceptor>;

/// Build an authenticated channel to `target` (e.g. `http://10.200.0.101:50500`).
pub fn auth_channel(target: &str, token: &str) -> Result<AuthChannel, tonic::transport::Error> {
    let channel = service_toolkit_rust::grpc::build_channel(target)?;
    let interceptor = InternalTokenInterceptor::new(token)
        .expect("internal token must be set for poller gRPC clients");
    Ok(InterceptedService::new(channel, interceptor))
}

pub fn catalog_client(channel: AuthChannel) -> CatalogServiceClient<AuthChannel> {
    CatalogServiceClient::new(channel)
}

pub fn internal_servers_client(channel: AuthChannel) -> InternalServersServiceClient<AuthChannel> {
    InternalServersServiceClient::new(channel)
}

pub fn monitoring_client(channel: AuthChannel) -> MonitoringInternalServiceClient<AuthChannel> {
    MonitoringInternalServiceClient::new(channel)
}

/// Convert a poller-built ingest JSON row into the IngestPoint proto.
/// The open-ended `extra` object is carried as a JSON string in `extra_json`.
pub fn value_to_ingest_point(row: &Value) -> self::monitoring::v1::IngestPoint {
    let s = |k: &str| row.get(k).and_then(|v| v.as_str()).map(|v| v.to_string());
    let i32opt = |k: &str| row.get(k).and_then(|v| v.as_i64()).map(|v| v as i32);
    let extra_json = row
        .get("extra")
        .filter(|v| !v.is_null())
        .map(|v| v.to_string());
    self::monitoring::v1::IngestPoint {
        server_id: row.get("server_id").and_then(value_as_i64).unwrap_or(0),
        collected_at: s("collected_at"),
        online: i32opt("online"),
        max_players: i32opt("max_players"),
        version: s("version"),
        motd: s("motd"),
        favicon: s("favicon"),
        country: s("country"),
        country_code: s("country_code"),
        event_type: s("event_type"),
        player_id: s("player_id"),
        player_name: s("player_name"),
        online_delta: i32opt("online_delta"),
        extra_json,
    }
}

/// server_id may arrive as a JSON number or a string.
fn value_as_i64(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    v.as_str().and_then(|s| s.parse().ok())
}

/// Convert a catalog item proto into the JSON shape the legacy HTTP catalog
/// returned, so downstream parsing in `models.rs` stays unchanged.
pub fn catalog_item_to_value(item: &self::server::v1::ServerCatalogItem) -> Value {
    json!({
        "id": item.id,
        "project_id": item.project_id,
        "project_online_strategy": item.project_online_strategy,
        "ip_or_domain": item.ip_or_domain,
        "server_role": item.server_role,
        "proxy_type": item.proxy_type,
        "parent_id": item.parent_id,
        "ping_ip_or_domain": item.ping_ip_or_domain,
        "ping_port": item.ping_port,
        "bedrock_port": item.bedrock_port,
        "game_edition": item.game_edition,
        "is_verified": item.is_verified,
        "verification_source": item.verification_source,
        "verified_plugin_last_seen_at": item.verified_plugin_last_seen_at,
    })
}
