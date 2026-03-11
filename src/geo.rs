//! Geolocation lookup with TTL-based caching.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use reqwest::Client;
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::debug;

use crate::config::GeoSettings;
use crate::metrics::GEO_CACHE_SIZE;
use crate::models::Edition;

#[derive(Debug, Clone)]
struct CacheEntry {
    country: String,
    country_code: String,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct DnsEntry {
    ip: String,
    expires_at: Instant,
}

#[derive(Deserialize)]
struct GeoResponse {
    #[serde(default)]
    status: String,
    #[serde(default)]
    country: String,
    #[serde(rename = "countryCode", default)]
    country_code: String,
}

pub struct GeoCache {
    geo_entries: RwLock<HashMap<String, CacheEntry>>,
    dns_entries: RwLock<HashMap<String, DnsEntry>>,
    settings: GeoSettings,
    lookup_timeout_seconds: f64,
}

impl GeoCache {
    pub fn new(settings: GeoSettings, lookup_timeout_seconds: f64) -> Arc<Self> {
        Arc::new(Self {
            geo_entries: RwLock::new(HashMap::new()),
            dns_entries: RwLock::new(HashMap::new()),
            settings,
            lookup_timeout_seconds,
        })
    }

    /// Resolve hostname to IP with caching.
    async fn resolve_ip(&self, host: &str, port: u16) -> Option<String> {
        let cache_key = format!("{host}:{port}");

        // Check cache.
        {
            let cache = self.dns_entries.read().await;
            if let Some(entry) = cache.get(&cache_key) {
                if Instant::now() < entry.expires_at {
                    return if entry.ip.is_empty() {
                        None
                    } else {
                        Some(entry.ip.clone())
                    };
                }
            }
        }

        // Resolve via tokio DNS.
        let result = tokio::net::lookup_host(format!("{host}:{port}")).await;
        let ip = match result {
            Ok(mut addrs) => addrs.next().map(|a| a.ip().to_string()).unwrap_or_default(),
            Err(e) => {
                debug!("DNS lookup failed for {host}:{port}: {e}");
                String::new()
            }
        };

        let ttl = if ip.is_empty() {
            self.settings.dns_retry_seconds
        } else {
            self.settings.dns_ttl_seconds
        };

        let mut cache = self.dns_entries.write().await;
        // Evict expired entries if cache is too large.
        if cache.len() >= self.settings.dns_cache_max_entries {
            let now = Instant::now();
            cache.retain(|_, v| v.expires_at > now);
        }
        cache.insert(
            cache_key,
            DnsEntry {
                ip: ip.clone(),
                expires_at: Instant::now() + std::time::Duration::from_secs(ttl),
            },
        );

        if ip.is_empty() { None } else { Some(ip) }
    }

    /// Fetch geolocation for a host, with caching.
    pub async fn fetch_geo(
        &self,
        http: &Client,
        host: &str,
        port: Option<u16>,
        edition: Edition,
    ) -> (String, String) {
        let cache_key = format!("{host}:{}", port.unwrap_or(0));

        // Check cache.
        {
            let cache = self.geo_entries.read().await;
            if let Some(entry) = cache.get(&cache_key) {
                if Instant::now() < entry.expires_at {
                    return (entry.country.clone(), entry.country_code.clone());
                }
            }
        }

        let result = self
            .fetch_geo_uncached(http, host, port, edition)
            .await;

        let is_empty = result.0.is_empty() && result.1.is_empty();
        let ttl = if is_empty {
            self.settings.retry_seconds
        } else {
            self.settings.ttl_seconds
        };

        let mut cache = self.geo_entries.write().await;
        cache.insert(
            cache_key,
            CacheEntry {
                country: result.0.clone(),
                country_code: result.1.clone(),
                expires_at: Instant::now() + std::time::Duration::from_secs(ttl),
            },
        );

        result
    }

    async fn fetch_geo_uncached(
        &self,
        http: &Client,
        host: &str,
        port: Option<u16>,
        edition: Edition,
    ) -> (String, String) {
        let effective_port = port.unwrap_or(edition.default_port());
        let resolved_ip = match self.resolve_ip(host, effective_port).await {
            Some(ip) => ip,
            None => return (String::new(), String::new()),
        };

        let url = format!(
            "http://ip-api.com/json/{resolved_ip}?fields=status,country,countryCode"
        );

        let timeout = std::time::Duration::from_secs_f64(self.lookup_timeout_seconds);
        let resp = match http.get(&url).timeout(timeout).send().await {
            Ok(r) => r,
            Err(e) => {
                debug!("Geo lookup failed for {host}: {e}");
                return (String::new(), String::new());
            }
        };

        if !resp.status().is_success() {
            return (String::new(), String::new());
        }

        match resp.json::<GeoResponse>().await {
            Ok(data) if data.status == "success" => (data.country, data.country_code),
            _ => (String::new(), String::new()),
        }
    }

    /// Update the Prometheus geo cache size gauge.
    pub async fn update_metrics(&self) {
        let size = self.geo_entries.read().await.len();
        GEO_CACHE_SIZE.set(size as i64);
    }
}
