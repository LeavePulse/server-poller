//! Minecraft Bedrock Edition server ping via RakNet Unconnected Ping/Pong.
//!
//! Protocol reference: <https://wiki.bedrock.dev/servers/raknet>
//!
//! Unconnected Ping (0x01):
//!   [u8 packet_id=0x01] [i64 client_time] [16 bytes magic] [i64 client_guid]
//!
//! Unconnected Pong (0x1c):
//!   [u8 packet_id=0x1c] [i64 client_time] [i64 server_guid] [16 bytes magic]
//!   [u16 string_length] [string motd_payload]
//!
//! The MOTD payload is semicolon-delimited:
//!   Edition;MOTD1;Protocol;Version;Online;MaxPlayers;ServerUID;MOTD2;Gamemode;GamemodeNum;PortV4;PortV6

use std::time::Duration;

use rand::Rng;
use tokio::net::UdpSocket;
use tracing::debug;

/// RakNet offline message magic bytes.
const RAKNET_MAGIC: [u8; 16] = [
    0x00, 0xff, 0xff, 0x00, 0xfe, 0xfe, 0xfe, 0xfe, 0xfd, 0xfd, 0xfd, 0xfd, 0x12, 0x34, 0x56, 0x78,
];

const UNCONNECTED_PING_ID: u8 = 0x01;
const UNCONNECTED_PONG_ID: u8 = 0x1c;

/// Parsed Bedrock server status from Unconnected Pong.
#[derive(Debug, Clone)]
pub struct BedrockStatus {
    pub edition: String,
    pub motd: String,
    pub version: String,
    pub online_players: Option<i64>,
    pub max_players: Option<i64>,
    pub motd_line2: String,
    pub gamemode: String,
}

/// Build the Unconnected Ping packet.
fn build_ping_packet() -> Vec<u8> {
    let mut packet = Vec::with_capacity(1 + 8 + 16 + 8);
    packet.push(UNCONNECTED_PING_ID);

    // Client alive time (current millis).
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    packet.extend_from_slice(&time.to_be_bytes());

    // Magic.
    packet.extend_from_slice(&RAKNET_MAGIC);

    // Client GUID (random).
    let guid: i64 = rand::thread_rng().r#gen();
    packet.extend_from_slice(&guid.to_be_bytes());

    packet
}

/// Parse the Unconnected Pong packet into BedrockStatus.
fn parse_pong(data: &[u8]) -> Option<BedrockStatus> {
    // Minimum: 1 (id) + 8 (time) + 8 (guid) + 16 (magic) + 2 (string_len) = 35 bytes
    if data.len() < 35 {
        return None;
    }
    if data[0] != UNCONNECTED_PONG_ID {
        return None;
    }

    // Skip: id(1) + time(8) + guid(8) + magic(16) = 33 bytes.
    let string_len = u16::from_be_bytes([data[33], data[34]]) as usize;
    if data.len() < 35 + string_len {
        return None;
    }

    let motd_payload = String::from_utf8_lossy(&data[35..35 + string_len]);
    let parts: Vec<&str> = motd_payload.split(';').collect();

    // Need at least 6 fields for basic info.
    if parts.len() < 6 {
        return None;
    }

    let online = parts[4].parse::<i64>().ok();
    let max = parts[5].parse::<i64>().ok();

    Some(BedrockStatus {
        edition: parts[0].to_string(),
        motd: parts[1].to_string(),
        version: parts[3].to_string(),
        online_players: online,
        max_players: max,
        motd_line2: parts.get(7).unwrap_or(&"").to_string(),
        gamemode: parts.get(8).unwrap_or(&"").to_string(),
    })
}

/// Ping a Minecraft Bedrock server using RakNet Unconnected Ping.
///
/// Returns `Some(BedrockStatus)` on success, `None` on failure/timeout.
pub async fn ping_bedrock(host: &str, port: u16, timeout: Duration) -> Option<BedrockStatus> {
    let result = tokio::time::timeout(timeout, async {
        // Bind to any available local port.
        let socket = UdpSocket::bind("0.0.0.0:0").await.ok()?;
        socket.connect(format!("{host}:{port}")).await.ok()?;

        let packet = build_ping_packet();
        socket.send(&packet).await.ok()?;

        let mut buf = [0u8; 2048];
        let n = socket.recv(&mut buf).await.ok()?;

        parse_pong(&buf[..n])
    })
    .await;

    match result {
        Ok(status) => status,
        Err(_) => {
            debug!("Bedrock ping timed out for {host}:{port}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_ping_packet() {
        let packet = build_ping_packet();
        assert_eq!(packet.len(), 1 + 8 + 16 + 8); // 33 bytes
        assert_eq!(packet[0], UNCONNECTED_PING_ID);
        assert_eq!(&packet[9..25], &RAKNET_MAGIC);
    }

    #[test]
    fn test_parse_pong_valid() {
        // Build a synthetic pong packet.
        let mut pkt = Vec::new();
        pkt.push(UNCONNECTED_PONG_ID);
        pkt.extend_from_slice(&0i64.to_be_bytes()); // time
        pkt.extend_from_slice(&1i64.to_be_bytes()); // guid
        pkt.extend_from_slice(&RAKNET_MAGIC);

        let motd = b"MCPE;My Server;527;1.19.1;5;20;12345;Second Line;Survival;1;19132;19133;";
        pkt.extend_from_slice(&(motd.len() as u16).to_be_bytes());
        pkt.extend_from_slice(motd);

        let status = parse_pong(&pkt).unwrap();
        assert_eq!(status.edition, "MCPE");
        assert_eq!(status.motd, "My Server");
        assert_eq!(status.version, "1.19.1");
        assert_eq!(status.online_players, Some(5));
        assert_eq!(status.max_players, Some(20));
        assert_eq!(status.motd_line2, "Second Line");
        assert_eq!(status.gamemode, "Survival");
    }

    #[test]
    fn test_parse_pong_too_short() {
        let pkt = vec![0x1c, 0, 0, 0];
        assert!(parse_pong(&pkt).is_none());
    }

    #[test]
    fn test_parse_pong_wrong_id() {
        let mut pkt = vec![0x00; 35]; // Wrong packet ID.
        pkt[0] = 0xFF;
        assert!(parse_pong(&pkt).is_none());
    }

    #[test]
    fn test_parse_pong_too_few_fields() {
        let mut pkt = Vec::new();
        pkt.push(UNCONNECTED_PONG_ID);
        pkt.extend_from_slice(&0i64.to_be_bytes());
        pkt.extend_from_slice(&1i64.to_be_bytes());
        pkt.extend_from_slice(&RAKNET_MAGIC);

        let motd = b"MCPE;Server;527"; // Only 3 fields, need 6.
        pkt.extend_from_slice(&(motd.len() as u16).to_be_bytes());
        pkt.extend_from_slice(motd);

        assert!(parse_pong(&pkt).is_none());
    }
}
