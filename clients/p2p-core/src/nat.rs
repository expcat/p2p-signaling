use std::collections::{HashMap, HashSet};
use std::env;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine;
use if_addrs::{get_if_addrs, IfAddr};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::net::{lookup_host, UdpSocket};
use tokio::time::{timeout, Instant};
use uuid::Uuid;

use crate::signaling::SignalingRole;

const STUN_BINDING_REQUEST: u16 = 0x0001;
const STUN_BINDING_SUCCESS: u16 = 0x0101;
const STUN_ATTR_MAPPED_ADDRESS: u16 = 0x0001;
const STUN_ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;
const STUN_MAGIC_COOKIE: u32 = 0x2112_A442;
const STUN_QUERY_TIMEOUT: Duration = Duration::from_millis(1400);
const MAX_STUN_SERVERS: usize = 3;

const DEFAULT_STUN_SERVERS: &[&str] = &[
    "stun.miwifi.com:3478",
    "stun.l.google.com:19302",
    "stun.cloudflare.com:3478",
];

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum CandidateKind {
    Local,
    ServerReflexive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Candidate {
    pub addr: SocketAddr,
    pub kind: CandidateKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectInfo {
    pub kind: ConnectInfoKind,
    pub version: u8,
    pub role: SignalingRole,
    pub candidates: Vec<Candidate>,
    #[serde(rename = "certHash")]
    pub cert_hash: String,
    #[serde(rename = "pairingToken")]
    pub pairing_token: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ConnectInfoKind {
    ConnectInfo,
}

impl ConnectInfo {
    pub fn is_supported(&self) -> bool {
        self.kind == ConnectInfoKind::ConnectInfo && self.version == 1
    }
}

pub async fn collect_connect_info(role: SignalingRole) -> Result<ConnectInfo> {
    let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
        .await
        .context("failed to bind UDP socket for candidate collection")?;
    let local_port = socket.local_addr()?.port();

    let mut candidates = local_candidates(local_port)?;
    candidates.extend(server_reflexive_candidates(&socket).await);
    dedupe_candidates(&mut candidates);

    Ok(ConnectInfo {
        kind: ConnectInfoKind::ConnectInfo,
        version: 1,
        role,
        candidates,
        cert_hash: provisional_cert_hash(),
        pairing_token: pairing_token(),
    })
}

fn local_candidates(port: u16) -> Result<Vec<Candidate>> {
    let mut candidates = Vec::new();
    for iface in get_if_addrs().context("failed to enumerate local network interfaces")? {
        if iface.is_loopback() {
            continue;
        }

        let ip = match iface.addr {
            IfAddr::V4(addr) => IpAddr::V4(addr.ip),
            IfAddr::V6(addr) if is_useful_ipv6(addr.ip) => IpAddr::V6(addr.ip),
            IfAddr::V6(_) => continue,
        };

        candidates.push(Candidate {
            addr: SocketAddr::new(ip, port),
            kind: CandidateKind::Local,
        });
    }
    Ok(candidates)
}

async fn server_reflexive_candidates(socket: &UdpSocket) -> Vec<Candidate> {
    let servers = stun_servers();
    if servers.is_empty() {
        return Vec::new();
    }

    let mut transactions = HashMap::new();
    for server in servers.into_iter().take(MAX_STUN_SERVERS) {
        let Ok(mut resolved) = lookup_host(&server).await else {
            continue;
        };
        let Some(server_addr) = resolved.find(|addr| addr.is_ipv4()) else {
            continue;
        };

        let transaction_id = transaction_id();
        let request = stun_binding_request(&transaction_id);
        if socket.send_to(&request, server_addr).await.is_ok() {
            transactions.insert(transaction_id, server_addr);
        }
    }

    let deadline = Instant::now() + STUN_QUERY_TIMEOUT;
    let mut buf = [0_u8; 1024];
    let mut candidates = Vec::new();

    while !transactions.is_empty() {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };

        let Ok(Ok((len, _from))) = timeout(remaining, socket.recv_from(&mut buf)).await else {
            break;
        };

        if let Some((transaction_id, addr)) = parse_stun_binding_response(&buf[..len]) {
            if transactions.remove(&transaction_id).is_some() {
                candidates.push(Candidate {
                    addr,
                    kind: CandidateKind::ServerReflexive,
                });
            }
        }
    }

    candidates
}

fn stun_servers() -> Vec<String> {
    env::var("P2P_STUN_SERVERS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_else(|| {
            DEFAULT_STUN_SERVERS
                .iter()
                .map(|value| value.to_string())
                .collect()
        })
}

fn dedupe_candidates(candidates: &mut Vec<Candidate>) {
    let mut seen = HashSet::new();
    candidates.retain(|candidate| seen.insert(candidate.clone()));
}

fn is_useful_ipv6(ip: Ipv6Addr) -> bool {
    !(ip.is_loopback() || ip.is_unspecified() || ip.is_unique_local() || ip.is_unicast_link_local())
}

fn transaction_id() -> [u8; 12] {
    let uuid = Uuid::new_v4();
    let mut id = [0_u8; 12];
    id.copy_from_slice(&uuid.as_bytes()[..12]);
    id
}

fn stun_binding_request(transaction_id: &[u8; 12]) -> [u8; 20] {
    let mut request = [0_u8; 20];
    request[0..2].copy_from_slice(&STUN_BINDING_REQUEST.to_be_bytes());
    request[4..8].copy_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
    request[8..20].copy_from_slice(transaction_id);
    request
}

fn parse_stun_binding_response(packet: &[u8]) -> Option<([u8; 12], SocketAddr)> {
    if packet.len() < 20 {
        return None;
    }

    let message_type = u16::from_be_bytes([packet[0], packet[1]]);
    let message_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
    let magic = u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]);
    if message_type != STUN_BINDING_SUCCESS || magic != STUN_MAGIC_COOKIE {
        return None;
    }

    let mut transaction_id = [0_u8; 12];
    transaction_id.copy_from_slice(&packet[8..20]);

    let end = 20 + message_len;
    if end > packet.len() {
        return None;
    }

    let mut offset = 20;
    let mut mapped = None;
    while offset + 4 <= end {
        let attr_type = u16::from_be_bytes([packet[offset], packet[offset + 1]]);
        let attr_len = u16::from_be_bytes([packet[offset + 2], packet[offset + 3]]) as usize;
        offset += 4;

        if offset + attr_len > end {
            return None;
        }

        let value = &packet[offset..offset + attr_len];
        match attr_type {
            STUN_ATTR_XOR_MAPPED_ADDRESS => {
                if let Some(addr) = parse_xor_mapped_address(value, &transaction_id) {
                    return Some((transaction_id, addr));
                }
            }
            STUN_ATTR_MAPPED_ADDRESS => {
                mapped = parse_mapped_address(value);
            }
            _ => {}
        }

        offset += (attr_len + 3) & !3;
    }

    mapped.map(|addr| (transaction_id, addr))
}

fn parse_mapped_address(value: &[u8]) -> Option<SocketAddr> {
    if value.len() < 8 || value[0] != 0 {
        return None;
    }

    let port = u16::from_be_bytes([value[2], value[3]]);
    match value[1] {
        0x01 => Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(value[4], value[5], value[6], value[7])),
            port,
        )),
        0x02 if value.len() >= 20 => {
            let mut octets = [0_u8; 16];
            octets.copy_from_slice(&value[4..20]);
            Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
        }
        _ => None,
    }
}

fn parse_xor_mapped_address(value: &[u8], transaction_id: &[u8; 12]) -> Option<SocketAddr> {
    if value.len() < 8 || value[0] != 0 {
        return None;
    }

    let port = u16::from_be_bytes([value[2], value[3]]) ^ ((STUN_MAGIC_COOKIE >> 16) as u16);
    match value[1] {
        0x01 => {
            let cookie = STUN_MAGIC_COOKIE.to_be_bytes();
            let ip = Ipv4Addr::new(
                value[4] ^ cookie[0],
                value[5] ^ cookie[1],
                value[6] ^ cookie[2],
                value[7] ^ cookie[3],
            );
            Some(SocketAddr::new(IpAddr::V4(ip), port))
        }
        0x02 if value.len() >= 20 => {
            let mut mask = [0_u8; 16];
            mask[..4].copy_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
            mask[4..].copy_from_slice(transaction_id);

            let mut octets = [0_u8; 16];
            for (index, octet) in octets.iter_mut().enumerate() {
                *octet = value[4 + index] ^ mask[index];
            }
            Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
        }
        _ => None,
    }
}

fn provisional_cert_hash() -> String {
    let mut hasher = Sha256::new();
    hasher.update(Uuid::new_v4().as_bytes());
    hasher.update(Uuid::new_v4().as_bytes());
    hex_lower(&hasher.finalize())
}

fn pairing_token() -> String {
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_xor_mapped_ipv4_response() {
        let transaction_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let addr = SocketAddr::from(([203, 0, 113, 7], 54321));
        let mut packet = Vec::new();
        packet.extend_from_slice(&STUN_BINDING_SUCCESS.to_be_bytes());
        packet.extend_from_slice(&12_u16.to_be_bytes());
        packet.extend_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
        packet.extend_from_slice(&transaction_id);
        packet.extend_from_slice(&STUN_ATTR_XOR_MAPPED_ADDRESS.to_be_bytes());
        packet.extend_from_slice(&8_u16.to_be_bytes());
        packet.push(0);
        packet.push(0x01);

        let xport = addr.port() ^ ((STUN_MAGIC_COOKIE >> 16) as u16);
        packet.extend_from_slice(&xport.to_be_bytes());
        let cookie = STUN_MAGIC_COOKIE.to_be_bytes();
        if let IpAddr::V4(ip) = addr.ip() {
            for (octet, mask) in ip.octets().iter().zip(cookie) {
                packet.push(octet ^ mask);
            }
        }

        let parsed = parse_stun_binding_response(&packet).unwrap();
        assert_eq!(parsed.0, transaction_id);
        assert_eq!(parsed.1, addr);
    }

    #[test]
    fn serializes_connect_info_wire_shape() {
        let info = ConnectInfo {
            kind: ConnectInfoKind::ConnectInfo,
            version: 1,
            role: SignalingRole::Host,
            candidates: vec![Candidate {
                addr: SocketAddr::from(([192, 168, 1, 5], 53000)),
                kind: CandidateKind::Local,
            }],
            cert_hash: "abcd".into(),
            pairing_token: "token".into(),
        };

        let value = serde_json::to_value(info).unwrap();
        assert_eq!(value["kind"], "connect-info");
        assert_eq!(value["role"], "host");
        assert_eq!(value["candidates"][0]["addr"], "192.168.1.5:53000");
        assert_eq!(value["candidates"][0]["kind"], "local");
        assert_eq!(value["certHash"], "abcd");
        assert_eq!(value["pairingToken"], "token");
    }
}
