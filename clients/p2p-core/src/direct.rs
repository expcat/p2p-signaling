use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use quinn::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use quinn::rustls::{self, CertificateError, DigitallySignedStruct, SignatureScheme};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::time::{timeout, Instant};

use crate::nat::{Candidate, CandidateKind, PreparedConnectInfo};
use crate::signaling::SignalingRole;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PUNCH_INTERVAL: Duration = Duration::from_millis(200);
const PUNCH_BYTES: &[u8] = b"p2p-signaling-hole-punch-v1";
const CONTROL_LIMIT: usize = 4096;
const SERVER_NAME: &str = "p2p.local";

#[derive(Debug, Clone)]
pub struct DirectLinkInfo {
    pub remote_addr: SocketAddr,
    pub local_role: SignalingRole,
}

pub struct DirectLink {
    connection: quinn::Connection,
    endpoint: quinn::Endpoint,
    info: DirectLinkInfo,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum DirectControl {
    Hello {
        #[serde(rename = "pairingToken")]
        pairing_token: String,
    },
}

impl DirectLink {
    pub fn info(&self) -> &DirectLinkInfo {
        &self.info
    }

    pub async fn closed_reason(self) -> String {
        let reason = self.connection.closed().await;
        self.endpoint.wait_idle().await;
        reason.to_string()
    }
}

pub async fn establish_direct_link(
    local: PreparedConnectInfo,
    peer: crate::nat::ConnectInfo,
) -> Result<DirectLink> {
    if peer.role == local.info.role {
        anyhow::bail!("对端角色与本端相同，无法建立直连");
    }

    match local.info.role {
        SignalingRole::Host => accept_direct_link(local, peer).await,
        SignalingRole::Guest => dial_direct_link(local, peer).await,
    }
}

async fn accept_direct_link(
    local: PreparedConnectInfo,
    peer: crate::nat::ConnectInfo,
) -> Result<DirectLink> {
    let punch_socket = local.socket.try_clone()?;
    let endpoint = endpoint(local.socket, Some(server_config(local.certificate)?))?;
    let punch_task = start_punch_loop(punch_socket, peer.candidates.clone())?;

    let accepted = timeout(CONNECT_TIMEOUT, async {
        let incoming = endpoint.accept().await.context("未收到 QUIC 连接")?;
        let accepted = incoming.await.context("QUIC 握手失败")?;
        verify_host_control(&accepted, &local.info.pairing_token, &peer.pairing_token).await?;
        Ok::<_, anyhow::Error>(accepted)
    })
    .await;
    punch_task.abort();
    let accepted = accepted.context("直连建立超时")??;
    let info = DirectLinkInfo {
        remote_addr: accepted.remote_address(),
        local_role: SignalingRole::Host,
    };

    Ok(DirectLink {
        connection: accepted,
        endpoint,
        info,
    })
}

async fn dial_direct_link(
    local: PreparedConnectInfo,
    peer: crate::nat::ConnectInfo,
) -> Result<DirectLink> {
    let candidates = ordered_compatible_candidates(&peer.candidates, local.socket.local_addr()?);
    if candidates.is_empty() {
        anyhow::bail!("对端没有可用的同族 UDP 候选地址");
    }

    let punch_socket = local.socket.try_clone()?;
    let mut endpoint = endpoint(local.socket, None)?;
    endpoint.set_default_client_config(client_config(peer.cert_hash.clone())?);
    let punch_task = start_punch_loop(punch_socket, peer.candidates.clone())?;

    let deadline = Instant::now() + CONNECT_TIMEOUT;
    let mut last_error = None;
    for candidate in candidates {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };

        let attempt = async {
            let connecting = endpoint
                .connect(candidate.addr, SERVER_NAME)
                .with_context(|| format!("无法拨号 {}", candidate.addr))?;
            let connection = connecting
                .await
                .with_context(|| format!("QUIC 握手失败 {}", candidate.addr))?;
            verify_guest_control(&connection, &local.info.pairing_token, &peer.pairing_token)
                .await?;
            Ok::<_, anyhow::Error>(connection)
        };

        match timeout(remaining, attempt).await {
            Ok(Ok(connection)) => {
                punch_task.abort();
                let info = DirectLinkInfo {
                    remote_addr: connection.remote_address(),
                    local_role: SignalingRole::Guest,
                };
                return Ok(DirectLink {
                    connection,
                    endpoint,
                    info,
                });
            }
            Ok(Err(error)) => last_error = Some(error),
            Err(error) => last_error = Some(error.into()),
        }
    }

    punch_task.abort();
    match last_error {
        Some(error) => Err(error).context("直连建立失败"),
        None => anyhow::bail!("直连建立超时"),
    }
}

fn endpoint(
    socket: std::net::UdpSocket,
    server_config: Option<quinn::ServerConfig>,
) -> Result<quinn::Endpoint> {
    let runtime = quinn::default_runtime().context("未找到 Quinn Tokio runtime")?;
    quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        server_config,
        socket,
        runtime,
    )
    .context("创建 QUIC endpoint 失败")
}

fn server_config(certificate: crate::nat::DirectCertificate) -> Result<quinn::ServerConfig> {
    let mut config =
        quinn::ServerConfig::with_single_cert(vec![certificate.cert_der], certificate.key_der)
            .context("创建 QUIC server config 失败")?;
    config.transport_config(Arc::new(transport_config()?));
    Ok(config)
}

fn client_config(expected_cert_hash: String) -> Result<quinn::ClientConfig> {
    let verifier = Arc::new(CertHashVerifier::new(expected_cert_hash));
    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    let mut config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls)?));
    config.transport_config(Arc::new(transport_config()?));
    Ok(config)
}

fn transport_config() -> Result<quinn::TransportConfig> {
    let mut config = quinn::TransportConfig::default();
    config.keep_alive_interval(Some(Duration::from_secs(10)));
    config.max_idle_timeout(Some(Duration::from_secs(30).try_into()?));
    Ok(config)
}

async fn verify_guest_control(
    connection: &quinn::Connection,
    local_token: &str,
    peer_token: &str,
) -> Result<()> {
    let (mut send, mut recv) = connection.open_bi().await?;
    write_hello(&mut send, local_token).await?;
    let hello = read_hello(&mut recv).await?;
    ensure_token(&hello, peer_token)
}

async fn verify_host_control(
    connection: &quinn::Connection,
    local_token: &str,
    peer_token: &str,
) -> Result<()> {
    let (mut send, mut recv) = connection.accept_bi().await?;
    let hello = read_hello(&mut recv).await?;
    ensure_token(&hello, peer_token)?;
    write_hello(&mut send, local_token).await
}

async fn write_hello(send: &mut quinn::SendStream, token: &str) -> Result<()> {
    let payload = serde_json::to_vec(&DirectControl::Hello {
        pairing_token: token.to_string(),
    })?;
    send.write_all(&payload).await?;
    send.finish()?;
    Ok(())
}

async fn read_hello(recv: &mut quinn::RecvStream) -> Result<DirectControl> {
    let bytes = recv.read_to_end(CONTROL_LIMIT).await?;
    serde_json::from_slice(&bytes).context("直连 Hello 帧无效")
}

fn ensure_token(hello: &DirectControl, expected: &str) -> Result<()> {
    match hello {
        DirectControl::Hello { pairing_token } if pairing_token == expected => Ok(()),
        DirectControl::Hello { .. } => anyhow::bail!("直连配对令牌不匹配"),
    }
}

fn ordered_compatible_candidates(
    candidates: &[Candidate],
    local_addr: SocketAddr,
) -> Vec<Candidate> {
    let mut candidates = candidates
        .iter()
        .filter(|candidate| candidate.addr.is_ipv4() == local_addr.is_ipv4())
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort_by_key(|candidate| match candidate.kind {
        CandidateKind::Local => 0,
        CandidateKind::ServerReflexive => 1,
    });
    candidates
}

fn start_punch_loop(
    socket: std::net::UdpSocket,
    candidates: Vec<Candidate>,
) -> Result<tokio::task::JoinHandle<()>> {
    let socket = tokio::net::UdpSocket::from_std(socket)?;
    Ok(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(PUNCH_INTERVAL);
        loop {
            ticker.tick().await;
            for candidate in &candidates {
                let _ = socket.send_to(PUNCH_BYTES, candidate.addr).await;
            }
        }
    }))
}

#[derive(Debug)]
struct CertHashVerifier {
    expected_hash: String,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl CertHashVerifier {
    fn new(expected_hash: String) -> Self {
        Self {
            expected_hash,
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        }
    }
}

impl ServerCertVerifier for CertHashVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let actual = hex_lower(&Sha256::digest(end_entity.as_ref()));
        if actual == self.expected_hash {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
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
