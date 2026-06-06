use std::{
    collections::{HashMap, VecDeque},
    io::{Error, ErrorKind},
    net::SocketAddr,
    sync::Arc,
};

use data_encoding::BASE32_NOPAD;
use tokio::net::UdpSocket;

use crate::{
    agent::identity::AgentIdentity,
    crypto::transport::{decode_transport_frame, encode_transport_frame, SharedKey},
    protocol::{frame::Frame, message::Message},
    session::{
        handshake::{complete_session, hello_ack_frame, hello_frame},
        heartbeat::heartbeat_frame,
        peer::{PeerInfo, PeerSession, SessionState},
    },
    utils::url::ParsedUrl,
};

const QUERY_CHUNK_BYTES: usize = 30;
const RESPONSE_CHUNK_BYTES: usize = 100;

#[derive(Default)]
struct FragmentBuffer {
    total: u16,
    chunks: Vec<Option<Vec<u8>>>,
}

#[derive(Default)]
struct InboundState {
    assemblies: HashMap<u64, FragmentBuffer>,
    completed: VecDeque<Frame>,
}

#[derive(Clone)]
struct OutboundChunk {
    seq: u64,
    idx: u16,
    total: u16,
    chunk_b32: String,
}

#[derive(Clone)]
pub struct ActiveSimplexDnsPeer {
    pub session: PeerSession,
    socket: Arc<UdpSocket>,
    zone_labels: Vec<String>,
    remote_addr: Arc<tokio::sync::Mutex<Option<SocketAddr>>>,
    session_token: Arc<tokio::sync::Mutex<Option<String>>>,
    next_send_seq: Arc<tokio::sync::Mutex<u64>>,
    next_poll_seq: Arc<tokio::sync::Mutex<u64>>,
    inbound: Arc<tokio::sync::Mutex<InboundState>>,
    outbound: Arc<tokio::sync::Mutex<VecDeque<OutboundChunk>>>,
    shared_key: Option<SharedKey>,
    is_client: bool,
}

impl ActiveSimplexDnsPeer {
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        let payload = encode_transport_frame(frame, self.shared_key.as_ref())?;
        let seq = {
            let mut guard = self.next_send_seq.lock().await;
            let current = *guard;
            *guard = guard.saturating_add(1);
            current
        };
        if self.is_client {
            let remote_addr = self
                .remote_addr
                .lock()
                .await
                .ok_or_else(|| Error::new(ErrorKind::NotConnected, "dns peer missing remote addr"))?;
            let session_token = self
                .session_token
                .lock()
                .await
                .clone()
                .ok_or_else(|| Error::new(ErrorKind::NotConnected, "dns peer missing session token"))?;
            for (idx, chunk) in chunk_query_payload(&payload).into_iter().enumerate() {
                let query = encode_query(
                    rand::random(),
                    &self.zone_labels,
                    &session_token,
                    seq,
                    idx as u16,
                    payload_chunk_count(payload.len(), QUERY_CHUNK_BYTES),
                    false,
                    Some(&chunk),
                )?;
                self.socket.send_to(&query, remote_addr).await?;
                let response = recv_dns_packet(&self.socket, Some(remote_addr)).await?;
                self.process_response(&response).await?;
            }
            Ok(())
        } else {
            let mut queue = self.outbound.lock().await;
            for (idx, chunk_b32) in chunk_response_payload(&payload).into_iter().enumerate() {
                queue.push_back(OutboundChunk {
                    seq,
                    idx: idx as u16,
                    total: payload_chunk_count(payload.len(), RESPONSE_CHUNK_BYTES),
                    chunk_b32,
                });
            }
            Ok(())
        }
    }

    pub async fn read_frame(&self) -> Result<Frame, Error> {
        if let Some(frame) = self.pop_completed_frame().await {
            return Ok(frame);
        }

        if self.is_client {
            let remote_addr = self
                .remote_addr
                .lock()
                .await
                .ok_or_else(|| Error::new(ErrorKind::NotConnected, "dns peer missing remote addr"))?;
            let session_token = self
                .session_token
                .lock()
                .await
                .clone()
                .ok_or_else(|| Error::new(ErrorKind::NotConnected, "dns peer missing session token"))?;
            loop {
                let poll_seq = {
                    let mut guard = self.next_poll_seq.lock().await;
                    let current = *guard;
                    *guard = guard.saturating_add(1);
                    current
                };
                let query = encode_query(
                    rand::random(),
                    &self.zone_labels,
                    &session_token,
                    poll_seq,
                    0,
                    0,
                    true,
                    None,
                )?;
                self.socket.send_to(&query, remote_addr).await?;
                let response = recv_dns_packet(&self.socket, Some(remote_addr)).await?;
                self.process_response(&response).await?;
                if let Some(frame) = self.pop_completed_frame().await {
                    return Ok(frame);
                }
            }
        }

        loop {
            let (packet, remote_addr) = recv_dns_packet_from_any(&self.socket).await?;
            let query = decode_query(&packet, &self.zone_labels)?;
            {
                let mut guard = self.remote_addr.lock().await;
                if guard.is_none() {
                    *guard = Some(remote_addr);
                }
            }
            {
                let mut guard = self.session_token.lock().await;
                if guard.is_none() {
                    *guard = Some(query.session_token.clone());
                }
            }

            let maybe_frame = if let Some(chunk_b32) = query.chunk_b32.as_ref() {
                let chunk = BASE32_NOPAD
                    .decode(chunk_b32.to_ascii_uppercase().as_bytes())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
                self.process_inbound_fragment(query.seq, query.frag_idx, query.frag_total, chunk)
                    .await?
            } else {
                None
            };

            let response = self.build_response(query.id, query.qname_labels).await?;
            self.socket.send_to(&response, remote_addr).await?;
            if let Some(frame) = maybe_frame {
                let _ = self.pop_completed_frame().await;
                return Ok(frame);
            }
        }
    }

    async fn build_response(&self, id: u16, qname_labels: Vec<String>) -> Result<Vec<u8>, Error> {
        let answer = self.outbound.lock().await.pop_front();
        let txt = if let Some(chunk) = answer {
            format!(
                "fx|v1|data|{}|{}|{}|{}|{}",
                self.session_token
                    .lock()
                    .await
                    .clone()
                    .unwrap_or_else(|| "session".to_string()),
                chunk.seq,
                chunk.idx,
                chunk.total,
                chunk.chunk_b32
            )
        } else {
            "fx|v1|ack".to_string()
        };
        encode_response(id, &qname_labels, &txt)
    }

    async fn process_response(&self, packet: &[u8]) -> Result<(), Error> {
        let response = decode_response(packet)?;
        if response.kind != "data" {
            return Ok(());
        }
        let chunk = BASE32_NOPAD
            .decode(response.chunk_b32.as_bytes())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
        self.process_inbound_fragment(response.seq, response.frag_idx, response.frag_total, chunk)
            .await?;
        Ok(())
    }

    async fn process_inbound_fragment(
        &self,
        seq: u64,
        frag_idx: u16,
        frag_total: u16,
        chunk: Vec<u8>,
    ) -> Result<Option<Frame>, Error> {
        let mut inbound = self.inbound.lock().await;
        let entry = inbound.assemblies.entry(seq).or_insert_with(|| FragmentBuffer {
            total: frag_total,
            chunks: vec![None; frag_total.max(1) as usize],
        });
        if entry.total != frag_total {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "inconsistent dns fragment total",
            ));
        }
        let idx = frag_idx as usize;
        if idx >= entry.chunks.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "dns fragment index out of range",
            ));
        }
        entry.chunks[idx] = Some(chunk);
        if entry.chunks.iter().all(Option::is_some) {
            let mut payload = Vec::new();
            for chunk in entry.chunks.iter().flatten() {
                payload.extend_from_slice(chunk);
            }
            inbound.assemblies.remove(&seq);
            let frame = decode_transport_frame(&payload, self.shared_key.as_ref())?;
            inbound.completed.push_back(frame.clone());
            return Ok(Some(frame));
        }
        Ok(None)
    }

    async fn pop_completed_frame(&self) -> Option<Frame> {
        self.inbound.lock().await.completed.pop_front()
    }
}

pub async fn bind(endpoint: &str) -> Result<UdpSocket, Error> {
    let parsed = ParsedUrl::parse(endpoint)?;
    let host = parsed.host.as_deref().unwrap_or("0.0.0.0");
    let port = parsed.port.unwrap_or(0);
    UdpSocket::bind(format!("{host}:{port}")).await
}

pub async fn accept_peer_on(
    identity: AgentIdentity,
    socket: UdpSocket,
    path: &str,
) -> Result<ActiveSimplexDnsPeer, Error> {
    let zone_labels = parse_zone_labels(path);
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let placeholder = ActiveSimplexDnsPeer {
        session: PeerSession {
            local: PeerInfo {
                agent_id: identity.id.clone(),
                agent_name: identity.name.clone(),
                capabilities: identity.capability_labels(),
            },
            remote: PeerInfo {
                agent_id: String::new(),
                agent_name: String::new(),
                capabilities: vec!["transport:simplex-dns".into()],
            },
            state: SessionState::Active,
        },
        socket: Arc::new(socket),
        zone_labels,
        remote_addr: Arc::new(tokio::sync::Mutex::new(None)),
        session_token: Arc::new(tokio::sync::Mutex::new(None)),
        next_send_seq: Arc::new(tokio::sync::Mutex::new(0)),
        next_poll_seq: Arc::new(tokio::sync::Mutex::new(1_000_000)),
        inbound: Arc::new(tokio::sync::Mutex::new(InboundState::default())),
        outbound: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
        shared_key: shared_key.clone(),
        is_client: false,
    };

    let hello = placeholder.read_frame().await?;
    let session = complete_session(&identity, &hello)?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    let peer = ActiveSimplexDnsPeer {
        session,
        ..placeholder
    };
    peer.send_frame(&ack).await?;

    let heartbeat = peer.read_frame().await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }
    let heartbeat_ack = heartbeat_frame(
        identity.id.clone(),
        Some(peer.session.remote.agent_id.clone()),
    );
    peer.send_frame(&heartbeat_ack).await?;
    Ok(peer)
}

pub async fn connect_peer(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<ActiveSimplexDnsPeer, Error> {
    let parsed = ParsedUrl::parse(endpoint)?;
    let host = parsed.host.clone().ok_or_else(|| {
        Error::new(ErrorKind::InvalidInput, "missing host for simplex+dns connect")
    })?;
    let port = parsed.port.ok_or_else(|| {
        Error::new(ErrorKind::InvalidInput, "missing port for simplex+dns connect")
    })?;
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let peer = ActiveSimplexDnsPeer {
        session: PeerSession {
            local: PeerInfo {
                agent_id: identity.id.clone(),
                agent_name: identity.name.clone(),
                capabilities: identity.capability_labels(),
            },
            remote: PeerInfo {
                agent_id: String::new(),
                agent_name: String::new(),
                capabilities: vec!["transport:simplex-dns".into()],
            },
            state: SessionState::Active,
        },
        socket: Arc::new(socket),
        zone_labels: parse_zone_labels(&parsed.path),
        remote_addr: Arc::new(tokio::sync::Mutex::new(Some(
            format!("{host}:{port}")
                .parse()
                .map_err(|e: std::net::AddrParseError| Error::new(ErrorKind::InvalidInput, e.to_string()))?,
        ))),
        session_token: Arc::new(tokio::sync::Mutex::new(Some(format!(
            "{:016x}",
            rand::random::<u64>()
        )))),
        next_send_seq: Arc::new(tokio::sync::Mutex::new(0)),
        next_poll_seq: Arc::new(tokio::sync::Mutex::new(1_000_000)),
        inbound: Arc::new(tokio::sync::Mutex::new(InboundState::default())),
        outbound: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
        shared_key,
        is_client: true,
    };

    let hello = hello_frame(&identity);
    peer.send_frame(&hello).await?;
    let ack = peer.read_frame().await?;
    match &ack.message {
        Message::HelloAck(msg) if msg.accepted => {}
        _ => {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "peer rejected hello handshake",
            ))
        }
    }

    let mut session = peer.session.clone();
    session.remote.agent_id = ack.header.src_agent.clone().unwrap_or_default();
    session.remote.agent_name = ack.header.src_agent.clone().unwrap_or_default();
    let connected = ActiveSimplexDnsPeer { session, ..peer };
    let heartbeat = heartbeat_frame(identity.id.clone(), ack.header.src_agent.clone());
    connected.send_frame(&heartbeat).await?;
    let heartbeat_ack = connected.read_frame().await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }
    Ok(connected)
}

pub async fn run_inbound_session_once(
    identity: AgentIdentity,
    socket: UdpSocket,
    path: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = accept_peer_on(identity, socket, path).await?;
    Ok((peer.session, Vec::new()))
}

pub async fn run_outbound_session_once(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = connect_peer(identity, endpoint).await?;
    Ok((peer.session, Vec::new()))
}

#[derive(Debug)]
struct QueryPacket {
    id: u16,
    qname_labels: Vec<String>,
    session_token: String,
    seq: u64,
    frag_idx: u16,
    frag_total: u16,
    chunk_b32: Option<String>,
}

#[derive(Debug)]
struct ResponsePacket {
    kind: String,
    seq: u64,
    frag_idx: u16,
    frag_total: u16,
    chunk_b32: String,
}

fn payload_chunk_count(payload_len: usize, chunk_size: usize) -> u16 {
    payload_len.div_ceil(chunk_size).max(1) as u16
}

fn chunk_query_payload(payload: &[u8]) -> Vec<String> {
    payload
        .chunks(QUERY_CHUNK_BYTES)
        .map(|chunk| BASE32_NOPAD.encode(chunk))
        .collect()
}

fn chunk_response_payload(payload: &[u8]) -> Vec<String> {
    payload
        .chunks(RESPONSE_CHUNK_BYTES)
        .map(|chunk| BASE32_NOPAD.encode(chunk))
        .collect()
}

fn parse_zone_labels(path: &str) -> Vec<String> {
    let zone = path.trim_matches('/');
    if zone.is_empty() {
        return vec!["fusion".into(), "local".into()];
    }
    zone.split('.')
        .filter(|label| !label.is_empty())
        .map(|label| label.to_ascii_lowercase())
        .collect()
}

fn encode_query(
    id: u16,
    zone_labels: &[String],
    session_token: &str,
    seq: u64,
    frag_idx: u16,
    frag_total: u16,
    is_poll: bool,
    chunk_b32: Option<&str>,
) -> Result<Vec<u8>, Error> {
    let mut labels = vec![
        "fx".to_string(),
        "v1".to_string(),
        if is_poll { "poll" } else { "data" }.to_string(),
        session_token.to_string(),
        seq.to_string(),
        frag_idx.to_string(),
        frag_total.to_string(),
    ];
    if let Some(chunk) = chunk_b32 {
        labels.push(chunk.to_string());
    }
    labels.extend(zone_labels.iter().cloned());
    let qname = encode_qname(&labels)?;

    let mut out = Vec::new();
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(&0x0100u16.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&qname);
    out.extend_from_slice(&16u16.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    Ok(out)
}

fn encode_response(id: u16, qname_labels: &[String], txt: &str) -> Result<Vec<u8>, Error> {
    if txt.len() > 255 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "dns txt response too large",
        ));
    }
    let qname = encode_qname(qname_labels)?;
    let mut out = Vec::new();
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(&0x8180u16.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&qname);
    out.extend_from_slice(&16u16.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&qname);
    out.extend_from_slice(&16u16.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&((txt.len() + 1) as u16).to_be_bytes());
    out.push(txt.len() as u8);
    out.extend_from_slice(txt.as_bytes());
    Ok(out)
}

fn decode_query(packet: &[u8], zone_labels: &[String]) -> Result<QueryPacket, Error> {
    if packet.len() < 12 {
        return Err(Error::new(ErrorKind::UnexpectedEof, "short dns query"));
    }
    let id = u16::from_be_bytes([packet[0], packet[1]]);
    let qdcount = u16::from_be_bytes([packet[4], packet[5]]);
    if qdcount != 1 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "dns query must contain exactly one question",
        ));
    }
    let (labels, _) = decode_qname(packet, 12)?;
    if !labels.ends_with(zone_labels) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "dns query zone does not match listener",
        ));
    }
    if labels.len() < zone_labels.len() + 7 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "dns query missing metadata labels",
        ));
    }
    let meta = &labels[..labels.len() - zone_labels.len()];
    let session_token = meta[3].clone();
    let seq = meta[4]
        .parse::<u64>()
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
    let frag_idx = meta[5]
        .parse::<u16>()
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
    let frag_total = meta[6]
        .parse::<u16>()
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
    let chunk_b32 = meta.get(7).cloned();
    Ok(QueryPacket {
        id,
        qname_labels: labels,
        session_token,
        seq,
        frag_idx,
        frag_total,
        chunk_b32,
    })
}

fn decode_response(packet: &[u8]) -> Result<ResponsePacket, Error> {
    if packet.len() < 12 {
        return Err(Error::new(ErrorKind::UnexpectedEof, "short dns response"));
    }
    let ancount = u16::from_be_bytes([packet[6], packet[7]]);
    if ancount == 0 {
        return Ok(ResponsePacket {
            kind: "ack".into(),
            seq: 0,
            frag_idx: 0,
            frag_total: 0,
            chunk_b32: String::new(),
        });
    }
    let (_, mut offset) = decode_qname(packet, 12)?;
    offset += 4;
    let (_, next) = decode_qname(packet, offset)?;
    offset = next;
    if packet.len() < offset + 10 {
        return Err(Error::new(ErrorKind::UnexpectedEof, "short dns answer"));
    }
    let rdlength = u16::from_be_bytes([packet[offset + 8], packet[offset + 9]]) as usize;
    offset += 10;
    if packet.len() < offset + rdlength || rdlength == 0 {
        return Err(Error::new(ErrorKind::UnexpectedEof, "short dns txt rdata"));
    }
    let txt_len = packet[offset] as usize;
    if txt_len + 1 > rdlength {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "invalid dns txt string length",
        ));
    }
    let txt = std::str::from_utf8(&packet[offset + 1..offset + 1 + txt_len])
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
    let parts: Vec<_> = txt.split('|').collect();
    if parts.len() == 3 && parts[2] == "ack" {
        return Ok(ResponsePacket {
            kind: "ack".into(),
            seq: 0,
            frag_idx: 0,
            frag_total: 0,
            chunk_b32: String::new(),
        });
    }
    if parts.len() != 8 || parts[0] != "fx" || parts[1] != "v1" {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "unrecognized dns txt response payload",
        ));
    }
    Ok(ResponsePacket {
        kind: parts[2].to_string(),
        seq: parts[4]
            .parse::<u64>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?,
        frag_idx: parts[5]
            .parse::<u16>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?,
        frag_total: parts[6]
            .parse::<u16>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?,
        chunk_b32: parts[7].to_string(),
    })
}

fn encode_qname(labels: &[String]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    for label in labels {
        if label.is_empty() || label.len() > 63 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "dns label length out of range",
            ));
        }
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
    Ok(out)
}

fn decode_qname(packet: &[u8], mut offset: usize) -> Result<(Vec<String>, usize), Error> {
    let mut labels = Vec::new();
    loop {
        if packet.len() <= offset {
            return Err(Error::new(ErrorKind::UnexpectedEof, "short dns qname"));
        }
        let len = packet[offset] as usize;
        offset += 1;
        if len == 0 {
            break;
        }
        if packet.len() < offset + len {
            return Err(Error::new(ErrorKind::UnexpectedEof, "short dns label"));
        }
        let label = std::str::from_utf8(&packet[offset..offset + len])
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
        labels.push(label.to_ascii_lowercase());
        offset += len;
    }
    Ok((labels, offset))
}

async fn recv_dns_packet(socket: &UdpSocket, expected: Option<SocketAddr>) -> Result<Vec<u8>, Error> {
    loop {
        let (packet, remote) = recv_dns_packet_from_any(socket).await?;
        if expected.is_none() || expected == Some(remote) {
            return Ok(packet);
        }
    }
}

async fn recv_dns_packet_from_any(socket: &UdpSocket) -> Result<(Vec<u8>, SocketAddr), Error> {
    let mut buf = vec![0_u8; 2048];
    let (n, remote) = socket.recv_from(&mut buf).await?;
    buf.truncate(n);
    Ok((buf, remote))
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, TaskAction, TaskRequestMessage},
        },
    };

    use super::{accept_peer_on, bind, connect_peer};

    #[tokio::test]
    async fn simplex_dns_task_frame_roundtrip() {
        let listener = bind("simplex+dns://127.0.0.1:0/tunnel.local").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-dns-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-dns-client".into()),
            key: None,
        });

        let server = tokio::spawn(async move {
            let peer = accept_peer_on(server_identity, listener, "/tunnel.local")
                .await
                .unwrap();
            let frame = peer.read_frame().await.unwrap();
            match frame.message {
                Message::TaskRequest(req) => assert_eq!(req.task_id, "dns-task-1"),
                other => panic!("unexpected frame: {:?}", other),
            }
        });

        let peer = connect_peer(
            client_identity,
            &format!("simplex+dns://{}/tunnel.local", addr),
        )
        .await
        .unwrap();
        let frame = Frame::new(
            MessageType::TaskRequest,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::TaskRequest(TaskRequestMessage {
                task_id: "dns-task-1".into(),
                action: TaskAction::Shell,
                args: vec!["echo".into()],
                data_hex: None,
            }),
        );
        peer.send_frame(&frame).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn simplex_dns_fragmented_roundtrip() {
        let listener = bind("simplex+dns://127.0.0.1:0/chunk.local").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-dns-frag-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-dns-frag-client".into()),
            key: None,
        });

        let server = tokio::spawn(async move {
            let peer = accept_peer_on(server_identity, listener, "/chunk.local")
                .await
                .unwrap();
            let frame = peer.read_frame().await.unwrap();
            match frame.message {
                Message::TaskRequest(req) => assert!(req.args.join(" ").len() > 64),
                other => panic!("unexpected frame: {:?}", other),
            }
        });

        let peer = connect_peer(
            client_identity,
            &format!("simplex+dns://{}/chunk.local", addr),
        )
        .await
        .unwrap();
        let frame = Frame::new(
            MessageType::TaskRequest,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::TaskRequest(TaskRequestMessage {
                task_id: "dns-task-frag".into(),
                action: TaskAction::Shell,
                args: vec!["x".repeat(160)],
                data_hex: None,
            }),
        );
        peer.send_frame(&frame).await.unwrap();
        server.await.unwrap();
    }
}
