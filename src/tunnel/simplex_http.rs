use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    io::{Error, ErrorKind},
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
    time::sleep,
};

use crate::{
    agent::identity::AgentIdentity,
    crypto::transport::{decode_transport_frame, encode_transport_frame, SharedKey},
    protocol::{frame::Frame, message::Message},
    session::{
        handshake::{complete_session, hello_ack_frame, hello_frame},
        heartbeat::heartbeat_frame,
        peer::{PeerInfo, PeerSession, SessionState},
    },
    tunnel::simplex::{fragment_payload, reassemble_fragments, SimplexFragment, SrArqWindow},
    utils::url::ParsedUrl,
};

const MAX_FRAGMENT_SIZE: usize = 768;
const ACK_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_RETRIES: u32 = 4;
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const SEND_WINDOW_SIZE: usize = 4;
const LONG_POLL_TIMEOUT: Duration = Duration::from_millis(150);
const MAX_BATCH_PACKETS: usize = 8;
const RECEIVED_SEQUENCE_CACHE_SIZE: usize = 2048;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SimplexHttpPacket {
    Data {
        sequence: u64,
        fragment: SimplexFragment,
    },
    Ack {
        sequence: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SimplexHttpEnvelope {
    packets: Vec<Vec<u8>>,
}

#[derive(Default)]
struct FragmentAssembly {
    totals: BTreeMap<u64, u32>,
    fragments: BTreeMap<u64, BTreeMap<u32, Vec<u8>>>,
}

impl FragmentAssembly {
    fn push(&mut self, fragment: SimplexFragment) -> Result<Option<Vec<u8>>, Error> {
        let entry = self.fragments.entry(fragment.message_id).or_default();
        self.totals
            .entry(fragment.message_id)
            .or_insert(fragment.total);
        entry.insert(fragment.index, fragment.payload.clone());

        let expected_total = self
            .totals
            .get(&fragment.message_id)
            .copied()
            .unwrap_or(fragment.total);
        if entry.len() != expected_total as usize {
            return Ok(None);
        }

        let fragments = entry
            .iter()
            .map(|(index, payload)| SimplexFragment {
                message_id: fragment.message_id,
                index: *index,
                total: expected_total,
                payload: payload.clone(),
            })
            .collect::<Vec<_>>();
        self.fragments.remove(&fragment.message_id);
        self.totals.remove(&fragment.message_id);
        reassemble_fragments(&fragments).map(Some)
    }
}

#[derive(Default)]
struct ReceivedSequenceWindow {
    order: VecDeque<u64>,
    seen: HashSet<u64>,
}

impl ReceivedSequenceWindow {
    fn remember(&mut self, sequence: u64) -> bool {
        if self.seen.contains(&sequence) {
            return false;
        }
        self.order.push_back(sequence);
        self.seen.insert(sequence);
        while self.order.len() > RECEIVED_SEQUENCE_CACHE_SIZE {
            if let Some(evicted) = self.order.pop_front() {
                self.seen.remove(&evicted);
            }
        }
        true
    }
}

#[derive(Clone)]
pub struct ActiveSimplexHttpPeer {
    pub session: PeerSession,
    endpoint: String,
    path: String,
    outgoing: Arc<Mutex<VecDeque<Vec<u8>>>>,
    incoming: Arc<Mutex<mpsc::Receiver<Frame>>>,
    incoming_tx: mpsc::Sender<Frame>,
    shared_key: Option<SharedKey>,
    reassembly: Arc<Mutex<FragmentAssembly>>,
    arq: Arc<Mutex<SrArqWindow>>,
    acked_sequences: Arc<Mutex<HashSet<u64>>>,
    received_sequences: Arc<Mutex<ReceivedSequenceWindow>>,
    next_message_id: Arc<Mutex<u64>>,
}

impl ActiveSimplexHttpPeer {
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        let payload = encode_transport_frame(frame, self.shared_key.as_ref())?;
        let message_id = {
            let mut next = self.next_message_id.lock().await;
            *next = next.saturating_add(1);
            *next
        };
        let fragments = fragment_payload(message_id, &payload, MAX_FRAGMENT_SIZE);

        let mut packets = VecDeque::new();
        {
            let mut arq = self.arq.lock().await;
            for fragment in fragments {
                let sequence = arq.allocate_sequence();
                let packet = SimplexHttpPacket::Data { sequence, fragment };
                let packet_bytes = encode_simplex_packet(&packet)?;
                arq.track(sequence, packet_bytes.clone());
                packets.push_back((sequence, packet_bytes));
            }
        }
        self.send_windowed_packets(packets).await?;
        Ok(())
    }

    pub async fn read_frame(&self) -> Result<Frame, Error> {
        if self.endpoint.is_empty() {
            let mut rx = self.incoming.lock().await;
            return rx.recv().await.ok_or_else(|| {
                Error::new(
                    ErrorKind::UnexpectedEof,
                    "simplex http inbound channel closed",
                )
            });
        }

        loop {
            {
                let mut rx = self.incoming.lock().await;
                match rx.try_recv() {
                    Ok(frame) => return Ok(frame),
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        return Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "simplex http inbound channel closed",
                        ))
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                }
            }
            self.poll_remote_once().await?;
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn dispatch_payload(&self, payload: &[u8]) -> Result<(), Error> {
        if self.endpoint.is_empty() {
            self.outgoing.lock().await.push_back(payload.to_vec());
            return Ok(());
        }
        http_post_bytes(&self.endpoint, &self.path, payload).await
    }

    async fn dispatch_payload_batch(&self, payloads: &[Vec<u8>]) -> Result<(), Error> {
        if payloads.is_empty() {
            return Ok(());
        }
        if self.endpoint.is_empty() {
            let mut outgoing = self.outgoing.lock().await;
            for payload in payloads {
                outgoing.push_back(payload.clone());
            }
            return Ok(());
        }
        http_post_envelope(&self.endpoint, &self.path, payloads).await
    }

    async fn send_windowed_packets(
        &self,
        mut pending_packets: VecDeque<(u64, Vec<u8>)>,
    ) -> Result<(), Error> {
        let mut in_flight = HashSet::new();

        while !pending_packets.is_empty() || !in_flight.is_empty() {
            let mut dispatch_batch = Vec::new();
            while in_flight.len() < SEND_WINDOW_SIZE {
                let Some((sequence, packet_bytes)) = pending_packets.pop_front() else {
                    break;
                };
                in_flight.insert(sequence);
                dispatch_batch.push(packet_bytes);
            }

            if !dispatch_batch.is_empty() {
                self.dispatch_payload_batch(&dispatch_batch).await?;
            }

            self.wait_for_window_progress(&mut in_flight).await?;
        }

        Ok(())
    }

    async fn wait_for_window_progress(&self, in_flight: &mut HashSet<u64>) -> Result<(), Error> {
        loop {
            if self.drain_acked(in_flight).await? {
                return Ok(());
            }

            let started_at = Instant::now();
            while started_at.elapsed() < ACK_TIMEOUT {
                if self.endpoint.is_empty() {
                    sleep(POLL_INTERVAL).await;
                } else {
                    self.poll_remote_once().await?;
                    sleep(POLL_INTERVAL).await;
                }

                if self.drain_acked(in_flight).await? {
                    return Ok(());
                }
            }

            let retried = self.retry_in_flight(in_flight).await?;
            if !retried {
                return Err(Error::new(
                    ErrorKind::TimedOut,
                    "simplex http window stalled without retry candidates",
                ));
            }
        }
    }

    async fn drain_acked(&self, in_flight: &mut HashSet<u64>) -> Result<bool, Error> {
        let acked = self.acked_sequences.lock().await;
        let acked_now = acked
            .iter()
            .copied()
            .filter(|sequence| in_flight.contains(sequence))
            .collect::<Vec<_>>();
        let progressed = !acked_now.is_empty();
        drop(acked);

        if !progressed {
            return Ok(false);
        }

        let mut arq = self.arq.lock().await;
        let mut acked = self.acked_sequences.lock().await;
        for sequence in acked_now {
            acked.remove(&sequence);
            in_flight.remove(&sequence);
            arq.ack(sequence);
        }
        Ok(true)
    }

    async fn retry_in_flight(&self, in_flight: &HashSet<u64>) -> Result<bool, Error> {
        let now = Instant::now();
        let mut retried_any = false;

        loop {
            let retry_packet = self.arq.lock().await.next_retry(now, ACK_TIMEOUT);
            let Some(packet) = retry_packet else {
                break;
            };
            if !in_flight.contains(&packet.sequence) {
                continue;
            }
            if packet.retries > MAX_RETRIES {
                return Err(Error::new(
                    ErrorKind::TimedOut,
                    format!(
                        "simplex http ack timeout sequence={} retries={}",
                        packet.sequence, packet.retries
                    ),
                ));
            }
            self.dispatch_payload(&packet.payload).await?;
            retried_any = true;
        }

        Ok(retried_any)
    }

    async fn poll_remote_once(&self) -> Result<(), Error> {
        if let Some(bytes) = http_get_bytes(&self.endpoint, &self.path).await? {
            self.process_client_payloads(&bytes).await?;
        }
        Ok(())
    }

    async fn process_client_payloads(&self, body: &[u8]) -> Result<(), Error> {
        for payload in decode_simplex_envelope(body)? {
            self.process_client_payload(&payload).await?;
        }
        Ok(())
    }

    async fn process_client_payload(&self, body: &[u8]) -> Result<(), Error> {
        match decode_simplex_packet(body)? {
            SimplexHttpPacket::Ack { sequence } => {
                self.acked_sequences.lock().await.insert(sequence);
            }
            SimplexHttpPacket::Data { sequence, fragment } => {
                let ack = encode_simplex_packet(&SimplexHttpPacket::Ack { sequence })?;
                http_post_bytes(&self.endpoint, &self.path, &ack).await?;
                let is_new = self.received_sequences.lock().await.remember(sequence);
                if !is_new {
                    return Ok(());
                }
                if let Some(frame_payload) = self.reassembly.lock().await.push(fragment)? {
                    let frame = decode_transport_frame(&frame_payload, self.shared_key.as_ref())?;
                    self.incoming_tx.send(frame).await.map_err(|_| {
                        Error::new(ErrorKind::BrokenPipe, "simplex http receiver dropped")
                    })?;
                }
            }
        }
        Ok(())
    }
}

pub async fn bind(endpoint: &str) -> Result<TcpListener, Error> {
    TcpListener::bind(endpoint).await
}

pub async fn accept_peer_on(
    identity: AgentIdentity,
    listener: TcpListener,
    path: &str,
) -> Result<ActiveSimplexHttpPeer, Error> {
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let outgoing = Arc::new(Mutex::new(VecDeque::new()));
    let (incoming_tx, incoming_rx) = mpsc::channel(64);
    let path = normalize_path(path);
    let reassembly = Arc::new(Mutex::new(FragmentAssembly::default()));
    let acked_sequences = Arc::new(Mutex::new(HashSet::new()));
    let received_sequences = Arc::new(Mutex::new(ReceivedSequenceWindow::default()));
    spawn_server_loop(
        listener,
        path.clone(),
        shared_key.clone(),
        outgoing.clone(),
        incoming_tx.clone(),
        reassembly.clone(),
        acked_sequences.clone(),
        received_sequences.clone(),
    );

    let mut rx = incoming_rx;
    let hello = rx.recv().await.ok_or_else(|| {
        Error::new(
            ErrorKind::UnexpectedEof,
            "simplex http hello channel closed",
        )
    })?;
    let session = complete_session(&identity, &hello)?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));

    let peer = ActiveSimplexHttpPeer {
        session,
        endpoint: String::new(),
        path,
        outgoing,
        incoming: Arc::new(Mutex::new(rx)),
        incoming_tx: incoming_tx.clone(),
        shared_key,
        reassembly,
        arq: Arc::new(Mutex::new(SrArqWindow::new())),
        acked_sequences,
        received_sequences,
        next_message_id: Arc::new(Mutex::new(0)),
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
) -> Result<ActiveSimplexHttpPeer, Error> {
    let parsed = ParsedUrl::parse(endpoint)?;
    let host = parsed.host.clone().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            "missing host for simplex+http connect",
        )
    })?;
    let port = parsed.port.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            "missing port for simplex+http connect",
        )
    })?;
    let base = format!("{host}:{port}");
    let path = normalize_path(&parsed.path);
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let (incoming_tx, incoming_rx) = mpsc::channel(64);
    let peer = ActiveSimplexHttpPeer {
        session: PeerSession {
            local: PeerInfo {
                agent_id: identity.id.clone(),
                agent_name: identity.name.clone(),
                capabilities: identity.capability_labels(),
            },
            remote: PeerInfo {
                agent_id: String::new(),
                agent_name: String::new(),
                capabilities: vec!["transport:simplex-http".into()],
            },
            state: SessionState::Active,
        },
        endpoint: base,
        path,
        outgoing: Arc::new(Mutex::new(VecDeque::new())),
        incoming: Arc::new(Mutex::new(incoming_rx)),
        incoming_tx,
        shared_key,
        reassembly: Arc::new(Mutex::new(FragmentAssembly::default())),
        arq: Arc::new(Mutex::new(SrArqWindow::new())),
        acked_sequences: Arc::new(Mutex::new(HashSet::new())),
        received_sequences: Arc::new(Mutex::new(ReceivedSequenceWindow::default())),
        next_message_id: Arc::new(Mutex::new(0)),
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
            ));
        }
    }

    let mut session = peer.session.clone();
    session.remote.agent_id = ack.header.src_agent.clone().unwrap_or_default();
    session.remote.agent_name = ack.header.src_agent.clone().unwrap_or_default();

    let connected_peer = ActiveSimplexHttpPeer { session, ..peer };
    let heartbeat = heartbeat_frame(identity.id.clone(), ack.header.src_agent.clone());
    connected_peer.send_frame(&heartbeat).await?;
    let heartbeat_ack = connected_peer.read_frame().await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }

    Ok(connected_peer)
}

pub async fn run_inbound_session_once(
    identity: AgentIdentity,
    listener: TcpListener,
    path: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = accept_peer_on(identity, listener, path).await?;
    Ok((peer.session, Vec::new()))
}

pub async fn run_outbound_session_once(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = connect_peer(identity, endpoint).await?;
    Ok((peer.session, Vec::new()))
}

fn spawn_server_loop(
    listener: TcpListener,
    path: String,
    shared_key: Option<SharedKey>,
    outgoing: Arc<Mutex<VecDeque<Vec<u8>>>>,
    incoming_tx: mpsc::Sender<Frame>,
    reassembly: Arc<Mutex<FragmentAssembly>>,
    acked_sequences: Arc<Mutex<HashSet<u64>>>,
    received_sequences: Arc<Mutex<ReceivedSequenceWindow>>,
) {
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let path = path.clone();
            let shared_key = shared_key.clone();
            let outgoing = outgoing.clone();
            let incoming_tx = incoming_tx.clone();
            let reassembly = reassembly.clone();
            let acked_sequences = acked_sequences.clone();
            let received_sequences = received_sequences.clone();
            tokio::spawn(async move {
                let _ = handle_http_exchange(
                    &mut stream,
                    &path,
                    shared_key,
                    outgoing,
                    incoming_tx,
                    reassembly,
                    acked_sequences,
                    received_sequences,
                )
                .await;
            });
        }
    });
}

async fn handle_http_exchange(
    stream: &mut TcpStream,
    expected_path: &str,
    shared_key: Option<SharedKey>,
    outgoing: Arc<Mutex<VecDeque<Vec<u8>>>>,
    incoming_tx: mpsc::Sender<Frame>,
    reassembly: Arc<Mutex<FragmentAssembly>>,
    acked_sequences: Arc<Mutex<HashSet<u64>>>,
    received_sequences: Arc<Mutex<ReceivedSequenceWindow>>,
) -> Result<(), Error> {
    let (method, path, _headers, body) = read_http_request(stream).await?;
    if path != expected_path {
        write_http_response(stream, 404, b"not found").await?;
        return Ok(());
    }

    match method.as_str() {
        "POST" => {
            for payload in decode_simplex_envelope(&body)? {
                match decode_simplex_packet(&payload)? {
                    SimplexHttpPacket::Ack { sequence } => {
                        acked_sequences.lock().await.insert(sequence);
                    }
                    SimplexHttpPacket::Data { sequence, fragment } => {
                        let ack = encode_simplex_packet(&SimplexHttpPacket::Ack { sequence })?;
                        outgoing.lock().await.push_back(ack);
                        let is_new = received_sequences.lock().await.remember(sequence);
                        if !is_new {
                            continue;
                        }
                        if let Some(frame_payload) = reassembly.lock().await.push(fragment)? {
                            let frame =
                                decode_transport_frame(&frame_payload, shared_key.as_ref())?;
                            incoming_tx.send(frame).await.map_err(|_| {
                                Error::new(ErrorKind::BrokenPipe, "simplex http receiver dropped")
                            })?;
                        }
                    }
                }
            }
            write_http_response(stream, 200, b"ok").await?;
        }
        "GET" => {
            let payloads = wait_for_outgoing_batch(outgoing, LONG_POLL_TIMEOUT).await;
            match payloads {
                Some(payloads) => {
                    let body = encode_simplex_envelope(&payloads)?;
                    write_http_response(stream, 200, &body).await?
                }
                None => write_http_response(stream, 204, b"").await?,
            }
        }
        _ => {
            write_http_response(stream, 405, b"method not allowed").await?;
        }
    }
    Ok(())
}

fn encode_simplex_packet(packet: &SimplexHttpPacket) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(packet).map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))
}

fn decode_simplex_packet(bytes: &[u8]) -> Result<SimplexHttpPacket, Error> {
    serde_json::from_slice(bytes).map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))
}

fn encode_simplex_envelope(packets: &[Vec<u8>]) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(&SimplexHttpEnvelope {
        packets: packets.to_vec(),
    })
    .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))
}

fn decode_simplex_envelope(bytes: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
    let envelope: SimplexHttpEnvelope = serde_json::from_slice(bytes)
        .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
    Ok(envelope.packets)
}

async fn wait_for_outgoing_batch(
    outgoing: Arc<Mutex<VecDeque<Vec<u8>>>>,
    timeout: Duration,
) -> Option<Vec<Vec<u8>>> {
    let started_at = Instant::now();
    loop {
        {
            let mut queue = outgoing.lock().await;
            if !queue.is_empty() {
                let mut batch = Vec::new();
                while batch.len() < MAX_BATCH_PACKETS {
                    let Some(payload) = queue.pop_front() else {
                        break;
                    };
                    batch.push(payload);
                }
                return Some(batch);
            }
        }

        if started_at.elapsed() >= timeout {
            return None;
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn read_http_request(
    stream: &mut TcpStream,
) -> Result<(String, String, Vec<(String, String)>, Vec<u8>), Error> {
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0_u8; 1024];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "http request closed early",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let header_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "http request missing header terminator",
            )
        })?
        + 4;
    let header_text = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = header_text.split("\r\n").filter(|line| !line.is_empty());
    let request_line = lines
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing request line"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let raw_path = parts.next().unwrap_or("/");
    let path = raw_path
        .split('?')
        .next()
        .map(normalize_path)
        .unwrap_or_else(|| "/".to_string());
    let headers = lines
        .filter_map(|line| {
            line.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .collect::<Vec<_>>();
    let content_length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let mut chunk = vec![0_u8; content_length - body.len()];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "http body closed early",
            ));
        }
        body.extend_from_slice(&chunk[..n]);
    }
    Ok((method, path, headers, body))
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
) -> Result<(), Error> {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "OK",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

async fn http_post_bytes(endpoint: &str, path: &str, payload: &[u8]) -> Result<(), Error> {
    http_post_envelope(endpoint, path, &[payload.to_vec()]).await
}

async fn http_post_envelope(endpoint: &str, path: &str, payloads: &[Vec<u8>]) -> Result<(), Error> {
    let mut stream = TcpStream::connect(endpoint).await?;
    let envelope = encode_simplex_envelope(payloads)?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {endpoint}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        envelope.len()
    );
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(&envelope).await?;
    stream.flush().await?;
    let (status, _) = read_http_response(&mut stream).await?;
    if status != 200 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("simplex http POST failed with status {status}"),
        ));
    }
    Ok(())
}

async fn http_get_bytes(endpoint: &str, path: &str) -> Result<Option<Vec<u8>>, Error> {
    let mut stream = TcpStream::connect(endpoint).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {endpoint}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;
    let (status, body) = read_http_response(&mut stream).await?;
    Ok(match status {
        200 => Some(body),
        204 => None,
        other => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("simplex http GET failed with status {other}"),
            ))
        }
    })
}

async fn read_http_response(stream: &mut TcpStream) -> Result<(u16, Vec<u8>), Error> {
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0_u8; 1024];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "http response closed early",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let header_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "http response missing header terminator",
            )
        })?
        + 4;
    let header_text = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = header_text.split("\r\n").filter(|line| !line.is_empty());
    let status_line = lines
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing status line"))?;
    let mut parts = status_line.split_whitespace();
    let _http = parts.next();
    let status = parts
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "invalid http status code"))?;
    let headers = lines
        .filter_map(|line| {
            line.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .collect::<Vec<_>>();
    let content_length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let mut chunk = vec![0_u8; content_length - body.len()];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "http response body closed early",
            ));
        }
        body.extend_from_slice(&chunk[..n]);
    }
    Ok((status, body))
}

fn normalize_path(path: &str) -> String {
    let clean = path.split('?').next().unwrap_or("/");
    if clean.is_empty() {
        "/".to_string()
    } else {
        clean.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        crypto::transport::encode_transport_frame,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, TaskAction, TaskRequestMessage},
        },
    };

    use super::{
        bind, connect_peer, encode_simplex_packet, http_post_envelope, run_inbound_session_once,
        run_outbound_session_once, SimplexHttpPacket,
    };

    #[tokio::test]
    async fn simplex_http_session_hello_heartbeat_roundtrip() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-http-server".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-http-client".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            run_inbound_session_once(server_identity, listener, "/tunnel")
                .await
                .unwrap()
        });

        let (client_session, _) =
            run_outbound_session_once(client_identity, &format!("simplex+http://{}/tunnel", addr))
                .await
                .unwrap();
        let (server_session, _) = server_task.await.unwrap();

        assert_eq!(client_session.local.agent_name, "simplex-http-client");
        assert_eq!(server_session.local.agent_name, "simplex-http-server");
    }

    #[tokio::test]
    async fn simplex_http_peer_can_exchange_task_frames_after_handshake() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-http-task-server".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-http-task-client".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = super::accept_peer_on(server_identity, listener, "/task")
                .await
                .unwrap();
            let frame = peer.read_frame().await.unwrap();
            match frame.message {
                Message::TaskRequest(req) => {
                    assert!(matches!(req.action, TaskAction::Shell));
                    assert_eq!(req.args.first().map(String::as_str), Some("whoami"));
                }
                other => panic!("unexpected message: {:?}", other),
            }
        });

        let client_peer = connect_peer(client_identity, &format!("simplex+http://{}/task", addr))
            .await
            .unwrap();
        let frame = Frame::new(
            MessageType::TaskRequest,
            Some(client_peer.session.local.agent_id.clone()),
            Some(client_peer.session.remote.agent_id.clone()),
            Message::TaskRequest(TaskRequestMessage {
                task_id: "simplex-http-task-1".into(),
                action: TaskAction::Shell,
                args: vec!["whoami".into()],
                data_hex: None,
            }),
        );
        client_peer.send_frame(&frame).await.unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn simplex_http_fragments_large_task_frame() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-http-frag-server".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-http-frag-client".to_string()),
            key: None,
        });
        let large_arg = "A".repeat(4096);
        let expected = large_arg.clone();

        let server_task = tokio::spawn(async move {
            let peer = super::accept_peer_on(server_identity, listener, "/frag")
                .await
                .unwrap();
            let frame = peer.read_frame().await.unwrap();
            match frame.message {
                Message::TaskRequest(req) => {
                    assert_eq!(
                        req.args.first().map(String::as_str),
                        Some(expected.as_str())
                    );
                }
                other => panic!("unexpected message: {:?}", other),
            }
        });

        let client_peer = connect_peer(client_identity, &format!("simplex+http://{}/frag", addr))
            .await
            .unwrap();
        let frame = Frame::new(
            MessageType::TaskRequest,
            Some(client_peer.session.local.agent_id.clone()),
            Some(client_peer.session.remote.agent_id.clone()),
            Message::TaskRequest(TaskRequestMessage {
                task_id: "simplex-http-task-frag".into(),
                action: TaskAction::Shell,
                args: vec![large_arg],
                data_hex: None,
            }),
        );
        client_peer.send_frame(&frame).await.unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn simplex_http_can_receive_multiple_frames_from_one_poll_batch() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-http-batch-server".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-http-batch-client".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = super::accept_peer_on(server_identity, listener, "/batch")
                .await
                .unwrap();
            for task_id in ["batch-1", "batch-2"] {
                let frame = Frame::new(
                    MessageType::TaskRequest,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::TaskRequest(TaskRequestMessage {
                        task_id: task_id.into(),
                        action: TaskAction::Shell,
                        args: vec![task_id.into()],
                        data_hex: None,
                    }),
                );
                peer.send_frame(&frame).await.unwrap();
            }
        });

        let client_peer = connect_peer(client_identity, &format!("simplex+http://{}/batch", addr))
            .await
            .unwrap();
        let first = client_peer.read_frame().await.unwrap();
        let second = client_peer.read_frame().await.unwrap();

        let extract_task_id = |frame: Frame| match frame.message {
            Message::TaskRequest(req) => req.task_id,
            other => panic!("unexpected message: {:?}", other),
        };
        assert_eq!(extract_task_id(first), "batch-1");
        assert_eq!(extract_task_id(second), "batch-2");
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn simplex_http_suppresses_duplicate_retransmitted_packet_delivery() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-http-dedup-server".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-http-dedup-client".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = super::accept_peer_on(server_identity, listener, "/dedup")
                .await
                .unwrap();
            let first = peer.read_frame().await.unwrap();
            let second = timeout(Duration::from_millis(300), peer.read_frame()).await;
            (first, second)
        });

        let client_peer = connect_peer(client_identity, &format!("simplex+http://{}/dedup", addr))
            .await
            .unwrap();
        let frame = Frame::new(
            MessageType::TaskRequest,
            Some(client_peer.session.local.agent_id.clone()),
            Some(client_peer.session.remote.agent_id.clone()),
            Message::TaskRequest(TaskRequestMessage {
                task_id: "simplex-http-dedup-task".into(),
                action: TaskAction::Shell,
                args: vec!["dedup".into()],
                data_hex: None,
            }),
        );
        let encoded = encode_transport_frame(&frame, None).unwrap();
        let fragment = crate::tunnel::simplex::fragment_payload(9001, &encoded, 4096)
            .into_iter()
            .next()
            .unwrap();
        let packet = encode_simplex_packet(&SimplexHttpPacket::Data {
            sequence: 777,
            fragment,
        })
        .unwrap();
        http_post_envelope(&addr.to_string(), "/dedup", &[packet.clone()])
            .await
            .unwrap();
        http_post_envelope(&addr.to_string(), "/dedup", &[packet])
            .await
            .unwrap();
        let (first, second) = server_task.await.unwrap();
        match first.message {
            Message::TaskRequest(req) => assert_eq!(req.task_id, "simplex-http-dedup-task"),
            other => panic!("unexpected message: {:?}", other),
        }
        assert!(
            second.is_err(),
            "duplicate packet should not produce second frame"
        );
    }
}
