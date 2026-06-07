use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    net::SocketAddr,
    sync::Arc,
};

use bytes::{Buf, Bytes, BytesMut};
use h2::{client, server, RecvStream, SendStream};
use http::{header, Request, Response, StatusCode};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::{mpsc, Mutex},
};
use tokio_rustls::TlsAcceptor;

use crate::{
    agent::identity::AgentIdentity,
    crypto::transport::{decode_transport_frame, encode_transport_frame, SharedKey},
    protocol::{
        frame::Frame,
        message::{Message, StreamOpenMessage},
    },
    session::{
        handshake::{complete_session, hello_ack_frame, hello_frame},
        heartbeat::heartbeat_frame,
        peer::{PeerInfo, PeerSession, SessionState},
    },
    tunnel::tls::{build_h2_tls_connector, connect_tcp_for_url},
    utils::url::ParsedUrl,
};

const CONTROL_STREAM_CT: &str = "application/fusion-h2-mux";
const DATA_STREAM_CT: &str = "application/fusion-h2-data";
const FUSION_STREAM_ID: &str = "x-fusion-stream-id";

#[derive(Debug, Default)]
struct StreamRoutingState {
    senders: HashMap<u32, mpsc::Sender<Frame>>,
    pending: HashMap<u32, Vec<Frame>>,
    pending_outbound: HashMap<u32, Vec<Frame>>,
}

#[derive(Debug, Default)]
struct DataStreamRegistry {
    senders: HashMap<u32, SendStream<Bytes>>,
}

struct FrameReader {
    buffer: BytesMut,
}

impl FrameReader {
    fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
        }
    }

    async fn read_frame(
        &mut self,
        recv: &mut RecvStream,
        shared_key: Option<&SharedKey>,
    ) -> Result<Frame, Error> {
        loop {
            if self.buffer.len() >= 4 {
                let len = u32::from_be_bytes(self.buffer[..4].try_into().unwrap()) as usize;
                if self.buffer.len() >= 4 + len {
                    self.buffer.advance(4);
                    let payload = self.buffer.split_to(len).to_vec();
                    return decode_transport_frame(&payload, shared_key);
                }
            }
            match recv.data().await {
                Some(Ok(chunk)) => {
                    if chunk.is_empty() {
                        continue;
                    }
                    self.buffer.extend_from_slice(&chunk);
                }
                Some(Err(err)) => {
                    return Err(Error::new(
                        ErrorKind::BrokenPipe,
                        format!("h2 recv error: {err}"),
                    ));
                }
                None => {
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "h2 stream closed before frame",
                    ));
                }
            }
        }
    }
}

async fn write_h2_frame(
    send: &mut SendStream<Bytes>,
    frame: &Frame,
    shared_key: Option<&SharedKey>,
) -> Result<(), Error> {
    let payload = encode_transport_frame(frame, shared_key)?;
    let len = u32::try_from(payload.len())
        .map_err(|_| Error::new(ErrorKind::InvalidData, "frame too large"))?;
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&payload);
    send.send_data(Bytes::from(buf), false)
        .map_err(|err| Error::new(ErrorKind::BrokenPipe, format!("h2 send error: {err}")))?;
    Ok(())
}

async fn read_h2_frame(
    reader: &mut FrameReader,
    recv: &mut RecvStream,
    shared_key: Option<&SharedKey>,
) -> Result<Frame, Error> {
    reader.read_frame(recv, shared_key).await
}

fn frame_uses_data_stream(frame: &Frame) -> bool {
    matches!(frame.message, Message::StreamData(_))
}

fn parse_fusion_stream_id(headers: &http::HeaderMap) -> Result<u32, Error> {
    let value = headers
        .get(FUSION_STREAM_ID)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "missing x-fusion-stream-id on h2 data stream",
            )
        })?
        .to_str()
        .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid x-fusion-stream-id"))?;
    value.parse::<u32>().map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            "invalid x-fusion-stream-id value",
        )
    })
}

fn is_data_stream_request(headers: &http::HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        == Some(DATA_STREAM_CT)
}

struct SharedH2MuxState {
    routing: Arc<Mutex<StreamRoutingState>>,
    data_streams: Arc<Mutex<DataStreamRegistry>>,
    shared_key: Option<SharedKey>,
    open_tx: mpsc::Sender<Frame>,
    control_tx: mpsc::Sender<Frame>,
    requester: Option<Arc<Mutex<client::SendRequest<Bytes>>>>,
    endpoint_authority: String,
    endpoint_path: String,
}

impl SharedH2MuxState {
    async fn register_data_stream(
        self: &Arc<Self>,
        stream_id: u32,
        mut send: SendStream<Bytes>,
        mut recv: RecvStream,
    ) {
        {
            let mut routing = self.routing.lock().await;
            if let Some(pending) = routing.pending_outbound.remove(&stream_id) {
                for frame in pending {
                    if write_h2_frame(&mut send, &frame, self.shared_key.as_ref())
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
        {
            let mut registry = self.data_streams.lock().await;
            registry.senders.insert(stream_id, send);
        }

        let routing = self.routing.clone();
        let shared_key = self.shared_key.clone();
        let data_streams = self.data_streams.clone();
        tokio::spawn(async move {
            let mut reader = FrameReader::new();
            loop {
                let frame = match read_h2_frame(&mut reader, &mut recv, shared_key.as_ref()).await
                {
                    Ok(frame) => frame,
                    Err(_) => break,
                };
                if matches!(frame.message, Message::StreamClose(_)) {
                    break;
                }
                let sid = frame.header.stream_id.unwrap_or(stream_id);
                let sender = {
                    let guard = routing.lock().await;
                    guard.senders.get(&sid).cloned()
                };
                if let Some(tx) = sender {
                    if tx.send(frame).await.is_err() {
                        let mut guard = routing.lock().await;
                        guard.senders.remove(&sid);
                    }
                } else {
                    let mut guard = routing.lock().await;
                    guard.pending.entry(sid).or_default().push(frame);
                }
            }
            data_streams.lock().await.senders.remove(&stream_id);
        });
    }

    async fn ensure_client_data_stream(self: &Arc<Self>, stream_id: u32) -> Result<(), Error> {
        if self
            .data_streams
            .lock()
            .await
            .senders
            .contains_key(&stream_id)
        {
            return Ok(());
        }
        let requester = self.requester.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::NotConnected,
                "h2 server role cannot open outbound data streams",
            )
        })?;
        let mut client = requester.lock().await;
        let request = Request::builder()
            .method("POST")
            .uri(format!(
                "http://{}{}",
                self.endpoint_authority, self.endpoint_path
            ))
            .header(header::CONTENT_TYPE, DATA_STREAM_CT)
            .header(FUSION_STREAM_ID, stream_id.to_string())
            .body(())
            .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;
        let (response_fut, send) = client
            .send_request(request, false)
            .map_err(|err| Error::new(ErrorKind::ConnectionAborted, err.to_string()))?;
        let response = response_fut
            .await
            .map_err(|err| Error::new(ErrorKind::ConnectionAborted, err.to_string()))?;
        if response.status() != StatusCode::OK {
            return Err(Error::new(
                ErrorKind::ConnectionAborted,
                format!("h2 data stream rejected with status {}", response.status()),
            ));
        }
        let recv = response.into_body();
        self.register_data_stream(stream_id, send, recv).await;
        Ok(())
    }

    async fn send_data_frame(self: &Arc<Self>, stream_id: u32, frame: &Frame) -> Result<(), Error> {
        for _ in 0..50 {
            {
                let mut registry = self.data_streams.lock().await;
                if let Some(send) = registry.senders.get_mut(&stream_id) {
                    return write_h2_frame(send, frame, self.shared_key.as_ref()).await;
                }
            }
            if self.requester.is_some() {
                if self.ensure_client_data_stream(stream_id).await.is_err() {
                    return Err(Error::new(
                        ErrorKind::ConnectionAborted,
                        "failed to open h2 data stream",
                    ));
                }
                continue;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        Err(Error::new(
            ErrorKind::TimedOut,
            "h2 data stream not ready",
        ))
    }
}

#[derive(Clone)]
pub struct MuxH2Peer {
    pub session: PeerSession,
    pub peer_addr: SocketAddr,
    control_writer: Arc<Mutex<SendStream<Bytes>>>,
    state: Arc<SharedH2MuxState>,
    opens: Arc<Mutex<mpsc::Receiver<Frame>>>,
    controls: Arc<Mutex<mpsc::Receiver<Frame>>>,
}

impl MuxH2Peer {
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        if frame_uses_data_stream(frame) {
            let stream_id = frame.header.stream_id.ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidData,
                    "StreamData missing stream_id on h2 mux",
                )
            })?;
            // Client opens outbound data streams; server responses stay on control
            // so relay return paths match ws_mux single-channel semantics.
            if self.state.requester.is_some()
                && self
                    .state
                    .send_data_frame(stream_id, frame)
                    .await
                    .is_ok()
            {
                return Ok(());
            }
            let mut writer = self.control_writer.lock().await;
            return write_h2_frame(&mut writer, frame, self.state.shared_key.as_ref()).await;
        }

        let mut writer = self.control_writer.lock().await;
        write_h2_frame(&mut writer, frame, self.state.shared_key.as_ref()).await?;
        if matches!(frame.message, Message::StreamClose(_)) {
            if let Some(stream_id) = frame.header.stream_id {
                self.state.data_streams.lock().await.senders.remove(&stream_id);
            }
        }
        Ok(())
    }

    pub async fn open_stream_receiver(&self, stream_id: u32) -> mpsc::Receiver<Frame> {
        let (tx, rx) = mpsc::channel(64);
        let pending = {
            let mut routing = self.state.routing.lock().await;
            routing.senders.insert(stream_id, tx.clone());
            routing.pending.remove(&stream_id).unwrap_or_default()
        };
        for frame in pending {
            if tx.send(frame).await.is_err() {
                break;
            }
        }
        rx
    }

    pub async fn read_control_frame(&self) -> Result<Frame, Error> {
        let mut controls = self.controls.lock().await;
        controls
            .recv()
            .await
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "control frame channel closed"))
    }

    pub async fn read_stream_open_frame(&self) -> Result<Frame, Error> {
        let mut opens = self.opens.lock().await;
        opens
            .recv()
            .await
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "stream open channel closed"))
    }

    pub async fn read_stream_open(&self) -> Result<(u32, StreamOpenMessage), Error> {
        let frame = self.read_stream_open_frame().await?;
        let stream_id = frame.header.stream_id.ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "missing stream_id on inbound StreamOpen",
            )
        })?;
        match frame.message {
            Message::StreamOpen(open) => Ok((stream_id, open)),
            other => Err(Error::new(
                ErrorKind::InvalidData,
                format!("expected StreamOpen, got {:?}", other),
            )),
        }
    }
}

async fn route_inbound_mux_frame(state: &Arc<SharedH2MuxState>, frame: Frame) {
    let Some(stream_id) = frame.header.stream_id else {
        let _ = state.control_tx.send(frame).await;
        return;
    };

    if matches!(frame.message, Message::StreamOpen(_)) {
        let _ = state.open_tx.send(frame).await;
        return;
    }

    if matches!(frame.message, Message::StreamClose(_)) {
        state.data_streams.lock().await.senders.remove(&stream_id);
    }

    let sender = {
        let guard = state.routing.lock().await;
        guard.senders.get(&stream_id).cloned()
    };
    if let Some(tx) = sender {
        if tx.send(frame).await.is_err() {
            let mut guard = state.routing.lock().await;
            guard.senders.remove(&stream_id);
        }
    } else {
        let mut guard = state.routing.lock().await;
        guard.pending.entry(stream_id).or_default().push(frame);
    }
}

async fn spawn_control_dispatch_loop(
    mut recv: RecvStream,
    state: Arc<SharedH2MuxState>,
) {
    let shared_key = state.shared_key.clone();
    tokio::spawn(async move {
        let mut reader = FrameReader::new();
        loop {
            let frame = match read_h2_frame(&mut reader, &mut recv, shared_key.as_ref()).await {
                Ok(frame) => frame,
                Err(_) => break,
            };
            route_inbound_mux_frame(&state, frame).await;
        }
    });
}

fn spawn_server_accept_loop<S>(mut connection: server::Connection<S, Bytes>, state: Arc<SharedH2MuxState>)
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        while let Some(result) = connection.accept().await {
            let Ok((request, mut respond)) = result else {
                continue;
            };
            let (parts, recv) = request.into_parts();
            if !is_data_stream_request(&parts.headers) {
                let _ = respond.send_response(
                    Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(())
                        .unwrap(),
                    true,
                );
                continue;
            }
            let Ok(stream_id) = parse_fusion_stream_id(&parts.headers) else {
                let _ = respond.send_response(
                    Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(())
                        .unwrap(),
                    true,
                );
                continue;
            };
            let Ok(send) = respond.send_response(
                Response::builder()
                    .status(StatusCode::OK)
                    .body(())
                    .unwrap(),
                false,
            ) else {
                continue;
            };
            state
                .register_data_stream(stream_id, send, recv)
                .await;
        }
    });
}

fn tunnel_path(parsed: &ParsedUrl) -> String {
    if parsed.path.is_empty() || parsed.path == "/" {
        "/tunnel".to_string()
    } else {
        parsed.path.clone()
    }
}

fn tunnel_authority(parsed: &ParsedUrl) -> Result<String, Error> {
    let host = parsed
        .host
        .as_deref()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "missing host for h2 connect"))?;
    let port = parsed
        .port
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "missing port for h2 connect"))?;
    Ok(format!("{host}:{port}"))
}

async fn client_h2_control_stream(
    endpoint: &str,
) -> Result<(client::SendRequest<Bytes>, SendStream<Bytes>, RecvStream, ParsedUrl, SocketAddr), Error>
{
    let parsed = ParsedUrl::parse(endpoint)?;
    let tcp = connect_tcp_for_url(&parsed).await?;
    let peer_addr = tcp.peer_addr()?;

    if let Some(connector) = build_h2_tls_connector(&parsed)? {
        let host = parsed
            .host
            .clone()
            .unwrap_or_else(|| "localhost".to_string());
        let server_name = rustls::pki_types::ServerName::try_from(host).map_err(|_| {
            Error::new(ErrorKind::InvalidInput, "invalid h2s server name")
        })?;
        let tls = connector.connect(server_name, tcp).await.map_err(|err| {
            Error::new(
                ErrorKind::ConnectionAborted,
                format!("h2s tls connect failed: {err}"),
            )
        })?;
        client_h2_control_stream_on_io(tls, parsed, peer_addr).await
    } else {
        client_h2_control_stream_on_io(tcp, parsed, peer_addr).await
    }
}

async fn client_h2_control_stream_on_io<S>(
    stream: S,
    parsed: ParsedUrl,
    peer_addr: SocketAddr,
) -> Result<(client::SendRequest<Bytes>, SendStream<Bytes>, RecvStream, ParsedUrl, SocketAddr), Error>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut client, connection) = client::handshake(stream)
        .await
        .map_err(|err| Error::new(ErrorKind::ConnectionAborted, err.to_string()))?;
    tokio::spawn(async move {
        connection.await.ok();
    });

    let path = tunnel_path(&parsed);
    let authority = tunnel_authority(&parsed)?;
    let request = Request::builder()
        .method("POST")
        .uri(format!("http://{authority}{path}"))
        .header(header::CONTENT_TYPE, CONTROL_STREAM_CT)
        .body(())
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;

    let (response_fut, send_stream) = client
        .send_request(request, false)
        .map_err(|err| Error::new(ErrorKind::ConnectionAborted, err.to_string()))?;
    let response = response_fut
        .await
        .map_err(|err| Error::new(ErrorKind::ConnectionAborted, err.to_string()))?;
    if response.status() != StatusCode::OK {
        return Err(Error::new(
            ErrorKind::ConnectionAborted,
            format!("h2 tunnel rejected with status {}", response.status()),
        ));
    }
    let recv_stream = response.into_body();
    Ok((client, send_stream, recv_stream, parsed, peer_addr))
}

async fn server_h2_control_stream<S>(
    stream: S,
) -> Result<(SendStream<Bytes>, RecvStream, server::Connection<S, Bytes>), Error>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let mut connection = server::Builder::new()
        .handshake(stream)
        .await
        .map_err(|err| Error::new(ErrorKind::ConnectionAborted, err.to_string()))?;

    let accept = connection
        .accept()
        .await
        .ok_or_else(|| Error::new(ErrorKind::ConnectionAborted, "h2 connection closed"))?
        .map_err(|err| Error::new(ErrorKind::ConnectionAborted, err.to_string()))?;

    let (request, mut respond) = accept;
    let (parts, recv_stream) = request.into_parts();
    if parts.headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok())
        != Some(CONTROL_STREAM_CT)
    {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "first h2 stream must be fusion control stream",
        ));
    }

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(())
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;
    let send_stream = respond
        .send_response(response, false)
        .map_err(|err| Error::new(ErrorKind::ConnectionAborted, err.to_string()))?;

    Ok((send_stream, recv_stream, connection))
}

fn build_mux_state(
    shared_key: Option<SharedKey>,
    requester: Option<Arc<Mutex<client::SendRequest<Bytes>>>>,
    endpoint_authority: String,
    endpoint_path: String,
) -> (Arc<SharedH2MuxState>, Arc<Mutex<mpsc::Receiver<Frame>>>, Arc<Mutex<mpsc::Receiver<Frame>>>) {
    let (open_tx, open_rx) = mpsc::channel(64);
    let (control_tx, control_rx) = mpsc::channel(64);
    let state = Arc::new(SharedH2MuxState {
        routing: Arc::new(Mutex::new(StreamRoutingState::default())),
        data_streams: Arc::new(Mutex::new(DataStreamRegistry::default())),
        shared_key,
        open_tx,
        control_tx,
        requester,
        endpoint_authority,
        endpoint_path,
    });
    (
        state,
        Arc::new(Mutex::new(open_rx)),
        Arc::new(Mutex::new(control_rx)),
    )
}

pub async fn bind(endpoint: &str) -> Result<TcpListener, Error> {
    let parsed = ParsedUrl::parse(endpoint)?;
    let host = parsed.host.as_deref().unwrap_or("0.0.0.0");
    let port = parsed.port.unwrap_or(0);
    TcpListener::bind(format!("{host}:{port}")).await
}

pub async fn accept_mux_peer(
    identity: AgentIdentity,
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<MuxH2Peer, Error> {
    accept_mux_peer_on(identity, &listener, tls_acceptor).await
}

async fn finish_server_mux_peer<S>(
    identity: AgentIdentity,
    addr: SocketAddr,
    mut send_stream: SendStream<Bytes>,
    mut recv_stream: RecvStream,
    connection: server::Connection<S, Bytes>,
) -> Result<MuxH2Peer, Error>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let (state, opens, controls) = build_mux_state(
        shared_key.clone(),
        None,
        String::new(),
        String::new(),
    );
    spawn_server_accept_loop(connection, state.clone());

    let mut reader = FrameReader::new();
    let hello = read_h2_frame(&mut reader, &mut recv_stream, shared_key.as_ref()).await?;
    let session = complete_session(&identity, &hello)?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    write_h2_frame(&mut send_stream, &ack, shared_key.as_ref()).await?;

    let heartbeat = read_h2_frame(&mut reader, &mut recv_stream, shared_key.as_ref()).await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }
    let heartbeat_ack = heartbeat_frame(identity.id.clone(), Some(session.remote.agent_id.clone()));
    write_h2_frame(&mut send_stream, &heartbeat_ack, shared_key.as_ref()).await?;

    spawn_control_dispatch_loop(recv_stream, state.clone()).await;

    Ok(MuxH2Peer {
        session,
        peer_addr: addr,
        control_writer: Arc::new(Mutex::new(send_stream)),
        state,
        opens,
        controls,
    })
}

pub async fn accept_mux_peer_on(
    identity: AgentIdentity,
    listener: &TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<MuxH2Peer, Error> {
    let (tcp, addr) = listener.accept().await?;
    if let Some(acceptor) = tls_acceptor {
        let tls = acceptor.accept(tcp).await.map_err(|err| {
            Error::new(
                ErrorKind::ConnectionAborted,
                format!("h2s tls accept failed: {err}"),
            )
        })?;
        let (send_stream, recv_stream, connection) = server_h2_control_stream(tls).await?;
        finish_server_mux_peer(identity, addr, send_stream, recv_stream, connection).await
    } else {
        let (send_stream, recv_stream, connection) = server_h2_control_stream(tcp).await?;
        finish_server_mux_peer(identity, addr, send_stream, recv_stream, connection).await
    }
}

pub async fn connect_mux_peer(identity: AgentIdentity, endpoint: &str) -> Result<MuxH2Peer, Error> {
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let (client, mut send_stream, mut recv_stream, parsed, peer_addr) =
        client_h2_control_stream(endpoint).await?;

    let hello = hello_frame(&identity);
    write_h2_frame(&mut send_stream, &hello, shared_key.as_ref()).await?;
    let mut reader = FrameReader::new();
    let ack = read_h2_frame(&mut reader, &mut recv_stream, shared_key.as_ref()).await?;
    match &ack.message {
        Message::HelloAck(msg) if msg.accepted => {}
        _ => {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "peer rejected hello handshake",
            ))
        }
    }

    let session = PeerSession {
        local: PeerInfo {
            agent_id: identity.id.clone(),
            agent_name: identity.name.clone(),
            capabilities: identity.capability_labels(),
        },
        remote: PeerInfo {
            agent_id: ack.header.src_agent.clone().unwrap_or_default(),
            agent_name: ack.header.src_agent.clone().unwrap_or_default(),
            capabilities: vec!["transport:h2".to_string()],
        },
        state: SessionState::Active,
    };

    let heartbeat = heartbeat_frame(identity.id.clone(), ack.header.src_agent.clone());
    write_h2_frame(&mut send_stream, &heartbeat, shared_key.as_ref()).await?;
    let heartbeat_ack = read_h2_frame(&mut reader, &mut recv_stream, shared_key.as_ref()).await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }

    let authority = tunnel_authority(&parsed)?;
    let path = tunnel_path(&parsed);
    let requester = Arc::new(Mutex::new(client));
    let (state, opens, controls) = build_mux_state(
        shared_key.clone(),
        Some(requester),
        authority,
        path,
    );
    spawn_control_dispatch_loop(recv_stream, state.clone()).await;

    Ok(MuxH2Peer {
        session,
        peer_addr,
        control_writer: Arc::new(Mutex::new(send_stream)),
        state,
        opens,
        controls,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, StreamDataMessage, StreamOpenMessage},
        },
        tunnel::tls::build_h2_tls_acceptor,
        utils::url::ParsedUrl,
    };

    use rcgen::generate_simple_self_signed;

    use super::{accept_mux_peer_on, bind, connect_mux_peer};

    fn temp_pem_path(stem: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fusion-{stem}-{nanos}.pem"))
    }

    fn write_self_signed_cert() -> (std::path::PathBuf, std::path::PathBuf) {
        let cert = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_path = temp_pem_path("h2s-cert");
        let key_path = temp_pem_path("h2s-key");
        fs::write(&cert_path, cert.serialize_pem().unwrap()).unwrap();
        fs::write(&key_path, cert.serialize_private_key_pem()).unwrap();
        (cert_path, key_path)
    }

    #[tokio::test]
    async fn h2_server_to_client_stream_data_delivery() {
        let listener = bind("h2://127.0.0.1:0/tunnel").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-s2c-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-s2c-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer_on(server_identity, &listener, None)
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let data = Frame::new(
                MessageType::StreamData,
                Some(peer.session.local.agent_id.clone()),
                Some(peer.session.remote.agent_id.clone()),
                Message::StreamData(StreamDataMessage::from_bytes(b"server-push")),
            )
            .with_stream_id(19);
            peer.send_frame(&data).await.unwrap();
        });

        let client = connect_mux_peer(client_identity, &format!("h2://{addr}/tunnel"))
            .await
            .unwrap();
        let mut rx = client.open_stream_receiver(19).await;
        let frame = rx.recv().await.unwrap();
        match frame.message {
            Message::StreamData(msg) => assert_eq!(msg.to_bytes().unwrap(), b"server-push"),
            other => panic!("expected StreamData, got {:?}", other),
        }
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn h2_cross_peer_stream_open_on_control() {
        let listener = bind("h2://127.0.0.1:0/tunnel").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-open-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-open-client".into()),
            key: None,
        });

        let server = tokio::spawn(async move {
            let peer = accept_mux_peer_on(server_identity, &listener, None)
                .await
                .unwrap();
            peer.read_stream_open().await.unwrap()
        });

        let client = connect_mux_peer(client_identity, &format!("h2://{addr}/tunnel"))
            .await
            .unwrap();
        let open = Frame::new(
            MessageType::StreamOpen,
            Some(client.session.local.agent_id.clone()),
            Some(client.session.remote.agent_id.clone()),
            Message::StreamOpen(StreamOpenMessage {
                service: "raw".into(),
                target_host: None,
                target_port: None,
            }),
        )
        .with_stream_id(5);
        client.send_frame(&open).await.unwrap();
        let (stream_id, _) = server.await.unwrap();
        assert_eq!(stream_id, 5);
    }

    #[tokio::test]
    async fn h2_session_hello_heartbeat_roundtrip() {
        let listener = bind("h2://127.0.0.1:0/tunnel").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-client".into()),
            key: None,
        });

        let server = tokio::spawn(async move {
            accept_mux_peer_on(server_identity, &listener, None)
                .await
                .unwrap()
        });

        let client = connect_mux_peer(client_identity, &format!("h2://{addr}/tunnel"))
            .await
            .unwrap();
        let server = server.await.unwrap();
        assert_eq!(client.session.remote.agent_id, server.session.local.agent_id);
        assert_eq!(server.session.remote.agent_id, client.session.local.agent_id);
    }

    #[tokio::test]
    async fn h2_mux_routes_frames_by_stream_id() {
        let listener = bind("h2://127.0.0.1:0/tunnel").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-mux-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-mux-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer_on(server_identity, &listener, None)
                .await
                .unwrap();
            let (stream_id, _) = peer.read_stream_open().await.unwrap();
            let mut rx = peer.open_stream_receiver(stream_id).await;
            rx.recv().await.unwrap()
        });

        let client = connect_mux_peer(client_identity.clone(), &format!("h2://{addr}/tunnel"))
            .await
            .unwrap();
        let stream_id = 42_u32;
        let open = Frame::new(
            MessageType::StreamOpen,
            Some(client.session.local.agent_id.clone()),
            Some(client.session.remote.agent_id.clone()),
            Message::StreamOpen(StreamOpenMessage {
                service: "raw".into(),
                target_host: Some("127.0.0.1".into()),
                target_port: Some(80),
            }),
        )
        .with_stream_id(stream_id);
        client.send_frame(&open).await.unwrap();

        let data = Frame::new(
            MessageType::StreamData,
            Some(client.session.local.agent_id.clone()),
            Some(client.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"h2-mux")),
        )
        .with_stream_id(stream_id);
        client.send_frame(&data).await.unwrap();

        let received = server_task.await.unwrap();
        match received.message {
            Message::StreamData(msg) => assert_eq!(msg.to_bytes().unwrap(), b"h2-mux"),
            other => panic!("expected StreamData, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn h2_mux_uses_separate_data_streams_for_concurrent_ids() {
        let listener = bind("h2://127.0.0.1:0/tunnel").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-mux-multi-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-mux-multi-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer_on(server_identity, &listener, None)
                .await
                .unwrap();
            let (id1, _) = peer.read_stream_open().await.unwrap();
            let (id2, _) = peer.read_stream_open().await.unwrap();
            let mut rx1 = peer.open_stream_receiver(id1).await;
            let mut rx2 = peer.open_stream_receiver(id2).await;
            let a = tokio::spawn(async move { rx1.recv().await.unwrap() });
            let b = tokio::spawn(async move { rx2.recv().await.unwrap() });
            (a.await.unwrap(), b.await.unwrap())
        });

        let client = connect_mux_peer(client_identity, &format!("h2://{addr}/tunnel"))
            .await
            .unwrap();
        for (stream_id, payload) in [(11_u32, b"one"), (12_u32, b"two")] {
            let open = Frame::new(
                MessageType::StreamOpen,
                Some(client.session.local.agent_id.clone()),
                Some(client.session.remote.agent_id.clone()),
                Message::StreamOpen(StreamOpenMessage {
                    service: "raw".into(),
                    target_host: None,
                    target_port: None,
                }),
            )
            .with_stream_id(stream_id);
            client.send_frame(&open).await.unwrap();
            let data = Frame::new(
                MessageType::StreamData,
                Some(client.session.local.agent_id.clone()),
                Some(client.session.remote.agent_id.clone()),
                Message::StreamData(StreamDataMessage::from_bytes(payload)),
            )
            .with_stream_id(stream_id);
            client.send_frame(&data).await.unwrap();
        }

        let (f1, f2) = server_task.await.unwrap();
        assert_eq!(f1.header.stream_id, Some(11));
        assert_eq!(f2.header.stream_id, Some(12));
    }

    #[tokio::test]
    async fn h2_mux_buffers_until_receiver_opened() {
        let listener = bind("h2://127.0.0.1:0/tunnel").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-buffer-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-buffer-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer_on(server_identity, &listener, None)
                .await
                .unwrap();
            let (stream_id, _) = peer.read_stream_open().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let mut rx = peer.open_stream_receiver(stream_id).await;
            rx.recv().await.unwrap()
        });

        let client = connect_mux_peer(client_identity, &format!("h2://{addr}/tunnel"))
            .await
            .unwrap();
        let stream_id = 7_u32;
        let open = Frame::new(
            MessageType::StreamOpen,
            Some(client.session.local.agent_id.clone()),
            Some(client.session.remote.agent_id.clone()),
            Message::StreamOpen(StreamOpenMessage {
                service: "raw".into(),
                target_host: None,
                target_port: None,
            }),
        )
        .with_stream_id(stream_id);
        client.send_frame(&open).await.unwrap();

        let data = Frame::new(
            MessageType::StreamData,
            Some(client.session.local.agent_id.clone()),
            Some(client.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"buffered")),
        )
        .with_stream_id(stream_id);
        client.send_frame(&data).await.unwrap();

        let received = server_task.await.unwrap();
        match received.message {
            Message::StreamData(msg) => assert_eq!(msg.to_bytes().unwrap(), b"buffered"),
            other => panic!("expected StreamData, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn h2s_handshake_and_stream_roundtrip_with_insecure_client() {
        let listener = bind("h2://127.0.0.1:0/tunnel").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (cert_path, key_path) = write_self_signed_cert();
        let acceptor = build_h2_tls_acceptor(
            &ParsedUrl::parse(&format!(
                "h2s://127.0.0.1:{}/tunnel?tls-cert={}&tls-key={}",
                addr.port(),
                cert_path.display(),
                key_path.display()
            ))
            .unwrap(),
        )
        .unwrap()
        .expect("h2s acceptor");

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2s-mux-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2s-mux-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer_on(server_identity, &listener, Some(acceptor))
                .await
                .unwrap();
            let mut rx = peer.open_stream_receiver(42).await;
            rx.recv().await.unwrap()
        });

        let peer = connect_mux_peer(
            client_identity,
            &format!("h2s://localhost:{}/tunnel?tls-insecure=1", addr.port()),
        )
        .await
        .unwrap();

        let data = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"h2s-secure")),
        )
        .with_stream_id(42);
        peer.send_frame(&data).await.unwrap();

        let frame = server_task.await.unwrap();
        match frame.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"h2s-secure"),
            _ => panic!("expected StreamData"),
        }

        let _ = fs::remove_file(cert_path);
        let _ = fs::remove_file(key_path);
    }

    #[tokio::test]
    async fn h2s_handshake_with_mutual_tls() {
        let listener = bind("h2://127.0.0.1:0/tunnel").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (server_cert_path, server_key_path) = write_self_signed_cert();
        let (client_cert_path, client_key_path) = write_self_signed_cert();
        let acceptor = build_h2_tls_acceptor(
            &ParsedUrl::parse(&format!(
                "h2s://127.0.0.1:{}/tunnel?tls-cert={}&tls-key={}&tls-client-ca={}",
                addr.port(),
                server_cert_path.display(),
                server_key_path.display(),
                client_cert_path.display(),
            ))
            .unwrap(),
        )
        .unwrap()
        .expect("h2s acceptor");

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2s-mtls-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2s-mtls-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer_on(server_identity, &listener, Some(acceptor))
                .await
                .unwrap();
            let mut rx = peer.open_stream_receiver(77).await;
            rx.recv().await.unwrap()
        });

        let peer = connect_mux_peer(
            client_identity,
            &format!(
                "h2s://localhost:{}/tunnel?tls-ca={}&tls-client-cert={}&tls-client-key={}",
                addr.port(),
                server_cert_path.display(),
                client_cert_path.display(),
                client_key_path.display(),
            ),
        )
        .await
        .unwrap();

        let data = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"h2s-mutual")),
        )
        .with_stream_id(77);
        peer.send_frame(&data).await.unwrap();

        let frame = server_task.await.unwrap();
        match frame.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"h2s-mutual"),
            _ => panic!("expected StreamData"),
        }

        let _ = fs::remove_file(server_cert_path);
        let _ = fs::remove_file(server_key_path);
        let _ = fs::remove_file(client_cert_path);
        let _ = fs::remove_file(client_key_path);
    }
}
