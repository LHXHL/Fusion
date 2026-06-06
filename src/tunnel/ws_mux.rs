use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
};

use futures_util::{
    sink::Sink,
    stream::{SplitSink, SplitStream, Stream},
    SinkExt, StreamExt,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    accept_async, connect_async, connect_async_tls_with_config,
    tungstenite::{self, Message as WsMessage},
    Connector, MaybeTlsStream, WebSocketStream,
};

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
    tunnel::tls::build_ws_tls_connector,
    utils::url::ParsedUrl,
};

type BoxWsSink = Pin<Box<dyn Sink<WsMessage, Error = tungstenite::Error> + Send>>;
type BoxWsStream = Pin<Box<dyn Stream<Item = Result<WsMessage, tungstenite::Error>> + Send>>;

#[derive(Debug, Default)]
struct StreamRoutingState {
    senders: HashMap<u32, mpsc::Sender<Frame>>,
    pending: HashMap<u32, Vec<Frame>>,
}

#[derive(Clone)]
pub struct MuxWsPeer {
    pub session: PeerSession,
    pub peer_addr: SocketAddr,
    writer: Arc<Mutex<BoxWsSink>>,
    routing: Arc<Mutex<StreamRoutingState>>,
    opens: Arc<Mutex<mpsc::Receiver<Frame>>>,
    controls: Arc<Mutex<mpsc::Receiver<Frame>>>,
    shared_key: Option<SharedKey>,
}

impl MuxWsPeer {
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        let payload = encode_transport_frame(frame, self.shared_key.as_ref())?;
        let mut writer = self.writer.lock().await;
        writer
            .send(WsMessage::Binary(payload.into()))
            .await
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))
    }

    pub async fn open_stream_receiver(&self, stream_id: u32) -> mpsc::Receiver<Frame> {
        let (tx, rx) = mpsc::channel(64);
        let pending = {
            let mut routing = self.routing.lock().await;
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

async fn read_boxed_frame(
    stream: &mut BoxWsStream,
    shared_key: Option<&SharedKey>,
) -> Result<Frame, Error> {
    let msg = stream
        .next()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "websocket closed before frame"))?
        .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?;

    let bytes = match msg {
        WsMessage::Binary(bytes) => bytes.to_vec(),
        WsMessage::Text(text) => text.as_str().as_bytes().to_vec(),
        WsMessage::Close(_) => {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "received websocket close during frame read",
            ));
        }
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unsupported websocket message type",
            ));
        }
    };

    decode_transport_frame(&bytes, shared_key)
}

async fn spawn_dispatch_loop(
    mut reader: BoxWsStream,
    shared_key: Option<SharedKey>,
    routing: Arc<Mutex<StreamRoutingState>>,
    open_tx: mpsc::Sender<Frame>,
    control_tx: mpsc::Sender<Frame>,
) {
    tokio::spawn(async move {
        loop {
            let frame = match read_boxed_frame(&mut reader, shared_key.as_ref()).await {
                Ok(frame) => frame,
                Err(_) => break,
            };
            let Some(stream_id) = frame.header.stream_id else {
                if control_tx.send(frame).await.is_err() {
                    break;
                }
                continue;
            };

            if matches!(frame.message, Message::StreamOpen(_)) {
                if open_tx.send(frame).await.is_err() {
                    break;
                }
                continue;
            }

            let sender = {
                let guard = routing.lock().await;
                guard.senders.get(&stream_id).cloned()
            };
            if let Some(tx) = sender {
                if tx.send(frame).await.is_err() {
                    let mut guard = routing.lock().await;
                    guard.senders.remove(&stream_id);
                }
            } else {
                let mut guard = routing.lock().await;
                guard.pending.entry(stream_id).or_default().push(frame);
            }
        }
    });
}

fn split_boxed<S>(ws: WebSocketStream<S>) -> (BoxWsSink, BoxWsStream)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let (writer, reader): (SplitSink<_, _>, SplitStream<_>) = ws.split();
    (Box::pin(writer), Box::pin(reader))
}

pub async fn bind(endpoint: &str) -> Result<TcpListener, Error> {
    TcpListener::bind(endpoint).await
}

pub async fn accept_mux_peer(
    identity: AgentIdentity,
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<MuxWsPeer, Error> {
    accept_mux_peer_on(identity, &listener, tls_acceptor).await
}

pub async fn accept_mux_peer_on(
    identity: AgentIdentity,
    listener: &TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<MuxWsPeer, Error> {
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let (stream, addr) = listener.accept().await?;
    match tls_acceptor {
        Some(acceptor) => {
            let tls_stream = acceptor.accept(stream).await.map_err(|e| {
                Error::new(
                    ErrorKind::ConnectionAborted,
                    format!("tls accept failed: {e}"),
                )
            })?;
            let ws_stream = accept_async(tls_stream)
                .await
                .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e.to_string()))?;
            finish_accept_mux_peer(identity, shared_key, ws_stream, addr).await
        }
        None => {
            let ws_stream = accept_async(stream)
                .await
                .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e.to_string()))?;
            finish_accept_mux_peer(identity, shared_key, ws_stream, addr).await
        }
    }
}

pub async fn connect_mux_peer(identity: AgentIdentity, endpoint: &str) -> Result<MuxWsPeer, Error> {
    let parsed = ParsedUrl::parse(endpoint)?;
    let connector = build_ws_tls_connector(&parsed)?;
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let (mut ws_stream, _) = connect_ws(endpoint, connector)
        .await
        .map_err(|e| Error::new(ErrorKind::ConnectionRefused, e.to_string()))?;
    let peer_addr = peer_addr_from_client_ws(&ws_stream)?;

    let hello = hello_frame(&identity);
    write_ws_frame(&mut ws_stream, &hello, shared_key.as_ref()).await?;
    let ack = read_ws_frame(&mut ws_stream, shared_key.as_ref()).await?;
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
            capabilities: vec!["transport:ws".to_string()],
        },
        state: SessionState::Active,
    };

    let heartbeat = heartbeat_frame(identity.id.clone(), ack.header.src_agent.clone());
    write_ws_frame(&mut ws_stream, &heartbeat, shared_key.as_ref()).await?;
    let heartbeat_ack = read_ws_frame(&mut ws_stream, shared_key.as_ref()).await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }

    let (writer, reader) = split_boxed(ws_stream);
    let routing = Arc::new(Mutex::new(StreamRoutingState::default()));
    let (open_tx, open_rx) = mpsc::channel(64);
    let (control_tx, control_rx) = mpsc::channel(64);
    spawn_dispatch_loop(
        reader,
        shared_key.clone(),
        routing.clone(),
        open_tx,
        control_tx,
    )
    .await;

    Ok(MuxWsPeer {
        session,
        peer_addr,
        writer: Arc::new(Mutex::new(writer)),
        routing,
        opens: Arc::new(Mutex::new(open_rx)),
        controls: Arc::new(Mutex::new(control_rx)),
        shared_key,
    })
}

async fn write_ws_frame<S>(
    stream: &mut WebSocketStream<S>,
    frame: &Frame,
    shared_key: Option<&SharedKey>,
) -> Result<(), Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let payload = encode_transport_frame(frame, shared_key)?;
    stream
        .send(WsMessage::Binary(payload.into()))
        .await
        .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))
}

async fn read_ws_frame<S>(
    stream: &mut WebSocketStream<S>,
    shared_key: Option<&SharedKey>,
) -> Result<Frame, Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let msg = stream
        .next()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "websocket closed before frame"))?
        .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?;

    let bytes = match msg {
        WsMessage::Binary(bytes) => bytes.to_vec(),
        WsMessage::Text(text) => text.as_str().as_bytes().to_vec(),
        WsMessage::Close(_) => {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "received websocket close during frame read",
            ));
        }
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unsupported websocket message type",
            ));
        }
    };

    decode_transport_frame(&bytes, shared_key)
}

fn peer_addr_from_client_ws(
    ws: &WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<SocketAddr, Error> {
    match ws.get_ref() {
        MaybeTlsStream::Plain(stream) => stream.peer_addr(),
        MaybeTlsStream::NativeTls(stream) => stream.get_ref().get_ref().get_ref().peer_addr(),
        _ => Err(Error::new(
            ErrorKind::Unsupported,
            "unsupported websocket tls transport for peer_addr lookup",
        )),
    }
}

async fn connect_ws(
    endpoint: &str,
    connector: Option<Connector>,
) -> Result<
    (
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        tungstenite::handshake::client::Response,
    ),
    tungstenite::Error,
> {
    if let Some(connector) = connector {
        connect_async_tls_with_config(endpoint, None, false, Some(connector)).await
    } else {
        connect_async(endpoint).await
    }
}

async fn finish_accept_mux_peer<S>(
    identity: AgentIdentity,
    shared_key: Option<SharedKey>,
    mut ws_stream: WebSocketStream<S>,
    addr: SocketAddr,
) -> Result<MuxWsPeer, Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let hello = read_ws_frame(&mut ws_stream, shared_key.as_ref()).await?;
    let session = complete_session(&identity, &hello)?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    write_ws_frame(&mut ws_stream, &ack, shared_key.as_ref()).await?;

    let heartbeat = read_ws_frame(&mut ws_stream, shared_key.as_ref()).await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }
    let heartbeat_ack = heartbeat_frame(identity.id.clone(), Some(session.remote.agent_id.clone()));
    write_ws_frame(&mut ws_stream, &heartbeat_ack, shared_key.as_ref()).await?;

    let (writer, reader) = split_boxed(ws_stream);
    let routing = Arc::new(Mutex::new(StreamRoutingState::default()));
    let (open_tx, open_rx) = mpsc::channel(64);
    let (control_tx, control_rx) = mpsc::channel(64);
    spawn_dispatch_loop(
        reader,
        shared_key.clone(),
        routing.clone(),
        open_tx,
        control_tx,
    )
    .await;

    Ok(MuxWsPeer {
        session,
        peer_addr: addr,
        writer: Arc::new(Mutex::new(writer)),
        routing,
        opens: Arc::new(Mutex::new(open_rx)),
        controls: Arc::new(Mutex::new(control_rx)),
        shared_key,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use rcgen::generate_simple_self_signed;

    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, StreamDataMessage, StreamOpenMessage},
        },
        tunnel::tls::build_ws_tls_acceptor,
        tunnel::ws_mux::{accept_mux_peer, bind, connect_mux_peer},
        utils::url::ParsedUrl,
    };

    fn temp_pem_path(stem: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fusion-{stem}-{nanos}.pem"))
    }

    fn write_self_signed_cert() -> (std::path::PathBuf, std::path::PathBuf) {
        let cert = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_path = temp_pem_path("wss-cert");
        let key_path = temp_pem_path("wss-key");
        fs::write(&cert_path, cert.serialize_pem().unwrap()).unwrap();
        fs::write(&key_path, cert.serialize_private_key_pem()).unwrap();
        (cert_path, key_path)
    }

    #[tokio::test]
    async fn ws_mux_routes_frames_by_stream_id() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-mux-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-mux-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer(server_identity, listener, None)
                .await
                .unwrap();
            let mut rx1 = peer.open_stream_receiver(11).await;
            let mut rx2 = peer.open_stream_receiver(12).await;
            let a = tokio::spawn(async move { rx1.recv().await.unwrap() });
            let b = tokio::spawn(async move { rx2.recv().await.unwrap() });
            (a.await.unwrap(), b.await.unwrap())
        });

        let peer = connect_mux_peer(client_identity, &format!("ws://{}/tunnel", addr))
            .await
            .unwrap();
        let f1 = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"left")),
        )
        .with_stream_id(11);
        let f2 = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"right")),
        )
        .with_stream_id(12);
        peer.send_frame(&f1).await.unwrap();
        peer.send_frame(&f2).await.unwrap();

        let (r1, r2) = server_task.await.unwrap();
        match r1.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"left"),
            _ => panic!(),
        }
        match r2.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"right"),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn ws_mux_buffers_stream_frames_until_receiver_is_opened() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-mux-buffer-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-mux-buffer-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer(server_identity, listener, None)
                .await
                .unwrap();
            let (stream_id, open) = peer.read_stream_open().await.unwrap();
            assert_eq!(stream_id, 21);
            assert_eq!(open.target_host.as_deref(), Some("buffer.ws"));
            let mut rx = peer.open_stream_receiver(stream_id).await;
            rx.recv().await.unwrap()
        });

        let peer = connect_mux_peer(client_identity, &format!("ws://{}/tunnel", addr))
            .await
            .unwrap();
        let open = Frame::new(
            MessageType::StreamOpen,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamOpen(StreamOpenMessage {
                service: "raw".into(),
                target_host: Some("buffer.ws".into()),
                target_port: Some(8443),
            }),
        )
        .with_stream_id(21);
        let data = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"queued")),
        )
        .with_stream_id(21);
        peer.send_frame(&open).await.unwrap();
        peer.send_frame(&data).await.unwrap();

        let frame = server_task.await.unwrap();
        match frame.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"queued"),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn wss_mux_handshake_and_stream_roundtrip_with_insecure_client() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (cert_path, key_path) = write_self_signed_cert();
        let acceptor = build_ws_tls_acceptor(
            &ParsedUrl::parse(&format!(
                "wss://127.0.0.1:{}/tunnel?tls-cert={}&tls-key={}",
                addr.port(),
                cert_path.display(),
                key_path.display()
            ))
            .unwrap(),
        )
        .unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("wss-mux-server".into()),
            key: Some("shared-secret".into()),
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("wss-mux-client".into()),
            key: Some("shared-secret".into()),
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer(server_identity, listener, acceptor)
                .await
                .unwrap();
            let mut rx = peer.open_stream_receiver(42).await;
            rx.recv().await.unwrap()
        });

        let peer = connect_mux_peer(
            client_identity,
            &format!("wss://localhost:{}/tunnel?tls-insecure=1", addr.port()),
        )
        .await
        .unwrap();

        let data = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"secure")),
        )
        .with_stream_id(42);
        peer.send_frame(&data).await.unwrap();

        let frame = server_task.await.unwrap();
        match frame.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"secure"),
            _ => panic!(),
        }

        let _ = fs::remove_file(cert_path);
        let _ = fs::remove_file(key_path);
    }

    #[tokio::test]
    async fn wss_mux_handshake_and_stream_roundtrip_with_mutual_tls() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (server_cert_path, server_key_path) = write_self_signed_cert();
        let (client_cert_path, client_key_path) = write_self_signed_cert();
        let acceptor = build_ws_tls_acceptor(
            &ParsedUrl::parse(&format!(
                "wss://127.0.0.1:{}/tunnel?tls-cert={}&tls-key={}&tls-client-ca={}",
                addr.port(),
                server_cert_path.display(),
                server_key_path.display(),
                client_cert_path.display(),
            ))
            .unwrap(),
        )
        .unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("wss-mtls-server".into()),
            key: Some("shared-secret".into()),
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("wss-mtls-client".into()),
            key: Some("shared-secret".into()),
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer(server_identity, listener, acceptor)
                .await
                .unwrap();
            let mut rx = peer.open_stream_receiver(77).await;
            rx.recv().await.unwrap()
        });

        let peer = connect_mux_peer(
            client_identity,
            &format!(
                "wss://localhost:{}/tunnel?tls-ca={}&tls-client-cert={}&tls-client-key={}",
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
            Message::StreamData(StreamDataMessage::from_bytes(b"mutual")),
        )
        .with_stream_id(77);
        peer.send_frame(&data).await.unwrap();

        let frame = server_task.await.unwrap();
        match frame.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"mutual"),
            _ => panic!(),
        }

        let _ = fs::remove_file(server_cert_path);
        let _ = fs::remove_file(server_key_path);
        let _ = fs::remove_file(client_cert_path);
        let _ = fs::remove_file(client_key_path);
    }
}
