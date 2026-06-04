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
use tokio_tungstenite::{
    accept_async, connect_async,
    tungstenite::{self, Message as WsMessage},
    MaybeTlsStream, WebSocketStream,
};

use crate::{
    agent::identity::AgentIdentity,
    protocol::{
        frame::Frame,
        message::{Message, StreamOpenMessage},
    },
    session::{
        handshake::{complete_session, hello_ack_frame, hello_frame},
        heartbeat::heartbeat_frame,
        peer::{PeerInfo, PeerSession, SessionState},
    },
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
}

impl MuxWsPeer {
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        let payload = crate::protocol::codec::encode_frame(frame)?;
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

async fn read_boxed_frame(stream: &mut BoxWsStream) -> Result<Frame, Error> {
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

    crate::protocol::codec::decode_frame(&bytes)
}

async fn spawn_dispatch_loop(
    mut reader: BoxWsStream,
    routing: Arc<Mutex<StreamRoutingState>>,
    open_tx: mpsc::Sender<Frame>,
    control_tx: mpsc::Sender<Frame>,
) {
    tokio::spawn(async move {
        loop {
            let frame = match read_boxed_frame(&mut reader).await {
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

fn split_boxed_tcp(ws: WebSocketStream<TcpStream>) -> (BoxWsSink, BoxWsStream) {
    let (writer, reader): (SplitSink<_, _>, SplitStream<_>) = ws.split();
    (Box::pin(writer), Box::pin(reader))
}

fn split_boxed_client(ws: WebSocketStream<MaybeTlsStream<TcpStream>>) -> (BoxWsSink, BoxWsStream) {
    let (writer, reader): (SplitSink<_, _>, SplitStream<_>) = ws.split();
    (Box::pin(writer), Box::pin(reader))
}

pub async fn bind(endpoint: &str) -> Result<TcpListener, Error> {
    TcpListener::bind(endpoint).await
}

pub async fn accept_mux_peer(
    identity: AgentIdentity,
    listener: TcpListener,
) -> Result<MuxWsPeer, Error> {
    accept_mux_peer_on(identity, &listener).await
}

pub async fn accept_mux_peer_on(
    identity: AgentIdentity,
    listener: &TcpListener,
) -> Result<MuxWsPeer, Error> {
    let (stream, addr) = listener.accept().await?;
    let mut ws_stream = accept_async(stream)
        .await
        .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e.to_string()))?;

    let hello = read_ws_frame(&mut ws_stream).await?;
    let session = complete_session(&identity, &hello)?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    write_ws_frame(&mut ws_stream, &ack).await?;

    let heartbeat = read_ws_frame(&mut ws_stream).await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }
    let heartbeat_ack = heartbeat_frame(identity.id.clone(), Some(session.remote.agent_id.clone()));
    write_ws_frame(&mut ws_stream, &heartbeat_ack).await?;

    let (writer, reader) = split_boxed_tcp(ws_stream);
    let routing = Arc::new(Mutex::new(StreamRoutingState::default()));
    let (open_tx, open_rx) = mpsc::channel(64);
    let (control_tx, control_rx) = mpsc::channel(64);
    spawn_dispatch_loop(reader, routing.clone(), open_tx, control_tx).await;

    Ok(MuxWsPeer {
        session,
        peer_addr: addr,
        writer: Arc::new(Mutex::new(writer)),
        routing,
        opens: Arc::new(Mutex::new(open_rx)),
        controls: Arc::new(Mutex::new(control_rx)),
    })
}

pub async fn connect_mux_peer(identity: AgentIdentity, endpoint: &str) -> Result<MuxWsPeer, Error> {
    let (mut ws_stream, _) = connect_async(endpoint)
        .await
        .map_err(|e| Error::new(ErrorKind::ConnectionRefused, e.to_string()))?;
    let peer_addr = peer_addr_from_client_ws(&ws_stream)?;

    let hello = hello_frame(&identity);
    write_ws_frame(&mut ws_stream, &hello).await?;
    let ack = read_ws_frame(&mut ws_stream).await?;
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
    write_ws_frame(&mut ws_stream, &heartbeat).await?;
    let heartbeat_ack = read_ws_frame(&mut ws_stream).await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }

    let (writer, reader) = split_boxed_client(ws_stream);
    let routing = Arc::new(Mutex::new(StreamRoutingState::default()));
    let (open_tx, open_rx) = mpsc::channel(64);
    let (control_tx, control_rx) = mpsc::channel(64);
    spawn_dispatch_loop(reader, routing.clone(), open_tx, control_tx).await;

    Ok(MuxWsPeer {
        session,
        peer_addr,
        writer: Arc::new(Mutex::new(writer)),
        routing,
        opens: Arc::new(Mutex::new(open_rx)),
        controls: Arc::new(Mutex::new(control_rx)),
    })
}

async fn write_ws_frame<S>(stream: &mut WebSocketStream<S>, frame: &Frame) -> Result<(), Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let payload = crate::protocol::codec::encode_frame(frame)?;
    stream
        .send(WsMessage::Binary(payload.into()))
        .await
        .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))
}

async fn read_ws_frame<S>(stream: &mut WebSocketStream<S>) -> Result<Frame, Error>
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

    crate::protocol::codec::decode_frame(&bytes)
}

fn peer_addr_from_client_ws(
    ws: &WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<SocketAddr, Error> {
    if let MaybeTlsStream::Plain(stream) = ws.get_ref() {
        stream.peer_addr()
    } else {
        Err(Error::new(
            ErrorKind::Unsupported,
            "tls websocket peer_addr lookup is not wired yet",
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, StreamDataMessage, StreamOpenMessage},
        },
        tunnel::ws_mux::{accept_mux_peer, bind, connect_mux_peer},
    };

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
            let peer = accept_mux_peer(server_identity, listener).await.unwrap();
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
            let peer = accept_mux_peer(server_identity, listener).await.unwrap();
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
}
