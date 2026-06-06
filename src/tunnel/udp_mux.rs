use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    net::SocketAddr,
    sync::Arc,
};

use tokio::{
    net::UdpSocket,
    sync::{mpsc, Mutex},
};

use crate::{
    agent::identity::AgentIdentity,
    crypto::transport::SharedKey,
    protocol::{
        frame::Frame,
        message::{Message, StreamOpenMessage},
    },
    session::{
        handshake::{complete_session, hello_ack_frame, hello_frame},
        heartbeat::heartbeat_frame,
        peer::{PeerInfo, PeerSession, SessionState},
    },
    tunnel::udp::{decode_transport_frame, encode_transport_frame},
};

#[derive(Debug, Default)]
struct StreamRoutingState {
    senders: HashMap<u32, mpsc::Sender<Frame>>,
    pending: HashMap<u32, Vec<Frame>>,
}

#[derive(Debug, Clone)]
pub struct MuxUdpPeer {
    pub session: PeerSession,
    pub peer_addr: SocketAddr,
    socket: Arc<UdpSocket>,
    routing: Arc<Mutex<StreamRoutingState>>,
    opens: Arc<Mutex<mpsc::Receiver<Frame>>>,
    controls: Arc<Mutex<mpsc::Receiver<Frame>>>,
    shared_key: Option<SharedKey>,
}

impl MuxUdpPeer {
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        let payload = encode_transport_frame(frame, self.shared_key.as_ref())?;
        if payload.len() > 65_507 {
            return Err(Error::new(ErrorKind::InvalidData, "udp frame too large"));
        }
        let written = self.socket.send(&payload).await?;
        if written != payload.len() {
            return Err(Error::new(ErrorKind::WriteZero, "short udp datagram write"));
        }
        Ok(())
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
    socket: &UdpSocket,
    shared_key: Option<&SharedKey>,
) -> Result<Frame, Error> {
    let mut buf = vec![0_u8; 65_535];
    let n = socket.recv(&mut buf).await?;
    decode_transport_frame(&buf[..n], shared_key)
}

async fn spawn_dispatch_loop(
    socket: Arc<UdpSocket>,
    shared_key: Option<SharedKey>,
    routing: Arc<Mutex<StreamRoutingState>>,
    open_tx: mpsc::Sender<Frame>,
    control_tx: mpsc::Sender<Frame>,
) {
    tokio::spawn(async move {
        loop {
            let frame = match read_boxed_frame(&socket, shared_key.as_ref()).await {
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

pub async fn accept_mux_peer(
    identity: AgentIdentity,
    socket: UdpSocket,
) -> Result<MuxUdpPeer, Error> {
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let mut buf = vec![0_u8; 65_535];
    let (n, addr) = socket.recv_from(&mut buf).await?;
    let hello = decode_transport_frame(&buf[..n], shared_key.as_ref())?;
    let session = complete_session(&identity, &hello)?;

    socket.connect(addr).await?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    let ack_payload = encode_transport_frame(&ack, shared_key.as_ref())?;
    socket.send(&ack_payload).await?;

    let heartbeat = read_boxed_frame(&socket, shared_key.as_ref()).await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }
    let heartbeat_ack = heartbeat_frame(identity.id.clone(), Some(session.remote.agent_id.clone()));
    let heartbeat_payload = encode_transport_frame(&heartbeat_ack, shared_key.as_ref())?;
    socket.send(&heartbeat_payload).await?;

    let routing = Arc::new(Mutex::new(StreamRoutingState::default()));
    let (open_tx, open_rx) = mpsc::channel(64);
    let (control_tx, control_rx) = mpsc::channel(64);
    let socket = Arc::new(socket);
    spawn_dispatch_loop(
        socket.clone(),
        shared_key.clone(),
        routing.clone(),
        open_tx,
        control_tx,
    )
    .await;

    Ok(MuxUdpPeer {
        session,
        peer_addr: addr,
        socket,
        routing,
        opens: Arc::new(Mutex::new(open_rx)),
        controls: Arc::new(Mutex::new(control_rx)),
        shared_key,
    })
}

pub async fn connect_mux_peer(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<MuxUdpPeer, Error> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(endpoint).await?;
    let peer_addr = socket.peer_addr()?;
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);

    let hello = hello_frame(&identity);
    let hello_payload = encode_transport_frame(&hello, shared_key.as_ref())?;
    socket.send(&hello_payload).await?;

    let ack = read_boxed_frame(&socket, shared_key.as_ref()).await?;
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
            capabilities: vec!["transport:udp".to_string()],
        },
        state: SessionState::Active,
    };

    let heartbeat = heartbeat_frame(identity.id.clone(), ack.header.src_agent.clone());
    let heartbeat_payload = encode_transport_frame(&heartbeat, shared_key.as_ref())?;
    socket.send(&heartbeat_payload).await?;
    let heartbeat_ack = read_boxed_frame(&socket, shared_key.as_ref()).await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }

    let routing = Arc::new(Mutex::new(StreamRoutingState::default()));
    let (open_tx, open_rx) = mpsc::channel(64);
    let (control_tx, control_rx) = mpsc::channel(64);
    let socket = Arc::new(socket);
    spawn_dispatch_loop(
        socket.clone(),
        shared_key.clone(),
        routing.clone(),
        open_tx,
        control_tx,
    )
    .await;

    Ok(MuxUdpPeer {
        session,
        peer_addr,
        socket,
        routing,
        opens: Arc::new(Mutex::new(open_rx)),
        controls: Arc::new(Mutex::new(control_rx)),
        shared_key,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{HeartbeatMessage, Message, StreamDataMessage, StreamOpenMessage},
        },
        tunnel::udp::bind,
        tunnel::udp_mux::{accept_mux_peer, connect_mux_peer},
    };

    #[tokio::test]
    async fn udp_mux_routes_frames_by_stream_id() {
        let socket = bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-mux-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-mux-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer(server_identity, socket).await.unwrap();
            let mut rx1 = peer.open_stream_receiver(1).await;
            let mut rx2 = peer.open_stream_receiver(2).await;

            let f1 = rx1.recv().await.unwrap();
            let f2 = rx2.recv().await.unwrap();
            match f1.message {
                Message::StreamData(data) => assert_eq!(data.to_bytes().unwrap(), b"one"),
                other => panic!("unexpected frame on stream 1: {:?}", other),
            }
            match f2.message {
                Message::StreamData(data) => assert_eq!(data.to_bytes().unwrap(), b"two"),
                other => panic!("unexpected frame on stream 2: {:?}", other),
            }
        });

        let peer = connect_mux_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        peer.send_frame(
            &Frame::new(
                MessageType::StreamData,
                Some(peer.session.local.agent_id.clone()),
                Some(peer.session.remote.agent_id.clone()),
                Message::StreamData(StreamDataMessage::from_bytes(b"one")),
            )
            .with_stream_id(1),
        )
        .await
        .unwrap();
        peer.send_frame(
            &Frame::new(
                MessageType::StreamData,
                Some(peer.session.local.agent_id.clone()),
                Some(peer.session.remote.agent_id.clone()),
                Message::StreamData(StreamDataMessage::from_bytes(b"two")),
            )
            .with_stream_id(2),
        )
        .await
        .unwrap();

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn udp_mux_buffers_stream_frames_until_receiver_is_opened() {
        let socket = bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-mux-buffer-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-mux-buffer-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer(server_identity, socket).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let mut rx = peer.open_stream_receiver(9).await;
            let frame = rx.recv().await.unwrap();
            match frame.message {
                Message::StreamData(data) => assert_eq!(data.to_bytes().unwrap(), b"buffered"),
                other => panic!("unexpected buffered frame: {:?}", other),
            }
        });

        let peer = connect_mux_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        peer.send_frame(
            &Frame::new(
                MessageType::StreamData,
                Some(peer.session.local.agent_id.clone()),
                Some(peer.session.remote.agent_id.clone()),
                Message::StreamData(StreamDataMessage::from_bytes(b"buffered")),
            )
            .with_stream_id(9),
        )
        .await
        .unwrap();

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn udp_mux_can_forward_control_frames() {
        let socket = bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-mux-ctrl-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-mux-ctrl-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer(server_identity, socket).await.unwrap();
            let frame = peer.read_control_frame().await.unwrap();
            match frame.message {
                Message::Heartbeat(HeartbeatMessage { .. }) => {}
                other => panic!("unexpected control frame: {:?}", other),
            }
        });

        let peer = connect_mux_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        peer.send_frame(&Frame::new(
            MessageType::Heartbeat,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::Heartbeat(HeartbeatMessage { unix_ts: 1 }),
        ))
        .await
        .unwrap();

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn udp_mux_reads_stream_open() {
        let socket = bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-mux-open-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-mux-open-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer(server_identity, socket).await.unwrap();
            let (stream_id, open) = peer.read_stream_open().await.unwrap();
            assert_eq!(stream_id, 21);
            assert_eq!(open.service, "raw");
            assert_eq!(open.target_host.as_deref(), Some("example.com"));
            assert_eq!(open.target_port, Some(80));
        });

        let peer = connect_mux_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        peer.send_frame(
            &Frame::new(
                MessageType::StreamOpen,
                Some(peer.session.local.agent_id.clone()),
                Some(peer.session.remote.agent_id.clone()),
                Message::StreamOpen(StreamOpenMessage {
                    service: "raw".into(),
                    target_host: Some("example.com".into()),
                    target_port: Some(80),
                }),
            )
            .with_stream_id(21),
        )
        .await
        .unwrap();

        server_task.await.unwrap();
    }
}
