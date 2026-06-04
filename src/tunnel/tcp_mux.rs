use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    net::SocketAddr,
    sync::Arc,
};

use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
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
    tunnel::tcp::{read_frame_with_key, write_frame_with_key},
};

#[derive(Debug, Default)]
struct StreamRoutingState {
    senders: HashMap<u32, mpsc::Sender<Frame>>,
    pending: HashMap<u32, Vec<Frame>>,
}

#[derive(Debug, Clone)]
pub struct MuxTcpPeer {
    pub session: PeerSession,
    pub peer_addr: SocketAddr,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    routing: Arc<Mutex<StreamRoutingState>>,
    opens: Arc<Mutex<mpsc::Receiver<Frame>>>,
    controls: Arc<Mutex<mpsc::Receiver<Frame>>>,
    shared_key: Option<SharedKey>,
}

impl MuxTcpPeer {
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        let mut writer = self.writer.lock().await;
        write_frame_half(&mut writer, frame, self.shared_key.as_ref()).await
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

async fn write_frame_half(
    writer: &mut OwnedWriteHalf,
    frame: &Frame,
    shared_key: Option<&SharedKey>,
) -> Result<(), Error> {
    use tokio::io::AsyncWriteExt;
    let payload = crate::crypto::transport::encode_transport_frame(frame, shared_key)?;
    let len = u32::try_from(payload.len())
        .map_err(|_| Error::new(ErrorKind::InvalidData, "frame too large"))?;
    writer.write_u32(len).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_frame_half(
    reader: &mut OwnedReadHalf,
    shared_key: Option<&SharedKey>,
) -> Result<Frame, Error> {
    use tokio::io::AsyncReadExt;
    let len = reader.read_u32().await? as usize;
    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload).await?;
    crate::crypto::transport::decode_transport_frame(&payload, shared_key)
}

async fn spawn_dispatch_loop(
    mut reader: OwnedReadHalf,
    shared_key: Option<SharedKey>,
    routing: Arc<Mutex<StreamRoutingState>>,
    open_tx: mpsc::Sender<Frame>,
    control_tx: mpsc::Sender<Frame>,
) {
    tokio::spawn(async move {
        loop {
            let frame = match read_frame_half(&mut reader, shared_key.as_ref()).await {
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
    listener: TcpListener,
) -> Result<MuxTcpPeer, Error> {
    accept_mux_peer_on(identity, &listener).await
}

pub async fn accept_mux_peer_on(
    identity: AgentIdentity,
    listener: &TcpListener,
) -> Result<MuxTcpPeer, Error> {
    let (mut stream, addr) = listener.accept().await?;
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let hello = read_frame_with_key(&mut stream, shared_key.as_ref()).await?;
    let session = complete_session(&identity, &hello)?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    write_frame_with_key(&mut stream, &ack, shared_key.as_ref()).await?;

    let heartbeat = read_frame_with_key(&mut stream, shared_key.as_ref()).await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }
    let heartbeat_ack = heartbeat_frame(identity.id.clone(), Some(session.remote.agent_id.clone()));
    write_frame_with_key(&mut stream, &heartbeat_ack, shared_key.as_ref()).await?;

    let (reader, writer) = stream.into_split();
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

    Ok(MuxTcpPeer {
        session,
        peer_addr: addr,
        writer: Arc::new(Mutex::new(writer)),
        routing,
        opens: Arc::new(Mutex::new(open_rx)),
        controls: Arc::new(Mutex::new(control_rx)),
        shared_key,
    })
}

pub async fn connect_mux_peer(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<MuxTcpPeer, Error> {
    let mut stream = TcpStream::connect(endpoint).await?;
    let peer_addr = stream.peer_addr()?;
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);

    let hello = hello_frame(&identity);
    write_frame_with_key(&mut stream, &hello, shared_key.as_ref()).await?;
    let ack = read_frame_with_key(&mut stream, shared_key.as_ref()).await?;
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
            capabilities: vec![],
        },
        state: SessionState::Active,
    };

    let heartbeat = heartbeat_frame(identity.id.clone(), ack.header.src_agent.clone());
    write_frame_with_key(&mut stream, &heartbeat, shared_key.as_ref()).await?;
    let heartbeat_ack = read_frame_with_key(&mut stream, shared_key.as_ref()).await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }

    let (reader, writer) = stream.into_split();
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

    Ok(MuxTcpPeer {
        session,
        peer_addr,
        writer: Arc::new(Mutex::new(writer)),
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
            message::{Message, StreamDataMessage, StreamOpenMessage},
        },
        tunnel::tcp::bind,
        tunnel::tcp_mux::{accept_mux_peer, connect_mux_peer},
    };

    #[tokio::test]
    async fn mux_peer_routes_frames_by_stream_id() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("mux-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("mux-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer(server_identity, listener).await.unwrap();
            let mut rx1 = peer.open_stream_receiver(1).await;
            let mut rx2 = peer.open_stream_receiver(2).await;
            let a = tokio::spawn(async move { rx1.recv().await.unwrap() });
            let b = tokio::spawn(async move { rx2.recv().await.unwrap() });
            (a.await.unwrap(), b.await.unwrap())
        });

        let peer = connect_mux_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        let f1 = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"one")),
        )
        .with_stream_id(1);
        let f2 = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"two")),
        )
        .with_stream_id(2);
        peer.send_frame(&f1).await.unwrap();
        peer.send_frame(&f2).await.unwrap();

        let (r1, r2) = server_task.await.unwrap();
        match r1.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"one"),
            _ => panic!(),
        }
        match r2.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"two"),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn mux_peer_can_forward_control_frames_across_relay() {
        let relay_down_listener = bind("127.0.0.1:0").await.unwrap();
        let relay_down_addr = relay_down_listener.local_addr().unwrap();
        let relay_up_listener = bind("127.0.0.1:0").await.unwrap();
        let relay_up_addr = relay_up_listener.local_addr().unwrap();

        let relay_down_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("relay-down".into()),
            key: None,
        });
        let relay_up_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("relay-up".into()),
            key: None,
        });
        let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("leaf-node".into()),
            key: None,
        });
        let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("target-node".into()),
            key: None,
        });
        let target_id = target_identity.id.clone();

        let upstream_task = tokio::spawn(async move {
            let peer = accept_mux_peer(target_identity, relay_up_listener)
                .await
                .unwrap();
            peer.read_control_frame().await.unwrap()
        });

        let relay_upstream = tokio::spawn(async move {
            accept_mux_peer(relay_up_identity, relay_down_listener)
                .await
                .unwrap()
        });

        let target_peer = connect_mux_peer(leaf_identity, &relay_down_addr.to_string())
            .await
            .unwrap();
        let downstream_peer = relay_upstream.await.unwrap();
        let upstream_peer = connect_mux_peer(relay_down_identity, &relay_up_addr.to_string())
            .await
            .unwrap();

        let frame = Frame::new(
            MessageType::TaskRequest,
            Some(target_peer.session.local.agent_id.clone()),
            Some(target_id.clone()),
            Message::TaskRequest(crate::protocol::message::TaskRequestMessage {
                task_id: "relay-task-1".into(),
                action: crate::protocol::message::TaskAction::Shell,
                args: vec!["whoami".into()],
                data_hex: None,
            }),
        );
        target_peer.send_frame(&frame).await.unwrap();

        let control = downstream_peer.read_control_frame().await.unwrap();
        assert_eq!(
            control.header.dst_agent.as_deref(),
            Some(target_id.as_str())
        );
        upstream_peer.send_frame(&control).await.unwrap();

        let delivered = upstream_task.await.unwrap();
        match delivered.message {
            Message::TaskRequest(req) => {
                assert!(matches!(
                    req.action,
                    crate::protocol::message::TaskAction::Shell
                ));
                assert_eq!(req.args.first().map(String::as_str), Some("whoami"));
            }
            other => panic!("unexpected relayed message: {:?}", other),
        }
    }

    #[tokio::test]
    async fn mux_peer_buffers_stream_frames_until_receiver_is_opened() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("mux-buffer-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("mux-buffer-client".into()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer(server_identity, listener).await.unwrap();
            let (stream_id, open) = peer.read_stream_open().await.unwrap();
            assert_eq!(stream_id, 7);
            assert_eq!(open.target_host.as_deref(), Some("buffer.test"));
            let mut rx = peer.open_stream_receiver(stream_id).await;
            rx.recv().await.unwrap()
        });

        let peer = connect_mux_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        let open = Frame::new(
            MessageType::StreamOpen,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamOpen(StreamOpenMessage {
                service: "raw".into(),
                target_host: Some("buffer.test".into()),
                target_port: Some(443),
            }),
        )
        .with_stream_id(7);
        let data = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"buffered")),
        )
        .with_stream_id(7);
        peer.send_frame(&open).await.unwrap();
        peer.send_frame(&data).await.unwrap();

        let frame = server_task.await.unwrap();
        match frame.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"buffered"),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn mux_peer_routes_frames_by_stream_id_with_shared_key() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("mux-server-keyed".into()),
            key: Some("shared-secret".into()),
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("mux-client-keyed".into()),
            key: Some("shared-secret".into()),
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_mux_peer(server_identity, listener).await.unwrap();
            let mut rx = peer.open_stream_receiver(9).await;
            rx.recv().await.unwrap()
        });

        let peer = connect_mux_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        let frame = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"secure-mux")),
        )
        .with_stream_id(9);
        peer.send_frame(&frame).await.unwrap();

        let delivered = server_task.await.unwrap();
        match delivered.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"secure-mux"),
            _ => panic!(),
        }
    }
}
