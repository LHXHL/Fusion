use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    sync::Arc,
};

use tokio::sync::{mpsc, Mutex};

use crate::{
    agent::identity::AgentIdentity,
    protocol::{
        frame::Frame,
        message::{Message, StreamOpenMessage},
    },
    tunnel::simplex_oss,
};

#[derive(Debug, Default)]
struct StreamRoutingState {
    senders: HashMap<u32, mpsc::Sender<Frame>>,
    pending: HashMap<u32, Vec<Frame>>,
}

#[derive(Clone)]
pub struct MuxSimplexOssPeer {
    pub session: crate::session::peer::PeerSession,
    pub(crate) inner: simplex_oss::ActiveSimplexOssPeer,
    routing: Arc<Mutex<StreamRoutingState>>,
    opens: Arc<Mutex<mpsc::Receiver<Frame>>>,
    controls: Arc<Mutex<mpsc::Receiver<Frame>>>,
}

impl MuxSimplexOssPeer {
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        self.inner.send_frame(frame).await
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

fn spawn_dispatch_loop(
    peer: simplex_oss::ActiveSimplexOssPeer,
    routing: Arc<Mutex<StreamRoutingState>>,
    open_tx: mpsc::Sender<Frame>,
    control_tx: mpsc::Sender<Frame>,
) {
    tokio::spawn(async move {
        loop {
            let frame = match peer.read_frame().await {
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

fn wrap_peer(inner: simplex_oss::ActiveSimplexOssPeer) -> MuxSimplexOssPeer {
    let routing = Arc::new(Mutex::new(StreamRoutingState::default()));
    let (open_tx, open_rx) = mpsc::channel(64);
    let (control_tx, control_rx) = mpsc::channel(64);
    spawn_dispatch_loop(inner.clone(), routing.clone(), open_tx, control_tx);
    MuxSimplexOssPeer {
        session: inner.session.clone(),
        inner,
        routing,
        opens: Arc::new(Mutex::new(open_rx)),
        controls: Arc::new(Mutex::new(control_rx)),
    }
}

pub async fn connect_mux_peer(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<MuxSimplexOssPeer, Error> {
    let peer = simplex_oss::connect_peer(identity, endpoint).await?;
    Ok(wrap_peer(peer))
}

pub async fn accept_mux_peer_on(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<MuxSimplexOssPeer, Error> {
    let peer = simplex_oss::accept_peer_on(identity, endpoint).await?;
    Ok(wrap_peer(peer))
}

#[cfg(test)]
mod tests {
    use tokio::fs;

    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, StreamDataMessage},
        },
    };

    use super::{accept_mux_peer_on, connect_mux_peer};

    #[tokio::test]
    async fn simplex_oss_mux_routes_frames_by_stream_id() {
        let root = std::env::temp_dir().join(format!(
            "fusion-simplex-oss-mux-test-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let endpoint = format!("simplex+oss://mesh-mux/channel?root={}", root.display());
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-oss-mux-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-oss-mux-client".into()),
            key: None,
        });

        let server_endpoint = endpoint.clone();
        let server = tokio::spawn(async move {
            let peer = accept_mux_peer_on(server_identity, &server_endpoint)
                .await
                .unwrap();
            let mut rx1 = peer.open_stream_receiver(1).await;
            let mut rx2 = peer.open_stream_receiver(2).await;
            let f1 = rx1.recv().await.unwrap();
            let f2 = rx2.recv().await.unwrap();
            match f1.message {
                Message::StreamData(data) => assert_eq!(data.to_bytes().unwrap(), b"one"),
                other => panic!("unexpected frame: {:?}", other),
            }
            match f2.message {
                Message::StreamData(data) => assert_eq!(data.to_bytes().unwrap(), b"two"),
                other => panic!("unexpected frame: {:?}", other),
            }
        });

        let peer = connect_mux_peer(client_identity, &endpoint).await.unwrap();
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

        server.await.unwrap();
        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn simplex_oss_mux_buffers_stream_frames_until_receiver_is_opened() {
        let root = std::env::temp_dir().join(format!(
            "fusion-simplex-oss-mux-buffer-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let endpoint = format!("simplex+oss://mesh-buffer/channel?root={}", root.display());
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-oss-mux-buffer-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-oss-mux-buffer-client".into()),
            key: None,
        });

        let endpoint_for_server = endpoint.clone();
        let server = tokio::spawn(async move {
            let peer = accept_mux_peer_on(server_identity, &endpoint_for_server)
                .await
                .unwrap();
            let (stream_id, open) = peer.read_stream_open().await.unwrap();
            assert_eq!(stream_id, 7);
            assert_eq!(open.target_host.as_deref(), Some("buffer.test"));
            let mut rx = peer.open_stream_receiver(stream_id).await;
            rx.recv().await.unwrap()
        });

        let peer = connect_mux_peer(client_identity, &endpoint).await.unwrap();
        let open = Frame::new(
            MessageType::StreamOpen,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamOpen(crate::protocol::message::StreamOpenMessage {
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

        let frame = server.await.unwrap();
        match frame.message {
            Message::StreamData(d) => assert_eq!(d.to_bytes().unwrap(), b"buffered"),
            other => panic!("unexpected frame: {:?}", other),
        }
        let _ = fs::remove_dir_all(root).await;
    }
}
