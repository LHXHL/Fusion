use std::io::{Error, ErrorKind};
use std::net::SocketAddr;

use log::info;
use tokio::net::UdpSocket;

pub use crate::crypto::transport::{decode_transport_frame, encode_transport_frame};

use crate::{
    agent::identity::AgentIdentity,
    crypto::transport::SharedKey,
    protocol::{frame::Frame, message::Message},
    session::{
        handshake::{complete_session, hello_ack_frame, hello_frame},
        heartbeat::heartbeat_frame,
        peer::{PeerInfo, PeerSession, SessionState},
    },
};

#[derive(Debug)]
pub struct ActiveUdpPeer {
    pub session: PeerSession,
    pub peer_addr: SocketAddr,
    socket: UdpSocket,
    shared_key: Option<SharedKey>,
}

impl ActiveUdpPeer {
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        send_frame_with_key(&self.socket, frame, self.shared_key.as_ref()).await
    }

    pub async fn read_frame(&self) -> Result<Frame, Error> {
        read_frame_with_key(&self.socket, self.shared_key.as_ref()).await
    }

    pub fn local_addr(&self) -> Result<SocketAddr, Error> {
        self.socket.local_addr()
    }
}

pub async fn bind(endpoint: &str) -> Result<UdpSocket, Error> {
    UdpSocket::bind(endpoint).await
}

pub async fn connect(endpoint: &str) -> Result<UdpSocket, Error> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(endpoint).await?;
    Ok(socket)
}

pub async fn send_frame(socket: &UdpSocket, frame: &Frame) -> Result<(), Error> {
    send_frame_with_key(socket, frame, None).await
}

pub async fn send_frame_with_key(
    socket: &UdpSocket,
    frame: &Frame,
    shared_key: Option<&SharedKey>,
) -> Result<(), Error> {
    let payload = encode_transport_frame(frame, shared_key)?;
    if payload.len() > 65_507 {
        return Err(Error::new(ErrorKind::InvalidData, "udp frame too large"));
    }
    let written = socket.send(&payload).await?;
    if written != payload.len() {
        return Err(Error::new(ErrorKind::WriteZero, "short udp datagram write"));
    }
    Ok(())
}

pub async fn read_frame(socket: &UdpSocket) -> Result<Frame, Error> {
    read_frame_with_key(socket, None).await
}

pub async fn read_frame_with_key(
    socket: &UdpSocket,
    shared_key: Option<&SharedKey>,
) -> Result<Frame, Error> {
    let mut buf = vec![0_u8; 65_535];
    let n = socket.recv(&mut buf).await?;
    decode_transport_frame(&buf[..n], shared_key)
}

pub async fn accept_peer(
    identity: AgentIdentity,
    socket: UdpSocket,
) -> Result<ActiveUdpPeer, Error> {
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let mut buf = vec![0_u8; 65_535];
    let (n, addr) = socket.recv_from(&mut buf).await?;
    let hello = decode_transport_frame(&buf[..n], shared_key.as_ref())?;
    let session = complete_session(&identity, &hello)?;

    socket.connect(addr).await?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    send_frame_with_key(&socket, &ack, shared_key.as_ref()).await?;

    let heartbeat = read_frame_with_key(&socket, shared_key.as_ref()).await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }

    let heartbeat_ack = heartbeat_frame(identity.id.clone(), Some(session.remote.agent_id.clone()));
    send_frame_with_key(&socket, &heartbeat_ack, shared_key.as_ref()).await?;

    info!("udp inbound session active from {addr}");
    Ok(ActiveUdpPeer {
        session,
        peer_addr: addr,
        socket,
        shared_key,
    })
}

pub async fn connect_peer(identity: AgentIdentity, endpoint: &str) -> Result<ActiveUdpPeer, Error> {
    let socket = connect(endpoint).await?;
    let peer_addr = socket.peer_addr()?;
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);

    let hello = hello_frame(&identity);
    send_frame_with_key(&socket, &hello, shared_key.as_ref()).await?;

    let ack = read_frame_with_key(&socket, shared_key.as_ref()).await?;
    match &ack.message {
        Message::HelloAck(msg) if msg.accepted => {}
        _ => {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "peer rejected hello handshake",
            ));
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
    send_frame_with_key(&socket, &heartbeat, shared_key.as_ref()).await?;
    let heartbeat_ack = read_frame_with_key(&socket, shared_key.as_ref()).await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }

    info!("udp outbound session active to {endpoint}");
    Ok(ActiveUdpPeer {
        session,
        peer_addr,
        socket,
        shared_key,
    })
}

pub async fn run_inbound_session_once(
    identity: AgentIdentity,
    socket: UdpSocket,
) -> Result<(PeerSession, Vec<Frame>, SocketAddr), Error> {
    let peer = accept_peer(identity, socket).await?;
    Ok((peer.session, Vec::new(), peer.peer_addr))
}

pub async fn run_outbound_session_once(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = connect_peer(identity, endpoint).await?;
    Ok((peer.session, Vec::new()))
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, StreamCloseMessage, StreamOpenMessage},
        },
        tunnel::udp::{
            accept_peer, bind, connect_peer, run_inbound_session_once, run_outbound_session_once,
        },
    };

    #[tokio::test]
    async fn udp_session_hello_heartbeat_roundtrip() {
        let socket = bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-server-node".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-client-node".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            run_inbound_session_once(server_identity, socket)
                .await
                .unwrap()
        });

        let (client_session, _) = run_outbound_session_once(client_identity, &addr.to_string())
            .await
            .unwrap();
        let (server_session, _, _) = server_task.await.unwrap();

        assert_eq!(client_session.local.agent_name, "udp-client-node");
        assert_eq!(server_session.local.agent_name, "udp-server-node");
    }

    #[tokio::test]
    async fn udp_peer_can_exchange_stream_control_frames_after_handshake() {
        let socket = bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-server-stream".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("udp-client-stream".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let peer = accept_peer(server_identity, socket).await.unwrap();
            let frame = peer.read_frame().await.unwrap();
            match frame.message {
                Message::StreamOpen(msg) => {
                    assert_eq!(msg.service, "raw");
                    assert_eq!(msg.target_host.as_deref(), Some("example.com"));
                    assert_eq!(msg.target_port, Some(80));
                }
                other => panic!("unexpected message: {:?}", other),
            }

            let close = Frame::new(
                MessageType::StreamClose,
                Some(peer.session.local.agent_id.clone()),
                Some(peer.session.remote.agent_id.clone()),
                Message::StreamClose(StreamCloseMessage {
                    reason: Some("ok".to_string()),
                }),
            )
            .with_stream_id(42);
            peer.send_frame(&close).await.unwrap();
        });

        let peer = connect_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        let open = Frame::new(
            MessageType::StreamOpen,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamOpen(StreamOpenMessage {
                service: "raw".to_string(),
                target_host: Some("example.com".to_string()),
                target_port: Some(80),
            }),
        )
        .with_stream_id(42);
        peer.send_frame(&open).await.unwrap();

        let response = peer.read_frame().await.unwrap();
        assert_eq!(response.header.stream_id, Some(42));
        assert!(matches!(response.message, Message::StreamClose(_)));

        server_task.await.unwrap();
    }
}
