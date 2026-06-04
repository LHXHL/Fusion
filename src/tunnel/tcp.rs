use std::io::{Error, ErrorKind};
use std::net::SocketAddr;

use log::info;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    agent::identity::AgentIdentity,
    protocol::{
        codec::{decode_frame, encode_frame},
        frame::Frame,
        message::Message,
    },
    session::{
        handshake::{complete_session, hello_ack_frame, hello_frame},
        heartbeat::heartbeat_frame,
        peer::{PeerInfo, PeerSession, SessionState},
    },
};

#[derive(Debug)]
pub struct ActiveTcpPeer {
    pub session: PeerSession,
    pub peer_addr: SocketAddr,
    stream: TcpStream,
}

impl ActiveTcpPeer {
    pub async fn send_frame(&mut self, frame: &Frame) -> Result<(), Error> {
        write_frame(&mut self.stream, frame).await
    }

    pub async fn read_frame(&mut self) -> Result<Frame, Error> {
        read_frame(&mut self.stream).await
    }
}

pub async fn bind(endpoint: &str) -> Result<TcpListener, Error> {
    TcpListener::bind(endpoint).await
}

pub async fn connect(endpoint: &str) -> Result<TcpStream, Error> {
    TcpStream::connect(endpoint).await
}

pub async fn write_frame(stream: &mut TcpStream, frame: &Frame) -> Result<(), Error> {
    let payload = encode_frame(frame)?;
    let len = u32::try_from(payload.len())
        .map_err(|_| Error::new(ErrorKind::InvalidData, "frame too large"))?;
    stream.write_u32(len).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn read_frame(stream: &mut TcpStream) -> Result<Frame, Error> {
    let len = stream.read_u32().await? as usize;
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await?;
    decode_frame(&payload)
}

pub async fn accept_peer(
    identity: AgentIdentity,
    listener: TcpListener,
) -> Result<ActiveTcpPeer, Error> {
    accept_peer_on(identity, &listener).await
}

pub async fn accept_peer_on(
    identity: AgentIdentity,
    listener: &TcpListener,
) -> Result<ActiveTcpPeer, Error> {
    let (mut stream, addr) = listener.accept().await?;
    let hello = read_frame(&mut stream).await?;
    let session = complete_session(&identity, &hello)?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    write_frame(&mut stream, &ack).await?;

    let heartbeat = read_frame(&mut stream).await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }

    let heartbeat_ack = heartbeat_frame(identity.id.clone(), Some(session.remote.agent_id.clone()));
    write_frame(&mut stream, &heartbeat_ack).await?;

    info!("tcp inbound session active from {addr}");
    Ok(ActiveTcpPeer {
        session,
        peer_addr: addr,
        stream,
    })
}

pub async fn connect_peer(identity: AgentIdentity, endpoint: &str) -> Result<ActiveTcpPeer, Error> {
    let mut stream = connect(endpoint).await?;
    let peer_addr = stream.peer_addr()?;

    let hello = hello_frame(&identity);
    write_frame(&mut stream, &hello).await?;

    let ack = read_frame(&mut stream).await?;
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
    write_frame(&mut stream, &heartbeat).await?;
    let heartbeat_ack = read_frame(&mut stream).await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }

    info!("tcp outbound session active to {endpoint}");
    Ok(ActiveTcpPeer {
        session,
        peer_addr,
        stream,
    })
}

pub async fn run_inbound_session_once(
    identity: AgentIdentity,
    listener: TcpListener,
) -> Result<(PeerSession, Vec<Frame>, SocketAddr), Error> {
    let peer = accept_peer(identity, listener).await?;
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
        tunnel::tcp::{
            accept_peer, bind, connect_peer, run_inbound_session_once, run_outbound_session_once,
        },
    };

    #[tokio::test]
    async fn tcp_session_hello_heartbeat_roundtrip() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("server-node".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("client-node".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            run_inbound_session_once(server_identity, listener)
                .await
                .unwrap()
        });

        let (client_session, _) = run_outbound_session_once(client_identity, &addr.to_string())
            .await
            .unwrap();
        let (server_session, _, _) = server_task.await.unwrap();

        assert_eq!(client_session.local.agent_name, "client-node");
        assert_eq!(server_session.local.agent_name, "server-node");
    }

    #[tokio::test]
    async fn tcp_peer_can_exchange_stream_control_frames_after_handshake() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("server-stream".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("client-stream".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let mut peer = accept_peer(server_identity, listener).await.unwrap();
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
            );
            peer.send_frame(&close).await.unwrap();
            peer.session
        });

        let mut client_peer = connect_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        let open = Frame::new(
            MessageType::StreamOpen,
            Some(client_peer.session.local.agent_id.clone()),
            Some(client_peer.session.remote.agent_id.clone()),
            Message::StreamOpen(StreamOpenMessage {
                service: "raw".to_string(),
                target_host: Some("example.com".to_string()),
                target_port: Some(80),
            }),
        );
        client_peer.send_frame(&open).await.unwrap();
        let close = client_peer.read_frame().await.unwrap();
        match close.message {
            Message::StreamClose(msg) => {
                assert_eq!(msg.reason.as_deref(), Some("ok"));
            }
            other => panic!("unexpected close reply: {:?}", other),
        }

        let server_session = server_task.await.unwrap();
        assert_eq!(server_session.remote.agent_name, "client-stream");
    }
}
