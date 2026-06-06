use std::io::{Error, ErrorKind};
use std::path::Path;

use log::info;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
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
};

#[derive(Debug)]
pub struct ActiveUnixPeer {
    pub session: PeerSession,
    pub path: String,
    stream: UnixStream,
    shared_key: Option<SharedKey>,
}

impl ActiveUnixPeer {
    pub async fn send_frame(&mut self, frame: &Frame) -> Result<(), Error> {
        write_frame_with_key(&mut self.stream, frame, self.shared_key.as_ref()).await
    }

    pub async fn read_frame(&mut self) -> Result<Frame, Error> {
        read_frame_with_key(&mut self.stream, self.shared_key.as_ref()).await
    }
}

pub async fn bind(path: &str) -> Result<UnixListener, Error> {
    let socket_path = Path::new(path);
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if socket_path.exists() {
        std::fs::remove_file(socket_path)?;
    }
    UnixListener::bind(socket_path)
}

pub async fn connect(path: &str) -> Result<UnixStream, Error> {
    UnixStream::connect(path).await
}

pub async fn write_frame_with_key(
    stream: &mut UnixStream,
    frame: &Frame,
    shared_key: Option<&SharedKey>,
) -> Result<(), Error> {
    let payload = encode_transport_frame(frame, shared_key)?;
    let len = u32::try_from(payload.len())
        .map_err(|_| Error::new(ErrorKind::InvalidData, "frame too large"))?;
    stream.write_u32(len).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn read_frame_with_key(
    stream: &mut UnixStream,
    shared_key: Option<&SharedKey>,
) -> Result<Frame, Error> {
    let len = stream.read_u32().await? as usize;
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await?;
    decode_transport_frame(&payload, shared_key)
}

pub async fn accept_peer_on(
    identity: AgentIdentity,
    listener: &UnixListener,
    path: &str,
) -> Result<ActiveUnixPeer, Error> {
    let (mut stream, _) = listener.accept().await?;
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

    info!("unix inbound session active from {path}");
    Ok(ActiveUnixPeer {
        session,
        path: path.to_string(),
        stream,
        shared_key,
    })
}

pub async fn connect_peer(identity: AgentIdentity, path: &str) -> Result<ActiveUnixPeer, Error> {
    let mut stream = connect(path).await?;
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
    write_frame_with_key(&mut stream, &heartbeat, shared_key.as_ref()).await?;
    let heartbeat_ack = read_frame_with_key(&mut stream, shared_key.as_ref()).await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }

    info!("unix outbound session active to {path}");
    Ok(ActiveUnixPeer {
        session,
        path: path.to_string(),
        stream,
        shared_key,
    })
}

pub async fn run_inbound_session_once(
    identity: AgentIdentity,
    listener: UnixListener,
    path: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = accept_peer_on(identity, &listener, path).await?;
    Ok((peer.session, Vec::new()))
}

pub async fn run_outbound_session_once(
    identity: AgentIdentity,
    path: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = connect_peer(identity, path).await?;
    Ok((peer.session, Vec::new()))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        tunnel::unix::{bind, run_inbound_session_once, run_outbound_session_once},
    };

    fn temp_socket_path(name: &str) -> String {
        let uniq = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/tmp/fusion-{name}-{uniq}.sock")
    }

    #[tokio::test]
    async fn unix_session_hello_heartbeat_roundtrip() {
        let path = temp_socket_path("roundtrip");
        let listener = bind(&path).await.unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("unix-server-node".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("unix-client-node".to_string()),
            key: None,
        });

        let path_for_server = path.clone();
        let server_task = tokio::spawn(async move {
            run_inbound_session_once(server_identity, listener, &path_for_server)
                .await
                .unwrap()
        });

        let (client_session, _) = run_outbound_session_once(client_identity, &path)
            .await
            .unwrap();
        let (server_session, _) = server_task.await.unwrap();

        assert_eq!(client_session.local.agent_name, "unix-client-node");
        assert_eq!(server_session.local.agent_name, "unix-server-node");
        let _ = std::fs::remove_file(path);
    }
}
