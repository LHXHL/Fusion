use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    sync::{Mutex, OnceLock},
};

use log::info;
use tokio::{
    io::{duplex, AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::{mpsc, Mutex as AsyncMutex},
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

static MEMORY_LISTENERS: OnceLock<Mutex<HashMap<String, mpsc::UnboundedSender<DuplexStream>>>> =
    OnceLock::new();

fn listeners() -> &'static Mutex<HashMap<String, mpsc::UnboundedSender<DuplexStream>>> {
    MEMORY_LISTENERS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Debug)]
pub struct MemoryListener {
    name: String,
    receiver: AsyncMutex<mpsc::UnboundedReceiver<DuplexStream>>,
}

impl MemoryListener {
    pub fn bind(name: &str) -> Result<Self, Error> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut guard = listeners()
            .lock()
            .map_err(|_| Error::other("memory listener registry poisoned"))?;
        if guard.contains_key(name) {
            return Err(Error::new(
                ErrorKind::AddrInUse,
                format!("memory listener `{name}` already exists"),
            ));
        }
        guard.insert(name.to_string(), tx);
        Ok(Self {
            name: name.to_string(),
            receiver: AsyncMutex::new(rx),
        })
    }

    pub async fn accept(&self) -> Result<DuplexStream, Error> {
        let mut receiver = self.receiver.lock().await;
        receiver
            .recv()
            .await
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "memory listener closed"))
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for MemoryListener {
    fn drop(&mut self) {
        if let Ok(mut guard) = listeners().lock() {
            guard.remove(&self.name);
        }
    }
}

#[derive(Debug)]
pub struct ActiveMemoryPeer {
    pub session: PeerSession,
    pub listener_name: String,
    stream: DuplexStream,
    shared_key: Option<SharedKey>,
}

impl ActiveMemoryPeer {
    pub async fn send_frame(&mut self, frame: &Frame) -> Result<(), Error> {
        write_frame_with_key(&mut self.stream, frame, self.shared_key.as_ref()).await
    }

    pub async fn read_frame(&mut self) -> Result<Frame, Error> {
        read_frame_with_key(&mut self.stream, self.shared_key.as_ref()).await
    }
}

pub async fn connect(name: &str) -> Result<DuplexStream, Error> {
    let tx = {
        let guard = listeners()
            .lock()
            .map_err(|_| Error::other("memory listener registry poisoned"))?;
        guard.get(name).cloned()
    }
    .ok_or_else(|| {
        Error::new(
            ErrorKind::NotFound,
            format!("memory listener `{name}` not found"),
        )
    })?;

    let (server, client) = duplex(64 * 1024);
    tx.send(server)
        .map_err(|_| Error::new(ErrorKind::BrokenPipe, "memory listener unavailable"))?;
    Ok(client)
}

pub async fn write_frame_with_key(
    stream: &mut DuplexStream,
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
    stream: &mut DuplexStream,
    shared_key: Option<&SharedKey>,
) -> Result<Frame, Error> {
    let len = stream.read_u32().await? as usize;
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await?;
    decode_transport_frame(&payload, shared_key)
}

pub async fn accept_peer_on(
    identity: AgentIdentity,
    listener: &MemoryListener,
) -> Result<ActiveMemoryPeer, Error> {
    let mut stream = listener.accept().await?;
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

    info!("memory inbound session active on {}", listener.name());
    Ok(ActiveMemoryPeer {
        session,
        listener_name: listener.name().to_string(),
        stream,
        shared_key,
    })
}

pub async fn connect_peer(identity: AgentIdentity, name: &str) -> Result<ActiveMemoryPeer, Error> {
    let mut stream = connect(name).await?;
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

    info!("memory outbound session active to {name}");
    Ok(ActiveMemoryPeer {
        session,
        listener_name: name.to_string(),
        stream,
        shared_key,
    })
}

pub async fn run_inbound_session_once(
    identity: AgentIdentity,
    listener: MemoryListener,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = accept_peer_on(identity, &listener).await?;
    Ok((peer.session, Vec::new()))
}

pub async fn run_outbound_session_once(
    identity: AgentIdentity,
    name: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = connect_peer(identity, name).await?;
    Ok((peer.session, Vec::new()))
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        tunnel::memory::{run_inbound_session_once, run_outbound_session_once, MemoryListener},
    };

    #[tokio::test]
    async fn memory_session_hello_heartbeat_roundtrip() {
        let listener = MemoryListener::bind("unit-memory-roundtrip").unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("memory-server-node".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("memory-client-node".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            run_inbound_session_once(server_identity, listener)
                .await
                .unwrap()
        });

        let (client_session, _) =
            run_outbound_session_once(client_identity, "unit-memory-roundtrip")
                .await
                .unwrap();
        let (server_session, _) = server_task.await.unwrap();

        assert_eq!(client_session.local.agent_name, "memory-client-node");
        assert_eq!(server_session.local.agent_name, "memory-server-node");
    }
}
