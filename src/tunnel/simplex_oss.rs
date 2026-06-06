use std::{
    io::{Error, ErrorKind},
    path::{Path, PathBuf},
    time::Duration,
};

use sha2::{Digest, Sha256};
use tokio::{fs, time::sleep};

use crate::{
    agent::identity::AgentIdentity,
    crypto::transport::{decode_transport_frame, encode_transport_frame, SharedKey},
    protocol::{frame::Frame, message::Message},
    session::{
        handshake::{complete_session, hello_ack_frame, hello_frame},
        heartbeat::heartbeat_frame,
        peer::{PeerInfo, PeerSession, SessionState},
    },
    utils::url::ParsedUrl,
};

const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone)]
pub struct ActiveSimplexOssPeer {
    pub session: PeerSession,
    send_dir: PathBuf,
    recv_dir: PathBuf,
    next_send_seq: std::sync::Arc<tokio::sync::Mutex<u64>>,
    next_recv_seq: std::sync::Arc<tokio::sync::Mutex<u64>>,
    shared_key: Option<SharedKey>,
}

impl ActiveSimplexOssPeer {
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        let payload = encode_transport_frame(frame, self.shared_key.as_ref())?;
        let seq = {
            let mut guard = self.next_send_seq.lock().await;
            let current = *guard;
            *guard = guard.saturating_add(1);
            current
        };
        fs::create_dir_all(&self.send_dir).await?;
        let tmp_path = self.send_dir.join(format!("{seq:020}.tmp"));
        let final_path = self.send_dir.join(format!("{seq:020}.frame"));
        fs::write(&tmp_path, payload).await?;
        fs::rename(&tmp_path, &final_path).await?;
        Ok(())
    }

    pub async fn read_frame(&self) -> Result<Frame, Error> {
        loop {
            let next_seq = { *self.next_recv_seq.lock().await };
            let frame_path = self.recv_dir.join(format!("{next_seq:020}.frame"));
            if fs::try_exists(&frame_path).await? {
                let payload = fs::read(&frame_path).await?;
                fs::remove_file(&frame_path).await?;
                let mut guard = self.next_recv_seq.lock().await;
                *guard = guard.saturating_add(1);
                return decode_transport_frame(&payload, self.shared_key.as_ref());
            }
            sleep(POLL_INTERVAL).await;
        }
    }
}

pub async fn bind(endpoint: &str) -> Result<String, Error> {
    let parsed = ParsedUrl::parse(endpoint)?;
    let root = storage_root(&parsed);
    let channel = channel_dir(&root, &parsed);
    fs::create_dir_all(channel.join("c2s")).await?;
    fs::create_dir_all(channel.join("s2c")).await?;
    Ok(endpoint.to_string())
}

pub async fn accept_peer_on(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<ActiveSimplexOssPeer, Error> {
    let parsed = ParsedUrl::parse(endpoint)?;
    let channel = channel_dir(&storage_root(&parsed), &parsed);
    fs::create_dir_all(channel.join("c2s")).await?;
    fs::create_dir_all(channel.join("s2c")).await?;
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);

    let placeholder = ActiveSimplexOssPeer {
        session: PeerSession {
            local: PeerInfo {
                agent_id: identity.id.clone(),
                agent_name: identity.name.clone(),
                capabilities: identity.capability_labels(),
            },
            remote: PeerInfo {
                agent_id: String::new(),
                agent_name: String::new(),
                capabilities: vec!["transport:simplex-oss".into()],
            },
            state: SessionState::Active,
        },
        send_dir: channel.join("s2c"),
        recv_dir: channel.join("c2s"),
        next_send_seq: std::sync::Arc::new(tokio::sync::Mutex::new(0)),
        next_recv_seq: std::sync::Arc::new(tokio::sync::Mutex::new(0)),
        shared_key: shared_key.clone(),
    };

    let hello = placeholder.read_frame().await?;
    let session = complete_session(&identity, &hello)?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    let peer = ActiveSimplexOssPeer { session, ..placeholder };
    peer.send_frame(&ack).await?;

    let heartbeat = peer.read_frame().await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }
    let heartbeat_ack = heartbeat_frame(
        identity.id.clone(),
        Some(peer.session.remote.agent_id.clone()),
    );
    peer.send_frame(&heartbeat_ack).await?;
    Ok(peer)
}

pub async fn connect_peer(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<ActiveSimplexOssPeer, Error> {
    let parsed = ParsedUrl::parse(endpoint)?;
    let channel = channel_dir(&storage_root(&parsed), &parsed);
    fs::create_dir_all(channel.join("c2s")).await?;
    fs::create_dir_all(channel.join("s2c")).await?;
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);

    let peer = ActiveSimplexOssPeer {
        session: PeerSession {
            local: PeerInfo {
                agent_id: identity.id.clone(),
                agent_name: identity.name.clone(),
                capabilities: identity.capability_labels(),
            },
            remote: PeerInfo {
                agent_id: String::new(),
                agent_name: String::new(),
                capabilities: vec!["transport:simplex-oss".into()],
            },
            state: SessionState::Active,
        },
        send_dir: channel.join("c2s"),
        recv_dir: channel.join("s2c"),
        next_send_seq: std::sync::Arc::new(tokio::sync::Mutex::new(0)),
        next_recv_seq: std::sync::Arc::new(tokio::sync::Mutex::new(0)),
        shared_key,
    };

    let hello = hello_frame(&identity);
    peer.send_frame(&hello).await?;
    let ack = peer.read_frame().await?;
    match &ack.message {
        Message::HelloAck(msg) if msg.accepted => {}
        _ => {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "peer rejected hello handshake",
            ))
        }
    }

    let mut session = peer.session.clone();
    session.remote.agent_id = ack.header.src_agent.clone().unwrap_or_default();
    session.remote.agent_name = ack.header.src_agent.clone().unwrap_or_default();
    let connected = ActiveSimplexOssPeer { session, ..peer };
    let heartbeat = heartbeat_frame(identity.id.clone(), ack.header.src_agent.clone());
    connected.send_frame(&heartbeat).await?;
    let heartbeat_ack = connected.read_frame().await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }
    Ok(connected)
}

pub async fn run_inbound_session_once(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = accept_peer_on(identity, endpoint).await?;
    Ok((peer.session, Vec::new()))
}

pub async fn run_outbound_session_once(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = connect_peer(identity, endpoint).await?;
    Ok((peer.session, Vec::new()))
}

fn storage_root(parsed: &ParsedUrl) -> PathBuf {
    parsed
        .query
        .get("root")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("fusion-simplex-oss"))
}

fn channel_dir(root: &Path, parsed: &ParsedUrl) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(parsed.host.clone().unwrap_or_default());
    hasher.update(parsed.path.as_bytes());
    let id = format!("{:x}", hasher.finalize());
    root.join(id)
}

#[cfg(test)]
mod tests {
    use tokio::fs;

    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, TaskAction, TaskRequestMessage},
        },
    };

    use super::{accept_peer_on, connect_peer};

    #[tokio::test]
    async fn simplex_oss_task_frame_roundtrip() {
        let root = std::env::temp_dir().join(format!(
            "fusion-simplex-oss-test-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let endpoint = format!(
            "simplex+oss://demo/channel?root={}",
            root.display()
        );
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-oss-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("simplex-oss-client".into()),
            key: None,
        });

        let server = tokio::spawn(async move {
            let peer = accept_peer_on(server_identity, &endpoint).await.unwrap();
            let frame = peer.read_frame().await.unwrap();
            match frame.message {
                Message::TaskRequest(req) => assert_eq!(req.task_id, "oss-task-1"),
                other => panic!("unexpected frame: {:?}", other),
            }
        });

        let endpoint = format!(
            "simplex+oss://demo/channel?root={}",
            root.display()
        );
        let peer = connect_peer(client_identity, &endpoint).await.unwrap();
        let frame = Frame::new(
            MessageType::TaskRequest,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::TaskRequest(TaskRequestMessage {
                task_id: "oss-task-1".into(),
                action: TaskAction::Shell,
                args: vec!["echo".into()],
                data_hex: None,
            }),
        );
        peer.send_frame(&frame).await.unwrap();
        server.await.unwrap();
        let _ = fs::remove_dir_all(root).await;
    }
}
