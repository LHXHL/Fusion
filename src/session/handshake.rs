use std::io::{Error, ErrorKind};

use crate::{
    agent::identity::AgentIdentity,
    protocol::{
        frame::{Frame, MessageType},
        message::{HelloAckMessage, HelloMessage, Message},
    },
    session::peer::{PeerInfo, PeerSession},
};

pub fn hello_frame(identity: &AgentIdentity) -> Frame {
    Frame::new(
        MessageType::Hello,
        Some(identity.id.clone()),
        None,
        Message::Hello(HelloMessage {
            agent_id: identity.id.clone(),
            agent_name: identity.name.clone(),
            capabilities: identity.capability_labels(),
            protocol_version: 1,
        }),
    )
}

pub fn hello_ack_frame(identity: &AgentIdentity, dst_agent: Option<String>) -> Frame {
    Frame::new(
        MessageType::HelloAck,
        Some(identity.id.clone()),
        dst_agent,
        Message::HelloAck(HelloAckMessage {
            accepted: true,
            peer_id: identity.id.clone(),
        }),
    )
}

pub fn peer_from_hello(frame: &Frame) -> Result<PeerInfo, Error> {
    match &frame.message {
        Message::Hello(msg) => Ok(PeerInfo {
            agent_id: msg.agent_id.clone(),
            agent_name: msg.agent_name.clone(),
            capabilities: msg.capabilities.clone(),
        }),
        _ => Err(Error::new(
            ErrorKind::InvalidData,
            "expected hello frame during handshake",
        )),
    }
}

pub fn complete_session(local: &AgentIdentity, hello: &Frame) -> Result<PeerSession, Error> {
    let remote = peer_from_hello(hello)?;
    Ok(PeerSession::new(local, remote))
}
