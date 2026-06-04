use serde::{Deserialize, Serialize};

use crate::agent::identity::AgentIdentity;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionState {
    Connecting,
    Handshaking,
    Active,
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerInfo {
    pub agent_id: String,
    pub agent_name: String,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerSession {
    pub local: PeerInfo,
    pub remote: PeerInfo,
    pub state: SessionState,
}

impl PeerSession {
    pub fn new(local: &AgentIdentity, remote: PeerInfo) -> Self {
        Self {
            local: PeerInfo {
                agent_id: local.id.clone(),
                agent_name: local.name.clone(),
                capabilities: local.capability_labels(),
            },
            remote,
            state: SessionState::Active,
        }
    }
}
