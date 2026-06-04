use serde::{Deserialize, Serialize};

use crate::{
    agent::registry::{RegisteredPeer, RegisteredRoute, RegisteredStream},
    session::peer::PeerSession,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentRuntimeState {
    pub sessions: Vec<PeerSession>,
    pub peers: Vec<RegisteredPeer>,
    pub routes: Vec<RegisteredRoute>,
    pub streams: Vec<RegisteredStream>,
    pub exposed_services: Vec<String>,
}

impl AgentRuntimeState {
    pub fn summary_lines(&self) -> Vec<String> {
        vec![
            format!("state.session_count={}", self.sessions.len()),
            format!("state.peer_count={}", self.peers.len()),
            format!("state.route_count={}", self.routes.len()),
            format!("state.stream_count={}", self.streams.len()),
            format!("state.exposed_service_count={}", self.exposed_services.len()),
        ]
    }
}
