use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::session::peer::{PeerSession, SessionState};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionHub {
    sessions: HashMap<String, PeerSession>,
}

impl SessionHub {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&mut self, session: PeerSession) {
        self.sessions
            .insert(session.remote.agent_id.clone(), session);
    }

    pub fn mark_closed(&mut self, peer_id: &str) {
        if let Some(session) = self.sessions.get_mut(peer_id) {
            session.state = SessionState::Closed;
        }
    }

    pub fn count(&self) -> usize {
        self.sessions.len()
    }

    pub fn contains(&self, peer_id: &str) -> bool {
        self.sessions.contains_key(peer_id)
    }

    pub fn summary_lines(&self) -> Vec<String> {
        let mut lines = vec![format!("session.count={}", self.sessions.len())];
        for session in self.sessions.values() {
            lines.push(format!(
                "session.peer={} name={} state={:?}",
                session.remote.agent_id, session.remote.agent_name, session.state
            ));
        }
        lines
    }

    pub fn sessions_snapshot(&self) -> Vec<PeerSession> {
        let mut sessions: Vec<_> = self.sessions.values().cloned().collect();
        sessions.sort_by(|a, b| a.remote.agent_id.cmp(&b.remote.agent_id));
        sessions
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        session::{
            hub::SessionHub,
            peer::{PeerInfo, PeerSession},
        },
    };

    #[test]
    fn upsert_tracks_peer_session() {
        let local = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("node-a".to_string()),
            key: None,
        });
        let remote = PeerInfo {
            agent_id: "peer-1".to_string(),
            agent_name: "peer-name".to_string(),
            capabilities: vec!["transport:tcp".to_string()],
        };

        let mut hub = SessionHub::new();
        hub.upsert(PeerSession::new(&local, remote));

        assert_eq!(hub.count(), 1);
        assert!(hub.contains("peer-1"));
    }
}
