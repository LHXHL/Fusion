use std::collections::HashSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteHop {
    pub agent_id: String,
    pub agent_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteAnnouncement {
    pub origin_agent_id: String,
    pub origin_agent_name: String,
    pub capabilities: Vec<String>,
    pub services: Vec<String>,
    pub path: Vec<RouteHop>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteUpdateMessage {
    pub announcements: Vec<RouteAnnouncement>,
}

impl RouteAnnouncement {
    pub fn direct_next_hop(&self) -> Option<&RouteHop> {
        self.path.first()
    }

    pub fn has_loop(&self) -> bool {
        let mut seen = HashSet::new();
        self.path
            .iter()
            .any(|hop| !seen.insert(hop.agent_id.as_str()))
    }

    pub fn contains_agent(&self, agent_id: &str) -> bool {
        self.path.iter().any(|hop| hop.agent_id == agent_id)
    }

    pub fn ends_at_origin(&self) -> bool {
        self.path
            .last()
            .map(|hop| hop.agent_id == self.origin_agent_id)
            .unwrap_or(false)
    }
}
