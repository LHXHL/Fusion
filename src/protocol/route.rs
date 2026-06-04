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
}
