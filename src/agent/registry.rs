use std::{collections::HashMap, time::SystemTime};

use serde::{Deserialize, Serialize};

use crate::{
    error::report_route_switched,
    protocol::{
        message::AgentAnnounceMessage,
        route::{RouteAnnouncement, RouteHop},
        stream::StreamLifecycle,
    },
    session::peer::{PeerSession, SessionState},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisteredStream {
    pub stream_id: u32,
    pub peer_agent_id: String,
    pub service: String,
    pub target: String,
    pub state: StreamLifecycle,
    pub opened_at_unix: u64,
    pub closed_at_unix: Option<u64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisteredPeer {
    pub session: PeerSession,
    pub last_seen_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisteredRoute {
    pub destination_agent_id: String,
    pub destination_agent_name: String,
    pub next_hop_agent_id: String,
    pub hop_count: usize,
    #[serde(default)]
    pub path: Vec<RouteHop>,
    pub services: Vec<String>,
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub learned_from: String,
    #[serde(default)]
    pub selection_reason: String,
    pub learned_at_unix: u64,
    #[serde(default)]
    pub last_success_at_unix: Option<u64>,
}

#[derive(Debug, Default, Clone)]
pub struct AgentRegistry {
    peers: HashMap<String, RegisteredPeer>,
    streams: HashMap<u32, RegisteredStream>,
    routes: HashMap<String, RegisteredRoute>,
    local_services: Vec<String>,
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_local_services(&mut self, services: Vec<String>) {
        self.local_services = services;
    }

    pub fn upsert_peer(&mut self, session: PeerSession) {
        let peer_id = session.remote.agent_id.clone();
        self.peers.insert(
            peer_id,
            RegisteredPeer {
                session,
                last_seen_unix: unix_now(),
            },
        );
    }

    pub fn mark_peer_closed(&mut self, peer_id: &str) {
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.session.state = SessionState::Closed;
            peer.last_seen_unix = unix_now();
        }
    }

    pub fn remove_peer_state(&mut self, peer_id: &str) {
        self.mark_peer_closed(peer_id);
        self.peers.remove(peer_id);
        self.routes.retain(|destination_agent_id, route| {
            destination_agent_id != peer_id && route.next_hop_agent_id != peer_id
        });
        for stream in self.streams.values_mut() {
            if stream.peer_agent_id == peer_id
                && matches!(
                    stream.state,
                    StreamLifecycle::Opening | StreamLifecycle::Active | StreamLifecycle::Closing
                )
            {
                stream.state = StreamLifecycle::Closed;
                stream.closed_at_unix = Some(unix_now());
                if stream.last_error.is_none() {
                    stream.last_error = Some("peer disconnected".to_string());
                }
            }
        }
    }

    pub fn upsert_announce(&mut self, announce: &AgentAnnounceMessage, next_hop_agent_id: &str) {
        self.upsert_route(RegisteredRoute {
            destination_agent_id: announce.agent_id.clone(),
            destination_agent_name: announce.agent_name.clone(),
            next_hop_agent_id: next_hop_agent_id.to_string(),
            hop_count: 1,
            path: vec![RouteHop {
                agent_id: next_hop_agent_id.to_string(),
                agent_name: announce.agent_name.clone(),
            }],
            services: announce.services.clone(),
            capabilities: announce.capabilities.clone(),
            learned_from: "direct_announce".to_string(),
            selection_reason: "new_destination".to_string(),
            learned_at_unix: unix_now(),
            last_success_at_unix: None,
        });
    }

    pub fn upsert_route_announcement(&mut self, announcement: &RouteAnnouncement) {
        let Some(next_hop) = announcement.direct_next_hop() else {
            return;
        };
        self.upsert_route(RegisteredRoute {
            destination_agent_id: announcement.origin_agent_id.clone(),
            destination_agent_name: announcement.origin_agent_name.clone(),
            next_hop_agent_id: next_hop.agent_id.clone(),
            hop_count: announcement.path.len(),
            path: announcement.path.clone(),
            services: announcement.services.clone(),
            capabilities: announcement.capabilities.clone(),
            learned_from: "route_update".to_string(),
            selection_reason: "new_destination".to_string(),
            learned_at_unix: unix_now(),
            last_success_at_unix: None,
        });
    }

    pub fn next_hop_for(&self, destination_agent_id: &str) -> Option<&str> {
        self.routes
            .get(destination_agent_id)
            .map(|route| route.next_hop_agent_id.as_str())
    }

    pub fn mark_route_forward_success(&mut self, destination_agent_id: &str) {
        if let Some(route) = self.routes.get_mut(destination_agent_id) {
            route.last_success_at_unix = Some(unix_now());
        }
    }

    pub fn prune_stale_routes(&mut self, max_age_secs: u64) -> usize {
        let now = unix_now();
        let before = self.routes.len();
        self.routes
            .retain(|_, route| now.saturating_sub(route.learned_at_unix) <= max_age_secs);
        before.saturating_sub(self.routes.len())
    }

    pub fn routes_snapshot(&self) -> Vec<RegisteredRoute> {
        let mut routes: Vec<_> = self.routes.values().cloned().collect();
        routes.sort_by(|a, b| a.destination_agent_id.cmp(&b.destination_agent_id));
        routes
    }

    pub fn peers_snapshot(&self) -> Vec<RegisteredPeer> {
        let mut peers: Vec<_> = self.peers.values().cloned().collect();
        peers.sort_by(|a, b| a.session.remote.agent_id.cmp(&b.session.remote.agent_id));
        peers
    }

    pub fn is_peer_active(&self, peer_id: &str) -> bool {
        self.peers
            .get(peer_id)
            .map(|peer| matches!(peer.session.state, SessionState::Active))
            .unwrap_or(false)
    }

    pub fn streams_snapshot(&self) -> Vec<RegisteredStream> {
        let mut streams: Vec<_> = self.streams.values().cloned().collect();
        streams.sort_by_key(|stream| stream.stream_id);
        streams
    }

    pub fn local_services_snapshot(&self) -> Vec<String> {
        let mut services = self.local_services.clone();
        services.sort();
        services
    }

    pub fn open_stream(
        &mut self,
        stream_id: u32,
        peer_agent_id: String,
        service: String,
        target: String,
    ) {
        self.streams.insert(
            stream_id,
            RegisteredStream {
                stream_id,
                peer_agent_id,
                service,
                target,
                state: StreamLifecycle::Opening,
                opened_at_unix: unix_now(),
                closed_at_unix: None,
                last_error: None,
            },
        );
    }

    pub fn mark_stream_active(&mut self, stream_id: u32) {
        self.set_stream_state(stream_id, StreamLifecycle::Active, None);
    }

    pub fn mark_stream_closing(&mut self, stream_id: u32) {
        self.set_stream_state(stream_id, StreamLifecycle::Closing, None);
    }

    pub fn mark_stream_closed(&mut self, stream_id: u32) {
        self.set_stream_state(stream_id, StreamLifecycle::Closed, None);
    }

    pub fn mark_stream_failed(&mut self, stream_id: u32, error: impl Into<String>) {
        self.set_stream_state(stream_id, StreamLifecycle::Failed, Some(error.into()));
    }

    fn set_stream_state(&mut self, stream_id: u32, state: StreamLifecycle, error: Option<String>) {
        if let Some(stream) = self.streams.get_mut(&stream_id) {
            stream.state = state.clone();
            match state {
                StreamLifecycle::Closed | StreamLifecycle::Failed => {
                    stream.closed_at_unix = Some(unix_now());
                }
                StreamLifecycle::Opening | StreamLifecycle::Active | StreamLifecycle::Closing => {
                    stream.closed_at_unix = None;
                }
            }
            if let Some(error) = error {
                stream.last_error = Some(error);
            }
        }
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    pub fn route_count(&self) -> usize {
        self.routes.len()
    }

    pub fn active_stream_count(&self) -> usize {
        self.streams
            .values()
            .filter(|stream| {
                matches!(
                    stream.state,
                    StreamLifecycle::Opening | StreamLifecycle::Active | StreamLifecycle::Closing
                )
            })
            .count()
    }

    pub fn summary_lines(&self) -> Vec<String> {
        let mut lines = vec![
            format!("registry.peer_count={}", self.peer_count()),
            format!("registry.stream_count={}", self.stream_count()),
            format!("registry.route_count={}", self.route_count()),
            format!("registry.local_service_count={}", self.local_services.len()),
            format!(
                "registry.active_stream_count={}",
                self.active_stream_count()
            ),
        ];

        for service in &self.local_services {
            lines.push(format!("registry.local_service={service}"));
        }

        let mut peers: Vec<_> = self.peers.values().cloned().collect();
        peers.sort_by(|a, b| a.session.remote.agent_id.cmp(&b.session.remote.agent_id));
        for peer in peers {
            lines.push(format!(
                "registry.peer={} name={} state={:?}",
                peer.session.remote.agent_id, peer.session.remote.agent_name, peer.session.state
            ));
        }

        let mut routes: Vec<_> = self.routes.values().cloned().collect();
        routes.sort_by(|a, b| a.destination_agent_id.cmp(&b.destination_agent_id));
        for route in routes {
            let path = route
                .path
                .iter()
                .map(|hop| hop.agent_id.as_str())
                .collect::<Vec<_>>()
                .join(">");
            lines.push(format!(
                "registry.route={} next_hop={} hops={} path={} services={} learned_from={} selected_by={}",
                route.destination_agent_id,
                route.next_hop_agent_id,
                route.hop_count,
                path,
                route.services.join(","),
                route.learned_from,
                route.selection_reason
            ));
        }

        let mut streams: Vec<_> = self.streams.values().cloned().collect();
        streams.sort_by_key(|stream| stream.stream_id);
        for stream in streams {
            lines.push(format!(
                "registry.stream={} peer={} service={} target={} state={:?}",
                stream.stream_id, stream.peer_agent_id, stream.service, stream.target, stream.state
            ));
        }

        lines
    }
}

impl AgentRegistry {
    fn upsert_route(&mut self, candidate: RegisteredRoute) {
        let destination = candidate.destination_agent_id.clone();
        match self.routes.get(&destination) {
            Some(existing) => {
                let decision = choose_route_update(existing, &candidate, |peer_id| {
                    self.is_peer_active(peer_id)
                });
                match decision {
                    RouteUpdateDecision::KeepExisting => {}
                    RouteUpdateDecision::Replace {
                        mut candidate,
                        selection_reason,
                    } => {
                        if existing.next_hop_agent_id != candidate.next_hop_agent_id {
                            report_route_switched(
                                &destination,
                                &existing.next_hop_agent_id,
                                &candidate.next_hop_agent_id,
                                selection_reason,
                            );
                        }
                        candidate.selection_reason = selection_reason.to_string();
                        if selection_reason == "same_next_hop_refresh" {
                            candidate.learned_at_unix = existing.learned_at_unix;
                            candidate.last_success_at_unix = existing.last_success_at_unix;
                        } else {
                            candidate.learned_at_unix = unix_now();
                            candidate.last_success_at_unix = candidate
                                .last_success_at_unix
                                .or(existing.last_success_at_unix);
                        }
                        self.routes.insert(destination, candidate);
                    }
                }
            }
            _ => {
                self.routes.insert(destination, candidate);
            }
        }
    }
}

enum RouteUpdateDecision {
    KeepExisting,
    Replace {
        candidate: RegisteredRoute,
        selection_reason: &'static str,
    },
}

fn choose_route_update<F>(
    existing: &RegisteredRoute,
    candidate: &RegisteredRoute,
    is_peer_active: F,
) -> RouteUpdateDecision
where
    F: Fn(&str) -> bool,
{
    if candidate.next_hop_agent_id == existing.next_hop_agent_id {
        let mut refreshed = candidate.clone();
        refreshed.selection_reason = "same_next_hop_refresh".to_string();
        refreshed.last_success_at_unix = existing.last_success_at_unix;
        return RouteUpdateDecision::Replace {
            candidate: refreshed,
            selection_reason: "same_next_hop_refresh",
        };
    }

    let existing_active = is_peer_active(&existing.next_hop_agent_id);
    let candidate_active = is_peer_active(&candidate.next_hop_agent_id);
    if candidate_active && !existing_active {
        let mut replacement = candidate.clone();
        replacement.selection_reason = "prefer_active_next_hop".to_string();
        return RouteUpdateDecision::Replace {
            candidate: replacement,
            selection_reason: "prefer_active_next_hop",
        };
    }
    if existing_active && !candidate_active {
        return RouteUpdateDecision::KeepExisting;
    }

    if candidate.hop_count < existing.hop_count {
        let mut replacement = candidate.clone();
        replacement.selection_reason = "shorter_path".to_string();
        return RouteUpdateDecision::Replace {
            candidate: replacement,
            selection_reason: "shorter_path",
        };
    }
    if candidate.hop_count > existing.hop_count {
        return RouteUpdateDecision::KeepExisting;
    }

    match (
        existing.last_success_at_unix,
        candidate.last_success_at_unix,
    ) {
        (Some(existing_ts), Some(candidate_ts)) if candidate_ts > existing_ts => {
            let mut replacement = candidate.clone();
            replacement.selection_reason = "prefer_recent_success".to_string();
            return RouteUpdateDecision::Replace {
                candidate: replacement,
                selection_reason: "prefer_recent_success",
            };
        }
        (Some(existing_ts), Some(candidate_ts)) if existing_ts > candidate_ts => {
            return RouteUpdateDecision::KeepExisting;
        }
        (None, Some(_)) => {
            let mut replacement = candidate.clone();
            replacement.selection_reason = "prefer_recent_success".to_string();
            return RouteUpdateDecision::Replace {
                candidate: replacement,
                selection_reason: "prefer_recent_success",
            };
        }
        (Some(_), None) => {
            return RouteUpdateDecision::KeepExisting;
        }
        _ => {}
    }

    if candidate.next_hop_agent_id < existing.next_hop_agent_id {
        let mut replacement = candidate.clone();
        replacement.selection_reason = "stable_tie_break".to_string();
        return RouteUpdateDecision::Replace {
            candidate: replacement,
            selection_reason: "stable_tie_break",
        };
    }

    RouteUpdateDecision::KeepExisting
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::{identity::AgentIdentity, registry::AgentRegistry},
        app::config::AgentIdentityConfig,
        protocol::{
            message::AgentAnnounceMessage,
            route::{RouteAnnouncement, RouteHop},
            stream::StreamLifecycle,
        },
        session::peer::{PeerInfo, PeerSession, SessionState},
    };

    #[test]
    fn registry_tracks_peer_and_stream_lifecycle() {
        let local = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("registry-local".into()),
            key: None,
        });
        let remote = PeerInfo {
            agent_id: "peer-1".into(),
            agent_name: "peer-name".into(),
            capabilities: vec!["transport:tcp".into()],
        };
        let session = PeerSession::new(&local, remote);

        let mut registry = AgentRegistry::new();
        registry.upsert_peer(session);
        registry.open_stream(1, "peer-1".into(), "raw".into(), "example.com:80".into());
        registry.mark_stream_active(1);
        registry.mark_stream_closing(1);
        registry.mark_stream_closed(1);
        registry.mark_peer_closed("peer-1");

        assert_eq!(registry.peer_count(), 1);
        assert_eq!(registry.stream_count(), 1);
        assert_eq!(registry.active_stream_count(), 0);
        let stream = registry.summary_lines().join("\n");
        assert!(stream.contains("registry.peer=peer-1"));
        assert!(stream.contains("registry.stream=1"));
        assert!(matches!(
            registry.clone().streams.get(&1).unwrap().state,
            StreamLifecycle::Closed
        ));
        assert!(matches!(
            registry.clone().peers.get("peer-1").unwrap().session.state,
            SessionState::Closed
        ));
        assert!(!registry.is_peer_active("peer-1"));
    }

    #[test]
    fn registry_marks_stream_failure() {
        let mut registry = AgentRegistry::new();
        registry.open_stream(9, "peer-x".into(), "socks5".into(), "dynamic".into());
        registry.mark_stream_failed(9, "boom");
        let summary = registry.summary_lines().join("\n");
        assert!(summary.contains("registry.stream=9"));
        assert!(summary.contains("state=Failed"));
        let stream = registry.clone().streams.get(&9).unwrap().clone();
        assert!(stream.closed_at_unix.is_some());
        assert_eq!(stream.last_error.as_deref(), Some("boom"));
    }

    #[test]
    fn registry_clears_closed_at_when_stream_reactivates() {
        let mut registry = AgentRegistry::new();
        registry.open_stream(15, "peer-y".into(), "raw".into(), "example.com:80".into());
        registry.mark_stream_failed(15, "temporary");
        assert!(registry
            .clone()
            .streams
            .get(&15)
            .unwrap()
            .closed_at_unix
            .is_some());
        registry.mark_stream_active(15);
        let stream = registry.clone().streams.get(&15).unwrap().clone();
        assert!(matches!(stream.state, StreamLifecycle::Active));
        assert!(stream.closed_at_unix.is_none());
    }

    #[test]
    fn registry_tracks_direct_and_forwarded_routes() {
        let mut registry = AgentRegistry::new();
        registry.set_local_services(vec!["socks5://127.0.0.1:1080".into()]);
        registry.upsert_announce(
            &AgentAnnounceMessage {
                agent_id: "peer-a".into(),
                agent_name: "peer-a-name".into(),
                capabilities: vec!["task:shell".into()],
                services: vec!["raw://dynamic".into()],
            },
            "peer-a",
        );
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-c".into(),
            origin_agent_name: "peer-c-name".into(),
            capabilities: vec!["task:file-download".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-c".into(),
                    agent_name: "peer-c-name".into(),
                },
            ],
        });

        assert_eq!(registry.route_count(), 2);
        assert_eq!(registry.next_hop_for("peer-a"), Some("peer-a"));
        assert_eq!(registry.next_hop_for("peer-c"), Some("peer-b"));
        assert_eq!(
            registry.clone().routes.get("peer-c").unwrap().path,
            vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-c".into(),
                    agent_name: "peer-c-name".into(),
                },
            ]
        );
        let summary = registry.summary_lines().join("\n");
        assert!(summary.contains("registry.route=peer-c next_hop=peer-b"));
        assert!(summary.contains("learned_from=route_update"));
        assert!(summary.contains("selected_by=new_destination"));
        assert!(summary.contains("registry.local_service=socks5://127.0.0.1:1080"));
    }

    #[test]
    fn registry_removes_routes_and_closes_streams_when_peer_is_removed() {
        let local = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("registry-cleanup-local".into()),
            key: None,
        });
        let remote = PeerInfo {
            agent_id: "peer-b".into(),
            agent_name: "peer-b-name".into(),
            capabilities: vec!["transport:tcp".into()],
        };
        let session = PeerSession::new(&local, remote);

        let mut registry = AgentRegistry::new();
        registry.upsert_peer(session);
        registry.upsert_announce(
            &AgentAnnounceMessage {
                agent_id: "peer-b".into(),
                agent_name: "peer-b-name".into(),
                capabilities: vec!["task:shell".into()],
                services: vec!["raw://dynamic".into()],
            },
            "peer-b",
        );
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-c".into(),
            origin_agent_name: "peer-c-name".into(),
            capabilities: vec!["task:file-download".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-c".into(),
                    agent_name: "peer-c-name".into(),
                },
            ],
        });
        registry.open_stream(11, "peer-b".into(), "raw".into(), "dynamic".into());
        registry.mark_stream_active(11);

        registry.remove_peer_state("peer-b");

        assert_eq!(registry.peer_count(), 0);
        assert_eq!(registry.route_count(), 0);
        assert_eq!(registry.next_hop_for("peer-c"), None);
        assert_eq!(registry.active_stream_count(), 0);
        let summary = registry.summary_lines().join("\n");
        assert!(summary.contains("registry.stream=11"));
        assert!(summary.contains("state=Closed"));
    }

    #[test]
    fn registry_prunes_stale_routes() {
        let mut registry = AgentRegistry::new();
        registry.upsert_announce(
            &AgentAnnounceMessage {
                agent_id: "peer-live".into(),
                agent_name: "peer-live-name".into(),
                capabilities: vec!["task:shell".into()],
                services: vec!["raw://dynamic".into()],
            },
            "peer-live",
        );
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-stale".into(),
            origin_agent_name: "peer-stale-name".into(),
            capabilities: vec!["task:file-download".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "relay-x".into(),
                    agent_name: "relay-x-name".into(),
                },
                RouteHop {
                    agent_id: "peer-stale".into(),
                    agent_name: "peer-stale-name".into(),
                },
            ],
        });

        {
            let stale = registry.routes.get_mut("peer-stale").unwrap();
            stale.learned_at_unix = stale.learned_at_unix.saturating_sub(3600);
        }

        let removed = registry.prune_stale_routes(60);
        assert_eq!(removed, 1);
        assert_eq!(registry.route_count(), 1);
        assert_eq!(registry.next_hop_for("peer-live"), Some("peer-live"));
        assert_eq!(registry.next_hop_for("peer-stale"), None);
    }

    #[test]
    fn registry_prefers_shorter_route_and_keeps_tie_break_stable() {
        let mut registry = AgentRegistry::new();
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-c".into(),
                    agent_name: "peer-c-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });
        assert_eq!(registry.next_hop_for("peer-z"), Some("peer-b"));
        assert_eq!(
            registry
                .clone()
                .routes
                .get("peer-z")
                .unwrap()
                .selection_reason,
            "stable_tie_break"
        );

        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-d".into(),
                    agent_name: "peer-d-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });
        assert_eq!(registry.next_hop_for("peer-z"), Some("peer-b"));
    }

    #[test]
    fn registry_prefers_active_next_hop_over_inactive_shorter_peer() {
        let local = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("registry-route-active".into()),
            key: None,
        });
        let mut registry = AgentRegistry::new();

        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });
        assert_eq!(registry.next_hop_for("peer-z"), Some("peer-b"));

        let active_session = PeerSession::new(
            &local,
            PeerInfo {
                agent_id: "peer-c".into(),
                agent_name: "peer-c-name".into(),
                capabilities: vec!["transport:tcp".into()],
            },
        );
        registry.upsert_peer(active_session);
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-c".into(),
                    agent_name: "peer-c-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });

        assert_eq!(registry.next_hop_for("peer-z"), Some("peer-c"));
        let route = registry.clone().routes.get("peer-z").unwrap().clone();
        assert_eq!(route.selection_reason, "prefer_active_next_hop");
        assert_eq!(route.learned_from, "route_update");
    }

    #[test]
    fn registry_recovers_route_after_next_hop_disconnect_and_refresh() {
        let mut registry = AgentRegistry::new();
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });
        assert_eq!(registry.next_hop_for("peer-z"), Some("peer-b"));

        registry.remove_peer_state("peer-b");
        assert_eq!(registry.next_hop_for("peer-z"), None);

        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-c".into(),
                    agent_name: "peer-c-name".into(),
                },
                RouteHop {
                    agent_id: "peer-y".into(),
                    agent_name: "peer-y-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });
        assert_eq!(registry.next_hop_for("peer-z"), Some("peer-c"));
    }

    #[test]
    fn registry_records_route_switch_in_recent_errors() {
        use crate::error::{
            clear_recent_errors_for_tests, recent_error_test_guard, recent_errors_snapshot,
        };

        let _guard = recent_error_test_guard();
        clear_recent_errors_for_tests();
        let mut registry = AgentRegistry::new();
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-x".into(),
                    agent_name: "peer-x-name".into(),
                },
                RouteHop {
                    agent_id: "peer-y".into(),
                    agent_name: "peer-y-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-c".into(),
                    agent_name: "peer-c-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });

        assert_eq!(registry.next_hop_for("peer-z"), Some("peer-c"));
        assert!(recent_errors_snapshot()
            .iter()
            .any(|entry| entry.code == "route.switched"));
    }

    #[test]
    fn registry_prefers_recent_success_over_lex_tie_break() {
        let mut registry = AgentRegistry::new();
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });
        registry.mark_route_forward_success("peer-z");
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-a".into(),
                    agent_name: "peer-a-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });

        assert_eq!(registry.next_hop_for("peer-z"), Some("peer-b"));
    }

    #[test]
    fn registry_keeps_route_stable_on_equivalent_reannounce() {
        let mut registry = AgentRegistry::new();
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });
        let learned_at = registry
            .clone()
            .routes
            .get("peer-z")
            .unwrap()
            .learned_at_unix;
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });
        let route = registry.clone().routes.get("peer-z").unwrap().clone();
        assert_eq!(registry.next_hop_for("peer-z"), Some("peer-b"));
        assert_eq!(route.selection_reason, "same_next_hop_refresh");
        assert_eq!(route.learned_at_unix, learned_at);
    }

    #[test]
    fn registry_drops_stale_route_and_stops_forwarding() {
        let mut registry = AgentRegistry::new();
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        });
        {
            let stale = registry.routes.get_mut("peer-z").unwrap();
            stale.learned_at_unix = stale.learned_at_unix.saturating_sub(3600);
        }
        registry.prune_stale_routes(60);
        assert_eq!(registry.next_hop_for("peer-z"), None);
    }

    #[test]
    fn registry_multi_exit_selection_stays_stable_on_alternating_announces() {
        let mut registry = AgentRegistry::new();
        let peer_a_route = RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-a".into(),
                    agent_name: "peer-a-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        };
        let peer_b_route = RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        };

        registry.upsert_route_announcement(&peer_b_route);
        registry.upsert_route_announcement(&peer_a_route);
        assert_eq!(registry.next_hop_for("peer-z"), Some("peer-a"));

        for _ in 0..4 {
            registry.upsert_route_announcement(&peer_b_route);
            registry.upsert_route_announcement(&peer_a_route);
        }
        assert_eq!(registry.next_hop_for("peer-z"), Some("peer-a"));
        assert_eq!(
            registry.routes.get("peer-z").unwrap().selection_reason,
            "same_next_hop_refresh"
        );
    }
}
