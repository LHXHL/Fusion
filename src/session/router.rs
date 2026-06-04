use crate::{agent::registry::AgentRegistry, protocol::frame::Frame};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    Local,
    Forward { next_hop_agent_id: String },
    DropNoRoute { destination_agent_id: String },
}

pub fn decide_frame_route(
    local_agent_id: &str,
    registry: &AgentRegistry,
    frame: &Frame,
) -> RouteDecision {
    let Some(dst_agent) = frame.header.dst_agent.as_deref() else {
        return RouteDecision::Local;
    };

    if dst_agent == local_agent_id {
        return RouteDecision::Local;
    }

    match registry.next_hop_for(dst_agent) {
        Some(next_hop_agent_id) => RouteDecision::Forward {
            next_hop_agent_id: next_hop_agent_id.to_string(),
        },
        None => RouteDecision::DropNoRoute {
            destination_agent_id: dst_agent.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::{identity::AgentIdentity, registry::AgentRegistry},
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{HelloMessage, Message},
            route::{RouteAnnouncement, RouteHop},
        },
        session::router::{decide_frame_route, RouteDecision},
    };

    #[test]
    fn route_decision_is_local_when_dst_is_self() {
        let identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("router-self".into()),
            key: None,
        });
        let registry = AgentRegistry::new();
        let frame = Frame::new(
            MessageType::Hello,
            Some("peer-a".into()),
            Some(identity.id.clone()),
            Message::Hello(HelloMessage {
                agent_id: "peer-a".into(),
                agent_name: "peer-a".into(),
                capabilities: vec![],
                protocol_version: 1,
            }),
        );
        assert_eq!(
            decide_frame_route(&identity.id, &registry, &frame),
            RouteDecision::Local
        );
    }

    #[test]
    fn route_decision_forwards_when_next_hop_exists() {
        let identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("router-node".into()),
            key: None,
        });
        let mut registry = AgentRegistry::new();
        registry.upsert_route_announcement(&RouteAnnouncement {
            origin_agent_id: "peer-c".into(),
            origin_agent_name: "peer-c-name".into(),
            capabilities: vec!["task:shell".into()],
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
        let frame = Frame::new(
            MessageType::Hello,
            Some("peer-a".into()),
            Some("peer-c".into()),
            Message::Hello(HelloMessage {
                agent_id: "peer-a".into(),
                agent_name: "peer-a".into(),
                capabilities: vec![],
                protocol_version: 1,
            }),
        );
        assert_eq!(
            decide_frame_route(&identity.id, &registry, &frame),
            RouteDecision::Forward {
                next_hop_agent_id: "peer-b".into()
            }
        );
    }

    #[test]
    fn route_decision_drops_when_no_route_exists() {
        let identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("router-drop".into()),
            key: None,
        });
        let registry = AgentRegistry::new();
        let frame = Frame::new(
            MessageType::Hello,
            Some("peer-a".into()),
            Some("peer-z".into()),
            Message::Hello(HelloMessage {
                agent_id: "peer-a".into(),
                agent_name: "peer-a".into(),
                capabilities: vec![],
                protocol_version: 1,
            }),
        );
        assert_eq!(
            decide_frame_route(&identity.id, &registry, &frame),
            RouteDecision::DropNoRoute {
                destination_agent_id: "peer-z".into()
            }
        );
    }
}
