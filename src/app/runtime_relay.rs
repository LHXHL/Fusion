use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{
    agent::{
        identity::AgentIdentity,
        registry::{AgentRegistry, RegisteredRoute},
    },
    app::runtime_status::{remove_relay_link, upsert_relay_link, RelayLinkMap},
    protocol::{
        frame::{Frame, MessageType},
        message::{AgentAnnounceMessage, Message},
        route::{RouteAnnouncement, RouteHop, RouteUpdateMessage},
    },
    tunnel::{
        simplex_dns, simplex_dns_mux, simplex_http, simplex_http_mux, simplex_oss,
        simplex_oss_mux, tcp_mux, ws_mux,
    },
};

pub const ROUTE_TTL_SECS: u64 = 300;

fn build_local_announce(identity: &AgentIdentity, services: &[String]) -> AgentAnnounceMessage {
    AgentAnnounceMessage {
        agent_id: identity.id.clone(),
        agent_name: identity.name.clone(),
        capabilities: identity.capability_labels(),
        services: services.to_vec(),
    }
}

fn build_direct_route_update(identity: &AgentIdentity, services: &[String]) -> RouteUpdateMessage {
    RouteUpdateMessage {
        announcements: vec![RouteAnnouncement {
            origin_agent_id: identity.id.clone(),
            origin_agent_name: identity.name.clone(),
            capabilities: identity.capability_labels(),
            services: services.to_vec(),
            path: vec![RouteHop {
                agent_id: identity.id.clone(),
                agent_name: identity.name.clone(),
            }],
        }],
    }
}

pub async fn send_direct_announce_ws_mux(
    peer: &ws_mux::MuxWsPeer,
    identity: &AgentIdentity,
    services: &[String],
) -> Result<(), Error> {
    let announce = Frame::new(
        MessageType::AgentAnnounce,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::AgentAnnounce(build_local_announce(identity, services)),
    );
    peer.send_frame(&announce).await?;
    let route = Frame::new(
        MessageType::RouteUpdate,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::RouteUpdate(build_direct_route_update(identity, services)),
    );
    peer.send_frame(&route).await
}

pub async fn send_direct_announce_simplex_http(
    peer: &simplex_http::ActiveSimplexHttpPeer,
    identity: &AgentIdentity,
    services: &[String],
) -> Result<(), Error> {
    let announce = Frame::new(
        MessageType::AgentAnnounce,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::AgentAnnounce(build_local_announce(identity, services)),
    );
    peer.send_frame(&announce).await?;
    let route = Frame::new(
        MessageType::RouteUpdate,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::RouteUpdate(build_direct_route_update(identity, services)),
    );
    peer.send_frame(&route).await
}

pub async fn send_direct_announce_simplex_oss(
    peer: &simplex_oss::ActiveSimplexOssPeer,
    identity: &AgentIdentity,
    services: &[String],
) -> Result<(), Error> {
    let announce = Frame::new(
        MessageType::AgentAnnounce,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::AgentAnnounce(build_local_announce(identity, services)),
    );
    peer.send_frame(&announce).await?;
    let route = Frame::new(
        MessageType::RouteUpdate,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::RouteUpdate(build_direct_route_update(identity, services)),
    );
    peer.send_frame(&route).await
}

pub async fn send_direct_announce_simplex_dns(
    peer: &simplex_dns::ActiveSimplexDnsPeer,
    identity: &AgentIdentity,
    services: &[String],
) -> Result<(), Error> {
    let announce = Frame::new(
        MessageType::AgentAnnounce,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::AgentAnnounce(build_local_announce(identity, services)),
    );
    peer.send_frame(&announce).await?;
    let route = Frame::new(
        MessageType::RouteUpdate,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::RouteUpdate(build_direct_route_update(identity, services)),
    );
    peer.send_frame(&route).await
}

pub fn handle_registry_control_message(
    registry: &mut AgentRegistry,
    local_agent_id: &str,
    peer_agent_id: &str,
    message: &Message,
) -> bool {
    let pruned = registry.prune_stale_routes(ROUTE_TTL_SECS);
    if pruned > 0 {
        eprintln!("registry.route_pruned={}", pruned);
    }
    match message {
        Message::AgentAnnounce(announce) => {
            registry.upsert_announce(announce, peer_agent_id);
            true
        }
        Message::RouteUpdate(update) => {
            for announcement in &update.announcements {
                if let Err(reason) =
                    validate_route_announcement(local_agent_id, peer_agent_id, announcement)
                {
                    eprintln!(
                        "registry.route_rejected origin={} source_peer={} reason={}",
                        announcement.origin_agent_id, peer_agent_id, reason
                    );
                    continue;
                }
                registry.upsert_route_announcement(announcement);
            }
            true
        }
        _ => false,
    }
}

pub async fn send_direct_announce_tcp_mux(
    peer: &tcp_mux::MuxTcpPeer,
    identity: &AgentIdentity,
    services: &[String],
) -> Result<(), Error> {
    let announce = Frame::new(
        MessageType::AgentAnnounce,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::AgentAnnounce(build_local_announce(identity, services)),
    );
    peer.send_frame(&announce).await?;
    let route = Frame::new(
        MessageType::RouteUpdate,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::RouteUpdate(build_direct_route_update(identity, services)),
    );
    peer.send_frame(&route).await
}

pub async fn send_route_snapshot_tcp_mux(
    peer: &tcp_mux::MuxTcpPeer,
    identity: &AgentIdentity,
    routes: &[RegisteredRoute],
    exclude_destination: Option<String>,
) -> Result<(), Error> {
    let mut route_announcements = Vec::new();
    for route_line in routes {
        if exclude_destination.as_deref() == Some(route_line.destination_agent_id.as_str()) {
            continue;
        }
        route_announcements.push(RouteAnnouncement {
            origin_agent_id: route_line.destination_agent_id.clone(),
            origin_agent_name: route_line.destination_agent_name.clone(),
            capabilities: route_line.capabilities.clone(),
            services: route_line.services.clone(),
            path: prepend_route_path(identity, &route_line.path),
        });
    }
    if route_announcements.is_empty() {
        return Ok(());
    }
    let frame = Frame::new(
        MessageType::RouteUpdate,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::RouteUpdate(RouteUpdateMessage {
            announcements: route_announcements,
        }),
    );
    peer.send_frame(&frame).await
}

fn prepare_forward_frame_for_broadcast(identity: &AgentIdentity, frame: &Frame) -> Frame {
    let mut forwarded = frame.clone();
    forwarded.header.src_agent = Some(identity.id.clone());
    if let Message::RouteUpdate(update) = &mut forwarded.message {
        for announcement in &mut update.announcements {
            announcement.path.insert(
                0,
                RouteHop {
                    agent_id: identity.id.clone(),
                    agent_name: identity.name.clone(),
                },
            );
        }
    }
    forwarded
}

pub async fn broadcast_control_frame_tcp(
    peer_map: &Arc<Mutex<HashMap<String, tcp_mux::MuxTcpPeer>>>,
    identity: &AgentIdentity,
    source_peer_id: &str,
    frame: &Frame,
) -> Result<(), Error> {
    let forwarded = prepare_forward_frame_for_broadcast(identity, frame);
    let peers: Vec<_> = peer_map
        .lock()
        .await
        .iter()
        .filter(|(peer_id, _)| peer_id.as_str() != source_peer_id)
        .map(|(_, peer)| peer.clone())
        .collect();
    for peer in peers {
        peer.send_frame(&forwarded).await?;
    }
    Ok(())
}

pub async fn send_route_snapshot_ws_mux(
    peer: &ws_mux::MuxWsPeer,
    identity: &AgentIdentity,
    routes: &[RegisteredRoute],
    exclude_destination: Option<String>,
) -> Result<(), Error> {
    let mut route_announcements = Vec::new();
    for route_line in routes {
        if exclude_destination.as_deref() == Some(route_line.destination_agent_id.as_str()) {
            continue;
        }
        route_announcements.push(RouteAnnouncement {
            origin_agent_id: route_line.destination_agent_id.clone(),
            origin_agent_name: route_line.destination_agent_name.clone(),
            capabilities: route_line.capabilities.clone(),
            services: route_line.services.clone(),
            path: prepend_route_path(identity, &route_line.path),
        });
    }
    if route_announcements.is_empty() {
        return Ok(());
    }
    let frame = Frame::new(
        MessageType::RouteUpdate,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::RouteUpdate(RouteUpdateMessage {
            announcements: route_announcements,
        }),
    );
    peer.send_frame(&frame).await
}

pub async fn send_route_snapshot_simplex_http_mux(
    peer: &simplex_http_mux::MuxSimplexHttpPeer,
    identity: &AgentIdentity,
    routes: &[RegisteredRoute],
    exclude_destination: Option<String>,
) -> Result<(), Error> {
    let mut route_announcements = Vec::new();
    for route_line in routes {
        if exclude_destination.as_deref() == Some(route_line.destination_agent_id.as_str()) {
            continue;
        }
        route_announcements.push(RouteAnnouncement {
            origin_agent_id: route_line.destination_agent_id.clone(),
            origin_agent_name: route_line.destination_agent_name.clone(),
            capabilities: route_line.capabilities.clone(),
            services: route_line.services.clone(),
            path: prepend_route_path(identity, &route_line.path),
        });
    }
    if route_announcements.is_empty() {
        return Ok(());
    }
    let frame = Frame::new(
        MessageType::RouteUpdate,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::RouteUpdate(RouteUpdateMessage {
            announcements: route_announcements,
        }),
    );
    peer.send_frame(&frame).await
}

pub async fn send_route_snapshot_simplex_oss_mux(
    peer: &simplex_oss_mux::MuxSimplexOssPeer,
    identity: &AgentIdentity,
    routes: &[RegisteredRoute],
    exclude_destination: Option<String>,
) -> Result<(), Error> {
    let mut route_announcements = Vec::new();
    for route_line in routes {
        if exclude_destination.as_deref() == Some(route_line.destination_agent_id.as_str()) {
            continue;
        }
        route_announcements.push(RouteAnnouncement {
            origin_agent_id: route_line.destination_agent_id.clone(),
            origin_agent_name: route_line.destination_agent_name.clone(),
            capabilities: route_line.capabilities.clone(),
            services: route_line.services.clone(),
            path: prepend_route_path(identity, &route_line.path),
        });
    }
    if route_announcements.is_empty() {
        return Ok(());
    }
    let frame = Frame::new(
        MessageType::RouteUpdate,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::RouteUpdate(RouteUpdateMessage {
            announcements: route_announcements,
        }),
    );
    peer.send_frame(&frame).await
}

pub async fn send_route_snapshot_simplex_dns_mux(
    peer: &simplex_dns_mux::MuxSimplexDnsPeer,
    identity: &AgentIdentity,
    routes: &[RegisteredRoute],
    exclude_destination: Option<String>,
) -> Result<(), Error> {
    let mut route_announcements = Vec::new();
    for route_line in routes {
        if exclude_destination.as_deref() == Some(route_line.destination_agent_id.as_str()) {
            continue;
        }
        route_announcements.push(RouteAnnouncement {
            origin_agent_id: route_line.destination_agent_id.clone(),
            origin_agent_name: route_line.destination_agent_name.clone(),
            capabilities: route_line.capabilities.clone(),
            services: route_line.services.clone(),
            path: prepend_route_path(identity, &route_line.path),
        });
    }
    if route_announcements.is_empty() {
        return Ok(());
    }
    let frame = Frame::new(
        MessageType::RouteUpdate,
        Some(identity.id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::RouteUpdate(RouteUpdateMessage {
            announcements: route_announcements,
        }),
    );
    peer.send_frame(&frame).await
}

pub async fn broadcast_control_frame_simplex_http(
    peer_map: &Arc<Mutex<HashMap<String, simplex_http_mux::MuxSimplexHttpPeer>>>,
    identity: &AgentIdentity,
    source_peer_id: &str,
    frame: &Frame,
) -> Result<(), Error> {
    let forwarded = prepare_forward_frame_for_broadcast(identity, frame);
    let peers: Vec<_> = peer_map
        .lock()
        .await
        .iter()
        .filter(|(peer_id, _)| peer_id.as_str() != source_peer_id)
        .map(|(_, peer)| peer.clone())
        .collect();
    for peer in peers {
        peer.send_frame(&forwarded).await?;
    }
    Ok(())
}

pub async fn broadcast_control_frame_simplex_oss(
    peer_map: &Arc<Mutex<HashMap<String, simplex_oss_mux::MuxSimplexOssPeer>>>,
    identity: &AgentIdentity,
    source_peer_id: &str,
    frame: &Frame,
) -> Result<(), Error> {
    let forwarded = prepare_forward_frame_for_broadcast(identity, frame);
    let peers: Vec<_> = peer_map
        .lock()
        .await
        .iter()
        .filter(|(peer_id, _)| peer_id.as_str() != source_peer_id)
        .map(|(_, peer)| peer.clone())
        .collect();
    for peer in peers {
        peer.send_frame(&forwarded).await?;
    }
    Ok(())
}

pub async fn broadcast_control_frame_simplex_dns(
    peer_map: &Arc<Mutex<HashMap<String, simplex_dns_mux::MuxSimplexDnsPeer>>>,
    identity: &AgentIdentity,
    source_peer_id: &str,
    frame: &Frame,
) -> Result<(), Error> {
    let forwarded = prepare_forward_frame_for_broadcast(identity, frame);
    let peers: Vec<_> = peer_map
        .lock()
        .await
        .iter()
        .filter(|(peer_id, _)| peer_id.as_str() != source_peer_id)
        .map(|(_, peer)| peer.clone())
        .collect();
    for peer in peers {
        peer.send_frame(&forwarded).await?;
    }
    Ok(())
}

pub async fn broadcast_control_frame_ws(
    peer_map: &Arc<Mutex<HashMap<String, ws_mux::MuxWsPeer>>>,
    identity: &AgentIdentity,
    source_peer_id: &str,
    frame: &Frame,
) -> Result<(), Error> {
    let forwarded = prepare_forward_frame_for_broadcast(identity, frame);
    let peers: Vec<_> = peer_map
        .lock()
        .await
        .iter()
        .filter(|(peer_id, _)| peer_id.as_str() != source_peer_id)
        .map(|(_, peer)| peer.clone())
        .collect();
    for peer in peers {
        peer.send_frame(&forwarded).await?;
    }
    Ok(())
}

async fn allocate_tcp_relay_stream_id(allocator: &Arc<Mutex<u32>>) -> u32 {
    let mut guard = allocator.lock().await;
    let current = *guard;
    *guard = guard.saturating_add(1);
    current
}

fn prepend_route_path(identity: &AgentIdentity, path: &[RouteHop]) -> Vec<RouteHop> {
    let mut announced_path = Vec::with_capacity(path.len() + 1);
    announced_path.push(RouteHop {
        agent_id: identity.id.clone(),
        agent_name: identity.name.clone(),
    });
    announced_path.extend(path.iter().cloned());
    announced_path
}

fn validate_route_announcement(
    local_agent_id: &str,
    source_peer_id: &str,
    announcement: &RouteAnnouncement,
) -> Result<(), &'static str> {
    let Some(next_hop) = announcement.direct_next_hop() else {
        return Err("missing_next_hop");
    };
    if next_hop.agent_id != source_peer_id {
        return Err("source_peer_mismatch");
    }
    if announcement.origin_agent_id == local_agent_id {
        return Err("origin_is_local");
    }
    if announcement.contains_agent(local_agent_id) {
        return Err("path_contains_local_agent");
    }
    if announcement.has_loop() {
        return Err("path_loop_detected");
    }
    if !announcement.ends_at_origin() {
        return Err("path_does_not_end_at_origin");
    }
    Ok(())
}

async fn allocate_ws_relay_stream_id(allocator: &Arc<Mutex<u32>>) -> u32 {
    let mut guard = allocator.lock().await;
    let current = *guard;
    *guard = guard.saturating_add(1);
    current
}

fn rewrite_stream_frame(
    frame: &Frame,
    stream_id: u32,
    dst_agent: Option<String>,
) -> Result<Frame, Error> {
    if frame.header.stream_id.is_none() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected stream frame with stream_id",
        ));
    }
    let mut forwarded = frame.clone();
    forwarded.header.stream_id = Some(stream_id);
    forwarded.header.dst_agent = dst_agent;
    Ok(forwarded)
}

async fn bridge_tcp_stream_frames(
    mut from_rx: tokio::sync::mpsc::Receiver<Frame>,
    to_peer: tcp_mux::MuxTcpPeer,
    target_stream_id: u32,
    target_dst_agent: Option<String>,
) -> Result<(), Error> {
    while let Some(frame) = from_rx.recv().await {
        let forwarded = rewrite_stream_frame(&frame, target_stream_id, target_dst_agent.clone())?;
        let should_close = matches!(forwarded.message, Message::StreamClose(_));
        to_peer.send_frame(&forwarded).await?;
        if should_close {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{prepend_route_path, validate_route_announcement};
    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::route::{RouteAnnouncement, RouteHop},
    };

    #[test]
    fn validate_route_announcement_rejects_local_loop() {
        let announcement = RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec![],
            services: vec!["task".into()],
            path: vec![
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "local-agent".into(),
                    agent_name: "local-name".into(),
                },
                RouteHop {
                    agent_id: "peer-z".into(),
                    agent_name: "peer-z-name".into(),
                },
            ],
        };

        assert_eq!(
            validate_route_announcement("local-agent", "peer-b", &announcement),
            Err("path_contains_local_agent")
        );
    }

    #[test]
    fn validate_route_announcement_rejects_source_peer_mismatch() {
        let announcement = RouteAnnouncement {
            origin_agent_id: "peer-z".into(),
            origin_agent_name: "peer-z-name".into(),
            capabilities: vec![],
            services: vec!["task".into()],
            path: vec![RouteHop {
                agent_id: "peer-b".into(),
                agent_name: "peer-b-name".into(),
            }],
        };

        assert_eq!(
            validate_route_announcement("local-agent", "peer-c", &announcement),
            Err("source_peer_mismatch")
        );
    }

    #[test]
    fn prepend_route_path_keeps_known_multi_hop_path() {
        let identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("relay-a".into()),
            key: None,
        });
        let path = prepend_route_path(
            &identity,
            &[
                RouteHop {
                    agent_id: "peer-b".into(),
                    agent_name: "peer-b-name".into(),
                },
                RouteHop {
                    agent_id: "peer-c".into(),
                    agent_name: "peer-c-name".into(),
                },
            ],
        );

        assert_eq!(path.first().unwrap().agent_id, identity.id);
        assert_eq!(path[1].agent_id, "peer-b");
        assert_eq!(path[2].agent_id, "peer-c");
    }
}

async fn bridge_ws_stream_frames(
    mut from_rx: tokio::sync::mpsc::Receiver<Frame>,
    to_peer: ws_mux::MuxWsPeer,
    target_stream_id: u32,
    target_dst_agent: Option<String>,
) -> Result<(), Error> {
    while let Some(frame) = from_rx.recv().await {
        let forwarded = rewrite_stream_frame(&frame, target_stream_id, target_dst_agent.clone())?;
        let should_close = matches!(forwarded.message, Message::StreamClose(_));
        to_peer.send_frame(&forwarded).await?;
        if should_close {
            break;
        }
    }
    Ok(())
}

async fn bridge_simplex_stream_frames(
    mut from_rx: tokio::sync::mpsc::Receiver<Frame>,
    to_peer: simplex_http_mux::MuxSimplexHttpPeer,
    target_stream_id: u32,
    target_dst_agent: Option<String>,
) -> Result<(), Error> {
    while let Some(frame) = from_rx.recv().await {
        let forwarded = rewrite_stream_frame(&frame, target_stream_id, target_dst_agent.clone())?;
        let should_close = matches!(forwarded.message, Message::StreamClose(_));
        to_peer.send_frame(&forwarded).await?;
        if should_close {
            break;
        }
    }
    Ok(())
}

async fn bridge_simplex_oss_stream_frames(
    mut from_rx: tokio::sync::mpsc::Receiver<Frame>,
    to_peer: simplex_oss_mux::MuxSimplexOssPeer,
    target_stream_id: u32,
    target_dst_agent: Option<String>,
) -> Result<(), Error> {
    while let Some(frame) = from_rx.recv().await {
        let forwarded = rewrite_stream_frame(&frame, target_stream_id, target_dst_agent.clone())?;
        let should_close = matches!(forwarded.message, Message::StreamClose(_));
        to_peer.send_frame(&forwarded).await?;
        if should_close {
            break;
        }
    }
    Ok(())
}

async fn bridge_simplex_dns_stream_frames(
    mut from_rx: tokio::sync::mpsc::Receiver<Frame>,
    to_peer: simplex_dns_mux::MuxSimplexDnsPeer,
    target_stream_id: u32,
    target_dst_agent: Option<String>,
) -> Result<(), Error> {
    while let Some(frame) = from_rx.recv().await {
        let forwarded = rewrite_stream_frame(&frame, target_stream_id, target_dst_agent.clone())?;
        let should_close = matches!(forwarded.message, Message::StreamClose(_));
        to_peer.send_frame(&forwarded).await?;
        if should_close {
            break;
        }
    }
    Ok(())
}

pub async fn handle_tcp_relay_stream_open(
    peer_map: Arc<Mutex<HashMap<String, tcp_mux::MuxTcpPeer>>>,
    allocator: Arc<Mutex<u32>>,
    relay_links: RelayLinkMap,
    source_peer: tcp_mux::MuxTcpPeer,
    open_frame: Frame,
) -> Result<(), Error> {
    let source_stream_id = open_frame.header.stream_id.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "missing stream_id on relay StreamOpen frame",
        )
    })?;
    let destination_agent_id = open_frame
        .header
        .dst_agent
        .clone()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing dst_agent on StreamOpen"))?;
    let source_agent_id = open_frame
        .header
        .src_agent
        .clone()
        .unwrap_or_else(|| source_peer.session.remote.agent_id.clone());

    let next_hop =
        { peer_map.lock().await.get(&destination_agent_id).cloned() }.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("no next hop available for stream destination {destination_agent_id}"),
            )
        })?;

    let target_stream_id = allocate_tcp_relay_stream_id(&allocator).await;
    upsert_relay_link(
        &relay_links,
        "tcp",
        source_peer.session.remote.agent_id.clone(),
        source_stream_id,
        next_hop.session.remote.agent_id.clone(),
        target_stream_id,
        destination_agent_id.clone(),
    )
    .await;
    let downstream_rx = source_peer.open_stream_receiver(source_stream_id).await;
    let upstream_rx = next_hop.open_stream_receiver(target_stream_id).await;
    let forwarded_open = rewrite_stream_frame(
        &open_frame,
        target_stream_id,
        Some(destination_agent_id.clone()),
    )?;
    next_hop.send_frame(&forwarded_open).await?;

    let downstream_peer = source_peer.clone();
    let upstream_peer = next_hop.clone();
    let destination_agent_id_for_forward = destination_agent_id.clone();
    let relay_links_for_forward = relay_links.clone();
    let source_peer_id_for_forward = source_peer.session.remote.agent_id.clone();
    tokio::spawn(async move {
        if let Err(err) = bridge_tcp_stream_frames(
            downstream_rx,
            upstream_peer.clone(),
            target_stream_id,
            Some(destination_agent_id_for_forward.clone()),
        )
        .await
        {
            eprintln!(
                "stream.relay.forward.error={} src_peer={} stream_id={}",
                err, downstream_peer.session.remote.agent_id, source_stream_id
            );
        }
        remove_relay_link(
            &relay_links_for_forward,
            "tcp",
            &source_peer_id_for_forward,
            source_stream_id,
        )
        .await;
    });

    let downstream_peer = source_peer.clone();
    let upstream_peer = next_hop.clone();
    let source_agent_id_for_return = source_agent_id.clone();
    let relay_links_for_return = relay_links.clone();
    let source_peer_id_for_return = source_peer.session.remote.agent_id.clone();
    tokio::spawn(async move {
        if let Err(err) = bridge_tcp_stream_frames(
            upstream_rx,
            downstream_peer.clone(),
            source_stream_id,
            Some(source_agent_id_for_return.clone()),
        )
        .await
        {
            eprintln!(
                "stream.relay.return.error={} src_peer={} stream_id={}",
                err, upstream_peer.session.remote.agent_id, target_stream_id
            );
        }
        remove_relay_link(
            &relay_links_for_return,
            "tcp",
            &source_peer_id_for_return,
            source_stream_id,
        )
        .await;
    });

    eprintln!(
        "stream.relay.open src_peer={} src_stream={} next_hop={} relay_stream={} dst={}",
        source_peer.session.remote.agent_id,
        source_stream_id,
        next_hop.session.remote.agent_id,
        target_stream_id,
        destination_agent_id
    );
    Ok(())
}

pub async fn handle_ws_relay_stream_open(
    peer_map: Arc<Mutex<HashMap<String, ws_mux::MuxWsPeer>>>,
    allocator: Arc<Mutex<u32>>,
    relay_links: RelayLinkMap,
    source_peer: ws_mux::MuxWsPeer,
    open_frame: Frame,
) -> Result<(), Error> {
    let source_stream_id = open_frame.header.stream_id.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "missing stream_id on relay StreamOpen frame",
        )
    })?;
    let destination_agent_id = open_frame
        .header
        .dst_agent
        .clone()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing dst_agent on StreamOpen"))?;
    let source_agent_id = open_frame
        .header
        .src_agent
        .clone()
        .unwrap_or_else(|| source_peer.session.remote.agent_id.clone());

    let next_hop =
        { peer_map.lock().await.get(&destination_agent_id).cloned() }.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("no next hop available for stream destination {destination_agent_id}"),
            )
        })?;

    let target_stream_id = allocate_ws_relay_stream_id(&allocator).await;
    upsert_relay_link(
        &relay_links,
        "ws",
        source_peer.session.remote.agent_id.clone(),
        source_stream_id,
        next_hop.session.remote.agent_id.clone(),
        target_stream_id,
        destination_agent_id.clone(),
    )
    .await;
    let downstream_rx = source_peer.open_stream_receiver(source_stream_id).await;
    let upstream_rx = next_hop.open_stream_receiver(target_stream_id).await;
    let forwarded_open = rewrite_stream_frame(
        &open_frame,
        target_stream_id,
        Some(destination_agent_id.clone()),
    )?;
    next_hop.send_frame(&forwarded_open).await?;

    let downstream_peer = source_peer.clone();
    let upstream_peer = next_hop.clone();
    let destination_agent_id_for_forward = destination_agent_id.clone();
    let relay_links_for_forward = relay_links.clone();
    let source_peer_id_for_forward = source_peer.session.remote.agent_id.clone();
    tokio::spawn(async move {
        if let Err(err) = bridge_ws_stream_frames(
            downstream_rx,
            upstream_peer.clone(),
            target_stream_id,
            Some(destination_agent_id_for_forward.clone()),
        )
        .await
        {
            eprintln!(
                "stream.relay.forward.error={} src_peer={} stream_id={}",
                err, downstream_peer.session.remote.agent_id, source_stream_id
            );
        }
        remove_relay_link(
            &relay_links_for_forward,
            "ws",
            &source_peer_id_for_forward,
            source_stream_id,
        )
        .await;
    });

    let downstream_peer = source_peer.clone();
    let upstream_peer = next_hop.clone();
    let source_agent_id_for_return = source_agent_id.clone();
    let relay_links_for_return = relay_links.clone();
    let source_peer_id_for_return = source_peer.session.remote.agent_id.clone();
    tokio::spawn(async move {
        if let Err(err) = bridge_ws_stream_frames(
            upstream_rx,
            downstream_peer.clone(),
            source_stream_id,
            Some(source_agent_id_for_return.clone()),
        )
        .await
        {
            eprintln!(
                "stream.relay.return.error={} src_peer={} stream_id={}",
                err, upstream_peer.session.remote.agent_id, target_stream_id
            );
        }
        remove_relay_link(
            &relay_links_for_return,
            "ws",
            &source_peer_id_for_return,
            source_stream_id,
        )
        .await;
    });

    eprintln!(
        "stream.relay.open src_peer={} src_stream={} next_hop={} relay_stream={} dst={}",
        source_peer.session.remote.agent_id,
        source_stream_id,
        next_hop.session.remote.agent_id,
        target_stream_id,
        destination_agent_id
    );
    Ok(())
}

pub async fn handle_simplex_relay_stream_open(
    peer_map: Arc<Mutex<HashMap<String, simplex_http_mux::MuxSimplexHttpPeer>>>,
    allocator: Arc<Mutex<u32>>,
    relay_links: RelayLinkMap,
    source_peer: simplex_http_mux::MuxSimplexHttpPeer,
    open_frame: Frame,
) -> Result<(), Error> {
    let source_stream_id = open_frame.header.stream_id.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "missing stream_id on relay StreamOpen frame",
        )
    })?;
    let destination_agent_id = open_frame
        .header
        .dst_agent
        .clone()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing dst_agent on StreamOpen"))?;
    let source_agent_id = open_frame
        .header
        .src_agent
        .clone()
        .unwrap_or_else(|| source_peer.session.remote.agent_id.clone());

    let next_hop =
        { peer_map.lock().await.get(&destination_agent_id).cloned() }.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("no next hop available for stream destination {destination_agent_id}"),
            )
        })?;

    let target_stream_id = allocate_ws_relay_stream_id(&allocator).await;
    upsert_relay_link(
        &relay_links,
        "simplex-http",
        source_peer.session.remote.agent_id.clone(),
        source_stream_id,
        next_hop.session.remote.agent_id.clone(),
        target_stream_id,
        destination_agent_id.clone(),
    )
    .await;
    let downstream_rx = source_peer.open_stream_receiver(source_stream_id).await;
    let upstream_rx = next_hop.open_stream_receiver(target_stream_id).await;
    let forwarded_open = rewrite_stream_frame(
        &open_frame,
        target_stream_id,
        Some(destination_agent_id.clone()),
    )?;
    next_hop.send_frame(&forwarded_open).await?;

    let downstream_peer = source_peer.clone();
    let upstream_peer = next_hop.clone();
    let destination_agent_id_for_forward = destination_agent_id.clone();
    let relay_links_for_forward = relay_links.clone();
    let source_peer_id_for_forward = source_peer.session.remote.agent_id.clone();
    tokio::spawn(async move {
        if let Err(err) = bridge_simplex_stream_frames(
            downstream_rx,
            upstream_peer.clone(),
            target_stream_id,
            Some(destination_agent_id_for_forward.clone()),
        )
        .await
        {
            eprintln!(
                "stream.relay.forward.error={} src_peer={} stream_id={}",
                err, downstream_peer.session.remote.agent_id, source_stream_id
            );
        }
        remove_relay_link(
            &relay_links_for_forward,
            "simplex-http",
            &source_peer_id_for_forward,
            source_stream_id,
        )
        .await;
    });

    let downstream_peer = source_peer.clone();
    let upstream_peer = next_hop.clone();
    let source_agent_id_for_return = source_agent_id.clone();
    let relay_links_for_return = relay_links.clone();
    let source_peer_id_for_return = source_peer.session.remote.agent_id.clone();
    tokio::spawn(async move {
        if let Err(err) = bridge_simplex_stream_frames(
            upstream_rx,
            downstream_peer.clone(),
            source_stream_id,
            Some(source_agent_id_for_return.clone()),
        )
        .await
        {
            eprintln!(
                "stream.relay.return.error={} src_peer={} stream_id={}",
                err, upstream_peer.session.remote.agent_id, target_stream_id
            );
        }
        remove_relay_link(
            &relay_links_for_return,
            "simplex-http",
            &source_peer_id_for_return,
            source_stream_id,
        )
        .await;
    });

    eprintln!(
        "stream.relay.open src_peer={} src_stream={} next_hop={} relay_stream={} dst={}",
        source_peer.session.remote.agent_id,
        source_stream_id,
        next_hop.session.remote.agent_id,
        target_stream_id,
        destination_agent_id
    );
    Ok(())
}

pub async fn handle_simplex_oss_relay_stream_open(
    peer_map: Arc<Mutex<HashMap<String, simplex_oss_mux::MuxSimplexOssPeer>>>,
    allocator: Arc<Mutex<u32>>,
    relay_links: RelayLinkMap,
    source_peer: simplex_oss_mux::MuxSimplexOssPeer,
    open_frame: Frame,
) -> Result<(), Error> {
    let source_stream_id = open_frame.header.stream_id.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "missing stream_id on relay StreamOpen frame",
        )
    })?;
    let destination_agent_id = open_frame
        .header
        .dst_agent
        .clone()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing dst_agent on StreamOpen"))?;
    let source_agent_id = open_frame
        .header
        .src_agent
        .clone()
        .unwrap_or_else(|| source_peer.session.remote.agent_id.clone());

    let next_hop =
        { peer_map.lock().await.get(&destination_agent_id).cloned() }.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("no next hop available for stream destination {destination_agent_id}"),
            )
        })?;

    let target_stream_id = allocate_ws_relay_stream_id(&allocator).await;
    upsert_relay_link(
        &relay_links,
        "simplex-oss",
        source_peer.session.remote.agent_id.clone(),
        source_stream_id,
        next_hop.session.remote.agent_id.clone(),
        target_stream_id,
        destination_agent_id.clone(),
    )
    .await;
    let downstream_rx = source_peer.open_stream_receiver(source_stream_id).await;
    let upstream_rx = next_hop.open_stream_receiver(target_stream_id).await;
    let forwarded_open = rewrite_stream_frame(
        &open_frame,
        target_stream_id,
        Some(destination_agent_id.clone()),
    )?;
    next_hop.send_frame(&forwarded_open).await?;

    let downstream_peer = source_peer.clone();
    let upstream_peer = next_hop.clone();
    let destination_agent_id_for_forward = destination_agent_id.clone();
    let relay_links_for_forward = relay_links.clone();
    let source_peer_id_for_forward = source_peer.session.remote.agent_id.clone();
    tokio::spawn(async move {
        if let Err(err) = bridge_simplex_oss_stream_frames(
            downstream_rx,
            upstream_peer.clone(),
            target_stream_id,
            Some(destination_agent_id_for_forward.clone()),
        )
        .await
        {
            eprintln!(
                "stream.relay.forward.error={} src_peer={} stream_id={}",
                err, downstream_peer.session.remote.agent_id, source_stream_id
            );
        }
        remove_relay_link(
            &relay_links_for_forward,
            "simplex-oss",
            &source_peer_id_for_forward,
            source_stream_id,
        )
        .await;
    });

    let downstream_peer = source_peer.clone();
    let upstream_peer = next_hop.clone();
    let source_agent_id_for_return = source_agent_id.clone();
    let relay_links_for_return = relay_links.clone();
    let source_peer_id_for_return = source_peer.session.remote.agent_id.clone();
    tokio::spawn(async move {
        if let Err(err) = bridge_simplex_oss_stream_frames(
            upstream_rx,
            downstream_peer.clone(),
            source_stream_id,
            Some(source_agent_id_for_return.clone()),
        )
        .await
        {
            eprintln!(
                "stream.relay.return.error={} src_peer={} stream_id={}",
                err, upstream_peer.session.remote.agent_id, target_stream_id
            );
        }
        remove_relay_link(
            &relay_links_for_return,
            "simplex-oss",
            &source_peer_id_for_return,
            source_stream_id,
        )
        .await;
    });

    eprintln!(
        "stream.relay.open src_peer={} src_stream={} next_hop={} relay_stream={} dst={}",
        source_peer.session.remote.agent_id,
        source_stream_id,
        next_hop.session.remote.agent_id,
        target_stream_id,
        destination_agent_id
    );
    Ok(())
}

pub async fn handle_simplex_dns_relay_stream_open(
    peer_map: Arc<Mutex<HashMap<String, simplex_dns_mux::MuxSimplexDnsPeer>>>,
    allocator: Arc<Mutex<u32>>,
    relay_links: RelayLinkMap,
    source_peer: simplex_dns_mux::MuxSimplexDnsPeer,
    open_frame: Frame,
) -> Result<(), Error> {
    let source_stream_id = open_frame.header.stream_id.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "missing stream_id on relay StreamOpen frame",
        )
    })?;
    let destination_agent_id = open_frame
        .header
        .dst_agent
        .clone()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing dst_agent on StreamOpen"))?;
    let source_agent_id = open_frame
        .header
        .src_agent
        .clone()
        .unwrap_or_else(|| source_peer.session.remote.agent_id.clone());

    let next_hop =
        { peer_map.lock().await.get(&destination_agent_id).cloned() }.ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("no next hop available for stream destination {destination_agent_id}"),
            )
        })?;

    let target_stream_id = allocate_ws_relay_stream_id(&allocator).await;
    upsert_relay_link(
        &relay_links,
        "simplex-dns",
        source_peer.session.remote.agent_id.clone(),
        source_stream_id,
        next_hop.session.remote.agent_id.clone(),
        target_stream_id,
        destination_agent_id.clone(),
    )
    .await;
    let downstream_rx = source_peer.open_stream_receiver(source_stream_id).await;
    let upstream_rx = next_hop.open_stream_receiver(target_stream_id).await;
    let forwarded_open = rewrite_stream_frame(
        &open_frame,
        target_stream_id,
        Some(destination_agent_id.clone()),
    )?;
    next_hop.send_frame(&forwarded_open).await?;

    let downstream_peer = source_peer.clone();
    let upstream_peer = next_hop.clone();
    let destination_agent_id_for_forward = destination_agent_id.clone();
    let relay_links_for_forward = relay_links.clone();
    let source_peer_id_for_forward = source_peer.session.remote.agent_id.clone();
    tokio::spawn(async move {
        if let Err(err) = bridge_simplex_dns_stream_frames(
            downstream_rx,
            upstream_peer.clone(),
            target_stream_id,
            Some(destination_agent_id_for_forward.clone()),
        )
        .await
        {
            eprintln!(
                "stream.relay.forward.error={} src_peer={} stream_id={}",
                err, downstream_peer.session.remote.agent_id, source_stream_id
            );
        }
        remove_relay_link(
            &relay_links_for_forward,
            "simplex-dns",
            &source_peer_id_for_forward,
            source_stream_id,
        )
        .await;
    });

    let downstream_peer = source_peer.clone();
    let upstream_peer = next_hop.clone();
    let source_agent_id_for_return = source_agent_id.clone();
    let relay_links_for_return = relay_links.clone();
    let source_peer_id_for_return = source_peer.session.remote.agent_id.clone();
    tokio::spawn(async move {
        if let Err(err) = bridge_simplex_dns_stream_frames(
            upstream_rx,
            downstream_peer.clone(),
            source_stream_id,
            Some(source_agent_id_for_return.clone()),
        )
        .await
        {
            eprintln!(
                "stream.relay.return.error={} src_peer={} stream_id={}",
                err, upstream_peer.session.remote.agent_id, target_stream_id
            );
        }
        remove_relay_link(
            &relay_links_for_return,
            "simplex-dns",
            &source_peer_id_for_return,
            source_stream_id,
        )
        .await;
    });

    eprintln!(
        "stream.relay.open src_peer={} src_stream={} next_hop={} relay_stream={} dst={}",
        source_peer.session.remote.agent_id,
        source_stream_id,
        next_hop.session.remote.agent_id,
        target_stream_id,
        destination_agent_id
    );
    Ok(())
}
