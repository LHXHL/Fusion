use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    sync::Arc,
};

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    app::{
        config::TunnelEndpoint,
        runtime_relay::{
            broadcast_control_frame_simplex_dns, broadcast_control_frame_simplex_http,
            broadcast_control_frame_tcp, broadcast_control_frame_simplex_oss,
            broadcast_control_frame_ws,
            handle_registry_control_message, handle_tcp_relay_stream_open,
            handle_simplex_dns_relay_stream_open, handle_simplex_oss_relay_stream_open,
            handle_simplex_relay_stream_open,
            handle_ws_relay_stream_open,
            send_direct_announce_simplex_dns, send_direct_announce_simplex_http,
            send_direct_announce_simplex_oss, send_direct_announce_tcp_mux,
            send_direct_announce_ws_mux, send_route_snapshot_simplex_dns_mux,
            send_route_snapshot_simplex_http_mux, send_route_snapshot_simplex_oss_mux,
            send_route_snapshot_tcp_mux, send_route_snapshot_ws_mux, ROUTE_TTL_SECS,
        },
        runtime_status::RelayLinkMap,
    },
    protocol::{
        frame::{Frame, MessageType},
        message::Message,
    },
    session::{
        hub::SessionHub,
        router::{decide_frame_route, RouteDecision},
    },
    task::dispatcher,
    tunnel::{simplex_dns_mux, simplex_http_mux, simplex_oss_mux, tcp_mux, ws_mux},
};
use tokio::{net::{TcpListener, UdpSocket}, sync::Mutex};

pub type TcpTaskPeerMap = Arc<Mutex<HashMap<String, tcp_mux::MuxTcpPeer>>>;
pub type WsTaskPeerMap = Arc<Mutex<HashMap<String, ws_mux::MuxWsPeer>>>;
pub type SimplexDnsTaskPeerMap = Arc<Mutex<HashMap<String, simplex_dns_mux::MuxSimplexDnsPeer>>>;
pub type SimplexHttpTaskPeerMap = Arc<Mutex<HashMap<String, simplex_http_mux::MuxSimplexHttpPeer>>>;
pub type SimplexOssTaskPeerMap = Arc<Mutex<HashMap<String, simplex_oss_mux::MuxSimplexOssPeer>>>;
pub type TcpRelayStreamAllocator = Arc<Mutex<u32>>;
pub type WsRelayStreamAllocator = Arc<Mutex<u32>>;
pub type SimplexDnsRelayStreamAllocator = Arc<Mutex<u32>>;
pub type SimplexHttpRelayStreamAllocator = Arc<Mutex<u32>>;
pub type SimplexOssRelayStreamAllocator = Arc<Mutex<u32>>;

pub async fn run_inbound_task_server_tcp(
    identity: AgentIdentity,
    listener: TcpListener,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: TcpTaskPeerMap,
    relay_stream_allocator: TcpRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    loop {
        let peer = tcp_mux::accept_mux_peer_on(identity.clone(), &listener).await?;
        let identity = identity.clone();
        let local_services = local_services.clone();
        let hub = hub.clone();
        let registry = registry.clone();
        let peer_map = peer_map.clone();
        let relay_stream_allocator = relay_stream_allocator.clone();
        let relay_links = relay_links.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_inbound_task_peer_tcp(
                identity,
                local_services,
                hub,
                registry,
                peer_map,
                relay_stream_allocator,
                relay_links,
                peer,
            )
            .await
            {
                eprintln!("session.inbound.peer.error={err}");
            }
        });
    }
}

pub async fn handle_inbound_task_peer_tcp(
    identity: AgentIdentity,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: TcpTaskPeerMap,
    relay_stream_allocator: TcpRelayStreamAllocator,
    relay_links: RelayLinkMap,
    peer: tcp_mux::MuxTcpPeer,
) -> Result<(), Error> {
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    peer_map
        .lock()
        .await
        .insert(peer.session.remote.agent_id.clone(), peer.clone());
    println!(
        "session.inbound.peer={} from={} via=tcp",
        peer.session.remote.agent_id, peer.peer_addr
    );
    send_direct_announce_tcp_mux(&peer, &identity, &local_services).await?;
    let route_snapshot = {
        let mut registry_guard = registry.lock().await;
        let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
        if pruned > 0 {
            eprintln!("registry.route_pruned={}", pruned);
        }
        registry_guard.routes_snapshot()
    };
    send_route_snapshot_tcp_mux(
        &peer,
        &identity,
        &route_snapshot,
        Some(peer.session.remote.agent_id.clone()),
    )
    .await?;

    let stream_peer = peer.clone();
    let stream_peer_map = peer_map.clone();
    let stream_allocator = relay_stream_allocator.clone();
    let stream_relay_links = relay_links.clone();
    tokio::spawn(async move {
        loop {
            let open_frame = match stream_peer.read_stream_open_frame().await {
                Ok(frame) => frame,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    eprintln!("stream.relay.accept.error={err}");
                    break;
                }
            };
            if let Err(err) = handle_tcp_relay_stream_open(
                stream_peer_map.clone(),
                stream_allocator.clone(),
                stream_relay_links.clone(),
                stream_peer.clone(),
                open_frame,
            )
            .await
            {
                eprintln!("stream.relay.open.error={err}");
            }
        }
    });

    loop {
        let frame = match peer.read_control_frame().await {
            Ok(frame) => frame,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };

        let handled_control = {
            let mut registry_guard = registry.lock().await;
            handle_registry_control_message(
                &mut registry_guard,
                &identity.id,
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_tcp(
                &peer_map,
                &identity,
                &peer.session.remote.agent_id,
                &frame,
            )
            .await?;
            continue;
        }

        let route_decision = {
            let mut registry_guard = registry.lock().await;
            let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
            if pruned > 0 {
                eprintln!("registry.route_pruned={}", pruned);
            }
            decide_frame_route(&identity.id, &registry_guard, &frame)
        };
        match route_decision {
            RouteDecision::Local => {}
            RouteDecision::Forward { next_hop_agent_id } => {
                if next_hop_agent_id == peer.session.remote.agent_id {
                    eprintln!(
                        "relay.drop.loop dst={:?} next_hop={} source_peer={}",
                        frame.header.dst_agent, next_hop_agent_id, peer.session.remote.agent_id
                    );
                    continue;
                }
                let next_hop = { peer_map.lock().await.get(&next_hop_agent_id).cloned() };
                if let Some(next_hop) = next_hop {
                    next_hop.send_frame(&frame).await?;
                    eprintln!(
                        "relay.forward.sent dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                } else {
                    eprintln!(
                        "relay.forward.missing_next_hop dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                }
                continue;
            }
            RouteDecision::DropNoRoute {
                destination_agent_id,
            } => {
                eprintln!("relay.drop.no_route dst={}", destination_agent_id);
                continue;
            }
        }

        match frame.message {
            Message::TaskRequest(request) => {
                let response_dst = frame
                    .header
                    .src_agent
                    .clone()
                    .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                let result = dispatcher::dispatch(&request).await;
                let response = Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(response_dst),
                    Message::TaskResult(result),
                );
                peer.send_frame(&response).await?;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unexpected message on task server: {:?}", other),
                ))
            }
        }
    }

    peer_map.lock().await.remove(&peer.session.remote.agent_id);
    registry
        .lock()
        .await
        .remove_peer_state(&peer.session.remote.agent_id);
    Ok(())
}

pub async fn run_outbound_relay_peer_tcp(
    identity: AgentIdentity,
    endpoint: &TunnelEndpoint,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: TcpTaskPeerMap,
    relay_stream_allocator: TcpRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    let connect_host = endpoint
        .url
        .host
        .clone()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "missing host for tcp connect"))?;
    let connect_port = endpoint
        .url
        .port
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "missing port for tcp connect"))?;
    let connect_addr = format!("{}:{}", connect_host, connect_port);
    let peer = tcp_mux::connect_mux_peer(identity.clone(), &connect_addr).await?;

    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    peer_map
        .lock()
        .await
        .insert(peer.session.remote.agent_id.clone(), peer.clone());
    println!(
        "session.outbound.peer={} to={} via=tcp",
        peer.session.remote.agent_id, connect_addr
    );
    send_direct_announce_tcp_mux(&peer, &identity, &local_services).await?;
    let route_snapshot = {
        let mut registry_guard = registry.lock().await;
        let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
        if pruned > 0 {
            eprintln!("registry.route_pruned={}", pruned);
        }
        registry_guard.routes_snapshot()
    };
    send_route_snapshot_tcp_mux(
        &peer,
        &identity,
        &route_snapshot,
        Some(peer.session.remote.agent_id.clone()),
    )
    .await?;

    let stream_peer = peer.clone();
    let stream_peer_map = peer_map.clone();
    let stream_allocator = relay_stream_allocator.clone();
    let stream_relay_links = relay_links.clone();
    tokio::spawn(async move {
        loop {
            let open_frame = match stream_peer.read_stream_open_frame().await {
                Ok(frame) => frame,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    eprintln!("stream.relay.accept.error={err}");
                    break;
                }
            };
            if let Err(err) = handle_tcp_relay_stream_open(
                stream_peer_map.clone(),
                stream_allocator.clone(),
                stream_relay_links.clone(),
                stream_peer.clone(),
                open_frame,
            )
            .await
            {
                eprintln!("stream.relay.open.error={err}");
            }
        }
    });

    loop {
        let frame = match peer.read_control_frame().await {
            Ok(frame) => frame,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };

        let handled_control = {
            let mut registry_guard = registry.lock().await;
            handle_registry_control_message(
                &mut registry_guard,
                &identity.id,
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_tcp(
                &peer_map,
                &identity,
                &peer.session.remote.agent_id,
                &frame,
            )
            .await?;
            continue;
        }

        let route_decision = {
            let mut registry_guard = registry.lock().await;
            let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
            if pruned > 0 {
                eprintln!("registry.route_pruned={}", pruned);
            }
            decide_frame_route(&identity.id, &registry_guard, &frame)
        };
        match route_decision {
            RouteDecision::Local => match frame.message {
                Message::TaskRequest(request) => {
                    let response_dst = frame
                        .header
                        .src_agent
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    let result = dispatcher::dispatch(&request).await;
                    let response = Frame::new(
                        MessageType::TaskResult,
                        Some(peer.session.local.agent_id.clone()),
                        Some(response_dst),
                        Message::TaskResult(result),
                    );
                    peer.send_frame(&response).await?;
                }
                other => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected message on outbound relay peer: {:?}", other),
                    ))
                }
            },
            RouteDecision::Forward { next_hop_agent_id } => {
                if next_hop_agent_id == peer.session.remote.agent_id {
                    eprintln!(
                        "relay.drop.loop dst={:?} next_hop={} source_peer={}",
                        frame.header.dst_agent, next_hop_agent_id, peer.session.remote.agent_id
                    );
                    continue;
                }
                let next_hop = { peer_map.lock().await.get(&next_hop_agent_id).cloned() };
                if let Some(next_hop) = next_hop {
                    next_hop.send_frame(&frame).await?;
                    eprintln!(
                        "relay.forward.sent dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                } else {
                    eprintln!(
                        "relay.forward.missing_next_hop dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                }
            }
            RouteDecision::DropNoRoute {
                destination_agent_id,
            } => {
                eprintln!("relay.drop.no_route dst={}", destination_agent_id);
            }
        }
    }

    peer_map.lock().await.remove(&peer.session.remote.agent_id);
    registry
        .lock()
        .await
        .remove_peer_state(&peer.session.remote.agent_id);
    Ok(())
}

pub async fn run_inbound_task_server_simplex_http(
    identity: AgentIdentity,
    listener: TcpListener,
    path: &str,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: SimplexHttpTaskPeerMap,
    relay_stream_allocator: SimplexHttpRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    let peer = simplex_http_mux::accept_mux_peer_on(identity.clone(), listener, path).await?;
    handle_inbound_task_peer_simplex_http(
        identity,
        local_services,
        hub,
        registry,
        peer_map,
        relay_stream_allocator,
        relay_links,
        peer,
    )
    .await
}

pub async fn handle_inbound_task_peer_simplex_http(
    identity: AgentIdentity,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: SimplexHttpTaskPeerMap,
    relay_stream_allocator: SimplexHttpRelayStreamAllocator,
    relay_links: RelayLinkMap,
    peer: simplex_http_mux::MuxSimplexHttpPeer,
) -> Result<(), Error> {
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    peer_map
        .lock()
        .await
        .insert(peer.session.remote.agent_id.clone(), peer.clone());
    println!(
        "session.inbound.peer={} via=simplex-http",
        peer.session.remote.agent_id
    );
    send_direct_announce_simplex_http(&peer.inner, &identity, &local_services).await?;
    let route_snapshot = {
        let mut registry_guard = registry.lock().await;
        let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
        if pruned > 0 {
            eprintln!("registry.route_pruned={}", pruned);
        }
        registry_guard.routes_snapshot()
    };
    send_route_snapshot_simplex_http_mux(
        &peer,
        &identity,
        &route_snapshot,
        Some(peer.session.remote.agent_id.clone()),
    )
    .await?;

    let stream_peer = peer.clone();
    let stream_peer_map = peer_map.clone();
    let stream_allocator = relay_stream_allocator.clone();
    let stream_relay_links = relay_links.clone();
    tokio::spawn(async move {
        loop {
            let open_frame = match stream_peer.read_stream_open_frame().await {
                Ok(frame) => frame,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    eprintln!("stream.relay.accept.error={err}");
                    break;
                }
            };
            if let Err(err) = handle_simplex_relay_stream_open(
                stream_peer_map.clone(),
                stream_allocator.clone(),
                stream_relay_links.clone(),
                stream_peer.clone(),
                open_frame,
            )
            .await
            {
                eprintln!("stream.relay.open.error={err}");
            }
        }
    });

    loop {
        let frame = match peer.read_control_frame().await {
            Ok(frame) => frame,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };

        let handled_control = {
            let mut registry_guard = registry.lock().await;
            handle_registry_control_message(
                &mut registry_guard,
                &identity.id,
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_simplex_http(
                &peer_map,
                &identity,
                &peer.session.remote.agent_id,
                &frame,
            )
            .await?;
            continue;
        }

        let route_decision = {
            let mut registry_guard = registry.lock().await;
            let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
            if pruned > 0 {
                eprintln!("registry.route_pruned={}", pruned);
            }
            decide_frame_route(&identity.id, &registry_guard, &frame)
        };
        match route_decision {
            RouteDecision::Local => {}
            RouteDecision::Forward { next_hop_agent_id } => {
                if next_hop_agent_id == peer.session.remote.agent_id {
                    eprintln!(
                        "relay.drop.loop dst={:?} next_hop={} source_peer={}",
                        frame.header.dst_agent, next_hop_agent_id, peer.session.remote.agent_id
                    );
                    continue;
                }
                let next_hop = { peer_map.lock().await.get(&next_hop_agent_id).cloned() };
                if let Some(next_hop) = next_hop {
                    next_hop.send_frame(&frame).await?;
                } else {
                    eprintln!(
                        "relay.forward.missing_next_hop dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                }
                continue;
            }
            RouteDecision::DropNoRoute { destination_agent_id } => {
                eprintln!("relay.drop.no_route dst={}", destination_agent_id);
                continue;
            }
        }

        match frame.message {
            Message::TaskRequest(request) => {
                let response_dst = frame
                    .header
                    .src_agent
                    .clone()
                    .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                let result = dispatcher::dispatch(&request).await;
                let response = Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(response_dst),
                    Message::TaskResult(result),
                );
                peer.send_frame(&response).await?;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unexpected message on simplex task server: {:?}", other),
                ))
            }
        }
    }

    peer_map.lock().await.remove(&peer.session.remote.agent_id);
    registry
        .lock()
        .await
        .remove_peer_state(&peer.session.remote.agent_id);
    Ok(())
}

pub async fn run_outbound_relay_peer_simplex_http(
    identity: AgentIdentity,
    endpoint: &TunnelEndpoint,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: SimplexHttpTaskPeerMap,
    relay_stream_allocator: SimplexHttpRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    let peer = simplex_http_mux::connect_mux_peer(identity.clone(), &endpoint.url.original).await?;
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    peer_map
        .lock()
        .await
        .insert(peer.session.remote.agent_id.clone(), peer.clone());
    println!(
        "session.outbound.peer={} to={} via=simplex-http",
        peer.session.remote.agent_id, endpoint.url.original
    );
    send_direct_announce_simplex_http(&peer.inner, &identity, &local_services).await?;
    let route_snapshot = {
        let mut registry_guard = registry.lock().await;
        let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
        if pruned > 0 {
            eprintln!("registry.route_pruned={}", pruned);
        }
        registry_guard.routes_snapshot()
    };
    send_route_snapshot_simplex_http_mux(
        &peer,
        &identity,
        &route_snapshot,
        Some(peer.session.remote.agent_id.clone()),
    )
    .await?;

    let stream_peer = peer.clone();
    let stream_peer_map = peer_map.clone();
    let stream_allocator = relay_stream_allocator.clone();
    let stream_relay_links = relay_links.clone();
    tokio::spawn(async move {
        loop {
            let open_frame = match stream_peer.read_stream_open_frame().await {
                Ok(frame) => frame,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    eprintln!("stream.relay.accept.error={err}");
                    break;
                }
            };
            if let Err(err) = handle_simplex_relay_stream_open(
                stream_peer_map.clone(),
                stream_allocator.clone(),
                stream_relay_links.clone(),
                stream_peer.clone(),
                open_frame,
            )
            .await
            {
                eprintln!("stream.relay.open.error={err}");
            }
        }
    });

    loop {
        let frame = match peer.read_control_frame().await {
            Ok(frame) => frame,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };

        let handled_control = {
            let mut registry_guard = registry.lock().await;
            handle_registry_control_message(
                &mut registry_guard,
                &identity.id,
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_simplex_http(
                &peer_map,
                &identity,
                &peer.session.remote.agent_id,
                &frame,
            )
            .await?;
            continue;
        }

        let route_decision = {
            let mut registry_guard = registry.lock().await;
            let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
            if pruned > 0 {
                eprintln!("registry.route_pruned={}", pruned);
            }
            decide_frame_route(&identity.id, &registry_guard, &frame)
        };
        match route_decision {
            RouteDecision::Local => match frame.message {
                Message::TaskRequest(request) => {
                    let response_dst = frame
                        .header
                        .src_agent
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    let result = dispatcher::dispatch(&request).await;
                    let response = Frame::new(
                        MessageType::TaskResult,
                        Some(peer.session.local.agent_id.clone()),
                        Some(response_dst),
                        Message::TaskResult(result),
                    );
                    peer.send_frame(&response).await?;
                }
                other => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected message on outbound simplex relay peer: {:?}", other),
                    ))
                }
            },
            RouteDecision::Forward { next_hop_agent_id } => {
                if next_hop_agent_id == peer.session.remote.agent_id {
                    continue;
                }
                let next_hop = { peer_map.lock().await.get(&next_hop_agent_id).cloned() };
                if let Some(next_hop) = next_hop {
                    next_hop.send_frame(&frame).await?;
                } else {
                    eprintln!(
                        "relay.forward.missing_next_hop dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                }
            }
            RouteDecision::DropNoRoute { destination_agent_id } => {
                eprintln!("relay.drop.no_route dst={}", destination_agent_id);
            }
        }
    }

    peer_map.lock().await.remove(&peer.session.remote.agent_id);
    registry
        .lock()
        .await
        .remove_peer_state(&peer.session.remote.agent_id);
    Ok(())
}

pub async fn run_inbound_task_server_simplex_oss(
    identity: AgentIdentity,
    endpoint: &str,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: SimplexOssTaskPeerMap,
    relay_stream_allocator: SimplexOssRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    let peer = simplex_oss_mux::accept_mux_peer_on(identity.clone(), endpoint).await?;
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    peer_map
        .lock()
        .await
        .insert(peer.session.remote.agent_id.clone(), peer.clone());
    println!(
        "session.inbound.peer={} via=simplex-oss",
        peer.session.remote.agent_id
    );
    send_direct_announce_simplex_oss(&peer.inner, &identity, &local_services).await?;
    let route_snapshot = {
        let mut registry_guard = registry.lock().await;
        let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
        if pruned > 0 {
            eprintln!("registry.route_pruned={}", pruned);
        }
        registry_guard.routes_snapshot()
    };
    send_route_snapshot_simplex_oss_mux(
        &peer,
        &identity,
        &route_snapshot,
        Some(peer.session.remote.agent_id.clone()),
    )
    .await?;

    let stream_peer = peer.clone();
    let stream_peer_map = peer_map.clone();
    let stream_allocator = relay_stream_allocator.clone();
    let stream_relay_links = relay_links.clone();
    tokio::spawn(async move {
        loop {
            let open_frame = match stream_peer.read_stream_open_frame().await {
                Ok(frame) => frame,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    eprintln!("stream.relay.accept.error={err}");
                    break;
                }
            };
            if let Err(err) = handle_simplex_oss_relay_stream_open(
                stream_peer_map.clone(),
                stream_allocator.clone(),
                stream_relay_links.clone(),
                stream_peer.clone(),
                open_frame,
            )
            .await
            {
                eprintln!("stream.relay.open.error={err}");
            }
        }
    });

    loop {
        let frame = match peer.read_control_frame().await {
            Ok(frame) => frame,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };

        let handled_control = {
            let mut registry_guard = registry.lock().await;
            handle_registry_control_message(
                &mut registry_guard,
                &identity.id,
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_simplex_oss(
                &peer_map,
                &identity,
                &peer.session.remote.agent_id,
                &frame,
            )
            .await?;
            continue;
        }

        let route_decision = {
            let mut registry_guard = registry.lock().await;
            let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
            if pruned > 0 {
                eprintln!("registry.route_pruned={}", pruned);
            }
            decide_frame_route(&identity.id, &registry_guard, &frame)
        };
        match route_decision {
            RouteDecision::Local => {}
            RouteDecision::Forward { next_hop_agent_id } => {
                if next_hop_agent_id == peer.session.remote.agent_id {
                    continue;
                }
                let next_hop = { peer_map.lock().await.get(&next_hop_agent_id).cloned() };
                if let Some(next_hop) = next_hop {
                    next_hop.send_frame(&frame).await?;
                } else {
                    eprintln!(
                        "relay.forward.missing_next_hop dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                }
                continue;
            }
            RouteDecision::DropNoRoute { destination_agent_id } => {
                eprintln!("relay.drop.no_route dst={}", destination_agent_id);
                continue;
            }
        }

        match frame.message {
            Message::TaskRequest(request) => {
                let response_dst = frame
                    .header
                    .src_agent
                    .clone()
                    .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                let result = dispatcher::dispatch(&request).await;
                let response = Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(response_dst),
                    Message::TaskResult(result),
                );
                peer.send_frame(&response).await?;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unexpected message on simplex oss task server: {:?}", other),
                ))
            }
        }
    }

    peer_map.lock().await.remove(&peer.session.remote.agent_id);
    registry
        .lock()
        .await
        .remove_peer_state(&peer.session.remote.agent_id);
    Ok(())
}

pub async fn run_outbound_relay_peer_simplex_oss(
    identity: AgentIdentity,
    endpoint: &TunnelEndpoint,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: SimplexOssTaskPeerMap,
    relay_stream_allocator: SimplexOssRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    let peer = simplex_oss_mux::connect_mux_peer(identity.clone(), &endpoint.url.original).await?;

    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    peer_map
        .lock()
        .await
        .insert(peer.session.remote.agent_id.clone(), peer.clone());
    println!(
        "session.outbound.peer={} to={} via=simplex-oss",
        peer.session.remote.agent_id, endpoint.url.original
    );
    send_direct_announce_simplex_oss(&peer.inner, &identity, &local_services).await?;
    let route_snapshot = {
        let mut registry_guard = registry.lock().await;
        let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
        if pruned > 0 {
            eprintln!("registry.route_pruned={}", pruned);
        }
        registry_guard.routes_snapshot()
    };
    send_route_snapshot_simplex_oss_mux(
        &peer,
        &identity,
        &route_snapshot,
        Some(peer.session.remote.agent_id.clone()),
    )
    .await?;

    let stream_peer = peer.clone();
    let stream_peer_map = peer_map.clone();
    let stream_allocator = relay_stream_allocator.clone();
    let stream_relay_links = relay_links.clone();
    tokio::spawn(async move {
        loop {
            let open_frame = match stream_peer.read_stream_open_frame().await {
                Ok(frame) => frame,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    eprintln!("stream.relay.accept.error={err}");
                    break;
                }
            };
            if let Err(err) = handle_simplex_oss_relay_stream_open(
                stream_peer_map.clone(),
                stream_allocator.clone(),
                stream_relay_links.clone(),
                stream_peer.clone(),
                open_frame,
            )
            .await
            {
                eprintln!("stream.relay.open.error={err}");
            }
        }
    });

    loop {
        let frame = match peer.read_control_frame().await {
            Ok(frame) => frame,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };

        let handled_control = {
            let mut registry_guard = registry.lock().await;
            handle_registry_control_message(
                &mut registry_guard,
                &identity.id,
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_simplex_oss(
                &peer_map,
                &identity,
                &peer.session.remote.agent_id,
                &frame,
            )
            .await?;
            continue;
        }

        let route_decision = {
            let mut registry_guard = registry.lock().await;
            let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
            if pruned > 0 {
                eprintln!("registry.route_pruned={}", pruned);
            }
            decide_frame_route(&identity.id, &registry_guard, &frame)
        };
        match route_decision {
            RouteDecision::Local => match frame.message {
                Message::TaskRequest(request) => {
                    let response_dst = frame
                        .header
                        .src_agent
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    let result = dispatcher::dispatch(&request).await;
                    let response = Frame::new(
                        MessageType::TaskResult,
                        Some(peer.session.local.agent_id.clone()),
                        Some(response_dst),
                        Message::TaskResult(result),
                    );
                    peer.send_frame(&response).await?;
                }
                other => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected message on outbound simplex oss relay peer: {:?}", other),
                    ))
                }
            },
            RouteDecision::Forward { next_hop_agent_id } => {
                if next_hop_agent_id == peer.session.remote.agent_id {
                    continue;
                }
                let next_hop = { peer_map.lock().await.get(&next_hop_agent_id).cloned() };
                if let Some(next_hop) = next_hop {
                    next_hop.send_frame(&frame).await?;
                } else {
                    eprintln!(
                        "relay.forward.missing_next_hop dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                }
            }
            RouteDecision::DropNoRoute { destination_agent_id } => {
                eprintln!("relay.drop.no_route dst={}", destination_agent_id);
            }
        }
    }

    peer_map.lock().await.remove(&peer.session.remote.agent_id);
    registry
        .lock()
        .await
        .remove_peer_state(&peer.session.remote.agent_id);
    Ok(())
}

pub async fn run_inbound_task_server_simplex_dns(
    identity: AgentIdentity,
    socket: UdpSocket,
    path: &str,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: SimplexDnsTaskPeerMap,
    relay_stream_allocator: SimplexDnsRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    let peer = simplex_dns_mux::accept_mux_peer_on(identity.clone(), socket, path).await?;
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    peer_map
        .lock()
        .await
        .insert(peer.session.remote.agent_id.clone(), peer.clone());
    println!(
        "session.inbound.peer={} via=simplex-dns",
        peer.session.remote.agent_id
    );
    send_direct_announce_simplex_dns(&peer.inner, &identity, &local_services).await?;
    let route_snapshot = {
        let mut registry_guard = registry.lock().await;
        let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
        if pruned > 0 {
            eprintln!("registry.route_pruned={}", pruned);
        }
        registry_guard.routes_snapshot()
    };
    send_route_snapshot_simplex_dns_mux(
        &peer,
        &identity,
        &route_snapshot,
        Some(peer.session.remote.agent_id.clone()),
    )
    .await?;

    let stream_peer = peer.clone();
    let stream_peer_map = peer_map.clone();
    let stream_allocator = relay_stream_allocator.clone();
    let stream_relay_links = relay_links.clone();
    tokio::spawn(async move {
        loop {
            let open_frame = match stream_peer.read_stream_open_frame().await {
                Ok(frame) => frame,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    eprintln!("stream.relay.accept.error={err}");
                    break;
                }
            };
            if let Err(err) = handle_simplex_dns_relay_stream_open(
                stream_peer_map.clone(),
                stream_allocator.clone(),
                stream_relay_links.clone(),
                stream_peer.clone(),
                open_frame,
            )
            .await
            {
                eprintln!("stream.relay.open.error={err}");
            }
        }
    });

    loop {
        let frame = match peer.read_control_frame().await {
            Ok(frame) => frame,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };

        let handled_control = {
            let mut registry_guard = registry.lock().await;
            handle_registry_control_message(
                &mut registry_guard,
                &identity.id,
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_simplex_dns(
                &peer_map,
                &identity,
                &peer.session.remote.agent_id,
                &frame,
            )
            .await?;
            continue;
        }

        let route_decision = {
            let mut registry_guard = registry.lock().await;
            let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
            if pruned > 0 {
                eprintln!("registry.route_pruned={}", pruned);
            }
            decide_frame_route(&identity.id, &registry_guard, &frame)
        };
        match route_decision {
            RouteDecision::Local => {}
            RouteDecision::Forward { next_hop_agent_id } => {
                if next_hop_agent_id == peer.session.remote.agent_id {
                    continue;
                }
                let next_hop = { peer_map.lock().await.get(&next_hop_agent_id).cloned() };
                if let Some(next_hop) = next_hop {
                    next_hop.send_frame(&frame).await?;
                } else {
                    eprintln!(
                        "relay.forward.missing_next_hop dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                }
                continue;
            }
            RouteDecision::DropNoRoute { destination_agent_id } => {
                eprintln!("relay.drop.no_route dst={}", destination_agent_id);
                continue;
            }
        }

        match frame.message {
            Message::TaskRequest(request) => {
                let response_dst = frame
                    .header
                    .src_agent
                    .clone()
                    .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                let result = dispatcher::dispatch(&request).await;
                let response = Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(response_dst),
                    Message::TaskResult(result),
                );
                peer.send_frame(&response).await?;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unexpected message on simplex dns task server: {:?}", other),
                ))
            }
        }
    }

    peer_map.lock().await.remove(&peer.session.remote.agent_id);
    registry
        .lock()
        .await
        .remove_peer_state(&peer.session.remote.agent_id);
    Ok(())
}

pub async fn run_outbound_relay_peer_simplex_dns(
    identity: AgentIdentity,
    endpoint: &TunnelEndpoint,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: SimplexDnsTaskPeerMap,
    relay_stream_allocator: SimplexDnsRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    let peer = simplex_dns_mux::connect_mux_peer(identity.clone(), &endpoint.url.original).await?;

    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    peer_map
        .lock()
        .await
        .insert(peer.session.remote.agent_id.clone(), peer.clone());
    println!(
        "session.outbound.peer={} to={} via=simplex-dns",
        peer.session.remote.agent_id, endpoint.url.original
    );
    send_direct_announce_simplex_dns(&peer.inner, &identity, &local_services).await?;
    let route_snapshot = {
        let mut registry_guard = registry.lock().await;
        let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
        if pruned > 0 {
            eprintln!("registry.route_pruned={}", pruned);
        }
        registry_guard.routes_snapshot()
    };
    send_route_snapshot_simplex_dns_mux(
        &peer,
        &identity,
        &route_snapshot,
        Some(peer.session.remote.agent_id.clone()),
    )
    .await?;

    let stream_peer = peer.clone();
    let stream_peer_map = peer_map.clone();
    let stream_allocator = relay_stream_allocator.clone();
    let stream_relay_links = relay_links.clone();
    tokio::spawn(async move {
        loop {
            let open_frame = match stream_peer.read_stream_open_frame().await {
                Ok(frame) => frame,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    eprintln!("stream.relay.accept.error={err}");
                    break;
                }
            };
            if let Err(err) = handle_simplex_dns_relay_stream_open(
                stream_peer_map.clone(),
                stream_allocator.clone(),
                stream_relay_links.clone(),
                stream_peer.clone(),
                open_frame,
            )
            .await
            {
                eprintln!("stream.relay.open.error={err}");
            }
        }
    });

    loop {
        let frame = match peer.read_control_frame().await {
            Ok(frame) => frame,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };

        let handled_control = {
            let mut registry_guard = registry.lock().await;
            handle_registry_control_message(
                &mut registry_guard,
                &identity.id,
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_simplex_dns(
                &peer_map,
                &identity,
                &peer.session.remote.agent_id,
                &frame,
            )
            .await?;
            continue;
        }

        let route_decision = {
            let mut registry_guard = registry.lock().await;
            let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
            if pruned > 0 {
                eprintln!("registry.route_pruned={}", pruned);
            }
            decide_frame_route(&identity.id, &registry_guard, &frame)
        };
        match route_decision {
            RouteDecision::Local => match frame.message {
                Message::TaskRequest(request) => {
                    let response_dst = frame
                        .header
                        .src_agent
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    let result = dispatcher::dispatch(&request).await;
                    let response = Frame::new(
                        MessageType::TaskResult,
                        Some(peer.session.local.agent_id.clone()),
                        Some(response_dst),
                        Message::TaskResult(result),
                    );
                    peer.send_frame(&response).await?;
                }
                other => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected message on outbound simplex dns relay peer: {:?}", other),
                    ))
                }
            },
            RouteDecision::Forward { next_hop_agent_id } => {
                if next_hop_agent_id == peer.session.remote.agent_id {
                    continue;
                }
                let next_hop = { peer_map.lock().await.get(&next_hop_agent_id).cloned() };
                if let Some(next_hop) = next_hop {
                    next_hop.send_frame(&frame).await?;
                } else {
                    eprintln!(
                        "relay.forward.missing_next_hop dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                }
            }
            RouteDecision::DropNoRoute { destination_agent_id } => {
                eprintln!("relay.drop.no_route dst={}", destination_agent_id);
            }
        }
    }

    peer_map.lock().await.remove(&peer.session.remote.agent_id);
    registry
        .lock()
        .await
        .remove_peer_state(&peer.session.remote.agent_id);
    Ok(())
}

pub async fn run_outbound_relay_peer_ws(
    identity: AgentIdentity,
    endpoint: &TunnelEndpoint,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: WsTaskPeerMap,
    relay_stream_allocator: WsRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    let peer = ws_mux::connect_mux_peer(identity.clone(), &endpoint.url.original).await?;

    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    peer_map
        .lock()
        .await
        .insert(peer.session.remote.agent_id.clone(), peer.clone());
    println!(
        "session.outbound.peer={} to={} via=ws",
        peer.session.remote.agent_id, endpoint.url.original
    );
    send_direct_announce_ws_mux(&peer, &identity, &local_services).await?;
    let route_snapshot = {
        let mut registry_guard = registry.lock().await;
        let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
        if pruned > 0 {
            eprintln!("registry.route_pruned={}", pruned);
        }
        registry_guard.routes_snapshot()
    };
    send_route_snapshot_ws_mux(
        &peer,
        &identity,
        &route_snapshot,
        Some(peer.session.remote.agent_id.clone()),
    )
    .await?;

    let stream_peer = peer.clone();
    let stream_peer_map = peer_map.clone();
    let stream_allocator = relay_stream_allocator.clone();
    let stream_relay_links = relay_links.clone();
    tokio::spawn(async move {
        loop {
            let open_frame = match stream_peer.read_stream_open_frame().await {
                Ok(frame) => frame,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    eprintln!("stream.relay.accept.error={err}");
                    break;
                }
            };
            if let Err(err) = handle_ws_relay_stream_open(
                stream_peer_map.clone(),
                stream_allocator.clone(),
                stream_relay_links.clone(),
                stream_peer.clone(),
                open_frame,
            )
            .await
            {
                eprintln!("stream.relay.open.error={err}");
            }
        }
    });

    loop {
        let frame = match peer.read_control_frame().await {
            Ok(frame) => frame,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };

        let handled_control = {
            let mut registry_guard = registry.lock().await;
            handle_registry_control_message(
                &mut registry_guard,
                &identity.id,
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_ws(&peer_map, &identity, &peer.session.remote.agent_id, &frame)
                .await?;
            continue;
        }

        let route_decision = {
            let mut registry_guard = registry.lock().await;
            let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
            if pruned > 0 {
                eprintln!("registry.route_pruned={}", pruned);
            }
            decide_frame_route(&identity.id, &registry_guard, &frame)
        };
        match route_decision {
            RouteDecision::Local => match frame.message {
                Message::TaskRequest(request) => {
                    let response_dst = frame
                        .header
                        .src_agent
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    let result = dispatcher::dispatch(&request).await;
                    let response = Frame::new(
                        MessageType::TaskResult,
                        Some(peer.session.local.agent_id.clone()),
                        Some(response_dst),
                        Message::TaskResult(result),
                    );
                    peer.send_frame(&response).await?;
                }
                other => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("unexpected message on outbound ws relay peer: {:?}", other),
                    ))
                }
            },
            RouteDecision::Forward { next_hop_agent_id } => {
                if next_hop_agent_id == peer.session.remote.agent_id {
                    eprintln!(
                        "relay.drop.loop dst={:?} next_hop={} source_peer={}",
                        frame.header.dst_agent, next_hop_agent_id, peer.session.remote.agent_id
                    );
                    continue;
                }
                let next_hop = { peer_map.lock().await.get(&next_hop_agent_id).cloned() };
                if let Some(next_hop) = next_hop {
                    next_hop.send_frame(&frame).await?;
                    eprintln!(
                        "relay.forward.sent dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                } else {
                    eprintln!(
                        "relay.forward.missing_next_hop dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                }
            }
            RouteDecision::DropNoRoute {
                destination_agent_id,
            } => {
                eprintln!("relay.drop.no_route dst={}", destination_agent_id);
            }
        }
    }

    peer_map.lock().await.remove(&peer.session.remote.agent_id);
    registry
        .lock()
        .await
        .remove_peer_state(&peer.session.remote.agent_id);
    Ok(())
}

pub async fn run_inbound_task_server_ws(
    identity: AgentIdentity,
    listener: TcpListener,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: WsTaskPeerMap,
    relay_stream_allocator: WsRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    loop {
        let peer =
            ws_mux::accept_mux_peer_on(identity.clone(), &listener, tls_acceptor.clone()).await?;
        let identity = identity.clone();
        let local_services = local_services.clone();
        let hub = hub.clone();
        let registry = registry.clone();
        let peer_map = peer_map.clone();
        let relay_stream_allocator = relay_stream_allocator.clone();
        let relay_links = relay_links.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_inbound_task_peer_ws(
                identity,
                local_services,
                hub,
                registry,
                peer_map,
                relay_stream_allocator,
                relay_links,
                peer,
            )
            .await
            {
                eprintln!("session.inbound.peer.error={err}");
            }
        });
    }
}

pub async fn handle_inbound_task_peer_ws(
    identity: AgentIdentity,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    peer_map: WsTaskPeerMap,
    relay_stream_allocator: WsRelayStreamAllocator,
    relay_links: RelayLinkMap,
    peer: ws_mux::MuxWsPeer,
) -> Result<(), Error> {
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    peer_map
        .lock()
        .await
        .insert(peer.session.remote.agent_id.clone(), peer.clone());
    println!(
        "session.inbound.peer={} from={} via=ws",
        peer.session.remote.agent_id, peer.peer_addr
    );
    send_direct_announce_ws_mux(&peer, &identity, &local_services).await?;
    let route_snapshot = {
        let mut registry_guard = registry.lock().await;
        let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
        if pruned > 0 {
            eprintln!("registry.route_pruned={}", pruned);
        }
        registry_guard.routes_snapshot()
    };
    send_route_snapshot_ws_mux(
        &peer,
        &identity,
        &route_snapshot,
        Some(peer.session.remote.agent_id.clone()),
    )
    .await?;

    let stream_peer = peer.clone();
    let stream_peer_map = peer_map.clone();
    let stream_allocator = relay_stream_allocator.clone();
    let stream_relay_links = relay_links.clone();
    tokio::spawn(async move {
        loop {
            let open_frame = match stream_peer.read_stream_open_frame().await {
                Ok(frame) => frame,
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    eprintln!("stream.relay.accept.error={err}");
                    break;
                }
            };
            if let Err(err) = handle_ws_relay_stream_open(
                stream_peer_map.clone(),
                stream_allocator.clone(),
                stream_relay_links.clone(),
                stream_peer.clone(),
                open_frame,
            )
            .await
            {
                eprintln!("stream.relay.open.error={err}");
            }
        }
    });

    loop {
        let frame = match peer.read_control_frame().await {
            Ok(frame) => frame,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };

        let handled_control = {
            let mut registry_guard = registry.lock().await;
            handle_registry_control_message(
                &mut registry_guard,
                &identity.id,
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_ws(&peer_map, &identity, &peer.session.remote.agent_id, &frame)
                .await?;
            continue;
        }

        let route_decision = {
            let mut registry_guard = registry.lock().await;
            let pruned = registry_guard.prune_stale_routes(ROUTE_TTL_SECS);
            if pruned > 0 {
                eprintln!("registry.route_pruned={}", pruned);
            }
            decide_frame_route(&identity.id, &registry_guard, &frame)
        };
        match route_decision {
            RouteDecision::Local => {}
            RouteDecision::Forward { next_hop_agent_id } => {
                if next_hop_agent_id == peer.session.remote.agent_id {
                    eprintln!(
                        "relay.drop.loop dst={:?} next_hop={} source_peer={}",
                        frame.header.dst_agent, next_hop_agent_id, peer.session.remote.agent_id
                    );
                    continue;
                }
                let next_hop = { peer_map.lock().await.get(&next_hop_agent_id).cloned() };
                if let Some(next_hop) = next_hop {
                    next_hop.send_frame(&frame).await?;
                    eprintln!(
                        "relay.forward.sent dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                } else {
                    eprintln!(
                        "relay.forward.missing_next_hop dst={:?} next_hop={}",
                        frame.header.dst_agent, next_hop_agent_id
                    );
                }
                continue;
            }
            RouteDecision::DropNoRoute {
                destination_agent_id,
            } => {
                eprintln!("relay.drop.no_route dst={}", destination_agent_id);
                continue;
            }
        }

        match frame.message {
            Message::TaskRequest(request) => {
                let response_dst = frame
                    .header
                    .src_agent
                    .clone()
                    .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                let result = dispatcher::dispatch(&request).await;
                let response = Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(response_dst),
                    Message::TaskResult(result),
                );
                peer.send_frame(&response).await?;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unexpected message on ws task server: {:?}", other),
                ))
            }
        }
    }

    peer_map.lock().await.remove(&peer.session.remote.agent_id);
    registry
        .lock()
        .await
        .remove_peer_state(&peer.session.remote.agent_id);
    Ok(())
}
