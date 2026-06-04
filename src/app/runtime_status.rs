use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    path::PathBuf,
    sync::Arc,
};

use chrono::Utc;
use tokio::{sync::Mutex, time::sleep};

use crate::{
    agent::{
        registry::{AgentRegistry, RegisteredPeer, RegisteredRoute, RegisteredStream},
        state::AgentRuntimeState,
    },
    app::config::{ControlCommandConfig, StatusScope},
    session::{hub::SessionHub, peer::PeerSession},
};

pub type RelayLinkMap = Arc<Mutex<HashMap<String, RelayStreamLink>>>;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RuntimeStatusSnapshot {
    pub generated_at_unix: i64,
    pub lines: Vec<String>,
    pub peer_count: usize,
    pub route_count: usize,
    pub stream_count: usize,
    pub sessions: Vec<PeerSession>,
    pub peers: Vec<RegisteredPeer>,
    pub routes: Vec<RegisteredRoute>,
    pub streams: Vec<RegisteredStream>,
    pub local_services: Vec<String>,
    pub relay_links: Vec<RelayStreamLink>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct RelayStreamLink {
    pub transport: String,
    pub source_peer_agent_id: String,
    pub source_stream_id: u32,
    pub next_hop_agent_id: String,
    pub relay_stream_id: u32,
    pub destination_agent_id: String,
    pub opened_at_unix: i64,
}

impl RelayStreamLink {
    pub fn summary_line(&self) -> String {
        format!(
            "relay.link transport={} src_peer={} src_stream={} next_hop={} relay_stream={} dst={} opened_at={}",
            self.transport,
            self.source_peer_agent_id,
            self.source_stream_id,
            self.next_hop_agent_id,
            self.relay_stream_id,
            self.destination_agent_id,
            self.opened_at_unix
        )
    }
}

pub fn runtime_status_snapshot_path(data_dir: &PathBuf) -> PathBuf {
    data_dir.join("runtime-status.json")
}

pub fn relay_link_key(
    transport: &str,
    source_peer_agent_id: &str,
    source_stream_id: u32,
) -> String {
    format!("{transport}:{source_peer_agent_id}:{source_stream_id}")
}

pub async fn upsert_relay_link(
    relay_links: &RelayLinkMap,
    transport: &str,
    source_peer_agent_id: String,
    source_stream_id: u32,
    next_hop_agent_id: String,
    relay_stream_id: u32,
    destination_agent_id: String,
) {
    relay_links.lock().await.insert(
        relay_link_key(transport, &source_peer_agent_id, source_stream_id),
        RelayStreamLink {
            transport: transport.to_string(),
            source_peer_agent_id,
            source_stream_id,
            next_hop_agent_id,
            relay_stream_id,
            destination_agent_id,
            opened_at_unix: Utc::now().timestamp(),
        },
    );
}

pub async fn remove_relay_link(
    relay_links: &RelayLinkMap,
    transport: &str,
    source_peer_agent_id: &str,
    source_stream_id: u32,
) {
    relay_links.lock().await.remove(&relay_link_key(
        transport,
        source_peer_agent_id,
        source_stream_id,
    ));
}

pub async fn write_status_snapshot(
    data_dir: &PathBuf,
    hub: &Arc<Mutex<SessionHub>>,
    registry: &Arc<Mutex<AgentRegistry>>,
    relay_links: &RelayLinkMap,
) -> Result<(), Error> {
    tokio::fs::create_dir_all(data_dir).await?;
    let hub_guard = hub.lock().await;
    let sessions = hub_guard.sessions_snapshot();
    let mut lines = hub_guard.summary_lines();
    drop(hub_guard);

    let registry_guard = registry.lock().await;
    let local_services = registry_guard.local_services_snapshot();
    let peers = registry_guard.peers_snapshot();
    let routes = registry_guard.routes_snapshot();
    let streams = registry_guard.streams_snapshot();
    let registry_summary = registry_guard.summary_lines();
    drop(registry_guard);

    lines.extend(registry_summary.clone());
    let relay_links = {
        let guard = relay_links.lock().await;
        let mut links: Vec<_> = guard.values().cloned().collect();
        links.sort_by(|a, b| {
            a.transport
                .cmp(&b.transport)
                .then(a.source_peer_agent_id.cmp(&b.source_peer_agent_id))
                .then(a.source_stream_id.cmp(&b.source_stream_id))
        });
        links
    };
    lines.push(format!("relay.link_count={}", relay_links.len()));
    for link in &relay_links {
        lines.push(link.summary_line());
    }
    let state = AgentRuntimeState {
        sessions: sessions.clone(),
        peers: peers.clone(),
        routes: routes.clone(),
        streams: streams.clone(),
        exposed_services: local_services.clone(),
    };
    lines.extend(state.summary_lines());
    let snapshot = RuntimeStatusSnapshot {
        generated_at_unix: Utc::now().timestamp(),
        lines,
        peer_count: state.peers.len(),
        route_count: state.routes.len(),
        stream_count: state.streams.len(),
        sessions,
        peers,
        routes,
        streams,
        local_services,
        relay_links,
    };
    let path = runtime_status_snapshot_path(data_dir);
    let payload = serde_json::to_vec_pretty(&snapshot)
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
    tokio::fs::write(path, payload).await
}

pub fn render_status_lines(snapshot: &RuntimeStatusSnapshot, scope: StatusScope) -> Vec<String> {
    let mut lines = vec![format!(
        "status.generated_at={}",
        snapshot.generated_at_unix
    )];
    match scope {
        StatusScope::All => {
            lines.push(format!("session.count={}", snapshot.sessions.len()));
            for session in &snapshot.sessions {
                lines.push(format!(
                    "session.peer={} name={} state={:?}",
                    session.remote.agent_id, session.remote.agent_name, session.state
                ));
            }
            lines.push(format!("registry.peer_count={}", snapshot.peers.len()));
            for peer in &snapshot.peers {
                lines.push(format!(
                    "registry.peer={} name={} state={:?} last_seen={}",
                    peer.session.remote.agent_id,
                    peer.session.remote.agent_name,
                    peer.session.state,
                    peer.last_seen_unix
                ));
            }
            lines.push(format!("registry.route_count={}", snapshot.routes.len()));
            for route in &snapshot.routes {
                let path = route
                    .path
                    .iter()
                    .map(|hop| hop.agent_id.as_str())
                    .collect::<Vec<_>>()
                    .join(">");
                lines.push(format!(
                    "registry.route={} next_hop={} hops={} path={} services={} capabilities={} learned_at={}",
                    route.destination_agent_id,
                    route.next_hop_agent_id,
                    route.hop_count,
                    path,
                    route.services.join(","),
                    route.capabilities.join(","),
                    route.learned_at_unix
                ));
            }
            lines.push(format!("registry.stream_count={}", snapshot.streams.len()));
            for stream in &snapshot.streams {
                lines.push(format!(
                    "registry.stream={} peer={} service={} target={} state={:?} opened_at={} closed_at={} last_error={}",
                    stream.stream_id,
                    stream.peer_agent_id,
                    stream.service,
                    stream.target,
                    stream.state,
                    stream.opened_at_unix,
                    stream
                        .closed_at_unix
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    stream
                        .last_error
                        .clone()
                        .unwrap_or_else(|| "-".to_string())
                ));
            }
            lines.push(format!("relay.link_count={}", snapshot.relay_links.len()));
            for link in &snapshot.relay_links {
                lines.push(link.summary_line());
            }
        }
        StatusScope::Peers => {
            lines.push(format!("session.count={}", snapshot.sessions.len()));
            for session in &snapshot.sessions {
                lines.push(format!(
                    "session.peer={} name={} state={:?}",
                    session.remote.agent_id, session.remote.agent_name, session.state
                ));
            }
            lines.push(format!("registry.peer_count={}", snapshot.peers.len()));
            for peer in &snapshot.peers {
                lines.push(format!(
                    "registry.peer={} name={} state={:?} last_seen={}",
                    peer.session.remote.agent_id,
                    peer.session.remote.agent_name,
                    peer.session.state,
                    peer.last_seen_unix
                ));
            }
        }
        StatusScope::Routes => {
            lines.push(format!("registry.route_count={}", snapshot.routes.len()));
            for route in &snapshot.routes {
                let path = route
                    .path
                    .iter()
                    .map(|hop| hop.agent_id.as_str())
                    .collect::<Vec<_>>()
                    .join(">");
                lines.push(format!(
                    "registry.route={} next_hop={} hops={} path={} services={} capabilities={} learned_at={}",
                    route.destination_agent_id,
                    route.next_hop_agent_id,
                    route.hop_count,
                    path,
                    route.services.join(","),
                    route.capabilities.join(","),
                    route.learned_at_unix
                ));
            }
        }
        StatusScope::Streams => {
            lines.push(format!("registry.stream_count={}", snapshot.streams.len()));
            for stream in &snapshot.streams {
                lines.push(format!(
                    "registry.stream={} peer={} service={} target={} state={:?} opened_at={} closed_at={} last_error={}",
                    stream.stream_id,
                    stream.peer_agent_id,
                    stream.service,
                    stream.target,
                    stream.state,
                    stream.opened_at_unix,
                    stream
                        .closed_at_unix
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    stream
                        .last_error
                        .clone()
                        .unwrap_or_else(|| "-".to_string())
                ));
            }
            lines.push(format!("relay.link_count={}", snapshot.relay_links.len()));
            for link in &snapshot.relay_links {
                lines.push(link.summary_line());
            }
        }
    }
    lines
}

fn render_services_lines(snapshot: &RuntimeStatusSnapshot) -> Vec<String> {
    let mut lines = vec![format!(
        "status.generated_at={}",
        snapshot.generated_at_unix
    )];
    lines.push(format!(
        "service.exposed_count={}",
        snapshot.local_services.len()
    ));
    for service in &snapshot.local_services {
        lines.push(service.clone());
    }

    let remote_count: usize = snapshot
        .routes
        .iter()
        .map(|route| route.services.len())
        .sum();
    lines.push(format!("service.remote_count={remote_count}"));
    for route in &snapshot.routes {
        for service in &route.services {
            lines.push(format!(
                "service.remote={} owner={} next_hop={} hops={}",
                service, route.destination_agent_id, route.next_hop_agent_id, route.hop_count
            ));
        }
    }
    lines
}

pub fn spawn_status_snapshot_task(
    data_dir: PathBuf,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    relay_links: RelayLinkMap,
) {
    tokio::spawn(async move {
        loop {
            if let Err(err) = write_status_snapshot(&data_dir, &hub, &registry, &relay_links).await
            {
                eprintln!("status.snapshot.error={}", err);
            }
            sleep(std::time::Duration::from_secs(1)).await;
        }
    });
}

pub async fn print_status_snapshot(
    data_dir: &PathBuf,
    scope: StatusScope,
    json: bool,
) -> Result<(), Error> {
    let path = runtime_status_snapshot_path(data_dir);
    let payload = tokio::fs::read(&path).await.map_err(|err| {
        Error::new(
            err.kind(),
            format!(
                "failed to read runtime status snapshot {}: {}",
                path.display(),
                err
            ),
        )
    })?;
    let snapshot: RuntimeStatusSnapshot = serde_json::from_slice(&payload)
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
    if json {
        let filtered = match scope {
            StatusScope::All => serde_json::json!({
                "generated_at_unix": snapshot.generated_at_unix,
                "session_count": snapshot.sessions.len(),
                "peer_count": snapshot.peer_count,
                "route_count": snapshot.route_count,
                "stream_count": snapshot.stream_count,
                "sessions": snapshot.sessions,
                "peers": snapshot.peers,
                "routes": snapshot.routes,
                "streams": snapshot.streams,
                "relay_links": snapshot.relay_links,
            }),
            StatusScope::Peers => serde_json::json!({
                "generated_at_unix": snapshot.generated_at_unix,
                "sessions": snapshot.sessions,
                "peers": snapshot.peers,
            }),
            StatusScope::Routes => serde_json::json!({
                "generated_at_unix": snapshot.generated_at_unix,
                "routes": snapshot.routes,
            }),
            StatusScope::Streams => serde_json::json!({
                "generated_at_unix": snapshot.generated_at_unix,
                "streams": snapshot.streams,
                "relay_links": snapshot.relay_links,
            }),
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&filtered)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?
        );
        return Ok(());
    }
    for line in render_status_lines(&snapshot, scope) {
        println!("{line}");
    }
    Ok(())
}

pub async fn print_control_snapshot(
    data_dir: &PathBuf,
    command: &ControlCommandConfig,
) -> Result<(), Error> {
    let path = runtime_status_snapshot_path(data_dir);
    let payload = tokio::fs::read(&path).await.map_err(|err| {
        Error::new(
            err.kind(),
            format!(
                "failed to read runtime status snapshot {}: {}",
                path.display(),
                err
            ),
        )
    })?;
    let snapshot: RuntimeStatusSnapshot = serde_json::from_slice(&payload)
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

    match command {
        ControlCommandConfig::PeersList { json } => {
            if *json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "generated_at_unix": snapshot.generated_at_unix,
                        "sessions": snapshot.sessions,
                        "peers": snapshot.peers,
                    }))
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?
                );
            } else {
                for line in render_status_lines(&snapshot, StatusScope::Peers) {
                    println!("{line}");
                }
            }
        }
        ControlCommandConfig::PeersInfo { peer_id, json } => {
            let session = snapshot
                .sessions
                .iter()
                .find(|session| session.remote.agent_id == *peer_id)
                .cloned();
            let peer = snapshot
                .peers
                .iter()
                .find(|peer| peer.session.remote.agent_id == *peer_id)
                .cloned();
            let routes: Vec<_> = snapshot
                .routes
                .iter()
                .filter(|route| {
                    route.destination_agent_id == *peer_id || route.next_hop_agent_id == *peer_id
                })
                .cloned()
                .collect();
            let streams: Vec<_> = snapshot
                .streams
                .iter()
                .filter(|stream| stream.peer_agent_id == *peer_id)
                .cloned()
                .collect();
            let relay_links: Vec<_> = snapshot
                .relay_links
                .iter()
                .filter(|link| {
                    link.source_peer_agent_id == *peer_id
                        || link.next_hop_agent_id == *peer_id
                        || link.destination_agent_id == *peer_id
                })
                .cloned()
                .collect();
            if *json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "generated_at_unix": snapshot.generated_at_unix,
                        "peer_id": peer_id,
                        "session": session,
                        "peer": peer,
                        "routes": routes,
                        "streams": streams,
                        "relay_links": relay_links,
                    }))
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?
                );
            } else {
                println!("status.generated_at={}", snapshot.generated_at_unix);
                println!("peer.id={peer_id}");
                if let Some(session) = session {
                    println!(
                        "peer.session.remote_name={} state={:?}",
                        session.remote.agent_name, session.state
                    );
                }
                if let Some(peer) = peer {
                    println!("peer.last_seen={}", peer.last_seen_unix);
                    println!(
                        "peer.capabilities={}",
                        peer.session.remote.capabilities.join(",")
                    );
                }
                println!("peer.route_count={}", routes.len());
                for route in routes {
                    let path = route
                        .path
                        .iter()
                        .map(|hop| hop.agent_id.as_str())
                        .collect::<Vec<_>>()
                        .join(">");
                    println!(
                        "peer.route={} next_hop={} hops={} path={} services={}",
                        route.destination_agent_id,
                        route.next_hop_agent_id,
                        route.hop_count,
                        path,
                        route.services.join(",")
                    );
                }
                println!("peer.stream_count={}", streams.len());
                for stream in streams {
                    println!(
                        "peer.stream={} service={} target={} state={:?}",
                        stream.stream_id, stream.service, stream.target, stream.state
                    );
                }
                println!("peer.relay_link_count={}", relay_links.len());
                for link in relay_links {
                    println!("{}", link.summary_line());
                }
            }
        }
        ControlCommandConfig::RoutesList { json } => {
            if *json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "generated_at_unix": snapshot.generated_at_unix,
                        "routes": snapshot.routes,
                    }))
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?
                );
            } else {
                for line in render_status_lines(&snapshot, StatusScope::Routes) {
                    println!("{line}");
                }
            }
        }
        ControlCommandConfig::ServicesList { json } => {
            if *json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "generated_at_unix": snapshot.generated_at_unix,
                        "local_services": snapshot.local_services,
                        "remote_services": snapshot.routes.iter().map(|route| serde_json::json!({
                            "owner_agent_id": route.destination_agent_id,
                            "owner_agent_name": route.destination_agent_name,
                            "next_hop_agent_id": route.next_hop_agent_id,
                            "hop_count": route.hop_count,
                            "path": route.path,
                            "services": route.services,
                        })).collect::<Vec<_>>(),
                    }))
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?
                );
            } else {
                for line in render_services_lines(&snapshot) {
                    println!("{line}");
                }
            }
        }
    }

    Ok(())
}
