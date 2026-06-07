use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    path::PathBuf,
    sync::Arc,
};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, time::sleep};

use crate::{
    agent::{
        registry::{AgentRegistry, RegisteredPeer, RegisteredRoute, RegisteredStream},
        state::AgentRuntimeState,
    },
    app::config::{AppConfig, ControlCommandConfig, StatusScope},
    error::{recent_errors_snapshot, RecordedError},
    session::{hub::SessionHub, peer::PeerSession},
};

pub type RelayLinkMap = Arc<Mutex<HashMap<String, RelayStreamLink>>>;
pub type UpstreamPoolStatusMap = Arc<Mutex<Vec<UpstreamPoolEntryStatus>>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UpstreamPoolEntryStatus {
    pub handler: String,
    pub transport: String,
    pub cached_keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UpstreamConfigSummary {
    pub conn_policy: String,
    pub connect_urls: Vec<String>,
    pub up_connect_urls: Vec<String>,
    pub down_connect_urls: Vec<String>,
    pub proxy_chain_count: usize,
    pub front_proxy_configured: bool,
}

pub async fn publish_upstream_pool_status(
    status: &UpstreamPoolStatusMap,
    handler: &str,
    transport: &str,
    cached_keys: Vec<String>,
) {
    let mut guard = status.lock().await;
    if let Some(entry) = guard
        .iter_mut()
        .find(|entry| entry.handler == handler && entry.transport == transport)
    {
        entry.cached_keys = cached_keys;
        return;
    }
    guard.push(UpstreamPoolEntryStatus {
        handler: handler.to_string(),
        transport: transport.to_string(),
        cached_keys,
    });
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RuntimeConfigSummary {
    pub wrapper_compress: bool,
    pub wrapper_padding: Option<usize>,
    pub shared_key_configured: bool,
    #[serde(default)]
    pub upstream: UpstreamConfigSummary,
    #[serde(default)]
    pub tls: crate::tunnel::tls::TlsUsageSummary,
}

impl RuntimeConfigSummary {
    pub fn from_app_config(config: &AppConfig) -> Self {
        let connect_urls: Vec<_> = config
            .connects
            .iter()
            .chain(config.up_connects.iter())
            .chain(config.down_connects.iter())
            .map(|endpoint| endpoint.url.clone())
            .collect();
        let listen_urls: Vec<_> = config
            .listens
            .iter()
            .map(|endpoint| endpoint.url.clone())
            .collect();
        let tls = crate::tunnel::tls::summarize_tls_from_urls(&listen_urls, &connect_urls);
        Self {
            wrapper_compress: config.wrapper.compress,
            wrapper_padding: config.wrapper.padding,
            shared_key_configured: config.identity.key.is_some(),
            upstream: UpstreamConfigSummary {
                conn_policy: format!("{:?}", config.conn_policy),
                connect_urls: config
                    .connects
                    .iter()
                    .map(|endpoint| endpoint.url.original.clone())
                    .collect(),
                up_connect_urls: config
                    .up_connects
                    .iter()
                    .map(|endpoint| endpoint.url.original.clone())
                    .collect(),
                down_connect_urls: config
                    .down_connects
                    .iter()
                    .map(|endpoint| endpoint.url.original.clone())
                    .collect(),
                proxy_chain_count: config.proxy_chain.len(),
                front_proxy_configured: config.front_proxy.is_some(),
            },
            tls,
        }
    }

    pub fn summary_lines(&self) -> Vec<String> {
        let mut lines = vec![
            format!("config.wrapper.compress={}", self.wrapper_compress),
            format!(
                "config.wrapper.padding={}",
                self.wrapper_padding
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "disabled".to_string())
            ),
            format!(
                "config.shared_key={}",
                if self.shared_key_configured {
                    "configured"
                } else {
                    "disabled"
                }
            ),
            format!("config.conn_policy={}", self.upstream.conn_policy),
            format!("config.connect.count={}", self.upstream.connect_urls.len()),
            format!(
                "config.up_connect.count={}",
                self.upstream.up_connect_urls.len()
            ),
            format!(
                "config.down_connect.count={}",
                self.upstream.down_connect_urls.len()
            ),
            format!(
                "config.proxy.chain.count={}",
                self.upstream.proxy_chain_count + usize::from(self.upstream.front_proxy_configured)
            ),
            format!("config.tls.wss_listen={}", self.tls.wss_listen_endpoints),
            format!("config.tls.wss_connect={}", self.tls.wss_connect_endpoints),
            format!("config.tls.insecure={}", self.tls.insecure_enabled),
            format!("config.tls.custom_ca={}", self.tls.custom_ca_configured),
            format!(
                "config.tls.client_identity={}",
                self.tls.client_identity_configured
            ),
            format!(
                "config.tls.listener_mtls={}",
                self.tls.listener_mutual_tls_enabled
            ),
        ];
        for url in &self.upstream.connect_urls {
            lines.push(format!("config.connect={url}"));
        }
        for url in &self.upstream.up_connect_urls {
            lines.push(format!("config.up_connect={url}"));
        }
        for url in &self.upstream.down_connect_urls {
            lines.push(format!("config.down_connect={url}"));
        }
        lines
    }
}

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
    #[serde(default)]
    pub config: RuntimeConfigSummary,
    #[serde(default)]
    pub upstream_pools: Vec<UpstreamPoolEntryStatus>,
    #[serde(default)]
    pub recent_errors: Vec<RecordedError>,
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
    upstream_pools: &UpstreamPoolStatusMap,
    config: &RuntimeConfigSummary,
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
    lines.extend(config.summary_lines());
    let recent_errors = recent_errors_snapshot();
    lines.push(format!("recent.error_count={}", recent_errors.len()));
    for entry in &recent_errors {
        lines.push(entry.summary_line());
    }
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
    let upstream_pools = {
        let guard = upstream_pools.lock().await;
        let mut pools = guard.clone();
        pools.sort_by(|a, b| {
            a.handler
                .cmp(&b.handler)
                .then(a.transport.cmp(&b.transport))
        });
        pools
    };
    lines.push(format!("upstream.pool_count={}", upstream_pools.len()));
    for pool in &upstream_pools {
        lines.push(format!(
            "upstream.pool handler={} transport={} cached_keys={}",
            pool.handler,
            pool.transport,
            pool.cached_keys.join(",")
        ));
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
        config: config.clone(),
        upstream_pools,
        recent_errors,
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
            lines.extend(snapshot.config.summary_lines());
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
                    "registry.route={} next_hop={} hops={} path={} services={} capabilities={} learned_from={} selected_by={} learned_at={} last_success={}",
                    route.destination_agent_id,
                    route.next_hop_agent_id,
                    route.hop_count,
                    path,
                    route.services.join(","),
                    route.capabilities.join(","),
                    route.learned_from,
                    route.selection_reason,
                    route.learned_at_unix,
                    route
                        .last_success_at_unix
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string())
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
            lines.push(format!(
                "upstream.pool_count={}",
                snapshot.upstream_pools.len()
            ));
            for pool in &snapshot.upstream_pools {
                lines.push(format!(
                    "upstream.pool handler={} transport={} cached_keys={}",
                    pool.handler,
                    pool.transport,
                    pool.cached_keys.join(",")
                ));
            }
            lines.push(format!(
                "recent.error_count={}",
                snapshot.recent_errors.len()
            ));
            for entry in &snapshot.recent_errors {
                lines.push(entry.summary_line());
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
                    "registry.route={} next_hop={} hops={} path={} services={} capabilities={} learned_from={} selected_by={} learned_at={} last_success={}",
                    route.destination_agent_id,
                    route.next_hop_agent_id,
                    route.hop_count,
                    path,
                    route.services.join(","),
                    route.capabilities.join(","),
                    route.learned_from,
                    route.selection_reason,
                    route.learned_at_unix,
                    route
                        .last_success_at_unix
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string())
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
    upstream_pools: UpstreamPoolStatusMap,
    config: RuntimeConfigSummary,
) {
    tokio::spawn(async move {
        loop {
            if let Err(err) = write_status_snapshot(
                &data_dir,
                &hub,
                &registry,
                &relay_links,
                &upstream_pools,
                &config,
            )
            .await
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
                "config": snapshot.config,
                "sessions": snapshot.sessions,
                "peers": snapshot.peers,
                "routes": snapshot.routes,
                "streams": snapshot.streams,
                "relay_links": snapshot.relay_links,
                "upstream_pools": snapshot.upstream_pools,
                "recent_errors": snapshot.recent_errors,
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

pub fn filter_status_snapshot_json(
    snapshot: &RuntimeStatusSnapshot,
    scope: StatusScope,
) -> Result<String, Error> {
    let filtered = match scope {
        StatusScope::All => serde_json::json!({
            "generated_at_unix": snapshot.generated_at_unix,
            "session_count": snapshot.sessions.len(),
            "peer_count": snapshot.peer_count,
            "route_count": snapshot.route_count,
            "stream_count": snapshot.stream_count,
            "config": snapshot.config,
            "sessions": snapshot.sessions,
            "peers": snapshot.peers,
            "routes": snapshot.routes,
            "streams": snapshot.streams,
            "relay_links": snapshot.relay_links,
            "upstream_pools": snapshot.upstream_pools,
            "recent_errors": snapshot.recent_errors,
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
    serde_json::to_string(&filtered).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))
}

pub fn read_status_json(data_dir: &PathBuf, scope: StatusScope) -> Result<String, Error> {
    let path = runtime_status_snapshot_path(data_dir);
    let payload = std::fs::read(&path).map_err(|err| {
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
    filter_status_snapshot_json(&snapshot, scope)
}

pub async fn refresh_status_json(
    data_dir: &PathBuf,
    hub: &Arc<Mutex<SessionHub>>,
    registry: &Arc<Mutex<AgentRegistry>>,
    relay_links: &RelayLinkMap,
    upstream_pools: &UpstreamPoolStatusMap,
    config: &RuntimeConfigSummary,
    scope: StatusScope,
) -> Result<String, Error> {
    write_status_snapshot(data_dir, hub, registry, relay_links, upstream_pools, config).await?;
    read_status_json(data_dir, scope)
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
                        "peer.route={} next_hop={} hops={} path={} services={} learned_from={} selected_by={}",
                        route.destination_agent_id,
                        route.next_hop_agent_id,
                        route.hop_count,
                        path,
                        route.services.join(","),
                        route.learned_from,
                        route.selection_reason
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app::config::StatusScope,
        error::{
            clear_recent_errors_for_tests, recent_error_test_guard, recent_errors_snapshot,
            record_error, ErrorCode,
        },
    };

    #[test]
    fn render_status_lines_includes_upstream_pools_and_connect_config() {
        let snapshot = RuntimeStatusSnapshot {
            generated_at_unix: 1,
            lines: Vec::new(),
            peer_count: 0,
            route_count: 0,
            stream_count: 0,
            sessions: Vec::new(),
            peers: Vec::new(),
            routes: Vec::new(),
            streams: Vec::new(),
            local_services: Vec::new(),
            relay_links: Vec::new(),
            upstream_pools: vec![UpstreamPoolEntryStatus {
                handler: "socks5".into(),
                transport: "tcp_mux".into(),
                cached_keys: vec!["tcp://127.0.0.1:34996".into()],
            }],
            config: RuntimeConfigSummary {
                wrapper_compress: false,
                wrapper_padding: None,
                shared_key_configured: false,
                upstream: UpstreamConfigSummary {
                    conn_policy: "Fallback".into(),
                    connect_urls: vec!["tcp://127.0.0.1:34996".into()],
                    up_connect_urls: Vec::new(),
                    down_connect_urls: Vec::new(),
                    proxy_chain_count: 1,
                    front_proxy_configured: true,
                },
                tls: Default::default(),
            },
            recent_errors: Vec::new(),
        };
        let lines = render_status_lines(&snapshot, StatusScope::All);
        let joined = lines.join("\n");
        assert!(joined.contains("config.conn_policy=Fallback"));
        assert!(joined.contains("config.connect=tcp://127.0.0.1:34996"));
        assert!(joined.contains("upstream.pool_count=1"));
        assert!(joined.contains("handler=socks5 transport=tcp_mux"));
    }

    #[tokio::test]
    async fn publish_upstream_pool_status_updates_existing_entry() {
        let status: UpstreamPoolStatusMap = Arc::new(Mutex::new(Vec::new()));
        publish_upstream_pool_status(
            &status,
            "http",
            "ws_mux",
            vec!["ws://127.0.0.1:1/tunnel".into()],
        )
        .await;
        publish_upstream_pool_status(
            &status,
            "http",
            "ws_mux",
            vec![
                "ws://127.0.0.1:1/tunnel".into(),
                "ws://127.0.0.1:2/tunnel".into(),
            ],
        )
        .await;
        let guard = status.lock().await;
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].cached_keys.len(), 2);
    }

    #[test]
    fn render_status_lines_includes_config_and_recent_errors() {
        let _guard = recent_error_test_guard();
        clear_recent_errors_for_tests();
        record_error(ErrorCode::RouteNoRoute, "missing route", false, None, None);
        let snapshot = RuntimeStatusSnapshot {
            generated_at_unix: 1,
            lines: Vec::new(),
            peer_count: 0,
            route_count: 0,
            stream_count: 0,
            sessions: Vec::new(),
            peers: Vec::new(),
            routes: Vec::new(),
            streams: Vec::new(),
            local_services: Vec::new(),
            relay_links: Vec::new(),
            upstream_pools: Vec::new(),
            config: RuntimeConfigSummary {
                wrapper_compress: true,
                wrapper_padding: Some(32),
                shared_key_configured: true,
                ..RuntimeConfigSummary::default()
            },
            recent_errors: recent_errors_snapshot(),
        };
        let lines = render_status_lines(&snapshot, StatusScope::All);
        let joined = lines.join("\n");
        assert!(joined.contains("config.wrapper.compress=true"));
        assert!(joined.contains("config.wrapper.padding=32"));
        assert!(joined.contains("config.shared_key=configured"));
        assert!(joined.contains("recent.error_count=1"));
        assert!(joined.contains("route.no_route"));
    }
}
