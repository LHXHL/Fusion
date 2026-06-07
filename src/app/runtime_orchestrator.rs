use std::{collections::HashMap, io::Error, sync::Arc};

use log::{info, warn};
use tokio::{sync::Mutex, task::JoinHandle, time::sleep};

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    app::{
        config::{AppConfig, TunnelEndpoint},
        conn_hub::{
            build_proxy_chain, order_endpoints, select_downstream_pool, select_upstream_pool,
        },
        runtime_http::{run_outbound_http_once, run_outbound_http_ws_once},
        runtime_mode::{
            collect_remote_port_forward_services, decide_inbound_runtime_mode,
            decide_outbound_runtime_mode, InboundRuntimeMode,
            OutboundRuntimeMode,
        },
        runtime_portfwd::run_outbound_port_forward_simplex_http_once,
        runtime_h2::{
            run_inbound_task_server_h2, run_outbound_relay_peer_h2, H2RelayStreamAllocator,
            H2TaskPeerMap,
        },
        runtime_peer::{
            run_inbound_task_server_simplex_dns, run_inbound_task_server_simplex_http,
            run_inbound_task_server_simplex_oss, run_inbound_task_server_tcp,
            run_inbound_task_server_ws, run_outbound_relay_peer_simplex_dns,
            run_outbound_relay_peer_simplex_http, run_outbound_relay_peer_simplex_oss,
            run_outbound_relay_peer_tcp, run_outbound_relay_peer_ws,
            SimplexDnsRelayStreamAllocator, SimplexDnsTaskPeerMap, SimplexHttpRelayStreamAllocator,
            SimplexHttpTaskPeerMap, SimplexOssRelayStreamAllocator, SimplexOssTaskPeerMap,
            TcpRelayStreamAllocator, TcpTaskPeerMap, WsRelayStreamAllocator, WsTaskPeerMap,
        },
        runtime_service::{
            run_inbound_raw_once, run_inbound_raw_h2_once, run_inbound_raw_simplex_dns_once,
            run_inbound_raw_simplex_once,
            run_inbound_raw_simplex_oss_once, run_inbound_raw_ws_once,
            run_remote_port_forward_listener,
        },
        runtime_shadowsocks::{run_outbound_shadowsocks_once, run_outbound_shadowsocks_ws_once},
        runtime_trojan::{run_outbound_trojan_once, run_outbound_trojan_ws_once},
        runtime_socks5::{
            run_outbound_socks5_once, run_outbound_socks5_simplex_http_once,
            run_outbound_socks5_ws_once,
        },
        runtime_status::{
            spawn_status_snapshot_task, RelayLinkMap, RuntimeConfigSummary, UpstreamPoolStatusMap,
        },
        runtime_task::run_outbound_task_once,
    },
    serve::{portfwd::PortForwardService, service::ServiceKind},
    session::{hub::SessionHub, reconnect::ReconnectState},
    tunnel::{
        dialer::{classify_endpoint, DialTarget},
        listener::{bind_endpoint, BoundListenerHandle},
        memory, simplex_dns, simplex_http, simplex_oss, streamhttp, tcp, udp, unix, ws,
    },
    utils::url::ParsedUrl,
};

#[derive(Clone)]
pub struct RuntimeShared {
    pub hub: Arc<Mutex<SessionHub>>,
    pub registry: Arc<Mutex<AgentRegistry>>,
    pub tcp_task_peers: TcpTaskPeerMap,
    pub ws_task_peers: WsTaskPeerMap,
    pub h2_task_peers: H2TaskPeerMap,
    pub simplex_dns_task_peers: SimplexDnsTaskPeerMap,
    pub simplex_http_task_peers: SimplexHttpTaskPeerMap,
    pub simplex_oss_task_peers: SimplexOssTaskPeerMap,
    pub tcp_relay_stream_allocator: TcpRelayStreamAllocator,
    pub ws_relay_stream_allocator: WsRelayStreamAllocator,
    pub h2_relay_stream_allocator: H2RelayStreamAllocator,
    pub simplex_dns_relay_stream_allocator: SimplexDnsRelayStreamAllocator,
    pub simplex_http_relay_stream_allocator: SimplexHttpRelayStreamAllocator,
    pub simplex_oss_relay_stream_allocator: SimplexOssRelayStreamAllocator,
    pub relay_links: RelayLinkMap,
    pub upstream_pools: UpstreamPoolStatusMap,
}

impl RuntimeShared {
    pub async fn new(
        exposed_service_labels: Vec<String>,
        data_dir: &std::path::Path,
        config: RuntimeConfigSummary,
    ) -> Self {
        let hub = Arc::new(Mutex::new(SessionHub::new()));
        let registry = Arc::new(Mutex::new(AgentRegistry::new()));
        let tcp_task_peers: TcpTaskPeerMap = Arc::new(Mutex::new(HashMap::new()));
        let ws_task_peers: WsTaskPeerMap = Arc::new(Mutex::new(HashMap::new()));
        let h2_task_peers: H2TaskPeerMap = Arc::new(Mutex::new(HashMap::new()));
        let simplex_dns_task_peers: SimplexDnsTaskPeerMap = Arc::new(Mutex::new(HashMap::new()));
        let simplex_http_task_peers: SimplexHttpTaskPeerMap = Arc::new(Mutex::new(HashMap::new()));
        let simplex_oss_task_peers: SimplexOssTaskPeerMap = Arc::new(Mutex::new(HashMap::new()));
        let tcp_relay_stream_allocator: TcpRelayStreamAllocator = Arc::new(Mutex::new(100_000));
        let ws_relay_stream_allocator: WsRelayStreamAllocator = Arc::new(Mutex::new(200_000));
        let h2_relay_stream_allocator: H2RelayStreamAllocator = Arc::new(Mutex::new(210_000));
        let simplex_dns_relay_stream_allocator: SimplexDnsRelayStreamAllocator =
            Arc::new(Mutex::new(250_000));
        let simplex_http_relay_stream_allocator: SimplexHttpRelayStreamAllocator =
            Arc::new(Mutex::new(300_000));
        let simplex_oss_relay_stream_allocator: SimplexOssRelayStreamAllocator =
            Arc::new(Mutex::new(400_000));
        let relay_links: RelayLinkMap = Arc::new(Mutex::new(HashMap::new()));
        let upstream_pools: UpstreamPoolStatusMap = Arc::new(Mutex::new(Vec::new()));

        registry
            .lock()
            .await
            .set_local_services(exposed_service_labels);

        spawn_status_snapshot_task(
            data_dir.to_path_buf(),
            hub.clone(),
            registry.clone(),
            relay_links.clone(),
            upstream_pools.clone(),
            config,
        );

        Self {
            hub,
            registry,
            tcp_task_peers,
            ws_task_peers,
            h2_task_peers,
            simplex_dns_task_peers,
            simplex_http_task_peers,
            simplex_oss_task_peers,
            tcp_relay_stream_allocator,
            ws_relay_stream_allocator,
            h2_relay_stream_allocator,
            simplex_dns_relay_stream_allocator,
            simplex_http_relay_stream_allocator,
            simplex_oss_relay_stream_allocator,
            relay_links,
            upstream_pools,
        }
    }
}

pub async fn spawn_remote_service_tasks(
    remote_port_forward_services: &[PortForwardService],
) -> Result<Vec<JoinHandle<()>>, Error> {
    let mut tasks = Vec::new();
    for service in remote_port_forward_services {
        let listener = service.bind_listener().await?;
        let local_addr = listener.local_addr()?;
        println!(
            "service.remote.active=port://{}->{}",
            local_addr,
            service.target_label()
        );
        info!(
            "service.remote.active=port://{}->{}",
            local_addr,
            service.target_label()
        );
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(err) = run_remote_port_forward_listener(listener, service).await {
                eprintln!("service.remote.port.error={err}");
            }
        }));
    }
    Ok(tasks)
}

pub async fn spawn_inbound_tasks(
    config: &AppConfig,
    identity: &AgentIdentity,
    shared: &RuntimeShared,
    inbound_raw_service: Option<crate::serve::service::ServiceDefinition>,
    local_services_present: bool,
    exposed_service_labels: &[String],
) -> Vec<JoinHandle<()>> {
    let mut tasks = Vec::new();
    for endpoint in &config.listens {
        let bound = match bind_endpoint(endpoint).await {
            Ok(bound) => bound,
            Err(err) => {
                warn!("listen endpoint {} skipped: {}", endpoint.url.original, err);
                continue;
            }
        };
        match decide_inbound_runtime_mode(
            bound.transport,
            inbound_raw_service.is_some(),
            local_services_present,
        ) {
            InboundRuntimeMode::RawTcp => {
                let BoundListenerHandle::Tcp(listener) = bound.handle else {
                    eprintln!("session.inbound.error=expected tcp listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let raw_service = inbound_raw_service.clone().unwrap();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_raw_once(
                        listener_identity,
                        listener,
                        raw_service,
                        shared.hub,
                        shared.registry,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::TaskTcp => {
                let BoundListenerHandle::Tcp(listener) = bound.handle else {
                    eprintln!("session.inbound.error=expected tcp listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let listener_services = exposed_service_labels.to_vec();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_task_server_tcp(
                        listener_identity,
                        listener,
                        listener_services,
                        shared.hub,
                        shared.registry,
                        shared.tcp_task_peers,
                        shared.tcp_relay_stream_allocator,
                        shared.relay_links,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::RawWs => {
                let BoundListenerHandle::Tcp(listener) = bound.handle else {
                    eprintln!("session.inbound.error=expected ws listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let raw_service = inbound_raw_service.clone().unwrap();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_raw_ws_once(
                        listener_identity,
                        listener,
                        bound.ws_tls_acceptor,
                        raw_service,
                        shared.hub,
                        shared.registry,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::TaskWs => {
                let BoundListenerHandle::Tcp(listener) = bound.handle else {
                    eprintln!("session.inbound.error=expected ws listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let listener_services = exposed_service_labels.to_vec();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_task_server_ws(
                        listener_identity,
                        listener,
                        bound.ws_tls_acceptor,
                        listener_services,
                        shared.hub,
                        shared.registry,
                        shared.ws_task_peers,
                        shared.ws_relay_stream_allocator,
                        shared.relay_links,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::RawH2 => {
                let BoundListenerHandle::Tcp(listener) = bound.handle else {
                    eprintln!("session.inbound.error=expected h2 listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let raw_service = inbound_raw_service.clone().unwrap();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_raw_h2_once(
                        listener_identity,
                        listener,
                        bound.ws_tls_acceptor,
                        raw_service,
                        shared.hub,
                        shared.registry,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::TaskH2 => {
                let BoundListenerHandle::Tcp(listener) = bound.handle else {
                    eprintln!("session.inbound.error=expected h2 listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let listener_services = exposed_service_labels.to_vec();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_task_server_h2(
                        listener_identity,
                        listener,
                        bound.ws_tls_acceptor,
                        listener_services,
                        shared.hub,
                        shared.registry,
                        shared.h2_task_peers,
                        shared.h2_relay_stream_allocator,
                        shared.relay_links,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::DirectUdp => {
                let BoundListenerHandle::Udp(socket) = bound.handle else {
                    eprintln!("session.inbound.error=expected udp listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    match udp::run_inbound_session_once(listener_identity, socket).await {
                        Ok((session, _, _)) => {
                            shared.hub.lock().await.upsert(session.clone());
                            shared.registry.lock().await.upsert_peer(session);
                        }
                        Err(err) => eprintln!("session.inbound.error={err}"),
                    }
                }));
            }
            InboundRuntimeMode::RawSimplexDns => {
                let display_url = bound.display_url.clone();
                let path = match ParsedUrl::parse(&display_url) {
                    Ok(parsed) => parsed.path,
                    Err(err) => {
                        eprintln!("session.inbound.error=invalid simplex dns listener url: {err}");
                        continue;
                    }
                };
                let BoundListenerHandle::Udp(socket) = bound.handle else {
                    eprintln!("session.inbound.error=expected simplex dns listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let raw_service = inbound_raw_service.clone().unwrap();
                println!("listen.active={}", display_url);
                info!("listen.active={}", display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_raw_simplex_dns_once(
                        listener_identity,
                        socket,
                        &path,
                        raw_service,
                        shared.hub,
                        shared.registry,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::TaskSimplexDns => {
                let display_url = bound.display_url.clone();
                let path = match ParsedUrl::parse(&display_url) {
                    Ok(parsed) => parsed.path,
                    Err(err) => {
                        eprintln!("session.inbound.error=invalid simplex dns listener url: {err}");
                        continue;
                    }
                };
                let BoundListenerHandle::Udp(socket) = bound.handle else {
                    eprintln!("session.inbound.error=expected simplex dns listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let listener_services = exposed_service_labels.to_vec();
                println!("listen.active={}", display_url);
                info!("listen.active={}", display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_task_server_simplex_dns(
                        listener_identity,
                        socket,
                        &path,
                        listener_services,
                        shared.hub,
                        shared.registry,
                        shared.simplex_dns_task_peers,
                        shared.simplex_dns_relay_stream_allocator,
                        shared.relay_links,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::RawSimplexHttp => {
                let display_url = bound.display_url.clone();
                let path = match ParsedUrl::parse(&display_url) {
                    Ok(parsed) => parsed.path,
                    Err(err) => {
                        eprintln!("session.inbound.error=invalid simplex http listener url: {err}");
                        continue;
                    }
                };
                let BoundListenerHandle::Tcp(listener) = bound.handle else {
                    eprintln!("session.inbound.error=expected simplex http listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let raw_service = inbound_raw_service.clone().unwrap();
                println!("listen.active={}", display_url);
                info!("listen.active={}", display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_raw_simplex_once(
                        listener_identity,
                        listener,
                        &path,
                        raw_service,
                        shared.hub,
                        shared.registry,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::TaskSimplexHttp => {
                let display_url = bound.display_url.clone();
                let path = match ParsedUrl::parse(&display_url) {
                    Ok(parsed) => parsed.path,
                    Err(err) => {
                        eprintln!("session.inbound.error=invalid simplex http listener url: {err}");
                        continue;
                    }
                };
                let BoundListenerHandle::Tcp(listener) = bound.handle else {
                    eprintln!("session.inbound.error=expected simplex http listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let listener_services = exposed_service_labels.to_vec();
                println!("listen.active={}", display_url);
                info!("listen.active={}", display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_task_server_simplex_http(
                        listener_identity,
                        listener,
                        &path,
                        listener_services,
                        shared.hub,
                        shared.registry,
                        shared.simplex_http_task_peers,
                        shared.simplex_http_relay_stream_allocator,
                        shared.relay_links,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::TaskStreamHttp => {
                let display_url = bound.display_url.clone();
                let path = match ParsedUrl::parse(&display_url) {
                    Ok(parsed) => parsed.path,
                    Err(err) => {
                        eprintln!("session.inbound.error=invalid streamhttp listener url: {err}");
                        continue;
                    }
                };
                let BoundListenerHandle::Tcp(listener) = bound.handle else {
                    eprintln!("session.inbound.error=expected streamhttp listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                println!("listen.active={}", display_url);
                info!("listen.active={}", display_url);
                tasks.push(tokio::spawn(async move {
                    match streamhttp::accept_peer_on(listener_identity, listener, &path).await {
                        Ok(peer) => {
                            shared.hub.lock().await.upsert(peer.session.clone());
                            shared.registry.lock().await.upsert_peer(peer.session);
                        }
                        Err(err) => eprintln!("session.inbound.error={err}"),
                    }
                }));
            }
            InboundRuntimeMode::TaskSimplexOss => {
                let BoundListenerHandle::SimplexOss(endpoint) = bound.handle else {
                    eprintln!("session.inbound.error=expected simplex oss listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let listener_services = exposed_service_labels.to_vec();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_task_server_simplex_oss(
                        listener_identity,
                        &endpoint,
                        listener_services,
                        shared.hub,
                        shared.registry,
                        shared.simplex_oss_task_peers,
                        shared.simplex_oss_relay_stream_allocator,
                        shared.relay_links,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::RawSimplexOss => {
                let BoundListenerHandle::SimplexOss(endpoint) = bound.handle else {
                    eprintln!("session.inbound.error=expected simplex oss listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let raw_service = inbound_raw_service.clone().unwrap();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_raw_simplex_oss_once(
                        listener_identity,
                        &endpoint,
                        raw_service,
                        shared.hub,
                        shared.registry,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::DirectSimplexHttp => {}
            InboundRuntimeMode::DirectSimplexOss => {
                let BoundListenerHandle::SimplexOss(endpoint) = bound.handle else {
                    eprintln!("session.inbound.error=expected simplex oss listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    match simplex_oss::run_inbound_session_once(listener_identity, &endpoint).await
                    {
                        Ok((session, _)) => {
                            shared.hub.lock().await.upsert(session.clone());
                            shared.registry.lock().await.upsert_peer(session);
                        }
                        Err(err) => eprintln!("session.inbound.error={err}"),
                    }
                }));
            }
            InboundRuntimeMode::DirectIcmp => {
                let BoundListenerHandle::Udp(socket) = bound.handle else {
                    eprintln!("session.inbound.error=expected icmp listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    match udp::run_inbound_session_once(listener_identity, socket).await {
                        Ok((session, _, _)) => {
                            shared.hub.lock().await.upsert(session.clone());
                            shared.registry.lock().await.upsert_peer(session);
                        }
                        Err(err) => eprintln!("session.inbound.error={err}"),
                    }
                }));
            }
            InboundRuntimeMode::DirectWg => {
                let BoundListenerHandle::Udp(socket) = bound.handle else {
                    eprintln!("session.inbound.error=expected wg listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    match udp::run_inbound_session_once(listener_identity, socket).await {
                        Ok((session, _, _)) => {
                            shared.hub.lock().await.upsert(session.clone());
                            shared.registry.lock().await.upsert_peer(session);
                        }
                        Err(err) => eprintln!("session.inbound.error={err}"),
                    }
                }));
            }
            InboundRuntimeMode::DirectUnix => {
                let BoundListenerHandle::Unix(listener) = bound.handle else {
                    eprintln!("session.inbound.error=expected unix listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let display_url = bound.display_url.clone();
                println!("listen.active={}", display_url);
                info!("listen.active={}", display_url);
                tasks.push(tokio::spawn(async move {
                    match unix::run_inbound_session_once(
                        listener_identity,
                        listener,
                        &display_url[7..],
                    )
                    .await
                    {
                        Ok((session, _)) => {
                            shared.hub.lock().await.upsert(session.clone());
                            shared.registry.lock().await.upsert_peer(session);
                        }
                        Err(err) => eprintln!("session.inbound.error={err}"),
                    }
                }));
            }
            InboundRuntimeMode::DirectMemory => {
                let BoundListenerHandle::Memory(listener) = bound.handle else {
                    eprintln!("session.inbound.error=expected memory listener handle");
                    continue;
                };
                let listener_identity = identity.clone();
                let shared = shared.clone();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    match memory::run_inbound_session_once(listener_identity, listener).await {
                        Ok((session, _)) => {
                            shared.hub.lock().await.upsert(session.clone());
                            shared.registry.lock().await.upsert_peer(session);
                        }
                        Err(err) => eprintln!("session.inbound.error={err}"),
                    }
                }));
            }
        }
    }
    tasks
}

pub fn spawn_outbound_tasks(
    config: &AppConfig,
    identity: &AgentIdentity,
    shared: &RuntimeShared,
    outbound_socks5_service: Option<crate::serve::service::ServiceDefinition>,
    outbound_http_proxy_service: Option<crate::serve::service::ServiceDefinition>,
    outbound_shadowsocks_service: Option<crate::serve::service::ServiceDefinition>,
    outbound_trojan_service: Option<crate::serve::service::ServiceDefinition>,
    outbound_egress_service: Option<crate::serve::service::ServiceDefinition>,
    remote_port_forward_services: &[PortForwardService],
    exposed_service_labels: &[String],
) -> Vec<JoinHandle<()>> {
    let mut tasks = Vec::new();
    let upstream_pool = select_upstream_pool(&config.connects, &config.up_connects);
    let downstream_pool = select_downstream_pool(&config.connects, &config.down_connects);
    let proxy_chain = build_proxy_chain(config.front_proxy.as_deref(), &config.proxy_chain);

    if let Some(task_request) = config.task_request.clone() {
        let connect_identity = identity.clone();
        let retry = config.retry.clone();
        let shared = shared.clone();
        let data_dir = config.data_dir.clone();
        let exposed_service_labels = exposed_service_labels.to_vec();
        let conn_policy = config.conn_policy.clone();
        let endpoints = upstream_pool.clone();
        let proxy_chain = proxy_chain.clone();
        tasks.push(tokio::spawn(async move {
            let mut state = ReconnectState::new(&retry);
            loop {
                let result = run_outbound_task_once(
                    connect_identity.clone(),
                    &endpoints,
                    task_request.clone(),
                    data_dir.clone(),
                    exposed_service_labels.clone(),
                    shared.hub.clone(),
                    shared.registry.clone(),
                    conn_policy.clone(),
                    proxy_chain.clone(),
                )
                .await;
                match result {
                    Ok(()) => break,
                    Err(err) => {
                        eprintln!("session.outbound.error={} target_pool=task", err);
                        state.fail_and_schedule(&retry);
                        if state.exhausted {
                            eprintln!("session.outbound.exhausted target_pool=task");
                            break;
                        }
                        sleep(state.next_delay).await;
                    }
                }
            }
        }));
        return tasks;
    }

    for endpoint in &downstream_pool {
        let connect_identity = identity.clone();
        let connect_endpoint = endpoint.clone();
        let retry = config.retry.clone();
        let shared = shared.clone();
        let local_socks = outbound_socks5_service.clone();
        let local_http_proxy = outbound_http_proxy_service.clone();
        let local_shadowsocks = outbound_shadowsocks_service.clone();
        let local_trojan = outbound_trojan_service.clone();
        let remote_egress = outbound_egress_service.clone();
        let port_forward = remote_port_forward_services.first().cloned();
        let has_remote_port_forward = port_forward.is_some();
        let exposed_service_labels = exposed_service_labels.to_vec();
        let has_listener = !config.listens.is_empty();
        let remote_peer_id = config.remote_peer_id.clone();
        let conn_policy = config.conn_policy.clone();
        let upstream_pool = upstream_pool.clone();
        let proxy_chain = proxy_chain.clone();
        tasks.push(tokio::spawn(async move {
            let mut state = ReconnectState::new(&retry);
            loop {
                let result = match decide_outbound_runtime_mode(
                    &connect_endpoint,
                    None,
                    local_socks.is_some(),
                    local_http_proxy.is_some(),
                    local_shadowsocks.is_some(),
                    local_trojan.is_some(),
                    remote_egress.is_some(),
                    has_remote_port_forward,
                    has_listener,
                ) {
                    OutboundRuntimeMode::Task => unreachable!(),
                    OutboundRuntimeMode::Socks5Tcp => {
                        run_outbound_socks5_once(
                            connect_identity.clone(),
                            &upstream_pool,
                            local_socks.clone().unwrap(),
                            remote_egress.clone().unwrap(),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            conn_policy.clone(),
                            proxy_chain.clone(),
                            shared.upstream_pools.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::Socks5Ws => {
                        run_outbound_socks5_ws_once(
                            connect_identity.clone(),
                            &upstream_pool,
                            local_socks.clone().unwrap(),
                            remote_egress.clone().unwrap(),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            conn_policy.clone(),
                            shared.upstream_pools.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::Socks5SimplexHttp => {
                        run_outbound_socks5_simplex_http_once(
                            connect_identity.clone(),
                            &upstream_pool,
                            local_socks.clone().unwrap(),
                            remote_egress.clone().unwrap(),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            conn_policy.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::PortForwardSimplexHttp => {
                        run_outbound_port_forward_simplex_http_once(
                            connect_identity.clone(),
                            &upstream_pool,
                            port_forward.clone().expect("port forward service"),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            conn_policy.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::HttpProxyTcp => {
                        run_outbound_http_once(
                            connect_identity.clone(),
                            &upstream_pool,
                            local_http_proxy.clone().unwrap(),
                            remote_egress.clone().unwrap(),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            conn_policy.clone(),
                            proxy_chain.clone(),
                            shared.upstream_pools.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::HttpProxyWs => {
                        run_outbound_http_ws_once(
                            connect_identity.clone(),
                            &upstream_pool,
                            local_http_proxy.clone().unwrap(),
                            remote_egress.clone().unwrap(),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            conn_policy.clone(),
                            shared.upstream_pools.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::ShadowsocksTcp => {
                        run_outbound_shadowsocks_once(
                            connect_identity.clone(),
                            &upstream_pool,
                            local_shadowsocks.clone().unwrap(),
                            remote_egress.clone().unwrap(),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            conn_policy.clone(),
                            proxy_chain.clone(),
                            shared.upstream_pools.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::ShadowsocksWs => {
                        run_outbound_shadowsocks_ws_once(
                            connect_identity.clone(),
                            &upstream_pool,
                            local_shadowsocks.clone().unwrap(),
                            remote_egress.clone().unwrap(),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            conn_policy.clone(),
                            shared.upstream_pools.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::TrojanTcp => {
                        run_outbound_trojan_once(
                            connect_identity.clone(),
                            &upstream_pool,
                            local_trojan.clone().unwrap(),
                            remote_egress.clone().unwrap(),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            conn_policy.clone(),
                            proxy_chain.clone(),
                            shared.upstream_pools.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::TrojanWs => {
                        run_outbound_trojan_ws_once(
                            connect_identity.clone(),
                            &upstream_pool,
                            local_trojan.clone().unwrap(),
                            remote_egress.clone().unwrap(),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            conn_policy.clone(),
                            shared.upstream_pools.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::RelayTcp => {
                        run_outbound_relay_peer_tcp(
                            connect_identity.clone(),
                            &connect_endpoint,
                            exposed_service_labels.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            shared.tcp_task_peers.clone(),
                            shared.tcp_relay_stream_allocator.clone(),
                            shared.relay_links.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::RelayWs => {
                        run_outbound_relay_peer_ws(
                            connect_identity.clone(),
                            &connect_endpoint,
                            exposed_service_labels.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            shared.ws_task_peers.clone(),
                            shared.ws_relay_stream_allocator.clone(),
                            shared.relay_links.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::RelayH2 => {
                        run_outbound_relay_peer_h2(
                            connect_identity.clone(),
                            &connect_endpoint,
                            exposed_service_labels.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            shared.h2_task_peers.clone(),
                            shared.h2_relay_stream_allocator.clone(),
                            shared.relay_links.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::RelaySimplexDns => {
                        run_outbound_relay_peer_simplex_dns(
                            connect_identity.clone(),
                            &connect_endpoint,
                            exposed_service_labels.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            shared.simplex_dns_task_peers.clone(),
                            shared.simplex_dns_relay_stream_allocator.clone(),
                            shared.relay_links.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::RelaySimplexHttp => {
                        run_outbound_relay_peer_simplex_http(
                            connect_identity.clone(),
                            &connect_endpoint,
                            exposed_service_labels.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            shared.simplex_http_task_peers.clone(),
                            shared.simplex_http_relay_stream_allocator.clone(),
                            shared.relay_links.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::RelaySimplexOss => {
                        run_outbound_relay_peer_simplex_oss(
                            connect_identity.clone(),
                            &connect_endpoint,
                            exposed_service_labels.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                            shared.simplex_oss_task_peers.clone(),
                            shared.simplex_oss_relay_stream_allocator.clone(),
                            shared.relay_links.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::Direct => {
                        run_outbound_once(
                            connect_identity.clone(),
                            &upstream_pool,
                            shared.hub.clone(),
                            shared.registry.clone(),
                            conn_policy.clone(),
                            proxy_chain.clone(),
                        )
                        .await
                    }
                };
                match result {
                    Ok(()) => break,
                    Err(err) => {
                        eprintln!(
                            "session.outbound.error={} target={}",
                            err, connect_endpoint.url.original
                        );
                        state.fail_and_schedule(&retry);
                        if state.exhausted {
                            eprintln!(
                                "session.outbound.exhausted target={}",
                                connect_endpoint.url.original
                            );
                            break;
                        }
                        sleep(state.next_delay).await;
                    }
                }
            }
        }));
    }
    tasks
}

pub async fn print_runtime_summary(shared: &RuntimeShared) {
    let summary = shared.hub.lock().await.summary_lines();
    for line in summary {
        println!("{line}");
    }
    let registry_summary = shared.registry.lock().await.summary_lines();
    for line in registry_summary {
        println!("{line}");
    }
}

async fn run_outbound_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: crate::app::config::ConnPolicy,
    proxy_chain: Vec<String>,
) -> Result<(), Error> {
    let ordered = order_endpoints(endpoints, &conn_policy)?;
    let mut last_err = None;
    for endpoint in ordered {
        let result: Result<(), Error> = async {
            match classify_endpoint(&endpoint)? {
                DialTarget::Tcp { addr } => {
                    let (session, _) = tcp::run_outbound_session_once_via_proxy_chain(
                        identity.clone(),
                        &addr,
                        &proxy_chain,
                    )
                    .await?;
                    hub.lock().await.upsert(session.clone());
                    registry.lock().await.upsert_peer(session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=tcp",
                        session.remote.agent_id, addr
                    );
                }
                DialTarget::Ws { url } => {
                    let (session, _) =
                        ws::run_outbound_session_once(identity.clone(), &url).await?;
                    hub.lock().await.upsert(session.clone());
                    registry.lock().await.upsert_peer(session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=ws",
                        session.remote.agent_id, url
                    );
                }
                DialTarget::H2 { url } => {
                    let peer =
                        crate::tunnel::h2_mux::connect_mux_peer(identity.clone(), &url).await?;
                    hub.lock().await.upsert(peer.session.clone());
                    registry.lock().await.upsert_peer(peer.session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=h2",
                        peer.session.remote.agent_id, url
                    );
                }
                DialTarget::Udp { addr } => {
                    let (session, _) =
                        udp::run_outbound_session_once(identity.clone(), &addr).await?;
                    hub.lock().await.upsert(session.clone());
                    registry.lock().await.upsert_peer(session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=udp",
                        session.remote.agent_id, addr
                    );
                }
                DialTarget::SimplexDns { url } => {
                    let (session, _) =
                        simplex_dns::run_outbound_session_once(identity.clone(), &url).await?;
                    hub.lock().await.upsert(session.clone());
                    registry.lock().await.upsert_peer(session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=simplex-dns",
                        session.remote.agent_id, url
                    );
                }
                DialTarget::SimplexHttp { url } => {
                    let (session, _) =
                        simplex_http::run_outbound_session_once(identity.clone(), &url).await?;
                    hub.lock().await.upsert(session.clone());
                    registry.lock().await.upsert_peer(session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=http-long-poll",
                        session.remote.agent_id, url
                    );
                }
                DialTarget::StreamHttp { url } => {
                    let (session, _) =
                        streamhttp::run_outbound_session_once(identity.clone(), &url).await?;
                    hub.lock().await.upsert(session.clone());
                    registry.lock().await.upsert_peer(session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=streamhttp",
                        session.remote.agent_id, url
                    );
                }
                DialTarget::SimplexOss { url } => {
                    let (session, _) =
                        simplex_oss::run_outbound_session_once(identity.clone(), &url).await?;
                    hub.lock().await.upsert(session.clone());
                    registry.lock().await.upsert_peer(session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=simplex-oss",
                        session.remote.agent_id, url
                    );
                }
                DialTarget::Icmp { addr } => {
                    let (session, _) =
                        udp::run_outbound_session_once(identity.clone(), &addr).await?;
                    hub.lock().await.upsert(session.clone());
                    registry.lock().await.upsert_peer(session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=icmp",
                        session.remote.agent_id, addr
                    );
                }
                DialTarget::Wg { addr } => {
                    let (session, _) =
                        udp::run_outbound_session_once(identity.clone(), &addr).await?;
                    hub.lock().await.upsert(session.clone());
                    registry.lock().await.upsert_peer(session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=wg",
                        session.remote.agent_id, addr
                    );
                }
                DialTarget::Unix { path } => {
                    let (session, _) =
                        unix::run_outbound_session_once(identity.clone(), &path).await?;
                    hub.lock().await.upsert(session.clone());
                    registry.lock().await.upsert_peer(session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=unix",
                        session.remote.agent_id, path
                    );
                }
                DialTarget::Memory { name } => {
                    let (session, _) =
                        memory::run_outbound_session_once(identity.clone(), &name).await?;
                    hub.lock().await.upsert(session.clone());
                    registry.lock().await.upsert_peer(session.clone());
                    println!(
                        "session.outbound.peer={} to={} via=memory",
                        session.remote.agent_id, name
                    );
                }
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => return Ok(()),
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err.unwrap_or_else(|| Error::other("no outbound endpoint succeeded")))
}

pub fn find_runtime_services(
    local_services: &[crate::serve::service::ServiceDefinition],
    remote_services: &[crate::serve::service::ServiceDefinition],
) -> (
    Option<crate::serve::service::ServiceDefinition>,
    Option<crate::serve::service::ServiceDefinition>,
    Option<crate::serve::service::ServiceDefinition>,
    Option<crate::serve::service::ServiceDefinition>,
    Option<crate::serve::service::ServiceDefinition>,
    Option<crate::serve::service::ServiceDefinition>,
    Vec<PortForwardService>,
) {
    let inbound_raw_service = remote_services
        .iter()
        .find(|svc| matches!(svc.kind, ServiceKind::RemoteRaw(_)))
        .cloned();
    let outbound_socks5_service = local_services
        .iter()
        .find(|svc| matches!(svc.kind, ServiceKind::LocalSocks5(_)))
        .cloned();
    let outbound_http_proxy_service = local_services
        .iter()
        .find(|svc| matches!(svc.kind, ServiceKind::LocalHttpProxy(_)))
        .cloned();
    let outbound_shadowsocks_service = local_services
        .iter()
        .find(|svc| matches!(svc.kind, ServiceKind::LocalShadowsocks(_)))
        .cloned();
    let outbound_trojan_service = local_services
        .iter()
        .find(|svc| matches!(svc.kind, ServiceKind::LocalTrojan(_)))
        .cloned();
    let outbound_egress_service = remote_services
        .iter()
        .find(|svc| {
            matches!(
                svc.kind,
                ServiceKind::RemoteRaw(_) | ServiceKind::RemotePortForward(_)
            )
        })
        .cloned();
    let remote_port_forward_services = collect_remote_port_forward_services(remote_services);

    (
        inbound_raw_service,
        outbound_socks5_service,
        outbound_http_proxy_service,
        outbound_shadowsocks_service,
        outbound_trojan_service,
        outbound_egress_service,
        remote_port_forward_services,
    )
}
