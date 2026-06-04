use std::{collections::HashMap, io::Error, sync::Arc};

use log::{info, warn};
use tokio::{sync::Mutex, task::JoinHandle, time::sleep};

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    app::{
        config::{AppConfig, TunnelEndpoint},
        runtime_mode::{
            collect_remote_port_forward_services, decide_inbound_runtime_mode,
            decide_outbound_runtime_mode, InboundRuntimeMode, OutboundRuntimeMode,
        },
        runtime_peer::{
            run_inbound_task_server_tcp, run_inbound_task_server_ws, run_outbound_relay_peer_tcp,
            run_outbound_relay_peer_ws, TcpRelayStreamAllocator, TcpTaskPeerMap,
            WsRelayStreamAllocator, WsTaskPeerMap,
        },
        runtime_service::{
            run_inbound_raw_once, run_inbound_raw_ws_once, run_remote_port_forward_listener,
        },
        runtime_socks5::{run_outbound_socks5_once, run_outbound_socks5_ws_once},
        runtime_status::{spawn_status_snapshot_task, RelayLinkMap},
        runtime_task::run_outbound_task_once,
    },
    serve::{portfwd::PortForwardService, service::ServiceKind},
    session::{hub::SessionHub, reconnect::ReconnectState},
    tunnel::{
        dialer::{classify_endpoint, DialTarget},
        listener::bind_endpoint,
        tcp, ws,
    },
};

#[derive(Clone)]
pub struct RuntimeShared {
    pub hub: Arc<Mutex<SessionHub>>,
    pub registry: Arc<Mutex<AgentRegistry>>,
    pub tcp_task_peers: TcpTaskPeerMap,
    pub ws_task_peers: WsTaskPeerMap,
    pub tcp_relay_stream_allocator: TcpRelayStreamAllocator,
    pub ws_relay_stream_allocator: WsRelayStreamAllocator,
    pub relay_links: RelayLinkMap,
}

impl RuntimeShared {
    pub async fn new(exposed_service_labels: Vec<String>, data_dir: &std::path::Path) -> Self {
        let hub = Arc::new(Mutex::new(SessionHub::new()));
        let registry = Arc::new(Mutex::new(AgentRegistry::new()));
        let tcp_task_peers: TcpTaskPeerMap = Arc::new(Mutex::new(HashMap::new()));
        let ws_task_peers: WsTaskPeerMap = Arc::new(Mutex::new(HashMap::new()));
        let tcp_relay_stream_allocator: TcpRelayStreamAllocator = Arc::new(Mutex::new(100_000));
        let ws_relay_stream_allocator: WsRelayStreamAllocator = Arc::new(Mutex::new(200_000));
        let relay_links: RelayLinkMap = Arc::new(Mutex::new(HashMap::new()));

        registry
            .lock()
            .await
            .set_local_services(exposed_service_labels);

        spawn_status_snapshot_task(
            data_dir.to_path_buf(),
            hub.clone(),
            registry.clone(),
            relay_links.clone(),
        );

        Self {
            hub,
            registry,
            tcp_task_peers,
            ws_task_peers,
            tcp_relay_stream_allocator,
            ws_relay_stream_allocator,
            relay_links,
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
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let raw_service = inbound_raw_service.clone().unwrap();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_raw_once(
                        listener_identity,
                        bound.listener,
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
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let listener_services = exposed_service_labels.to_vec();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_task_server_tcp(
                        listener_identity,
                        bound.listener,
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
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let raw_service = inbound_raw_service.clone().unwrap();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_raw_ws_once(
                        listener_identity,
                        bound.listener,
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
                let listener_identity = identity.clone();
                let shared = shared.clone();
                let listener_services = exposed_service_labels.to_vec();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_task_server_ws(
                        listener_identity,
                        bound.listener,
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
        }
    }
    tasks
}

pub fn spawn_outbound_tasks(
    config: &AppConfig,
    identity: &AgentIdentity,
    shared: &RuntimeShared,
    outbound_socks5_service: Option<crate::serve::service::ServiceDefinition>,
    outbound_egress_service: Option<crate::serve::service::ServiceDefinition>,
    exposed_service_labels: &[String],
) -> Vec<JoinHandle<()>> {
    let mut tasks = Vec::new();
    for endpoint in &config.connects {
        let connect_identity = identity.clone();
        let connect_endpoint = endpoint.clone();
        let retry = config.retry.clone();
        let shared = shared.clone();
        let task_request = config.task_request.clone();
        let data_dir = config.data_dir.clone();
        let local_socks = outbound_socks5_service.clone();
        let remote_egress = outbound_egress_service.clone();
        let exposed_service_labels = exposed_service_labels.to_vec();
        let has_listener = !config.listens.is_empty();
        let remote_peer_id = config.remote_peer_id.clone();
        tasks.push(tokio::spawn(async move {
            let mut state = ReconnectState::new(&retry);
            loop {
                let result = match decide_outbound_runtime_mode(
                    &connect_endpoint,
                    task_request.as_ref(),
                    local_socks.is_some(),
                    remote_egress.is_some(),
                    has_listener,
                ) {
                    OutboundRuntimeMode::Task => {
                        run_outbound_task_once(
                            connect_identity.clone(),
                            &connect_endpoint,
                            task_request.clone().unwrap(),
                            data_dir.clone(),
                            exposed_service_labels.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::Socks5Tcp => {
                        run_outbound_socks5_once(
                            connect_identity.clone(),
                            &connect_endpoint,
                            local_socks.clone().unwrap(),
                            remote_egress.clone().unwrap(),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
                        )
                        .await
                    }
                    OutboundRuntimeMode::Socks5Ws => {
                        run_outbound_socks5_ws_once(
                            connect_identity.clone(),
                            &connect_endpoint,
                            local_socks.clone().unwrap(),
                            remote_egress.clone().unwrap(),
                            remote_peer_id.clone(),
                            shared.hub.clone(),
                            shared.registry.clone(),
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
                    OutboundRuntimeMode::Direct => {
                        run_outbound_once(
                            connect_identity.clone(),
                            &connect_endpoint,
                            shared.hub.clone(),
                            shared.registry.clone(),
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
    endpoint: &TunnelEndpoint,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    match classify_endpoint(endpoint)? {
        DialTarget::Tcp { addr } => {
            let (session, _) = tcp::run_outbound_session_once(identity, &addr).await?;
            hub.lock().await.upsert(session.clone());
            registry.lock().await.upsert_peer(session.clone());
            println!(
                "session.outbound.peer={} to={} via=tcp",
                session.remote.agent_id, addr
            );
            Ok(())
        }
        DialTarget::Ws { url } => {
            let (session, _) = ws::run_outbound_session_once(identity, &url).await?;
            hub.lock().await.upsert(session.clone());
            registry.lock().await.upsert_peer(session.clone());
            println!(
                "session.outbound.peer={} to={} via=ws",
                session.remote.agent_id, url
            );
            Ok(())
        }
    }
}

pub fn find_runtime_services(
    local_services: &[crate::serve::service::ServiceDefinition],
    remote_services: &[crate::serve::service::ServiceDefinition],
) -> (
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
        outbound_egress_service,
        remote_port_forward_services,
    )
}
