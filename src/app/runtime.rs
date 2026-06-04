use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    path::PathBuf,
    sync::Arc,
};

use chrono::Utc;
use log::{info, warn};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinHandle,
    time::sleep,
};

use crate::{
    agent::{
        identity::AgentIdentity,
        registry::AgentRegistry,
    },
    app::{
        config::{AppConfig, TaskRequestConfig, TunnelEndpoint},
        runtime_mode::{
            collect_remote_port_forward_services, decide_inbound_runtime_mode,
            decide_outbound_runtime_mode, InboundRuntimeMode, OutboundRuntimeMode,
        },
        runtime_relay::{
            broadcast_control_frame_tcp, broadcast_control_frame_ws,
            handle_registry_control_message, handle_tcp_relay_stream_open,
            handle_ws_relay_stream_open, send_direct_announce_tcp_mux,
            send_direct_announce_ws_mux, send_route_snapshot_tcp_mux,
            send_route_snapshot_ws_mux, ROUTE_TTL_SECS,
        },
        runtime_status::{
            print_control_snapshot, print_status_snapshot, spawn_status_snapshot_task, RelayLinkMap,
        },
        runtime_task::maybe_store_task_artifact,
    },
    protocol::{
        codec::encode_frame,
        frame::{Frame, MessageType},
        message::{
            HelloMessage, Message, StreamCloseMessage, StreamDataMessage,
            TaskRequestMessage,
        },
    },
    serve::{
        portfwd::{proxy_connection as proxy_port_forward_connection, PortForwardService},
        raw::{proxy_mux_stream_loop, proxy_ws_mux_stream_loop},
        service::{
            build_local_services, build_remote_services, build_remote_stream_open_for_request,
            validate_service_pairing, ServiceDefinition, ServiceKind,
        },
        socks5::{accept_no_auth, read_connect_request, write_success_response},
    },
    session::{
        hub::SessionHub,
        reconnect::ReconnectState,
        router::{decide_frame_route, RouteDecision},
        stream::StreamIdAllocator,
    },
    task::dispatcher,
    tunnel::{
        dialer::{classify_endpoint, DialTarget},
        listener::bind_endpoint,
        tcp, tcp_mux, ws, ws_mux,
    },
};

type TcpTaskPeerMap = Arc<Mutex<HashMap<String, tcp_mux::MuxTcpPeer>>>;
type WsTaskPeerMap = Arc<Mutex<HashMap<String, ws_mux::MuxWsPeer>>>;
type TcpRelayStreamAllocator = Arc<Mutex<u32>>;
type WsRelayStreamAllocator = Arc<Mutex<u32>>;


pub async fn run(config: AppConfig) -> Result<(), Error> {
    if let Some(status) = &config.status_command {
        return print_status_snapshot(&config.data_dir, status.scope.clone(), status.json).await;
    }
    if let Some(control) = &config.control_command {
        return print_control_snapshot(&config.data_dir, control).await;
    }

    info!("starting fusion unified runtime");
    let identity = AgentIdentity::from_config(&config.identity);
    let hello = Frame::new(
        MessageType::Hello,
        Some(identity.id.clone()),
        None,
        Message::Hello(HelloMessage {
            agent_id: identity.id.clone(),
            agent_name: identity.name.clone(),
            capabilities: identity.capability_labels(),
            protocol_version: 1,
        }),
    );

    let hello_json = String::from_utf8(encode_frame(&hello).map_err(|e| {
        Error::new(
            ErrorKind::InvalidData,
            format!("failed to encode hello frame: {e}"),
        )
    })?)
    .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

    for line in config.summary_lines() {
        info!("{line}");
    }
    info!("agent.id={}", identity.id);
    info!("agent.hostname={}", identity.hostname);
    info!("bootstrap.hello={hello_json}");

    println!("Fusion unified runtime bootstrap complete.");
    for line in config.summary_lines() {
        println!("{line}");
    }
    println!("agent.id={}", identity.id);
    println!("agent.name={}", identity.name);
    println!("agent.hostname={}", identity.hostname);
    println!("agent.os={}", identity.os);
    println!("agent.arch={}", identity.arch);
    println!("bootstrap.timestamp={}", Utc::now().timestamp());
    println!("bootstrap.hello={hello_json}");

    let local_services = build_local_services(&config.local_serves)?;
    let remote_services = build_remote_services(&config.remote_serves)?;
    validate_service_pairing(&local_services, &remote_services)?;
    for svc in &local_services {
        println!("{}", svc.summary_line());
    }
    for svc in &remote_services {
        println!("{}", svc.summary_line());
    }

    let local_service_labels: Vec<String> = local_services
        .iter()
        .map(|svc| svc.summary_line())
        .collect();
    let remote_service_labels: Vec<String> = remote_services
        .iter()
        .map(|svc| svc.summary_line())
        .collect();
    let exposed_service_labels: Vec<String> = local_service_labels
        .iter()
        .chain(remote_service_labels.iter())
        .cloned()
        .collect();

    let has_port_forward_service = remote_services
        .iter()
        .any(|svc| matches!(svc.kind, ServiceKind::RemotePortForward(_)));
    let active_runtime = !config.listens.is_empty() || !config.connects.is_empty() || has_port_forward_service;
    if !active_runtime {
        return Ok(());
    }

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
        .set_local_services(exposed_service_labels.clone());
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();

    spawn_status_snapshot_task(
        config.data_dir.clone(),
        hub.clone(),
        registry.clone(),
        relay_links.clone(),
    );

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
        .find(|svc| matches!(svc.kind, ServiceKind::RemoteRaw(_) | ServiceKind::RemotePortForward(_)))
        .cloned();
    let remote_port_forward_services: Vec<PortForwardService> =
        collect_remote_port_forward_services(&remote_services);

    for service in &remote_port_forward_services {
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
            !local_services.is_empty(),
        ) {
            InboundRuntimeMode::RawTcp => {
                let listener_identity = identity.clone();
                let hub = hub.clone();
                let registry = registry.clone();
                let raw_service = inbound_raw_service.clone().unwrap();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_raw_once(
                        listener_identity,
                        bound.listener,
                        raw_service,
                        hub,
                        registry,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::TaskTcp => {
                let listener_identity = identity.clone();
                let hub = hub.clone();
                let registry = registry.clone();
                let listener_services = exposed_service_labels.clone();
                let peer_map = tcp_task_peers.clone();
                let relay_stream_allocator = tcp_relay_stream_allocator.clone();
                let relay_links = relay_links.clone();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_task_server_tcp(
                        listener_identity,
                        bound.listener,
                        listener_services,
                        hub,
                        registry,
                        peer_map,
                        relay_stream_allocator,
                        relay_links,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::RawWs => {
                let listener_identity = identity.clone();
                let hub = hub.clone();
                let registry = registry.clone();
                let raw_service = inbound_raw_service.clone().unwrap();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_raw_ws_once(
                        listener_identity,
                        bound.listener,
                        bound.ws_tls_acceptor,
                        raw_service,
                        hub,
                        registry,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
            InboundRuntimeMode::TaskWs => {
                let listener_identity = identity.clone();
                let hub = hub.clone();
                let registry = registry.clone();
                let listener_services = exposed_service_labels.clone();
                let peer_map = ws_task_peers.clone();
                let relay_stream_allocator = ws_relay_stream_allocator.clone();
                let relay_links = relay_links.clone();
                println!("listen.active={}", bound.display_url);
                info!("listen.active={}", bound.display_url);
                tasks.push(tokio::spawn(async move {
                    if let Err(err) = run_inbound_task_server_ws(
                        listener_identity,
                        bound.listener,
                        bound.ws_tls_acceptor,
                        listener_services,
                        hub,
                        registry,
                        peer_map,
                        relay_stream_allocator,
                        relay_links,
                    )
                    .await
                    {
                        eprintln!("session.inbound.error={err}");
                    }
                }));
            }
        }
    }

    for endpoint in &config.connects {
        let connect_identity = identity.clone();
        let connect_endpoint = endpoint.clone();
        let retry = config.retry.clone();
        let hub = hub.clone();
        let registry = registry.clone();
        let tcp_task_peers = tcp_task_peers.clone();
        let ws_task_peers = ws_task_peers.clone();
        let task_request = config.task_request.clone();
        let data_dir = config.data_dir.clone();
        let local_socks = outbound_socks5_service.clone();
        let remote_egress = outbound_egress_service.clone();
        let exposed_service_labels = exposed_service_labels.clone();
        let has_listener = !config.listens.is_empty();
        let remote_peer_id = config.remote_peer_id.clone();
        let tcp_relay_stream_allocator = tcp_relay_stream_allocator.clone();
        let ws_relay_stream_allocator = ws_relay_stream_allocator.clone();
        let relay_links = relay_links.clone();
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
                    OutboundRuntimeMode::Task => run_outbound_task_once(
                        connect_identity.clone(),
                        &connect_endpoint,
                        task_request.clone().unwrap(),
                        data_dir.clone(),
                        exposed_service_labels.clone(),
                        hub.clone(),
                        registry.clone(),
                    )
                    .await,
                    OutboundRuntimeMode::Socks5Tcp => run_outbound_socks5_once(
                        connect_identity.clone(),
                        &connect_endpoint,
                        local_socks.clone().unwrap(),
                        remote_egress.clone().unwrap(),
                        remote_peer_id.clone(),
                        hub.clone(),
                        registry.clone(),
                    )
                    .await,
                    OutboundRuntimeMode::Socks5Ws => run_outbound_socks5_ws_once(
                        connect_identity.clone(),
                        &connect_endpoint,
                        local_socks.clone().unwrap(),
                        remote_egress.clone().unwrap(),
                        remote_peer_id.clone(),
                        hub.clone(),
                        registry.clone(),
                    )
                    .await,
                    OutboundRuntimeMode::RelayTcp => run_outbound_relay_peer_tcp(
                        connect_identity.clone(),
                        &connect_endpoint,
                        exposed_service_labels.clone(),
                        hub.clone(),
                        registry.clone(),
                        tcp_task_peers.clone(),
                        tcp_relay_stream_allocator.clone(),
                        relay_links.clone(),
                    )
                    .await,
                    OutboundRuntimeMode::RelayWs => run_outbound_relay_peer_ws(
                        connect_identity.clone(),
                        &connect_endpoint,
                        exposed_service_labels.clone(),
                        hub.clone(),
                        registry.clone(),
                        ws_task_peers.clone(),
                        ws_relay_stream_allocator.clone(),
                        relay_links.clone(),
                    )
                    .await,
                    OutboundRuntimeMode::Direct => run_outbound_once(
                        connect_identity.clone(),
                        &connect_endpoint,
                        hub.clone(),
                        registry.clone(),
                    )
                    .await,
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

    for task in tasks {
        let _ = task.await;
    }

    let summary = hub.lock().await.summary_lines();
    for line in summary {
        println!("{line}");
    }
    let registry_summary = registry.lock().await.summary_lines();
    for line in registry_summary {
        println!("{line}");
    }

    Ok(())
}

async fn run_remote_port_forward_listener(
    listener: TcpListener,
    service: PortForwardService,
) -> Result<(), Error> {
    loop {
        let (client, client_addr) = listener.accept().await?;
        let service = service.clone();
        tokio::spawn(async move {
            if let Err(err) = proxy_port_forward_connection(client, service.clone()).await {
                eprintln!(
                    "service.remote.port.client.error={} listen={} target={} client={}",
                    err,
                    service.bind_label(),
                    service.target_label(),
                    client_addr
                );
            }
        });
    }
}

async fn run_inbound_task_server_tcp(
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

async fn handle_inbound_task_peer_tcp(
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

async fn run_outbound_relay_peer_tcp(
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

async fn run_outbound_relay_peer_ws(
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
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_ws(
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
                        format!("unexpected message on outbound ws relay peer: {:?}", other),
                    ))
                }
            },
            RouteDecision::Forward { next_hop_agent_id } => {
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

async fn run_outbound_task_once(
    identity: AgentIdentity,
    endpoint: &TunnelEndpoint,
    task_request: TaskRequestConfig,
    data_dir: PathBuf,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    match endpoint.url.scheme.as_str() {
        "tcp" => {
            let connect_host = endpoint.url.host.clone().ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing host for tcp connect")
            })?;
            let connect_port = endpoint.url.port.ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing port for tcp connect")
            })?;
            let connect_addr = format!("{}:{}", connect_host, connect_port);
            let peer = tcp_mux::connect_mux_peer(identity.clone(), &connect_addr).await?;
            hub.lock().await.upsert(peer.session.clone());
            registry.lock().await.upsert_peer(peer.session.clone());
            println!(
                "session.outbound.peer={} to={} via=tcp",
                peer.session.remote.agent_id, connect_addr
            );
            send_direct_announce_tcp_mux(&peer, &identity, &local_services).await?;

            let task_id = format!("task-{}", Utc::now().timestamp_millis());
            let request = TaskRequestMessage {
                task_id: task_id.clone(),
                action: task_request.action.clone(),
                args: task_request.args.clone(),
                data_hex: task_request.data_hex.clone(),
            };
            let frame = Frame::new(
                MessageType::TaskRequest,
                Some(peer.session.local.agent_id.clone()),
                Some(
                    task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone()),
                ),
                Message::TaskRequest(request),
            );
            peer.send_frame(&frame).await?;
            let response = loop {
                let response = peer.read_control_frame().await?;
                if handle_registry_control_message(
                    &mut *registry.lock().await,
                    &peer.session.remote.agent_id,
                    &response.message,
                ) {
                    continue;
                }
                break response;
            };
            match response.message {
                Message::TaskResult(result) => {
                    println!("task.result.id={}", result.task_id);
                    println!("task.result.ok={}", result.ok);
                    println!("task.result.output={}", result.output.trim_end());
                    if let Some(path) =
                        maybe_store_task_artifact(&data_dir, &task_request, &result).await?
                    {
                        println!("task.result.artifact={}", path.display());
                    }
                    if let Some(data_hex) = result.data_hex.as_ref() {
                        println!("task.result.data_hex_len={}", data_hex.len());
                    }
                    Ok(())
                }
                other => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("expected TaskResult, got {:?}", other),
                )),
            }
        }
        "ws" | "wss" => {
            let peer = ws_mux::connect_mux_peer(identity.clone(), &endpoint.url.original).await?;
            hub.lock().await.upsert(peer.session.clone());
            registry.lock().await.upsert_peer(peer.session.clone());
            println!(
                "session.outbound.peer={} to={} via=ws",
                peer.session.remote.agent_id, endpoint.url.original
            );
            send_direct_announce_ws_mux(&peer, &identity, &local_services).await?;

            let task_id = format!("task-{}", Utc::now().timestamp_millis());
            let request = TaskRequestMessage {
                task_id: task_id.clone(),
                action: task_request.action.clone(),
                args: task_request.args.clone(),
                data_hex: task_request.data_hex.clone(),
            };
            let frame = Frame::new(
                MessageType::TaskRequest,
                Some(peer.session.local.agent_id.clone()),
                Some(
                    task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone()),
                ),
                Message::TaskRequest(request),
            );
            peer.send_frame(&frame).await?;
            let response = loop {
                let response = peer.read_control_frame().await?;
                if handle_registry_control_message(
                    &mut *registry.lock().await,
                    &peer.session.remote.agent_id,
                    &response.message,
                ) {
                    continue;
                }
                break response;
            };
            match response.message {
                Message::TaskResult(result) => {
                    println!("task.result.id={}", result.task_id);
                    println!("task.result.ok={}", result.ok);
                    println!("task.result.output={}", result.output.trim_end());
                    if let Some(path) =
                        maybe_store_task_artifact(&data_dir, &task_request, &result).await?
                    {
                        println!("task.result.artifact={}", path.display());
                    }
                    if let Some(data_hex) = result.data_hex.as_ref() {
                        println!("task.result.data_hex_len={}", data_hex.len());
                    }
                    Ok(())
                }
                other => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("expected TaskResult, got {:?}", other),
                )),
            }
        }
        other => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("task request over scheme `{}` not implemented yet", other),
        )),
    }
}

async fn run_inbound_task_server_ws(
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

async fn handle_inbound_task_peer_ws(
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
                &peer.session.remote.agent_id,
                &frame.message,
            )
        };
        if handled_control {
            broadcast_control_frame_ws(
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

async fn run_inbound_raw_once(
    identity: AgentIdentity,
    listener: TcpListener,
    raw_service_definition: ServiceDefinition,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let peer = tcp_mux::accept_mux_peer(identity, listener).await?;
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    println!(
        "session.inbound.peer={} from={} via=tcp",
        peer.session.remote.agent_id, peer.peer_addr
    );

    let raw_service = match raw_service_definition.kind {
        ServiceKind::RemoteRaw(service) => service,
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "inbound raw handler requires remote raw service",
            ))
        }
    };

    loop {
        let (stream_id, open) = match peer.read_stream_open().await {
            Ok(value) => value,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };
        let target = match (&open.target_host, open.target_port) {
            (Some(host), Some(port)) => format!("{}:{}", host, port),
            _ => raw_service.target_label(),
        };
        registry.lock().await.open_stream(
            stream_id,
            peer.session.remote.agent_id.clone(),
            open.service.clone(),
            target,
        );
        registry.lock().await.mark_stream_active(stream_id);
        let stream_rx = peer.open_stream_receiver(stream_id).await;
        let peer_handle = peer.clone();
        let raw_service = raw_service.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(err) =
                proxy_mux_stream_loop(peer_handle, &raw_service, open, stream_id, stream_rx).await
            {
                registry
                    .lock()
                    .await
                    .mark_stream_failed(stream_id, err.to_string());
                eprintln!("stream.inbound.error={} stream_id={}", err, stream_id);
            } else {
                registry.lock().await.mark_stream_closed(stream_id);
            }
        });
    }

    Ok(())
}

async fn run_inbound_raw_ws_once(
    identity: AgentIdentity,
    listener: TcpListener,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    raw_service_definition: ServiceDefinition,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let peer = ws_mux::accept_mux_peer(identity, listener, tls_acceptor).await?;
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    println!(
        "session.inbound.peer={} from={} via=ws",
        peer.session.remote.agent_id, peer.peer_addr
    );

    let raw_service = match raw_service_definition.kind {
        ServiceKind::RemoteRaw(service) => service,
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "inbound raw handler requires remote raw service",
            ))
        }
    };

    loop {
        let (stream_id, open) = match peer.read_stream_open().await {
            Ok(value) => value,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        };
        let target = match (&open.target_host, open.target_port) {
            (Some(host), Some(port)) => format!("{}:{}", host, port),
            _ => raw_service.target_label(),
        };
        registry.lock().await.open_stream(
            stream_id,
            peer.session.remote.agent_id.clone(),
            open.service.clone(),
            target,
        );
        registry.lock().await.mark_stream_active(stream_id);
        let stream_rx = peer.open_stream_receiver(stream_id).await;
        let peer_handle = peer.clone();
        let raw_service = raw_service.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(err) =
                proxy_ws_mux_stream_loop(peer_handle, &raw_service, open, stream_id, stream_rx)
                    .await
            {
                registry
                    .lock()
                    .await
                    .mark_stream_failed(stream_id, err.to_string());
                eprintln!("stream.inbound.error={} stream_id={}", err, stream_id);
            } else {
                registry.lock().await.mark_stream_closed(stream_id);
            }
        });
    }

    Ok(())
}

async fn run_outbound_socks5_once(
    identity: AgentIdentity,
    endpoint: &TunnelEndpoint,
    local_socks_definition: ServiceDefinition,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
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
    let peer = tcp_mux::connect_mux_peer(identity, &connect_addr).await?;
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    println!(
        "session.outbound.peer={} to={} via=tcp",
        peer.session.remote.agent_id, connect_addr
    );

    let socks_service = match local_socks_definition.kind {
        ServiceKind::LocalSocks5(service) => service,
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "outbound socks5 handler requires local socks5 service",
            ))
        }
    };

    let listener = TcpListener::bind(socks_service.bind_label()).await?;
    let local_addr = listener.local_addr()?;
    let allocator = StreamIdAllocator::new(1);
    println!("service.local.active=socks5://{}", local_addr);

    loop {
        let (client, client_addr) = listener.accept().await?;
        println!("service.local.client={} via=socks5", client_addr);
        let peer = peer.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let stream_id = allocator.next();
        tokio::spawn(async move {
            if let Err(err) = handle_outbound_socks5_client(
                peer,
                remote_raw_definition,
                remote_peer_id,
                client,
                stream_id,
                registry,
            )
            .await
            {
                eprintln!("service.local.client.error={} stream_id={}", err, stream_id);
            }
        });
    }
}

async fn handle_outbound_socks5_client(
    peer: tcp_mux::MuxTcpPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    accept_no_auth(&mut client).await?;
    let request = read_connect_request(&mut client).await?;

    let mut rx = peer.open_stream_receiver(stream_id).await;
    let open_message = build_remote_stream_open_for_request(&remote_raw_definition, &request)?;
    registry.lock().await.open_stream(
        stream_id,
        peer.session.remote.agent_id.clone(),
        open_message.service.clone(),
        match (&open_message.target_host, open_message.target_port) {
            (Some(host), Some(port)) => format!("{}:{}", host, port),
            _ => "dynamic".to_string(),
        },
    );
    let open = Frame::new(
        MessageType::StreamOpen,
        Some(peer.session.local.agent_id.clone()),
        Some(
            remote_peer_id
                .clone()
                .unwrap_or_else(|| peer.session.remote.agent_id.clone()),
        ),
        Message::StreamOpen(open_message),
    )
    .with_stream_id(stream_id);
    peer.send_frame(&open).await?;
    registry.lock().await.mark_stream_active(stream_id);
    write_success_response(&mut client).await?;

    loop {
        let mut buf = [0_u8; 4096];
        let n = client.read(&mut buf).await?;
        if n == 0 {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }

        let payload = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(&buf[..n])),
        )
        .with_stream_id(stream_id);
        peer.send_frame(&payload).await?;

        let response = rx
            .recv()
            .await
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "stream receiver closed"))?;
        if response.header.stream_id != Some(stream_id) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unexpected stream_id on socks5 response",
            ));
        }
        match response.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                client.write_all(&bytes).await?;
                client.flush().await?;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("expected StreamData response, got {:?}", other),
                ))
            }
        }
    }

    let close = Frame::new(
        MessageType::StreamClose,
        Some(peer.session.local.agent_id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::StreamClose(StreamCloseMessage { reason: None }),
    )
    .with_stream_id(stream_id);
    peer.send_frame(&close).await?;
    let close_ack = rx
        .recv()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "close ack receiver closed"))?;
    if close_ack.header.stream_id != Some(stream_id) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "unexpected stream_id on close ack",
        ));
    }
    match close_ack.message {
        Message::StreamClose(_) => {
            registry.lock().await.mark_stream_closed(stream_id);
            Ok(())
        }
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected StreamClose ack, got {:?}", other),
        )),
    }
}

async fn run_outbound_socks5_ws_once(
    identity: AgentIdentity,
    endpoint: &TunnelEndpoint,
    local_socks_definition: ServiceDefinition,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let peer = ws_mux::connect_mux_peer(identity, &endpoint.url.original).await?;
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    println!(
        "session.outbound.peer={} to={} via=ws",
        peer.session.remote.agent_id, endpoint.url.original
    );

    let socks_service = match local_socks_definition.kind {
        ServiceKind::LocalSocks5(service) => service,
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "outbound socks5 handler requires local socks5 service",
            ))
        }
    };

    let listener = TcpListener::bind(socks_service.bind_label()).await?;
    let local_addr = listener.local_addr()?;
    let allocator = StreamIdAllocator::new(1);
    println!("service.local.active=socks5://{}", local_addr);

    loop {
        let (client, client_addr) = listener.accept().await?;
        println!("service.local.client={} via=socks5", client_addr);
        let peer = peer.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let stream_id = allocator.next();
        tokio::spawn(async move {
            if let Err(err) = handle_outbound_socks5_ws_client(
                peer,
                remote_raw_definition,
                remote_peer_id,
                client,
                stream_id,
                registry,
            )
            .await
            {
                eprintln!("service.local.client.error={} stream_id={}", err, stream_id);
            }
        });
    }
}

async fn handle_outbound_socks5_ws_client(
    peer: ws_mux::MuxWsPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    accept_no_auth(&mut client).await?;
    let request = read_connect_request(&mut client).await?;

    let mut rx = peer.open_stream_receiver(stream_id).await;
    let open_message = build_remote_stream_open_for_request(&remote_raw_definition, &request)?;
    registry.lock().await.open_stream(
        stream_id,
        peer.session.remote.agent_id.clone(),
        open_message.service.clone(),
        match (&open_message.target_host, open_message.target_port) {
            (Some(host), Some(port)) => format!("{}:{}", host, port),
            _ => "dynamic".to_string(),
        },
    );
    let open = Frame::new(
        MessageType::StreamOpen,
        Some(peer.session.local.agent_id.clone()),
        Some(
            remote_peer_id
                .clone()
                .unwrap_or_else(|| peer.session.remote.agent_id.clone()),
        ),
        Message::StreamOpen(open_message),
    )
    .with_stream_id(stream_id);
    peer.send_frame(&open).await?;
    registry.lock().await.mark_stream_active(stream_id);
    write_success_response(&mut client).await?;

    loop {
        let mut buf = [0_u8; 4096];
        let n = client.read(&mut buf).await?;
        if n == 0 {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }

        let payload = Frame::new(
            MessageType::StreamData,
            Some(peer.session.local.agent_id.clone()),
            Some(peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(&buf[..n])),
        )
        .with_stream_id(stream_id);
        peer.send_frame(&payload).await?;

        let response = rx
            .recv()
            .await
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "stream receiver closed"))?;
        if response.header.stream_id != Some(stream_id) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unexpected stream_id on socks5 response",
            ));
        }
        match response.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                client.write_all(&bytes).await?;
                client.flush().await?;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("expected StreamData response, got {:?}", other),
                ))
            }
        }
    }

    let close = Frame::new(
        MessageType::StreamClose,
        Some(peer.session.local.agent_id.clone()),
        Some(peer.session.remote.agent_id.clone()),
        Message::StreamClose(StreamCloseMessage { reason: None }),
    )
    .with_stream_id(stream_id);
    peer.send_frame(&close).await?;
    let close_ack = rx
        .recv()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "close ack receiver closed"))?;
    if close_ack.header.stream_id != Some(stream_id) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "unexpected stream_id on close ack",
        ));
    }
    match close_ack.message {
        Message::StreamClose(_) => {
            registry.lock().await.mark_stream_closed(stream_id);
            Ok(())
        }
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected StreamClose ack, got {:?}", other),
        )),
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

#[cfg(test)]
mod runtime_tests {
    use super::{handle_outbound_socks5_client, handle_outbound_socks5_ws_client};
    use crate::{
        agent::identity::AgentIdentity,
        app::{
            config::{AgentIdentityConfig, ServeEndpoint, StatusScope, TaskRequestConfig},
            runtime_relay::{handle_tcp_relay_stream_open, handle_ws_relay_stream_open},
            runtime_status::{
                print_status_snapshot, render_status_lines, write_status_snapshot, RelayStreamLink,
                RuntimeStatusSnapshot,
            },
            runtime_task::maybe_store_task_artifact,
        },
        protocol::{
            frame::{Frame, MessageType},
            message::{
                Message, StreamCloseMessage, StreamDataMessage, StreamOpenMessage, TaskAction,
                TaskResultMessage,
            },
        },
        serve::{
            raw::{proxy_mux_stream_loop, proxy_ws_mux_stream_loop, RawService},
            service::{build_remote_services, ServiceDefinition},
        },
        tunnel::{
            tcp::bind,
            tcp_mux::{accept_mux_peer, connect_mux_peer},
            ws_mux::{
                accept_mux_peer as accept_ws_mux_peer, bind as bind_ws,
                connect_mux_peer as connect_ws_mux_peer,
            },
        },
        utils::url::ParsedUrl,
    };
    use data_encoding::HEXLOWER;
    use std::sync::Arc;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::Mutex,
    };

    fn dynamic_raw_service_definition() -> ServiceDefinition {
        build_remote_services(&[ServeEndpoint {
            url: ParsedUrl::parse("raw://").unwrap(),
        }])
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    }

    async fn tcp_socket_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server, _) = listener.accept().await.unwrap();
        (client.await.unwrap(), server)
    }

    #[tokio::test]
    async fn task_artifact_save_path_override_is_used() {
        let root = std::env::temp_dir().join(format!("fusion-runtime-test-{}", std::process::id()));
        let path = root.join("custom").join("artifact.txt");
        let req = TaskRequestConfig {
            action: TaskAction::Shell,
            args: vec!["echo hi".into()],
            data_hex: None,
            save_path: Some(path.clone()),
            target_agent_id: None,
        };
        let result = TaskResultMessage {
            task_id: "task-custom-save".into(),
            ok: true,
            output: "hi".into(),
            data_hex: Some(HEXLOWER.encode(b"hello-artifact")),
        };
        let saved = maybe_store_task_artifact(&root, &req, &result)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved, path);
        let bytes = tokio::fs::read(&saved).await.unwrap();
        assert_eq!(bytes, b"hello-artifact");
        let _ = tokio::fs::remove_file(&saved).await;
    }

    #[tokio::test]
    async fn status_snapshot_is_written_and_filtered() {
        let root = std::env::temp_dir().join(format!("fusion-status-test-{}", std::process::id()));
        let hub = Arc::new(Mutex::new(crate::session::hub::SessionHub::new()));
        let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));
        registry.lock().await.set_local_services(vec!["service.local=socks5://127.0.0.1:1080".into()]);
        registry.lock().await.upsert_announce(
            &crate::protocol::message::AgentAnnounceMessage {
                agent_id: "peer-x".into(),
                agent_name: "peer-x-name".into(),
                capabilities: vec!["task:shell".into()],
                services: vec!["raw://dynamic".into()],
            },
            "peer-x",
        );
        registry
            .lock()
            .await
            .open_stream(7, "peer-x".into(), "raw".into(), "dynamic".into());
        registry.lock().await.mark_stream_active(7);

        let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
        relay_links.lock().await.insert(
            "tcp:peer-x:7".into(),
            RelayStreamLink {
                transport: "tcp".into(),
                source_peer_agent_id: "peer-x".into(),
                source_stream_id: 7,
                next_hop_agent_id: "peer-y".into(),
                relay_stream_id: 100000,
                destination_agent_id: "peer-z".into(),
                opened_at_unix: 123,
            },
        );
        write_status_snapshot(&root, &hub, &registry, &relay_links)
            .await
            .unwrap();

        let status_file = root.join("runtime-status.json");
        assert!(status_file.exists());
        let payload = tokio::fs::read(&status_file).await.unwrap();
        let snapshot: RuntimeStatusSnapshot = serde_json::from_slice(&payload).unwrap();
        assert_eq!(snapshot.relay_links.len(), 1);
        let stream_lines = render_status_lines(&snapshot, StatusScope::Streams);
        assert!(stream_lines
            .iter()
            .any(|line| line == "relay.link_count=1"));
        assert!(stream_lines
            .iter()
            .any(|line| line.contains("relay.link transport=tcp")));

        print_status_snapshot(&root, StatusScope::Peers, false)
            .await
            .unwrap();
        print_status_snapshot(&root, StatusScope::Routes, false)
            .await
            .unwrap();
        print_status_snapshot(&root, StatusScope::Streams, false)
            .await
            .unwrap();
        print_status_snapshot(&root, StatusScope::Streams, true)
            .await
            .unwrap();

        let _ = tokio::fs::remove_file(status_file).await;
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn tcp_relay_stream_bridge_roundtrip() {
        let echo_listener = bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = echo_listener.accept().await.unwrap();
            let mut buf = [0_u8; 256];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                stream.write_all(&buf[..n]).await.unwrap();
            }
        });

        let target_listener = bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        let relay_down_listener = bind("127.0.0.1:0").await.unwrap();
        let relay_down_addr = relay_down_listener.local_addr().unwrap();

        let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("relay-stream-target".into()),
            key: None,
        });
        let relay_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("relay-stream-node".into()),
            key: None,
        });
        let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("relay-stream-leaf".into()),
            key: None,
        });

        let target_task = tokio::spawn(async move {
            let peer = accept_mux_peer(target_identity, target_listener).await.unwrap();
            let (stream_id, open) = peer.read_stream_open().await.unwrap();
            let rx = peer.open_stream_receiver(stream_id).await;
            proxy_mux_stream_loop(
                peer,
                &RawService {
                    host: None,
                    port: None,
                },
                open,
                stream_id,
                rx,
            )
            .await
            .unwrap();
        });

        let relay_accept = tokio::spawn(async move {
            accept_mux_peer(relay_identity.clone(), relay_down_listener)
                .await
                .unwrap()
        });

        let target_peer = connect_mux_peer(
            AgentIdentity::from_config(&AgentIdentityConfig {
                name: Some("relay-upstream".into()),
                key: None,
            }),
            &target_addr.to_string(),
        )
        .await
        .unwrap();
        let leaf_peer = connect_mux_peer(leaf_identity, &relay_down_addr.to_string())
            .await
            .unwrap();
        let downstream_peer = relay_accept.await.unwrap();

        let mut peer_map = std::collections::HashMap::new();
        peer_map.insert(target_peer.session.remote.agent_id.clone(), target_peer.clone());
        let peer_map = Arc::new(Mutex::new(peer_map));
        let allocator = Arc::new(Mutex::new(1000_u32));

        let open_frame = Frame::new(
            MessageType::StreamOpen,
            Some(leaf_peer.session.local.agent_id.clone()),
            Some(target_peer.session.remote.agent_id.clone()),
            Message::StreamOpen(StreamOpenMessage {
                service: "raw".into(),
                target_host: Some("127.0.0.1".into()),
                target_port: Some(echo_addr.port()),
            }),
        )
        .with_stream_id(7);

        let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
        handle_tcp_relay_stream_open(peer_map, allocator, relay_links, downstream_peer, open_frame)
            .await
            .unwrap();

        let mut leaf_rx = leaf_peer.open_stream_receiver(7).await;
        let payload = Frame::new(
            MessageType::StreamData,
            Some(leaf_peer.session.local.agent_id.clone()),
            Some(target_peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"relay-stream-ok")),
        )
        .with_stream_id(7);
        leaf_peer.send_frame(&payload).await.unwrap();

        let response = leaf_rx.recv().await.unwrap();
        match response.message {
            Message::StreamData(data) => {
                assert_eq!(data.to_bytes().unwrap(), b"relay-stream-ok");
            }
            other => panic!("unexpected relayed stream response: {:?}", other),
        }

        let close = Frame::new(
            MessageType::StreamClose,
            Some(leaf_peer.session.local.agent_id.clone()),
            Some(target_peer.session.remote.agent_id.clone()),
            Message::StreamClose(StreamCloseMessage { reason: None }),
        )
        .with_stream_id(7);
        leaf_peer.send_frame(&close).await.unwrap();
        let close_ack = leaf_rx.recv().await.unwrap();
        assert!(matches!(close_ack.message, Message::StreamClose(_)));

        target_task.await.unwrap();
    }

    #[tokio::test]
    async fn ws_relay_stream_bridge_roundtrip() {
        let echo_listener = bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = echo_listener.accept().await.unwrap();
            let mut buf = [0_u8; 256];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                stream.write_all(&buf[..n]).await.unwrap();
            }
        });

        let target_listener = bind_ws("127.0.0.1:0").await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        let relay_down_listener = bind_ws("127.0.0.1:0").await.unwrap();
        let relay_down_addr = relay_down_listener.local_addr().unwrap();

        let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-relay-stream-target".into()),
            key: None,
        });
        let relay_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-relay-stream-node".into()),
            key: None,
        });
        let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-relay-stream-leaf".into()),
            key: None,
        });

        let target_task = tokio::spawn(async move {
            let peer = accept_ws_mux_peer(target_identity, target_listener, None)
                .await
                .unwrap();
            let (stream_id, open) = peer.read_stream_open().await.unwrap();
            let rx = peer.open_stream_receiver(stream_id).await;
            proxy_ws_mux_stream_loop(
                peer,
                &RawService {
                    host: None,
                    port: None,
                },
                open,
                stream_id,
                rx,
            )
            .await
            .unwrap();
        });

        let relay_accept = tokio::spawn(async move {
            accept_ws_mux_peer(relay_identity.clone(), relay_down_listener, None)
                .await
                .unwrap()
        });

        let target_peer = connect_ws_mux_peer(
            AgentIdentity::from_config(&AgentIdentityConfig {
                name: Some("ws-relay-upstream".into()),
                key: None,
            }),
            &format!("ws://{}/tunnel", target_addr),
        )
        .await
        .unwrap();
        let leaf_peer = connect_ws_mux_peer(leaf_identity, &format!("ws://{}/tunnel", relay_down_addr))
            .await
            .unwrap();
        let downstream_peer = relay_accept.await.unwrap();

        let mut peer_map = std::collections::HashMap::new();
        peer_map.insert(target_peer.session.remote.agent_id.clone(), target_peer.clone());
        let peer_map = Arc::new(Mutex::new(peer_map));
        let allocator = Arc::new(Mutex::new(2000_u32));

        let open_frame = Frame::new(
            MessageType::StreamOpen,
            Some(leaf_peer.session.local.agent_id.clone()),
            Some(target_peer.session.remote.agent_id.clone()),
            Message::StreamOpen(StreamOpenMessage {
                service: "raw".into(),
                target_host: Some("127.0.0.1".into()),
                target_port: Some(echo_addr.port()),
            }),
        )
        .with_stream_id(9);

        let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
        handle_ws_relay_stream_open(peer_map, allocator, relay_links, downstream_peer, open_frame)
            .await
            .unwrap();

        let mut leaf_rx = leaf_peer.open_stream_receiver(9).await;
        let payload = Frame::new(
            MessageType::StreamData,
            Some(leaf_peer.session.local.agent_id.clone()),
            Some(target_peer.session.remote.agent_id.clone()),
            Message::StreamData(StreamDataMessage::from_bytes(b"ws-relay-stream-ok")),
        )
        .with_stream_id(9);
        leaf_peer.send_frame(&payload).await.unwrap();

        let response = leaf_rx.recv().await.unwrap();
        match response.message {
            Message::StreamData(data) => {
                assert_eq!(data.to_bytes().unwrap(), b"ws-relay-stream-ok");
            }
            other => panic!("unexpected ws relayed stream response: {:?}", other),
        }

        let close = Frame::new(
            MessageType::StreamClose,
            Some(leaf_peer.session.local.agent_id.clone()),
            Some(target_peer.session.remote.agent_id.clone()),
            Message::StreamClose(StreamCloseMessage { reason: None }),
        )
        .with_stream_id(9);
        leaf_peer.send_frame(&close).await.unwrap();
        let close_ack = leaf_rx.recv().await.unwrap();
        assert!(matches!(close_ack.message, Message::StreamClose(_)));

        target_task.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_socks5_over_relay_roundtrip() {
        let echo_listener = bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = echo_listener.accept().await.unwrap();
            let mut buf = [0_u8; 256];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                stream.write_all(&buf[..n]).await.unwrap();
            }
        });

        let target_listener = bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        let relay_listener = bind("127.0.0.1:0").await.unwrap();
        let relay_addr = relay_listener.local_addr().unwrap();

        let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("tcp-socks-target".into()),
            key: None,
        });
        let relay_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("tcp-socks-relay".into()),
            key: None,
        });
        let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("tcp-socks-leaf".into()),
            key: None,
        });

        let target_id = target_identity.id.clone();
        let target_task = tokio::spawn(async move {
            let peer = accept_mux_peer(target_identity, target_listener).await.unwrap();
            let (stream_id, open) = peer.read_stream_open().await.unwrap();
            let rx = peer.open_stream_receiver(stream_id).await;
            proxy_mux_stream_loop(
                peer,
                &RawService {
                    host: None,
                    port: None,
                },
                open,
                stream_id,
                rx,
            )
            .await
            .unwrap();
        });

        let relay_task = tokio::spawn(async move {
            let downstream_peer = accept_mux_peer(relay_identity, relay_listener).await.unwrap();
            let upstream_peer = connect_mux_peer(
                AgentIdentity::from_config(&AgentIdentityConfig {
                    name: Some("tcp-socks-upstream".into()),
                    key: None,
                }),
                &target_addr.to_string(),
            )
            .await
            .unwrap();
            let mut peer_map = std::collections::HashMap::new();
            peer_map.insert(upstream_peer.session.remote.agent_id.clone(), upstream_peer.clone());
            let peer_map = Arc::new(Mutex::new(peer_map));
            let allocator = Arc::new(Mutex::new(3000_u32));
            let open_frame = downstream_peer.read_stream_open_frame().await.unwrap();
            let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
            handle_tcp_relay_stream_open(
                peer_map,
                allocator,
                relay_links,
                downstream_peer,
                open_frame,
            )
                .await
                .unwrap();
        });

        let leaf_peer = connect_mux_peer(leaf_identity, &relay_addr.to_string())
            .await
            .unwrap();
        let (mut local_client, local_server) = tcp_socket_pair().await;
        let remote_def = dynamic_raw_service_definition();
        let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));

        let client_task = tokio::spawn(async move {
            local_client.write_all(b"\x05\x01\x00").await.unwrap();
            let mut method_resp = [0_u8; 2];
            local_client.read_exact(&mut method_resp).await.unwrap();
            assert_eq!(&method_resp, b"\x05\x00");

            let mut connect_req = vec![0x05, 0x01, 0x00, 0x01];
            connect_req.extend_from_slice(&[127, 0, 0, 1]);
            connect_req.extend_from_slice(&echo_addr.port().to_be_bytes());
            local_client.write_all(&connect_req).await.unwrap();

            let mut connect_resp = [0_u8; 10];
            local_client.read_exact(&mut connect_resp).await.unwrap();
            assert_eq!(&connect_resp[..2], b"\x05\x00");

            local_client.write_all(b"tcp-socks-relay-ok").await.unwrap();
            let mut buf = [0_u8; 64];
            let n = local_client.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"tcp-socks-relay-ok");
        });

        handle_outbound_socks5_client(
            leaf_peer,
            remote_def,
            Some(target_id),
            local_server,
            1,
            registry,
        )
        .await
        .unwrap();

        client_task.await.unwrap();
        relay_task.await.unwrap();
        target_task.await.unwrap();
    }

    #[tokio::test]
    async fn ws_socks5_over_relay_roundtrip() {
        let echo_listener = bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = echo_listener.accept().await.unwrap();
            let mut buf = [0_u8; 256];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                stream.write_all(&buf[..n]).await.unwrap();
            }
        });

        let target_listener = bind_ws("127.0.0.1:0").await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        let relay_listener = bind_ws("127.0.0.1:0").await.unwrap();
        let relay_addr = relay_listener.local_addr().unwrap();

        let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-socks-target".into()),
            key: None,
        });
        let relay_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-socks-relay".into()),
            key: None,
        });
        let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-socks-leaf".into()),
            key: None,
        });

        let target_id = target_identity.id.clone();
        let target_task = tokio::spawn(async move {
            let peer = accept_ws_mux_peer(target_identity, target_listener, None)
                .await
                .unwrap();
            let (stream_id, open) = peer.read_stream_open().await.unwrap();
            let rx = peer.open_stream_receiver(stream_id).await;
            proxy_ws_mux_stream_loop(
                peer,
                &RawService {
                    host: None,
                    port: None,
                },
                open,
                stream_id,
                rx,
            )
            .await
            .unwrap();
        });

        let relay_task = tokio::spawn(async move {
            let downstream_peer = accept_ws_mux_peer(relay_identity, relay_listener, None)
                .await
                .unwrap();
            let upstream_peer = connect_ws_mux_peer(
                AgentIdentity::from_config(&AgentIdentityConfig {
                    name: Some("ws-socks-upstream".into()),
                    key: None,
                }),
                &format!("ws://{}/tunnel", target_addr),
            )
            .await
            .unwrap();
            let mut peer_map = std::collections::HashMap::new();
            peer_map.insert(upstream_peer.session.remote.agent_id.clone(), upstream_peer.clone());
            let peer_map = Arc::new(Mutex::new(peer_map));
            let allocator = Arc::new(Mutex::new(4000_u32));
            let open_frame = downstream_peer.read_stream_open_frame().await.unwrap();
            let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
            handle_ws_relay_stream_open(
                peer_map,
                allocator,
                relay_links,
                downstream_peer,
                open_frame,
            )
                .await
                .unwrap();
        });

        let leaf_peer = connect_ws_mux_peer(leaf_identity, &format!("ws://{}/tunnel", relay_addr))
            .await
            .unwrap();
        let (mut local_client, local_server) = tcp_socket_pair().await;
        let remote_def = dynamic_raw_service_definition();
        let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));

        let client_task = tokio::spawn(async move {
            local_client.write_all(b"\x05\x01\x00").await.unwrap();
            let mut method_resp = [0_u8; 2];
            local_client.read_exact(&mut method_resp).await.unwrap();
            assert_eq!(&method_resp, b"\x05\x00");

            let mut connect_req = vec![0x05, 0x01, 0x00, 0x01];
            connect_req.extend_from_slice(&[127, 0, 0, 1]);
            connect_req.extend_from_slice(&echo_addr.port().to_be_bytes());
            local_client.write_all(&connect_req).await.unwrap();

            let mut connect_resp = [0_u8; 10];
            local_client.read_exact(&mut connect_resp).await.unwrap();
            assert_eq!(&connect_resp[..2], b"\x05\x00");

            local_client.write_all(b"ws-socks-relay-ok").await.unwrap();
            let mut buf = [0_u8; 64];
            let n = local_client.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"ws-socks-relay-ok");
        });

        handle_outbound_socks5_ws_client(
            leaf_peer,
            remote_def,
            Some(target_id),
            local_server,
            1,
            registry,
        )
        .await
        .unwrap();

        client_task.await.unwrap();
        relay_task.await.unwrap();
        target_task.await.unwrap();
    }
}
