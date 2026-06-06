use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    app::{
        config::ConnPolicy,
        config::TunnelEndpoint,
        runtime::build_stream_target_label,
        runtime_bridge::{
            build_stream_close_frame, build_stream_data_frame, expect_stream_close_ack,
            write_next_stream_data_to_client,
        },
        upstream_pool::{TcpMuxUpstreamPool, WsMuxUpstreamPool},
    },
    protocol::{
        frame::{Frame, MessageType},
        message::Message,
    },
    serve::{
        service::{build_remote_stream_open_for_request, ServiceDefinition, ServiceKind},
        socks5::{accept_auth, read_connect_request, write_success_response, Socks5Service},
    },
    session::{hub::SessionHub, stream::StreamIdAllocator},
    tunnel::{tcp_mux, ws_mux},
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::{sleep, Duration},
};

pub async fn run_outbound_socks5_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    local_socks_definition: ServiceDefinition,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: ConnPolicy,
    proxy_chain: Vec<String>,
) -> Result<(), Error> {
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
    let upstream_pool = TcpMuxUpstreamPool::new();
    spawn_tcp_upstream_pool_maintenance(upstream_pool.clone(), hub.clone(), registry.clone());
    println!("service.local.active=socks5://{}", local_addr);

    loop {
        let (client, client_addr) = listener.accept().await?;
        println!("service.local.client={} via=socks5", client_addr);
        let endpoints = endpoints.to_vec();
        let identity = identity.clone();
        let socks_service = socks_service.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let hub = hub.clone();
        let conn_policy = conn_policy.clone();
        let proxy_chain = proxy_chain.clone();
        let upstream_pool = upstream_pool.clone();
        let stream_id = allocator.next();
        tokio::spawn(async move {
            if let Err(err) = handle_outbound_socks5_client_with_failover_tcp(
                upstream_pool,
                identity,
                endpoints,
                conn_policy,
                proxy_chain,
                socks_service,
                remote_raw_definition,
                remote_peer_id,
                client,
                stream_id,
                hub,
                registry,
            )
            .await
            {
                eprintln!("service.local.client.error={} stream_id={}", err, stream_id);
            }
        });
    }
}

pub async fn handle_outbound_socks5_client(
    peer: tcp_mux::MuxTcpPeer,
    local_socks_service: Socks5Service,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    accept_auth(&mut client, &local_socks_service).await?;
    let request = read_connect_request(&mut client).await?;
    process_outbound_socks5_tcp_request(
        peer,
        remote_raw_definition,
        remote_peer_id,
        &mut client,
        stream_id,
        registry,
        request,
    )
    .await
}

async fn process_outbound_socks5_tcp_request(
    peer: tcp_mux::MuxTcpPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    client: &mut TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
    request: crate::serve::socks5::Socks5ConnectRequest,
) -> Result<(), Error> {
    let mut rx = peer.open_stream_receiver(stream_id).await;
    let open_message = build_remote_stream_open_for_request(&remote_raw_definition, &request)?;
    registry.lock().await.open_stream(
        stream_id,
        peer.session.remote.agent_id.clone(),
        open_message.service.clone(),
        build_stream_target_label(&open_message.target_host, open_message.target_port),
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
    write_success_response(client).await?;

    loop {
        let mut buf = [0_u8; 4096];
        let n = client.read(&mut buf).await?;
        if n == 0 {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }

        let payload = build_stream_data_frame(
            &peer.session.local.agent_id,
            &peer.session.remote.agent_id,
            stream_id,
            &buf[..n],
        );
        peer.send_frame(&payload).await?;
        write_next_stream_data_to_client(&mut rx, client, stream_id, "socks5").await?;
    }

    let close = build_stream_close_frame(
        &peer.session.local.agent_id,
        &peer.session.remote.agent_id,
        stream_id,
    );
    peer.send_frame(&close).await?;
    expect_stream_close_ack(&mut rx, stream_id).await?;
    registry.lock().await.mark_stream_closed(stream_id);
    Ok(())
}

pub async fn run_outbound_socks5_ws_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    local_socks_definition: ServiceDefinition,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: ConnPolicy,
) -> Result<(), Error> {
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
    let upstream_pool = WsMuxUpstreamPool::new();
    spawn_ws_upstream_pool_maintenance(upstream_pool.clone(), hub.clone(), registry.clone());
    println!("service.local.active=socks5://{}", local_addr);

    loop {
        let (client, client_addr) = listener.accept().await?;
        println!("service.local.client={} via=socks5", client_addr);
        let endpoints = endpoints.to_vec();
        let identity = identity.clone();
        let socks_service = socks_service.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let hub = hub.clone();
        let conn_policy = conn_policy.clone();
        let upstream_pool = upstream_pool.clone();
        let stream_id = allocator.next();
        tokio::spawn(async move {
            if let Err(err) = handle_outbound_socks5_client_with_failover_ws(
                upstream_pool,
                identity,
                endpoints,
                conn_policy,
                socks_service,
                remote_raw_definition,
                remote_peer_id,
                client,
                stream_id,
                hub,
                registry,
            )
            .await
            {
                eprintln!("service.local.client.error={} stream_id={}", err, stream_id);
            }
        });
    }
}

async fn connect_selected_socks5_peer_tcp(
    upstream_pool: TcpMuxUpstreamPool,
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    conn_policy: &ConnPolicy,
    proxy_chain: &[String],
    hub: &Arc<Mutex<SessionHub>>,
    registry: &Arc<Mutex<AgentRegistry>>,
) -> Result<(String, tcp_mux::MuxTcpPeer), Error> {
    upstream_pool
        .acquire(identity, endpoints, conn_policy, proxy_chain, hub, registry)
        .await
}

async fn connect_selected_socks5_peer_ws(
    upstream_pool: WsMuxUpstreamPool,
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    conn_policy: &ConnPolicy,
    hub: &Arc<Mutex<SessionHub>>,
    registry: &Arc<Mutex<AgentRegistry>>,
) -> Result<(String, ws_mux::MuxWsPeer), Error> {
    upstream_pool
        .acquire(identity, endpoints, conn_policy, hub, registry)
        .await
}

pub async fn handle_outbound_socks5_ws_client(
    peer: ws_mux::MuxWsPeer,
    local_socks_service: Socks5Service,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    accept_auth(&mut client, &local_socks_service).await?;
    let request = read_connect_request(&mut client).await?;
    process_outbound_socks5_ws_request(
        peer,
        remote_raw_definition,
        remote_peer_id,
        &mut client,
        stream_id,
        registry,
        request,
    )
    .await
}

async fn process_outbound_socks5_ws_request(
    peer: ws_mux::MuxWsPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    client: &mut TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
    request: crate::serve::socks5::Socks5ConnectRequest,
) -> Result<(), Error> {
    let mut rx = peer.open_stream_receiver(stream_id).await;
    let open_message = build_remote_stream_open_for_request(&remote_raw_definition, &request)?;
    registry.lock().await.open_stream(
        stream_id,
        peer.session.remote.agent_id.clone(),
        open_message.service.clone(),
        build_stream_target_label(&open_message.target_host, open_message.target_port),
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
    write_success_response(client).await?;

    loop {
        let mut buf = [0_u8; 4096];
        let n = client.read(&mut buf).await?;
        if n == 0 {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }

        let payload = build_stream_data_frame(
            &peer.session.local.agent_id,
            &peer.session.remote.agent_id,
            stream_id,
            &buf[..n],
        );
        peer.send_frame(&payload).await?;
        write_next_stream_data_to_client(&mut rx, client, stream_id, "socks5").await?;
    }

    let close = build_stream_close_frame(
        &peer.session.local.agent_id,
        &peer.session.remote.agent_id,
        stream_id,
    );
    peer.send_frame(&close).await?;
    expect_stream_close_ack(&mut rx, stream_id).await?;
    registry.lock().await.mark_stream_closed(stream_id);
    Ok(())
}

async fn handle_outbound_socks5_client_with_failover_tcp(
    upstream_pool: TcpMuxUpstreamPool,
    identity: AgentIdentity,
    endpoints: Vec<TunnelEndpoint>,
    conn_policy: ConnPolicy,
    proxy_chain: Vec<String>,
    local_socks_service: Socks5Service,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    accept_auth(&mut client, &local_socks_service).await?;
    let request = read_connect_request(&mut client).await?;
    let attempts = endpoints.len().max(1);
    let mut last_err = None;
    for _ in 0..attempts {
        let (key, peer) = match connect_selected_socks5_peer_tcp(
            upstream_pool.clone(),
            identity.clone(),
            &endpoints,
            &conn_policy,
            &proxy_chain,
            &hub,
            &registry,
        )
        .await
        {
            Ok(v) => v,
            Err(err) => {
                last_err = Some(err);
                continue;
            }
        };
        println!("upstream.pool.reuse=tcp endpoint={key}");
        match process_outbound_socks5_tcp_request(
            peer,
            remote_raw_definition.clone(),
            remote_peer_id.clone(),
            &mut client,
            stream_id,
            registry.clone(),
            request.clone(),
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) => {
                upstream_pool.invalidate(&key).await;
                last_err = Some(err);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::other("no socks5 tcp upstream succeeded")))
}

async fn handle_outbound_socks5_client_with_failover_ws(
    upstream_pool: WsMuxUpstreamPool,
    identity: AgentIdentity,
    endpoints: Vec<TunnelEndpoint>,
    conn_policy: ConnPolicy,
    local_socks_service: Socks5Service,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    accept_auth(&mut client, &local_socks_service).await?;
    let request = read_connect_request(&mut client).await?;
    let attempts = endpoints.len().max(1);
    let mut last_err = None;
    for _ in 0..attempts {
        let (key, peer) = match connect_selected_socks5_peer_ws(
            upstream_pool.clone(),
            identity.clone(),
            &endpoints,
            &conn_policy,
            &hub,
            &registry,
        )
        .await
        {
            Ok(v) => v,
            Err(err) => {
                last_err = Some(err);
                continue;
            }
        };
        println!("upstream.pool.reuse=ws endpoint={key}");
        match process_outbound_socks5_ws_request(
            peer,
            remote_raw_definition.clone(),
            remote_peer_id.clone(),
            &mut client,
            stream_id,
            registry.clone(),
            request.clone(),
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) => {
                upstream_pool.invalidate(&key).await;
                last_err = Some(err);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::other("no socks5 ws upstream succeeded")))
}

fn spawn_tcp_upstream_pool_maintenance(
    pool: TcpMuxUpstreamPool,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) {
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(2)).await;
            let removed = pool.prune_stale(&hub, &registry).await;
            if removed > 0 {
                eprintln!("upstream.pool.pruned transport=tcp removed={removed}");
            }
        }
    });
}

fn spawn_ws_upstream_pool_maintenance(
    pool: WsMuxUpstreamPool,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) {
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(2)).await;
            let removed = pool.prune_stale(&hub, &registry).await;
            if removed > 0 {
                eprintln!("upstream.pool.pruned transport=ws removed={removed}");
            }
        }
    });
}
