use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::{sleep, Duration},
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
        service::{build_remote_stream_open_for_target, ServiceDefinition, ServiceKind},
        shadowsocks::{parse_shadowsocks_request, ShadowsocksRequest},
    },
    session::{hub::SessionHub, stream::StreamIdAllocator},
    tunnel::{tcp_mux, ws_mux},
};

pub async fn run_outbound_shadowsocks_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    local_ss_definition: ServiceDefinition,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: ConnPolicy,
    proxy_chain: Vec<String>,
) -> Result<(), Error> {
    let ss_service = match local_ss_definition.kind {
        ServiceKind::LocalShadowsocks(service) => service,
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "outbound shadowsocks handler requires local ss service",
            ))
        }
    };

    let listener = TcpListener::bind(ss_service.bind_label()).await?;
    let local_addr = listener.local_addr()?;
    let allocator = StreamIdAllocator::new(1);
    let upstream_pool = TcpMuxUpstreamPool::new();
    spawn_tcp_upstream_pool_maintenance(upstream_pool.clone(), hub.clone(), registry.clone());
    println!(
        "service.local.active=ss://{}{}",
        local_addr,
        ss_service.summary_suffix()
    );

    loop {
        let (client, client_addr) = listener.accept().await?;
        println!("service.local.client={} via=ss", client_addr);
        let endpoints = endpoints.to_vec();
        let identity = identity.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let hub = hub.clone();
        let conn_policy = conn_policy.clone();
        let proxy_chain = proxy_chain.clone();
        let upstream_pool = upstream_pool.clone();
        let stream_id = allocator.next();
        tokio::spawn(async move {
            if let Err(err) = handle_outbound_shadowsocks_client_with_failover_tcp(
                upstream_pool,
                identity,
                endpoints,
                conn_policy,
                proxy_chain,
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

pub async fn run_outbound_shadowsocks_ws_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    local_ss_definition: ServiceDefinition,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: ConnPolicy,
) -> Result<(), Error> {
    let ss_service = match local_ss_definition.kind {
        ServiceKind::LocalShadowsocks(service) => service,
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "outbound shadowsocks handler requires local ss service",
            ))
        }
    };

    let listener = TcpListener::bind(ss_service.bind_label()).await?;
    let local_addr = listener.local_addr()?;
    let allocator = StreamIdAllocator::new(1);
    let upstream_pool = WsMuxUpstreamPool::new();
    spawn_ws_upstream_pool_maintenance(upstream_pool.clone(), hub.clone(), registry.clone());
    println!(
        "service.local.active=ss://{}{}",
        local_addr,
        ss_service.summary_suffix()
    );

    loop {
        let (client, client_addr) = listener.accept().await?;
        println!("service.local.client={} via=ss", client_addr);
        let endpoints = endpoints.to_vec();
        let identity = identity.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let hub = hub.clone();
        let conn_policy = conn_policy.clone();
        let upstream_pool = upstream_pool.clone();
        let stream_id = allocator.next();
        tokio::spawn(async move {
            if let Err(err) = handle_outbound_shadowsocks_client_with_failover_ws(
                upstream_pool,
                identity,
                endpoints,
                conn_policy,
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

async fn read_shadowsocks_request(client: &mut TcpStream) -> Result<ShadowsocksRequest, Error> {
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0_u8; 4096];
        let n = client.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "shadowsocks client closed before request was complete",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(request) = parse_shadowsocks_request(&buf)? {
            return Ok(request);
        }
    }
}

pub async fn handle_outbound_shadowsocks_client(
    peer: tcp_mux::MuxTcpPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_shadowsocks_request(&mut client).await?;
    handle_shadowsocks_client_inner(
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

pub async fn handle_outbound_shadowsocks_ws_client(
    peer: ws_mux::MuxWsPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_shadowsocks_request(&mut client).await?;
    handle_shadowsocks_ws_client_inner(
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

async fn handle_shadowsocks_client_inner(
    peer: tcp_mux::MuxTcpPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    client: &mut TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
    request: ShadowsocksRequest,
) -> Result<(), Error> {
    let mut rx = peer.open_stream_receiver(stream_id).await;
    let open_message = build_remote_stream_open_for_target(
        &remote_raw_definition,
        &request.target_host,
        request.target_port,
    )?;
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

    if !request.initial_payload.is_empty() {
        send_stream_data_tcp(&peer, stream_id, &request.initial_payload).await?;
        let _ = write_next_stream_data_to_client(&mut rx, client, stream_id, "ss").await?;
    }

    loop {
        let mut buf = [0_u8; 4096];
        let n = client.read(&mut buf).await?;
        if n == 0 {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }

        send_stream_data_tcp(&peer, stream_id, &buf[..n]).await?;
        if !write_next_stream_data_to_client(&mut rx, client, stream_id, "ss").await? {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }
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

async fn handle_shadowsocks_ws_client_inner(
    peer: ws_mux::MuxWsPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    client: &mut TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
    request: ShadowsocksRequest,
) -> Result<(), Error> {
    let mut rx = peer.open_stream_receiver(stream_id).await;
    let open_message = build_remote_stream_open_for_target(
        &remote_raw_definition,
        &request.target_host,
        request.target_port,
    )?;
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

    if !request.initial_payload.is_empty() {
        send_stream_data_ws(&peer, stream_id, &request.initial_payload).await?;
        let _ = write_next_stream_data_to_client(&mut rx, client, stream_id, "ss").await?;
    }

    loop {
        let mut buf = [0_u8; 4096];
        let n = client.read(&mut buf).await?;
        if n == 0 {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }

        send_stream_data_ws(&peer, stream_id, &buf[..n]).await?;
        if !write_next_stream_data_to_client(&mut rx, client, stream_id, "ss").await? {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }
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

async fn send_stream_data_tcp(
    peer: &tcp_mux::MuxTcpPeer,
    stream_id: u32,
    bytes: &[u8],
) -> Result<(), Error> {
    let payload = build_stream_data_frame(
        &peer.session.local.agent_id,
        &peer.session.remote.agent_id,
        stream_id,
        bytes,
    );
    peer.send_frame(&payload).await
}

async fn send_stream_data_ws(
    peer: &ws_mux::MuxWsPeer,
    stream_id: u32,
    bytes: &[u8],
) -> Result<(), Error> {
    let payload = build_stream_data_frame(
        &peer.session.local.agent_id,
        &peer.session.remote.agent_id,
        stream_id,
        bytes,
    );
    peer.send_frame(&payload).await
}

async fn connect_selected_shadowsocks_peer_tcp(
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

async fn connect_selected_shadowsocks_peer_ws(
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

async fn handle_outbound_shadowsocks_client_with_failover_tcp(
    upstream_pool: TcpMuxUpstreamPool,
    identity: AgentIdentity,
    endpoints: Vec<TunnelEndpoint>,
    conn_policy: ConnPolicy,
    proxy_chain: Vec<String>,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_shadowsocks_request(&mut client).await?;
    let attempts = endpoints.len().max(1);
    let mut last_err = None;
    for _ in 0..attempts {
        let (key, peer) = match connect_selected_shadowsocks_peer_tcp(
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
        match handle_shadowsocks_client_inner(
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
    Err(last_err.unwrap_or_else(|| Error::other("no shadowsocks tcp upstream succeeded")))
}

async fn handle_outbound_shadowsocks_client_with_failover_ws(
    upstream_pool: WsMuxUpstreamPool,
    identity: AgentIdentity,
    endpoints: Vec<TunnelEndpoint>,
    conn_policy: ConnPolicy,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_shadowsocks_request(&mut client).await?;
    let attempts = endpoints.len().max(1);
    let mut last_err = None;
    for _ in 0..attempts {
        let (key, peer) = match connect_selected_shadowsocks_peer_ws(
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
        match handle_shadowsocks_ws_client_inner(
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
    Err(last_err.unwrap_or_else(|| Error::other("no shadowsocks ws upstream succeeded")))
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
