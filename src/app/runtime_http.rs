use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
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
        runtime_status::UpstreamPoolStatusMap,
        upstream_pool::{TcpMuxUpstreamPool, WsMuxUpstreamPool},
    },
    protocol::{
        frame::{Frame, MessageType},
        message::Message,
    },
    serve::{
        http::{parse_http_proxy_request, HttpProxyRequest, HttpProxyService},
        service::{build_remote_stream_open_for_http_request, ServiceDefinition, ServiceKind},
    },
    session::{hub::SessionHub, stream::StreamIdAllocator},
    tunnel::{tcp_mux, ws_mux},
};

pub async fn run_outbound_http_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    local_http_definition: ServiceDefinition,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: ConnPolicy,
    proxy_chain: Vec<String>,
    pool_status: UpstreamPoolStatusMap,
) -> Result<(), Error> {
    let http_service = match local_http_definition.kind {
        ServiceKind::LocalHttpProxy(service) => service,
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "outbound http proxy handler requires local http service",
            ))
        }
    };

    let listener = TcpListener::bind(http_service.bind_label()).await?;
    let _local_addr = listener.local_addr()?;
    let allocator = StreamIdAllocator::new(1);
    let upstream_pool = TcpMuxUpstreamPool::with_status(pool_status, "http");
    spawn_tcp_upstream_pool_maintenance(upstream_pool.clone(), hub.clone(), registry.clone());

    loop {
        let (client, _client_addr) = listener.accept().await?;
        let endpoints = endpoints.to_vec();
        let identity = identity.clone();
        let http_service = http_service.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let hub = hub.clone();
        let conn_policy = conn_policy.clone();
        let proxy_chain = proxy_chain.clone();
        let upstream_pool = upstream_pool.clone();
        let stream_id = allocator.next();
        tokio::spawn(async move {
            if let Err(err) = handle_outbound_http_client_with_failover_tcp(
                upstream_pool,
                identity,
                endpoints,
                conn_policy,
                proxy_chain,
                http_service,
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

pub async fn handle_outbound_http_client(
    peer: tcp_mux::MuxTcpPeer,
    local_http_service: HttpProxyService,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_http_proxy_request(&mut client).await?;
    authorize_http_proxy_request(&mut client, &local_http_service, &request).await?;
    handle_http_proxy_client_inner(
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

pub async fn run_outbound_http_ws_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    local_http_definition: ServiceDefinition,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: ConnPolicy,
    pool_status: UpstreamPoolStatusMap,
) -> Result<(), Error> {
    let http_service = match local_http_definition.kind {
        ServiceKind::LocalHttpProxy(service) => service,
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "outbound http proxy handler requires local http service",
            ))
        }
    };

    let listener = TcpListener::bind(http_service.bind_label()).await?;
    let _local_addr = listener.local_addr()?;
    let allocator = StreamIdAllocator::new(1);
    let upstream_pool = WsMuxUpstreamPool::with_status(pool_status, "http");
    spawn_ws_upstream_pool_maintenance(upstream_pool.clone(), hub.clone(), registry.clone());

    loop {
        let (client, _client_addr) = listener.accept().await?;
        let endpoints = endpoints.to_vec();
        let identity = identity.clone();
        let http_service = http_service.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let hub = hub.clone();
        let conn_policy = conn_policy.clone();
        let upstream_pool = upstream_pool.clone();
        let stream_id = allocator.next();
        tokio::spawn(async move {
            if let Err(err) = handle_outbound_http_client_with_failover_ws(
                upstream_pool,
                identity,
                endpoints,
                conn_policy,
                http_service,
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

pub async fn handle_outbound_http_ws_client(
    peer: ws_mux::MuxWsPeer,
    local_http_service: HttpProxyService,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_http_proxy_request(&mut client).await?;
    authorize_http_proxy_request(&mut client, &local_http_service, &request).await?;
    handle_http_proxy_ws_client_inner(
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

async fn read_http_proxy_request(client: &mut TcpStream) -> Result<HttpProxyRequest, Error> {
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0_u8; 4096];
        let n = client.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "http proxy client closed before request was complete",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    parse_http_proxy_request(&buf)
}

async fn authorize_http_proxy_request(
    client: &mut TcpStream,
    service: &HttpProxyService,
    request: &HttpProxyRequest,
) -> Result<(), Error> {
    if let Err(err) = service.authorize(request.proxy_authorization.as_deref()) {
        if err.kind() == ErrorKind::PermissionDenied {
            client
                .write_all(
                    b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"fusion\"\r\nContent-Length: 0\r\n\r\n",
                )
                .await?;
            client.flush().await?;
        }
        return Err(err);
    }
    Ok(())
}

async fn handle_http_proxy_client_inner(
    peer: tcp_mux::MuxTcpPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    client: &mut TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
    request: HttpProxyRequest,
) -> Result<(), Error> {
    let mut rx = peer.open_stream_receiver(stream_id).await;
    let open_message = build_remote_stream_open_for_http_request(&remote_raw_definition, &request)?;
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

    if request.connect_tunnel {
        client
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await?;
        client.flush().await?;
    } else {
        send_stream_data_tcp(&peer, stream_id, &request.initial_payload).await?;
        let _ = write_next_stream_data_to_client(&mut rx, client, stream_id, "http").await?;
    }

    loop {
        let mut buf = [0_u8; 4096];
        let n = client.read(&mut buf).await?;
        if n == 0 {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }

        send_stream_data_tcp(&peer, stream_id, &buf[..n]).await?;
        if !write_next_stream_data_to_client(&mut rx, client, stream_id, "http").await? {
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

async fn handle_http_proxy_ws_client_inner(
    peer: ws_mux::MuxWsPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    client: &mut TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
    request: HttpProxyRequest,
) -> Result<(), Error> {
    let mut rx = peer.open_stream_receiver(stream_id).await;
    let open_message = build_remote_stream_open_for_http_request(&remote_raw_definition, &request)?;
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

    if request.connect_tunnel {
        client
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await?;
        client.flush().await?;
    } else {
        send_stream_data_ws(&peer, stream_id, &request.initial_payload).await?;
        let _ = write_next_stream_data_to_client(&mut rx, client, stream_id, "http").await?;
    }

    loop {
        let mut buf = [0_u8; 4096];
        let n = client.read(&mut buf).await?;
        if n == 0 {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }

        send_stream_data_ws(&peer, stream_id, &buf[..n]).await?;
        if !write_next_stream_data_to_client(&mut rx, client, stream_id, "http").await? {
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

async fn connect_selected_http_peer_tcp(
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

async fn connect_selected_http_peer_ws(
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

async fn handle_outbound_http_client_with_failover_tcp(
    upstream_pool: TcpMuxUpstreamPool,
    identity: AgentIdentity,
    endpoints: Vec<TunnelEndpoint>,
    conn_policy: ConnPolicy,
    proxy_chain: Vec<String>,
    local_http_service: HttpProxyService,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_http_proxy_request(&mut client).await?;
    authorize_http_proxy_request(&mut client, &local_http_service, &request).await?;
    let attempts = endpoints.len().max(1);
    let mut last_err = None;
    for _ in 0..attempts {
        let (key, peer) = match connect_selected_http_peer_tcp(
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
        match handle_http_proxy_client_inner(
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
    Err(last_err.unwrap_or_else(|| Error::other("no http tcp upstream succeeded")))
}

async fn handle_outbound_http_client_with_failover_ws(
    upstream_pool: WsMuxUpstreamPool,
    identity: AgentIdentity,
    endpoints: Vec<TunnelEndpoint>,
    conn_policy: ConnPolicy,
    local_http_service: HttpProxyService,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_http_proxy_request(&mut client).await?;
    authorize_http_proxy_request(&mut client, &local_http_service, &request).await?;
    let attempts = endpoints.len().max(1);
    let mut last_err = None;
    for _ in 0..attempts {
        let (key, peer) = match connect_selected_http_peer_ws(
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
        match handle_http_proxy_ws_client_inner(
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
    Err(last_err.unwrap_or_else(|| Error::other("no http ws upstream succeeded")))
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
