use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::{sleep, Duration},
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    app::{
        config::{ConnPolicy, TunnelEndpoint},
        runtime::build_stream_target_label,
        runtime_bridge::{
            build_stream_close_frame, build_stream_data_frame, expect_stream_close_ack,
        },
        runtime_status::UpstreamPoolStatusMap,
        upstream_pool::{TcpMuxUpstreamPool, WsMuxUpstreamPool},
    },
    protocol::{
        frame::{Frame, MessageType},
        message::Message,
    },
    serve::{
        service::{build_remote_stream_open_for_target, ServiceDefinition, ServiceKind},
        trojan::{parse_trojan_request, TrojanRequest, TrojanService},
    },
    session::{hub::SessionHub, stream::StreamIdAllocator},
    tunnel::{tcp_mux, tls, ws_mux},
    utils::url::ParsedUrl,
};

pub(crate) enum TrojanClientStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

#[cfg(test)]
pub(crate) fn trojan_plain_client(stream: TcpStream) -> TrojanClientStream {
    TrojanClientStream::Plain(stream)
}

impl AsyncRead for TrojanClientStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
        }
    }
}

async fn read_trojan_request(
    client: &mut TrojanClientStream,
    service: &TrojanService,
) -> Result<TrojanRequest, Error> {
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0_u8; 4096];
        let n = client.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "trojan client closed before request was complete",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(request) = parse_trojan_request(&buf, &service.password_hash)? {
            return Ok(request);
        }
    }
}

pub async fn run_outbound_trojan_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    local_trojan_definition: ServiceDefinition,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: ConnPolicy,
    proxy_chain: Vec<String>,
    pool_status: UpstreamPoolStatusMap,
) -> Result<(), Error> {
    let (trojan_service, tls_acceptor) = trojan_listener_config(&local_trojan_definition)?;
    let listener = TcpListener::bind(trojan_service.bind_label()).await?;
    let _local_addr = listener.local_addr()?;
    let allocator = StreamIdAllocator::new(1);
    let upstream_pool = TcpMuxUpstreamPool::with_status(pool_status, "trojan");
    spawn_tcp_upstream_pool_maintenance(upstream_pool.clone(), hub.clone(), registry.clone());

    loop {
        let (client, _client_addr) = listener.accept().await?;
        let endpoints = endpoints.to_vec();
        let identity = identity.clone();
        let trojan_service = trojan_service.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let hub = hub.clone();
        let conn_policy = conn_policy.clone();
        let proxy_chain = proxy_chain.clone();
        let upstream_pool = upstream_pool.clone();
        let stream_id = allocator.next();
        let tls_acceptor = tls_acceptor.clone();
        tokio::spawn(async move {
            let mut client = match accept_trojan_client(client, tls_acceptor).await {
                Ok(stream) => stream,
                Err(err) => {
                    eprintln!("service.local.client.error={} stream_id={}", err, stream_id);
                    return;
                }
            };
            if let Err(err) = handle_outbound_trojan_client_with_failover_tcp(
                upstream_pool,
                identity,
                endpoints,
                conn_policy,
                proxy_chain,
                trojan_service,
                remote_raw_definition,
                remote_peer_id,
                &mut client,
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

pub async fn run_outbound_trojan_ws_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    local_trojan_definition: ServiceDefinition,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: ConnPolicy,
    pool_status: UpstreamPoolStatusMap,
) -> Result<(), Error> {
    let (trojan_service, tls_acceptor) = trojan_listener_config(&local_trojan_definition)?;
    let listener = TcpListener::bind(trojan_service.bind_label()).await?;
    let _local_addr = listener.local_addr()?;
    let allocator = StreamIdAllocator::new(1);
    let upstream_pool = WsMuxUpstreamPool::with_status(pool_status, "trojan");
    spawn_ws_upstream_pool_maintenance(upstream_pool.clone(), hub.clone(), registry.clone());

    loop {
        let (client, _client_addr) = listener.accept().await?;
        let endpoints = endpoints.to_vec();
        let identity = identity.clone();
        let trojan_service = trojan_service.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let hub = hub.clone();
        let conn_policy = conn_policy.clone();
        let upstream_pool = upstream_pool.clone();
        let stream_id = allocator.next();
        let tls_acceptor = tls_acceptor.clone();
        tokio::spawn(async move {
            let mut client = match accept_trojan_client(client, tls_acceptor).await {
                Ok(stream) => stream,
                Err(err) => {
                    eprintln!("service.local.client.error={} stream_id={}", err, stream_id);
                    return;
                }
            };
            if let Err(err) = handle_outbound_trojan_client_with_failover_ws(
                upstream_pool,
                identity,
                endpoints,
                conn_policy,
                trojan_service,
                remote_raw_definition,
                remote_peer_id,
                &mut client,
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

fn trojan_listener_config(
    definition: &ServiceDefinition,
) -> Result<(TrojanService, Option<TlsAcceptor>), Error> {
    let service = match &definition.kind {
        ServiceKind::LocalTrojan(service) => service.clone(),
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "outbound trojan handler requires local trojan service",
            ))
        }
    };
    let url = ParsedUrl::parse(&definition.original)?;
    let tls_acceptor = if service.tls_enabled {
        tls::build_optional_tls_acceptor(&url)?
    } else {
        None
    };
    Ok((service, tls_acceptor))
}

async fn accept_trojan_client(
    stream: TcpStream,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<TrojanClientStream, Error> {
    if let Some(acceptor) = tls_acceptor {
        let tls_stream = acceptor.accept(stream).await.map_err(|err| {
            Error::new(
                ErrorKind::InvalidData,
                format!("trojan tls handshake failed: {err}"),
            )
        })?;
        Ok(TrojanClientStream::Tls(tls_stream))
    } else {
        Ok(TrojanClientStream::Plain(stream))
    }
}

#[cfg(test)]
pub(crate) async fn handle_outbound_trojan_client(
    peer: tcp_mux::MuxTcpPeer,
    trojan_service: TrojanService,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    client: &mut TrojanClientStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_trojan_request(client, &trojan_service).await?;
    handle_trojan_client_inner_tcp(
        peer,
        remote_raw_definition,
        remote_peer_id,
        client,
        stream_id,
        registry,
        request,
    )
    .await
}

async fn handle_trojan_client_inner_tcp(
    peer: tcp_mux::MuxTcpPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    client: &mut TrojanClientStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
    request: TrojanRequest,
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
        let _ = write_next_stream_data_to_client(&mut rx, client, stream_id, "trojan").await?;
    }

    loop {
        let mut buf = [0_u8; 4096];
        let n = client.read(&mut buf).await?;
        if n == 0 {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }

        send_stream_data_tcp(&peer, stream_id, &buf[..n]).await?;
        if !write_next_stream_data_to_client(&mut rx, client, stream_id, "trojan").await? {
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

async fn handle_trojan_client_inner_ws(
    peer: ws_mux::MuxWsPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    client: &mut TrojanClientStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
    request: TrojanRequest,
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
        let _ = write_next_stream_data_to_client(&mut rx, client, stream_id, "trojan").await?;
    }

    loop {
        let mut buf = [0_u8; 4096];
        let n = client.read(&mut buf).await?;
        if n == 0 {
            registry.lock().await.mark_stream_closing(stream_id);
            break;
        }

        send_stream_data_ws(&peer, stream_id, &buf[..n]).await?;
        if !write_next_stream_data_to_client(&mut rx, client, stream_id, "trojan").await? {
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

async fn handle_outbound_trojan_client_with_failover_tcp(
    upstream_pool: TcpMuxUpstreamPool,
    identity: AgentIdentity,
    endpoints: Vec<TunnelEndpoint>,
    conn_policy: ConnPolicy,
    proxy_chain: Vec<String>,
    trojan_service: TrojanService,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    client: &mut TrojanClientStream,
    stream_id: u32,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_trojan_request(client, &trojan_service).await?;
    let attempts = endpoints.len().max(1);
    let mut last_err = None;
    for _ in 0..attempts {
        let (key, peer) = match upstream_pool
            .acquire(
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
        match handle_trojan_client_inner_tcp(
            peer,
            remote_raw_definition.clone(),
            remote_peer_id.clone(),
            client,
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
    Err(last_err.unwrap_or_else(|| Error::other("no trojan tcp upstream succeeded")))
}

async fn handle_outbound_trojan_client_with_failover_ws(
    upstream_pool: WsMuxUpstreamPool,
    identity: AgentIdentity,
    endpoints: Vec<TunnelEndpoint>,
    conn_policy: ConnPolicy,
    trojan_service: TrojanService,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    client: &mut TrojanClientStream,
    stream_id: u32,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_trojan_request(client, &trojan_service).await?;
    let attempts = endpoints.len().max(1);
    let mut last_err = None;
    for _ in 0..attempts {
        let (key, peer) = match upstream_pool
            .acquire(identity.clone(), &endpoints, &conn_policy, &hub, &registry)
            .await
        {
            Ok(v) => v,
            Err(err) => {
                last_err = Some(err);
                continue;
            }
        };
        match handle_trojan_client_inner_ws(
            peer,
            remote_raw_definition.clone(),
            remote_peer_id.clone(),
            client,
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
    Err(last_err.unwrap_or_else(|| Error::other("no trojan ws upstream succeeded")))
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

async fn write_next_stream_data_to_client(
    rx: &mut tokio::sync::mpsc::Receiver<Frame>,
    client: &mut TrojanClientStream,
    stream_id: u32,
    context: &str,
) -> Result<bool, Error> {
    use tokio::io::AsyncWriteExt;
    let response = rx
        .recv()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "stream receiver closed"))?;
    if response.header.stream_id != Some(stream_id) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("unexpected stream_id on {context} response"),
        ));
    }
    match response.message {
        Message::StreamData(data) => {
            let bytes = data.to_bytes()?;
            match client {
                TrojanClientStream::Plain(stream) => stream.write_all(&bytes).await?,
                TrojanClientStream::Tls(stream) => stream.write_all(&bytes).await?,
            }
            Ok(true)
        }
        Message::StreamClose(_) => Ok(false),
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected StreamData response, got {:?}", other),
        )),
    }
}
