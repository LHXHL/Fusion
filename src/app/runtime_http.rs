use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    app::{
        config::TunnelEndpoint,
        runtime::build_stream_target_label,
        runtime_bridge::{
            build_stream_close_frame, build_stream_data_frame, expect_stream_close_ack,
            write_next_stream_data_to_client,
        },
    },
    protocol::{
        frame::{Frame, MessageType},
        message::Message,
    },
    serve::{
        http::{parse_http_proxy_request, HttpProxyRequest},
        service::{build_remote_stream_open_for_http_request, ServiceDefinition, ServiceKind},
    },
    session::{hub::SessionHub, stream::StreamIdAllocator},
    tunnel::{tcp_mux, ws_mux},
};

pub async fn run_outbound_http_once(
    identity: AgentIdentity,
    endpoint: &TunnelEndpoint,
    local_http_definition: ServiceDefinition,
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
    let local_addr = listener.local_addr()?;
    let allocator = StreamIdAllocator::new(1);
    println!("service.local.active=http://{}", local_addr);

    loop {
        let (client, client_addr) = listener.accept().await?;
        println!("service.local.client={} via=http", client_addr);
        let peer = peer.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let stream_id = allocator.next();
        tokio::spawn(async move {
            if let Err(err) = handle_outbound_http_client(
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

pub async fn handle_outbound_http_client(
    peer: tcp_mux::MuxTcpPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_http_proxy_request(&mut client).await?;
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
    endpoint: &TunnelEndpoint,
    local_http_definition: ServiceDefinition,
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
    let local_addr = listener.local_addr()?;
    let allocator = StreamIdAllocator::new(1);
    println!("service.local.active=http://{}", local_addr);

    loop {
        let (client, client_addr) = listener.accept().await?;
        println!("service.local.client={} via=http", client_addr);
        let peer = peer.clone();
        let remote_raw_definition = remote_raw_definition.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let stream_id = allocator.next();
        tokio::spawn(async move {
            if let Err(err) = handle_outbound_http_ws_client(
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

pub async fn handle_outbound_http_ws_client(
    peer: ws_mux::MuxWsPeer,
    remote_raw_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let request = read_http_proxy_request(&mut client).await?;
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
