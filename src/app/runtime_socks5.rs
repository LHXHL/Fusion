use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    app::{config::TunnelEndpoint, runtime::build_stream_target_label},
    protocol::{
        frame::{Frame, MessageType},
        message::{Message, StreamCloseMessage, StreamDataMessage},
    },
    serve::{
        service::{build_remote_stream_open_for_request, ServiceDefinition, ServiceKind},
        socks5::{accept_no_auth, read_connect_request, write_success_response},
    },
    session::{hub::SessionHub, stream::StreamIdAllocator},
    tunnel::{tcp_mux, ws_mux},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

pub async fn run_outbound_socks5_once(
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

pub async fn handle_outbound_socks5_client(
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

pub async fn run_outbound_socks5_ws_once(
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

pub async fn handle_outbound_socks5_ws_client(
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
