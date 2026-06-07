use std::{
    io::Error,
    sync::Arc,
};

use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    sync::Mutex,
};

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    app::{
        runtime::build_stream_target_label,
        config::{ConnPolicy, TunnelEndpoint},
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
        portfwd::PortForwardService,
        service::{build_remote_stream_open, ServiceDefinition, ServiceKind},
    },
    session::{hub::SessionHub, stream::StreamIdAllocator},
    tunnel::simplex_http_mux,
};

async fn connect_simplex_http_peer(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    conn_policy: &ConnPolicy,
    hub: &Arc<Mutex<SessionHub>>,
    registry: &Arc<Mutex<AgentRegistry>>,
) -> Result<(String, simplex_http_mux::MuxSimplexHttpPeer), Error> {
    let ordered = crate::app::conn_hub::order_endpoints(endpoints, conn_policy)?;
    let mut last_err = None;
    for endpoint in ordered {
        match simplex_http_mux::connect_mux_peer(identity.clone(), &endpoint.url.original).await {
            Ok(peer) => {
                hub.lock().await.upsert(peer.session.clone());
                registry.lock().await.upsert_peer(peer.session.clone());
                return Ok((endpoint.url.original.clone(), peer));
            }
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err.unwrap_or_else(|| Error::other("no simplex-http upstream endpoint succeeded")))
}

pub async fn handle_port_forward_simplex_http_client(
    peer: simplex_http_mux::MuxSimplexHttpPeer,
    port_forward: PortForwardService,
    remote_peer_id: Option<String>,
    client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let remote_definition = ServiceDefinition {
        original: format!("port://{}", port_forward.summary_label()),
        kind: ServiceKind::RemotePortForward(port_forward),
    };
    process_port_forward_simplex_http_connection(
        peer,
        remote_definition,
        remote_peer_id,
        client,
        stream_id,
        registry,
    )
    .await
}

async fn process_port_forward_simplex_http_connection(
    peer: simplex_http_mux::MuxSimplexHttpPeer,
    remote_definition: ServiceDefinition,
    remote_peer_id: Option<String>,
    mut client: TcpStream,
    stream_id: u32,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let mut rx = peer.open_stream_receiver(stream_id).await;
    let open_message = build_remote_stream_open(&remote_definition)?;
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
        if !write_next_stream_data_to_client(&mut rx, &mut client, stream_id, "port-forward").await?
        {
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

async fn handle_port_forward_client_with_failover(
    identity: AgentIdentity,
    endpoints: Vec<TunnelEndpoint>,
    conn_policy: ConnPolicy,
    port_forward: PortForwardService,
    remote_peer_id: Option<String>,
    client: TcpStream,
    stream_id: u32,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let remote_definition = ServiceDefinition {
        original: format!("port://{}", port_forward.summary_label()),
        kind: ServiceKind::RemotePortForward(port_forward),
    };
    let (_key, peer) = connect_simplex_http_peer(
        identity,
        &endpoints,
        &conn_policy,
        &hub,
        &registry,
    )
    .await?;
    process_port_forward_simplex_http_connection(
        peer,
        remote_definition,
        remote_peer_id,
        client,
        stream_id,
        registry,
    )
    .await
}

pub async fn run_outbound_port_forward_simplex_http_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    port_forward: PortForwardService,
    remote_peer_id: Option<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: ConnPolicy,
) -> Result<(), Error> {
    let listener = port_forward.bind_listener().await?;
    let _local_addr = listener.local_addr()?;
    let allocator = StreamIdAllocator::new(1);

    loop {
        let (client, _client_addr) = listener.accept().await?;
        let endpoints = endpoints.to_vec();
        let identity = identity.clone();
        let port_forward = port_forward.clone();
        let remote_peer_id = remote_peer_id.clone();
        let registry = registry.clone();
        let hub = hub.clone();
        let conn_policy = conn_policy.clone();
        let stream_id = allocator.next();
        tokio::spawn(async move {
            if let Err(err) = handle_port_forward_client_with_failover(
                identity,
                endpoints,
                conn_policy,
                port_forward,
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
