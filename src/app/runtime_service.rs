use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    serve::{
        portfwd::{proxy_connection as proxy_port_forward_connection, PortForwardService},
        raw::{
            proxy_mux_stream_loop, proxy_simplex_mux_stream_loop,
            proxy_simplex_dns_mux_stream_loop, proxy_simplex_oss_mux_stream_loop,
            proxy_ws_mux_stream_loop,
        },
        service::{ServiceDefinition, ServiceKind},
    },
    session::hub::SessionHub,
    tunnel::{simplex_dns_mux, simplex_http_mux, simplex_oss_mux, tcp_mux, ws_mux},
};
use tokio::{net::TcpListener, sync::Mutex};

fn resolve_stream_target_label(host: &Option<String>, port: Option<u16>, fallback: &str) -> String {
    match (host.as_deref(), port) {
        (Some(host), Some(port)) => format!("{}:{}", host, port),
        _ => fallback.to_string(),
    }
}

pub async fn run_remote_port_forward_listener(
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

pub async fn run_inbound_raw_once(
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
        let target = resolve_stream_target_label(
            &open.target_host,
            open.target_port,
            &raw_service.target_label(),
        );
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

pub async fn run_inbound_raw_ws_once(
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
        let target = resolve_stream_target_label(
            &open.target_host,
            open.target_port,
            &raw_service.target_label(),
        );
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

pub async fn run_inbound_raw_simplex_once(
    identity: AgentIdentity,
    listener: TcpListener,
    path: &str,
    raw_service_definition: ServiceDefinition,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let peer = simplex_http_mux::accept_mux_peer_on(identity, listener, path).await?;
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    println!(
        "session.inbound.peer={} via=simplex-http",
        peer.session.remote.agent_id
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
        let target = resolve_stream_target_label(
            &open.target_host,
            open.target_port,
            &raw_service.target_label(),
        );
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
            if let Err(err) = proxy_simplex_mux_stream_loop(
                peer_handle,
                &raw_service,
                open,
                stream_id,
                stream_rx,
            )
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

pub async fn run_inbound_raw_simplex_oss_once(
    identity: AgentIdentity,
    endpoint: &str,
    raw_service_definition: ServiceDefinition,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let peer = simplex_oss_mux::accept_mux_peer_on(identity, endpoint).await?;
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    println!(
        "session.inbound.peer={} via=simplex-oss",
        peer.session.remote.agent_id
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
        let target = resolve_stream_target_label(
            &open.target_host,
            open.target_port,
            &raw_service.target_label(),
        );
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
            if let Err(err) = proxy_simplex_oss_mux_stream_loop(
                peer_handle,
                &raw_service,
                open,
                stream_id,
                stream_rx,
            )
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

pub async fn run_inbound_raw_simplex_dns_once(
    identity: AgentIdentity,
    socket: tokio::net::UdpSocket,
    path: &str,
    raw_service_definition: ServiceDefinition,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    let peer = simplex_dns_mux::accept_mux_peer_on(identity, socket, path).await?;
    hub.lock().await.upsert(peer.session.clone());
    registry.lock().await.upsert_peer(peer.session.clone());
    println!(
        "session.inbound.peer={} via=simplex-dns",
        peer.session.remote.agent_id
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
        let target = resolve_stream_target_label(
            &open.target_host,
            open.target_port,
            &raw_service.target_label(),
        );
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
            if let Err(err) = proxy_simplex_dns_mux_stream_loop(
                peer_handle,
                &raw_service,
                open,
                stream_id,
                stream_rx,
            )
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
