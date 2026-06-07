use std::{
    io::{Error, ErrorKind},
    path::PathBuf,
    sync::Arc,
};

use chrono::Utc;
use tokio::sync::Mutex;

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    app::{
        config::{TaskRequestConfig, TunnelEndpoint},
        conn_hub::order_endpoints,
        runtime_relay::{
            handle_registry_control_message, send_direct_announce_h2_mux,
            send_direct_announce_simplex_dns, send_direct_announce_simplex_http,
            send_direct_announce_simplex_oss, send_direct_announce_tcp_mux,
            send_direct_announce_ws_mux,
        },
    },
    protocol::{
        frame::{Frame, MessageType},
        message::{Message, TaskAction, TaskRequestMessage, TaskResultMessage},
    },
    session::hub::SessionHub,
    task::{
        file_transfer::{self, default_download_path},
        interactive_shell::ShellPeer,
    },
    tunnel::{
        h2_mux, simplex_dns_mux, simplex_http_mux, simplex_oss_mux, tcp_mux, ws_mux,
    },
};

fn build_task_request_frame(
    local_agent_id: &str,
    remote_agent_id: &str,
    task_request: &TaskRequestConfig,
) -> Frame {
    Frame::new(
        MessageType::TaskRequest,
        Some(local_agent_id.to_string()),
        Some(
            task_request
                .target_agent_id
                .clone()
                .unwrap_or_else(|| remote_agent_id.to_string()),
        ),
        Message::TaskRequest(TaskRequestMessage {
            task_id: format!("task-{}", Utc::now().timestamp_millis()),
            action: task_request.action.clone(),
            args: task_request.args.clone(),
            data_hex: None,
        }),
    )
}

async fn read_task_result_tcp(
    peer: &tcp_mux::MuxTcpPeer,
    registry: &Arc<Mutex<AgentRegistry>>,
    identity: &AgentIdentity,
) -> Result<TaskResultMessage, Error> {
    let response = loop {
        let response = peer.read_control_frame().await?;
        if handle_registry_control_message(
            &mut *registry.lock().await,
            &identity.id,
            &peer.session.remote.agent_id,
            &response.message,
        ) {
            continue;
        }
        break response;
    };
    match response.message {
        Message::TaskResult(result) => Ok(result),
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected TaskResult, got {:?}", other),
        )),
    }
}

async fn read_task_result_ws(
    peer: &ws_mux::MuxWsPeer,
    registry: &Arc<Mutex<AgentRegistry>>,
    identity: &AgentIdentity,
) -> Result<TaskResultMessage, Error> {
    let response = loop {
        let response = peer.read_control_frame().await?;
        if handle_registry_control_message(
            &mut *registry.lock().await,
            &identity.id,
            &peer.session.remote.agent_id,
            &response.message,
        ) {
            continue;
        }
        break response;
    };
    match response.message {
        Message::TaskResult(result) => Ok(result),
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected TaskResult, got {:?}", other),
        )),
    }
}

async fn read_task_result_h2(
    peer: &h2_mux::MuxH2Peer,
    registry: &Arc<Mutex<AgentRegistry>>,
    identity: &AgentIdentity,
) -> Result<TaskResultMessage, Error> {
    let response = loop {
        let response = peer.read_control_frame().await?;
        if handle_registry_control_message(
            &mut *registry.lock().await,
            &identity.id,
            &peer.session.remote.agent_id,
            &response.message,
        ) {
            continue;
        }
        break response;
    };
    match response.message {
        Message::TaskResult(result) => Ok(result),
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected TaskResult, got {:?}", other),
        )),
    }
}

async fn read_task_result_simplex_dns(
    peer: &simplex_dns_mux::MuxSimplexDnsPeer,
    registry: &Arc<Mutex<AgentRegistry>>,
    identity: &AgentIdentity,
) -> Result<TaskResultMessage, Error> {
    let response = loop {
        let response = peer.read_control_frame().await?;
        if handle_registry_control_message(
            &mut *registry.lock().await,
            &identity.id,
            &peer.session.remote.agent_id,
            &response.message,
        ) {
            continue;
        }
        break response;
    };
    match response.message {
        Message::TaskResult(result) => Ok(result),
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected TaskResult, got {:?}", other),
        )),
    }
}

async fn read_task_result_simplex_http(
    peer: &simplex_http_mux::MuxSimplexHttpPeer,
    registry: &Arc<Mutex<AgentRegistry>>,
    identity: &AgentIdentity,
) -> Result<TaskResultMessage, Error> {
    let response = loop {
        let response = peer.read_control_frame().await?;
        if handle_registry_control_message(
            &mut *registry.lock().await,
            &identity.id,
            &peer.session.remote.agent_id,
            &response.message,
        ) {
            continue;
        }
        break response;
    };
    match response.message {
        Message::TaskResult(result) => Ok(result),
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected TaskResult, got {:?}", other),
        )),
    }
}

async fn read_task_result_simplex_oss(
    peer: &simplex_oss_mux::MuxSimplexOssPeer,
    registry: &Arc<Mutex<AgentRegistry>>,
    identity: &AgentIdentity,
) -> Result<TaskResultMessage, Error> {
    let response = loop {
        let response = peer.read_control_frame().await?;
        if handle_registry_control_message(
            &mut *registry.lock().await,
            &identity.id,
            &peer.session.remote.agent_id,
            &response.message,
        ) {
            continue;
        }
        break response;
    };
    match response.message {
        Message::TaskResult(result) => Ok(result),
        other => Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected TaskResult, got {:?}", other),
        )),
    }
}

async fn start_file_transfer(
    result: TaskResultMessage,
    peer: ShellPeer,
    destination_agent_id: String,
    task_request: &TaskRequestConfig,
    data_dir: &PathBuf,
) -> Result<TaskResultMessage, Error> {
    if !result.ok {
        return Ok(result);
    }
    let remote_path = task_request
        .args
        .first()
        .cloned()
        .unwrap_or_default();
    match &task_request.action {
        TaskAction::FileDownload => {
            let save_path = task_request.save_path.clone().unwrap_or_else(|| {
                default_download_path(data_dir, &result.task_id, &remote_path)
            });
            let bytes =
                file_transfer::begin_client_download(peer, &result.task_id, &destination_agent_id, save_path.clone())
                    .await?;
            Ok(TaskResultMessage {
                task_id: result.task_id,
                ok: true,
                output: format!("downloaded {} bytes to {}", bytes, save_path.display()),
                data_hex: None,
                stream_id: None,
            })
        }
        TaskAction::FileUpload => {
            let local_path = task_request.local_path.clone().ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing local upload path")
            })?;
            let bytes = file_transfer::begin_client_upload(
                peer,
                &result.task_id,
                &destination_agent_id,
                local_path.clone(),
            )
            .await?;
            Ok(TaskResultMessage {
                task_id: result.task_id,
                ok: true,
                output: format!(
                    "uploaded {} bytes from {} to {}",
                    bytes,
                    local_path.display(),
                    remote_path
                ),
                data_hex: None,
                stream_id: None,
            })
        }
        other => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unsupported file transfer action {:?}", other),
        )),
    }
}

pub async fn run_outbound_file_transfer_once_with_result(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    task_request: TaskRequestConfig,
    data_dir: PathBuf,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: crate::app::config::ConnPolicy,
    proxy_chain: Vec<String>,
) -> Result<TaskResultMessage, Error> {
    let ordered = order_endpoints(endpoints, &conn_policy)?;
    let mut last_err = None;
    for endpoint in ordered {
        if !file_transfer::mux_stream_transport_supported(&endpoint.url.scheme) {
            last_err = Some(Error::new(
                ErrorKind::Unsupported,
                format!(
                    "file transfer requires mux transport, got `{}`",
                    endpoint.url.scheme
                ),
            ));
            continue;
        }

        let result = match endpoint.url.scheme.as_str() {
            "tcp" => {
                async {
                    let connect_host = endpoint.url.host.clone().ok_or_else(|| {
                        Error::new(ErrorKind::InvalidInput, "missing host for tcp connect")
                    })?;
                    let connect_port = endpoint.url.port.ok_or_else(|| {
                        Error::new(ErrorKind::InvalidInput, "missing port for tcp connect")
                    })?;
                    let connect_addr = format!("{}:{}", connect_host, connect_port);
                    let peer = tcp_mux::connect_mux_peer_via_proxy_chain(
                        identity.clone(),
                        &connect_addr,
                        &proxy_chain,
                    )
                    .await?;
                    hub.lock().await.upsert(peer.session.clone());
                    registry.lock().await.upsert_peer(peer.session.clone());
                    send_direct_announce_tcp_mux(&peer, &identity, &local_services).await?;
                    let frame = build_task_request_frame(
                        &peer.session.local.agent_id,
                        &peer.session.remote.agent_id,
                        &task_request,
                    );
                    peer.send_frame(&frame).await?;
                    let result = read_task_result_tcp(&peer, &registry, &identity).await?;
                    let destination = task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    start_file_transfer(
                        result,
                        ShellPeer::Tcp(peer),
                        destination,
                        &task_request,
                        &data_dir,
                    )
                    .await
                }
                .await
            }
            "ws" | "wss" => {
                async {
                    let peer =
                        ws_mux::connect_mux_peer(identity.clone(), &endpoint.url.original).await?;
                    hub.lock().await.upsert(peer.session.clone());
                    registry.lock().await.upsert_peer(peer.session.clone());
                    send_direct_announce_ws_mux(&peer, &identity, &local_services).await?;
                    let frame = build_task_request_frame(
                        &peer.session.local.agent_id,
                        &peer.session.remote.agent_id,
                        &task_request,
                    );
                    peer.send_frame(&frame).await?;
                    let result = read_task_result_ws(&peer, &registry, &identity).await?;
                    let destination = task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    start_file_transfer(
                        result,
                        ShellPeer::Ws(peer),
                        destination,
                        &task_request,
                        &data_dir,
                    )
                    .await
                }
                .await
            }
            scheme if crate::tunnel::h2_tunnel::is_h2_tunnel_scheme(scheme) => {
                async {
                    let peer =
                        h2_mux::connect_mux_peer(identity.clone(), &endpoint.url.original).await?;
                    hub.lock().await.upsert(peer.session.clone());
                    registry.lock().await.upsert_peer(peer.session.clone());
                    send_direct_announce_h2_mux(&peer, &identity, &local_services).await?;
                    let frame = build_task_request_frame(
                        &peer.session.local.agent_id,
                        &peer.session.remote.agent_id,
                        &task_request,
                    );
                    peer.send_frame(&frame).await?;
                    let result = read_task_result_h2(&peer, &registry, &identity).await?;
                    let destination = task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    start_file_transfer(
                        result,
                        ShellPeer::H2(peer),
                        destination,
                        &task_request,
                        &data_dir,
                    )
                    .await
                }
                .await
            }
            scheme if crate::tunnel::dns_tunnel::is_dns_tunnel_scheme(scheme) => {
                async {
                    let peer = simplex_dns_mux::connect_mux_peer(
                        identity.clone(),
                        &endpoint.url.original,
                    )
                    .await?;
                    hub.lock().await.upsert(peer.session.clone());
                    registry.lock().await.upsert_peer(peer.session.clone());
                    send_direct_announce_simplex_dns(&peer.inner, &identity, &local_services).await?;
                    let frame = build_task_request_frame(
                        &peer.session.local.agent_id,
                        &peer.session.remote.agent_id,
                        &task_request,
                    );
                    peer.send_frame(&frame).await?;
                    let result =
                        read_task_result_simplex_dns(&peer, &registry, &identity).await?;
                    let destination = task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    start_file_transfer(
                        result,
                        ShellPeer::SimplexDns(peer),
                        destination,
                        &task_request,
                        &data_dir,
                    )
                    .await
                }
                .await
            }
            scheme if crate::tunnel::http_poll::is_http_poll_scheme(scheme) => {
                async {
                    let peer = simplex_http_mux::connect_mux_peer(
                        identity.clone(),
                        &endpoint.url.original,
                    )
                    .await?;
                    hub.lock().await.upsert(peer.session.clone());
                    registry.lock().await.upsert_peer(peer.session.clone());
                    send_direct_announce_simplex_http(&peer.inner, &identity, &local_services).await?;
                    let frame = build_task_request_frame(
                        &peer.session.local.agent_id,
                        &peer.session.remote.agent_id,
                        &task_request,
                    );
                    peer.send_frame(&frame).await?;
                    let result =
                        read_task_result_simplex_http(&peer, &registry, &identity).await?;
                    let destination = task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    start_file_transfer(
                        result,
                        ShellPeer::SimplexHttp(peer),
                        destination,
                        &task_request,
                        &data_dir,
                    )
                    .await
                }
                .await
            }
            "simplex+oss" => {
                async {
                    let peer = simplex_oss_mux::connect_mux_peer(
                        identity.clone(),
                        &endpoint.url.original,
                    )
                    .await?;
                    hub.lock().await.upsert(peer.session.clone());
                    registry.lock().await.upsert_peer(peer.session.clone());
                    send_direct_announce_simplex_oss(&peer.inner, &identity, &local_services).await?;
                    let frame = build_task_request_frame(
                        &peer.session.local.agent_id,
                        &peer.session.remote.agent_id,
                        &task_request,
                    );
                    peer.send_frame(&frame).await?;
                    let result =
                        read_task_result_simplex_oss(&peer, &registry, &identity).await?;
                    let destination = task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    start_file_transfer(
                        result,
                        ShellPeer::SimplexOss(peer),
                        destination,
                        &task_request,
                        &data_dir,
                    )
                    .await
                }
                .await
            }
            other => Err(Error::new(
                ErrorKind::Unsupported,
                format!("file transfer over `{other}` is not supported"),
            )),
        };

        match result {
            Ok(task_result) => return Ok(task_result),
            Err(err) => last_err = Some(err),
        }
    }

    Err(last_err.unwrap_or_else(|| Error::other("no outbound file transfer endpoint succeeded")))
}

pub async fn run_outbound_file_transfer_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    task_request: TaskRequestConfig,
    data_dir: PathBuf,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: crate::app::config::ConnPolicy,
    proxy_chain: Vec<String>,
) -> Result<(), Error> {
    let result = run_outbound_file_transfer_once_with_result(
        identity,
        endpoints,
        task_request,
        data_dir,
        local_services,
        hub,
        registry,
        conn_policy,
        proxy_chain,
    )
    .await?;
    if !result.ok {
        return Err(Error::other(result.output));
    }
    println!("{}", result.output);
    Ok(())
}
