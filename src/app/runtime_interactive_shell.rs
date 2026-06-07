use std::{
    io::{Error, ErrorKind},
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
            send_direct_announce_tcp_mux, send_direct_announce_ws_mux,
        },
    },
    protocol::{
        frame::{Frame, MessageType},
        message::{Message, TaskRequestMessage, TaskResultMessage},
    },
    session::hub::SessionHub,
    task::interactive_shell::{self, ShellPeer},
    tunnel::{h2_mux, tcp_mux, ws_mux},
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
            data_hex: task_request.data_hex.clone(),
        }),
    )
}

async fn read_task_result_from_control(
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

async fn read_task_result_from_control_ws(
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

async fn read_task_result_from_control_h2(
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

async fn start_interactive_session(
    result: TaskResultMessage,
    peer: ShellPeer,
    destination_agent_id: String,
) -> Result<(), Error> {
    if !result.ok {
        return Err(Error::other(result.output));
    }
    interactive_shell::begin_client_session(peer, &result.task_id, &destination_agent_id).await
}

pub async fn run_outbound_interactive_shell_once(
    identity: AgentIdentity,
    endpoints: &[TunnelEndpoint],
    task_request: TaskRequestConfig,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
    conn_policy: crate::app::config::ConnPolicy,
    proxy_chain: Vec<String>,
) -> Result<(), Error> {
    let ordered = order_endpoints(endpoints, &conn_policy)?;
    let mut last_err = None;
    for endpoint in ordered {
        if !interactive_shell::mux_transport_supported(&endpoint.url.scheme) {
            last_err = Some(Error::new(
                ErrorKind::Unsupported,
                format!(
                    "interactive shell requires mux transport (tcp/ws/h2), got `{}`",
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
                    let result =
                        read_task_result_from_control(&peer, &registry, &identity).await?;
                    let destination = task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    start_interactive_session(result, ShellPeer::Tcp(peer), destination).await
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
                    let result =
                        read_task_result_from_control_ws(&peer, &registry, &identity).await?;
                    let destination = task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    start_interactive_session(result, ShellPeer::Ws(peer), destination).await
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
                    let result =
                        read_task_result_from_control_h2(&peer, &registry, &identity).await?;
                    let destination = task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone());
                    start_interactive_session(result, ShellPeer::H2(peer), destination).await
                }
                .await
            }
            other => Err(Error::new(
                ErrorKind::Unsupported,
                format!("interactive shell over `{other}` is not supported"),
            )),
        };

        match result {
            Ok(()) => return Ok(()),
            Err(err) => last_err = Some(err),
        }
    }

    Err(last_err.unwrap_or_else(|| Error::other("no outbound interactive shell endpoint succeeded")))
}
