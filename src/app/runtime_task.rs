use std::{
    io::{Error, ErrorKind},
    path::PathBuf,
    sync::Arc,
};

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    app::{
        config::{TaskRequestConfig, TunnelEndpoint},
        runtime_relay::{
            handle_registry_control_message, send_direct_announce_tcp_mux,
            send_direct_announce_ws_mux,
        },
    },
    protocol::{
        frame::{Frame, MessageType},
        message::{Message, TaskAction, TaskRequestMessage, TaskResultMessage},
    },
    session::hub::SessionHub,
    tunnel::{tcp_mux, ws_mux},
};
use chrono::Utc;
use tokio::sync::Mutex;

pub fn default_artifact_extension(task_request: &TaskRequestConfig) -> &'static str {
    match task_request.action {
        TaskAction::Screenshot => "png",
        TaskAction::FileDownload => "bin",
        TaskAction::Shell => "txt",
        TaskAction::FileUpload => "txt",
    }
}

pub async fn maybe_store_task_artifact(
    data_dir: &PathBuf,
    task_request: &TaskRequestConfig,
    result: &TaskResultMessage,
) -> Result<Option<PathBuf>, Error> {
    let Some(data_hex) = result.data_hex.as_ref() else {
        return Ok(None);
    };
    let bytes = data_encoding::HEXLOWER
        .decode(data_hex.as_bytes())
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
    let path = if let Some(save_path) = task_request.save_path.as_ref() {
        save_path.clone()
    } else {
        let dir = data_dir.join("tasks");
        tokio::fs::create_dir_all(&dir).await?;
        dir.join(format!(
            "{}.{}",
            result.task_id,
            default_artifact_extension(task_request)
        ))
    };
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }
    tokio::fs::write(&path, &bytes).await?;
    Ok(Some(path))
}

fn print_task_result_summary(result: &TaskResultMessage) {
    println!("task.result.id={}", result.task_id);
    println!("task.result.ok={}", result.ok);
    println!("task.result.output={}", result.output.trim_end());
    if let Some(data_hex) = result.data_hex.as_ref() {
        println!("task.result.data_hex_len={}", data_hex.len());
    }
}

pub async fn run_outbound_task_once(
    identity: AgentIdentity,
    endpoint: &TunnelEndpoint,
    task_request: TaskRequestConfig,
    data_dir: PathBuf,
    local_services: Vec<String>,
    hub: Arc<Mutex<SessionHub>>,
    registry: Arc<Mutex<AgentRegistry>>,
) -> Result<(), Error> {
    match endpoint.url.scheme.as_str() {
        "tcp" => {
            let connect_host = endpoint.url.host.clone().ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing host for tcp connect")
            })?;
            let connect_port = endpoint.url.port.ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing port for tcp connect")
            })?;
            let connect_addr = format!("{}:{}", connect_host, connect_port);
            let peer = tcp_mux::connect_mux_peer(identity.clone(), &connect_addr).await?;
            hub.lock().await.upsert(peer.session.clone());
            registry.lock().await.upsert_peer(peer.session.clone());
            println!(
                "session.outbound.peer={} to={} via=tcp",
                peer.session.remote.agent_id, connect_addr
            );
            send_direct_announce_tcp_mux(&peer, &identity, &local_services).await?;

            let frame = Frame::new(
                MessageType::TaskRequest,
                Some(peer.session.local.agent_id.clone()),
                Some(
                    task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone()),
                ),
                Message::TaskRequest(TaskRequestMessage {
                    task_id: format!("task-{}", Utc::now().timestamp_millis()),
                    action: task_request.action.clone(),
                    args: task_request.args.clone(),
                    data_hex: task_request.data_hex.clone(),
                }),
            );
            peer.send_frame(&frame).await?;
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
                Message::TaskResult(result) => {
                    print_task_result_summary(&result);
                    if let Some(path) =
                        maybe_store_task_artifact(&data_dir, &task_request, &result).await?
                    {
                        println!("task.result.artifact={}", path.display());
                    }
                    Ok(())
                }
                other => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("expected TaskResult, got {:?}", other),
                )),
            }
        }
        "ws" | "wss" => {
            let peer = ws_mux::connect_mux_peer(identity.clone(), &endpoint.url.original).await?;
            hub.lock().await.upsert(peer.session.clone());
            registry.lock().await.upsert_peer(peer.session.clone());
            println!(
                "session.outbound.peer={} to={} via=ws",
                peer.session.remote.agent_id, endpoint.url.original
            );
            send_direct_announce_ws_mux(&peer, &identity, &local_services).await?;

            let frame = Frame::new(
                MessageType::TaskRequest,
                Some(peer.session.local.agent_id.clone()),
                Some(
                    task_request
                        .target_agent_id
                        .clone()
                        .unwrap_or_else(|| peer.session.remote.agent_id.clone()),
                ),
                Message::TaskRequest(TaskRequestMessage {
                    task_id: format!("task-{}", Utc::now().timestamp_millis()),
                    action: task_request.action.clone(),
                    args: task_request.args.clone(),
                    data_hex: task_request.data_hex.clone(),
                }),
            );
            peer.send_frame(&frame).await?;
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
                Message::TaskResult(result) => {
                    print_task_result_summary(&result);
                    if let Some(path) =
                        maybe_store_task_artifact(&data_dir, &task_request, &result).await?
                    {
                        println!("task.result.artifact={}", path.display());
                    }
                    Ok(())
                }
                other => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("expected TaskResult, got {:?}", other),
                )),
            }
        }
        other => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("task request over scheme `{}` not implemented yet", other),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{default_artifact_extension, maybe_store_task_artifact};
    use crate::{
        app::config::TaskRequestConfig,
        protocol::message::{TaskAction, TaskResultMessage},
    };

    #[test]
    fn artifact_extension_matches_task_action() {
        let request = TaskRequestConfig {
            action: TaskAction::Screenshot,
            args: vec![],
            data_hex: None,
            save_path: None,
            target_agent_id: None,
        };
        assert_eq!(default_artifact_extension(&request), "png");
    }

    #[tokio::test]
    async fn stores_task_artifact_in_default_tasks_dir() {
        let temp_dir = std::env::temp_dir().join(format!(
            "fusion-task-artifact-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let request = TaskRequestConfig {
            action: TaskAction::Shell,
            args: vec![],
            data_hex: None,
            save_path: None,
            target_agent_id: None,
        };
        let result = TaskResultMessage {
            task_id: "task-1".into(),
            ok: true,
            output: String::new(),
            data_hex: Some(data_encoding::HEXLOWER.encode(b"hello")),
        };
        let path = maybe_store_task_artifact(&temp_dir, &request, &result)
            .await
            .unwrap()
            .unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert_eq!(bytes, b"hello");
        let _ = tokio::fs::remove_file(&path).await;
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}
