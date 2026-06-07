use std::io::{Error, ErrorKind};

use data_encoding::HEXLOWER;

use crate::{
    agent::capabilities::CapabilityRegistry,
    protocol::message::{TaskAction, TaskRequestMessage, TaskResultMessage},
    task::{screenshot, shell},
};

pub async fn dispatch(request: &TaskRequestMessage) -> TaskResultMessage {
    match dispatch_inner(request).await {
        Ok(result) => result,
        Err(err) => TaskResultMessage {
            task_id: request.task_id.clone(),
            ok: false,
            output: err.to_string(),
            data_hex: None,
            stream_id: None,
        },
    }
}

async fn dispatch_inner(request: &TaskRequestMessage) -> Result<TaskResultMessage, Error> {
    let registry =
        CapabilityRegistry::default_for_platform(std::env::consts::OS, std::env::consts::ARCH);
    if !registry.supports_task_action(&request.action) {
        return Err(Error::new(
            ErrorKind::Unsupported,
            format!(
                "task action {:?} is not enabled on this agent",
                request.action
            ),
        ));
    }

    match request.action {
        TaskAction::Shell => {
            let command = request
                .args
                .first()
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "missing shell command"))?;
            let output = shell::execute(command).await?;
            Ok(TaskResultMessage {
                task_id: request.task_id.clone(),
                ok: true,
                output: String::from_utf8_lossy(&output).to_string(),
                data_hex: Some(HEXLOWER.encode(&output)),
                stream_id: None,
            })
        }
        TaskAction::Screenshot => {
            let png = screenshot::capture_png().await?;
            Ok(TaskResultMessage {
                task_id: request.task_id.clone(),
                ok: true,
                output: format!("captured {} bytes", png.len()),
                data_hex: Some(HEXLOWER.encode(&png)),
                stream_id: None,
            })
        }
        TaskAction::FileDownload => {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "file download must be handled by the streaming task runtime",
            ));
        }
        TaskAction::FileUpload => {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "file upload must be handled by the streaming task runtime",
            ));
        }
        TaskAction::InteractiveShell => {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "interactive shell must be handled by the task server runtime",
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, TaskAction, TaskRequestMessage},
        },
        task::dispatcher::dispatch,
        tunnel::tcp::{accept_peer, bind, connect_peer},
    };

    #[tokio::test]
    async fn dispatch_shell_task_returns_output() {
        let request = TaskRequestMessage {
            task_id: "task-shell-1".into(),
            action: TaskAction::Shell,
            args: vec!["echo fusion-task".into()],
            data_hex: None,
        };
        let result = dispatch(&request).await;
        assert!(result.ok);
        assert!(result.output.contains("fusion-task"));
    }

    #[tokio::test]
    async fn task_shell_roundtrip_over_peer_session() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("task-server".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("task-client".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let mut peer = accept_peer(server_identity, listener).await.unwrap();
            let frame = peer.read_frame().await.unwrap();
            let request = match frame.message {
                Message::TaskRequest(req) => req,
                other => panic!("unexpected message: {:?}", other),
            };
            let result = dispatch(&request).await;
            let response = Frame::new(
                MessageType::TaskResult,
                Some(peer.session.local.agent_id.clone()),
                Some(peer.session.remote.agent_id.clone()),
                Message::TaskResult(result),
            );
            peer.send_frame(&response).await.unwrap();
        });

        let mut client_peer = connect_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        let request = TaskRequestMessage {
            task_id: "task-roundtrip-1".into(),
            action: TaskAction::Shell,
            args: vec!["echo fusion-peer-task".into()],
            data_hex: None,
        };
        let frame = Frame::new(
            MessageType::TaskRequest,
            Some(client_peer.session.local.agent_id.clone()),
            Some(client_peer.session.remote.agent_id.clone()),
            Message::TaskRequest(request),
        );
        client_peer.send_frame(&frame).await.unwrap();

        let response = client_peer.read_frame().await.unwrap();
        match response.message {
            Message::TaskResult(result) => {
                assert!(result.ok);
                assert!(result.output.contains("fusion-peer-task"));
            }
            other => panic!("unexpected response: {:?}", other),
        }

        server_task.await.unwrap();
    }
}
