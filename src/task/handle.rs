use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    protocol::message::{TaskAction, TaskRequestMessage, TaskResultMessage},
    task::{
        dispatcher,
        file_transfer,
        interactive_shell::{self, ShellPeer},
    },
};

fn empty_task_result(task_id: &str) -> TaskResultMessage {
    TaskResultMessage {
        task_id: task_id.to_string(),
        ok: true,
        output: String::new(),
        data_hex: None,
        stream_id: None,
    }
}

pub async fn handle_local_task_request(
    request: &TaskRequestMessage,
    _stream_allocator: &Arc<Mutex<u32>>,
    peer: ShellPeer,
) -> TaskResultMessage {
    match request.action {
        TaskAction::InteractiveShell => {
            interactive_shell::arm_interactive_shell(&request.task_id, request.args.first().cloned())
                .await;
            let _ = peer;
            empty_task_result(&request.task_id)
        }
        TaskAction::FileDownload => {
            let remote_path = request.args.first().cloned().unwrap_or_default();
            file_transfer::arm_download(&request.task_id, remote_path).await;
            let _ = peer;
            empty_task_result(&request.task_id)
        }
        TaskAction::FileUpload => {
            let remote_path = request.args.first().cloned().unwrap_or_default();
            file_transfer::arm_upload(&request.task_id, remote_path).await;
            let _ = peer;
            empty_task_result(&request.task_id)
        }
        _ => dispatcher::dispatch(request).await,
    }
}
