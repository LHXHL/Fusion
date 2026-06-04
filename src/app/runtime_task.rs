use std::{
    io::{Error, ErrorKind},
    path::PathBuf,
};

use crate::{
    app::config::TaskRequestConfig,
    protocol::message::{TaskAction, TaskResultMessage},
};

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
