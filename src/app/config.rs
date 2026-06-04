use std::{
    io::{Error, ErrorKind},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use crate::{protocol::message::TaskAction, utils::url::ParsedUrl};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_retries: Option<u32>,
    pub interval_secs: u64,
    pub max_interval_secs: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: None,
            interval_secs: 10,
            max_interval_secs: 300,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentIdentityConfig {
    pub name: Option<String>,
    pub key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRequestConfig {
    pub action: TaskAction,
    pub args: Vec<String>,
    pub data_hex: Option<String>,
    pub save_path: Option<PathBuf>,
    pub target_agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StatusScope {
    All,
    Peers,
    Routes,
    Streams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusCommandConfig {
    pub scope: StatusScope,
    pub json: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileRetryPolicy {
    pub max_retries: Option<u32>,
    pub interval_secs: Option<u64>,
    pub max_interval_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileAgentIdentityConfig {
    pub name: Option<String>,
    pub key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileAppConfig {
    pub listens: Option<Vec<String>>,
    pub connects: Option<Vec<String>>,
    pub local_serves: Option<Vec<String>>,
    pub remote_serves: Option<Vec<String>>,
    pub remote_peer_id: Option<String>,
    pub identity: Option<FileAgentIdentityConfig>,
    pub retry: Option<FileRetryPolicy>,
    pub data_dir: Option<PathBuf>,
    pub log_level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlCommandConfig {
    PeersList { json: bool },
    PeersInfo { peer_id: String, json: bool },
    RoutesList { json: bool },
    ServicesList { json: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TunnelEndpoint {
    pub url: ParsedUrl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServeEndpoint {
    pub url: ParsedUrl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub listens: Vec<TunnelEndpoint>,
    pub connects: Vec<TunnelEndpoint>,
    pub local_serves: Vec<ServeEndpoint>,
    pub remote_serves: Vec<ServeEndpoint>,
    pub remote_peer_id: Option<String>,
    pub identity: AgentIdentityConfig,
    pub retry: RetryPolicy,
    pub task_request: Option<TaskRequestConfig>,
    pub status_command: Option<StatusCommandConfig>,
    pub control_command: Option<ControlCommandConfig>,
    pub config_file: Option<PathBuf>,
    pub data_dir: PathBuf,
    pub log_level: String,
}

impl AppConfig {
    pub fn summary_lines(&self) -> Vec<String> {
        vec![
            format!(
                "agent.name={}",
                self.identity.name.as_deref().unwrap_or("<auto>")
            ),
            format!("listen.count={}", self.listens.len()),
            format!("connect.count={}", self.connects.len()),
            format!("local_serve.count={}", self.local_serves.len()),
            format!("remote_serve.count={}", self.remote_serves.len()),
            format!(
                "remote.peer={}",
                self.remote_peer_id.as_deref().unwrap_or("<direct>")
            ),
            format!(
                "task.mode={}",
                self.task_request
                    .as_ref()
                    .map(|task| match &task.save_path {
                        Some(path) => format!("{:?}@{}", task.action, path.display()),
                        None => format!("{:?}", task.action),
                    })
                    .unwrap_or_else(|| "disabled".to_string())
            ),
            format!(
                "status.mode={}",
                self.status_command
                    .as_ref()
                    .map(|s| format!("{:?}", s.scope))
                    .unwrap_or_else(|| "disabled".to_string())
            ),
            format!(
                "control.mode={}",
                self.control_command
                    .as_ref()
                    .map(|cmd| match cmd {
                        ControlCommandConfig::PeersList { .. } => "PeersList".to_string(),
                        ControlCommandConfig::PeersInfo { peer_id, .. } => {
                            format!("PeersInfo@{peer_id}")
                        }
                        ControlCommandConfig::RoutesList { .. } => "RoutesList".to_string(),
                        ControlCommandConfig::ServicesList { .. } => "ServicesList".to_string(),
                    })
                    .unwrap_or_else(|| "disabled".to_string())
            ),
            format!(
                "config.file={}",
                self.config_file
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<none>".to_string())
            ),
            format!("data_dir={}", self.data_dir.display()),
            format!("log_level={}", self.log_level),
            format!(
                "retry=max:{:?},interval:{}s,max_interval:{}s",
                self.retry.max_retries, self.retry.interval_secs, self.retry.max_interval_secs
            ),
        ]
    }
}

pub fn load_file_config(path: &PathBuf) -> Result<FileAppConfig, Error> {
    let payload = std::fs::read_to_string(path).map_err(|err| {
        Error::new(
            err.kind(),
            format!("failed to read config file {}: {}", path.display(), err),
        )
    })?;
    toml::from_str(&payload).map_err(|err| {
        Error::new(
            ErrorKind::InvalidData,
            format!("failed to parse config file {}: {}", path.display(), err),
        )
    })
}
